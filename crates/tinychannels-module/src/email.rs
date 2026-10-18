//! Bounded owner for confidential send-only MIME requests.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tinybus::{Error, Result};
use tinychannels::providers::mail::{
    LettreMailSender, MailAttachment, MailCredentials, MailSender, OutboundEmail,
};
use tinychannels_bus::controllers::ChannelSendMessageResult;
use tinychannels_bus::module_email::{
    EMAIL_ATTACHMENT_BYTES, EMAIL_ATTACHMENT_COUNT, EMAIL_UPLOAD_CHUNK_BYTES,
    EMAIL_UPLOAD_MAX_CHUNKS, EmailSendRequest, EmailUploadBegin, EmailUploadChunk,
    EmailUploadHandle,
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

const QUEUE_CAPACITY: usize = 2;
const UPLOAD_CAPACITY: usize = 4;
const UPLOAD_RESULT_CAPACITY: usize = 64;
const UPLOAD_TTL: Duration = Duration::from_secs(10 * 60);

enum Command {
    Send {
        request: Box<EmailSendRequest>,
        reply: oneshot::Sender<Result<ChannelSendMessageResult>>,
    },
    SendUpload {
        handle: String,
        request: Box<EmailSendRequest>,
    },
    Stop(oneshot::Sender<()>),
}

struct UploadAttachment {
    filename: String,
    content_type: String,
    expected_bytes: usize,
    bytes: Vec<u8>,
    next_sequence: u32,
    chunk_ranges: Vec<(usize, usize)>,
}

struct UploadData {
    begin: EmailUploadBegin,
    attachments: Vec<UploadAttachment>,
}

enum UploadState {
    Uploading(Box<UploadData>),
    Sending(watch::Sender<Option<UploadResult>>),
    Complete(UploadResult),
}

#[derive(Clone)]
enum UploadResult {
    Success,
    Failed(String),
}

struct Upload {
    expires_at: Instant,
    reserved_bytes: usize,
    state: UploadState,
}

#[derive(Default)]
struct UploadStore {
    uploads: HashMap<String, Upload>,
    reserved_bytes: usize,
}

struct Owner {
    sender: mpsc::Sender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    terminal: AtomicBool,
    admission: Mutex<()>,
    uploads: Arc<Mutex<UploadStore>>,
    #[cfg(test)]
    result_wait_gate: Mutex<Option<Arc<ResultWaitGate>>>,
    #[cfg(test)]
    send_admission_gate: Mutex<Option<Arc<ResultWaitGate>>>,
}

#[cfg(test)]
struct ResultWaitGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().take() {
            worker.abort();
        }
    }
}

/// A bounded, ordered send-only email actor. Caller cancellation never cancels
/// an SMTP send already accepted into its queue.
#[derive(Clone)]
pub(crate) struct EmailSender(Arc<Owner>);

impl EmailSender {
    pub(crate) fn new() -> Self {
        let (sender, mut receiver) = mpsc::channel(QUEUE_CAPACITY);
        let uploads = Arc::new(Mutex::new(UploadStore::default()));
        let worker_uploads = uploads.clone();
        let worker = tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Send { request, reply } => {
                        let result = send(*request).await;
                        let _ = reply.send(result);
                    }
                    Command::SendUpload { handle, request } => {
                        let result = send(*request).await;
                        let mut store = worker_uploads.lock().await;
                        let completion = if let Some(upload) = store.uploads.get_mut(&handle) {
                            let completion_signal = match &upload.state {
                                UploadState::Sending(signal) => Some(signal.clone()),
                                _ => None,
                            };
                            let reserved_bytes = upload.reserved_bytes;
                            upload.reserved_bytes = 0;
                            upload.expires_at = Instant::now() + UPLOAD_TTL;
                            upload.state = match result {
                                Ok(_) => UploadState::Complete(UploadResult::Success),
                                Err(error) => {
                                    UploadState::Complete(UploadResult::Failed(error.to_string()))
                                }
                            };
                            let result = match &upload.state {
                                UploadState::Complete(result) => Some(result.clone()),
                                _ => None,
                            };
                            Some((reserved_bytes, completion_signal, result))
                        } else {
                            None
                        };
                        if let Some((reserved_bytes, completion_signal, result)) = completion {
                            store.reserved_bytes =
                                store.reserved_bytes.saturating_sub(reserved_bytes);
                            prune_completed(&mut store);
                            drop(store);
                            if let (Some(completion_signal), Some(result)) =
                                (completion_signal, result)
                            {
                                completion_signal.send_replace(Some(result));
                            }
                        }
                    }
                    Command::Stop(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
        });
        Self(Arc::new(Owner {
            sender,
            worker: Mutex::new(Some(worker)),
            terminal: AtomicBool::new(false),
            admission: Mutex::new(()),
            uploads,
            #[cfg(test)]
            result_wait_gate: Mutex::new(None),
            #[cfg(test)]
            send_admission_gate: Mutex::new(None),
        }))
    }

    pub(crate) async fn send(&self, request: EmailSendRequest) -> Result<ChannelSendMessageResult> {
        let admission = self.0.admission.lock().await;
        #[cfg(test)]
        if let Some(gate) = self.0.send_admission_gate.lock().await.clone() {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        if self.0.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("email sender is shutting down"));
        }
        validate(&request)?;
        let (reply, wait) = oneshot::channel();
        self.0
            .sender
            .try_send(Command::Send {
                request: Box::new(request),
                reply,
            })
            .map_err(|_| Error::failed("email send queue is full or closed"))?;
        drop(admission);
        wait.await
            .map_err(|_| Error::failed("email send owner stopped"))?
    }

    pub(crate) async fn begin_upload(&self, begin: EmailUploadBegin) -> Result<EmailUploadHandle> {
        let _admission = self.0.admission.lock().await;
        if self.0.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("email sender is shutting down"));
        }
        let request = EmailSendRequest {
            config: begin.config.clone(),
            recipient: begin.recipient.clone(),
            subject: begin.subject.clone(),
            body: begin.body.clone(),
            attachments: Vec::new(),
        };
        validate(&request)?;
        let bytes = begin
            .attachments
            .iter()
            .fold(0usize, |sum, part| sum.saturating_add(part.byte_count));
        if begin.attachments.len() > EMAIL_ATTACHMENT_COUNT
            || bytes > EMAIL_ATTACHMENT_BYTES
            || begin.attachments.iter().any(|part| {
                part.filename.is_empty()
                    || part.filename.len() > 255
                    || part.content_type.is_empty()
                    || part.content_type.len() > 256
            })
        {
            return Err(Error::failed("invalid or oversized email upload"));
        }
        let mut store = self.0.uploads.lock().await;
        prune_expired(&mut store);
        let active_uploads = store
            .uploads
            .values()
            .filter(|upload| {
                matches!(
                    upload.state,
                    UploadState::Uploading(_) | UploadState::Sending(_)
                )
            })
            .count();
        if self.0.terminal.load(Ordering::SeqCst)
            || active_uploads >= UPLOAD_CAPACITY
            || store.reserved_bytes.saturating_add(bytes) > EMAIL_ATTACHMENT_BYTES
        {
            return Err(Error::failed("email upload capacity is full or closed"));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let attachments = begin
            .attachments
            .iter()
            .map(|part| UploadAttachment {
                filename: part.filename.clone(),
                content_type: part.content_type.clone(),
                expected_bytes: part.byte_count,
                bytes: Vec::with_capacity(part.byte_count),
                next_sequence: 0,
                chunk_ranges: Vec::new(),
            })
            .collect();
        store.reserved_bytes += bytes;
        store.uploads.insert(
            id.clone(),
            Upload {
                expires_at: Instant::now() + UPLOAD_TTL,
                reserved_bytes: bytes,
                state: UploadState::Uploading(Box::new(UploadData { begin, attachments })),
            },
        );
        Ok(EmailUploadHandle(id))
    }

    pub(crate) async fn append_upload_chunk(&self, chunk: EmailUploadChunk) -> Result<u32> {
        let _admission = self.0.admission.lock().await;
        if self.0.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("email sender is shutting down"));
        }
        validate_upload_handle(&chunk.handle)?;
        if chunk.bytes.is_empty() || chunk.bytes.len() > EMAIL_UPLOAD_CHUNK_BYTES {
            return Err(Error::failed("email upload chunk byte limit exceeded"));
        }
        let mut store = self.0.uploads.lock().await;
        prune_expired(&mut store);
        let upload = store
            .uploads
            .get_mut(&chunk.handle.0)
            .ok_or_else(|| Error::failed("email upload handle expired or unknown"))?;
        let UploadState::Uploading(data) = &mut upload.state else {
            return Err(Error::failed("email upload is no longer writable"));
        };
        let attachment = data
            .attachments
            .get_mut(chunk.attachment_index as usize)
            .ok_or_else(|| Error::failed("email upload attachment index is invalid"))?;
        if chunk.sequence < attachment.next_sequence {
            let Some((start, end)) = attachment.chunk_ranges.get(chunk.sequence as usize) else {
                return Err(Error::failed("email upload chunk sequence is invalid"));
            };
            if attachment.bytes[*start..*end] == chunk.bytes {
                return Ok(attachment.next_sequence);
            }
            return Err(Error::failed(
                "email upload chunk conflicts with prior sequence",
            ));
        }
        if chunk.sequence != attachment.next_sequence
            || attachment.next_sequence as usize >= EMAIL_UPLOAD_MAX_CHUNKS
            || attachment.bytes.len().saturating_add(chunk.bytes.len()) > attachment.expected_bytes
        {
            return Err(Error::failed(
                "email upload chunk sequence or length is invalid",
            ));
        }
        let start = attachment.bytes.len();
        attachment.bytes.extend_from_slice(&chunk.bytes);
        attachment
            .chunk_ranges
            .push((start, attachment.bytes.len()));
        attachment.next_sequence = attachment
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::failed("email upload chunk sequence exhausted"))?;
        upload.expires_at = Instant::now() + UPLOAD_TTL;
        Ok(attachment.next_sequence)
    }

    pub(crate) async fn send_upload(
        &self,
        handle: EmailUploadHandle,
    ) -> Result<ChannelSendMessageResult> {
        let admission = self.0.admission.lock().await;
        validate_upload_handle(&handle)?;
        let mut store = self.0.uploads.lock().await;
        prune_expired(&mut store);
        let upload = store
            .uploads
            .get_mut(&handle.0)
            .ok_or_else(|| Error::failed("email upload handle expired or unknown"))?;
        let mut waiter = match &mut upload.state {
            UploadState::Complete(result) => return result.clone().into_result(),
            UploadState::Sending(signal) => signal.subscribe(),
            UploadState::Uploading(data) => {
                if self.0.terminal.load(Ordering::SeqCst) {
                    return Err(Error::failed("email sender is shutting down"));
                }
                if data
                    .attachments
                    .iter()
                    .any(|part| part.bytes.len() != part.expected_bytes)
                {
                    return Err(Error::failed("email upload is incomplete"));
                }
                let permit = self
                    .0
                    .sender
                    .try_reserve()
                    .map_err(|_| Error::failed("email send queue is full or closed"))?;
                let (completion_signal, waiter) = watch::channel(None);
                let data = match std::mem::replace(
                    &mut upload.state,
                    UploadState::Sending(completion_signal),
                ) {
                    UploadState::Uploading(data) => *data,
                    _ => unreachable!("upload state changed under lock"),
                };
                let request = EmailSendRequest {
                    config: data.begin.config,
                    recipient: data.begin.recipient,
                    subject: data.begin.subject,
                    body: data.begin.body,
                    attachments: data
                        .attachments
                        .into_iter()
                        .map(|part| tinychannels_bus::EmailAttachment {
                            filename: part.filename,
                            content_type: part.content_type,
                            bytes: part.bytes,
                        })
                        .collect(),
                };
                permit.send(Command::SendUpload {
                    handle: handle.0.clone(),
                    request: Box::new(request),
                });
                waiter
            }
        };
        drop(store);
        drop(admission);
        if waiter.borrow().is_none() {
            waiter
                .changed()
                .await
                .map_err(|_| Error::failed("email upload completion was interrupted"))?;
        }
        #[cfg(test)]
        if let Some(gate) = self.0.result_wait_gate.lock().await.clone() {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        waiter
            .borrow()
            .clone()
            .ok_or_else(|| Error::failed("email upload completion was interrupted"))?
            .into_result()
    }

    pub(crate) async fn abort_upload(&self, handle: EmailUploadHandle) -> Result<()> {
        validate_upload_handle(&handle)?;
        loop {
            let admission = self.0.admission.lock().await;
            let mut store = self.0.uploads.lock().await;
            prune_expired(&mut store);
            let Some(upload) = store.uploads.get(&handle.0) else {
                return Ok(());
            };
            if let UploadState::Sending(signal) = &upload.state {
                let mut waiter = signal.subscribe();
                drop(store);
                drop(admission);
                if waiter.borrow().is_none() {
                    let _ = waiter.changed().await;
                }
                continue;
            }
            if matches!(upload.state, UploadState::Complete(_)) {
                return Err(Error::failed("email upload is already being delivered"));
            }
            let upload = store.uploads.remove(&handle.0).expect("checked above");
            store.reserved_bytes = store.reserved_bytes.saturating_sub(upload.reserved_bytes);
            return Ok(());
        }
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        let admission = self.0.admission.lock().await;
        self.0.terminal.store(true, Ordering::SeqCst);
        let mut worker = self.0.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            let (reply, wait) = oneshot::channel();
            let stop_enqueued = self.0.sender.send(Command::Stop(reply)).await.is_ok();
            drop(admission);
            if stop_enqueued {
                wait.await
                    .map_err(|_| Error::failed("email shutdown barrier interrupted"))?;
            }
            task.await
                .map_err(|_| Error::failed("email worker failed during shutdown"))?;
        }
        worker.take();
        let mut uploads = self.0.uploads.lock().await;
        uploads
            .uploads
            .retain(|_, upload| matches!(upload.state, UploadState::Complete(_)));
        uploads.reserved_bytes = 0;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn delay_upload_result_until(
        &self,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) {
        *self.0.result_wait_gate.lock().await = Some(Arc::new(ResultWaitGate { entered, release }));
    }

    #[cfg(test)]
    pub(crate) async fn delay_send_enqueue_until(
        &self,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) {
        *self.0.send_admission_gate.lock().await =
            Some(Arc::new(ResultWaitGate { entered, release }));
    }
}

impl UploadResult {
    fn into_result(self) -> Result<ChannelSendMessageResult> {
        match self {
            Self::Success => Ok(ChannelSendMessageResult::default()),
            Self::Failed(message) => Err(Error::failed(message)),
        }
    }
}

fn prune_expired(store: &mut UploadStore) {
    let expired: Vec<_> = store
        .uploads
        .iter()
        .filter(|(_, upload)| upload.expires_at <= Instant::now())
        .filter(|(_, upload)| !matches!(upload.state, UploadState::Sending(_)))
        .map(|(id, _)| id.clone())
        .collect();
    for id in expired {
        if let Some(upload) = store.uploads.remove(&id) {
            store.reserved_bytes = store.reserved_bytes.saturating_sub(upload.reserved_bytes);
        }
    }
    prune_completed(store);
}

fn prune_completed(store: &mut UploadStore) {
    let mut completed: Vec<_> = store
        .uploads
        .iter()
        .filter(|(_, upload)| matches!(upload.state, UploadState::Complete(_)))
        .map(|(id, upload)| (upload.expires_at, id.clone()))
        .collect();
    completed.sort_by_key(|(expires_at, _)| *expires_at);
    let excess = completed.len().saturating_sub(UPLOAD_RESULT_CAPACITY);
    for (_, id) in completed.into_iter().take(excess) {
        store.uploads.remove(&id);
    }
}

fn validate_upload_handle(handle: &EmailUploadHandle) -> Result<()> {
    if uuid::Uuid::parse_str(&handle.0).is_err() {
        return Err(Error::failed("email upload handle is invalid"));
    }
    Ok(())
}

fn validate(request: &EmailSendRequest) -> Result<()> {
    let attachment_bytes: usize = request
        .attachments
        .iter()
        .map(|part| part.bytes.len())
        .fold(0usize, usize::saturating_add);
    if request.recipient.is_empty()
        || request.recipient.len() > 1024
        || request.subject.len() > 4096
        || request.body.len() > 1024 * 1024
        || request.attachments.len() > EMAIL_ATTACHMENT_COUNT
        || attachment_bytes > EMAIL_ATTACHMENT_BYTES
        || request.attachments.iter().any(|part| {
            part.filename.is_empty()
                || part.filename.len() > 255
                || part.content_type.is_empty()
                || part.content_type.len() > 256
        })
        || request.config.smtp_host.len() > 1024
        || request.config.username.len() > 1024
        || request.config.password.len() > 4096
        || request.config.from_address.len() > 1024
    {
        return Err(Error::failed("invalid or oversized email send request"));
    }
    Ok(())
}

async fn send(request: EmailSendRequest) -> Result<ChannelSendMessageResult> {
    let channel = tinychannels::EmailChannel::new(request.config);
    let credentials = MailCredentials::Smtp(channel.smtp_credentials());
    let message = OutboundEmail {
        to: request.recipient,
        subject: request.subject,
        body: request.body,
        attachments: request
            .attachments
            .into_iter()
            .map(|part| MailAttachment {
                filename: part.filename,
                content_type: part.content_type,
                bytes: part.bytes,
            })
            .collect(),
        headers: Default::default(),
    };
    LettreMailSender::new()
        .send(&credentials, &message)
        .await
        .map_err(|error| Error::failed(error.to_string()))?;
    Ok(ChannelSendMessageResult::default())
}

#[cfg(test)]
#[path = "email_tests.rs"]
mod tests;
