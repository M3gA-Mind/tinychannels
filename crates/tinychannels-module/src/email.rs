//! Bounded owner for confidential send-only MIME requests.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tinybus::{Error, Result};
use tinychannels::providers::mail::{
    LettreMailSender, MailAttachment, MailCredentials, MailSender, OutboundEmail,
};
use tinychannels_bus::controllers::ChannelSendMessageResult;
use tinychannels_bus::module_email::{
    EMAIL_ATTACHMENT_BYTES, EMAIL_ATTACHMENT_COUNT, EmailSendRequest,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

const QUEUE_CAPACITY: usize = 2;

enum Command {
    Send {
        request: Box<EmailSendRequest>,
        reply: oneshot::Sender<Result<ChannelSendMessageResult>>,
    },
    Stop(oneshot::Sender<()>),
}

struct Owner {
    sender: mpsc::Sender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    terminal: AtomicBool,
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
        let worker = tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Send { request, reply } => {
                        let result = send(*request).await;
                        let _ = reply.send(result);
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
        }))
    }

    pub(crate) async fn send(&self, request: EmailSendRequest) -> Result<ChannelSendMessageResult> {
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
        wait.await
            .map_err(|_| Error::failed("email send owner stopped"))?
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.0.terminal.store(true, Ordering::SeqCst);
        let mut worker = self.0.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            let (reply, wait) = oneshot::channel();
            if self.0.sender.send(Command::Stop(reply)).await.is_ok() {
                wait.await
                    .map_err(|_| Error::failed("email shutdown barrier interrupted"))?;
            }
            task.await
                .map_err(|_| Error::failed("email worker failed during shutdown"))?;
        }
        worker.take();
        Ok(())
    }
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
