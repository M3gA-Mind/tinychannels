//! Bounded, runtime-local host-to-module media uploads.
use futures_util::FutureExt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashMap, time::Duration};
use tinybus::{Error, Result};
use tinychannels_bus::{
    MediaUploadBegin, MediaUploadChunk, MediaUploadFinish, MediaUploadHandle, MediaUploadResult,
};
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

#[async_trait::async_trait]
pub(crate) trait MediaPersistence: Send + Sync {
    async fn store_public_media(
        &self,
        request: tinychannels::host::MediaStoreRequest,
    ) -> Result<MediaUploadResult>;
}

const UPLOAD_LIFETIME: Duration = Duration::from_secs(60);
const TERMINAL_LIFETIME: Duration = Duration::from_secs(5 * 60);
const TERMINAL_CAPACITY: usize = 128;
const READ_LIFETIME: Duration = Duration::from_secs(60);
const READ_IO_TIMEOUT: Duration = Duration::from_secs(30);

struct Read {
    instance: String,
    activation: String,
    expires: Instant,
    bytes: Vec<u8>,
    next_sequence: usize,
    last_chunk: Option<tinychannels_bus::MediaReadChunkResult>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Default)]
struct ReadState {
    reads: HashMap<String, Read>,
}

/// Runtime-local bounded byte buffers created only after scoped-store validation.
pub(crate) struct MediaReads {
    state: Arc<Mutex<ReadState>>,
    capacity: Arc<Semaphore>,
    expiry_janitor_running: Arc<AtomicBool>,
}

impl Default for MediaReads {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(ReadState::default())),
            capacity: Arc::new(Semaphore::new(
                tinychannels_bus::module_media::MEDIA_READ_CAPACITY,
            )),
            expiry_janitor_running: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub(crate) struct MediaReadReservation(OwnedSemaphorePermit);

impl MediaReads {
    pub(crate) async fn reserve(&self) -> Result<MediaReadReservation> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        drop(state);
        self.capacity
            .clone()
            .try_acquire_owned()
            .map(MediaReadReservation)
            .map_err(|_| Error::failed("media read capacity is full"))
    }

    pub(crate) async fn resolve<F, Fut, V, Vut>(
        &self,
        request: tinychannels_bus::MediaReadBegin,
        activation: String,
        load: F,
        still_active: V,
    ) -> Result<tinychannels_bus::MediaReadInfo>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<tinychannels::host::StoredMedia>>,
        V: FnOnce() -> Vut,
        Vut: Future<Output = bool>,
    {
        self.resolve_with_deadline(request, activation, READ_IO_TIMEOUT, load, still_active)
            .await
    }

    async fn resolve_with_deadline<F, Fut, V, Vut>(
        &self,
        request: tinychannels_bus::MediaReadBegin,
        activation: String,
        deadline: Duration,
        load: F,
        still_active: V,
    ) -> Result<tinychannels_bus::MediaReadInfo>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<tinychannels::host::StoredMedia>>,
        V: FnOnce() -> Vut,
        Vut: Future<Output = bool>,
    {
        let reservation = self.reserve().await?;
        let media = tokio::time::timeout(deadline, load())
            .await
            .map_err(|_| Error::failed("media resolution timed out"))??;
        if !still_active().await {
            return Err(Error::failed(
                "media provider activation changed during resolution",
            ));
        }
        self.complete(request, activation, media, reservation).await
    }

    pub(crate) async fn complete(
        &self,
        request: tinychannels_bus::MediaReadBegin,
        activation: String,
        media: tinychannels::host::StoredMedia,
        reservation: MediaReadReservation,
    ) -> Result<tinychannels_bus::MediaReadInfo> {
        let max = tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_BYTES;
        let bytes = media.bytes.len();
        let chunk_size = tinychannels_bus::module_media::MEDIA_READ_CHUNK_BYTES;
        if request.instance.trim().is_empty()
            || bytes == 0
            || bytes > max
            || bytes as u64 > request.max_bytes
            || media.media.size_bytes != Some(bytes as u64)
        {
            return Err(Error::failed("resolved media exceeds its size limit"));
        }
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        let handle = loop {
            let candidate = uuid::Uuid::new_v4().simple().to_string();
            if !state.reads.contains_key(&candidate) {
                break candidate;
            }
        };
        let chunk_count = bytes.div_ceil(chunk_size);
        let mut resolved_reference = media.media;
        resolved_reference.path = None;
        resolved_reference.url = None;
        resolved_reference.local_cache_path = None;
        let info = tinychannels_bus::MediaReadInfo {
            handle: tinychannels_bus::MediaReadHandle(handle.clone()),
            media: resolved_reference,
            content_type: media.content_type,
            file_name: media.file_name,
            byte_count: bytes,
            chunk_count: chunk_count as u32,
        };
        let info_bytes = serde_json::to_vec(&info)
            .map_err(|_| Error::failed("resolved media metadata is invalid"))?;
        if info_bytes.len() > tinychannels_bus::module_media::MEDIA_UPLOAD_BEGIN_MAX_BYTES {
            return Err(Error::failed("resolved media metadata exceeds its limit"));
        }
        let expires = Instant::now() + READ_LIFETIME;
        state.reads.insert(
            handle,
            Read {
                instance: request.instance,
                activation,
                expires,
                bytes: media.bytes,
                next_sequence: 0,
                last_chunk: None,
                _permit: reservation.0,
            },
        );
        drop(state);
        self.start_expiry_janitor();
        Ok(info)
    }

    pub(crate) async fn binding(
        &self,
        handle: &tinychannels_bus::MediaReadHandle,
    ) -> Result<(String, String)> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        state
            .reads
            .get(&handle.0)
            .map(|read| (read.instance.clone(), read.activation.clone()))
            .ok_or_else(|| Error::failed("media read is unknown or expired"))
    }

    pub(crate) async fn chunk(
        &self,
        request: tinychannels_bus::MediaReadChunk,
    ) -> Result<tinychannels_bus::MediaReadChunkResult> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        let read = state
            .reads
            .get_mut(&request.handle.0)
            .ok_or_else(|| Error::failed("media read is unknown or expired"))?;
        let sequence = request.sequence as usize;
        if let Some(last) = &read.last_chunk
            && sequence + 1 == read.next_sequence
            && sequence == last.sequence as usize
        {
            return Ok(last.clone());
        }
        if sequence != read.next_sequence {
            return Err(Error::failed("media chunks must be read in order"));
        }
        let size = tinychannels_bus::module_media::MEDIA_READ_CHUNK_BYTES;
        let start = sequence
            .checked_mul(size)
            .ok_or_else(|| Error::failed("media chunk sequence exceeds its limit"))?;
        if start >= read.bytes.len() {
            return Err(Error::failed("media chunk sequence exceeds its limit"));
        }
        let end = (start + size).min(read.bytes.len());
        let result = tinychannels_bus::MediaReadChunkResult {
            sequence: request.sequence,
            bytes: read.bytes[start..end].to_vec(),
            done: end == read.bytes.len(),
        };
        read.next_sequence += 1;
        read.last_chunk = Some(result.clone());
        Ok(result)
    }

    pub(crate) async fn abort(&self, request: tinychannels_bus::MediaReadFinish) -> Result<()> {
        self.state
            .lock()
            .await
            .reads
            .remove(&request.0.0)
            .map(|_| ())
            .ok_or_else(|| Error::failed("media read is unknown or expired"))
    }

    pub(crate) async fn invalidate_instance(&self, instance: &str) {
        self.state
            .lock()
            .await
            .reads
            .retain(|_, read| read.instance != instance);
    }

    fn start_expiry_janitor(&self) {
        if self.expiry_janitor_running.swap(true, Ordering::SeqCst) {
            return;
        }
        let state = Arc::downgrade(&self.state);
        let running = Arc::clone(&self.expiry_janitor_running);
        tokio::spawn(async move {
            loop {
                let next_expiry = {
                    let Some(state) = state.upgrade() else {
                        running.store(false, Ordering::SeqCst);
                        return;
                    };
                    let mut guard = state.lock().await;
                    Self::prune(&mut guard);
                    let next_expiry = guard.reads.values().map(|read| read.expires).min();
                    if next_expiry.is_none() {
                        running.store(false, Ordering::SeqCst);
                    }
                    next_expiry
                };
                let Some(next_expiry) = next_expiry else {
                    return;
                };
                tokio::time::sleep_until(next_expiry).await;
            }
        });
    }

    fn prune(state: &mut ReadState) {
        let now = Instant::now();
        state.reads.retain(|_, read| read.expires > now);
    }
}

struct Upload {
    request: MediaUploadBegin,
    activation: String,
    expires: Instant,
    chunks: Vec<Option<Vec<u8>>>,
    next_sequence: usize,
    finalizing: std::sync::Arc<AtomicBool>,
    changed: std::sync::Arc<Notify>,
}

enum Terminal {
    Finished(Box<MediaUploadResult>),
    Aborted,
}

struct TerminalEntry {
    value: Terminal,
    instance: String,
    activation: String,
    expires: Instant,
}

#[derive(Default)]
struct State {
    uploads: HashMap<String, Upload>,
    terminal: HashMap<String, TerminalEntry>,
}

/// Per-service-object upload state. Runtime object ownership bounds uploads to
/// the authenticated runtime that received the begin call.
#[derive(Default)]
pub(crate) struct MediaUploads {
    state: Mutex<State>,
}

impl MediaUploads {
    pub(crate) async fn invalidate_instance(&self, instance: &str) {
        self.state.lock().await.uploads.retain(|_, upload| {
            upload.request.instance != instance || upload.finalizing.load(Ordering::SeqCst)
        });
    }

    pub(crate) async fn binding_handle(
        &self,
        handle: &MediaUploadHandle,
    ) -> Result<(String, String)> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        state
            .uploads
            .get(&handle.0)
            .map(|upload| (upload.request.instance.clone(), upload.activation.clone()))
            .ok_or_else(|| Error::failed("media upload is unknown or expired"))
    }

    pub(crate) async fn binding(&self, request: &MediaUploadFinish) -> Result<(String, String)> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        state
            .uploads
            .get(&request.0.0)
            .map(|upload| (upload.request.instance.clone(), upload.activation.clone()))
            .or_else(|| {
                state
                    .terminal
                    .get(&request.0.0)
                    .map(|entry| (entry.instance.clone(), entry.activation.clone()))
            })
            .ok_or_else(|| Error::failed("media upload is unknown or expired"))
    }

    pub(crate) async fn begin(
        &self,
        request: MediaUploadBegin,
        activation: String,
    ) -> Result<MediaUploadHandle> {
        let max = tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_BYTES;
        if request.instance.trim().is_empty()
            || request.scope.conversation.id.trim().is_empty()
            || request.scope.turn_id.trim().is_empty()
            || request.byte_count == 0
            || request.byte_count > max
        {
            return Err(Error::failed("media upload metadata is invalid"));
        }
        let metadata_bytes = serde_json::to_vec(&request)
            .map_err(|_| Error::failed("media upload metadata is invalid"))?;
        if metadata_bytes.len() > tinychannels_bus::module_media::MEDIA_UPLOAD_BEGIN_MAX_BYTES {
            return Err(Error::failed("media upload metadata exceeds its limit"));
        }
        let count = request
            .byte_count
            .div_ceil(tinychannels_bus::module_media::MEDIA_UPLOAD_CHUNK_BYTES);
        if count == 0 || count > tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_CHUNKS {
            return Err(Error::failed("media upload exceeds its chunk limit"));
        }
        let mut state = self.state.lock().await;
        state.uploads.retain(|_, upload| {
            upload.finalizing.load(Ordering::SeqCst)
                || upload.request.instance != request.instance
                || upload.activation == activation
        });
        Self::prune(&mut state);
        if state.uploads.len() >= tinychannels_bus::module_media::MEDIA_UPLOAD_CAPACITY {
            return Err(Error::failed("media upload capacity is full"));
        }
        let handle = loop {
            let candidate = uuid::Uuid::new_v4().simple().to_string();
            if !state.uploads.contains_key(&candidate) && !state.terminal.contains_key(&candidate) {
                break candidate;
            }
        };
        state.uploads.insert(
            handle.clone(),
            Upload {
                request,
                activation,
                expires: Instant::now() + UPLOAD_LIFETIME,
                chunks: (0..count).map(|_| None).collect(),
                next_sequence: 0,
                finalizing: std::sync::Arc::new(AtomicBool::new(false)),
                changed: std::sync::Arc::new(Notify::new()),
            },
        );
        Ok(MediaUploadHandle(handle))
    }

    pub(crate) async fn append(&self, chunk: MediaUploadChunk) -> Result<u32> {
        if chunk.bytes.is_empty()
            || chunk.bytes.len() > tinychannels_bus::module_media::MEDIA_UPLOAD_CHUNK_BYTES
        {
            return Err(Error::failed("media chunk is outside its size limit"));
        }
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        let upload = state
            .uploads
            .get_mut(&chunk.handle.0)
            .ok_or_else(|| Error::failed("media upload is unknown or expired"))?;
        if upload.expires <= Instant::now() {
            state.uploads.remove(&chunk.handle.0);
            return Err(Error::failed("media upload is unknown or expired"));
        }
        if upload.finalizing.load(Ordering::SeqCst) {
            return Err(Error::failed("media upload is already finalizing"));
        }
        let sequence = chunk.sequence as usize;
        if sequence >= upload.chunks.len() {
            return Err(Error::failed("media chunk sequence exceeds its limit"));
        }
        let expected_len = if sequence + 1 == upload.chunks.len() {
            upload.request.byte_count
                - sequence * tinychannels_bus::module_media::MEDIA_UPLOAD_CHUNK_BYTES
        } else {
            tinychannels_bus::module_media::MEDIA_UPLOAD_CHUNK_BYTES
        };
        if chunk.bytes.len() != expected_len {
            return Err(Error::failed(
                "media chunk size does not match its sequence",
            ));
        }
        if sequence < upload.next_sequence {
            if upload.chunks[sequence].as_ref() == Some(&chunk.bytes) {
                return Ok(upload.next_sequence as u32);
            }
            return Err(Error::failed("media chunk conflicts with an earlier write"));
        }
        if sequence != upload.next_sequence {
            return Err(Error::failed("media chunks must be appended in order"));
        }
        upload.chunks[sequence] = Some(chunk.bytes);
        upload.next_sequence += 1;
        Ok(upload.next_sequence as u32)
    }

    pub(crate) async fn finish(
        self: std::sync::Arc<Self>,
        request: MediaUploadFinish,
        activation: String,
        repository: std::sync::Arc<dyn MediaPersistence>,
    ) -> Result<MediaUploadResult> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        if let Some(entry) = state.terminal.get(&request.0.0) {
            if entry.activation != activation {
                return Err(Error::failed(
                    "media upload belongs to another provider activation",
                ));
            }
            return match &entry.value {
                Terminal::Finished(media) => Ok(media.as_ref().clone()),
                Terminal::Aborted => Err(Error::failed("media upload was aborted")),
            };
        }
        let Some(upload) = state.uploads.get_mut(&request.0.0) else {
            return Err(Error::failed("media upload is unknown or expired"));
        };
        if upload.activation != activation {
            return Err(Error::failed(
                "media upload belongs to another provider activation",
            ));
        }
        if upload.finalizing.load(Ordering::SeqCst) {
            let changed = upload.changed.clone();
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            drop(state);
            notified.await;
            return Box::pin(self.finish(request, activation, repository)).await;
        }
        if upload.expires <= Instant::now() {
            state.uploads.remove(&request.0.0);
            return Err(Error::failed("media upload is unknown or expired"));
        }
        if upload.next_sequence != upload.chunks.len() {
            return Err(Error::failed("media upload is incomplete"));
        }
        upload.finalizing.store(true, Ordering::SeqCst);
        let changed = upload.changed.clone();
        let instance = upload.request.instance.clone();
        let activation_for_terminal = upload.activation.clone();
        let request_metadata = upload.request.clone();
        let chunks = upload.chunks.clone();
        let bytes: Vec<u8> = chunks.into_iter().flatten().flatten().collect();
        if bytes.len() != request_metadata.byte_count {
            if let Some(upload) = state.uploads.get_mut(&request.0.0) {
                upload.finalizing.store(false, Ordering::SeqCst);
                upload.changed.notify_waiters();
            }
            return Err(Error::failed("media upload byte count does not match"));
        }
        drop(state);
        let owner = self.clone();
        let handle = request.0.0;
        let task = tokio::spawn(async move {
            let write = std::panic::AssertUnwindSafe(repository.store_public_media(
                tinychannels::host::MediaStoreRequest {
                    metadata: tinychannels::host::MediaMetadata {
                        scope: request_metadata.scope,
                        kind: request_metadata.kind,
                        content_type: request_metadata.content_type,
                        file_name: request_metadata.file_name,
                        duration_ms: request_metadata.duration_ms,
                        codec: request_metadata.codec,
                        voice_direction: None,
                        dedupe_key: Some(format!("module-upload:{handle}")),
                        max_bytes: tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_BYTES as u64,
                    },
                    bytes,
                },
            ))
            .catch_unwind();
            let media = match write.await {
                Ok(result) => result,
                Err(_) => Err(Error::failed("media storage failed")),
            };
            let mut state = owner.state.lock().await;
            Self::prune(&mut state);
            let Some(mut upload) = state.uploads.remove(&handle) else {
                changed.notify_waiters();
                return Err(Error::failed("media upload is no longer active"));
            };
            match media {
                Ok(media) => {
                    state.terminal.insert(
                        handle,
                        TerminalEntry {
                            value: Terminal::Finished(Box::new(media.clone())),
                            instance,
                            activation: activation_for_terminal,
                            expires: Instant::now() + TERMINAL_LIFETIME,
                        },
                    );
                    Self::trim_terminal(&mut state);
                    changed.notify_waiters();
                    Ok(media)
                }
                Err(error) => {
                    upload.finalizing.store(false, Ordering::SeqCst);
                    upload.expires = Instant::now() + UPLOAD_LIFETIME;
                    state.uploads.insert(handle, upload);
                    changed.notify_waiters();
                    Err(error)
                }
            }
        });
        task.await
            .map_err(|_| Error::failed("media upload finalization failed"))?
    }

    pub(crate) async fn abort(&self, request: MediaUploadFinish) -> Result<()> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        if let Some(entry) = state.terminal.get(&request.0.0) {
            return match entry.value {
                Terminal::Aborted => Ok(()),
                Terminal::Finished(_) => Err(Error::failed("media upload is already finalized")),
            };
        }
        let Some(upload) = state.uploads.get(&request.0.0) else {
            return Err(Error::failed("media upload is unknown or expired"));
        };
        if upload.finalizing.load(Ordering::SeqCst) {
            return Err(Error::failed("media upload is already finalizing"));
        }
        let upload = state.uploads.remove(&request.0.0).expect("checked above");
        state.terminal.insert(
            request.0.0,
            TerminalEntry {
                value: Terminal::Aborted,
                instance: upload.request.instance,
                activation: upload.activation,
                expires: Instant::now() + TERMINAL_LIFETIME,
            },
        );
        Self::trim_terminal(&mut state);
        Ok(())
    }

    fn prune(state: &mut State) {
        let now = Instant::now();
        state
            .uploads
            .retain(|_, upload| upload.finalizing.load(Ordering::SeqCst) || upload.expires > now);
        state.terminal.retain(|_, entry| entry.expires > now);
    }

    fn trim_terminal(state: &mut State) {
        while state.terminal.len() > TERMINAL_CAPACITY {
            let Some(oldest) = state
                .terminal
                .iter()
                .min_by_key(|(_, entry)| entry.expires)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            state.terminal.remove(&oldest);
        }
    }
}

#[cfg(test)]
#[path = "media_tests.rs"]
mod tests;
