//! Bounded, runtime-local host-to-module media uploads.
use crate::provider::ModuleMediaRepository;
use std::{collections::HashMap, time::Duration};
use tinybus::{Error, Result};
use tinychannels_bus::{
    MediaUploadBegin, MediaUploadChunk, MediaUploadFinish, MediaUploadHandle, MediaUploadResult,
};
use tokio::sync::Mutex;
use tokio::time::Instant;

const UPLOAD_LIFETIME: Duration = Duration::from_secs(60);
const TERMINAL_LIFETIME: Duration = Duration::from_secs(5 * 60);
const TERMINAL_CAPACITY: usize = 128;

struct Upload {
    request: MediaUploadBegin,
    expires: Instant,
    chunks: Vec<Option<Vec<u8>>>,
    next_sequence: usize,
}

enum Terminal {
    Finished(MediaUploadResult),
    Aborted,
}

struct TerminalEntry {
    value: Terminal,
    instance: String,
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
    pub(crate) async fn instance(&self, request: &MediaUploadFinish) -> Result<String> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        state
            .uploads
            .get(&request.0.0)
            .map(|upload| upload.request.instance.clone())
            .or_else(|| state.terminal.get(&request.0.0).map(|entry| entry.instance.clone()))
            .ok_or_else(|| Error::failed("media upload is unknown or expired"))
    }

    pub(crate) async fn begin(&self, request: MediaUploadBegin) -> Result<MediaUploadHandle> {
        let max = tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_BYTES;
        if request.instance.trim().is_empty()
            || request.scope.conversation.id.trim().is_empty()
            || request.scope.turn_id.trim().is_empty()
            || request.byte_count == 0
            || request.byte_count > max
        {
            return Err(Error::failed("media upload metadata is invalid"));
        }
        let count = request
            .byte_count
            .div_ceil(tinychannels_bus::module_media::MEDIA_UPLOAD_CHUNK_BYTES);
        if count == 0 || count > tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_CHUNKS {
            return Err(Error::failed("media upload exceeds its chunk limit"));
        }
        let mut state = self.state.lock().await;
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
                expires: Instant::now() + UPLOAD_LIFETIME,
                chunks: (0..count).map(|_| None).collect(),
                next_sequence: 0,
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
        let sequence = chunk.sequence as usize;
        if sequence >= upload.chunks.len() {
            return Err(Error::failed("media chunk sequence exceeds its limit"));
        }
        let expected_len = if sequence + 1 == upload.chunks.len() {
            upload.request.byte_count - sequence * tinychannels_bus::module_media::MEDIA_UPLOAD_CHUNK_BYTES
        } else {
            tinychannels_bus::module_media::MEDIA_UPLOAD_CHUNK_BYTES
        };
        if chunk.bytes.len() != expected_len {
            return Err(Error::failed("media chunk size does not match its sequence"));
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
        &self,
        request: MediaUploadFinish,
        repository: &ModuleMediaRepository,
    ) -> Result<MediaUploadResult> {
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        if let Some(entry) = state.terminal.get(&request.0.0) {
            return match &entry.value {
                Terminal::Finished(media) => Ok(media.clone()),
                Terminal::Aborted => Err(Error::failed("media upload was aborted")),
            };
        }
        let upload = state
            .uploads
            .get(&request.0.0)
            .ok_or_else(|| Error::failed("media upload is unknown or expired"))?;
        if upload.expires <= Instant::now() {
            state.uploads.remove(&request.0.0);
            return Err(Error::failed("media upload is unknown or expired"));
        }
        if upload.next_sequence != upload.chunks.len() {
            return Err(Error::failed("media upload is incomplete"));
        }
        let upload = state.uploads.remove(&request.0.0).expect("checked above");
        let bytes: Vec<u8> = upload.chunks.into_iter().flatten().flatten().collect();
        if bytes.len() != upload.request.byte_count {
            return Err(Error::failed("media upload byte count does not match"));
        }
        drop(state);
        let media = repository
            .store_public_media(tinychannels::host::MediaStoreRequest {
                metadata: tinychannels::host::MediaMetadata {
                    scope: upload.request.scope,
                    kind: upload.request.kind,
                    content_type: upload.request.content_type,
                    file_name: upload.request.file_name,
                    duration_ms: upload.request.duration_ms,
                    codec: upload.request.codec,
                    voice_direction: None,
                    dedupe_key: None,
                    max_bytes: tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_BYTES as u64,
                },
                bytes,
            })
            .await
            .map_err(|_| Error::failed("media could not be stored"))?;
        let mut state = self.state.lock().await;
        Self::prune(&mut state);
        state.terminal.insert(
            request.0.0,
            TerminalEntry {
                value: Terminal::Finished(media.clone()),
                instance: upload.request.instance,
                expires: Instant::now() + TERMINAL_LIFETIME,
            },
        );
        Self::trim_terminal(&mut state);
        Ok(media)
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
        let Some(upload) = state.uploads.remove(&request.0.0) else {
            return Err(Error::failed("media upload is unknown or expired"));
        };
        state.terminal.insert(
            request.0.0,
            TerminalEntry {
                value: Terminal::Aborted,
                instance: upload.request.instance,
                expires: Instant::now() + TERMINAL_LIFETIME,
            },
        );
        Self::trim_terminal(&mut state);
        Ok(())
    }

    fn prune(state: &mut State) {
        let now = Instant::now();
        state.uploads.retain(|_, upload| upload.expires > now);
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
