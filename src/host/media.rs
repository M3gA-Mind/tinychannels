//! Durable scoped media references backed by the host's existing blob store.

use std::{
    io::Cursor,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{BlobStore, CollectionSpec, DocumentStore, Precondition};
use tokio::io::AsyncReadExt;

const MEDIA_COLLECTION: &str = "channels_media_metadata";
const MEDIA_KEY_PREFIX: &str = "channels/media/";

/// The identity required to store or resolve media for one channel turn.
pub type MediaScope = tinychannels_bus::HostVoiceScope;

/// A stored media reference and its bounded bytes when read for native upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMedia {
    pub media: tinychannels_bus::MediaReference,
    pub content_type: String,
    pub file_name: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MediaRecord {
    scope: MediaScope,
    media: tinychannels_bus::MediaReference,
    content_type: String,
    file_name: Option<String>,
    digest: String,
    created_at_unix_ms: u64,
    state: MediaRecordState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MediaRecordState {
    Writing,
    Ready,
    Deleting,
}

/// Blob-backed storage that validates the complete channel scope on every read.
pub struct ScopedMediaStore {
    documents: Arc<dyn DocumentStore>,
    blobs: Arc<dyn BlobStore>,
    writes: tokio::sync::Mutex<()>,
}

impl ScopedMediaStore {
    /// Bind the store to the module's already scoped host storage handles.
    pub fn new(documents: Arc<dyn DocumentStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            documents,
            blobs,
            writes: tokio::sync::Mutex::new(()),
        }
    }

    /// Persist bounded media and return its durable opaque reference.
    pub async fn store(
        &self,
        scope: MediaScope,
        kind: tinychannels_bus::MediaKind,
        content_type: String,
        file_name: Option<String>,
        duration_ms: Option<u64>,
        bytes: Vec<u8>,
        dedupe_key: Option<String>,
        max_bytes: u64,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        validate_scope(&scope)?;
        if bytes.is_empty() || bytes.len() as u64 > max_bytes {
            anyhow::bail!("media exceeds the configured size limit");
        }
        let content_type = normalized_content_type(&content_type);
        validate_content_type(kind, &content_type, &bytes)?;
        self.documents
            .ensure_collection(&CollectionSpec::new(MEDIA_COLLECTION))
            .await
            .map_err(|_| anyhow::anyhow!("media metadata storage is unavailable"))?;

        let digest = digest(&bytes);
        let opaque_id = match dedupe_key.as_deref() {
            Some(key) if !key.trim().is_empty() => scoped_id(&scope, key)?,
            _ => uuid::Uuid::new_v4().to_string(),
        };
        let key = blob_key(&opaque_id)?;
        let file_name = file_name.and_then(safe_file_name);
        let reference = tinychannels_bus::MediaReference {
            opaque_id: Some(opaque_id.clone()),
            content_type: Some(content_type.clone()),
            file_name: file_name.clone(),
            size_bytes: Some(bytes.len() as u64),
            duration_ms,
            kind,
            ..Default::default()
        };
        let record = MediaRecord {
            scope,
            media: reference.clone(),
            content_type: content_type.clone(),
            file_name,
            digest: digest.clone(),
            created_at_unix_ms: now_unix_ms(),
            state: MediaRecordState::Writing,
        };

        let _write = self.writes.lock().await;
        if let Some((existing, _)) = self.record(&opaque_id).await? {
            if existing.state == MediaRecordState::Ready
                && existing.scope == record.scope
                && existing.digest == digest
            {
                return Ok(existing.media);
            }
            anyhow::bail!("media deduplication key conflicts with stored media");
        }

        // Reserve the metadata id before writing the blob. The bus storage
        // grant is module-wide; this record also binds the id to the complete
        // authenticated account/conversation/turn scope.
        let document = serde_json::to_value(&record)?;
        let reservation_version = self
            .documents
            .put(MEDIA_COLLECTION, &opaque_id, document, Precondition::Absent)
            .await
            .map_err(|_| anyhow::anyhow!("media metadata reservation failed"))?;

        if self
            .blobs
            .put_stream(
                &key,
                Box::pin(Cursor::new(bytes)),
                record.media.size_bytes.unwrap_or_default(),
                Some(&content_type),
            )
            .await
            .is_err()
        {
            let _ = self
                .documents
                .delete(
                    MEDIA_COLLECTION,
                    &opaque_id,
                    Precondition::Version(reservation_version),
                )
                .await;
            anyhow::bail!("media blob could not be stored");
        }
        let mut ready = record;
        ready.state = MediaRecordState::Ready;
        if self
            .documents
            .put(
                MEDIA_COLLECTION,
                &opaque_id,
                serde_json::to_value(&ready)?,
                Precondition::Version(reservation_version),
            )
            .await
            .is_err()
        {
            // Leave the reservation and blob for explicit recovery. Deleting
            // the blob here could erase bytes committed by another process.
            anyhow::bail!("media metadata could not be finalized");
        }
        Ok(reference)
    }

    /// Resolve a durable reference only for the scope that stored it.
    pub async fn read(
        &self,
        scope: MediaScope,
        opaque_id: &str,
        max_bytes: u64,
    ) -> anyhow::Result<StoredMedia> {
        validate_scope(&scope)?;
        let (record, _) = self
            .record(opaque_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("media reference is unavailable"))?;
        if record.scope != scope || record.state != MediaRecordState::Ready {
            anyhow::bail!("media reference is outside the configured channel scope");
        }
        let key = blob_key(opaque_id)?;
        let mut stream = self
            .blobs
            .get_stream(&key)
            .await
            .map_err(|_| anyhow::anyhow!("media blob is unavailable"))?
            .ok_or_else(|| anyhow::anyhow!("media blob is unavailable"))?;
        if stream.meta.len == 0 || stream.meta.len > max_bytes {
            anyhow::bail!("media exceeds the configured read limit");
        }
        let mut bytes = Vec::with_capacity(stream.meta.len as usize);
        (&mut stream.reader)
            .take(max_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| anyhow::anyhow!("media blob could not be read"))?;
        if bytes.len() as u64 != stream.meta.len
            || bytes.len() as u64 > max_bytes
            || digest(&bytes) != record.digest
        {
            anyhow::bail!("stored media failed integrity or size validation");
        }
        Ok(StoredMedia {
            media: record.media,
            content_type: record.content_type,
            file_name: record.file_name,
            bytes,
        })
    }

    /// Delete one media object after validating its complete stored scope.
    pub async fn delete(&self, scope: MediaScope, opaque_id: &str) -> anyhow::Result<bool> {
        validate_scope(&scope)?;
        let Some((mut record, version)) = self.record(opaque_id).await? else {
            return Ok(false);
        };
        if record.scope != scope {
            anyhow::bail!("media reference is outside the configured channel scope");
        }
        let tombstone_version = if record.state == MediaRecordState::Deleting {
            version
        } else {
            record.state = MediaRecordState::Deleting;
            self.documents
                .put(
                    MEDIA_COLLECTION,
                    opaque_id,
                    serde_json::to_value(&record)?,
                    Precondition::Version(version),
                )
                .await
                .map_err(|_| anyhow::anyhow!("media cleanup reservation failed"))?
        };
        self.blobs
            .delete(&blob_key(opaque_id)?)
            .await
            .map_err(|_| anyhow::anyhow!("media blob cleanup is pending"))?;
        self.documents
            .delete(
                MEDIA_COLLECTION,
                opaque_id,
                Precondition::Version(tombstone_version),
            )
            .await
            .map_err(|_| anyhow::anyhow!("media cleanup metadata is pending"))?;
        Ok(true)
    }

    async fn record(
        &self,
        opaque_id: &str,
    ) -> anyhow::Result<Option<(MediaRecord, tinystoragedrivers_core::Version)>> {
        blob_key(opaque_id)?;
        let Some(document) = self
            .documents
            .get(MEDIA_COLLECTION, opaque_id)
            .await
            .map_err(|_| anyhow::anyhow!("media metadata is unavailable"))?
        else {
            return Ok(None);
        };
        let record = serde_json::from_value(document.doc)
            .map_err(|_| anyhow::anyhow!("media metadata is invalid"))?;
        Ok(Some((record, document.version)))
    }
}

fn validate_scope(scope: &MediaScope) -> anyhow::Result<()> {
    if scope.configured_instance.trim().is_empty()
        || scope.channel.id.trim().is_empty()
        || scope.channel.account_id.as_deref().is_none_or(str::is_empty)
        || scope.conversation.id.trim().is_empty()
        || scope.turn_id.trim().is_empty()
    {
        anyhow::bail!("media storage requires configured account, conversation, and turn scope");
    }
    Ok(())
}

fn scoped_id(scope: &MediaScope, dedupe_key: &str) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(scope)?;
    let mut hash = Sha256::new();
    hash.update(b"tinychannels-media-v1\0");
    hash.update((encoded.len() as u64).to_le_bytes());
    hash.update(encoded);
    hash.update((dedupe_key.len() as u64).to_le_bytes());
    hash.update(dedupe_key.as_bytes());
    Ok(hex::encode(hash.finalize()))
}

fn blob_key(opaque_id: &str) -> anyhow::Result<String> {
    if !(opaque_id.len() == 36 || opaque_id.len() == 64)
        || !opaque_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        anyhow::bail!("media reference is invalid");
    }
    Ok(format!("{MEDIA_KEY_PREFIX}{opaque_id}"))
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn safe_file_name(name: String) -> Option<String> {
    let name = name.trim();
    if name.is_empty()
        || name.len() > 255
        || name.contains(['/', '\\', '\0'])
        || name == "."
        || name == ".."
    {
        None
    } else {
        Some(name.to_owned())
    }
}

fn normalized_content_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn validate_content_type(
    kind: tinychannels_bus::MediaKind,
    content_type: &str,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let allowed = match kind {
        tinychannels_bus::MediaKind::Image => {
            matches!(content_type, "image/jpeg" | "image/png" | "image/gif" | "image/webp")
        }
        tinychannels_bus::MediaKind::Video => {
            matches!(content_type, "video/mp4" | "video/quicktime" | "video/webm")
        }
        tinychannels_bus::MediaKind::Audio => matches!(
            content_type,
            "audio/ogg" | "audio/mpeg" | "audio/mp4" | "audio/wav" | "audio/webm"
        ),
        tinychannels_bus::MediaKind::Document => matches!(
            content_type,
            "application/pdf"
                | "text/plain"
                | "text/csv"
                | "application/json"
                | "application/zip"
                | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        tinychannels_bus::MediaKind::Unknown => false,
    };
    if !allowed || !content_matches(content_type, bytes) {
        anyhow::bail!("media content type or body is not allowed");
    }
    Ok(())
}

fn content_matches(content_type: &str, bytes: &[u8]) -> bool {
    match content_type {
        "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
        "image/gif" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "image/webp" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"),
        "video/mp4" | "video/quicktime" => bytes.get(4..8) == Some(b"ftyp"),
        "video/webm" | "audio/webm" => bytes.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]),
        "audio/ogg" => bytes.starts_with(b"OggS"),
        "audio/mpeg" => bytes.starts_with(b"ID3") || bytes.starts_with(&[0xff, 0xfb]),
        "audio/mp4" => bytes.get(4..8) == Some(b"ftyp"),
        "audio/wav" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WAVE"),
        "application/pdf" => bytes.starts_with(b"%PDF-"),
        "application/zip"
        | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => {
            bytes.starts_with(b"PK\x03\x04")
        }
        "text/plain" | "text/csv" | "application/json" => {
            std::str::from_utf8(bytes).is_ok() && !bytes.contains(&0)
        }
        _ => false,
    }
}

#[cfg(test)]
#[path = "media_tests.rs"]
mod tests;
