//! Durable scoped media references backed by the host's existing blob store.

use std::sync::Arc;

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

/// Blob-backed storage that validates the complete channel scope on every read.
pub struct ScopedMediaStore {
    _documents: Arc<dyn tinystoragedrivers_core::DocumentStore>,
    _blobs: Arc<dyn tinystoragedrivers_core::BlobStore>,
}

impl ScopedMediaStore {
    /// Bind the store to the module's already scoped host storage handles.
    pub fn new(
        documents: Arc<dyn tinystoragedrivers_core::DocumentStore>,
        blobs: Arc<dyn tinystoragedrivers_core::BlobStore>,
    ) -> Self {
        Self {
            _documents: documents,
            _blobs: blobs,
        }
    }

    /// Persist bounded media and return its durable opaque reference.
    pub async fn store(
        &self,
        _scope: MediaScope,
        _kind: tinychannels_bus::MediaKind,
        _content_type: String,
        _file_name: Option<String>,
        _duration_ms: Option<u64>,
        _bytes: Vec<u8>,
        _dedupe_key: Option<String>,
        _max_bytes: u64,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        anyhow::bail!("scoped media storage is not implemented")
    }

    /// Resolve a durable reference only for the scope that stored it.
    pub async fn read(
        &self,
        _scope: MediaScope,
        _opaque_id: &str,
        _max_bytes: u64,
    ) -> anyhow::Result<StoredMedia> {
        anyhow::bail!("scoped media reads are not implemented")
    }
}

#[cfg(test)]
#[path = "media_tests.rs"]
mod tests;
