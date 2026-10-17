//! Bounded host-to-module media ingestion vocabulary.
use crate::{HostVoiceScope, MediaKind, MediaReference};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maximum single media object accepted by the public upload contract.
pub const MEDIA_UPLOAD_MAX_BYTES: usize = 20 * 1024 * 1024;
/// Maximum confidential request body accepted as one media chunk.
pub const MEDIA_UPLOAD_CHUNK_BYTES: usize = 512 * 1024;
/// Maximum serialized begin metadata retained for one upload reservation.
pub const MEDIA_UPLOAD_BEGIN_MAX_BYTES: usize = 64 * 1024;
/// Maximum chunk count for one media object.
pub const MEDIA_UPLOAD_MAX_CHUNKS: usize =
    MEDIA_UPLOAD_MAX_BYTES.div_ceil(MEDIA_UPLOAD_CHUNK_BYTES);
/// Maximum simultaneously buffered media uploads in one runtime.
pub const MEDIA_UPLOAD_CAPACITY: usize = 2;
/// Maximum confidential media read response body, below TinyBus frame limits.
pub const MEDIA_READ_CHUNK_BYTES: usize = MEDIA_UPLOAD_CHUNK_BYTES;
/// Maximum full media objects retained for chunked confidential reads.
pub const MEDIA_READ_CAPACITY: usize = 2;

/// Begin a media upload on the runtime's active configured provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaUploadBegin {
    /// Active configured provider instance in this runtime.
    pub instance: String,
    /// Conversation and originating-turn facts; provider/account are replaced by the module.
    pub scope: HostVoiceScope,
    pub kind: MediaKind,
    pub content_type: String,
    pub file_name: Option<String>,
    pub duration_ms: Option<u64>,
    pub codec: Option<String>,
    /// Exact body size supplied by the caller and verified at finalize.
    pub byte_count: usize,
}

/// Opaque handle for one bounded upload.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct MediaUploadHandle(pub String);

/// One ordered and retry-safe confidential media chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaUploadChunk {
    pub handle: MediaUploadHandle,
    pub sequence: u32,
    pub bytes: Vec<u8>,
}

/// Finalize or abort an upload by its opaque handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct MediaUploadFinish(pub MediaUploadHandle);

/// Durable reference created by a successful finalize.
pub type MediaUploadResult = MediaReference;

/// Resolve one durable opaque reference in the exact configured account,
/// conversation and originating turn scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaReadBegin {
    /// Active configured provider instance in this runtime.
    pub instance: String,
    /// Full conversation and turn identity used when the media was stored.
    pub scope: HostVoiceScope,
    /// Durable opaque reference returned by media ingestion.
    pub media: MediaReference,
    /// Caller limit; the module also enforces the global 20 MiB ceiling.
    pub max_bytes: u64,
}

/// Opaque handle for one bounded in-memory resolution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct MediaReadHandle(pub String);

/// Validated metadata for a successfully resolved reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaReadInfo {
    pub handle: MediaReadHandle,
    pub media: MediaReference,
    pub content_type: String,
    pub file_name: Option<String>,
    pub byte_count: usize,
    pub chunk_count: u32,
}

/// Request one sequential chunk from a confidential read handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaReadChunk {
    pub handle: MediaReadHandle,
    pub sequence: u32,
}

/// A bounded confidential chunk. `done` marks EOF; callers should abort the
/// handle after consuming the final chunk or let its short lease expire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaReadChunkResult {
    pub sequence: u32,
    pub bytes: Vec<u8>,
    pub done: bool,
}

/// Abort a bounded read and release its temporary bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct MediaReadFinish(pub MediaReadHandle);

#[cfg(test)]
#[path = "module_media_tests.rs"]
mod tests;
