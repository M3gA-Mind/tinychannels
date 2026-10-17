//! Portable payloads for the host callback object.
use crate::channel::{ChannelRef, ConversationRef, MediaReference};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Request speech recognition through the host's configured voice provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct HostTranscriptionRequest {
    /// Base64-encoded audio bytes.
    pub audio_base64: String,
    /// MIME hint such as `audio/ogg` or `audio/mp4`.
    pub mime_type: Option<String>,
    /// Original filename hint.
    pub file_name: Option<String>,
    /// BCP-47 language hint; `None` requests automatic detection.
    pub language: Option<String>,
}

/// Scoped STT callback used by the shared voice pipeline. The module fills the
/// configured instance from its runtime instead of accepting it from a caller.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceTranscriptionRequest {
    pub scope: HostVoiceScope,
    pub audio_base64: String,
    pub mime_type: Option<String>,
    pub file_name: Option<String>,
    pub language: Option<String>,
}

/// Module-authenticated scope for a host voice operation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceScope {
    /// Configured channel instance selected by the module at runtime start.
    pub configured_instance: String,
    /// Full provider account address, overwritten by the module.
    pub channel: ChannelRef,
    /// Full conversation address, including thread and topic.
    pub conversation: ConversationRef,
    /// Stable identity of the originating turn or inbound message.
    pub turn_id: String,
}

/// Speech recognition result returned by the host.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct HostTranscriptionResult {
    /// Recognized text.
    pub text: String,
    /// Detected or requested BCP-47 language.
    pub language: Option<String>,
    /// Audio duration, when known.
    pub duration_secs: Option<f64>,
}

/// Request speech synthesis through the host's configured voice provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostSpeechSynthesisRequest {
    pub scope: HostVoiceScope,
    pub text: String,
    pub voice: Option<String>,
    pub format: Option<String>,
}

/// Synthesized speech bytes returned by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostSpeechSynthesisResult {
    pub audio_base64: String,
    pub mime_type: String,
    pub duration_ms: Option<u64>,
}

/// Direction of media being persisted by the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HostVoiceMediaDirection {
    Inbound,
    Outbound,
}

/// Request durable host storage for voice bytes before the module forwards a
/// normalized envelope or a public media link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostStoreVoiceMediaRequest {
    pub scope: HostVoiceScope,
    pub direction: HostVoiceMediaDirection,
    pub audio_base64: String,
    pub mime_type: String,
    pub codec: Option<String>,
    pub duration_ms: Option<u64>,
    pub file_name: Option<String>,
}

/// Opaque durable media reference authorized by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostStoreVoiceMediaResult {
    pub media: MediaReference,
}

/// An event published by a provider through the host's event sink.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HostEventPublication {
    /// Event domain, for example `channel` or `web`.
    pub domain: String,
    /// Event kind within the domain.
    pub kind: String,
    /// Domain-specific JSON payload.
    pub payload: serde_json::Value,
}
