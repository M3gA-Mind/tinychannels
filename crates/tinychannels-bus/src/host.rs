//! Portable payloads for the host callback object.
use crate::{ChannelRef, ConversationRef, MediaReference};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authenticated scope for media operations owned by one configured channel.
/// The module fills provider identity from its active runtime; callers must
/// supply the conversation and originating turn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceScope {
    pub configured_instance: String,
    pub channel: ChannelRef,
    pub conversation: ConversationRef,
    pub turn_id: String,
}

/// One-release compatibility result for legacy voice stores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostStoreVoiceMediaResult {
    pub media: MediaReference,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accessible_url: Option<String>,
}

/// One-release compatibility result for legacy voice reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostReadVoiceMediaResult {
    pub audio_base64: String,
    pub mime_type: String,
    pub file_name: Option<String>,
}

/// Whether stored channel media arrived from a provider or was prepared for
/// outbound delivery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HostVoiceMediaDirection {
    #[default]
    Inbound,
    Outbound,
}

/// Request speech recognition through the host's configured voice provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct HostTranscriptionRequest {
    /// Authenticated provider, conversation and turn scope for the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<HostVoiceScope>,
    /// Base64-encoded audio bytes.
    pub audio_base64: String,
    /// MIME hint such as `audio/ogg` or `audio/mp4`.
    pub mime_type: Option<String>,
    /// Original filename hint.
    pub file_name: Option<String>,
    /// BCP-47 language hint; `None` requests automatic detection.
    pub language: Option<String>,
}

/// Scoped speech recognition request for module to host calls.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceTranscriptionRequest {
    pub scope: HostVoiceScope,
    pub audio_base64: String,
    pub mime_type: Option<String>,
    pub file_name: Option<String>,
    pub language: Option<String>,
}

/// Scoped text-to-speech request for module to host calls.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostSpeechSynthesisRequest {
    pub scope: HostVoiceScope,
    pub text: String,
    pub voice: Option<String>,
    pub format: Option<String>,
}

/// Synthesized audio returned by a scoped host callback.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostSpeechSynthesisResult {
    pub audio_base64: String,
    pub mime_type: String,
    pub duration_ms: Option<u64>,
    pub visemes: Option<serde_json::Value>,
}

/// Scoped request to persist provider audio through the host.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostStoreVoiceMediaRequest {
    pub scope: HostVoiceScope,
    pub audio_base64: String,
    pub mime_type: String,
    pub file_name: Option<String>,
    pub direction: HostVoiceMediaDirection,
}

/// Scoped request to read previously stored voice bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostReadVoiceMediaRequest {
    pub scope: HostVoiceScope,
    pub media: MediaReference,
    pub max_bytes: u64,
}

/// Host-selected policy for whether the current turn should have a spoken reply.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HostVoiceReplyMode {
    #[default]
    Mirror,
    Always,
    Never,
    SpeakReply,
}

/// Scoped request for host voice reply policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceReplyPolicyRequest {
    pub scope: HostVoiceScope,
    pub inbound_has_voice: bool,
    pub explicitly_requested: bool,
}

/// Voice reply policy returned by the host.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceReplyPolicy {
    pub mode: HostVoiceReplyMode,
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

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
