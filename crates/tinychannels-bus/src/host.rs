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
    /// Durable opaque reference used in history and normalized envelopes.
    pub media: MediaReference,
    /// Optional host-authorized URL that another peer can actually open.
    /// Omit it unless link access is intentionally granted for this media.
    pub accessible_url: Option<String>,
}

/// Resolve authorized stored bytes for a native provider upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostReadVoiceMediaRequest {
    pub scope: HostVoiceScope,
    pub opaque_id: String,
}

/// Bounded media returned only for the requested conversation and turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostReadVoiceMediaResult {
    pub audio_base64: String,
    pub mime_type: String,
    pub file_name: Option<String>,
}

/// Host-selected speech policy and verified facts for one active turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HostVoiceReplyMode {
    Mirror,
    Always,
    Never,
    SpeakReply,
}

/// Resolve voice reply policy from the host's authoritative turn state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostVoiceReplyPolicyRequest {
    pub scope: HostVoiceScope,
}

/// Host-authoritative policy and turn facts; codec constraints come from the
/// provider's advertised voice capabilities and are applied by the module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostVoiceReplyPolicy {
    pub mode: HostVoiceReplyMode,
    pub inbound_has_voice: bool,
    pub explicitly_requested: bool,
    pub accepted_codecs: Vec<String>,
    pub max_size_bytes: Option<u64>,
    pub max_duration_ms: Option<u64>,
    pub normalized_waveform: bool,
}

/// Host reply-policy callback name.
pub const GET_VOICE_REPLY_POLICY: &str = "GetVoiceReplyPolicy";

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
