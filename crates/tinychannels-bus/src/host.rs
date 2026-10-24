//! Portable payloads for the host callback object.
use crate::{ChannelRef, ConversationRef};
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
    /// Base64-encoded audio bytes.
    pub audio_base64: String,
    /// MIME hint such as `audio/ogg` or `audio/mp4`.
    pub mime_type: Option<String>,
    /// Original filename hint.
    pub file_name: Option<String>,
    /// BCP-47 language hint; `None` requests automatic detection.
    pub language: Option<String>,
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

/// Scoped request for host-owned speech recognition of one channel turn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceTranscriptionRequest {
    pub scope: HostVoiceScope,
    pub audio_base64: String,
    pub mime_type: Option<String>,
    pub file_name: Option<String>,
    pub language: Option<String>,
}

/// Scoped request for host-owned speech synthesis.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostSpeechSynthesisRequest {
    pub scope: HostVoiceScope,
    pub text: String,
    pub voice: Option<String>,
    pub format: Option<String>,
}

/// Audio produced by host-owned speech synthesis.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostSpeechSynthesisResult {
    pub audio_base64: String,
    pub mime_type: String,
    pub duration_ms: Option<u64>,
}

/// Request for host policy controlling whether a channel reply is spoken.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceReplyPolicyRequest {
    pub scope: HostVoiceScope,
}

/// Configured behavior for producing spoken channel replies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HostVoiceReplyMode {
    Mirror,
    Always,
    #[default]
    Never,
    SpeakReply,
}

/// Host-authoritative facts and mode for one spoken-reply decision.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostVoiceReplyPolicy {
    pub mode: HostVoiceReplyMode,
    pub inbound_has_voice: bool,
    pub explicitly_requested: bool,
}

/// Store raw host-processed voice audio under the authenticated turn scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostStoreVoiceMediaRequest {
    pub scope: HostVoiceScope,
    pub direction: HostVoiceMediaDirection,
    pub audio_base64: String,
    pub media_reference: Option<crate::MediaReference>,
}

/// Durable reference returned after storing scoped voice audio.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct HostStoreVoiceMediaResult {
    pub media: crate::MediaReference,
    pub accessible_url: Option<String>,
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
