//! Normalized inbound channel envelopes.

use crate::channel::types::{ChannelRef, ConversationRef, SenderRef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Direct-message access verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SenderDmDecision {
    Allow,
    Pairing,
    Deny,
}

/// Group access policy that produced the inbound authorization result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GroupAccessPolicy {
    Open,
    Allowlist,
    Disabled,
}

/// Mention gate that allowed a message through without explicit addressing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MentionGate {
    ExplicitMention,
    ReplyToBot,
    BotThreadParticipant,
    NativeCommand,
    None,
}

/// Access facts carried with an inbound envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct AccessContext {
    pub dm_decision: SenderDmDecision,
    pub group_policy: GroupAccessPolicy,
    pub mention_gate: MentionGate,
    pub command_authorized: bool,
    #[serde(skip)]
    pub delivered_via_upstream_relay: bool,
}

impl Default for AccessContext {
    fn default() -> Self {
        Self {
            dm_decision: SenderDmDecision::Deny,
            group_policy: GroupAccessPolicy::Disabled,
            mention_gate: MentionGate::None,
            command_authorized: false,
            delivered_via_upstream_relay: false,
        }
    }
}

/// Provider-neutral media kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Image,
    Video,
    Audio,
    Document,
    #[default]
    Unknown,
}

/// One inbound media reference, preserving provider attachment order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct MediaReference {
    pub path: Option<String>,
    pub url: Option<String>,
    pub content_type: Option<String>,
    pub kind: MediaKind,
    pub transcribed: bool,
    pub platform_message_id: Option<String>,
    pub local_cache_path: Option<String>,
}

/// Transcript enrichment attached to a voice note without replacing its audio.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VoiceTranscript {
    pub text: String,
    pub language: Option<String>,
    pub state: VoiceTranscriptState,
    pub source: VoiceTranscriptSource,
}

/// Whether a voice transcript is still being produced or is final.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VoiceTranscriptState {
    Pending,
    Complete,
    Unavailable,
}

/// Origin of voice transcript enrichment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VoiceTranscriptSource {
    Provider,
    HostStt,
}

/// Audio message metadata and optional transcript enrichment.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct VoiceNote {
    pub media: MediaReference,
    pub codec: Option<String>,
    pub duration_ms: Option<u64>,
    pub waveform: Option<Vec<f32>>,
    pub push_to_talk: bool,
    pub transcript: Option<VoiceTranscript>,
}

/// Provider-neutral ordered inbound content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContentPart {
    Text { text: String },
    Image { media: MediaReference, caption: Option<String>, alt_text: Option<String> },
    Video { media: MediaReference, caption: Option<String>, duration_ms: Option<u64> },
    Audio { media: MediaReference, transcript: Option<String> },
    File { media: MediaReference, file_name: Option<String>, caption: Option<String> },
    VoiceNote { voice_note: VoiceNote },
    Sticker { media: MediaReference, alt_text: Option<String> },
    Location { latitude: f64, longitude: f64, label: Option<String>, address: Option<String> },
    Contact { display_name: String, phone_numbers: Vec<String>, emails: Vec<String> },
    Buttons { rows: Vec<Vec<serde_json::Value>> },
    Poll { question: String, options: Vec<String> },
    Presentation { blocks: Value },
    Native { provider: String, data: Value },
}

/// Legacy prompt-media fields projected from normalized media references.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "PascalCase")]
pub struct InboundMediaPayload {
    pub media_path: Option<String>,
    pub media_url: Option<String>,
    pub media_type: Option<String>,
    pub media_paths: Option<Vec<String>>,
    pub media_urls: Option<Vec<String>>,
    pub media_types: Option<Vec<String>>,
    pub media_transcribed_indexes: Option<Vec<usize>>,
}

/// Normalized inbound event facts consumed by hosts and harness bridges.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelInboundEnvelope {
    pub channel: ChannelRef,
    pub message_id: String,
    pub conversation: ConversationRef,
    pub sender: SenderRef,
    pub text: String,
    /// Canonical ordered content. `text` and `media` remain compatibility projections.
    pub parts: Vec<ContentPart>,
    pub access: AccessContext,
    pub media: Vec<MediaReference>,
    pub raw: Option<Value>,
}
