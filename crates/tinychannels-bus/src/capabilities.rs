//! Unified, configured-instance channel capability declaration.
//!
//! Capability values describe what one configured channel account can do.
//! Empty lists mean that the adapter does not advertise a restriction; a
//! missing content kind or event means it is unsupported.

use crate::channel::VoiceTranscriptSource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Text length accounting used by a configured provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LengthUnit {
    #[default]
    Characters,
    Utf8Bytes,
    Utf16Units,
}

/// Markup dialect accepted or emitted by a provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MarkdownDialect {
    #[default]
    Plain,
    Markdown,
    Html,
    SlackMrkdwn,
    DiscordMarkdown,
    TelegramMarkdownV2,
}

/// Support and limits for one typed content kind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ContentCapability {
    pub inbound: bool,
    pub outbound: bool,
    pub accepted_types: Vec<String>,
    pub emitted_types: Vec<String>,
    pub max_size_bytes: Option<u64>,
    pub max_duration_ms: Option<u64>,
    pub max_width: Option<u32>,
    pub max_height: Option<u32>,
    pub max_count: Option<u32>,
}

/// Voice note support and constraints.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct VoiceCapabilities {
    pub supported: bool,
    pub accepted_codecs: Vec<String>,
    pub max_duration_ms: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub normalized_waveform: bool,
    pub native_waveform: bool,
    pub transcript_sources: Vec<VoiceTranscriptSource>,
    pub synthesis: bool,
    pub voices: Vec<String>,
}

/// Normalized channel events supported by an account.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct EventCapabilities {
    pub supported: Vec<String>,
}

/// Conversation types and independently addressable fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct AddressingCapabilities {
    pub conversation_types: Vec<String>,
    pub account_id: bool,
    pub scope_id: bool,
    pub parent_id: bool,
    pub topic_id: bool,
    pub reply_to_id: bool,
    pub thread_id: bool,
}

/// Supported operations, with provider-level restrictions kept explicit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ActionCapabilities {
    pub supported: Vec<String>,
    pub restrictions: BTreeMap<String, Vec<String>>,
}

/// Text and chunking limits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct TextCapabilities {
    pub text_limit: Option<u64>,
    pub length_unit: LengthUnit,
    pub markdown_dialects: Vec<MarkdownDialect>,
    pub chunk_limit: Option<u64>,
    pub chunk_overlap: Option<u64>,
}

/// Typed support for canonical ContentPart kinds and their limits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ContentCapabilities {
    pub parts: Vec<String>,
    pub text: ContentCapability,
    pub image: ContentCapability,
    pub video: ContentCapability,
    pub audio: ContentCapability,
    pub file: ContentCapability,
    pub sticker: ContentCapability,
    pub location: ContentCapability,
    pub contact: ContentCapability,
    pub buttons: ContentCapability,
    pub poll: ContentCapability,
    pub presentation: ContentCapability,
    pub native: ContentCapability,
    pub button_presentation: ButtonPresentationCapabilities,
}

/// Rich button rendering constraints formerly carried by the presentation DTO.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ButtonPresentationCapabilities {
    pub max_actions_per_row: Option<u32>,
    pub max_rows: Option<u32>,
    pub max_label_length: Option<u32>,
    pub max_value_bytes: Option<u32>,
    pub supports_styles: bool,
    pub supports_disabled: bool,
    pub supports_layout_hints: bool,
}

/// Final delivery guarantees offered by this configured instance.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DeliveryCapabilities {
    pub durable_final: Vec<String>,
    pub idempotency: bool,
    pub batch: bool,
    pub partial_results: bool,
    pub reconcile_unknown_send: bool,
}

/// Configured account and current runtime availability.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct AccountCapabilities {
    pub linked: Option<bool>,
    pub configured: Option<bool>,
    pub enabled: Option<bool>,
    pub healthy: Option<bool>,
    pub reconnecting: Option<bool>,
    pub health: Option<String>,
    pub proactive_target: Option<String>,
}

/// Canonical capability descriptor shared by module, host and relay v2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelCapabilities {
    pub version: u32,
    pub remote_control: bool,
    pub chat_approvals: bool,
    pub progressive_edits: bool,
    /// The provider requires the streaming reply parser, independent of whether
    /// it can display incremental edits to the user.
    pub streaming_reply_pipeline: bool,
    pub history_key_ignores_thread: bool,
    pub reactions: bool,
    pub content: ContentCapabilities,
    pub voice_in: VoiceCapabilities,
    pub voice_out: VoiceCapabilities,
    pub events: EventCapabilities,
    pub addressing: AddressingCapabilities,
    pub actions: ActionCapabilities,
    pub text: TextCapabilities,
    pub delivery: DeliveryCapabilities,
    pub account: AccountCapabilities,
    pub extensions: BTreeMap<String, serde_json::Value>,
}

impl ChannelCapabilities {
    pub const VERSION: u32 = 2;

    /// Conservative facts for an unconfigured or unknown channel instance.
    pub fn unknown() -> Self {
        Self::default()
    }
}

impl Default for ChannelCapabilities {
    fn default() -> Self {
        Self {
            version: Self::VERSION,
            remote_control: false,
            chat_approvals: false,
            progressive_edits: false,
            streaming_reply_pipeline: false,
            history_key_ignores_thread: false,
            reactions: false,
            content: ContentCapabilities::default(),
            voice_in: VoiceCapabilities::default(),
            voice_out: VoiceCapabilities::default(),
            events: EventCapabilities::default(),
            addressing: AddressingCapabilities::default(),
            actions: ActionCapabilities::default(),
            text: TextCapabilities::default(),
            delivery: DeliveryCapabilities::default(),
            account: AccountCapabilities::default(),
            extensions: BTreeMap::new(),
        }
    }
}

/// Frozen pre-v2 four-flag payload used by the existing `Describe` and
/// `Capabilities` operations. New consumers should request `ChannelCapabilities`
/// through the additive v2 operations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LegacyChannelCapabilities {
    pub remote_control: bool,
    pub chat_approvals: bool,
    pub progressive_edits: bool,
    pub history_key_ignores_thread: bool,
}

impl From<&ChannelCapabilities> for LegacyChannelCapabilities {
    fn from(capabilities: &ChannelCapabilities) -> Self {
        Self {
            remote_control: capabilities.remote_control,
            chat_approvals: capabilities.chat_approvals,
            progressive_edits: capabilities.progressive_edits,
            history_key_ignores_thread: capabilities.history_key_ignores_thread,
        }
    }
}

/// Map a channel id (optionally provider-prefixed) to its provider id.
pub fn provider_id(channel: &str) -> &str {
    match channel.split(':').next().unwrap_or(channel) {
        "tg" => "telegram",
        provider => provider,
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
