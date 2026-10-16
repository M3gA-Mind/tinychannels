//! Unified, configured-instance channel capability declaration.
//!
//! Capability values describe what one configured channel account can do.
//! Empty lists mean that the adapter does not advertise a restriction; a
//! missing content kind or event means it is unsupported.

use crate::channel::{ContentPart, OutboundVoice, VoiceTranscriptSource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Text length accounting used by a configured provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LengthUnit { #[default] Characters, Utf8Bytes, Utf16Units }

/// Markup dialect accepted or emitted by a provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MarkdownDialect { #[default] Plain, Markdown, Html, SlackMrkdwn, DiscordMarkdown, TelegramMarkdownV2 }

/// Support and limits for one typed content kind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ContentCapability {
    pub inbound: bool, pub outbound: bool,
    pub accepted_types: Vec<String>, pub emitted_types: Vec<String>,
    pub max_size_bytes: Option<u64>, pub max_duration_ms: Option<u64>,
    pub max_width: Option<u32>, pub max_height: Option<u32>, pub max_count: Option<u32>,
}

/// Voice note support and constraints.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct VoiceCapabilities {
    pub supported: bool, pub accepted_codecs: Vec<String>,
    pub max_duration_ms: Option<u64>, pub max_size_bytes: Option<u64>,
    pub normalized_waveform: bool, pub native_waveform: bool,
    pub transcript_sources: Vec<VoiceTranscriptSource>, pub synthesis: bool,
    pub voices: Vec<String>,
}

/// Direction-specific voice support.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct VoiceSupport {
    pub voice_in: VoiceCapabilities, pub voice_out: VoiceCapabilities,
    /// Supported OutboundVoice operation names.
    pub outbound_modes: Vec<String>,
}

/// Normalized channel events supported by an account.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct EventCapabilities { pub supported: Vec<String> }

/// Conversation types and independently addressable fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct AddressingCapabilities {
    pub conversation_types: Vec<String>, pub account_id: bool, pub scope_id: bool,
    pub parent_id: bool, pub topic_id: bool, pub reply_to_id: bool, pub thread_id: bool,
}

/// Supported operations, with provider-level restrictions kept explicit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ActionCapabilities {
    pub supported: Vec<String>, pub restrictions: BTreeMap<String, Vec<String>>,
}

/// Text and chunking limits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct TextCapabilities {
    pub text_limit: Option<u64>, pub length_unit: LengthUnit,
    pub markdown_dialects: Vec<MarkdownDialect>, pub chunk_limit: Option<u64>,
    pub chunk_overlap: Option<u64>,
}

/// Typed support for canonical ContentPart kinds and their limits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ContentCapabilities {
    pub parts: Vec<String>, pub markdown_dialects: Vec<MarkdownDialect>,
    pub image: ContentCapability, pub video: ContentCapability,
    pub audio: ContentCapability, pub file: ContentCapability,
    pub sticker: ContentCapability, pub location: ContentCapability,
    pub contact: ContentCapability, pub buttons: ContentCapability,
    pub poll: ContentCapability, pub presentation: ContentCapability,
    pub native: ContentCapability,
}

/// Final delivery guarantees offered by this configured instance.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DeliveryCapabilities {
    pub durable_final: Vec<String>, pub idempotency: bool, pub batch: bool,
    pub partial_results: bool, pub reconcile_unknown_send: bool,
}

/// Configured account and current runtime availability.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct AccountCapabilities {
    pub linked: Option<bool>, pub configured: Option<bool>, pub enabled: Option<bool>,
    pub healthy: Option<bool>, pub reconnecting: Option<bool>, pub health: Option<String>,
    pub proactive_target: Option<String>,
}

/// Canonical capability descriptor shared by module, host and relay v2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelCapabilities {
    pub version: u32,
    pub remote_control: bool, pub chat_approvals: bool, pub progressive_edits: bool,
    pub history_key_ignores_thread: bool, pub reactions: bool,
    pub content: ContentCapabilities, pub voice: VoiceSupport,
    pub events: EventCapabilities, pub addressing: AddressingCapabilities,
    pub actions: ActionCapabilities, pub text: TextCapabilities,
    pub delivery: DeliveryCapabilities, pub account: AccountCapabilities,
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
            history_key_ignores_thread: false,
            reactions: false,
            content: ContentCapabilities::default(),
            voice: VoiceSupport::default(),
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

/// Map a channel id (optionally provider-prefixed) to its provider id.
pub fn provider_id(channel: &str) -> &str { channel.split(':').next().unwrap_or(channel) }

/// Legacy policy projection retained while callers migrate to configured
/// instance Describe. New code must use Channel::capabilities.
#[deprecated(note = "query Channel::capabilities on the configured instance")]
pub fn capabilities_for(channel: &str) -> ChannelCapabilities {
    match provider_id(channel) {
        "yuanbao" => ChannelCapabilities {
            remote_control: true,
            chat_approvals: true,
            progressive_edits: true,
            version: ChannelCapabilities::VERSION,
            ..ChannelCapabilities::default()
        },
        "telegram" => ChannelCapabilities {
            remote_control: true,
            chat_approvals: true,
            progressive_edits: true,
            history_key_ignores_thread: true,
            version: ChannelCapabilities::VERSION,
            ..ChannelCapabilities::default()
        },
        "discord" | "slack" | "mattermost" | "imessage" | "signal" | "whatsapp"
        | "whatsapp_web" | "irc" | "dingtalk" | "qq" | "lark" | "linq" => {
            ChannelCapabilities { version: ChannelCapabilities::VERSION, remote_control: true,
                chat_approvals: true, ..ChannelCapabilities::default() }
        }
        _ => ChannelCapabilities::unknown(),
    }
}

// Keep compile-time references to the canonical vocabulary types.
const _: Option<ContentPart> = None;
const _: Option<OutboundVoice> = None;
const _: Option<VoiceTranscriptSource> = None;

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
