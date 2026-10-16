//! Channel capability surfaces.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub use crate::capabilities::{LengthUnit, MarkdownDialect};

/// Static provider feature flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelStaticCapabilities {
    pub chat_types: Vec<String>,
    pub polls: bool,
    pub reactions: bool,
    pub edit: bool,
    pub unsend: bool,
    pub reply: bool,
    pub threads: bool,
    pub media: bool,
    pub native_commands: bool,
    pub block_streaming: bool,
}

/// Presentation and text limits advertised by a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelPresentationCapabilities {
    pub text_limit: Option<usize>,
    #[serde(with = "legacy_length_unit")]
    #[schemars(with = "legacy_length_unit::LegacyLengthUnit")]
    pub length_unit: LengthUnit,
    #[serde(with = "legacy_markdown_dialect")]
    #[schemars(with = "legacy_markdown_dialect::LegacyMarkdownDialect")]
    pub markdown_dialect: MarkdownDialect,
    pub supports_edit: bool,
    pub max_actions: Option<usize>,
    pub max_actions_per_row: Option<usize>,
    pub max_rows: Option<usize>,
    pub max_label_length: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub supports_styles: bool,
    pub supports_disabled: bool,
    pub supports_layout_hints: bool,
}

mod legacy_length_unit {
    use super::LengthUnit;
    use schemars::JsonSchema;
    use serde::{Deserialize, Deserializer, Serializer};

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    enum LegacyLengthUnit {
        #[schemars(rename = "chars")]
        Characters,
        #[schemars(rename = "utf8-bytes")]
        Utf8Bytes,
        #[schemars(rename = "utf16-units")]
        Utf16Units,
    }

    pub fn serialize<S>(value: &LengthUnit, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match value {
            LengthUnit::Characters => "chars",
            LengthUnit::Utf8Bytes => "utf8-bytes",
            LengthUnit::Utf16Units => "utf16-units",
        })
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<LengthUnit, D::Error>
    where
        D: Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "chars" | "characters" => Ok(LengthUnit::Characters),
            "utf8-bytes" | "utf8_bytes" => Ok(LengthUnit::Utf8Bytes),
            "utf16-units" | "utf16_units" => Ok(LengthUnit::Utf16Units),
            value => Err(serde::de::Error::custom(format!(
                "unknown legacy length unit {value}"
            ))),
        }
    }
}

mod legacy_markdown_dialect {
    use super::MarkdownDialect;
    use schemars::JsonSchema;
    use serde::{Deserialize, Deserializer, Serializer};

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    #[schemars(rename_all = "snake_case")]
    enum LegacyMarkdownDialect {
        Plain,
        Markdown,
        Html,
        #[schemars(rename = "slack-mrkdwn")]
        SlackMrkdwn,
        #[schemars(rename = "discord-markdown")]
        DiscordMarkdown,
        #[schemars(rename = "telegram-markdown-v2")]
        TelegramMarkdownV2,
    }

    pub fn serialize<S>(value: &MarkdownDialect, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match value {
            MarkdownDialect::Plain => "plain",
            MarkdownDialect::Markdown => "markdown",
            MarkdownDialect::Html => "html",
            MarkdownDialect::SlackMrkdwn => "slack-mrkdwn",
            MarkdownDialect::DiscordMarkdown => "discord-markdown",
            MarkdownDialect::TelegramMarkdownV2 => "telegram-markdown-v2",
        })
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<MarkdownDialect, D::Error>
    where
        D: Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "plain" => Ok(MarkdownDialect::Plain),
            "markdown" => Ok(MarkdownDialect::Markdown),
            "html" => Ok(MarkdownDialect::Html),
            "slack-mrkdwn" | "slack_mrkdwn" => Ok(MarkdownDialect::SlackMrkdwn),
            "discord-markdown" | "discord_markdown" => Ok(MarkdownDialect::DiscordMarkdown),
            "telegram-markdown-v2" | "telegram_markdown_v2" => {
                Ok(MarkdownDialect::TelegramMarkdownV2)
            }
            value => Err(serde::de::Error::custom(format!(
                "unknown legacy markdown dialect {value}"
            ))),
        }
    }
}

impl Default for ChannelPresentationCapabilities {
    fn default() -> Self {
        Self {
            text_limit: None,
            length_unit: LengthUnit::Characters,
            markdown_dialect: MarkdownDialect::Plain,
            supports_edit: false,
            max_actions: None,
            max_actions_per_row: None,
            max_rows: None,
            max_label_length: None,
            max_value_bytes: None,
            supports_styles: false,
            supports_disabled: false,
            supports_layout_hints: false,
        }
    }
}

/// Mechanical v1 projection of canonical v2 capabilities for legacy adapters.
impl From<&crate::ChannelCapabilities> for ChannelStaticCapabilities {
    fn from(capabilities: &crate::ChannelCapabilities) -> Self {
        let has_action = |name: &str| {
            capabilities
                .actions
                .supported
                .iter()
                .any(|item| item == name)
        };
        Self {
            chat_types: capabilities.addressing.conversation_types.clone(),
            polls: capabilities.content.poll.outbound,
            reactions: capabilities.reactions,
            edit: has_action("edit"),
            unsend: has_action("delete") || has_action("unsend"),
            reply: has_action("reply"),
            threads: capabilities.addressing.thread_id,
            media: capabilities.content.image.outbound
                || capabilities.content.video.outbound
                || capabilities.content.audio.outbound
                || capabilities.content.file.outbound,
            native_commands: has_action("native-command"),
            block_streaming: capabilities.progressive_edits,
        }
    }
}

/// Mechanical v1 projection of canonical v2 presentation facts.
impl From<&crate::ChannelCapabilities> for ChannelPresentationCapabilities {
    fn from(capabilities: &crate::ChannelCapabilities) -> Self {
        Self {
            text_limit: capabilities
                .text
                .text_limit
                .and_then(|limit| usize::try_from(limit).ok()),
            length_unit: capabilities.text.length_unit,
            markdown_dialect: capabilities
                .text
                .markdown_dialects
                .first()
                .copied()
                .unwrap_or_default(),
            supports_edit: capabilities
                .actions
                .supported
                .iter()
                .any(|action| action == "edit"),
            max_actions: capabilities
                .content
                .buttons
                .max_count
                .and_then(|count| usize::try_from(count).ok()),
            max_actions_per_row: capabilities
                .content
                .button_presentation
                .max_actions_per_row
                .and_then(|n| usize::try_from(n).ok()),
            max_rows: capabilities
                .content
                .button_presentation
                .max_rows
                .and_then(|n| usize::try_from(n).ok()),
            max_label_length: capabilities
                .content
                .button_presentation
                .max_label_length
                .and_then(|n| usize::try_from(n).ok()),
            max_value_bytes: capabilities
                .content
                .button_presentation
                .max_value_bytes
                .and_then(|n| usize::try_from(n).ok()),
            supports_styles: capabilities.content.button_presentation.supports_styles,
            supports_disabled: capabilities.content.button_presentation.supports_disabled,
            supports_layout_hints: capabilities
                .content
                .button_presentation
                .supports_layout_hints,
        }
    }
}

/// Durable-final delivery capability key understood by message adapters.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum DurableFinalDeliveryCapability {
    Text,
    Media,
    Poll,
    Payload,
    Silent,
    ReplyTo,
    Thread,
    NativeQuote,
    MessageSendingHooks,
    Batch,
    ReconcileUnknownSend,
    AfterSendSuccess,
    AfterCommit,
}

/// Capability map used by adapters to declare durable final-delivery support.
pub type DurableFinalDeliveryRequirementMap = BTreeMap<DurableFinalDeliveryCapability, bool>;

/// OpenClaw's stable durable-final capability key list, in contract order.
pub fn durable_final_delivery_capabilities() -> &'static [DurableFinalDeliveryCapability] {
    use DurableFinalDeliveryCapability::*;
    &[
        Text,
        Media,
        Poll,
        Payload,
        Silent,
        ReplyTo,
        Thread,
        NativeQuote,
        MessageSendingHooks,
        Batch,
        ReconcileUnknownSend,
        AfterSendSuccess,
        AfterCommit,
    ]
}

/// Canonical OpenClaw message action names, preserving upstream order.
pub const CHANNEL_MESSAGE_ACTION_NAMES: &[&str] = &[
    "send",
    "broadcast",
    "poll",
    "poll-vote",
    "react",
    "reactions",
    "read",
    "edit",
    "unsend",
    "reply",
    "sendWithEffect",
    "renameGroup",
    "setGroupIcon",
    "addParticipant",
    "removeParticipant",
    "leaveGroup",
    "sendAttachment",
    "delete",
    "pin",
    "unpin",
    "list-pins",
    "permissions",
    "thread-create",
    "thread-list",
    "thread-reply",
    "search",
    "sticker",
    "sticker-search",
    "member-info",
    "role-info",
    "emoji-list",
    "emoji-upload",
    "sticker-upload",
    "role-add",
    "role-remove",
    "channel-info",
    "channel-list",
    "channel-create",
    "channel-edit",
    "channel-delete",
    "channel-move",
    "category-create",
    "category-edit",
    "category-delete",
    "topic-create",
    "topic-edit",
    "voice-status",
    "event-list",
    "event-create",
    "timeout",
    "kick",
    "ban",
    "set-profile",
    "set-presence",
    "download-file",
    "upload-file",
];

pub fn channel_message_action_names() -> &'static [&'static str] {
    CHANNEL_MESSAGE_ACTION_NAMES
}
