//! Provider interaction vocabulary for running module-owned channels.
use crate::{ChannelCapabilities, LegacyChannelCapabilities, SendMessage};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An interaction on one running provider. The module orders these with sends.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ChannelInteractionRequest {
    pub channel: String,
    pub interaction: ChannelInteraction,
}

/// Provider operations used while composing a response.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum ChannelInteraction {
    Describe,
    /// Return the canonical v2 descriptor. `Describe` keeps its v1 payload
    /// during the compatibility window.
    DescribeV2,
    StartTyping {
        recipient: String,
    },
    /// Refresh an active typing indicator inside the provider owner. The
    /// initial indicator is started by `StartTyping`; this keeps subsequent
    /// provider calls and their cancellation under the module lifecycle.
    StartTypingRefresh {
        recipient: String,
        interval_secs: u64,
    },
    StopTyping {
        recipient: String,
    },
    /// Stop and join the module-owned refresh task before acknowledging.
    StopTypingRefresh {
        handle: String,
    },
    SendDraft {
        message: SendMessage,
    },
    UpdateDraft {
        recipient: String,
        message_id: String,
        text: String,
    },
    FinalizeDraft {
        recipient: String,
        message_id: String,
        text: String,
        thread_ts: Option<String>,
    },
}

/// Frozen v21 interaction information returned by the existing `Describe`
/// operation. New consumers should use `DescribeV2`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ChannelInteractionInfo {
    pub capabilities: LegacyChannelCapabilities,
    pub proactive_target: Option<String>,
    pub supports_reactions: bool,
    pub supports_draft_updates: bool,
}

/// Canonical configured-instance information returned by `DescribeV2` and
/// configured-channel catalog queries.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ChannelInteractionInfoV2 {
    pub capabilities: ChannelCapabilities,
}

/// Result of a provider interaction.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ChannelInteractionResult {
    Completed,
    /// A typed provider send completed with its actual provider receipt.
    Sent(crate::channel::MessageReceipt),
    Draft(Option<String>),
    Info(Box<ChannelInteractionInfo>),
    InfoV2(Box<ChannelInteractionInfoV2>),
    TypingRefresh { handle: String },
}

#[cfg(test)]
#[path = "module_interaction_tests.rs"]
mod tests;
