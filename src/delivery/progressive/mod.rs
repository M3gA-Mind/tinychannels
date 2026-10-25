//! Progressive reply delivery over a message-relay backend.
//!
//! While an agent turn runs, a chat user should see activity: a typing
//! indicator, an evolving draft of the reply, an ephemeral "💭 Thinking:"
//! bubble, and a rotating "still working" filler during long turns. When the
//! turn ends, the canonical reply replaces the draft and every ephemeral
//! bubble is deleted.
//!
//! This module owns that choreography: the timing, the per-turn state, the
//! edit/delete/typing failure latches, and the finalize invariants (never post
//! a duplicate bubble, never leave a stale draft on screen). The host supplies
//! the transport as a [`ProgressiveSender`]; OpenHuman implements it over its
//! backend REST relay.
//!
//! Drive a turn with [`ProgressiveReply`].

mod bubbles;
mod draft;
mod finalize;
mod latch;
mod reply;
mod state;

use async_trait::async_trait;
use serde_json::Value;

pub use latch::{channel_edits_unsupported, edit_capability_key, mark_channel_edits_unsupported};
pub use reply::ProgressiveReply;
pub use state::{
    StreamingState, TypingState, extract_message_id, extract_message_ids, latest_thinking_snippet,
};

/// Provider deliveries produced while consuming one canonical output batch.
#[derive(Debug, Default)]
pub struct ProgressiveOutputDispatch {
    pub outcomes: Vec<tinychannels_bus::channel::ChannelSendOutcome>,
    pub unsupported: Vec<tinychannels_bus::OutboundContentPart>,
    pub failure: Option<tinychannels_bus::module_progressive::ProgressiveOutputFailure>,
    pub hosted_accepted: bool,
}

impl ProgressiveOutputDispatch {
    /// Retain a provider receipt and fence the batch when delivery is partial
    /// or its side effect is uncertain.
    pub fn push_outcome(&mut self, outcome: tinychannels_bus::channel::ChannelSendOutcome) {
        if outcome.error.is_some() {
            self.failure = Some(
                tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport,
            );
        }
        self.outcomes.push(outcome);
    }

    /// Append one event's delivery facts while preserving event and part order.
    pub fn absorb(&mut self, mut other: Self) {
        self.outcomes.append(&mut other.outcomes);
        self.unsupported.append(&mut other.unsupported);
        self.failure = self.failure.or(other.failure);
        self.hosted_accepted |= other.hosted_accepted;
    }

    /// Normalize accumulated engine facts into the public module result DTO.
    pub fn into_result(self) -> tinychannels_bus::module_progressive::ProgressiveResult {
        use tinychannels_bus::module_progressive::ProgressiveResult as Result;
        match (
            self.outcomes.len(),
            self.unsupported.is_empty(),
            self.failure,
            self.hosted_accepted,
        ) {
            (0, true, None, false) => Result::Updated,
            (1, true, None, false) => Result::DeliveryOutcome {
                outcome: Box::new(self.outcomes.into_iter().next().expect("one outcome")),
            },
            (0, false, None, false) => Result::DeliveryUnsupported {
                parts: self.unsupported,
            },
            (outcomes, true, None, false) if outcomes > 1 => Result::DeliveryOutcomes {
                outcomes: self.outcomes,
            },
            (_, _, failure, _) => Result::DeliveryBatch {
                outcomes: self.outcomes,
                unsupported: self.unsupported,
                failure,
                hosted_accepted: self.hosted_accepted,
            },
        }
    }

    /// Recover accumulated delivery facts before terminal output is applied.
    pub fn from_result(result: &tinychannels_bus::module_progressive::ProgressiveResult) -> Self {
        use tinychannels_bus::module_progressive::ProgressiveResult as Result;
        match result {
            Result::DeliveryOutcome { outcome } => Self {
                outcomes: vec![(**outcome).clone()],
                ..Default::default()
            },
            Result::DeliveryOutcomes { outcomes } => Self {
                outcomes: outcomes.clone(),
                ..Default::default()
            },
            Result::DeliveryUnsupported { parts } => Self {
                unsupported: parts.clone(),
                ..Default::default()
            },
            Result::DeliveryBatch {
                outcomes,
                unsupported,
                failure,
                hosted_accepted,
            } => Self {
                outcomes: outcomes.clone(),
                unsupported: unsupported.clone(),
                failure: *failure,
                hosted_accepted: *hosted_accepted,
            },
            _ => Self::default(),
        }
    }
}

/// Minimum interval between progressive edits of the outbound message. Stays
/// comfortably below Telegram's ~1 edit/sec per-chat cap; Slack has a similar
/// soft limit.
pub const EDIT_FLUSH_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_millis(1000);

/// Consecutive edit failures tolerated before falling back to atomic-final
/// delivery.
pub const MAX_EDIT_FAILURES: u32 = 2;

/// How often to re-send the typing indicator. Telegram's `sendChatAction`
/// lasts about 5 seconds, so a 4 second refresh keeps it continuous.
pub const TYPING_REFRESH_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(4);

/// Consecutive typing failures before the indicator is disabled for the turn.
/// One failure is usually "endpoint doesn't exist"; two is conclusive.
pub const MAX_TYPING_FAILURES: u32 = 2;

/// How often to post a "still working" filler during long turns.
pub const FILLER_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(13);

/// Consecutive filler failures before fillers are disabled for the turn.
pub const MAX_FILLER_FAILURES: u32 = 2;

/// Maximum Unicode scalars in a filler derived from the thinking stream.
pub const MAX_FILLER_CHARS: usize = 200;

/// Rotating fallback fillers, used when the thinking stream has nothing new.
pub const STATIC_FILLERS: &[&str] = &[
    "💭 Still working on it…",
    "💭 Just a moment…",
    "💭 Almost there…",
];

/// Maximum length of the thinking snippet shown in the ephemeral bubble.
pub const MAX_THINKING_DISPLAY_CHARS: usize = 500;

/// Why a progressive send, edit, delete or typing call failed.
///
/// The variants need different recoveries, which is why they are typed rather
/// than matched on error text. Conflating route absence with message absence
/// is what once left a permanent "💭 Thinking:" bubble in the chat (#5230).
#[derive(Debug, thiserror::Error)]
pub enum ProgressiveSendError {
    /// The host cannot reach its backend at all right now (no credential, no
    /// config). Nothing was attempted; callers bail quietly without counting a
    /// failure.
    #[error("progressive sender unavailable")]
    Unavailable,
    /// The backend serves no message-edit route. The message itself is
    /// untouched and still ours: keep its id so it can be deleted.
    #[error("backend has no message-edit route")]
    EditUnsupported,
    /// The message is gone provider-side. Its id is worthless.
    #[error("message not found")]
    MessageGone,
    #[error("channel does not support typed progressive content delivery")]
    UnsupportedContent,
    /// Provider reports a partial or uncertain send with normalized receipt
    /// facts; callers must not retry or flatten this into an ordinary error.
    #[error("progressive send returned a partial or uncertain receipt")]
    PartialOutcome(Box<tinychannels_bus::channel::ChannelSendOutcome>),
    /// Anything else (5xx, transport, rate limit). Counts against the
    /// per-turn failure budget.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Transport for progressive delivery, implemented by the host.
///
/// `channel` is the backend's channel id (possibly provider-prefixed, e.g.
/// `telegram:<chat>`). Implementations attach whatever the backend needs
/// (credentials, idempotency keys) and map their errors onto
/// [`ProgressiveSendError`].
#[async_trait]
pub trait ProgressiveSender: Send + Sync {
    /// Forward the canonical event to the host's channel/web output surface.
    /// Implementations that predate the v2 callback retain the no-op default.
    async fn output_event(
        &self,
        _channel: &str,
        _context: Option<&ProgressiveOutputContext>,
        _sequence: u64,
        _event: &tinychannels_bus::module_progressive::ChannelOutputEvent,
    ) -> Result<(), ProgressiveSendError> {
        Ok(())
    }
    /// Post a new message. Returns the backend's raw response; the message id
    /// is read from it with [`extract_message_id`].
    async fn send(&self, channel: &str, text: &str) -> Result<Value, ProgressiveSendError>;

    /// Deliver ordered provider content and preserve the complete normalized
    /// send outcome. Legacy transports refuse rich content by default.
    async fn send_parts(
        &self,
        _channel: &str,
        _parts: &[tinychannels_bus::OutboundContentPart],
        _text: &str,
        _context: Option<&ProgressiveOutputContext>,
        _sequence: u64,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError> {
        Err(ProgressiveSendError::UnsupportedContent)
    }

    /// Deliver one segment of a logical progressive text event. Legacy
    /// transports may ignore the index; v2 transports use it in their stable
    /// per-event idempotency key.
    async fn send_parts_with_segment(
        &self,
        channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        _segment_index: u32,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError> {
        self.send_parts(channel, parts, text, context, sequence)
            .await
    }

    /// Whether a completed text final must bypass the edit-only draft path so
    /// scoped final-output policy can add voice media. Legacy senders do not
    /// opt into synthesized output.
    async fn should_synthesize_final(
        &self,
        _channel: &str,
        _context: Option<&ProgressiveOutputContext>,
        _text: &str,
    ) -> Result<bool, ProgressiveSendError> {
        Ok(false)
    }

    /// Deliver the completed assistant final through shared turn policy.
    /// Mid-turn typed content uses `send_parts_with_segment` and cannot trigger
    /// automatic voice synthesis.
    async fn send_final_parts_with_segment(
        &self,
        channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        segment_index: u32,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError> {
        self.send_parts_with_segment(channel, parts, text, context, sequence, segment_index)
            .await
    }

    /// Replace the text of a posted message.
    async fn edit(
        &self,
        channel: &str,
        message_id: &str,
        text: &str,
    ) -> Result<(), ProgressiveSendError>;

    /// Edit using the provider's normalized receipt authority when available.
    /// Legacy implementations receive the primary message ID through `edit`.
    async fn edit_with_receipt(
        &self,
        channel: &str,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        text: &str,
    ) -> Result<(), ProgressiveSendError> {
        let message_id = receipt
            .primary_platform_message_id
            .as_deref()
            .ok_or_else(|| {
                ProgressiveSendError::Other(anyhow::anyhow!("receipt has no primary message ID"))
            })?;
        self.edit(channel, message_id, text).await
    }

    /// Delete a posted message.
    async fn delete(&self, channel: &str, message_id: &str) -> Result<(), ProgressiveSendError>;

    /// Delete using the provider's normalized receipt authority when available.
    async fn delete_with_receipt(
        &self,
        channel: &str,
        receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> Result<(), ProgressiveSendError> {
        let message_id = receipt
            .primary_platform_message_id
            .as_deref()
            .ok_or_else(|| {
                ProgressiveSendError::Other(anyhow::anyhow!("receipt has no primary message ID"))
            })?;
        self.delete(channel, message_id).await
    }

    /// Show the typing indicator once.
    async fn typing(&self, channel: &str) -> Result<(), ProgressiveSendError>;
}

/// Host-authoritative identity attached to each event in a scoped v2 turn.
#[derive(Debug, Clone)]
pub struct ProgressiveOutputContext {
    pub turn_id: String,
    pub channel: tinychannels_bus::ChannelRef,
    pub conversation: tinychannels_bus::ConversationRef,
}

/// Whether one configured channel instance supports progressive placeholders.
pub fn channel_supports_progressive_ui(
    capabilities: &tinychannels_bus::ChannelCapabilities,
) -> bool {
    capabilities.progressive_edits
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;
