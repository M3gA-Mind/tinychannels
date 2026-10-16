//! The stored (v2) form of the delivery records.
//!
//! [`QueuedDelivery`] and [`DeliveryTerminalResult`] are bus wire types: they
//! are camelCase, carry `*UnixMs` times and travel unchanged between a host and
//! the module. What the document store holds is a separate shape: snake_case
//! bodies, one `*_ms` field per instant, no field that repeats the document id.
//! These DTOs are the only place the two meet.

use serde::{Deserialize, Serialize};
use tinychannels_bus::{
    ChannelOutboundIntent, DeliveryDurability, DeliveryTerminalResult, OutboundPayload,
};

use crate::delivery::{DeliveryRecoveryState, QueuedDelivery};

/// A wire millisecond count as the stored `i64` (saturating, never negative).
pub(crate) fn to_stored_ms(ms: u64) -> i64 {
    i64::try_from(ms).unwrap_or(i64::MAX)
}

/// A stored millisecond count as the wire `u64` (negative counts read as 0).
pub(crate) fn from_stored_ms(ms: i64) -> u64 {
    u64::try_from(ms).unwrap_or(0)
}

/// The outbound intent as stored. The payload is already snake_case on the
/// wire, so only the five camelCase fields differ.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredIntent {
    pub idempotency_key: String,
    pub channel_id: String,
    pub conversation_id: String,
    pub reply_to_id: Option<String>,
    pub thread_id: Option<String>,
    pub durability: DeliveryDurability,
    pub payload: OutboundPayload,
}

impl From<&ChannelOutboundIntent> for StoredIntent {
    fn from(intent: &ChannelOutboundIntent) -> Self {
        Self {
            idempotency_key: intent.idempotency_key.clone(),
            channel_id: intent.channel_id.clone(),
            conversation_id: intent.conversation_id.clone(),
            reply_to_id: intent.reply_to_id.clone(),
            thread_id: intent.thread_id.clone(),
            durability: intent.durability,
            payload: intent.payload.clone(),
        }
    }
}

impl From<StoredIntent> for ChannelOutboundIntent {
    fn from(stored: StoredIntent) -> Self {
        Self {
            idempotency_key: stored.idempotency_key,
            channel_id: stored.channel_id,
            conversation_id: stored.conversation_id,
            reply_to_id: stored.reply_to_id,
            thread_id: stored.thread_id,
            durability: stored.durability,
            payload: stored.payload,
        }
    }
}

/// A pending or failed delivery as stored. The entry id is the intent's
/// idempotency key, so the body does not repeat it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredDelivery {
    pub intent: StoredIntent,
    pub enqueued_ms: i64,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_send_started_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_state: Option<DeliveryRecoveryState>,
}

impl From<&QueuedDelivery> for StoredDelivery {
    fn from(entry: &QueuedDelivery) -> Self {
        Self {
            intent: StoredIntent::from(&entry.intent),
            enqueued_ms: to_stored_ms(entry.enqueued_at_unix_ms),
            retry_count: entry.retry_count,
            last_attempt_ms: entry.last_attempt_at_unix_ms.map(to_stored_ms),
            last_error: entry.last_error.clone(),
            platform_send_started_ms: entry.platform_send_started_at_unix_ms.map(to_stored_ms),
            recovery_state: entry.recovery_state,
        }
    }
}

impl From<StoredDelivery> for QueuedDelivery {
    fn from(stored: StoredDelivery) -> Self {
        let intent = ChannelOutboundIntent::from(stored.intent);
        Self {
            id: intent.idempotency_key.clone(),
            intent,
            enqueued_at_unix_ms: from_stored_ms(stored.enqueued_ms),
            retry_count: stored.retry_count,
            last_attempt_at_unix_ms: stored.last_attempt_ms.map(from_stored_ms),
            last_error: stored.last_error,
            platform_send_started_at_unix_ms: stored.platform_send_started_ms.map(from_stored_ms),
            recovery_state: stored.recovery_state,
        }
    }
}

/// A terminal result as stored. Its lifetime is the record's `expires_ms`
/// metadata, not a body field. The id is kept because a long idempotency key
/// makes a hashed document id that cannot be turned back into it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredResult {
    pub id: String,
    pub intent_fingerprint: String,
    pub completed_ms: i64,
}

impl From<&DeliveryTerminalResult> for StoredResult {
    fn from(result: &DeliveryTerminalResult) -> Self {
        Self {
            id: result.id.clone(),
            intent_fingerprint: result.intent_fingerprint.clone(),
            completed_ms: to_stored_ms(result.completed_at_unix_ms),
        }
    }
}

impl From<StoredResult> for DeliveryTerminalResult {
    fn from(stored: StoredResult) -> Self {
        Self {
            id: stored.id,
            intent_fingerprint: stored.intent_fingerprint,
            completed_at_unix_ms: from_stored_ms(stored.completed_ms),
        }
    }
}
