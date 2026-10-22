//! Pure callback vocabulary for host-owned durable delivery records.
use crate::ChannelOutboundIntent;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maximum number of pending records the module may ask a host to retain.
pub const DELIVERY_PENDING_CAPACITY: usize = 256;
/// Maximum number of replay results a host retains for lost replies.
pub const DELIVERY_RESULT_CAPACITY: usize = 256;
/// Minimum result replay retention, in seconds.
pub const DELIVERY_RESULT_TTL_SECONDS: u64 = 24 * 60 * 60;
/// Maximum serialized delivery intent passed through one callback.
pub const DELIVERY_RECORD_BYTES: usize = 128 * 1024;

/// Crash state for one persisted final delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryRecoveryState {
    SendAttemptStarted,
    UnknownAfterSend,
}

/// Host-persisted pending delivery record. Payloads remain host-scoped storage.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DeliveryQueueEntry {
    pub id: String,
    pub intent: ChannelOutboundIntent,
    pub enqueued_at_unix_ms: u64,
    pub retry_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_send_started_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_state: Option<DeliveryRecoveryState>,
    /// Receipt from a provider that emitted part of this operation before failing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_receipt: Option<crate::channel::MessageReceipt>,
}

impl DeliveryQueueEntry {
    /// Create a fresh pending record with no prior send attempts.
    pub fn new(
        id: impl Into<String>,
        intent: ChannelOutboundIntent,
        enqueued_at_unix_ms: u64,
    ) -> Self {
        Self {
            id: id.into(),
            intent,
            enqueued_at_unix_ms,
            ..Self::default()
        }
    }
}

/// Bounded replay record written before a successful pending entry is removed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryTerminalResult {
    pub id: String,
    /// SHA-256 of the serialized intent, used to reject idempotency-key reuse.
    pub intent_fingerprint: String,
    pub completed_at_unix_ms: u64,
    /// Provider receipt retained for idempotent replay of the completed operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt: Option<crate::channel::MessageReceipt>,
}

#[cfg(test)]
#[path = "module_delivery_tests.rs"]
mod tests;
