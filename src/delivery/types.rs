//! Durable delivery queue types.

use crate::channel::MessageReceipt;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use tinychannels_bus::{DeliveryQueueEntry as QueuedDelivery, DeliveryRecoveryState};

/// Maximum recovery attempts before a pending entry is moved to failed storage.
pub const MAX_RETRIES: u32 = 5;

/// Queue retry eligibility decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRetryEligibility {
    Eligible,
    Deferred { remaining_backoff_ms: u64 },
}

/// Unknown-send reconciliation verdict from an adapter or host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum UnknownSendReconciliation {
    Sent {
        receipt: Box<MessageReceipt>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    NotSent,
    Unresolved {
        #[serde(default)]
        retryable: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

/// Delivery attempt success returned by the host delivery driver.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DeliveryAttemptSuccess {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt: Option<MessageReceipt>,
}

/// Delivery attempt failure returned by the host delivery driver.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DeliveryAttemptFailure {
    pub error: String,
    pub sent_before_error: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permanent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt: Option<MessageReceipt>,
}

/// Result of one host delivery attempt.
pub type DeliveryAttemptResult =
    std::result::Result<DeliveryAttemptSuccess, DeliveryAttemptFailure>;

/// Recovery counters for one drain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DeliveryRecoverySummary {
    pub recovered: u32,
    pub failed: u32,
    pub skipped_max_retries: u32,
    pub deferred_backoff: u32,
}

/// Result of claiming an active delivery queue entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActiveDeliveryClaimResult<T> {
    Claimed(T),
    ClaimedByOtherOwner,
}

/// Delivery queue error.
#[derive(Debug, thiserror::Error)]
pub enum DeliveryQueueError {
    #[error("delivery queue store error: {0}")]
    Store(String),
    #[error("host delivery queue callback is unavailable")]
    Unsupported,
}

/// Host-owned durable queue storage.
#[async_trait]
pub trait DeliveryQueueStore: Send + Sync {
    async fn save_pending(&self, entry: QueuedDelivery) -> Result<(), DeliveryQueueError>;
    async fn load_pending(&self, id: &str) -> Result<Option<QueuedDelivery>, DeliveryQueueError>;
    async fn load_pending_all(&self) -> Result<Vec<QueuedDelivery>, DeliveryQueueError>;
    async fn remove_pending(&self, id: &str) -> Result<(), DeliveryQueueError>;
    async fn save_failed(&self, entry: QueuedDelivery) -> Result<(), DeliveryQueueError>;
}

/// Bounded terminal replay and failed-record lookups owned by host storage.
#[async_trait]
pub trait DeliveryResultStore: DeliveryQueueStore {
    async fn load_failed(&self, id: &str) -> Result<Option<QueuedDelivery>, DeliveryQueueError>;
    async fn load_result(
        &self,
        id: &str,
    ) -> Result<Option<tinychannels_bus::DeliveryTerminalResult>, DeliveryQueueError>;
    async fn save_result(
        &self,
        result: tinychannels_bus::DeliveryTerminalResult,
    ) -> Result<(), DeliveryQueueError>;
}

/// Host delivery hooks used by recovery drains.
#[async_trait]
pub trait DeliveryQueueHandler: Send + Sync {
    async fn deliver(&self, entry: &QueuedDelivery) -> DeliveryAttemptResult;

    async fn reconcile_unknown_send(
        &self,
        _entry: &QueuedDelivery,
    ) -> Result<Option<UnknownSendReconciliation>, DeliveryQueueError> {
        Ok(None)
    }
}

/// Selection result for targeted reconnect drains.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PendingDeliveryDrainDecision {
    pub matches: bool,
    pub bypass_backoff: bool,
}

/// Capability negotiation result for a durable send request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityNegotiation {
    pub durability: crate::channel::DeliveryDurability,
    pub missing_capabilities: Vec<crate::channel::DurableFinalDeliveryCapability>,
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
