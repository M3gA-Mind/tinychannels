//! Additive module-owned relay lifecycle vocabulary; no transport or crypto.
use crate::{ChannelCapabilities, ChannelInboundEnvelope};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Additive module operation vocabulary; original version-one members remain.
pub const MODULE_OPERATION_VERSION: (u32, u32) = (1, 25);
/// Maximum relay messages awaiting an outbound result.
pub const RELAY_OUTBOUND_QUEUE: usize = 32;
/// Maximum received relay frame/message bytes, before JSON allocation.
pub const RELAY_FRAME_BYTES: usize = 128 * 1024;
/// Idle reservation lease duration in seconds.
pub const RELAY_RESERVATION_SECONDS: u64 = 30;

/// Module-minted, single-use relay reservation. Removed IDs never start again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct RelayReservation(pub String);

/// Safe relay lifecycle facts, excluding URL, bot identities and credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RelayStatus {
    /// Additive operation vocabulary supported by the module.
    pub contract_version: (u32, u32),
    /// Current resource lifecycle phase.
    pub state: RelayPhase,
}

/// One identity-scoped canonical v2 relay capability declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RelayInstanceCapabilitiesV2 {
    pub platform: String,
    pub bot_id: String,
    pub capabilities: ChannelCapabilities,
}

/// Result of negotiating canonical capabilities for all configured relay identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RelayCapabilitiesV2 {
    pub contract_version: u32,
    pub instances: Vec<RelayInstanceCapabilitiesV2>,
}
/// Relay lifecycle phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RelayPhase {
    Reserved,
    Starting,
    Connected,
    Reconnecting,
    Stopping,
    Stopped,
    Failed,
}

/// Authenticated socket provenance accompanying a normalized inbound envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RelayInboundDelivery {
    /// Provider-neutral inbound facts.
    pub envelope: ChannelInboundEnvelope,
    /// Provenance established by the owning relay transport, not model metadata.
    pub delivered_via_authenticated_relay: bool,
}
