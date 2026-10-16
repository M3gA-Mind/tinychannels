//! Opaque identity-scoped channel runtime vocabulary.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Inert reservation returned before any provider, callback worker or relay starts.
///
/// The host installs the same immutable identity adapter at `host_object_path`
/// before calling `StartRuntime(handle)`. Existing provider and relay methods
/// then execute at `object_path`. `StopRuntime(handle)` drains and removes that
/// object without affecting another runtime. Unactivated reservations expire
/// after sixty seconds; started runtimes require explicit stop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelRuntimeReservation {
    /// Module-generated opaque identifier; hosts must not mint or reuse it.
    pub handle: String,
    /// Private provider object using the existing Channels interface.
    pub object_path: String,
    /// Private host callback object using the existing ChannelsHost interface.
    pub host_object_path: String,
}

/// Opt-in turn dispatch for a reserved runtime. Legacy `StartRuntime` retains
/// serial `DeliverInbound` envelope notifications; this policy uses `ProcessInbound`
/// callbacks with module-owned bounded concurrency and stop/drain ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelRuntimeDispatchPolicy {
    /// Number of active provider listeners, including an optional relay.
    /// The module admits four turns per listener, clamped to eight through 64.
    pub listener_count: u32,
}

#[cfg(test)]
#[path = "module_runtime_tests.rs"]
mod tests;

/// How a configured provider receives incoming messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChannelIngressKind {
    /// The module owns its native listener and reconnect loop.
    Listener,
    /// An external webhook supplies incoming traffic; outbound remains available.
    ExternalWebhook,
}

/// Provider facts resolved by the module's configured factory without starting it.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConfiguredChannelInfo {
    pub name: String,
    pub ingress: ChannelIngressKind,
    pub interaction: crate::ChannelInteractionInfo,
}

/// Legacy message vocabulary whose normalization and hashing stay in the module.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum ChannelMessagePreparation {
    /// Preserve the rich JSON accepted by the existing controller.
    Legacy(serde_json::Value),
    /// Preserve the existing typed channel send and its idempotency algorithm.
    Typed(crate::SendMessage),
    /// Render an already prepared intent without changing its key or durability.
    Intent(crate::ChannelOutboundIntent),
}

/// An intent and the corresponding legacy backend JSON prepared inside the module.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct PreparedChannelIntent {
    pub intent: crate::ChannelOutboundIntent,
    pub legacy_message: serde_json::Value,
}

/// Encoded message limit leaving room for both returned payloads in a 16 MiB frame.
pub const PREPARATION_MESSAGE_BYTES: usize = 8 * 1024 * 1024 - 4096;
