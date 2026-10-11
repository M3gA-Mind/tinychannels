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

#[cfg(test)]
#[path = "module_runtime_tests.rs"]
mod tests;
