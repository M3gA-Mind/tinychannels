//! Host-addressed web chat projection operations owned by the channel module.
use crate::module_progressive::ChannelOutputEvent;
use crate::web::{WebChannelEvent, WebEventContext};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Begin one projected web turn. `scope_key` is an opaque host-derived
/// isolation key; profile and tenant routing stamps never cross this contract.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BeginWebTurn {
    pub scope_key: String,
    pub context: WebEventContext,
}

/// Opaque identifier for one active module projection state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebTurnHandle {
    pub handle: String,
}

/// Project one canonical output event on an active module-owned turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectWebOutput {
    pub handle: String,
    pub event: ChannelOutputEvent,
}

/// End a turn and invalidate its opaque handle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EndWebTurn {
    pub handle: String,
}

/// Ordered wire events produced by one projection call.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebOutputEvents {
    pub events: Vec<WebChannelEvent>,
}

#[cfg(test)]
#[path = "module_web_output_tests.rs"]
mod tests;
