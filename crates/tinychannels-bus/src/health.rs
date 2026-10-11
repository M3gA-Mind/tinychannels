//! Serialized provider health observations. Providers and deadlines execute
//! inside the module; the host formats these observations for its user.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Outcome of one bounded provider health check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChannelHealthState {
    Healthy,
    Unhealthy,
    Timeout,
}

/// One provider's observation, returned in configured provider order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChannelHealthReport {
    pub channel: String,
    pub state: ChannelHealthState,
}
