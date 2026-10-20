//! Requests for module-owned channel command and presentation preparation.
use crate::module_remote::{RemoteRoute, RemoteSessionScope};
use crate::{ChannelCapabilities, ChannelMessage, LegacyChannelCapabilities};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A provider offered by the host's approved inference catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderDescriptor {
    pub name: String,
    pub aliases: Vec<String>,
}

/// Bounded facts: execution, parsing and formatting remain in the module.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum ChannelControlRequest {
    Route {
        workspace_dir: String,
        message: Box<ChannelMessage>,
        route: RemoteRoute,
        history_len: usize,
        session_scope: Option<RemoteSessionScope>,
        providers: Vec<ProviderDescriptor>,
    },
    RuntimeLimits {
        timeout_secs: u64,
        listener_count: usize,
    },
    Connected {
        configured: Vec<String>,
        channel: String,
        mode: crate::ChannelAuthMode,
    },
    Overflow {
        message: String,
    },
    Capabilities {
        channel: String,
    },
    /// Return the canonical configured-instance descriptor without changing
    /// the frozen `Capabilities` response payload.
    CapabilitiesV2 {
        channel: String,
    },
    Acknowledgment {
        content: String,
    },
    Approval {
        tool_name: String,
        action_summary: String,
    },
}
/// Product policy applies returned facts; the module never selects approval policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ChannelControlResult {
    Route {
        reply: Option<String>,
        next_route: Option<RemoteRoute>,
        clear_history: bool,
    },
    RuntimeLimits {
        timeout_secs: u64,
        max_in_flight: usize,
    },
    Connected(bool),
    Overflow(bool),
    Capabilities(LegacyChannelCapabilities),
    CapabilitiesV2(Box<ChannelCapabilities>),
    Approval(String),
    Acknowledgment(String),
}
