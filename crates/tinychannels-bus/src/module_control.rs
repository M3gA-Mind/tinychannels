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

/// Module-described authority for the exact configured instance associated
/// with a route-preparation request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConfiguredControlChannel {
    /// Configured instance name, which may include an account suffix.
    pub name: String,
    /// Capabilities returned by the module's configured-instance descriptor.
    pub capabilities: ChannelCapabilities,
}

/// Bounded facts: execution, parsing and formatting remain in the module.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum ChannelControlRequest {
    Route {
        workspace_dir: String,
        message: Box<ChannelMessage>,
        /// Capabilities of the configured channel instance selected by the host.
        /// Isolated turn runtimes do not own provider instances themselves.
        /// Missing values retain compatibility with callers that rely on the
        /// module's own configured provider registry.
        #[serde(default)]
        capabilities: Option<Box<ChannelCapabilities>>,
        route: RemoteRoute,
        history_len: usize,
        session_scope: Option<Box<RemoteSessionScope>>,
        /// Trusted descriptor facts for the exact active configured channel.
        /// The host supplies them when preparing a command through a
        /// temporary callback object that has no running provider map.
        #[serde(default)]
        configured_channel: Option<Box<ConfiguredControlChannel>>,
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
