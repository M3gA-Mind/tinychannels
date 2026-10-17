//! Harness-facing channel turn and output event types.

use crate::channel::ChannelInboundEnvelope;

/// Turn admission verdict.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TurnAdmissionVerdict {
    Dispatch,
    ObserveOnly,
    Handled,
    Drop,
}

/// Ordered inbound processing lifecycle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum InboundLifecycleStage {
    Ingest,
    Classify,
    Preflight,
    Resolve,
    Authorize,
    Assemble,
    Record,
    Dispatch,
    Finalize,
}

/// One harness turn assembled from a channel envelope.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelTurn {
    pub id: String,
    pub session_key: String,
    pub envelope: ChannelInboundEnvelope,
    pub admission: TurnAdmissionVerdict,
    pub lifecycle: Vec<InboundLifecycleStage>,
}

impl Default for ChannelTurn {
    fn default() -> Self {
        Self {
            id: String::new(),
            session_key: String::new(),
            envelope: ChannelInboundEnvelope::default(),
            admission: TurnAdmissionVerdict::Dispatch,
            lifecycle: Vec::new(),
        }
    }
}

/// Harness lifecycle event.
#[derive(
    Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(default, rename_all = "camelCase")]
pub struct HarnessLifecycleEvent {
    pub stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

pub use tinychannels_bus::module_progressive::ChannelOutputEvent;
