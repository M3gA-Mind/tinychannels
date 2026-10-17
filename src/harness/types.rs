//! Harness-facing channel turn and output event types.

use crate::channel::{ChannelInboundEnvelope, MediaReference};
use serde_json::Value;

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

/// Output emitted by the harness for channel delivery.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChannelOutputEvent {
    TextDelta {
        text: String,
    },
    FinalMessage {
        text: String,
    },
    ToolProgress {
        label: String,
        message: Option<String>,
    },
    /// A tool invocation began. Correlation and call arguments are retained
    /// for host timelines; renderers should use `display_label` and must not
    /// print arguments unless their own audience policy allows it.
    ToolCallStarted {
        call_id: String,
        tool_name: String,
        arguments: Value,
        iteration: u32,
        display_label: Option<String>,
        display_detail: Option<String>,
    },
    /// Raw arguments streamed while the model composes a tool call.
    ToolCallArgsDelta {
        call_id: String,
        tool_name: String,
        delta: String,
        iteration: u32,
    },
    /// Progress from a running tool, correlated to its parent invocation.
    ToolCallProgress {
        call_id: String,
        message: String,
    },
    /// A tool finished with its output and optional structured result intact.
    ToolCallCompleted {
        call_id: String,
        tool_name: String,
        success: bool,
        output_chars: usize,
        output: String,
        arguments: Option<Value>,
        elapsed_ms: u64,
        iteration: u32,
        display_label: Option<String>,
        display_detail: Option<String>,
        structured: Option<Value>,
    },
    /// Nested child-agent event. The host projects only fields allowed for the
    /// recipient; model input/output capture does not belong in this DTO.
    Subagent {
        agent_id: String,
        task_id: String,
        state: String,
        call_id: Option<String>,
        iteration: Option<u32>,
        message: Option<String>,
        data: Option<Value>,
    },
    /// Cumulative turn token/cost facts, when the harness reports them.
    Usage {
        model: Option<String>,
        iteration: Option<u32>,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
        total_usd: Option<f64>,
    },
    /// Artifact lifecycle event associated with this turn.
    Artifact {
        operation: String,
        artifact: Value,
    },
    ApprovalRequest {
        id: String,
        prompt: String,
        choices: Vec<String>,
    },
    Clarification {
        prompt: String,
    },
    Media {
        text: Option<String>,
        media: Vec<MediaReference>,
    },
    Cancellation {
        reason: Option<String>,
    },
    Lifecycle {
        event: HarnessLifecycleEvent,
    },
    Native {
        data: Value,
    },
}
