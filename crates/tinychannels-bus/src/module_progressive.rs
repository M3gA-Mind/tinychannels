//! Stateful progressive reply vocabulary; delivery choreography stays in the module.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Canonical channel output vocabulary shared by host dispatch and module
/// progressive delivery. Fields are transport DTOs; the host owns policy for
/// selecting which payloads may be shown to a recipient.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum ChannelOutputEvent {
    TextDelta { text: String },
    ThinkingDelta { text: String, iteration: Option<u32> },
    FinalMessage { text: String },
    ToolProgress { label: String, message: Option<String> },
    ToolCallStarted { call_id: String, tool_name: String, arguments: Value, iteration: u32, display_label: Option<String>, display_detail: Option<String> },
    ToolCallArgsDelta { call_id: String, tool_name: String, delta: String, iteration: u32 },
    ToolCallProgress { call_id: String, message: String },
    ToolCallCompleted { call_id: String, tool_name: String, success: bool, output_chars: usize, output: String, arguments: Option<Value>, elapsed_ms: u64, iteration: u32, display_label: Option<String>, display_detail: Option<String>, structured: Option<Value> },
    ApprovalRequest { id: String, prompt: String, choices: Vec<String> },
    Clarification { prompt: String },
    Media { text: Option<String>, media: Vec<crate::channel::MediaReference> },
    VoiceNote { text: Option<String>, voice: crate::channel::VoiceNote },
    Subagent { agent_id: String, task_id: String, state: String, call_id: Option<String>, iteration: Option<u32>, message: Option<String>, data: Option<Value> },
    Usage { model: Option<String>, iteration: Option<u32>, input_tokens: u64, output_tokens: u64, cached_input_tokens: u64, total_usd: Option<f64> },
    Artifact { operation: String, artifact: Value },
    Cancellation { reason: Option<String> },
    Lifecycle { stage: String, message: Option<String> },
    Native { data: Value },
}

/// Compatibility type name for progressive commands. It is the same
/// canonical DTO used by host turn dispatch.
#[deprecated(note = "use ChannelOutputEvent")]
pub type ProgressiveEvent = ChannelOutputEvent;

/// Compatibility name retained while module callers migrate to the shared
/// output DTO. New producers should send `ChannelOutputEvent` values.

/// A handle belongs to the private runtime that opened it.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ProgressiveCommand {
    Open {
        channel: String,
    },
    Feed {
        handle: String,
        events: Vec<ChannelOutputEvent>,
    },
    Finish {
        handle: String,
        text: String,
    },
    Close {
        handle: String,
    },
}

/// Completion of one bounded command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ProgressiveResult {
    Opened { handle: String },
    Updated,
    Closed,
}

/// The host supplies its managed REST transport and captured credential.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ProgressiveTransportRequest {
    Send {
        channel: String,
        message: Value,
    },
    Edit {
        channel: String,
        message_id: String,
        text: String,
    },
    Delete {
        channel: String,
        message_id: String,
    },
    Typing {
        channel: String,
    },
}

/// Typed transport facts preserve the module's existing recovery rules.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ProgressiveTransportResult {
    Success(Value),
    Unavailable,
    EditUnsupported,
    MessageGone,
    Failed,
}
