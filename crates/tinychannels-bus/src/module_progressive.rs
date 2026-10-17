//! Stateful progressive reply vocabulary; delivery choreography stays in the module.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Canonical channel output vocabulary shared by host dispatch and module
/// progressive delivery. Fields are transport DTOs; the host owns policy for
/// selecting which payloads may be shown to a recipient.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ChannelOutputEvent {
    TextDelta {
        text: String,
        iteration: Option<u32>,
    },
    ThinkingDelta {
        text: String,
        iteration: Option<u32>,
    },
    FinalMessage {
        text: String,
    },
    ToolProgress {
        label: String,
        message: Option<String>,
    },
    ToolCallStarted {
        call_id: String,
        tool_name: String,
        arguments: Value,
        iteration: u32,
        display_label: Option<String>,
        display_detail: Option<String>,
    },
    ToolCallArgsDelta {
        call_id: String,
        tool_name: String,
        delta: String,
        iteration: u32,
    },
    ToolCallProgress {
        call_id: String,
        message: String,
    },
    ToolCallCompleted {
        call_id: String,
        tool_name: String,
        success: bool,
        output_chars: usize,
        output: String,
        arguments: Option<Value>,
        elapsed_ms: u64,
        iteration: u32,
        failure: Option<ToolFailureSummary>,
        display_label: Option<String>,
        display_detail: Option<String>,
        structured: Option<Value>,
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
        parts: Vec<crate::channel::ContentPart>,
    },
    VoiceNote {
        text: Option<String>,
        voice: crate::channel::VoiceNote,
    },
    Subagent {
        agent_id: String,
        task_id: String,
        state: String,
        mode: Option<String>,
        dedicated_thread: Option<bool>,
        parent_call_id: Option<String>,
        call_id: Option<String>,
        iteration: Option<u32>,
        max_iterations: Option<u32>,
        prompt_chars: Option<usize>,
        message: Option<String>,
        display_name: Option<String>,
        tool_name: Option<String>,
        arguments: Option<Value>,
        success: Option<bool>,
        elapsed_ms: Option<u64>,
        output_chars: Option<usize>,
        structured: Option<Value>,
        usage: Option<SubagentUsage>,
        failure: Option<ToolFailureSummary>,
        display_label: Option<String>,
        display_detail: Option<String>,
        worker_thread_id: Option<String>,
        changed_files: Vec<String>,
        dirty_status: Option<bool>,
        stop: Option<TurnStopSummary>,
    },
    ModelUsage {
        model: String,
        provider_id: String,
        subagent_task_id: Option<String>,
        iteration: u32,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
        cache_creation_tokens: u64,
        reasoning_tokens: u64,
        cost_usd: Option<f64>,
    },
    Usage {
        model: Option<String>,
        iteration: Option<u32>,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
        total_usd: Option<f64>,
    },
    Artifact {
        operation: String,
        artifact: Value,
    },
    Cancellation {
        reason: Option<String>,
    },
    Lifecycle {
        stage: String,
        message: Option<String>,
        iteration: Option<u32>,
        max_iterations: Option<u32>,
        stop: Option<TurnStopSummary>,
    },
    Native {
        data: Value,
    },
}

/// Transport-safe projection of a classified tool failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ToolFailureSummary {
    pub class: String,
    pub category: String,
    pub cause_plain: String,
    pub next_action: String,
    pub recoverable: bool,
}

/// Token and charged-cost totals that belong to one child agent.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubagentUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub charged_amount_usd: f64,
    pub cost_source: CostSource,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CostSource {
    Charged,
    Estimated,
    #[default]
    Unknown,
}

/// Content-free reason a turn or child run stopped early.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnStopSummary {
    pub kind: String,
    pub failure_class: Option<String>,
    pub operation: Option<String>,
}

/// Frozen v1 progressive event wire shape retained for the compatibility
/// operation. New hosts should use `FeedOutput` and `ChannelOutputEvent`.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum ProgressiveEvent {
    TextDelta(String),
    ThinkingDelta(String),
    ToolCall(String),
    ToolResult { tool_name: String, success: bool },
}

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
        events: Vec<ProgressiveEvent>,
    },
    /// Feed canonical output events without converting them through the v1
    /// four-variant progressive projection.
    FeedOutput {
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
    /// Ordered, host-authorized projection of one canonical turn event.
    OutputEvent {
        channel: String,
        event: ChannelOutputEvent,
    },
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
