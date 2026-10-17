//! Transport-free DTOs for the deployed OpenHuman web chat event stream.
//!
//! These events are a separate vocabulary from [`crate::channel::ChannelEvent`]
//! and [`crate::module_progressive::ChannelOutputEvent`]: `event` is the
//! historical Socket.IO discriminator and is intentionally kept in snake case.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Shared addressing and ordering fields present on historical web events.
/// Tenant/profile stamps belong to the host and must never cross the wire.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebEventContext {
    pub client_id: String,
    pub thread_id: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<u64>,
}

/// The tagged, strongly typed web event contract.
///
/// Optional fields are omitted to preserve the deployed struct's wire shape.
/// The `chat_error` variant intentionally remains able to represent the legacy
/// `error_type: "cancelled"` payload during the compatibility window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum WebChannelEvent {
    ChatDone { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] full_response: Option<String>, #[serde(skip_serializing_if="Option::is_none")] citations: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] usage: Option<WebTurnUsagePayload>, #[serde(skip_serializing_if="Option::is_none")] timing: Option<WebTurnTimingPayload> },
    ChatInterim { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] full_response: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32> },
    ChatSuggestions { #[serde(flatten)] context: WebEventContext, suggestions: Vec<ChatSuggestion> },
    ChatError { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] error_type: Option<String>, #[serde(skip_serializing_if="Option::is_none")] error_source: Option<String>, #[serde(skip_serializing_if="Option::is_none")] error_retryable: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] error_retry_after_ms: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] error_provider: Option<String>, #[serde(skip_serializing_if="Option::is_none")] error_fallback_available: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] copy_key: Option<String>, #[serde(skip_serializing_if="Option::is_none")] copy_params: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] guardrail: Option<WebGuardrailPayload> },
    ChatCancelled { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] cancel_reason: Option<String>, #[serde(skip_serializing_if="Option::is_none")] superseded_by: Option<String> },
    TextDelta { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] delta: Option<String>, #[serde(skip_serializing_if="Option::is_none")] delta_kind: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32> },
    ThinkingDelta { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] delta: Option<String>, #[serde(skip_serializing_if="Option::is_none")] delta_kind: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32> },
    ToolArgsDelta { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] delta: Option<String>, #[serde(skip_serializing_if="Option::is_none")] delta_kind: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32> },
    InferenceStart { #[serde(flatten)] context: WebEventContext }, InferenceHeartbeat { #[serde(flatten)] context: WebEventContext },
    IterationStart { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32> },
    ToolCall { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_display_label: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_display_detail: Option<String> },
    ToolProgress { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] message: Option<String> },
    ToolResult { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] output: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] success: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] failure: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] elapsed_ms: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] tool_display_label: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_display_detail: Option<String>, #[serde(skip_serializing_if="Option::is_none")] structured: Option<serde_json::Value> },
    TurnCost { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] usage: Option<WebTurnUsagePayload>, #[serde(skip_serializing_if="Option::is_none")] cost_usd: Option<f64>, #[serde(skip_serializing_if="Option::is_none")] cost_source: Option<WebCostSource>, #[serde(skip_serializing_if="Option::is_none")] context_window: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] context_tokens: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] subagents: Option<Vec<WebSubagentUsagePayload>> },
    SubagentSpawned { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] mode: Option<String>, #[serde(skip_serializing_if="Option::is_none")] dedicated_thread: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] prompt_chars: Option<u64> },
    SubagentCompleted { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] success: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] elapsed_ms: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] iterations: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] output_chars: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] output: Option<String>, #[serde(skip_serializing_if="Option::is_none")] input_tokens: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] output_tokens: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] cached_input_tokens: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] cost_usd: Option<f64> },
    SubagentFailed { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] success: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32> },
    SubagentAwaitingUser { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] success: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail> },
    SubagentIterationStart { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] child_iteration: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] child_max_iterations: Option<u32> },
    SubagentToolCall { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_display_label: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_display_detail: Option<String>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] child_iteration: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] agent_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] task_id: Option<String> },
    SubagentToolResult { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] success: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] output: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] elapsed_ms: Option<u64>, #[serde(skip_serializing_if="Option::is_none")] tool_display_label: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_display_detail: Option<String>, #[serde(skip_serializing_if="Option::is_none")] failure: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] child_iteration: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] agent_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] task_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] output_chars: Option<u64> },
    SubagentTextDelta { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] delta: Option<String>, #[serde(skip_serializing_if="Option::is_none")] delta_kind: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] child_iteration: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] agent_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] task_id: Option<String> },
    SubagentThinkingDelta { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] delta: Option<String>, #[serde(skip_serializing_if="Option::is_none")] delta_kind: Option<String>, #[serde(skip_serializing_if="Option::is_none")] round: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] skill_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] subagent: Option<WebSubagentProgressDetail>, #[serde(skip_serializing_if="Option::is_none")] child_iteration: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] agent_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] task_id: Option<String> },
    ApprovalRequest { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] expires_at: Option<String>, #[serde(skip_serializing_if="Option::is_none")] detached: Option<bool> },
    ApprovalDecided { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] cancel_reason: Option<String> },
    PlanReviewRequest { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] expires_at: Option<String> },
    PlanReviewDecided { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_name: Option<String>, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] cancel_reason: Option<String> },
    QueueItemQueued { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] queue_item: Option<WebQueueItemPayload> }, QueueItemDelivered { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] queue_item: Option<WebQueueItemPayload> }, QueueItemRemoved { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] queue_item: Option<WebQueueItemPayload> },
    ThreadGoalUpdated { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] goal: Option<serde_json::Value> }, ThreadGoalCleared { #[serde(flatten)] context: WebEventContext }, ThreadTodosChanged { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] todos: Option<serde_json::Value> }, RunModeChanged { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String> },
    ArtifactReady { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value> }, ArtifactFailed { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value> }, ArtifactPending { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] tool_call_id: Option<String>, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value> }, MemoryActivity { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value> }, ExternalTransferPending { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] args: Option<serde_json::Value> },
    ProactiveMessage { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] full_response: Option<String>, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] success: Option<bool>, #[serde(skip_serializing_if="Option::is_none")] persisted_message_id: Option<String> }, VoiceSpeak { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] full_response: Option<String>, #[serde(skip_serializing_if="Option::is_none")] success: Option<bool> }, UserError { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] error_type: Option<String>, #[serde(skip_serializing_if="Option::is_none")] error_source: Option<String> },
    /// Accepted by older clients; no current core emitter produces this event.
    ChatMessage { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] full_response: Option<String> },
    /// Accepted by older clients; no current core emitter produces this event.
    ChatSegment { #[serde(flatten)] context: WebEventContext, #[serde(skip_serializing_if="Option::is_none")] message: Option<String>, #[serde(skip_serializing_if="Option::is_none")] full_response: Option<String>, #[serde(skip_serializing_if="Option::is_none")] segment_index: Option<u32>, #[serde(skip_serializing_if="Option::is_none")] segment_total: Option<u32> },
}
/// Cost provenance copied as a wire enum to avoid depending on the harness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WebCostSource {
    #[default]
    Charged,
    Estimated,
    Unknown,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebTurnTimingPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_token_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_tool_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens_per_second: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ChatSuggestion {
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebGuardrailReason {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebGuardrailPayload {
    pub verdict: String,
    pub score: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<WebGuardrailReason>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebQueueItemPayload {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lane: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_preview: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebTurnUsagePayload {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cost_usd: Option<f64>,
    #[serde(default)]
    pub cost_source: WebCostSource,
    pub context_window: u64,
    #[serde(default)]
    pub context_tokens: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subagents: Vec<WebSubagentUsagePayload>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebSubagentUsagePayload {
    pub task_id: String,
    pub agent_id: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct WebSubagentProgressDetail {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedicated_thread: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_chars: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_iteration: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_max_iterations: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iterations: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_chars: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_files: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dirty_status: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

/// Web request metadata; attribution fields do not imply routing authority.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ChatRequestMetadata {
        #[serde(skip_serializing_if = "Option::is_none")]
    pub speak_reply: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub hold_text_stream: bool,
}

/// Web chat's five request dispositions (distinct from harness queue lanes).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueueMode {
    #[default]
    Interrupt,
    Steer,
    Followup,
    Collect,
    Parallel,
}

impl std::fmt::Display for QueueMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Interrupt => "interrupt",
            Self::Steer => "steer",
            Self::Followup => "followup",
            Self::Collect => "collect",
            Self::Parallel => "parallel",
        })
    }
}

/// Pure Socket.IO event-name compatibility mapping. The returned string is a
/// transport event name; callers must emit the unchanged payload whose
/// `event` discriminator remains canonical. Streaming aliases stay suppressed.
pub fn legacy_event_alias(name: &str, enabled: bool) -> Option<String> {
    if !enabled {
        return None;
    }
    const STREAMING: &[&str] = &["text_delta", "thinking_delta", "tool_args_delta"];
    let normalized = name.replace(':', "_");
    let base = normalized.strip_prefix("subagent_").unwrap_or(&normalized);
    if STREAMING.contains(&base) {
        return None;
    }
    if name.contains('_') {
        Some(name.replace('_', ":"))
    } else if name.contains(':') {
        Some(name.replace(':', "_"))
    } else {
        None
    }
}

/// Deterministic JSON Schema document consumed by downstream TS generation.
pub fn web_event_schema_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schemars::schema_for!(WebChannelEvent))
}

/// Source-compatible alias for callers that used the generic event payload name.
pub type WebEventPayload = WebChannelEvent;
