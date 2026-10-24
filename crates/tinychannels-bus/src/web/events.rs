use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::types::*;

/// The tagged, strongly typed web event contract.
///
/// Optional fields are omitted to preserve the deployed struct's wire shape.
/// The `chat_error` variant intentionally remains able to represent the legacy
/// `error_type: "cancelled"` payload during the compatibility window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum WebChannelEvent {
    ChatDone {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_response: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        citations: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<WebTurnUsagePayload>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timing: Option<WebTurnTimingPayload>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parts: Option<Vec<crate::channel::OutboundContentPart>>,
    },
    ChatInterim {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_response: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
    },
    ChatSuggestions {
        #[serde(flatten)]
        context: WebEventContext,
        suggestions: Vec<ChatSuggestion>,
    },
    ChatError {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_source: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_retryable: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_retry_after_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_provider: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_fallback_available: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        copy_key: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        copy_params: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        guardrail: Option<WebGuardrailPayload>,
    },
    ChatCancelled {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        cancel_reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        superseded_by: Option<String>,
    },
    TextDelta {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta_kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
    },
    ThinkingDelta {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta_kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
    },
    ToolArgsDelta {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta_kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
    },
    InferenceStart {
        #[serde(flatten)]
        context: WebEventContext,
    },
    InferenceHeartbeat {
        #[serde(flatten)]
        context: WebEventContext,
    },
    IterationStart {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
    },
    /// The turn started summarizing earlier conversation to fit the context
    /// window; the next `iteration_start` ends it.
    ContextCompacting {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
    },
    ToolCall {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_detail: Option<String>,
    },
    ToolProgress {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    ToolResult {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        elapsed_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_detail: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        structured: Option<serde_json::Value>,
    },
    TurnCost {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<WebTurnUsagePayload>,
    },
    SubagentSpawned {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    SubagentCompleted {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    SubagentFailed {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
    },
    SubagentAwaitingUser {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    SubagentIterationStart {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    SubagentToolCall {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_detail: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    SubagentToolResult {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        elapsed_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_display_detail: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        structured: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    SubagentTextDelta {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta_kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    SubagentThinkingDelta {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        delta_kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        round: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skill_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        subagent: Option<WebSubagentProgressDetail>,
    },
    ApprovalRequest {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        expires_at: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        detached: Option<bool>,
    },
    ApprovalDecided {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cancel_reason: Option<String>,
    },
    PlanReviewRequest {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        expires_at: Option<String>,
    },
    PlanReviewDecided {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cancel_reason: Option<String>,
    },
    QueueItemQueued {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        queue_item: Option<WebQueueItemPayload>,
    },
    QueueItemDelivered {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        queue_item: Option<WebQueueItemPayload>,
    },
    QueueItemRemoved {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        queue_item: Option<WebQueueItemPayload>,
    },
    ThreadGoalUpdated {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        goal: Option<serde_json::Value>,
    },
    ThreadGoalCleared {
        #[serde(flatten)]
        context: WebEventContext,
    },
    ThreadTodosChanged {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        todos: Option<serde_json::Value>,
    },
    RunModeChanged {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    ArtifactReady {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
    },
    ArtifactFailed {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
    },
    ArtifactPending {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
    },
    MemoryActivity {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
    },
    ExternalTransferPending {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<serde_json::Value>,
    },
    ProactiveMessage {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_response: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        persisted_message_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parts: Option<Vec<crate::channel::OutboundContentPart>>,
    },
    VoiceSpeak {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_response: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
    },
    ChannelOutbound {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_response: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        structured: Option<serde_json::Value>,
    },
    /// A normalized provider event projected to the web client. The nested
    /// payload remains the canonical TinyChannels event contract, including
    /// its version, channel/conversation identity, and typed event kind.
    ChannelEvent {
        #[serde(flatten)]
        context: WebEventContext,
        /// OpenHuman's persisted row ID for the canonical event's target, when
        /// the host has an exact durable provider-receipt binding.
        #[serde(skip_serializing_if = "Option::is_none")]
        openhuman_message_id: Option<String>,
        channel_event: crate::channel::ChannelEvent,
    },
    UserError {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_source: Option<String>,
    },
    /// Accepted by older clients; no current core emitter produces this event.
    ChatMessage {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_response: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parts: Option<Vec<crate::channel::OutboundContentPart>>,
    },
    /// Accepted by older clients; no current core emitter produces this event.
    ChatSegment {
        #[serde(flatten)]
        context: WebEventContext,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_response: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        segment_index: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        segment_total: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parts: Option<Vec<crate::channel::OutboundContentPart>>,
    },
}

impl WebChannelEvent {
    /// Historical Socket.IO discriminator for this event.
    pub const fn event_name(&self) -> &'static str {
        use WebChannelEvent as E;
        match self {
            E::ChatDone { .. } => "chat_done",
            E::ChatInterim { .. } => "chat_interim",
            E::ChatSuggestions { .. } => "chat_suggestions",
            E::ChatError { .. } => "chat_error",
            E::ChatCancelled { .. } => "chat_cancelled",
            E::TextDelta { .. } => "text_delta",
            E::ThinkingDelta { .. } => "thinking_delta",
            E::ToolArgsDelta { .. } => "tool_args_delta",
            E::InferenceStart { .. } => "inference_start",
            E::InferenceHeartbeat { .. } => "inference_heartbeat",
            E::IterationStart { .. } => "iteration_start",
            E::ContextCompacting { .. } => "context_compacting",
            E::ToolCall { .. } => "tool_call",
            E::ToolProgress { .. } => "tool_progress",
            E::ToolResult { .. } => "tool_result",
            E::TurnCost { .. } => "turn_cost",
            E::SubagentSpawned { .. } => "subagent_spawned",
            E::SubagentCompleted { .. } => "subagent_completed",
            E::SubagentFailed { .. } => "subagent_failed",
            E::SubagentAwaitingUser { .. } => "subagent_awaiting_user",
            E::SubagentIterationStart { .. } => "subagent_iteration_start",
            E::SubagentToolCall { .. } => "subagent_tool_call",
            E::SubagentToolResult { .. } => "subagent_tool_result",
            E::SubagentTextDelta { .. } => "subagent_text_delta",
            E::SubagentThinkingDelta { .. } => "subagent_thinking_delta",
            E::ApprovalRequest { .. } => "approval_request",
            E::ApprovalDecided { .. } => "approval_decided",
            E::PlanReviewRequest { .. } => "plan_review_request",
            E::PlanReviewDecided { .. } => "plan_review_decided",
            E::QueueItemQueued { .. } => "queue_item_queued",
            E::QueueItemDelivered { .. } => "queue_item_delivered",
            E::QueueItemRemoved { .. } => "queue_item_removed",
            E::ThreadGoalUpdated { .. } => "thread_goal_updated",
            E::ThreadGoalCleared { .. } => "thread_goal_cleared",
            E::ThreadTodosChanged { .. } => "thread_todos_changed",
            E::RunModeChanged { .. } => "run_mode_changed",
            E::ArtifactReady { .. } => "artifact_ready",
            E::ArtifactFailed { .. } => "artifact_failed",
            E::ArtifactPending { .. } => "artifact_pending",
            E::MemoryActivity { .. } => "memory_activity",
            E::ExternalTransferPending { .. } => "external_transfer_pending",
            E::ProactiveMessage { .. } => "proactive_message",
            E::VoiceSpeak { .. } => "voice_speak",
            E::ChannelOutbound { .. } => "channel_outbound",
            E::ChannelEvent { .. } => "channel_event",
            E::UserError { .. } => "user_error",
            E::ChatMessage { .. } => "chat_message",
            E::ChatSegment { .. } => "chat_segment",
        }
    }

    /// The shared addressing and ordering context for this event.
    pub fn context(&self) -> &WebEventContext {
        use WebChannelEvent as E;
        match self {
            E::ChatDone { context, .. }
            | E::ChatInterim { context, .. }
            | E::ChatSuggestions { context, .. }
            | E::ChatError { context, .. }
            | E::ChatCancelled { context, .. }
            | E::TextDelta { context, .. }
            | E::ThinkingDelta { context, .. }
            | E::ToolArgsDelta { context, .. }
            | E::InferenceStart { context }
            | E::InferenceHeartbeat { context }
            | E::IterationStart { context, .. }
            | E::ContextCompacting { context, .. }
            | E::ToolCall { context, .. }
            | E::ToolProgress { context, .. }
            | E::ToolResult { context, .. }
            | E::TurnCost { context, .. }
            | E::SubagentSpawned { context, .. }
            | E::SubagentCompleted { context, .. }
            | E::SubagentFailed { context, .. }
            | E::SubagentAwaitingUser { context, .. }
            | E::SubagentIterationStart { context, .. }
            | E::SubagentToolCall { context, .. }
            | E::SubagentToolResult { context, .. }
            | E::SubagentTextDelta { context, .. }
            | E::SubagentThinkingDelta { context, .. }
            | E::ApprovalRequest { context, .. }
            | E::ApprovalDecided { context, .. }
            | E::PlanReviewRequest { context, .. }
            | E::PlanReviewDecided { context, .. }
            | E::QueueItemQueued { context, .. }
            | E::QueueItemDelivered { context, .. }
            | E::QueueItemRemoved { context, .. }
            | E::ThreadGoalUpdated { context, .. }
            | E::ThreadGoalCleared { context, .. }
            | E::ThreadTodosChanged { context, .. }
            | E::RunModeChanged { context, .. }
            | E::ArtifactReady { context, .. }
            | E::ArtifactFailed { context, .. }
            | E::ArtifactPending { context, .. }
            | E::MemoryActivity { context, .. }
            | E::ExternalTransferPending { context, .. }
            | E::ProactiveMessage { context, .. }
            | E::VoiceSpeak { context, .. }
            | E::ChannelOutbound { context, .. }
            | E::ChannelEvent { context, .. }
            | E::UserError { context, .. }
            | E::ChatMessage { context, .. }
            | E::ChatSegment { context, .. } => context,
        }
    }

    /// Mutable access to the shared addressing and ordering context.
    pub fn context_mut(&mut self) -> &mut WebEventContext {
        use WebChannelEvent as E;
        match self {
            E::ChatDone { context, .. }
            | E::ChatInterim { context, .. }
            | E::ChatSuggestions { context, .. }
            | E::ChatError { context, .. }
            | E::ChatCancelled { context, .. }
            | E::TextDelta { context, .. }
            | E::ThinkingDelta { context, .. }
            | E::ToolArgsDelta { context, .. }
            | E::InferenceStart { context }
            | E::InferenceHeartbeat { context }
            | E::IterationStart { context, .. }
            | E::ContextCompacting { context, .. }
            | E::ToolCall { context, .. }
            | E::ToolProgress { context, .. }
            | E::ToolResult { context, .. }
            | E::TurnCost { context, .. }
            | E::SubagentSpawned { context, .. }
            | E::SubagentCompleted { context, .. }
            | E::SubagentFailed { context, .. }
            | E::SubagentAwaitingUser { context, .. }
            | E::SubagentIterationStart { context, .. }
            | E::SubagentToolCall { context, .. }
            | E::SubagentToolResult { context, .. }
            | E::SubagentTextDelta { context, .. }
            | E::SubagentThinkingDelta { context, .. }
            | E::ApprovalRequest { context, .. }
            | E::ApprovalDecided { context, .. }
            | E::PlanReviewRequest { context, .. }
            | E::PlanReviewDecided { context, .. }
            | E::QueueItemQueued { context, .. }
            | E::QueueItemDelivered { context, .. }
            | E::QueueItemRemoved { context, .. }
            | E::ThreadGoalUpdated { context, .. }
            | E::ThreadGoalCleared { context, .. }
            | E::ThreadTodosChanged { context, .. }
            | E::RunModeChanged { context, .. }
            | E::ArtifactReady { context, .. }
            | E::ArtifactFailed { context, .. }
            | E::ArtifactPending { context, .. }
            | E::MemoryActivity { context, .. }
            | E::ExternalTransferPending { context, .. }
            | E::ProactiveMessage { context, .. }
            | E::VoiceSpeak { context, .. }
            | E::ChannelOutbound { context, .. }
            | E::ChannelEvent { context, .. }
            | E::UserError { context, .. }
            | E::ChatMessage { context, .. }
            | E::ChatSegment { context, .. } => context,
        }
    }
}
