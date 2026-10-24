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
        parts: Vec<crate::channel::OutboundContentPart>,
        receipt: Option<crate::channel::MessageReceipt>,
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
    Subagent(Box<SubagentOutput>),
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
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        iteration: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_iterations: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stop: Option<TurnStopSummary>,
    },
    Native {
        data: Value,
    },
}

/// Authorized subagent progress fields carried together in one boxed variant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubagentOutput {
    pub agent_id: String,
    pub task_id: String,
    pub state: String,
    pub mode: Option<String>,
    pub dedicated_thread: Option<bool>,
    pub parent_call_id: Option<String>,
    pub call_id: Option<String>,
    pub iteration: Option<u32>,
    pub max_iterations: Option<u32>,
    pub prompt_chars: Option<usize>,
    pub message: Option<String>,
    pub display_name: Option<String>,
    pub tool_name: Option<String>,
    pub arguments: Option<Value>,
    pub success: Option<bool>,
    pub elapsed_ms: Option<u64>,
    pub output_chars: Option<usize>,
    pub output: Option<String>,
    pub structured: Option<Value>,
    pub usage: Option<SubagentUsage>,
    pub failure: Option<ToolFailureSummary>,
    pub display_label: Option<String>,
    pub display_detail: Option<String>,
    pub worker_thread_id: Option<String>,
    pub worktree_path: Option<String>,
    pub changed_files: Vec<String>,
    pub dirty_status: Option<bool>,
    pub stop: Option<TurnStopSummary>,
    pub extended_policy: Option<bool>,
}

/// Transport-safe projection of a classified tool failure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
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
///
/// A handle belongs to the private runtime that opened it.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ProgressiveCommand {
    Open {
        channel: String,
    },
    /// Open a turn-scoped output stream using the full configured address.
    OpenOutput {
        channel: crate::channel::ChannelRef,
        conversation: crate::channel::ConversationRef,
        turn_id: String,
        /// Host-owned output lease in milliseconds; the host selects a value
        /// that covers its turn backstop and closes this handle on completion.
        lifetime_ms: u64,
    },
    /// Open a scoped output stream whose transport is the trusted host callback
    /// captured by this compiled module runtime. Unlike `OpenOutput`, this
    /// does not select a locally configured provider; the host callback owns
    /// account authorization and delivery. Capabilities remain conservative.
    OpenHostedOutput {
        channel: crate::channel::ChannelRef,
        conversation: crate::channel::ConversationRef,
        turn_id: String,
        /// Host-owned output lease in milliseconds.
        lifetime_ms: u64,
    },
    /// Open a handle bound to one provider target and host-authorized turn.
    /// Subsequent `FeedOutput` requests carry a host-assigned `sequence` and
    /// must repeat the same turn scope.
    OpenScoped {
        channel: String,
        turn_id: Option<String>,
        channel_ref: Option<Box<crate::channel::ChannelRef>>,
        conversation: Option<Box<crate::channel::ConversationRef>>,
    },
    Feed {
        handle: String,
        events: Vec<ProgressiveEvent>,
    },
    /// Feed canonical output events without converting them through the v1
    /// four-variant progressive projection.
    ///
    /// The optional scope fields assert the handle's turn scope: a request
    /// whose scope differs from the scope bound at open (or, for an unscoped
    /// `Open`, by the first accepted batch) is rejected. `sequence` is the
    /// first event's stable position within `turn_id`, so a host can retry the
    /// same batch without changing its delivery identity. It is required for
    /// handles from `Open`/`OpenScoped` and must be absent for module-sequenced
    /// handles from `OpenOutput`/`OpenHostedOutput`.
    FeedOutput {
        handle: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        channel_ref: Option<Box<crate::channel::ChannelRef>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conversation: Option<Box<crate::channel::ConversationRef>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ProgressiveResult {
    Opened {
        handle: String,
    },
    Updated,
    /// Canonical output events were observed; typed terminal delivery results
    /// are returned in event order and retain receipts and partial failures.
    /// Failed output-event callbacks are reported by sequence without
    /// discarding provider delivery results.
    OutputUpdated {
        deliveries: Vec<ProgressiveTransportResult>,
        #[serde(default)]
        output_event_failures: Vec<u64>,
    },
    Closed,
    /// V2 terminal delivery acknowledgement; carries every known platform
    /// receipt and any partial-send failure from the provider.
    DeliveryOutcome {
        outcome: Box<crate::channel::ChannelSendOutcome>,
    },
    /// Rich content is retained for a host/provider port that does not yet
    /// support it; it is never flattened to text or dropped.
    DeliveryUnsupported {
        parts: Vec<crate::channel::OutboundContentPart>,
    },
    DeliveryOutcomes {
        outcomes: Vec<crate::channel::ChannelSendOutcome>,
    },
    DeliveryBatch {
        outcomes: Vec<crate::channel::ChannelSendOutcome>,
        unsupported: Vec<crate::channel::OutboundContentPart>,
        failure: Option<ProgressiveOutputFailure>,
        /// Hosted output accepted by its callback. This is not a provider
        /// acknowledgement and carries no provider receipt.
        #[serde(default)]
        hosted_accepted: bool,
    },
}

/// Content-free reason that an ordered output batch stopped before completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProgressiveOutputFailure {
    ObservationCallback,
    ProviderTransport,
}

/// The host supplies its managed REST transport and captured credential.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ProgressiveTransportRequest {
    /// Ordered, host-authorized projection of one canonical turn event.
    OutputEvent {
        channel: String,
        turn_id: Option<String>,
        channel_ref: Option<crate::channel::ChannelRef>,
        conversation: Option<crate::channel::ConversationRef>,
        sequence: u64,
        event: Box<ChannelOutputEvent>,
    },
    Send {
        channel: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_ref: Option<crate::channel::ChannelRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        conversation: Option<crate::channel::ConversationRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
        message: Value,
    },
    /// Provider-facing typed delivery. Observation callbacks are separate and
    /// cannot acknowledge or replace this send.
    SendTyped {
        channel: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_ref: Option<crate::channel::ChannelRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        conversation: Option<crate::channel::ConversationRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        segment_index: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
        parts: Vec<crate::channel::OutboundContentPart>,
        text: String,
    },
    Edit {
        channel: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_ref: Option<crate::channel::ChannelRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        conversation: Option<crate::channel::ConversationRef>,
        message_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        receipt: Option<crate::channel::MessageReceipt>,
        text: String,
    },
    Delete {
        channel: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_ref: Option<crate::channel::ChannelRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        conversation: Option<crate::channel::ConversationRef>,
        message_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        receipt: Option<crate::channel::MessageReceipt>,
    },
    Typing {
        channel: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_ref: Option<crate::channel::ChannelRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        conversation: Option<crate::channel::ConversationRef>,
    },
}

/// Typed transport facts preserve the module's existing recovery rules.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ProgressiveTransportResult {
    /// Observation callback accepted the event; this does not acknowledge a
    /// provider send.
    Acknowledged,
    /// Trusted hosted callback accepted typed output for delivery, while the
    /// provider receipt is delivered asynchronously on another event path.
    /// This is not a provider receipt and is rejected for native sends.
    Accepted,
    Success(Value),
    /// Structured result for hosts that can preserve normalized receipts and
    /// partial-send failure facts across the module boundary.
    Outcome(Box<crate::channel::ChannelSendOutcome>),
    /// The provider adapter explicitly does not support this typed payload.
    UnsupportedContent,
    Unavailable,
    EditUnsupported,
    MessageGone,
    Failed,
}

#[cfg(test)]
#[path = "module_progressive_tests.rs"]
mod tests;
