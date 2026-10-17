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

}

/// Cost provenance copied as a wire enum to avoid depending on the harness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WebCostSource {
    Charged,
    Estimated,
    #[default]
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
        #[serde(skip_serializing_if = "Option::is_none")]
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
        #[serde(skip_serializing_if = "Option::is_none")]
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
