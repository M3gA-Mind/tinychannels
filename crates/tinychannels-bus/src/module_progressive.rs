//! Stateful progressive reply vocabulary; delivery choreography stays in the module.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Ordered facts projected from one host-owned agent turn.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum ProgressiveEvent {
    TextDelta(String),
    ThinkingDelta(String),
    ToolCall(String),
    ToolResult { tool_name: String, success: bool },
}

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
