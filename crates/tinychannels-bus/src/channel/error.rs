//! Send error taxonomy shared across adapters.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Machine-readable send failure categories.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SendErrorKind {
    TooLong,
    BadFormat,
    Forbidden,
    NotFound,
    RateLimited,
    Transient,
    #[default]
    Unknown,
}

/// Structured adapter send failure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelSendError {
    pub kind: SendErrorKind,
    pub message: String,
    /// True only when the adapter intentionally does not expose this operation.
    pub unsupported: bool,
    pub retryable: bool,
    /// Provider I/O may have reached the remote service despite the error.
    pub sent_before_error: bool,
    pub retry_after: Option<f64>,
    pub chat_level_not_found: bool,
    pub continuation_message_ids: Vec<String>,
    pub partial_overflow: Option<Value>,
}

impl Default for ChannelSendError {
    fn default() -> Self {
        Self {
            kind: SendErrorKind::Unknown,
            message: String::new(),
            unsupported: false,
            retryable: false,
            sent_before_error: false,
            retry_after: None,
            chat_level_not_found: false,
            continuation_message_ids: Vec::new(),
            partial_overflow: None,
        }
    }
}

impl ChannelSendError {
    const UNSUPPORTED_PREFIX: &'static str = "unsupported operation: ";

    /// Build an explicit unsupported-operation error without changing the
    /// public struct layout used by existing Rust callers.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: SendErrorKind::BadFormat,
            message: format!("{}{}", Self::UNSUPPORTED_PREFIX, message.into()),
            unsupported: true,
            ..Default::default()
        }
    }

    /// Whether this error was produced by [`Self::unsupported`].
    pub fn is_unsupported(&self) -> bool {
        self.kind == SendErrorKind::BadFormat && self.message.starts_with(Self::UNSUPPORTED_PREFIX)
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
