//! Shared channel context vocabulary.

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct ChannelRouteSelection {
    pub provider: String,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

impl ChatMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".into(),
            content: content.into(),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".into(),
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryEntry {
    pub key: String,
    pub content: String,
    pub score: Option<f64>,
}

/// Maximum history messages to keep per sender.
pub const MAX_CHANNEL_HISTORY: usize = 50;

pub const DEFAULT_CHANNEL_INITIAL_BACKOFF_SECS: u64 = 2;
pub const DEFAULT_CHANNEL_MAX_BACKOFF_SECS: u64 = 60;
pub const MIN_CHANNEL_MESSAGE_TIMEOUT_SECS: u64 = 30;
pub const CHANNEL_MESSAGE_TIMEOUT_SECS: u64 = 300;
pub const CHANNEL_PARALLELISM_PER_CHANNEL: usize = 4;
pub const CHANNEL_MIN_IN_FLIGHT_MESSAGES: usize = 8;
pub const CHANNEL_MAX_IN_FLIGHT_MESSAGES: usize = 64;
pub const CHANNEL_TYPING_REFRESH_INTERVAL_SECS: u64 = 4;
pub const MEMORY_CONTEXT_MAX_ENTRIES: usize = 4;
pub const MEMORY_CONTEXT_ENTRY_MAX_CHARS: usize = 800;
pub const MEMORY_CONTEXT_MAX_CHARS: usize = 4_000;
pub const CHANNEL_HISTORY_COMPACT_KEEP_MESSAGES: usize = 12;
pub const CHANNEL_HISTORY_COMPACT_CONTENT_CHARS: usize = 600;
