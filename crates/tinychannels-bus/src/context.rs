//! Shared channel context vocabulary and the pure per-sender history helpers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

/// Per-sender conversation history for channel messages.
pub type ConversationHistoryMap = Arc<Mutex<HashMap<String, Vec<ChatMessage>>>>;

fn truncate_chars_with_ellipsis(input: &str, max_chars: usize) -> String {
    let mut iter = input.chars();
    let mut out = String::new();
    for _ in 0..max_chars {
        let Some(ch) = iter.next() else {
            return input.to_string();
        };
        out.push(ch);
    }
    if iter.next().is_some() {
        out.push('\u{2026}');
    }
    out
}

/// Keep only the most recent turns and truncate long turn content.
/// Returns `false` when there was nothing to compact.
pub fn compact_history(turns: &mut Vec<ChatMessage>) -> bool {
    if turns.is_empty() {
        return false;
    }

    let keep_from = turns
        .len()
        .saturating_sub(CHANNEL_HISTORY_COMPACT_KEEP_MESSAGES);
    let mut compacted = turns[keep_from..].to_vec();

    for turn in &mut compacted {
        if turn.content.chars().count() > CHANNEL_HISTORY_COMPACT_CONTENT_CHARS {
            turn.content =
                truncate_chars_with_ellipsis(&turn.content, CHANNEL_HISTORY_COMPACT_CONTENT_CHARS);
        }
    }

    *turns = compacted;
    true
}

/// Drop one sender's history.
pub fn clear_sender_history(histories: &ConversationHistoryMap, sender_key: &str) {
    histories
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(sender_key);
}

/// Compact one sender's history in place; `false` when the sender has none.
pub fn compact_sender_history(histories: &ConversationHistoryMap, sender_key: &str) -> bool {
    let mut histories = histories.lock().unwrap_or_else(|e| e.into_inner());
    let Some(turns) = histories.get_mut(sender_key) else {
        return false;
    };
    compact_history(turns)
}

/// Log line for a crashed channel message worker, so every host reports a
/// join failure identically. The join itself is runtime-owned; this crate
/// stays free of async runtimes.
pub fn worker_crash_message(error: &dyn std::fmt::Display) -> String {
    format!("Channel message worker crashed: {error}")
}

#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;
