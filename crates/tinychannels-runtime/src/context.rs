//! Shared channel runtime helpers.

use crate::text::truncate_with_ellipsis;
use crate::traits::ChannelMessage;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Per-sender conversation history for channel messages.
pub type ConversationHistoryMap = Arc<Mutex<HashMap<String, Vec<ChatMessage>>>>;
pub use tinychannels_bus::context::*;

#[async_trait::async_trait]
pub trait Memory: Send + Sync {
    async fn recall(&self, query: &str, limit: usize) -> anyhow::Result<Vec<MemoryEntry>>;
}

pub fn effective_channel_message_timeout_secs(configured: u64) -> u64 {
    configured.max(MIN_CHANNEL_MESSAGE_TIMEOUT_SECS)
}

pub fn conversation_memory_key(msg: &ChannelMessage) -> String {
    [msg.channel.as_str(), msg.sender.as_str(), msg.id.as_str()]
        .iter()
        .map(|part| crate::channel::session::encode_identity_component(part, &['_', '%']))
        .collect::<Vec<_>>()
        .join("_")
}

pub fn conversation_history_key(msg: &ChannelMessage) -> String {
    conversation_history_key_parts(
        &msg.channel,
        &msg.sender,
        &msg.reply_target,
        msg.thread_ts.as_deref(),
    )
}

/// [`conversation_history_key`] from individual fields, for callers that see
/// an inbound message as event fields rather than a [`ChannelMessage`].
pub fn conversation_history_key_parts(
    channel: &str,
    sender: &str,
    reply_target: &str,
    thread_ts: Option<&str>,
) -> String {
    let base_key = [channel, sender, reply_target]
        .iter()
        .map(|part| crate::channel::session::encode_identity_component(part, &['_', '%']))
        .collect::<Vec<_>>()
        .join("_");
    // Some providers (Telegram topics) use thread_ts as a reply target, not as
    // a distinct conversation boundary.
    if crate::capabilities::capabilities_for(channel).history_key_ignores_thread {
        return base_key;
    }
    if let Some(thread_ts) = thread_ts {
        let thread_ts = thread_ts.trim();
        if !thread_ts.is_empty() {
            return format!(
                "{base_key}_thread:{}",
                crate::channel::session::encode_identity_component(thread_ts, &['_', '%', ':'])
            );
        }
    }
    base_key
}

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
                truncate_with_ellipsis(&turn.content, CHANNEL_HISTORY_COMPACT_CONTENT_CHARS);
        }
    }

    *turns = compacted;
    true
}

pub fn clear_sender_history(histories: &ConversationHistoryMap, sender_key: &str) {
    histories
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(sender_key);
}

pub fn compact_sender_history(histories: &ConversationHistoryMap, sender_key: &str) -> bool {
    let mut histories = histories.lock().unwrap_or_else(|e| e.into_inner());
    let Some(turns) = histories.get_mut(sender_key) else {
        return false;
    };
    compact_history(turns)
}

pub fn should_skip_memory_context_entry(key: &str, content: &str) -> bool {
    if key.trim().to_ascii_lowercase().ends_with("_history") {
        return true;
    }

    content.chars().count() > MEMORY_CONTEXT_MAX_CHARS
}

/// Whether an error message says the request exceeded the model's context
/// window (the prompt or conversation is too long for the model).
///
/// A deterministic usage condition, not a transient fault: retrying the same
/// oversized request cannot help. The match is status-agnostic (providers
/// disagree on the HTTP code) and anchored in two tiers so a retryable error is
/// never marked permanent:
///
/// - **Length/context phrases** only describe request-size overflow and match
///   on their own, including the LM Studio / llama.cpp un-evictable-prefix body
///   (`n_keep: N >= n_ctx: M`, which needs both tokens).
/// - **Token-count phrases** ("too many tokens", "token limit exceeded") collide
///   with per-minute token *rate* limits, which are transient. They count as
///   overflow only when no rate-limit marker is present.
///
/// This is a deliberate copy of `is_context_window_exceeded_message` in
/// tinyinference's `crates/tinyinference-llm/src/failure.rs`, and its match
/// list must track that function. It is duplicated rather than imported
/// because the channel runtime must not depend on the inference stack. When a
/// provider phrasing is added there, add it here with the same test string.
pub fn is_context_window_overflow_message(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();

    const CONTEXT_HINTS: &[&str] = &[
        "exceeds the context window",
        "context window of this model",
        "maximum context length",
        "context length exceeded",
        "context size has been exceeded",
        "prompt is too long",
        "input is too long",
        "greater than the context length",
        // Alibaba / DashScope (Qwen): `"Range of input length should be
        // [1, 98304]"` — the window is the range's upper bound.
        "range of input length should be",
    ];
    if CONTEXT_HINTS.iter().any(|hint| lower.contains(hint)) {
        return true;
    }

    if has_un_evictable_prefix_marker(&lower) {
        return true;
    }

    const TOKEN_HINTS: &[&str] = &["too many tokens", "token limit exceeded"];
    if TOKEN_HINTS.iter().any(|hint| lower.contains(hint)) {
        const RATE_LIMIT_MARKERS: &[&str] = &[
            "per minute",
            "per min",
            "per second",
            "per sec",
            "per hour",
            "per day",
            "per month",
            "rate limit",
            "rate_limit",
            "tpm",
            "requests per",
            "retry after",
            "try again in",
        ];
        return !RATE_LIMIT_MARKERS
            .iter()
            .any(|marker| lower.contains(marker));
    }

    false
}

fn has_un_evictable_prefix_marker(message: &str) -> bool {
    let Some(keep) = parse_number_after(message, "n_keep:") else {
        return false;
    };
    let Some(ctx_marker) = message.find("n_ctx:") else {
        return false;
    };
    let Some(keep_end) = message.find("n_keep:").map(|index| index + "n_keep:".len()) else {
        return false;
    };
    if ctx_marker < keep_end || !message[keep_end..ctx_marker].contains(">=") {
        return false;
    }
    let Some(ctx) = parse_number_after(&message[ctx_marker..], "n_ctx:") else {
        return false;
    };
    keep >= ctx
}

fn parse_number_after(message: &str, marker: &str) -> Option<u64> {
    let start = message.find(marker)? + marker.len();
    let digits = message[start..]
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

pub async fn build_memory_context(
    mem: &dyn Memory,
    user_msg: &str,
    min_relevance_score: f64,
) -> String {
    let mut context = String::new();

    if let Ok(entries) = mem.recall(user_msg, 5).await {
        let mut included = 0usize;
        let mut used_chars = 0usize;

        for entry in entries.iter().filter(|e| match e.score {
            Some(score) => score >= min_relevance_score,
            None => true,
        }) {
            if included >= MEMORY_CONTEXT_MAX_ENTRIES {
                break;
            }

            if should_skip_memory_context_entry(&entry.key, &entry.content) {
                continue;
            }

            let content = if entry.content.chars().count() > MEMORY_CONTEXT_ENTRY_MAX_CHARS {
                truncate_with_ellipsis(&entry.content, MEMORY_CONTEXT_ENTRY_MAX_CHARS)
            } else {
                entry.content.clone()
            };

            let line = format!("- {}: {}\n", entry.key, content);
            let line_chars = line.chars().count();
            if used_chars + line_chars > MEMORY_CONTEXT_MAX_CHARS {
                break;
            }

            if included == 0 {
                context.push_str("[Memory context]\n");
            }

            context.push_str(&line);
            used_chars += line_chars;
            included += 1;
        }

        if included > 0 {
            context.push('\n');
        }
    }

    context
}

#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;
