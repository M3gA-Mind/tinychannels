//! Session-key construction and legacy key compatibility.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Session key isolation policy, matching Hermes' default toggles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct SessionKeyPolicy {
    pub group_sessions_per_user: bool,
    pub thread_sessions_per_user: bool,
}

impl Default for SessionKeyPolicy {
    fn default() -> Self {
        Self {
            group_sessions_per_user: true,
            thread_sessions_per_user: false,
        }
    }
}

/// Legacy OpenHuman key candidates that may need lookup during migration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct LegacySessionKeys {
    pub conversation_history_key: String,
    pub conversation_memory_key: String,
}

/// Derive the stable host-local thread key used for legacy channel messages.
///
/// This is identity vocabulary rather than provider runtime behavior, so hosts
/// can derive the same key without linking the implementation crate.
pub fn derive_inbound_thread_id(
    channel: &str,
    sender: Option<&str>,
    reply_target: Option<&str>,
    thread_ts: Option<&str>,
) -> String {
    let mut key = format!(
        "channel:{}",
        encode_identity_component(channel, &['/', '#', '%'])
    );
    if let Some(sender) = sender.and_then(nonempty) {
        key.push('/');
        key.push_str(&encode_identity_component(sender, &['/', '#', '%']));
    }
    if let Some(reply_target) = reply_target.and_then(nonempty) {
        key.push('/');
        key.push_str(&encode_identity_component(reply_target, &['/', '#', '%']));
    }
    let provider = channel.split(':').next().unwrap_or("");
    if !matches!(provider, "telegram" | "tg")
        && let Some(thread_ts) = thread_ts.and_then(nonempty)
    {
        key.push_str("#thread:");
        key.push_str(&encode_identity_component(thread_ts, &['/', '#', '%']));
    }
    key
}

fn encode_identity_component(value: &str, separators: &[char]) -> String {
    let mut encoded = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch == '%' || separators.contains(&ch) {
            use std::fmt::Write as _;
            write!(encoded, "%{:02X}", ch as u32).expect("writing to a String cannot fail");
        } else {
            encoded.push(ch);
        }
    }
    encoded
}

fn nonempty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
