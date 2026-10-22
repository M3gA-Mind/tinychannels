//! Implementation helpers for channel session.
use crate::channel::{
    ChannelInboundEnvelope, ChannelRef, ConversationKind, ConversationRef, SenderRef,
};
use crate::traits::ChannelMessage;
pub use tinychannels_bus::channel::session::*;

/// Build a deterministic TinyChannels session key.
pub fn build_session_key(
    namespace: &str,
    channel: &ChannelRef,
    conversation: &ConversationRef,
    sender: &SenderRef,
    policy: SessionKeyPolicy,
) -> String {
    let namespace = normalize_namespace(namespace);
    let account = channel.account_id.as_deref().unwrap_or("default");
    let chat_type = conversation.kind.as_session_segment();
    let mut parts = vec![
        encode_identity_component(namespace, &[':', '%']),
        encode_identity_component(&channel.id, &[':', '%']),
        encode_identity_component(account, &[':', '%']),
        encode_identity_component(chat_type, &[':', '%']),
        encode_identity_component(&conversation.id, &[':', '%']),
    ];

    if let Some(scope_id) = conversation.scope_id.as_deref().filter(|s| !s.is_empty()) {
        parts.insert(4, encode_identity_component(scope_id, &[':', '%']));
    }

    let thread = conversation
        .topic_id
        .as_deref()
        .and_then(nonempty)
        .or_else(|| conversation.thread_id.as_deref().and_then(nonempty));
    if let Some(thread) = thread {
        parts.push(encode_identity_component(thread, &[':', '%']));
    }

    let participant = sender
        .alt_ids
        .first()
        .map(String::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| (!sender.id.is_empty()).then_some(sender.id.as_str()));
    let isolate_user = match conversation.kind {
        ConversationKind::Dm => false,
        _ if thread.is_some() => policy.thread_sessions_per_user,
        _ => policy.group_sessions_per_user,
    };
    if isolate_user && let Some(participant) = participant {
        parts.push(encode_identity_component(participant, &[':', '%']));
    }

    parts.join(":")
}

/// Build a deterministic TinyChannels session key from a normalized inbound envelope.
pub fn build_session_key_for_inbound_envelope(
    namespace: &str,
    envelope: &ChannelInboundEnvelope,
    policy: SessionKeyPolicy,
) -> String {
    build_session_key(
        namespace,
        &envelope.channel,
        &envelope.conversation,
        &envelope.sender,
        policy,
    )
}

/// Return legacy OpenHuman keys for migration lookup before writing a new key.
pub fn conversation_history_key_candidates(msg: &ChannelMessage) -> LegacySessionKeys {
    // Keep these in the original OpenHuman format: callers use them to find
    // data written before current identity keys began escaping separators.
    let base_key = format!("{}_{}_{}", msg.channel, msg.sender, msg.reply_target);
    let conversation_history_key =
        if crate::context::legacy_v21_history_key_ignores_thread(&msg.channel) {
            base_key
        } else if let Some(thread_ts) = msg
            .thread_ts
            .as_deref()
            .map(str::trim)
            .filter(|thread_ts| !thread_ts.is_empty())
        {
            format!("{base_key}_thread:{thread_ts}")
        } else {
            base_key
        };

    LegacySessionKeys {
        conversation_history_key,
        conversation_memory_key: format!("{}_{}_{}", msg.channel, msg.sender, msg.id),
    }
}

/// Derive a stable host-local thread key from inbound channel facts.
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

/// Derive a stable client identifier for an inbound channel sender.
pub fn derive_inbound_client_id(channel: &str, sender: Option<&str>) -> String {
    let channel = channel.trim();
    match sender.map(str::trim).filter(|sender| !sender.is_empty()) {
        Some(sender) if !channel.is_empty() => format!(
            "inbound:{}:{}",
            encode_identity_component(channel, &[':', '%']),
            encode_identity_component(sender, &[':', '%'])
        ),
        Some(sender) => format!("inbound:{}", encode_identity_component(sender, &[':', '%'])),
        None => "inbound".to_string(),
    }
}

pub(crate) fn encode_identity_component(value: &str, separators: &[char]) -> String {
    let mut encoded = String::with_capacity(value.len());
    for ch in value.chars() {
        let separator = ch == '%' || separators.contains(&ch);
        if separator {
            use std::fmt::Write as _;
            write!(encoded, "%{:02X}", ch as u32).expect("writing to a String cannot fail");
        } else {
            encoded.push(ch);
        }
    }
    encoded
}

fn normalize_namespace(namespace: &str) -> &str {
    match namespace.trim() {
        "" | "default" => "main",
        value => value,
    }
}

fn nonempty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}
