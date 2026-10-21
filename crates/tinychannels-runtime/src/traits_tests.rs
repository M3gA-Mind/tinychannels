use super::*;
use crate::traits::{ChannelMessage, SendMessage};

#[test]
fn channel_message_clone_preserves_legacy_wire_fields() {
    let message = ChannelMessage {
        id: "42".into(),
        sender: "alice".into(),
        reply_target: "alice".into(),
        content: "ping".into(),
        channel: "dummy".into(),
        timestamp: 999,
        topic_id: None,
        reply_to_id: None,
        thread_ts: None,
        sender_name: None,
    };

    let cloned = message.clone();
    assert_eq!(cloned.id, "42");
    assert_eq!(cloned.sender, "alice");
    assert_eq!(cloned.reply_target, "alice");
    assert_eq!(cloned.content, "ping");
    assert_eq!(cloned.channel, "dummy");
    assert_eq!(cloned.timestamp, 999);
}

#[test]
fn send_message_generates_deterministic_idempotency_key() {
    let message = SendMessage::new("hello", "alice").in_thread(Some("thread-1".to_string()));
    let again = SendMessage::new("hello", "alice").in_thread(Some("thread-1".to_string()));
    let intent = channel::outbound_intent_from_send_message("telegram", &message);
    let repeated = channel::outbound_intent_from_send_message("telegram", &again);

    assert_eq!(intent.idempotency_key, repeated.idempotency_key);
    assert!(intent.idempotency_key.starts_with("legacy-send:telegram:"));
}

#[test]
fn explicit_send_message_idempotency_key_is_preserved() {
    let message = SendMessage::new("hello", "alice")
        .with_idempotency_key("caller-key");

    let intent = channel::outbound_intent_from_send_message("telegram", &message);
    assert_eq!(intent.idempotency_key, "caller-key");
}

#[test]
fn v1_send_message_is_immediately_converted_to_a_scoped_typed_intent() {
    let message = SendMessage::new("hello", "alice")
        .in_thread(Some("thread-1".to_string()))
        .with_idempotency_key("caller-key");

    let intent = channel::outbound_intent_from_send_message("telegram", &message);
    assert_eq!(intent.channel_id, "telegram");
    assert_eq!(intent.conversation_id, "alice");
    assert_eq!(intent.idempotency_key, "caller-key");
    assert_eq!(intent.thread_id.as_deref(), Some("thread-1"));
    assert_eq!(
        intent.conversation_ref.as_ref().and_then(|scope| scope.thread_id.as_deref()),
        Some("thread-1")
    );
    assert!(matches!(
        intent.payload,
        channel::OutboundPayload::NativeChannelData { .. }
    ));
}
