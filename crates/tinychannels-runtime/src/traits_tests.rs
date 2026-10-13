use super::*;

#[test]
fn channel_message_clone_preserves_fields() {
    let message = ChannelMessage {
        id: "42".into(),
        sender: "alice".into(),
        reply_target: "alice".into(),
        content: "ping".into(),
        channel: "dummy".into(),
        timestamp: 999,
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
    let message = SendMessage::new("hello", "alice")
        .in_thread(Some("thread-1".to_string()))
        .with_deterministic_idempotency_key("telegram");
    let again = SendMessage::new("hello", "alice")
        .in_thread(Some("thread-1".to_string()))
        .with_deterministic_idempotency_key("telegram");

    assert_eq!(message.idempotency_key, again.idempotency_key);
    assert!(
        message
            .idempotency_key
            .as_deref()
            .unwrap()
            .starts_with("legacy-send:telegram:")
    );
}

#[test]
fn explicit_send_message_idempotency_key_is_preserved() {
    let message = SendMessage::new("hello", "alice")
        .with_idempotency_key("caller-key")
        .with_deterministic_idempotency_key("telegram");

    assert_eq!(message.idempotency_key.as_deref(), Some("caller-key"));
}
