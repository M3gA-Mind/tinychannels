use super::*;

#[test]
fn stored_delivery_roundtrip_preserves_v22_scope_turn_and_receipts() {
    let intent = ChannelOutboundIntent {
        idempotency_key: "idem-1".into(),
        channel_id: "telegram".into(),
        conversation_id: "chat-1".into(),
        channel_ref: Some(ChannelRef {
            id: "telegram".into(),
            account_id: Some("bot-a".into()),
        }),
        conversation_ref: Some(ConversationRef {
            kind: crate::channel::ConversationKind::Group,
            id: "chat-1".into(),
            scope_id: Some("tenant-a".into()),
            parent_id: Some("parent".into()),
            thread_id: Some("thread-a".into()),
            topic_id: Some("topic-a".into()),
        }),
        reply_to_id: Some("reply-1".into()),
        thread_id: Some("thread-a".into()),
        turn_id: Some("turn-a".into()),
        durability: DeliveryDurability::Required,
        payload: OutboundPayload::Text {
            text: "hello".into(),
        },
    };
    let receipt = MessageReceipt {
        idempotency_key: Some("idem-1".into()),
        channel: intent.channel_ref.clone(),
        conversation: intent.conversation_ref.clone(),
        primary_platform_message_id: Some("message-1".into()),
        platform_message_ids: vec!["message-1".into()],
        thread_id: Some("thread-a".into()),
        sent_at: 123,
        ..MessageReceipt::default()
    };
    let entry = QueuedDelivery {
        id: "idem-1".into(),
        intent,
        enqueued_at_unix_ms: 456,
        partial_receipt: Some(receipt.clone()),
        ..QueuedDelivery::default()
    };

    let stored = StoredDelivery::from(&entry);
    let stored: StoredDelivery =
        serde_json::from_value(serde_json::to_value(&stored).unwrap()).unwrap();
    let decoded = QueuedDelivery::from(stored);
    assert_eq!(decoded, entry);

    let result = DeliveryTerminalResult {
        id: "idem-1".into(),
        intent_fingerprint: "sha256:abc".into(),
        completed_at_unix_ms: 789,
        receipt: Some(receipt),
    };
    let stored = StoredResult::from(&result);
    let stored: StoredResult =
        serde_json::from_value(serde_json::to_value(&stored).unwrap()).unwrap();
    let decoded = DeliveryTerminalResult::from(stored);
    assert_eq!(decoded, result);
}

#[test]
fn stored_delivery_records_read_legacy_payloads_without_v22_fields() {
    let intent: StoredIntent = serde_json::from_value(serde_json::json!({
        "idempotency_key": "legacy",
        "channel_id": "telegram",
        "conversation_id": "chat",
        "reply_to_id": null,
        "thread_id": null,
        "durability": "best_effort",
        "payload": { "kind": "text", "text": "hello" }
    }))
    .unwrap();
    assert!(intent.channel_ref.is_none());
    assert!(intent.conversation_ref.is_none());
    assert!(intent.turn_id.is_none());

    let delivery: StoredDelivery = serde_json::from_value(serde_json::json!({
        "intent": serde_json::to_value(intent).unwrap(),
        "enqueued_ms": 1,
        "retry_count": 0,
        "last_attempt_ms": null,
        "last_error": null,
        "platform_send_started_ms": null,
        "recovery_state": null
    }))
    .unwrap();
    assert!(delivery.partial_receipt.is_none());

    let result: StoredResult = serde_json::from_value(serde_json::json!({
        "id": "legacy",
        "intent_fingerprint": "old",
        "completed_ms": 2
    }))
    .unwrap();
    assert!(result.receipt.is_none());
}
