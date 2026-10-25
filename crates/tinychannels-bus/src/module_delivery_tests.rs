use super::*;
use crate::DeliveryDurability;

#[test]
fn new_pending_record_starts_with_empty_retry_state() {
    let entry = DeliveryQueueEntry::new(
        "op-1",
        ChannelOutboundIntent {
            idempotency_key: "op-1".into(),
            turn_id: None,
            durability: DeliveryDurability::Required,
            ..Default::default()
        },
        42,
    );

    assert_eq!(entry.id, "op-1");
    assert_eq!(entry.enqueued_at_unix_ms, 42);
    assert_eq!(entry.retry_count, 0);
    assert!(entry.last_attempt_at_unix_ms.is_none());
    assert!(entry.last_error.is_none());
    assert!(entry.platform_send_started_at_unix_ms.is_none());
    assert!(entry.recovery_state.is_none());
}

#[test]
fn delivery_records_round_trip_without_losing_retry_or_intent_fields() {
    let record = DeliveryQueueEntry {
        id: "op-1".into(),
        intent: ChannelOutboundIntent {
            idempotency_key: "op-1".into(),
            channel_id: "telegram".into(),
            conversation_id: "room-2".into(),
            channel_ref: None,
            conversation_ref: None,
            thread_id: Some("thread-3".into()),
            turn_id: None,
            durability: DeliveryDurability::Required,
            ..Default::default()
        },
        enqueued_at_unix_ms: 42,
        retry_count: 2,
        last_attempt_at_unix_ms: Some(40),
        last_error: Some("retryable".into()),
        platform_send_started_at_unix_ms: Some(39),
        recovery_state: Some(DeliveryRecoveryState::UnknownAfterSend),
        partial_receipt: None,
    };
    let wire = serde_json::to_vec(&record).unwrap();
    assert!(wire.len() <= DELIVERY_RECORD_BYTES);
    assert_eq!(
        serde_json::from_slice::<DeliveryQueueEntry>(&wire).unwrap(),
        record
    );
}

#[test]
fn terminal_result_replays_only_the_operation_success_facts() {
    let record = DeliveryTerminalResult {
        id: "op-1".into(),
        intent_fingerprint: "a".repeat(64),
        completed_at_unix_ms: 900,
        receipt: None,
    };
    let value = serde_json::to_value(&record).unwrap();
    assert_eq!(value["id"], "op-1");
    assert_eq!(value["completedAtUnixMs"], 900);
    assert!(value.get("intent").is_none());
}
