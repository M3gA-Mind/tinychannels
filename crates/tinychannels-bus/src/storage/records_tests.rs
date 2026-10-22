use super::*;
use crate::channel::{ChannelRef, ConversationKind, ConversationRef, MessageReceipt};
use crate::module_delivery::{DeliveryQueueEntry, DeliveryTerminalResult};

#[test]
fn stored_delivery_round_trip_preserves_v22_scope_and_partial_receipt() {
    let receipt = MessageReceipt {
        primary_platform_message_id: Some("part-1".into()),
        platform_message_ids: vec!["part-1".into()],
        ..MessageReceipt::default()
    };
    let entry = DeliveryQueueEntry {
        intent: ChannelOutboundIntent {
            turn_id: Some("turn-42".into()),
            channel_ref: Some(ChannelRef {
                id: "telegram".into(),
                account_id: Some("account".into()),
            }),
            conversation_ref: Some(ConversationRef {
                kind: ConversationKind::Thread,
                id: "room".into(),
                thread_id: Some("thread".into()),
                ..ConversationRef::default()
            }),
            ..ChannelOutboundIntent::default()
        },
        partial_receipt: Some(receipt.clone()),
        ..DeliveryQueueEntry::default()
    };

    let stored = StoredDelivery::from(&entry);
    let restored = DeliveryQueueEntry::from(stored);

    assert_eq!(restored.intent.channel_ref, entry.intent.channel_ref);
    assert_eq!(
        restored.intent.conversation_ref,
        entry.intent.conversation_ref
    );
    assert_eq!(restored.partial_receipt, Some(receipt));
    assert_eq!(restored.intent.turn_id.as_deref(), Some("turn-42"));
}

#[test]
fn stored_terminal_result_round_trip_preserves_receipt() {
    let result = DeliveryTerminalResult {
        id: "key".into(),
        intent_fingerprint: "digest".into(),
        completed_at_unix_ms: 42,
        receipt: Some(MessageReceipt {
            primary_platform_message_id: Some("message".into()),
            ..MessageReceipt::default()
        }),
    };

    let restored = DeliveryTerminalResult::from(StoredResult::from(&result));

    assert_eq!(restored, result);
}
