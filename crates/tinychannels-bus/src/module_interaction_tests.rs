use super::*;

#[test]
fn draft_finalization_preserves_thread_and_content_on_wire() {
    let request = ChannelInteractionRequest {
        channel: "telegram".into(),
        interaction: ChannelInteraction::FinalizeDraft {
            recipient: "chat".into(),
            message_id: "draft".into(),
            text: "complete response".into(),
            thread_ts: Some("topic".into()),
        },
    };
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(value["interaction"]["operation"], "finalize_draft");
    let decoded: ChannelInteractionRequest = serde_json::from_value(value).unwrap();
    let ChannelInteraction::FinalizeDraft {
        text, thread_ts, ..
    } = decoded.interaction
    else {
        panic!("wrong interaction")
    };
    assert_eq!(text, "complete response");
    assert_eq!(thread_ts.as_deref(), Some("topic"));
}

#[test]
fn unsupported_draft_is_an_explicit_nullable_result() {
    assert_eq!(
        serde_json::to_value(ChannelInteractionResult::Draft(None)).unwrap(),
        serde_json::json!({"result":"draft", "value":null})
    );
}

#[test]
fn typing_refresh_operations_are_additive_and_typed() {
    let request = ChannelInteractionRequest {
        channel: "telegram".into(),
        interaction: ChannelInteraction::StartTypingRefresh {
            recipient: "chat".into(),
            interval_secs: 4,
        },
    };
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(value["interaction"]["operation"], "start_typing_refresh");
    assert_eq!(value["interaction"]["request"]["interval_secs"], 4);
    let decoded: ChannelInteractionRequest = serde_json::from_value(value).unwrap();
    assert!(matches!(
        decoded.interaction,
        ChannelInteraction::StartTypingRefresh {
            interval_secs: 4,
            ..
        }
    ));

    let stop = serde_json::to_value(ChannelInteraction::StopTypingRefresh {
        handle: "opaque-handle".into(),
    })
    .unwrap();
    assert_eq!(stop["operation"], "stop_typing_refresh");
    assert_eq!(
        serde_json::to_value(ChannelInteractionResult::TypingRefresh {
            handle: "opaque-handle".into(),
        })
        .unwrap(),
        serde_json::json!({
            "result": "typing_refresh",
            "value": {"handle": "opaque-handle"}
        })
    );
}

#[test]
fn typed_draft_interaction_round_trips_full_account_and_topic_scope() {
    let request = ChannelInteractionRequest {
        channel: "telegram:account-a".into(),
        interaction: ChannelInteraction::UpsertDraft {
            previous: Some(ChannelSendOutcome {
                receipt: MessageReceipt {
                    channel: Some(ChannelRef { id: "telegram".into(), account_id: Some("telegram:account-a".into()) }),
                    conversation: Some(ConversationRef { id: "-100".into(), topic_id: Some("topic-4".into()), ..Default::default() }),
                    primary_platform_message_id: Some("91".into()),
                    platform_message_ids: vec!["91".into()],
                    ..Default::default()
                },
                error: None,
            }.receipt),
            intent: ChannelOutboundIntent {
                idempotency_key: "draft-op".into(),
                channel_id: "telegram".into(),
                conversation_id: "-100".into(),
                channel_ref: Some(ChannelRef { id: "telegram".into(), account_id: Some("telegram:account-a".into()) }),
                conversation_ref: Some(ConversationRef { id: "-100".into(), topic_id: Some("topic-4".into()), ..Default::default() }),
                payload: crate::OutboundPayload::Text { text: "progress".into() },
                ..Default::default()
            },
        },
    };
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(value["interaction"]["operation"], "upsert_draft");
    let decoded: ChannelInteractionRequest = serde_json::from_value(value).unwrap();
    let ChannelInteraction::UpsertDraft { previous, intent } = decoded.interaction else {
        panic!("wrong interaction")
    };
    let previous = previous.unwrap();
    assert_eq!(previous.channel.unwrap().account_id.as_deref(), Some("telegram:account-a"));
    assert_eq!(previous.conversation.unwrap().topic_id.as_deref(), Some("topic-4"));
    assert_eq!(intent.conversation_ref.unwrap().topic_id.as_deref(), Some("topic-4"));
}
