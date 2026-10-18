use super::*;
use crate::ChannelOutboundIntent;

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
            previous: Some(crate::channel::MessageReceipt {
                channel: Some(crate::channel::ChannelRef {
                    id: "telegram".into(),
                    account_id: Some("telegram:account-a".into()),
                }),
                conversation: Some(crate::channel::ConversationRef {
                    id: "-100".into(),
                    topic_id: Some("topic-4".into()),
                    ..Default::default()
                }),
                primary_platform_message_id: Some("91".into()),
                platform_message_ids: vec!["91".into()],
                ..Default::default()
            }),
            intent: ChannelOutboundIntent {
                idempotency_key: "draft-op".into(),
                channel_id: "telegram".into(),
                conversation_id: "-100".into(),
                channel_ref: Some(crate::channel::ChannelRef {
                    id: "telegram".into(),
                    account_id: Some("telegram:account-a".into()),
                }),
                conversation_ref: Some(crate::channel::ConversationRef {
                    id: "-100".into(),
                    topic_id: Some("topic-4".into()),
                    ..Default::default()
                }),
                payload: crate::OutboundPayload::Text {
                    text: "progress".into(),
                },
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
    assert_eq!(
        previous.channel.unwrap().account_id.as_deref(),
        Some("telegram:account-a")
    );
    assert_eq!(
        previous.conversation.unwrap().topic_id.as_deref(),
        Some("topic-4")
    );
    assert_eq!(
        intent.conversation_ref.unwrap().topic_id.as_deref(),
        Some("topic-4")
    );
}

#[test]
fn webhook_and_read_receipt_operations_round_trip_on_the_wire() {
    let delivery = ChannelInteractionRequest {
        channel: "whatsapp:account-a".into(),
        interaction: ChannelInteraction::DeliverWebhook {
            signature: Some("sha256=abc".into()),
            body_base64: "e30=".into(),
        },
    };
    let value = serde_json::to_value(&delivery).unwrap();
    assert_eq!(value["interaction"]["operation"], "deliver_webhook");
    assert_eq!(value["interaction"]["request"]["signature"], "sha256=abc");
    assert_eq!(value["interaction"]["request"]["body_base64"], "e30=");
    let decoded: ChannelInteractionRequest = serde_json::from_value(value).unwrap();
    assert!(matches!(
        decoded.interaction,
        ChannelInteraction::DeliverWebhook { signature: Some(signature), .. } if signature == "sha256=abc"
    ));

    let handshake = serde_json::to_value(ChannelInteraction::VerifyWebhook {
        mode: "subscribe".into(),
        verify_token: "t".into(),
        challenge: "c".into(),
    })
    .unwrap();
    assert_eq!(handshake["operation"], "verify_webhook");

    let read = serde_json::to_value(ChannelInteraction::MarkRead {
        channel: crate::channel::ChannelRef {
            id: "whatsapp".into(),
            account_id: Some("account-a".into()),
        },
        conversation: crate::channel::ConversationRef {
            id: "+15550001111".into(),
            ..Default::default()
        },
        provider_message_id: "wamid.1".into(),
    })
    .unwrap();
    assert_eq!(read["operation"], "mark_read");
    assert_eq!(read["request"]["provider_message_id"], "wamid.1");
}

#[test]
fn webhook_results_are_additive_and_keep_their_counts() {
    let accepted = serde_json::to_value(ChannelInteractionResult::WebhookAccepted(
        crate::WebhookDeliveryOutcome {
            messages: 2,
            events: 1,
            duplicates: 3,
        },
    ))
    .unwrap();
    assert_eq!(
        accepted,
        serde_json::json!({
            "result": "webhook_accepted",
            "value": {"messages": 2, "events": 1, "duplicates": 3}
        })
    );
    assert_eq!(
        serde_json::to_value(ChannelInteractionResult::WebhookChallenge("c".into())).unwrap(),
        serde_json::json!({"result": "webhook_challenge", "value": "c"})
    );
    assert_eq!(crate::WebhookDeliveryOutcome::default().messages, 0);
}

#[test]
fn outbound_voice_carries_an_optional_waveform_without_breaking_older_payloads() {
    use crate::channel::{MediaReference, OutboundVoice};
    let with = OutboundVoice::Media {
        media: MediaReference::default(),
        audio_base64: None,
        codec: Some("ogg_opus".into()),
        duration_ms: Some(1000),
        waveform: Some(vec![0.0, 0.5, 1.0]),
    };
    let value = serde_json::to_value(&with).unwrap();
    assert_eq!(value["waveform"], serde_json::json!([0.0, 0.5, 1.0]));
    assert_eq!(
        serde_json::from_value::<OutboundVoice>(value).unwrap(),
        with
    );
    // A payload from before the field existed still decodes, with no waveform.
    let older = serde_json::json!({
        "kind": "media",
        "media": {},
        "codec": "ogg_opus",
        "durationMs": 1000
    });
    assert!(matches!(
        serde_json::from_value::<OutboundVoice>(older).unwrap(),
        OutboundVoice::Media { waveform: None, .. }
    ));
    let without = serde_json::to_value(OutboundVoice::Media {
        media: MediaReference::default(),
        audio_base64: None,
        codec: None,
        duration_ms: None,
        waveform: None,
    })
    .unwrap();
    assert!(without.get("waveform").is_none());
}
