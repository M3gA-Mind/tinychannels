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
        ChannelInteraction::StartTypingRefresh { interval_secs: 4, .. }
    ));

    let stop = serde_json::to_value(ChannelInteraction::StopTypingRefresh {
        recipient: "chat".into(),
    })
    .unwrap();
    assert_eq!(stop["operation"], "stop_typing_refresh");
}
