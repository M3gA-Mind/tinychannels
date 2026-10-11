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
