use super::*;
use crate::channel::OutboundContentPart;

#[test]
fn canonical_output_events_keep_typed_tags_and_camel_case_fields() {
    let event = ChannelOutputEvent::ToolCallCompleted {
        call_id: "call-1".into(),
        tool_name: "search".into(),
        success: false,
        output_chars: 8,
        output: "unavailable".into(),
        arguments: Some(serde_json::json!({"query": "weather"})),
        elapsed_ms: 12,
        iteration: 3,
        failure: Some(ToolFailureSummary {
            class: "network".into(),
            category: "transport".into(),
            cause_plain: "connection failed".into(),
            next_action: "retry".into(),
            recoverable: true,
        }),
        display_label: None,
        display_detail: None,
        structured: None,
    };

    assert_eq!(
        serde_json::to_value(event).unwrap(),
        serde_json::json!({
            "type": "tool_call_completed",
            "callId": "call-1",
            "toolName": "search",
            "success": false,
            "outputChars": 8,
            "output": "unavailable",
            "arguments": {"query": "weather"},
            "elapsedMs": 12,
            "iteration": 3,
            "failure": {
                "class": "network",
                "category": "transport",
                "causePlain": "connection failed",
                "nextAction": "retry",
                "recoverable": true
            },
            "displayLabel": null,
            "displayDetail": null,
            "structured": null
        })
    );
}

#[test]
fn final_output_keeps_ordered_typed_parts_and_receipt() {
    let event = ChannelOutputEvent::FinalMessage {
        text: "answer".into(),
        parts: vec![OutboundContentPart::Content {
            part: crate::channel::ContentPart::Text {
                text: "answer".into(),
                markdown_dialect: None,
            },
        }],
        receipt: Some(crate::channel::MessageReceipt {
            primary_platform_message_id: Some("message-1".into()),
            ..Default::default()
        }),
    };

    let value = serde_json::to_value(event).unwrap();
    assert_eq!(value["type"], "final_message");
    assert_eq!(value["parts"][0]["kind"], "content");
    assert_eq!(value["parts"][0]["part"]["kind"], "text");
    assert_eq!(value["receipt"]["primaryPlatformMessageId"], "message-1");
}

#[test]
fn v1_progressive_commands_keep_their_wire_shape() {
    assert_eq!(
        serde_json::to_value(ProgressiveCommand::Open {
            channel: "telegram:bot-1".into(),
        })
        .unwrap(),
        serde_json::json!({"operation": "open", "channel": "telegram:bot-1"})
    );
    assert_eq!(
        serde_json::to_value(ProgressiveResult::Opened {
            handle: "reply-1".into(),
        })
        .unwrap(),
        serde_json::json!({"result": "opened", "handle": "reply-1"})
    );
}

#[test]
fn absent_transport_scope_stays_omitted_for_legacy_sends() {
    let request = ProgressiveTransportRequest::Send {
        channel: "telegram:bot-1".into(),
        turn_id: None,
        channel_ref: None,
        conversation: None,
        sequence: None,
        idempotency_key: None,
        message: serde_json::json!({"text": "hello"}),
    };

    assert_eq!(
        serde_json::to_value(request).unwrap(),
        serde_json::json!({
            "operation": "send",
            "channel": "telegram:bot-1",
            "message": {"text": "hello"}
        })
    );
}

#[test]
fn output_event_transport_retains_order_and_turn_addressing() {
    let request = ProgressiveTransportRequest::OutputEvent {
        channel: "telegram".into(),
        turn_id: Some("turn-1".into()),
        channel_ref: None,
        conversation: None,
        sequence: 7,
        event: Box::new(ChannelOutputEvent::TextDelta {
            text: "hello".into(),
            iteration: Some(2),
        }),
    };

    assert_eq!(
        serde_json::to_value(request).unwrap(),
        serde_json::json!({
            "operation": "output_event",
            "channel": "telegram",
            "turn_id": "turn-1",
            "channel_ref": null,
            "conversation": null,
            "sequence": 7,
            "event": {
                "type": "text_delta",
                "text": "hello",
                "iteration": 2
            }
        })
    );
}
