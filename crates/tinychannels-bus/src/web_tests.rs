use super::*;

const CHAT_DONE: &str = include_str!("fixtures/web/chat_done.json");
const APPROVAL: &str = include_str!("fixtures/web/approval_request.json");
const SUBAGENT: &str = include_str!("fixtures/web/subagent_completed.json");
const LEGACY_CANCEL: &str = include_str!("fixtures/web/chat_error_cancelled.json");
const CANCEL: &str = include_str!("fixtures/web/chat_cancelled.json");
const TEXT_DELTA: &str = include_str!("fixtures/web/text_delta.json");
const PROACTIVE: &str = include_str!("fixtures/web/proactive_message.json");

fn assert_historical_fixture_round_trips(fixture: &str) {
    let historical: serde_json::Value = serde_json::from_str(fixture).unwrap();
    let event: WebChannelEvent = serde_json::from_value(historical.clone()).unwrap();
    assert_eq!(serde_json::to_value(event).unwrap(), historical);
}

#[test]
fn deployed_web_event_goldens_keep_their_serialized_shapes() {
    for fixture in [
        CHAT_DONE,
        APPROVAL,
        SUBAGENT,
        LEGACY_CANCEL,
        CANCEL,
        TEXT_DELTA,
        PROACTIVE,
    ] {
        assert_historical_fixture_round_trips(fixture);
    }
}

#[test]
fn usage_defaults_and_unknown_costs_match_the_harness_wire_contract() {
    assert_eq!(WebCostSource::default(), WebCostSource::Charged);
    let usage: WebTurnUsagePayload = serde_json::from_value(serde_json::json!({
        "input_tokens": 1,
        "output_tokens": 2,
        "cached_input_tokens": 0,
        "cost_usd": null,
        "context_window": 4096
    }))
    .unwrap();
    assert_eq!(usage.cost_source, WebCostSource::Charged);
    let serialized = serde_json::to_value(usage).unwrap();
    assert!(serialized["cost_usd"].is_null());
    assert_eq!(serialized["cost_source"], "charged");
    let child = WebSubagentUsagePayload {
        task_id: "child".into(),
        agent_id: "researcher".into(),
        input_tokens: 0,
        output_tokens: 0,
        cost_usd: None,
    };
    assert!(serde_json::to_value(child).unwrap()["cost_usd"].is_null());
}

#[test]
fn known_legacy_events_parse_but_unknown_discriminators_fail() {
    for event in ["chat_message", "chat_segment"] {
        let value = serde_json::json!({
            "event": event,
            "client_id": "c",
            "thread_id": "t",
            "request_id": "r"
        });
        assert!(serde_json::from_value::<WebChannelEvent>(value).is_ok());
    }
    let unknown = serde_json::json!({
        "event": "future_unknown",
        "client_id": "c",
        "thread_id": "t",
        "request_id": "r"
    });
    assert!(serde_json::from_value::<WebChannelEvent>(unknown).is_err());
}

#[test]
fn socket_aliases_are_transport_names_and_streaming_aliases_stay_suppressed() {
    assert_eq!(
        legacy_event_alias("tool_call", true).as_deref(),
        Some("tool:call")
    );
    assert_eq!(
        legacy_event_alias("tool:call", true).as_deref(),
        Some("tool_call")
    );
    assert_eq!(
        legacy_event_alias("subagent_tool_result", true).as_deref(),
        Some("subagent:tool:result")
    );
    for name in [
        "text_delta",
        "text:delta",
        "thinking_delta",
        "subagent_text_delta",
        "subagent:thinking:delta",
        "tool_args_delta",
        "subagent_tool_args_delta",
    ] {
        assert_eq!(legacy_event_alias(name, true), None, "{name}");
    }
    assert_eq!(legacy_event_alias("tool_call", false), None);
    let payload: serde_json::Value = serde_json::from_str(TEXT_DELTA).unwrap();
    assert_eq!(payload["event"], "text_delta");
}

#[test]
fn queue_modes_keep_all_five_web_dispositions() {
    let modes = [
        (QueueMode::Interrupt, "interrupt"),
        (QueueMode::Steer, "steer"),
        (QueueMode::Followup, "followup"),
        (QueueMode::Collect, "collect"),
        (QueueMode::Parallel, "parallel"),
    ];
    for (mode, wire) in modes {
        assert_eq!(mode.to_string(), wire);
        assert_eq!(serde_json::to_value(mode).unwrap(), wire);
    }
}

#[test]
fn web_wire_caps_bound_utf8_output_and_json_arguments() {
    let output = cap_web_output("é".repeat(MAX_WEB_TOOL_PAYLOAD_BYTES));
    assert!(output.len() <= MAX_WEB_TOOL_PAYLOAD_BYTES);
    assert!(output.is_char_boundary(output.len()));
    assert!(output.ends_with("of tool output]"));
    assert_eq!(cap_web_args(None), None);
    assert_eq!(cap_web_args(Some(serde_json::Value::Null)), None);
    let bounded = cap_web_args(Some(serde_json::json!({
        "body": "x".repeat(MAX_WEB_TOOL_PAYLOAD_BYTES)
    })))
    .unwrap();
    assert!(bounded.is_string());
    assert!(bounded.as_str().unwrap().len() <= MAX_WEB_TOOL_PAYLOAD_BYTES);
}

#[test]
fn timing_rate_is_finite_and_requires_a_positive_duration() {
    assert_eq!(tokens_per_second(Some(12), Some(2_000)), Some(6.0));
    assert_eq!(tokens_per_second(Some(12), Some(0)), None);
    assert_eq!(tokens_per_second(None, Some(1_000)), None);
}

#[test]
fn web_events_keep_ordered_typed_media_and_voice_parts() {
    use crate::channel::{ContentPart, MediaKind, MediaReference, OutboundContentPart, VoiceNote};

    let event = WebChannelEvent::ChatDone {
        context: WebEventContext {
            client_id: "c".into(),
            thread_id: "t".into(),
            request_id: "r".into(),
            ..Default::default()
        },
        full_response: Some("See attachment".into()),
        citations: None,
        usage: None,
        timing: None,
        parts: Some(vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "See attachment".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::VoiceNote {
                    voice_note: VoiceNote {
                        media: MediaReference {
                            url: Some("https://example.test/audio.ogg".into()),
                            kind: MediaKind::Audio,
                            ..Default::default()
                        },
                        codec: Some("opus".into()),
                        ..Default::default()
                    },
                },
            },
        ]),
    };
    let json = serde_json::to_value(event).unwrap();
    let parts = json["parts"].as_array().unwrap();
    assert_eq!(parts[0]["part"]["kind"], "text");
    assert_eq!(parts[1]["part"]["kind"], "voice_note");
    assert_eq!(
        parts[1]["part"]["voice_note"]["media"]["url"],
        "https://example.test/audio.ogg"
    );
}

#[test]
fn schema_export_is_deterministic_and_contains_every_variant() {
    let schema = web_event_schema_json().unwrap();
    assert_eq!(schema, web_event_schema_json().unwrap());
    for event in [
        "chat_done",
        "chat_error",
        "chat_cancelled",
        "proactive_message",
        "voice_speak",
        "subagent_completed",
        "approval_request",
        "chat_message",
        "chat_segment",
    ] {
        assert!(schema.contains(event), "schema omitted {event}");
    }
    assert!(schema.contains("OutboundContentPart"));
}

#[test]
fn flattened_context_is_the_only_owner_of_turn_request_id() {
    let event: WebChannelEvent = serde_json::from_str(
        r#"{"event":"artifact_pending","client_id":"c","thread_id":"t","request_id":"r","turn_request_id":"turn"}"#,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(event).unwrap()["turn_request_id"],
        "turn"
    );
}
