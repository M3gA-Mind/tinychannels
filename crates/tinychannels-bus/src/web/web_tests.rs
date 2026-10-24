use super::*;

const CHAT_DONE: &str = include_str!("../fixtures/web/chat_done.json");
const APPROVAL: &str = include_str!("../fixtures/web/approval_request.json");
const SUBAGENT: &str = include_str!("../fixtures/web/subagent_completed.json");
const LEGACY_CANCEL: &str = include_str!("../fixtures/web/chat_error_cancelled.json");
const CANCEL: &str = include_str!("../fixtures/web/chat_cancelled.json");
const TEXT_DELTA: &str = include_str!("../fixtures/web/text_delta.json");
const PROACTIVE: &str = include_str!("../fixtures/web/proactive_message.json");
const CHANNEL_OUTBOUND: &str = include_str!("../fixtures/web/channel_outbound.json");
const SUBAGENT_TOOL_RESULT: &str = include_str!("../fixtures/web/subagent_tool_result.json");
const CHAT_ERROR_GUARDRAIL: &str = include_str!("../fixtures/web/chat_error_guardrail.json");
const QUEUE_ITEM: &str = include_str!("../fixtures/web/queue_item_queued.json");
const THREAD_GOAL: &str = include_str!("../fixtures/web/thread_goal_updated.json");
const THREAD_TODOS: &str = include_str!("../fixtures/web/thread_todos_changed.json");
const TOOL_RESULT: &str = include_str!("../fixtures/web/tool_result.json");
const APPROVAL_DECIDED: &str = include_str!("../fixtures/web/approval_decided.json");
const ADDITIONAL_HISTORICAL_EVENTS: &str = include_str!("../fixtures/web/historical_events.json");
const WEB_SCHEMA: &str = include_str!("../../schema/web-channel-event.schema.json");

fn assert_historical_fixture_round_trips(fixture: &str) {
    let historical: serde_json::Value = serde_json::from_str(fixture).unwrap();
    let event: WebChannelEvent = serde_json::from_value(historical.clone()).unwrap();
    assert_eq!(event.event_name(), historical["event"].as_str().unwrap());
    assert_eq!(serde_json::to_value(event).unwrap(), historical);
}

#[test]
fn web_chat_start_is_a_host_authoritative_bus_request() {
    let parsed: WebChatStart = serde_json::from_str(
        r#"{"clientId":"browser","threadId":"thread","requestId":"request","text":"hello","senderId":"forged","commandAuthorized":true}"#,
    )
    .unwrap();
    assert_eq!(parsed.client_id, "browser");
    assert_eq!(parsed.sender_id, None);
    assert!(!parsed.command_authorized);

    let host_request = WebChatStart {
        sender_id: Some("user-1".into()),
        command_authorized: true,
        ..parsed
    };
    assert_eq!(host_request.sender_id.as_deref(), Some("user-1"));
    assert!(host_request.command_authorized);
}

fn normalize_line_endings(contents: &str) -> String {
    contents.replace("\r\n", "\n")
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
        CHANNEL_OUTBOUND,
        SUBAGENT_TOOL_RESULT,
        CHAT_ERROR_GUARDRAIL,
        QUEUE_ITEM,
        THREAD_GOAL,
        THREAD_TODOS,
        TOOL_RESULT,
        APPROVAL_DECIDED,
    ] {
        assert_historical_fixture_round_trips(fixture);
    }
    let additional: Vec<serde_json::Value> =
        serde_json::from_str(ADDITIONAL_HISTORICAL_EVENTS).unwrap();
    for fixture in additional {
        assert_historical_fixture_round_trips(&fixture.to_string());
    }
}

#[test]
fn event_context_access_preserves_every_historical_wire_shape() {
    for fixture in [
        CHAT_DONE,
        APPROVAL,
        SUBAGENT,
        LEGACY_CANCEL,
        CANCEL,
        TEXT_DELTA,
        PROACTIVE,
        CHANNEL_OUTBOUND,
        SUBAGENT_TOOL_RESULT,
        CHAT_ERROR_GUARDRAIL,
        QUEUE_ITEM,
        THREAD_GOAL,
        THREAD_TODOS,
        TOOL_RESULT,
        APPROVAL_DECIDED,
    ] {
        let historical: serde_json::Value = serde_json::from_str(fixture).unwrap();
        let mut event: WebChannelEvent = serde_json::from_value(historical.clone()).unwrap();
        assert_eq!(
            event.context().client_id,
            historical["client_id"].as_str().unwrap()
        );
        assert_eq!(
            event.context().thread_id,
            historical["thread_id"].as_str().unwrap()
        );
        assert_eq!(
            event.context().request_id,
            historical["request_id"].as_str().unwrap()
        );
        let original_timestamp = historical.get("ts").and_then(serde_json::Value::as_u64);
        event.context_mut().ts = Some(42);
        assert_eq!(event.context().ts, Some(42));
        event.context_mut().ts = original_timestamp;
        let with_timestamp = serde_json::to_value(event).unwrap();
        assert_eq!(with_timestamp, historical);
    }

    for fixture in
        serde_json::from_str::<Vec<serde_json::Value>>(ADDITIONAL_HISTORICAL_EVENTS).unwrap()
    {
        let mut event: WebChannelEvent = serde_json::from_value(fixture.clone()).unwrap();
        let original_timestamp = fixture.get("ts").and_then(serde_json::Value::as_u64);
        event.context_mut().ts = Some(42);
        assert_eq!(event.context().ts, Some(42));
        event.context_mut().ts = original_timestamp;
        let with_timestamp = serde_json::to_value(event).unwrap();
        assert_eq!(with_timestamp, fixture);
    }
}

#[test]
fn channel_event_uses_the_canonical_tinychannels_payload() {
    let value = serde_json::json!({
        "event": "channel_event",
        "client_id": "channel-runtime",
        "thread_id": "channel:telegram/alice/room-1",
        "request_id": "telegram:update:7",
        "openhuman_message_id": "user:row-42",
        "channel_event": {
            "version": 2,
            "eventId": "telegram:update:7:reaction:added:room-1:42:🔥",
            "sourceEventId": "7",
            "sourceTimestamp": 1720000000000u64,
            "channel": {"id": "telegram", "accountId": "bot-1"},
            "conversation": {
                "kind": "group",
                "id": "room-1",
                "scopeId": null,
                "parentId": null,
                "threadId": null,
                "topicId": null
            },
            "type": "reaction_added",
            "messageId": "42",
            "providerMessageId": "42",
            "actor": {"id": "alice", "altIds": [], "name": "Alice", "isBot": false, "roles": []},
            "reaction": {"value": "🔥", "customEmojiId": null}
        }
    });
    let event: WebChannelEvent = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(event.event_name(), "channel_event");
    assert_eq!(event.context().thread_id, "channel:telegram/alice/room-1");
    assert_eq!(serde_json::to_value(event).unwrap(), value);
}

#[test]
fn direct_web_events_preserve_null_and_empty_values_until_projection_normalizes() {
    let context = WebEventContext {
        client_id: "client".into(),
        thread_id: "thread".into(),
        request_id: "request".into(),
        ..Default::default()
    };
    let mut call = WebChannelEvent::ToolCall {
        context: context.clone(),
        tool_name: Some(String::new()),
        skill_id: None,
        args: Some(serde_json::Value::Null),
        round: None,
        tool_call_id: None,
        tool_display_label: None,
        tool_display_detail: None,
    };
    let direct_wire = serde_json::to_value(&call).unwrap();
    assert_eq!(direct_wire["tool_name"], "");
    assert_eq!(
        direct_wire.get("args"),
        Some(&serde_json::Value::Null),
        "direct producer payload must retain explicit null"
    );
    call.normalize_projected_wire_fields();
    let projected_wire = serde_json::to_value(call).unwrap();
    assert!(projected_wire.get("tool_name").is_none());
    assert!(projected_wire.get("args").is_none());

    let approval = WebChannelEvent::ApprovalRequest {
        context: context.clone(),
        tool_name: Some("request_approval".into()),
        message: None,
        args: Some(serde_json::Value::Null),
        tool_call_id: None,
        expires_at: None,
        detached: None,
    };
    assert_eq!(
        serde_json::to_value(&approval).unwrap().get("args"),
        Some(&serde_json::Value::Null),
        "direct approval payload must retain explicit null"
    );

    let mut delta = WebChannelEvent::ToolArgsDelta {
        context,
        delta: None,
        delta_kind: None,
        tool_call_id: None,
        tool_name: Some(String::new()),
        skill_id: None,
        round: None,
    };
    let wire = serde_json::to_value(&delta).unwrap();
    assert_eq!(wire["tool_name"], "");
    delta.normalize_projected_wire_fields();
    assert!(
        serde_json::to_value(delta)
            .unwrap()
            .get("tool_name")
            .is_none()
    );
}

#[test]
fn projected_legacy_normalization_covers_tool_subagent_artifact_and_other_events() {
    let context = WebEventContext {
        client_id: "client".into(),
        thread_id: "thread".into(),
        request_id: "request".into(),
        ..Default::default()
    };
    let mut tool_result = WebChannelEvent::ToolResult {
        context: context.clone(),
        tool_name: Some(String::new()),
        skill_id: None,
        output: None,
        args: Some(serde_json::Value::Null),
        success: None,
        round: None,
        tool_call_id: None,
        failure: None,
        elapsed_ms: None,
        tool_display_label: None,
        tool_display_detail: None,
        structured: None,
    };
    tool_result.normalize_projected_wire_fields();
    let wire = serde_json::to_value(tool_result).unwrap();
    assert!(wire.get("tool_name").is_none());
    assert!(wire.get("args").is_none());

    let mut subagent = WebChannelEvent::SubagentSpawned {
        context: context.clone(),
        message: None,
        tool_name: Some(String::new()),
        skill_id: None,
        round: None,
        subagent: None,
    };
    subagent.normalize_projected_wire_fields();
    assert!(
        serde_json::to_value(subagent)
            .unwrap()
            .get("tool_name")
            .is_none()
    );

    let mut projected_events = vec![
        WebChannelEvent::SubagentCompleted {
            context: context.clone(),
            message: None,
            tool_name: Some(String::new()),
            skill_id: None,
            success: None,
            round: None,
            subagent: None,
        },
        WebChannelEvent::SubagentFailed {
            context: context.clone(),
            message: None,
            tool_name: Some(String::new()),
            skill_id: None,
            success: None,
            subagent: None,
            round: None,
        },
        WebChannelEvent::SubagentAwaitingUser {
            context: context.clone(),
            message: None,
            tool_name: Some(String::new()),
            skill_id: None,
            success: None,
            round: None,
            subagent: None,
        },
        WebChannelEvent::SubagentIterationStart {
            context: context.clone(),
            message: None,
            tool_name: Some(String::new()),
            skill_id: None,
            round: None,
            subagent: None,
        },
        WebChannelEvent::SubagentToolCall {
            context: context.clone(),
            tool_name: Some(String::new()),
            skill_id: None,
            args: Some(serde_json::Value::Null),
            round: None,
            tool_call_id: None,
            tool_display_label: None,
            tool_display_detail: None,
            subagent: None,
        },
        WebChannelEvent::SubagentToolResult {
            context: context.clone(),
            tool_name: Some(String::new()),
            skill_id: None,
            success: None,
            round: None,
            tool_call_id: None,
            output: None,
            args: Some(serde_json::Value::Null),
            elapsed_ms: None,
            tool_display_label: None,
            tool_display_detail: None,
            structured: None,
            failure: None,
            subagent: None,
        },
        WebChannelEvent::ApprovalRequest {
            context: context.clone(),
            tool_name: Some(String::new()),
            message: None,
            args: Some(serde_json::Value::Null),
            tool_call_id: None,
            expires_at: None,
            detached: None,
        },
        WebChannelEvent::ApprovalDecided {
            context: context.clone(),
            tool_name: Some(String::new()),
            message: None,
            tool_call_id: None,
            cancel_reason: None,
        },
        WebChannelEvent::PlanReviewRequest {
            context: context.clone(),
            tool_name: Some(String::new()),
            message: None,
            args: Some(serde_json::Value::Null),
            tool_call_id: None,
            expires_at: None,
        },
        WebChannelEvent::PlanReviewDecided {
            context: context.clone(),
            tool_name: Some(String::new()),
            message: None,
            tool_call_id: None,
            cancel_reason: None,
        },
        WebChannelEvent::ArtifactReady {
            context: context.clone(),
            tool_call_id: None,
            args: Some(serde_json::Value::Null),
        },
        WebChannelEvent::ArtifactFailed {
            context: context.clone(),
            tool_call_id: None,
            args: Some(serde_json::Value::Null),
        },
        WebChannelEvent::MemoryActivity {
            context: context.clone(),
            args: Some(serde_json::Value::Null),
        },
        WebChannelEvent::ExternalTransferPending {
            context: context.clone(),
            args: Some(serde_json::Value::Null),
        },
    ];
    for event in &mut projected_events {
        event.normalize_projected_wire_fields();
        let wire = serde_json::to_value(&*event).unwrap();
        assert!(wire.get("tool_name").is_none());
        assert!(wire.get("args").is_none());
    }

    let mut artifact = WebChannelEvent::ArtifactPending {
        context: context.clone(),
        tool_call_id: None,
        args: Some(serde_json::Value::Null),
    };
    artifact.normalize_projected_wire_fields();
    assert!(
        serde_json::to_value(artifact)
            .unwrap()
            .get("args")
            .is_none()
    );

    let mut unrelated = WebChannelEvent::ChatDone {
        context,
        full_response: Some("done".into()),
        citations: None,
        usage: None,
        timing: None,
        parts: None,
    };
    unrelated.normalize_projected_wire_fields();
    assert_eq!(
        serde_json::to_value(unrelated).unwrap()["full_response"],
        "done"
    );
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
    assert_eq!(serialized.get("cost_usd"), Some(&serde_json::Value::Null));
    assert_eq!(serialized["cost_source"], "charged");
    let child = WebSubagentUsagePayload {
        task_id: "child".into(),
        agent_id: "researcher".into(),
        input_tokens: 0,
        output_tokens: 0,
        cost_usd: None,
    };
    let child = serde_json::to_value(child).unwrap();
    assert_eq!(child.get("cost_usd"), Some(&serde_json::Value::Null));
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
    assert_eq!(cap_web_output("small".into()), "small");
    let output = cap_web_output("界".repeat(MAX_WEB_TOOL_PAYLOAD_BYTES));
    assert!(output.len() <= MAX_WEB_TOOL_PAYLOAD_BYTES);
    assert!(output.is_char_boundary(output.len()));
    assert!(output.ends_with("of tool output]"));
    assert_eq!(cap_web_args(None), None);
    assert_eq!(cap_web_args(Some(serde_json::Value::Null)), None);
    assert_eq!(
        cap_web_args(Some(serde_json::json!({ "small": true }))),
        Some(serde_json::json!({ "small": true }))
    );
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
fn timing_state_keeps_firsts_round_one_counts_snapshot_and_throttle() {
    let mut timing = TurnTimingState::default();
    assert!(!timing.text_delta(10, "  ", 1));
    assert!(timing.text_delta(20, "hé", 1));
    assert!(!timing.text_delta(25, "again", 1));
    assert!(!timing.text_delta(30, "later", 2));
    assert!(timing.tool_call(40));
    assert!(!timing.tool_call(60));
    let snapshot = timing.snapshot(100);
    assert_eq!(snapshot.first_token_ms, Some(20));
    assert_eq!(snapshot.first_tool_ms, Some(40));
    assert_eq!(snapshot.round_one_narration_chars, 9);
    assert_eq!(snapshot.into_payload(Some(4)).tokens_per_second, Some(40.0));

    let mut throttle = TurnCostThrottle::default();
    assert!(throttle.should_emit(0));
    assert!(!throttle.should_emit(749));
    assert!(throttle.should_emit(750));
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
        parts[1]["part"]["voiceNote"]["media"]["url"],
        "https://example.test/audio.ogg"
    );
}

#[test]
fn schema_export_is_deterministic_and_contains_every_variant() {
    let schema = web_event_schema_json().unwrap();
    assert_eq!(schema, web_event_schema_json().unwrap());
    // Normalize only CRLF so schema-content changes still fail on every platform.
    assert_eq!(schema, normalize_line_endings(WEB_SCHEMA).trim_end());
    let windows_checkout = WEB_SCHEMA.replace("\r\n", "\n").replace('\n', "\r\n");
    assert_eq!(schema, normalize_line_endings(&windows_checkout).trim_end());
    let schema: serde_json::Value = serde_json::from_str(&schema).unwrap();
    let schema_text = schema.to_string();
    for event in HISTORICAL_EVENT_NAMES {
        assert!(schema_text.contains(event), "schema omitted {event}");
    }
    for type_name in ["OutboundContentPart", "MediaReference", "VoiceNote"] {
        assert!(
            schema_text.contains(type_name),
            "schema omitted {type_name}"
        );
    }
    let sender = &schema["$defs"]["SenderRef"];
    let required = sender["required"].as_array().unwrap();
    assert!(required.iter().any(|field| field == "id"));
    assert!(required.iter().any(|field| field == "name"));
    let name_schema = &sender["properties"]["name"];
    let name_types = name_schema["type"].as_array().unwrap();
    assert!(name_types.iter().any(|kind| kind == "string"));
    assert!(name_types.iter().any(|kind| kind == "null"));
}

const HISTORICAL_EVENT_NAMES: &[&str] = &[
    "chat_done",
    "chat_interim",
    "chat_suggestions",
    "chat_error",
    "chat_cancelled",
    "text_delta",
    "thinking_delta",
    "tool_args_delta",
    "inference_start",
    "inference_heartbeat",
    "iteration_start",
    "context_compacting",
    "tool_call",
    "tool_progress",
    "tool_result",
    "turn_cost",
    "subagent_spawned",
    "subagent_completed",
    "subagent_failed",
    "subagent_awaiting_user",
    "subagent_iteration_start",
    "subagent_tool_call",
    "subagent_tool_result",
    "subagent_text_delta",
    "subagent_thinking_delta",
    "approval_request",
    "approval_decided",
    "plan_review_request",
    "plan_review_decided",
    "queue_item_queued",
    "queue_item_delivered",
    "queue_item_removed",
    "thread_goal_updated",
    "thread_goal_cleared",
    "thread_todos_changed",
    "run_mode_changed",
    "artifact_ready",
    "artifact_failed",
    "artifact_pending",
    "memory_activity",
    "external_transfer_pending",
    "proactive_message",
    "voice_speak",
    "user_error",
    "chat_message",
    "chat_segment",
];

#[test]
fn every_historical_discriminator_has_a_strongly_tagged_variant() {
    for name in HISTORICAL_EVENT_NAMES {
        let fixture = serde_json::json!({
            "event": name,
            "client_id": "client",
            "thread_id": "thread",
            "request_id": "request",
            "suggestions": []
        });
        let parsed: WebChannelEvent = serde_json::from_value(fixture)
            .unwrap_or_else(|error| panic!("{name} rejected: {error}"));
        assert_eq!(serde_json::to_value(parsed).unwrap()["event"], *name);
    }
}

#[test]
fn flattened_context_is_the_only_owner_of_turn_request_id() {
    let event: WebChannelEvent = serde_json::from_str(
        r#"{"event":"artifact_pending","client_id":"c","thread_id":"t","request_id":"r","turn_request_id":"turn"}"#,
    )
    .unwrap();
    let encoded = serde_json::to_string(&event).unwrap();
    assert_eq!(encoded.matches("turn_request_id").count(), 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&encoded).unwrap()["turn_request_id"],
        "turn"
    );
}
