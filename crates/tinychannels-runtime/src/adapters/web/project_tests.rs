use super::*;
use serde_json::json;
use tinychannels_bus::channel::{ContentPart, MediaReference, VoiceNote};
use tinychannels_bus::module_progressive::{SubagentUsage, ToolFailureSummary};

fn projector() -> WebTurnProjector {
    WebTurnProjector::new("client-7", "thread-9", "request-2")
}

fn one(event: ChannelOutputEvent) -> WebChannelEvent {
    let mut projected = projector().project(event, 10);
    assert_eq!(projected.len(), 1);
    projected.remove(0)
}

fn wire(event: &WebChannelEvent) -> Value {
    serde_json::to_value(event).expect("serialize")
}

fn subagent(state: &str) -> SubagentOutput {
    SubagentOutput {
        agent_id: "researcher".into(),
        task_id: "task-1".into(),
        state: state.into(),
        mode: Some("typed".into()),
        dedicated_thread: Some(true),
        parent_call_id: Some("parent".into()),
        call_id: Some("call-7".into()),
        iteration: Some(2),
        max_iterations: Some(5),
        prompt_chars: Some(40),
        message: Some("hello".into()),
        display_name: Some("Researcher".into()),
        tool_name: Some("web_search".into()),
        arguments: Some(json!({"query": "weather"})),
        success: Some(true),
        elapsed_ms: Some(35),
        output_chars: Some(13),
        output: Some("Found results".into()),
        structured: Some(json!({"kind": "web_search"})),
        usage: Some(SubagentUsage {
            input_tokens: 3,
            output_tokens: 4,
            cached_input_tokens: 1,
            charged_amount_usd: 0.5,
            ..Default::default()
        }),
        failure: Some(ToolFailureSummary {
            class: "c".into(),
            category: "k".into(),
            cause_plain: "p".into(),
            next_action: "n".into(),
            recoverable: true,
        }),
        display_label: Some("Search".into()),
        display_detail: Some("weather".into()),
        worker_thread_id: Some("worker".into()),
        worktree_path: Some("/w".into()),
        changed_files: vec!["a.rs".into()],
        dirty_status: Some(false),
        stop: None,
        extended_policy: None,
    }
}

#[test]
fn text_delta_matches_the_golden_fixture_shape() {
    let mut p = projector();
    let mut events = p.project(
        ChannelOutputEvent::TextDelta {
            text: "Hello".into(),
            iteration: Some(1),
        },
        1_792_238_400_000,
    );
    let event = events.remove(0);
    let golden: Value = serde_json::from_str(
        r#"{"event":"text_delta","client_id":"client-7","thread_id":"thread-9","request_id":"request-2","round":1,"delta":"Hello","delta_kind":"text","seq":1,"ts":1792238400000}"#,
    )
    .unwrap();
    assert_eq!(wire(&event), golden);
}

#[test]
fn seq_increases_across_events_and_turn_request_id_is_carried() {
    let mut p = projector().with_turn_request_id("origin");
    assert_eq!(p.context().turn_request_id.as_deref(), Some("origin"));
    let first = p.project(
        ChannelOutputEvent::ThinkingDelta {
            text: "hm".into(),
            iteration: None,
        },
        1,
    );
    let second = p.project(
        ChannelOutputEvent::Lifecycle {
            stage: "inference_start".into(),
            message: None,
            iteration: None,
            max_iterations: None,
            stop: None,
        },
        2,
    );
    assert_eq!(wire(&first[0])["seq"], 1);
    assert_eq!(wire(&second[0])["seq"], 2);
    assert_eq!(wire(&second[0])["turn_request_id"], "origin");
    assert_eq!(wire(&first[0])["delta_kind"], "thinking");
}

#[test]
fn unrepresentable_events_project_to_nothing_without_consuming_seq() {
    let mut p = projector();
    assert!(
        p.project(ChannelOutputEvent::Native { data: json!({}) }, 1)
            .is_empty()
    );
    let next = p.project(
        ChannelOutputEvent::TextDelta {
            text: "x".into(),
            iteration: None,
        },
        1,
    );
    assert_eq!(wire(&next[0])["seq"], 1);
}

#[test]
fn tool_events_carry_ids_labels_and_failures() {
    let call = one(ChannelOutputEvent::ToolCallStarted {
        call_id: "call-1".into(),
        tool_name: "shell".into(),
        arguments: json!({"cmd": "ls"}),
        iteration: 3,
        display_label: Some("Run".into()),
        display_detail: Some("ls".into()),
    });
    let call = wire(&call);
    assert_eq!(call["event"], "tool_call");
    assert_eq!(call["tool_call_id"], "call-1");
    assert_eq!(call["round"], 3);
    assert_eq!(call["tool_display_label"], "Run");

    let delta = wire(&one(ChannelOutputEvent::ToolCallArgsDelta {
        call_id: "call-1".into(),
        tool_name: "shell".into(),
        delta: "{\"c".into(),
        iteration: 3,
    }));
    assert_eq!(delta["event"], "tool_args_delta");
    assert_eq!(delta["delta_kind"], "tool_args");

    let progress = wire(&one(ChannelOutputEvent::ToolCallProgress {
        call_id: "call-1".into(),
        message: "50%".into(),
    }));
    assert_eq!(progress["event"], "tool_progress");
    assert_eq!(progress["tool_call_id"], "call-1");

    let label_only = wire(&one(ChannelOutputEvent::ToolProgress {
        label: "Searching".into(),
        message: None,
    }));
    assert_eq!(label_only["message"], "Searching");
    let with_message = wire(&one(ChannelOutputEvent::ToolProgress {
        label: "Searching".into(),
        message: Some("page 2".into()),
    }));
    assert_eq!(with_message["message"], "page 2");

    let result = wire(&one(ChannelOutputEvent::ToolCallCompleted {
        call_id: "call-1".into(),
        tool_name: "shell".into(),
        success: false,
        output_chars: 2,
        output: "no".into(),
        arguments: Some(json!({"cmd": "ls"})),
        elapsed_ms: 9,
        iteration: 3,
        failure: Some(ToolFailureSummary {
            class: "c".into(),
            category: "k".into(),
            cause_plain: "p".into(),
            next_action: "n".into(),
            recoverable: false,
        }),
        display_label: None,
        display_detail: None,
        structured: Some(json!({"kind": "x"})),
    }));
    assert_eq!(result["event"], "tool_result");
    assert_eq!(result["success"], false);
    assert_eq!(result["failure"]["class"], "c");
    assert_eq!(result["elapsed_ms"], 9);
}

#[test]
fn final_message_becomes_chat_done_with_parts_only_when_present() {
    let plain = wire(&one(ChannelOutputEvent::FinalMessage {
        text: "done".into(),
        parts: Vec::new(),
        receipt: None,
    }));
    assert_eq!(plain["event"], "chat_done");
    assert_eq!(plain["full_response"], "done");
    assert!(plain.get("parts").is_none());

    let rich = wire(&one(ChannelOutputEvent::FinalMessage {
        text: "done".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Text {
                text: "done".into(),
                markdown_dialect: None,
            },
        }],
        receipt: None,
    }));
    assert_eq!(rich["parts"].as_array().map(Vec::len), Some(1));
}

#[test]
fn approvals_clarifications_and_cancellation_use_the_web_vocabulary() {
    let approval = wire(&one(ChannelOutputEvent::ApprovalRequest {
        id: "ap-1".into(),
        prompt: "Run it?".into(),
        choices: vec!["yes".into(), "no".into()],
    }));
    assert_eq!(approval["event"], "approval_request");
    assert_eq!(approval["tool_call_id"], "ap-1");
    assert_eq!(approval["args"]["choices"][1], "no");

    let clarification = wire(&one(ChannelOutputEvent::Clarification {
        prompt: "Which one?".into(),
    }));
    assert_eq!(clarification["event"], "chat_interim");
    assert_eq!(clarification["full_response"], "Which one?");

    let cancelled = wire(&one(ChannelOutputEvent::Cancellation {
        reason: Some("user".into()),
    }));
    assert_eq!(cancelled["event"], "chat_cancelled");
    assert_eq!(cancelled["cancel_reason"], "user");
}

#[test]
fn media_and_voice_notes_ride_as_typed_parts() {
    let media = wire(&one(ChannelOutputEvent::Media {
        text: Some("look".into()),
        parts: vec![ContentPart::Image {
            media: MediaReference {
                url: Some("https://example.test/a.png".into()),
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        }],
    }));
    assert_eq!(media["event"], "chat_message");
    assert_eq!(media["full_response"], "look");
    assert_eq!(media["parts"][0]["part"]["kind"], "image");

    let empty = wire(&one(ChannelOutputEvent::Media {
        text: None,
        parts: Vec::new(),
    }));
    assert!(empty.get("parts").is_none());

    let voice = wire(&one(ChannelOutputEvent::VoiceNote {
        text: None,
        voice: VoiceNote {
            media: MediaReference {
                opaque_id: Some("media-1".into()),
                ..Default::default()
            },
            ..VoiceNote::default()
        },
    }));
    assert_eq!(voice["parts"][0]["part"]["kind"], "voice_note");
}

#[test]
fn usage_events_become_turn_cost_and_subagent_usage_is_not_double_counted() {
    let cost = wire(&one(ChannelOutputEvent::ModelUsage {
        model: "m".into(),
        provider_id: "p".into(),
        subagent_task_id: None,
        iteration: 2,
        input_tokens: 10,
        output_tokens: 5,
        cached_input_tokens: 4,
        cache_creation_tokens: 0,
        reasoning_tokens: 0,
        cost_usd: Some(0.25),
    }));
    assert_eq!(cost["event"], "turn_cost");
    assert_eq!(cost["round"], 2);
    assert_eq!(cost["usage"]["cost_source"], "charged");

    let unknown = wire(&one(ChannelOutputEvent::Usage {
        model: None,
        iteration: None,
        input_tokens: 1,
        output_tokens: 2,
        cached_input_tokens: 0,
        total_usd: None,
    }));
    assert_eq!(unknown["usage"]["cost_source"], "unknown");

    let child = projector().project(
        ChannelOutputEvent::ModelUsage {
            model: "m".into(),
            provider_id: "p".into(),
            subagent_task_id: Some("task".into()),
            iteration: 1,
            input_tokens: 1,
            output_tokens: 1,
            cached_input_tokens: 0,
            cache_creation_tokens: 0,
            reasoning_tokens: 0,
            cost_usd: None,
        },
        1,
    );
    assert!(child.is_empty());
}

#[test]
fn artifacts_and_lifecycle_stages_map_by_name() {
    for (operation, event) in [
        ("ready", "artifact_ready"),
        ("failed", "artifact_failed"),
        ("pending", "artifact_pending"),
    ] {
        let projected = wire(&one(ChannelOutputEvent::Artifact {
            operation: operation.into(),
            artifact: json!({"id": "a"}),
        }));
        assert_eq!(projected["event"], event);
        assert_eq!(projected["args"]["id"], "a");
    }
    assert!(
        projector()
            .project(
                ChannelOutputEvent::Artifact {
                    operation: "deleted".into(),
                    artifact: json!({}),
                },
                1
            )
            .is_empty()
    );

    let lifecycle = |stage: &str| ChannelOutputEvent::Lifecycle {
        stage: stage.into(),
        message: Some("m".into()),
        iteration: Some(2),
        max_iterations: None,
        stop: None,
    };
    assert_eq!(
        wire(&one(lifecycle("inference_start")))["event"],
        "inference_start"
    );
    assert_eq!(
        wire(&one(lifecycle("inference_heartbeat")))["event"],
        "inference_heartbeat"
    );
    let iteration = wire(&one(lifecycle("iteration_start")));
    assert_eq!(iteration["event"], "iteration_start");
    assert_eq!(iteration["round"], 2);
    let compacting = wire(&one(lifecycle("context_compacting")));
    assert_eq!(compacting["event"], "context_compacting");
    assert_eq!(compacting["round"], 2);
    assert!(projector().project(lifecycle("compaction"), 1).is_empty());
}

#[test]
fn subagent_states_map_to_subagent_events_with_detail() {
    for (state, event) in [
        ("spawned", "subagent_spawned"),
        ("completed", "subagent_completed"),
        ("failed", "subagent_failed"),
        ("awaiting_user", "subagent_awaiting_user"),
        ("iteration_start", "subagent_iteration_start"),
        ("tool_call", "subagent_tool_call"),
        ("tool_result", "subagent_tool_result"),
        ("text_delta", "subagent_text_delta"),
        ("thinking_delta", "subagent_thinking_delta"),
    ] {
        let projected = wire(&one(ChannelOutputEvent::Subagent(Box::new(subagent(
            state,
        )))));
        assert_eq!(projected["event"], event, "{state}");
        assert_eq!(projected["skill_id"], "task-1");
        assert_eq!(projected["subagent"]["agent_id"], "researcher");
        assert_eq!(projected["subagent"]["cost_usd"], 0.5);
        assert_eq!(projected["subagent"]["changed_files"][0], "a.rs");
        if state == "completed" {
            assert_eq!(projected["subagent"]["output"], "Found results");
        }
    }
    let result = wire(&one(ChannelOutputEvent::Subagent(Box::new(subagent(
        "tool_result",
    )))));
    assert_eq!(result["output"], "Found results");
    assert_eq!(result["failure"]["class"], "c");

    let mut bare = subagent("spawned");
    bare.usage = None;
    bare.changed_files.clear();
    let bare = wire(&one(ChannelOutputEvent::Subagent(Box::new(bare))));
    assert!(bare["subagent"].get("changed_files").is_none());
    assert!(bare["subagent"].get("cost_usd").is_none());

    assert!(
        projector()
            .project(
                ChannelOutputEvent::Subagent(Box::new(subagent("mystery"))),
                1
            )
            .is_empty()
    );
}

#[test]
fn a_finished_subagent_reports_its_iterations_and_final_text_on_the_detail() {
    let done = wire(&one(ChannelOutputEvent::Subagent(Box::new(subagent(
        "completed",
    )))));
    assert_eq!(done["subagent"]["iterations"], 2);
    assert_eq!(done["subagent"]["output"], "Found results");

    let running = wire(&one(ChannelOutputEvent::Subagent(Box::new(subagent(
        "tool_call",
    )))));
    assert!(running["subagent"].get("iterations").is_none());
    assert!(running["subagent"].get("output").is_none());
}

#[test]
fn oversized_subagent_output_is_capped() {
    let mut big = subagent("tool_result");
    big.output = Some("x".repeat(tinychannels_bus::web::MAX_WEB_TOOL_PAYLOAD_BYTES + 10));
    let projected = wire(&one(ChannelOutputEvent::Subagent(Box::new(big))));
    let output = projected["output"].as_str().unwrap();
    assert!(output.len() <= tinychannels_bus::web::MAX_WEB_TOOL_PAYLOAD_BYTES);
    assert!(output.contains("truncated"));
}

#[test]
fn completed_subagent_output_in_detail_is_capped() {
    let mut completed = subagent("completed");
    completed.output = Some("x".repeat(tinychannels_bus::web::MAX_WEB_TOOL_PAYLOAD_BYTES + 10));
    let projected = wire(&one(ChannelOutputEvent::Subagent(Box::new(completed))));
    let output = projected["subagent"]["output"].as_str().unwrap();
    assert!(output.len() <= tinychannels_bus::web::MAX_WEB_TOOL_PAYLOAD_BYTES);
    assert!(output.contains("truncated"));
}

#[test]
fn context_mut_reaches_every_event_kind() {
    let mut event = WebChannelEvent::QueueItemQueued {
        context: WebEventContext::default(),
        queue_item: None,
    };
    context_mut(&mut event).client_id = "c".into();
    assert_eq!(wire(&event)["client_id"], "c");
}

#[test]
fn context_mut_supports_current_channel_bus_events() {
    let mut outbound = WebChannelEvent::ChannelOutbound {
        context: WebEventContext::default(),
        full_response: Some("sent".into()),
        structured: None,
    };
    context_mut(&mut outbound).seq = Some(3);
    assert_eq!(wire(&outbound)["seq"], 3);

    let mut channel_event = WebChannelEvent::ChannelEvent {
        context: WebEventContext::default(),
        openhuman_message_id: None,
        channel_event: tinychannels_bus::channel::ChannelEvent {
            version: 2,
            event_id: "event-1".into(),
            source_event_id: None,
            source_timestamp: None,
            channel: tinychannels_bus::channel::ChannelRef {
                id: "telegram".into(),
                account_id: None,
            },
            conversation: None,
            event: tinychannels_bus::channel::ChannelEventKind::Lifecycle {
                state: "connected".into(),
                reason: None,
            },
        },
    };
    context_mut(&mut channel_event).request_id = "request-1".into();
    assert_eq!(wire(&channel_event)["request_id"], "request-1");
}
