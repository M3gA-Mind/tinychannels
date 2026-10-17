use crate::channel::{
    ChannelInboundEnvelope, ChannelPresentationCapabilities, ChannelRef, ChannelStaticCapabilities,
    ConversationKind, ConversationRef, MediaKind, MediaReference, OutboundPayload, SenderRef,
};
use crate::harness::{
    BridgeTranslationOptions, ChannelOutputEvent, ChannelTurn, TurnAdmissionVerdict,
    translate_output_event,
};
use tinychannels_bus::ContentPart;

fn turn() -> ChannelTurn {
    ChannelTurn {
        id: "turn-1".into(),
        session_key: "session-1".into(),
        admission: TurnAdmissionVerdict::Dispatch,
        envelope: ChannelInboundEnvelope {
            channel: ChannelRef {
                id: "telegram".into(),
                account_id: Some("bot".into()),
            },
            message_id: "msg-1".into(),
            conversation: ConversationRef {
                kind: ConversationKind::Group,
                id: "chat-1".into(),
                topic_id: Some("topic-1".into()),
                ..Default::default()
            },
            sender: SenderRef {
                id: "alice".into(),
                ..Default::default()
            },
            text: "hello".into(),
            ..Default::default()
        },
        lifecycle: Vec::new(),
    }
}

#[test]
fn text_delta_uses_draft_when_editing_supported() {
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::TextDelta {
            text: "hi".into(),
            iteration: None,
        },
        &ChannelStaticCapabilities::default(),
        &ChannelPresentationCapabilities {
            supports_edit: true,
            ..Default::default()
        },
        BridgeTranslationOptions::default(),
    );
    assert!(matches!(
        intents[0].payload,
        OutboundPayload::NativeChannelData { .. }
    ));
}

#[test]
fn text_delta_degrades_to_segment_send_without_editing() {
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::TextDelta {
            text: "hi".into(),
            iteration: None,
        },
        &ChannelStaticCapabilities::default(),
        &ChannelPresentationCapabilities::default(),
        BridgeTranslationOptions::default(),
    );
    assert!(matches!(intents[0].payload, OutboundPayload::Text { .. }));
}

#[test]
fn approval_degrades_to_text_when_native_actions_unavailable() {
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::ApprovalRequest {
            id: "approve-1".into(),
            prompt: "Run command?".into(),
            choices: vec!["approve".into(), "deny".into()],
        },
        &ChannelStaticCapabilities::default(),
        &ChannelPresentationCapabilities::default(),
        BridgeTranslationOptions::default(),
    );
    let OutboundPayload::Text { text } = &intents[0].payload else {
        panic!("expected text fallback");
    };
    assert!(text.contains("Reply with one of: approve, deny"));
}

#[test]
fn media_event_preserves_ordered_typed_content_parts() {
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::Media {
            text: Some("image".into()),
            parts: vec![ContentPart::Image {
                media: MediaReference {
                    kind: MediaKind::Image,
                    url: Some("https://example.test/image.png".into()),
                    ..Default::default()
                },
                caption: None,
                alt_text: Some("preview".into()),
            }],
        },
        &ChannelStaticCapabilities::default(),
        &ChannelPresentationCapabilities::default(),
        BridgeTranslationOptions::default(),
    );
    let OutboundPayload::NativeChannelData { data } = &intents[0].payload else {
        panic!("expected typed media projection");
    };
    assert_eq!(data["parts"][0]["kind"], "image");
    assert_eq!(data["parts"][0]["altText"], "preview");
    assert_eq!(intents[0].thread_id, None);
    assert_eq!(
        intents[0]
            .conversation_ref
            .as_ref()
            .and_then(|reference| reference.topic_id.as_deref()),
        Some("topic-1")
    );
}

#[test]
fn harness_outbound_intent_preserves_full_turn_address() {
    let mut turn = turn();
    turn.envelope.channel.account_id = Some("account-opaque".into());
    turn.envelope.conversation.scope_id = Some("scope-opaque".into());
    turn.envelope.conversation.parent_id = Some("parent-opaque".into());
    turn.envelope.conversation.thread_id = Some("thread-opaque".into());
    turn.envelope.conversation.topic_id = Some("topic-opaque".into());
    let intents = translate_output_event(
        &turn,
        ChannelOutputEvent::FinalMessage {
            text: "reply".into(),
        },
        &ChannelStaticCapabilities::default(),
        &ChannelPresentationCapabilities::default(),
        BridgeTranslationOptions::default(),
    );
    let intent = &intents[0];
    assert_eq!(intent.channel_ref, Some(turn.envelope.channel.clone()));
    assert_eq!(
        intent.conversation_ref,
        Some(turn.envelope.conversation.clone())
    );
    assert_eq!(intent.thread_id.as_deref(), Some("thread-opaque"));
}

#[test]
fn harness_events_preserve_text_and_routing_metadata() {
    let events = [
        (
            ChannelOutputEvent::FinalMessage {
                text: "final".into(),
            },
            "final",
        ),
        (
            ChannelOutputEvent::ToolProgress {
                label: "tool".into(),
                message: Some("running".into()),
            },
            "tool: running",
        ),
        (
            ChannelOutputEvent::ToolProgress {
                label: "tool".into(),
                message: None,
            },
            "tool",
        ),
        (
            ChannelOutputEvent::Clarification {
                prompt: "which?".into(),
            },
            "which?",
        ),
        (
            ChannelOutputEvent::Cancellation {
                reason: Some("stopped".into()),
            },
            "stopped",
        ),
        (
            ChannelOutputEvent::Cancellation { reason: None },
            "Cancelled.",
        ),
    ];
    let mut turn = turn();
    turn.envelope.message_id.clear();
    turn.envelope.conversation.topic_id = None;
    turn.envelope.conversation.thread_id = Some("thread".into());
    for (event, text) in events {
        let intents = translate_output_event(
            &turn,
            event,
            &Default::default(),
            &Default::default(),
            BridgeTranslationOptions::default(),
        );
        assert_eq!(intents.len(), 1);
        assert_eq!(
            intents[0].payload,
            OutboundPayload::Text { text: text.into() }
        );
        assert_eq!(intents[0].idempotency_key, "turn-1:text");
        assert_eq!(intents[0].channel_id, "telegram");
        assert_eq!(intents[0].conversation_id, "chat-1");
        assert_eq!(intents[0].reply_to_id, None);
        assert_eq!(intents[0].thread_id.as_deref(), Some("thread"));
    }
}

#[test]
fn native_approval_requires_enough_actions_and_a_nonempty_row() {
    for (max_actions, max_actions_per_row, native) in [
        (Some(2), Some(2), true),
        (Some(2), None, true),
        (Some(1), Some(2), false),
        (Some(2), Some(0), false),
    ] {
        let intents = translate_output_event(
            &turn(),
            ChannelOutputEvent::ApprovalRequest {
                id: "approval".into(),
                prompt: "Run?".into(),
                choices: vec!["yes".into(), "no".into()],
            },
            &Default::default(),
            &ChannelPresentationCapabilities {
                max_actions,
                max_actions_per_row,
                ..Default::default()
            },
            BridgeTranslationOptions::default(),
        );
        if native {
            assert_eq!(
                intents[0].payload,
                OutboundPayload::PresentationBlocks {
                    blocks: serde_json::json!({"kind": "approval", "id": "approval", "prompt": "Run?", "choices": ["yes", "no"]})
                }
            );
        } else {
            assert_eq!(
                intents[0].payload,
                OutboundPayload::Text {
                    text: "Run?\n\nReply with one of: yes, no".into()
                }
            );
        }
    }
}

#[test]
fn lifecycle_and_native_events_preserve_structured_data() {
    let lifecycle = crate::harness::HarnessLifecycleEvent {
        stage: "started".into(),
        message: Some("ready".into()),
    };
    for (event, data) in [
        (
            ChannelOutputEvent::Lifecycle {
                stage: lifecycle.stage.clone(),
                message: lifecycle.message.clone(),
                iteration: None,
                max_iterations: None,
                stop: None,
            },
            serde_json::json!({"lifecycle": lifecycle}),
        ),
        (
            ChannelOutputEvent::Native {
                data: serde_json::json!({"custom": true}),
            },
            serde_json::json!({"custom": true}),
        ),
    ] {
        let intents = translate_output_event(
            &turn(),
            event,
            &Default::default(),
            &Default::default(),
            BridgeTranslationOptions::default(),
        );
        assert_eq!(
            intents[0].payload,
            OutboundPayload::NativeChannelData { data }
        );
        assert_eq!(intents[0].idempotency_key, "turn-1:native");
    }
}

#[test]
fn blocked_streaming_uses_text_even_when_edits_are_available() {
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::TextDelta {
            text: "delta".into(),
            iteration: None,
        },
        &ChannelStaticCapabilities {
            block_streaming: true,
            ..Default::default()
        },
        &ChannelPresentationCapabilities {
            supports_edit: true,
            ..Default::default()
        },
        BridgeTranslationOptions::default(),
    );
    assert_eq!(
        intents[0].payload,
        OutboundPayload::Text {
            text: "delta".into()
        }
    );
}

#[test]
fn tool_progress_projection_preserves_call_identity_arguments_and_result() {
    let event = ChannelOutputEvent::ToolCallCompleted {
        call_id: "call-7".into(),
        tool_name: "search".into(),
        success: true,
        output_chars: 13,
        output: "two results".into(),
        arguments: Some(serde_json::json!({"query": "weather"})),
        elapsed_ms: 42,
        iteration: 2,
        display_label: Some("Search the web".into()),
        display_detail: Some("weather".into()),
        structured: Some(serde_json::json!({"kind": "search", "count": 2})),
        failure: None,
    };
    let intents = translate_output_event(
        &turn(),
        event.clone(),
        &ChannelStaticCapabilities::default(),
        &ChannelPresentationCapabilities::default(),
        BridgeTranslationOptions::default(),
    );
    let OutboundPayload::NativeChannelData { data } = &intents[0].payload else {
        panic!("rich progress must remain typed data until the host projects it")
    };
    assert_eq!(*data, serde_json::to_value(event).unwrap());
}
