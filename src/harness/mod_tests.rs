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
    )
    .unwrap();
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
    )
    .unwrap();
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
    )
    .unwrap();
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
    )
    .unwrap();
    let OutboundPayload::Parts { parts } = &intents[0].payload else {
        panic!("expected typed media projection");
    };
    assert_eq!(parts.len(), 2);
    let tinychannels_bus::OutboundContentPart::Content { part } = &parts[1] else {
        panic!("expected image content part")
    };
    assert!(matches!(part, ContentPart::Image { alt_text: Some(text), .. } if text == "preview"));
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
fn scoped_event_sequence_keys_are_stable_for_replay_and_distinct_between_events() {
    let event = ChannelOutputEvent::TextDelta {
        text: "same".into(),
        iteration: Some(1),
    };
    let translate = |sequence| {
        translate_output_event(
            &turn(),
            event.clone(),
            &Default::default(),
            &Default::default(),
            BridgeTranslationOptions {
                event_sequence: sequence,
                ..Default::default()
            },
        )
        .unwrap()[0]
            .idempotency_key
            .clone()
    };
    assert_eq!(translate(4), translate(4));
    assert_ne!(translate(4), translate(5));
}

#[test]
fn final_message_keeps_caption_and_media_as_ordered_provider_parts() {
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::FinalMessage {
            text: "caption".into(),
            parts: vec![
                tinychannels_bus::OutboundContentPart::Content {
                    part: ContentPart::Text {
                        text: "caption".into(),
                        markdown_dialect: None,
                    },
                },
                tinychannels_bus::OutboundContentPart::Content {
                    part: ContentPart::Image {
                        media: MediaReference {
                            kind: MediaKind::Image,
                            url: Some("https://example.test/final.png".into()),
                            ..Default::default()
                        },
                        caption: None,
                        alt_text: Some("final".into()),
                    },
                },
            ],
            receipt: None,
        },
        &Default::default(),
        &Default::default(),
        BridgeTranslationOptions::default(),
    )
    .unwrap();
    let OutboundPayload::Parts { parts } = &intents[0].payload else {
        panic!("final media must remain typed")
    };
    assert!(
        matches!(&parts[0], tinychannels_bus::OutboundContentPart::Content { part: ContentPart::Text { text, .. } } if text == "caption")
    );
    assert!(matches!(
        &parts[1],
        tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Image { .. }
        }
    ));
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
            parts: vec![],
            receipt: None,
        },
        &ChannelStaticCapabilities::default(),
        &ChannelPresentationCapabilities::default(),
        BridgeTranslationOptions::default(),
    )
    .unwrap();
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
                parts: vec![],
                receipt: None,
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
        )
        .unwrap();
        assert_eq!(intents.len(), 1);
        if text == "final" {
            assert!(matches!(
                &intents[0].payload,
                OutboundPayload::Parts { parts }
                    if matches!(parts.first(), Some(tinychannels_bus::OutboundContentPart::Content {
                        part: ContentPart::Text { text: caption, .. }
                    }) if caption == "final")
            ));
        } else {
            assert_eq!(
                intents[0].payload,
                OutboundPayload::Text { text: text.into() }
            );
        }
        assert!(intents[0].idempotency_key.starts_with("turn-1:"));
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
        )
        .unwrap();
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
fn final_parts_are_authoritative_over_legacy_text_projection() {
    let parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Text {
                text: "**already included**".into(),
                markdown_dialect: Some("commonmark".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Text {
                text: "second block".into(),
                markdown_dialect: Some("plain".into()),
            },
        },
    ];
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::FinalMessage {
            text: "**already included**".into(),
            parts: parts.clone(),
            receipt: None,
        },
        &Default::default(),
        &Default::default(),
        BridgeTranslationOptions::default(),
    )
    .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload, OutboundPayload::Parts { parts });
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
            serde_json::json!({"lifecycle": {
                "stage": lifecycle.stage,
                "message": lifecycle.message,
            }}),
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
        )
        .unwrap();
        assert_eq!(
            intents[0].payload,
            OutboundPayload::NativeChannelData { data }
        );
        assert!(intents[0].idempotency_key.starts_with("turn-1:"));
    }
}

#[test]
fn voice_note_projection_keeps_transcript_precedence_and_audio_order() {
    let voice = || tinychannels_bus::VoiceNote {
        media: MediaReference {
            kind: MediaKind::Audio,
            url: Some("https://example.test/voice.ogg".into()),
            ..Default::default()
        },
        codec: Some("opus".into()),
        duration_ms: Some(1200),
        transcript: Some(tinychannels_bus::VoiceTranscript {
            text: "recognized words".into(),
            language: Some("en".into()),
            state: tinychannels_bus::VoiceTranscriptState::Complete,
            source: tinychannels_bus::VoiceTranscriptSource::Provider,
        }),
        ..Default::default()
    };
    let translated = |text| {
        translate_output_event(
            &turn(),
            ChannelOutputEvent::VoiceNote {
                text,
                voice: voice(),
            },
            &Default::default(),
            &Default::default(),
            BridgeTranslationOptions::default(),
        )
        .unwrap()
    };

    for (text, expected) in [
        (None, "recognized words"),
        (Some("host transcript".into()), "host transcript"),
    ] {
        let intents = translated(text);
        let OutboundPayload::Parts { parts } = &intents[0].payload else {
            panic!("voice note must remain an ordered typed payload")
        };
        assert_eq!(parts.len(), 2);
        assert!(matches!(
            &parts[0],
            tinychannels_bus::OutboundContentPart::Content {
                part: ContentPart::Text { text, .. }
            } if text == expected
        ));
        assert!(matches!(
            &parts[1],
            tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    codec: Some(codec), duration_ms: Some(1200), ..
                }
            } if codec == "opus"
        ));
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
    )
    .unwrap();
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
    )
    .unwrap();
    let OutboundPayload::NativeChannelData { data } = &intents[0].payload else {
        panic!("rich progress must remain typed data until the host projects it")
    };
    assert_eq!(*data, serde_json::to_value(event).unwrap());
}

#[test]
fn lifecycle_projection_retains_iteration_budget_and_stop_reason() {
    let event = ChannelOutputEvent::Lifecycle {
        stage: "stopped".into(),
        message: Some("iteration limit".into()),
        iteration: Some(4),
        max_iterations: Some(4),
        stop: Some(tinychannels_bus::module_progressive::TurnStopSummary {
            kind: "budget".into(),
            failure_class: Some("limit".into()),
            operation: Some("run".into()),
        }),
    };
    let intents = translate_output_event(
        &turn(),
        event,
        &Default::default(),
        &Default::default(),
        BridgeTranslationOptions::default(),
    )
    .unwrap();
    assert_eq!(
        intents[0].payload,
        OutboundPayload::NativeChannelData {
            data: serde_json::json!({"lifecycle": {
                "stage": "stopped", "message": "iteration limit",
                "iteration": 4, "maxIterations": 4,
                "stop": {"kind": "budget", "failureClass": "limit", "operation": "run"},
            }}),
        }
    );
}

#[test]
fn voice_without_transcript_is_an_audio_part_without_an_invented_caption() {
    let voice = tinychannels_bus::VoiceNote {
        media: MediaReference {
            kind: MediaKind::Audio,
            url: Some("https://example.test/voice.ogg".into()),
            ..Default::default()
        },
        codec: Some("opus".into()),
        duration_ms: Some(42),
        ..Default::default()
    };
    let intents = translate_output_event(
        &turn(),
        ChannelOutputEvent::VoiceNote {
            text: None,
            voice: voice.clone(),
        },
        &Default::default(),
        &Default::default(),
        BridgeTranslationOptions::default(),
    )
    .unwrap();
    assert_eq!(
        intents[0].payload,
        OutboundPayload::Parts {
            parts: vec![tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: voice.media,
                    codec: voice.codec,
                    duration_ms: voice.duration_ms,
                },
            }],
        }
    );
}

#[test]
fn intent_replay_keys_distinguish_accounts_topics_and_turns() {
    let original = turn();
    let project = |turn: &ChannelTurn| {
        translate_output_event(
            turn,
            ChannelOutputEvent::Clarification {
                prompt: "which?".into(),
            },
            &Default::default(),
            &Default::default(),
            BridgeTranslationOptions::default(),
        )
        .unwrap()
        .remove(0)
    };
    let baseline = project(&original);
    assert_eq!(baseline.idempotency_key, project(&original).idempotency_key);
    for changed in [
        {
            let mut changed = original.clone();
            changed.envelope.channel.account_id = Some("another-account".into());
            changed
        },
        {
            let mut changed = original.clone();
            changed.envelope.conversation.topic_id = Some("another-topic".into());
            changed
        },
        {
            let mut changed = original.clone();
            changed.id = "another-turn".into();
            changed
        },
    ] {
        let intent = project(&changed);
        assert_ne!(baseline.idempotency_key, intent.idempotency_key);
        assert_eq!(intent.channel_ref, Some(changed.envelope.channel));
        assert_eq!(intent.conversation_ref, Some(changed.envelope.conversation));
    }
}
