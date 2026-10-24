use super::*;
use crate::channel::{ChannelInboundEnvelope, DeliveryDurability};
use tinychannels_bus::channel::{MediaReference, OutboundVoice};

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<(WebScope, WebChannelEvent)>>,
    fail: AtomicBool,
}

#[async_trait]
impl WebEventSink for RecordingSink {
    async fn publish(
        &self,
        scope: &WebScope,
        event: WebChannelEvent,
    ) -> Result<(), ChannelSendError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(err(SendErrorKind::Transient, "sink down", true));
        }
        self.events.lock().push((scope.clone(), event));
        Ok(())
    }
}

#[derive(Default)]
struct RecordingInbound {
    pushed: Mutex<Vec<ChannelInboundEnvelope>>,
}

#[async_trait]
impl ChannelInboundSink for RecordingInbound {
    async fn push(&self, envelope: ChannelInboundEnvelope) -> Result<(), ChannelSendError> {
        self.pushed.lock().push(envelope);
        Ok(())
    }
}

fn adapter() -> (WebChannelAdapter, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    let adapter = WebChannelAdapter::new(
        Some("acct".into()),
        WebScope::for_agent("agent-1"),
        sink.clone(),
    )
    .with_clock(|| 42);
    (adapter, sink)
}

fn delta(text: &str) -> ChannelOutputEvent {
    ChannelOutputEvent::TextDelta {
        text: text.into(),
        iteration: None,
    }
}

fn intent(payload: OutboundPayload) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "idem-1".into(),
        channel_id: "web".into(),
        conversation_id: "thread-9".into(),
        durability: DeliveryDurability::BestEffort,
        payload,
        ..Default::default()
    }
}

fn seqs(sink: &RecordingSink) -> Vec<u64> {
    sink.events
        .lock()
        .iter()
        .map(|(_, event)| {
            serde_json::to_value(event).unwrap()["seq"]
                .as_u64()
                .unwrap_or(0)
        })
        .collect()
}

#[tokio::test]
async fn submit_requires_a_running_adapter_then_pushes_the_envelope() {
    let (adapter, _) = adapter();
    let inbound = RecordingInbound::default();
    let start = WebChatStart {
        client_id: "c".into(),
        thread_id: "t".into(),
        request_id: "r".into(),
        text: "hi".into(),
        ..Default::default()
    };
    let error = adapter.submit(&start, &inbound).await.unwrap_err();
    assert!(error.retryable);

    adapter.start(&inbound).await.unwrap();
    adapter.submit(&start, &inbound).await.unwrap();
    let pushed = inbound.pushed.lock();
    let envelope = &pushed[0];
    assert_eq!(envelope.channel.account_id.as_deref(), Some("acct"));
    assert_eq!(envelope.text, "hi");
}

#[tokio::test]
async fn submit_surfaces_normalization_errors() {
    let (adapter, _) = adapter();
    let inbound = RecordingInbound::default();
    adapter.start(&inbound).await.unwrap();
    let error = adapter
        .submit(&WebChatStart::default(), &inbound)
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(inbound.pushed.lock().is_empty());
}

#[tokio::test]
async fn publish_output_stamps_seq_per_turn_and_resets_after_a_terminal_event() {
    let (adapter, sink) = adapter();
    assert_eq!(
        adapter
            .publish_output("c", "t", "r1", delta("a"))
            .await
            .unwrap(),
        1
    );
    adapter
        .publish_output("c", "t", "r1", delta("b"))
        .await
        .unwrap();
    adapter
        .publish_output("c", "t", "r2", delta("x"))
        .await
        .unwrap();
    adapter
        .publish_output(
            "c",
            "t",
            "r1",
            ChannelOutputEvent::FinalMessage {
                text: "ab".into(),
                parts: Vec::new(),
                receipt: None,
            },
        )
        .await
        .unwrap();
    adapter
        .publish_output("c", "t", "r1", delta("again"))
        .await
        .unwrap();
    assert_eq!(seqs(&sink), [1, 2, 1, 3, 1]);
    let events = sink.events.lock();
    assert_eq!(events[0].0, WebScope::for_agent("agent-1"));
    assert_eq!(serde_json::to_value(&events[0].1).unwrap()["ts"], 42);
}

#[tokio::test(flavor = "current_thread")]
async fn publish_output_serializes_same_turn_sequence_and_sink_delivery() {
    use tokio::sync::Notify;

    struct OrderedSink {
        events: Mutex<Vec<u64>>,
        first_entered: Notify,
        release_first: Notify,
    }

    #[async_trait]
    impl WebEventSink for OrderedSink {
        async fn publish(
            &self,
            _scope: &WebScope,
            event: WebChannelEvent,
        ) -> Result<(), ChannelSendError> {
            let seq = serde_json::to_value(event).unwrap()["seq"]
                .as_u64()
                .unwrap_or_default();
            if seq == 1 {
                self.first_entered.notify_one();
                self.release_first.notified().await;
            }
            self.events.lock().push(seq);
            Ok(())
        }
    }

    let sink = Arc::new(OrderedSink {
        events: Mutex::new(Vec::new()),
        first_entered: Notify::new(),
        release_first: Notify::new(),
    });
    let adapter = Arc::new(WebChannelAdapter::new(
        Some("acct".into()),
        WebScope::for_agent("agent-1"),
        sink.clone(),
    ));

    let first_adapter = adapter.clone();
    let first = tokio::spawn(async move {
        first_adapter
            .publish_output("c", "t", "r", delta("one"))
            .await
    });
    sink.first_entered.notified().await;

    let second_adapter = adapter.clone();
    let second = tokio::spawn(async move {
        second_adapter
            .publish_output("c", "t", "r", delta("two"))
            .await
    });
    tokio::task::yield_now().await;
    sink.release_first.notify_one();

    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(*sink.events.lock(), [1, 2]);
}

#[tokio::test]
async fn failed_terminal_publication_keeps_projector_state() {
    let (adapter, sink) = adapter();
    sink.fail.store(true, Ordering::SeqCst);
    assert!(
        adapter
            .publish_output(
                "c",
                "t",
                "r",
                ChannelOutputEvent::FinalMessage {
                    text: "done".into(),
                    parts: Vec::new(),
                    receipt: None,
                },
            )
            .await
            .is_err()
    );
    assert_eq!(adapter.turns.lock().len(), 1);
}

#[tokio::test]
async fn failed_terminal_output_keeps_its_sequence_until_delivery_succeeds() {
    let (adapter, sink) = adapter();
    adapter
        .publish_output("c", "t", "r", delta("a"))
        .await
        .unwrap();
    sink.fail.store(true, Ordering::SeqCst);
    let terminal = ChannelOutputEvent::FinalMessage {
        text: "done".into(),
        parts: Vec::new(),
        receipt: None,
    };
    assert!(
        adapter
            .publish_output("c", "t", "r", terminal.clone())
            .await
            .is_err()
    );

    sink.fail.store(false, Ordering::SeqCst);
    adapter
        .publish_output("c", "t", "r", terminal)
        .await
        .unwrap();
    adapter
        .publish_output("c", "t", "r", delta("next"))
        .await
        .unwrap();
    assert_eq!(seqs(&sink), [1, 3, 1]);
}

#[tokio::test]
async fn host_terminal_event_keeps_projector_on_failure_and_clears_after_success() {
    let (adapter, sink) = adapter();
    adapter
        .publish_output("c", "t", "r", delta("a"))
        .await
        .unwrap();
    let terminal = WebChannelEvent::ChatDone {
        context: WebEventContext {
            client_id: "c".into(),
            thread_id: "t".into(),
            request_id: "r".into(),
            ..Default::default()
        },
        full_response: Some("done".into()),
        citations: None,
        usage: None,
        timing: None,
        parts: None,
    };

    sink.fail.store(true, Ordering::SeqCst);
    assert!(adapter.publish_event(terminal.clone()).await.is_err());
    sink.fail.store(false, Ordering::SeqCst);
    adapter
        .publish_output("c", "t", "r", delta("still same turn"))
        .await
        .unwrap();
    adapter.publish_event(terminal).await.unwrap();
    adapter
        .publish_output("c", "t", "r", delta("new turn"))
        .await
        .unwrap();

    assert_eq!(seqs(&sink), [1, 2, 0, 1]);
}

#[tokio::test]
async fn unrepresentable_output_publishes_nothing_and_sink_errors_propagate() {
    let (adapter, sink) = adapter();
    let none = adapter
        .publish_output(
            "c",
            "t",
            "r",
            ChannelOutputEvent::Native {
                data: serde_json::json!({}),
            },
        )
        .await
        .unwrap();
    assert_eq!(none, 0);
    assert!(sink.events.lock().is_empty());

    sink.fail.store(true, Ordering::SeqCst);
    let error = adapter
        .publish_output("c", "t", "r", delta("a"))
        .await
        .unwrap_err();
    assert!(error.retryable);
    assert!(
        adapter
            .publish_event(WebChannelEvent::InferenceStart {
                context: WebEventContext::default()
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn host_events_pass_through_unchanged() {
    let (adapter, sink) = adapter();
    let event = WebChannelEvent::InferenceHeartbeat {
        context: WebEventContext {
            client_id: "c".into(),
            thread_id: "t".into(),
            request_id: "r".into(),
            ..Default::default()
        },
    };
    adapter.publish_event(event.clone()).await.unwrap();
    assert_eq!(sink.events.lock()[0].1, event);
}

#[tokio::test]
async fn text_send_publishes_a_proactive_message_and_returns_a_receipt() {
    let (adapter, sink) = adapter();
    let mut request = intent(OutboundPayload::Text {
        text: "Scheduled update".into(),
    });
    request.thread_id = Some("proactive:system".into());
    request.reply_to_id = Some("m0".into());
    let outcome = adapter.send_outcome(request).await.unwrap();
    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.primary_platform_message_id, None);
    assert!(outcome.receipt.platform_message_ids.is_empty());
    assert_eq!(outcome.receipt.sent_at, 42);
    assert_eq!(
        outcome.receipt.channel.unwrap().account_id.as_deref(),
        Some("acct")
    );
    let wire = serde_json::to_value(&sink.events.lock()[0].1).unwrap();
    assert_eq!(wire["event"], "proactive_message");
    assert_eq!(wire["client_id"], "system");
    assert_eq!(wire["thread_id"], "proactive:system");
    assert_eq!(wire["request_id"], "idem-1");
    assert_eq!(wire["full_response"], "Scheduled update");
    assert_eq!(wire["success"], true);
}

#[tokio::test]
async fn parts_and_voice_sends_keep_their_typed_content() {
    let (adapter, sink) = adapter();
    let mut parts = intent(OutboundPayload::Parts {
        parts: vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "one".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "two".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::Image {
                    media: MediaReference::default(),
                    caption: None,
                    alt_text: None,
                },
            },
        ],
    });
    parts.turn_id = Some("turn-3".into());
    adapter.send(parts).await.unwrap();
    adapter
        .send(intent(OutboundPayload::Voice {
            voice: OutboundVoice::Synthesize {
                text: "hi".into(),
                voice: None,
            },
        }))
        .await
        .unwrap();
    let events = sink.events.lock();
    let first = serde_json::to_value(&events[0].1).unwrap();
    assert_eq!(first["full_response"], "one\ntwo");
    assert_eq!(first["request_id"], "turn-3");
    assert_eq!(first["parts"].as_array().map(Vec::len), Some(3));
    let second = serde_json::to_value(&events[1].1).unwrap();
    assert!(second.get("full_response").is_none());
    assert_eq!(second["parts"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn send_typed_preserves_parts_through_the_current_adapter_contract() {
    let (adapter, sink) = adapter();
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "web".into(),
        conversation_id: "thread-9".into(),
        channel_ref: Some(tinychannels_bus::channel::ChannelRef {
            id: "web".into(),
            account_id: Some("acct".into()),
        }),
        conversation_ref: Some(tinychannels_bus::channel::ConversationRef {
            id: "thread-9".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: None,
        idempotency_key: "typed-1".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("image-1".into()),
                    ..Default::default()
                },
                caption: Some("picture".into()),
                alt_text: None,
            },
        }],
    };

    let outcome = adapter.send_typed(&message).await.unwrap();
    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.idempotency_key.as_deref(), Some("typed-1"));
    let wire = serde_json::to_value(&sink.events.lock()[0].1).unwrap();
    assert_eq!(wire["parts"][0]["part"]["kind"], "image");
    assert_eq!(wire["parts"][0]["part"]["media"]["opaqueId"], "image-1");
}

#[tokio::test]
async fn send_typed_rejects_empty_parts_without_publishing() {
    let (adapter, sink) = adapter();
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "web".into(),
        conversation_id: "thread-9".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: None,
        idempotency_key: "typed-empty".into(),
        parts: vec![],
    };

    let error = adapter.send_typed(&message).await.unwrap_err();
    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(sink.events.lock().is_empty());
}

#[tokio::test]
async fn send_typed_rejects_parts_the_web_adapter_does_not_declare() {
    let (adapter, sink) = adapter();
    for part in [
        ContentPart::Poll {
            question: "q".into(),
            options: vec!["a".into()],
            multiple_choice: false,
            closes_at_unix_ms: None,
            provider_poll_id: None,
        },
        ContentPart::Location {
            latitude: 1.0,
            longitude: 2.0,
            label: None,
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: None,
        },
        ContentPart::Native {
            provider: "web".into(),
            data: serde_json::json!({}),
        },
    ] {
        let message = tinychannels_bus::ChannelOutboundMessage {
            channel_id: "web".into(),
            conversation_id: "thread-9".into(),
            channel_ref: None,
            conversation_ref: None,
            reply_to_id: None,
            thread_id: None,
            turn_id: None,
            idempotency_key: "typed-unsupported".into(),
            parts: vec![OutboundContentPart::Content { part }],
        };
        assert!(
            adapter
                .send_typed(&message)
                .await
                .unwrap_err()
                .is_unsupported()
        );
    }
    assert!(sink.events.lock().is_empty());
}

#[tokio::test]
async fn proactive_typed_send_uses_conversation_reference_thread_id() {
    let (adapter, sink) = adapter();
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "web".into(),
        conversation_id: "parent-conversation".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::channel::ConversationRef {
            id: "parent-conversation".into(),
            thread_id: Some("reply-thread".into()),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: None,
        idempotency_key: "typed-thread".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Text {
                text: "reply".into(),
                markdown_dialect: None,
            },
        }],
    };

    adapter.send_typed(&message).await.unwrap();
    let wire = serde_json::to_value(&sink.events.lock()[0].1).unwrap();
    assert_eq!(wire["thread_id"], "reply-thread");
}

#[tokio::test]
async fn send_rejects_foreign_addresses_and_undeclared_payloads() {
    let (adapter, sink) = adapter();
    let mut foreign = intent(OutboundPayload::Text { text: "x".into() });
    foreign.channel_id = "telegram".into();
    assert_eq!(
        adapter.send(foreign).await.unwrap_err().kind,
        SendErrorKind::Forbidden
    );

    let mut blank = intent(OutboundPayload::Text { text: "x".into() });
    blank.conversation_id = " ".into();
    assert_eq!(
        adapter.send(blank).await.unwrap_err().kind,
        SendErrorKind::Forbidden
    );

    let poll = adapter
        .send(intent(OutboundPayload::Poll {
            question: "q".into(),
            options: Vec::new(),
        }))
        .await
        .unwrap_err();
    assert!(poll.is_unsupported());
    assert!(sink.events.lock().is_empty());
}

#[tokio::test]
async fn status_and_capabilities_reflect_lifecycle_and_declared_content() {
    let (adapter, _) = adapter();
    let inbound = RecordingInbound::default();
    let states = |snapshot: ChannelAccountSnapshot| snapshot.states;
    assert!(states(adapter.status().await.unwrap()).contains(&ChannelAccountState::Disabled));
    adapter.start(&inbound).await.unwrap();
    assert!(states(adapter.status().await.unwrap()).contains(&ChannelAccountState::Enabled));
    adapter
        .publish_output("c", "t", "r", delta("a"))
        .await
        .unwrap();
    adapter.stop().await.unwrap();
    assert!(adapter.turns.lock().is_empty());
    assert!(states(adapter.status().await.unwrap()).contains(&ChannelAccountState::Disabled));

    let descriptor = adapter.descriptor();
    assert_eq!(descriptor.id, "web");
    assert_eq!(descriptor.account_id.as_deref(), Some("acct"));
    let capabilities = adapter.declared_capabilities();
    assert!(capabilities.voice_in.supported && capabilities.voice_out.supported);
    assert!(capabilities.content.image.inbound && capabilities.content.file.outbound);
    assert!(capabilities.chat_approvals);
    assert!(capabilities.addressing.thread_id);
    assert_eq!(capabilities.addressing.conversation_types, ["dm"]);
}

#[test]
fn default_clock_reports_wall_time() {
    assert!(unix_ms() > 1_700_000_000_000);
    let sink = Arc::new(RecordingSink::default());
    let adapter = WebChannelAdapter::new(None, WebScope::local(), sink);
    assert!((adapter.clock)() > 0);
}
