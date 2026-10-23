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
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("idem-1")
    );
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
    assert!(poll.unsupported);
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
}

#[test]
fn default_clock_reports_wall_time() {
    assert!(unix_ms() > 1_700_000_000_000);
    let sink = Arc::new(RecordingSink::default());
    let adapter = WebChannelAdapter::new(None, WebScope::local(), sink);
    assert!((adapter.clock)() > 0);
}
