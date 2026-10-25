use super::*;
use std::sync::Mutex;

struct ReceiptOnlyAdapter;

#[async_trait]
impl ChannelAdapter for ReceiptOnlyAdapter {
    fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor {
            id: "legacy".into(),
            ..Default::default()
        }
    }

    async fn start(
        &self,
        _sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn send(
        &self,
        _intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        Ok(MessageReceipt {
            primary_platform_message_id: Some("posted-1".into()),
            ..Default::default()
        })
    }

    async fn status(&self) -> Result<crate::controllers::ChannelAccountSnapshot, ChannelSendError> {
        Ok(Default::default())
    }
}

#[tokio::test]
async fn receipt_only_adapters_get_safe_capability_and_outcome_defaults() {
    let adapter = ReceiptOnlyAdapter;
    assert_eq!(
        adapter.declared_capabilities(),
        tinychannels_bus::ChannelCapabilities {
            content: tinychannels_bus::capabilities::ContentCapabilities {
                parts: vec!["text".into()],
                text: tinychannels_bus::capabilities::ContentCapability {
                    outbound: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            actions: tinychannels_bus::ActionCapabilities {
                supported: vec!["send".into()],
                ..Default::default()
            },
            ..tinychannels_bus::ChannelCapabilities::unknown()
        }
    );

    let outcome = adapter
        .send_outcome(ChannelOutboundIntent::default())
        .await
        .expect("legacy send succeeds");
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("posted-1")
    );
    assert!(outcome.error.is_none());
}

#[tokio::test]
async fn legacy_adapter_defaults_decline_optional_provider_operations() {
    let adapter = ReceiptOnlyAdapter;
    assert_eq!(adapter.media_account_identity(), None);
    assert_eq!(
        adapter.receive_ack_policy(),
        ChannelReceiveAckPolicy::AfterReceiveRecord
    );
    assert_eq!(adapter.proactive_target(), None);
    assert!(!adapter.supports_webhook_delivery());
    assert!(!adapter.supports_webhook_verification());
    assert_eq!(adapter.verify_webhook("mode", "token", "challenge"), None);

    let delivery_error = adapter
        .deliver_webhook(None, b"payload")
        .await
        .expect_err("webhook ingress is opt-in");
    assert!(
        delivery_error
            .to_string()
            .contains("no typed webhook ingress")
    );

    let read_error = adapter
        .mark_read(&Default::default(), "provider-message")
        .await
        .expect_err("read receipts are opt-in");
    assert!(read_error.is_unsupported());

    let text = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "legacy".into(),
        conversation_id: "room".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: None,
        idempotency_key: "validate-1".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "hello".into(),
                markdown_dialect: None,
            },
        }],
    };
    assert!(adapter.validate_typed_send(&text).is_ok());

    let rich = tinychannels_bus::ChannelOutboundMessage {
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: Default::default(),
                caption: None,
                alt_text: None,
            },
        }],
        ..text
    };
    assert!(adapter.validate_typed_send(&rich).is_err());
}

struct LegacyDraftAdapter;

#[async_trait]
impl ChannelStreamingDraft for LegacyDraftAdapter {}

#[tokio::test]
async fn legacy_streaming_draft_defaults_return_unsupported() {
    let adapter = LegacyDraftAdapter;
    assert!(
        adapter
            .start_draft(ChannelOutboundIntent::default())
            .await
            .expect_err("draft start is opt-in")
            .is_unsupported()
    );
    assert!(
        adapter
            .update_draft("draft-1", ChannelOutboundIntent::default())
            .await
            .expect_err("draft updates are opt-in")
            .is_unsupported()
    );
    assert!(
        adapter
            .finalize_draft("draft-1", ChannelOutboundIntent::default())
            .await
            .expect_err("draft finalization is opt-in")
            .is_unsupported()
    );
    assert!(
        adapter
            .upsert_draft(None, ChannelOutboundIntent::default())
            .await
            .expect_err("draft upsert is opt-in")
            .is_unsupported()
    );
}

#[tokio::test]
async fn receipt_only_adapters_project_text_but_reject_rich_typed_content() {
    let adapter = ReceiptOnlyAdapter;
    let text = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "legacy".into(),
        conversation_id: "room".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: None,
        idempotency_key: "send-1".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "hello".into(),
                markdown_dialect: None,
            },
        }],
    };
    let outcome = adapter.send_typed(&text).await.expect("text fallback");
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("posted-1")
    );

    let image = tinychannels_bus::ChannelOutboundMessage {
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: Default::default(),
                caption: None,
                alt_text: None,
            },
        }],
        ..text.clone()
    };
    let error = adapter
        .send_typed(&image)
        .await
        .expect_err("legacy adapter must refuse rich content explicitly");
    assert!(error.is_unsupported());

    let formatted = tinychannels_bus::ChannelOutboundMessage {
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "**hello**".into(),
                markdown_dialect: Some("commonmark".into()),
            },
        }],
        ..text
    };
    let error = adapter
        .send_typed(&formatted)
        .await
        .expect_err("legacy adapter must not discard the text dialect");
    assert!(error.is_unsupported());
}

#[derive(Default)]
struct RecordingSink {
    messages: Mutex<Vec<ChannelInboundEnvelope>>,
    events: Mutex<Vec<tinychannels_bus::ChannelEvent>>,
}

#[async_trait]
impl ChannelInboundSink for RecordingSink {
    async fn push(&self, envelope: ChannelInboundEnvelope) -> Result<(), ChannelSendError> {
        self.messages.lock().unwrap().push(envelope);
        Ok(())
    }

    async fn push_event(
        &self,
        event: tinychannels_bus::ChannelEvent,
    ) -> Result<(), ChannelSendError> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

struct EnvelopeOnlySink;

#[async_trait]
impl ChannelInboundSink for EnvelopeOnlySink {
    async fn push(&self, _envelope: ChannelInboundEnvelope) -> Result<(), ChannelSendError> {
        Ok(())
    }
}

#[tokio::test]
async fn ingress_keeps_envelope_only_sinks_compatible() {
    EnvelopeOnlySink
        .push_ingress(tinychannels_bus::ChannelIngress::Inbound(
            ChannelInboundEnvelope::default(),
        ))
        .await
        .expect("legacy sink accepts envelope ingress");

    let error = EnvelopeOnlySink
        .push_ingress(tinychannels_bus::ChannelIngress::Event(
            tinychannels_bus::ChannelEvent {
                version: 2,
                event_id: "event-1".into(),
                source_event_id: None,
                source_timestamp: None,
                channel: Default::default(),
                conversation: None,
                event: tinychannels_bus::ChannelEventKind::Lifecycle {
                    state: "connected".into(),
                    reason: None,
                },
            },
        ))
        .await
        .expect_err("event-only extension must fail explicitly by default");
    assert!(error.is_unsupported());
}

#[tokio::test]
async fn ingress_routes_events_to_sinks_that_opt_in() {
    let sink = RecordingSink::default();
    let event = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "event-1".into(),
        source_event_id: None,
        source_timestamp: None,
        channel: Default::default(),
        conversation: None,
        event: tinychannels_bus::ChannelEventKind::Lifecycle {
            state: "connected".into(),
            reason: None,
        },
    };

    sink.push_ingress(tinychannels_bus::ChannelIngress::Event(event.clone()))
        .await
        .expect("event sink accepts event ingress");
    assert_eq!(sink.events.lock().unwrap().as_slice(), &[event]);
    assert!(sink.messages.lock().unwrap().is_empty());
}
