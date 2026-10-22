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
        tinychannels_bus::ChannelCapabilities::unknown()
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
    assert!(error.unsupported);
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
