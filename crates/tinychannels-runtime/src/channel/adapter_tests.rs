use super::*;

struct DefaultAdapter;

#[async_trait]
impl ChannelAdapter for DefaultAdapter {
    fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor::default()
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
    ) -> Result<crate::channel::ChannelSendOutcome, ChannelSendError> {
        Ok(crate::channel::ChannelSendOutcome::default())
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        Ok(ChannelAccountSnapshot::default())
    }
}

#[async_trait]
impl ChannelStreamingDraft for DefaultAdapter {
    async fn upsert_draft(
        &self,
        _previous: Option<&MessageReceipt>,
        _intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        unreachable!("the compatibility default lifecycle must not call upsert_draft")
    }
}

#[test]
fn default_adapter_facts_are_conservative() {
    let adapter = DefaultAdapter;
    assert_eq!(
        adapter.declared_capabilities(),
        tinychannels_bus::ChannelCapabilities::unknown()
    );
    assert_eq!(adapter.proactive_target(), None);
    assert_eq!(
        adapter.receive_ack_policy(),
        ChannelReceiveAckPolicy::AfterReceiveRecord
    );
}

#[tokio::test]
async fn default_webhook_operations_are_explicitly_unsupported() {
    let adapter = DefaultAdapter;
    assert!(!adapter.supports_webhook_delivery());
    assert!(!adapter.supports_webhook_verification());

    let error = adapter
        .deliver_webhook(Some("signature"), b"exact request bytes")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no typed webhook ingress"));
    assert_eq!(
        adapter.verify_webhook("subscribe", "token", "challenge"),
        None
    );
}

#[tokio::test]
async fn default_read_receipt_operation_is_explicitly_unsupported() {
    let adapter = DefaultAdapter;
    let error = adapter
        .mark_read(
            &tinychannels_bus::channel::ConversationRef {
                id: "conversation-1".into(),
                ..Default::default()
            },
            "provider-message-1",
        )
        .await
        .unwrap_err();
    assert!(error.unsupported);
    assert!(!error.retryable);
}

#[tokio::test]
async fn typed_draft_lifecycle_defaults_refuse_unimplemented_operations() {
    let adapter = DefaultAdapter;
    let start = ChannelStreamingDraft::start_draft(&adapter, ChannelOutboundIntent::default())
        .await
        .unwrap_err();
    let update =
        ChannelStreamingDraft::update_draft(&adapter, "draft-1", ChannelOutboundIntent::default())
            .await
            .unwrap_err();
    let finalize = ChannelStreamingDraft::finalize_draft(
        &adapter,
        "draft-1",
        ChannelOutboundIntent::default(),
    )
    .await
    .unwrap_err();

    for error in [start, update, finalize] {
        assert!(error.unsupported);
        assert!(!error.retryable);
    }
}
