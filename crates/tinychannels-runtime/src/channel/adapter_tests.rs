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
