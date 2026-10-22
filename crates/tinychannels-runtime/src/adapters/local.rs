//! In-process adapter for host-owned channel surfaces.

use crate::channel::{
    ChannelAdapter, ChannelDescriptor, ChannelInboundSink, ChannelOutboundIntent, ChannelSendError,
    ChannelStaticCapabilities, MessageReceipt, OutboundPayload,
};
use crate::controllers::{ChannelAccountSnapshot, ChannelAccountState};
use async_trait::async_trait;
use std::sync::Arc;

/// Host callback used by `LocalChannelAdapter` to perform delivery.
#[async_trait]
pub trait LocalOutboundSink: Send + Sync {
    async fn send(&self, intent: ChannelOutboundIntent)
    -> Result<MessageReceipt, ChannelSendError>;
}

/// Minimal generic adapter for local/API/webhook style host integrations.
pub struct LocalChannelAdapter {
    descriptor: ChannelDescriptor,
    capabilities: tinychannels_bus::ChannelCapabilities,
    outbound: Arc<dyn LocalOutboundSink>,
}

impl LocalChannelAdapter {
    pub fn new(
        descriptor: ChannelDescriptor,
        capabilities: tinychannels_bus::ChannelCapabilities,
        outbound: Arc<dyn LocalOutboundSink>,
    ) -> Self {
        Self {
            descriptor,
            capabilities,
            outbound,
        }
    }

    pub fn static_capabilities(&self) -> ChannelStaticCapabilities {
        ChannelStaticCapabilities::from(&self.capabilities)
    }

    pub fn capabilities(&self) -> &tinychannels_bus::ChannelCapabilities {
        &self.capabilities
    }
}

#[async_trait]
impl ChannelAdapter for LocalChannelAdapter {
    fn descriptor(&self) -> ChannelDescriptor {
        self.descriptor.clone()
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.capabilities.clone()
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
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.outbound.send(intent).await
    }

    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ChannelSendError> {
        let receipt = self
            .outbound
            .send(ChannelOutboundIntent {
                idempotency_key: message.idempotency_key.clone(),
                channel_id: message.channel_id.clone(),
                conversation_id: message.conversation_id.clone(),
                channel_ref: message.channel_ref.clone(),
                conversation_ref: message.conversation_ref.clone(),
                reply_to_id: message.reply_to_id.clone(),
                thread_id: message.thread_id.clone(),
                turn_id: message.turn_id.clone(),
                payload: OutboundPayload::Parts {
                    parts: message.parts.clone(),
                },
                ..Default::default()
            })
            .await?;
        Ok(tinychannels_bus::channel::ChannelSendOutcome {
            receipt,
            error: None,
        })
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        Ok(ChannelAccountSnapshot {
            channel_id: self.descriptor.id.clone(),
            account_id: self.descriptor.account_id.clone(),
            states: vec![
                ChannelAccountState::Configured,
                ChannelAccountState::Enabled,
            ],
            ..Default::default()
        })
    }
}
