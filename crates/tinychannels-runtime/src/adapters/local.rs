//! In-process adapter for host-owned channel surfaces.

use crate::channel::{
    ChannelAdapter, ChannelDescriptor, ChannelInboundSink, ChannelOutboundIntent, ChannelSendError,
    ChannelStaticCapabilities, MessageReceipt,
};
use crate::controllers::{ChannelAccountSnapshot, ChannelAccountState};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::{OutboundPayload, channel::SendErrorKind};

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
    ) -> Result<crate::channel::ChannelSendOutcome, ChannelSendError> {
        let supported = match &intent.payload {
            OutboundPayload::Text { .. } => self.capabilities.content.text.outbound,
            OutboundPayload::Media { .. } => self.capabilities.content.image.outbound,
            OutboundPayload::Voice { .. } => self.capabilities.voice_out.supported,
            OutboundPayload::Files { .. } => self.capabilities.content.file.outbound,
            OutboundPayload::Parts { parts } => parts.iter().all(|part| match part {
                tinychannels_bus::OutboundContentPart::Voice { .. } => {
                    self.capabilities.voice_out.supported
                }
                tinychannels_bus::OutboundContentPart::Content { part } => match part {
                    tinychannels_bus::ContentPart::Text { .. } => {
                        self.capabilities.content.text.outbound
                    }
                    tinychannels_bus::ContentPart::Image { .. } => {
                        self.capabilities.content.image.outbound
                    }
                    tinychannels_bus::ContentPart::Video { .. } => {
                        self.capabilities.content.video.outbound
                    }
                    tinychannels_bus::ContentPart::Audio { .. } => {
                        self.capabilities.content.audio.outbound
                    }
                    tinychannels_bus::ContentPart::File { .. } => {
                        self.capabilities.content.file.outbound
                    }
                    _ => false,
                },
            }),
            _ => false,
        };
        if !supported {
            return Err(ChannelSendError {
                kind: SendErrorKind::Unsupported,
                message: "configured adapter does not declare this outbound payload".into(),
                retryable: false,
                ..Default::default()
            });
        }
        self.outbound
            .send(intent)
            .await
            .map(|receipt| crate::channel::ChannelSendOutcome {
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
