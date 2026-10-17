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
        validate_scope(&self.descriptor, &intent)?;
        let supported = match &intent.payload {
            OutboundPayload::Text { .. } => self.capabilities.content.text.outbound,
            OutboundPayload::Media { .. } => self.capabilities.content.image.outbound,
            OutboundPayload::Voice { voice } => match voice {
                tinychannels_bus::OutboundVoice::Media { .. } => {
                    self.capabilities.voice_out.supported
                }
                tinychannels_bus::OutboundVoice::Synthesize { .. } => {
                    self.capabilities.voice_out.supported && self.capabilities.voice_out.synthesis
                }
            },
            OutboundPayload::Files { .. } => self.capabilities.content.file.outbound,
            OutboundPayload::Parts { parts } => parts.iter().all(|part| match part {
                tinychannels_bus::OutboundContentPart::Voice { voice } => match voice {
                    tinychannels_bus::OutboundVoice::Media { .. } => {
                        self.capabilities.voice_out.supported
                    }
                    tinychannels_bus::OutboundVoice::Synthesize { .. } => {
                        self.capabilities.voice_out.supported
                            && self.capabilities.voice_out.synthesis
                    }
                },
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
                        tinychannels_bus::ContentPart::Sticker { .. } => {
                            self.capabilities.content.sticker.outbound
                        }
                        tinychannels_bus::ContentPart::Location { .. } => {
                            self.capabilities.content.location.outbound
                        }
                        tinychannels_bus::ContentPart::Contact { .. } => {
                            self.capabilities.content.contact.outbound
                        }
                        tinychannels_bus::ContentPart::Buttons { .. } => {
                            self.capabilities.content.buttons.outbound
                        }
                        tinychannels_bus::ContentPart::Poll { .. } => {
                            self.capabilities.content.poll.outbound
                        }
                        tinychannels_bus::ContentPart::Presentation { .. } => {
                            self.capabilities.content.presentation.outbound
                        }
                        tinychannels_bus::ContentPart::Native { .. } => {
                            self.capabilities.content.native.outbound
                        }
                        tinychannels_bus::ContentPart::VoiceNote { .. } => {
                            self.capabilities.voice_out.supported
                        }
                },
            }),
            _ => false,
        };
        if !supported {
            return Err(ChannelSendError {
                kind: SendErrorKind::BadFormat,
                message: "configured adapter does not declare this outbound payload".into(),
                unsupported: true,
                retryable: false,
                ..Default::default()
            });
        }
        let mut outcome = self.outbound.send(intent.clone()).await.map(|mut receipt| {
            receipt.idempotency_key.get_or_insert(intent.idempotency_key);
            receipt.channel.get_or_insert_with(|| {
                tinychannels_bus::ChannelRef {
                    id: self.descriptor.id.clone(),
                    account_id: self.descriptor.account_id.clone(),
                }
            });
            receipt.conversation.get_or_insert_with(|| {
                intent.conversation_ref.clone().unwrap_or_else(|| {
                    tinychannels_bus::ConversationRef {
                        id: intent.conversation_id.clone(),
                        thread_id: intent.thread_id.clone(),
                        ..Default::default()
                    }
                })
            });
            crate::channel::ChannelSendOutcome {
                receipt,
                error: None,
            }
        })?;
        let expected_channel = tinychannels_bus::ChannelRef {
            id: self.descriptor.id.clone(),
            account_id: self.descriptor.account_id.clone(),
        };
        let mut expected_conversation = intent.conversation_ref.clone().unwrap_or_else(|| {
            tinychannels_bus::ConversationRef {
                id: intent.conversation_id.clone(),
                thread_id: intent.thread_id.clone(),
                ..Default::default()
            }
        });
        if expected_conversation.thread_id.is_none() {
            expected_conversation.thread_id.clone_from(&intent.thread_id);
        }
        if outcome.receipt.channel.as_ref() != Some(&expected_channel)
            || outcome.receipt.conversation.as_ref() != Some(&expected_conversation)
        {
            outcome.error = Some(ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: "provider receipt scope does not match configured delivery".into(),
                sent_before_error: true,
                retryable: false,
                ..Default::default()
            });
        }
        Ok(outcome)
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

fn validate_scope(
    descriptor: &ChannelDescriptor,
    intent: &ChannelOutboundIntent,
) -> Result<(), ChannelSendError> {
    let expected_channel = tinychannels_bus::provider_id(&descriptor.id);
    if tinychannels_bus::provider_id(&intent.channel_id) != expected_channel
        || intent.channel_ref.as_ref().is_some_and(|reference| {
            reference.id != expected_channel
                || reference.account_id != descriptor.account_id
        })
        || intent.conversation_id.trim().is_empty()
        || intent.conversation_ref.as_ref().is_some_and(|conversation| {
            conversation.id != intent.conversation_id
                || (intent.thread_id.is_some()
                    && conversation.thread_id.is_some()
                    && conversation.thread_id != intent.thread_id)
        })
    {
        return Err(ChannelSendError {
            kind: SendErrorKind::Forbidden,
            message: "channel or conversation address does not match configured adapter".into(),
            retryable: false,
            ..Default::default()
        });
    }
    Ok(())
}
