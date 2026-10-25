//! Adapter traits for direct providers, relay connectors, and host bridges.

use crate::channel::{
    ChannelDescriptor, ChannelInboundEnvelope, ChannelOutboundIntent, ChannelSendError,
    MessageReceipt,
};
use crate::controllers::ChannelAccountSnapshot;
use async_trait::async_trait;
use serde_json::Value;

pub use tinychannels_bus::channel::adapter::*;

/// Sink used by adapters to deliver normalized inbound envelopes.
#[async_trait]
pub trait ChannelInboundSink: Send + Sync {
    async fn push(&self, envelope: ChannelInboundEnvelope) -> Result<(), ChannelSendError>;

    /// Deliver one normalized event. Existing sinks remain source-compatible;
    /// sinks that do not consume provider events return an explicit unsupported
    /// result until they opt into this additive surface.
    async fn push_event(
        &self,
        _event: tinychannels_bus::ChannelEvent,
    ) -> Result<(), ChannelSendError> {
        Err(ChannelSendError::unsupported(
            "inbound events are not supported by this sink",
        ))
    }

    /// Unified message/event ingress for providers. The default preserves all
    /// existing envelope-only sinks while allowing new sinks to handle events.
    async fn push_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), ChannelSendError> {
        match ingress {
            tinychannels_bus::ChannelIngress::Inbound(envelope) => self.push(envelope).await,
            tinychannels_bus::ChannelIngress::Event(event) => self.push_event(event).await,
        }
    }
}

/// Base channel adapter contract.
#[async_trait]
pub trait ChannelAdapter: Send + Sync {
    fn descriptor(&self) -> ChannelDescriptor;

    /// Compatibility identity accessor for providers that still scope media
    /// against their authenticated account. New adapters should populate the
    /// account in their descriptor and media scope instead.
    fn media_account_identity(&self) -> Option<String> {
        None
    }

    /// Capabilities declared by this configured provider instance. The
    /// default is conservative, so older adapters do not accidentally claim
    /// operations they have not implemented.
    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        let mut capabilities = tinychannels_bus::ChannelCapabilities::unknown();
        capabilities.content.parts.push("text".into());
        capabilities.content.text.outbound = true;
        capabilities.actions.supported.push("send".into());
        capabilities
    }

    fn receive_ack_policy(&self) -> ChannelReceiveAckPolicy {
        ChannelReceiveAckPolicy::AfterReceiveRecord
    }

    /// Compatibility projection for proactive channel delivery.
    fn proactive_target(&self) -> Option<String> {
        None
    }

    fn supports_webhook_delivery(&self) -> bool {
        false
    }

    async fn deliver_webhook(
        &self,
        _signature: Option<&str>,
        _body: &[u8],
    ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
        anyhow::bail!("provider has no typed webhook ingress")
    }

    fn supports_webhook_verification(&self) -> bool {
        false
    }

    fn verify_webhook(&self, _mode: &str, _verify_token: &str, _challenge: &str) -> Option<String> {
        None
    }

    async fn start(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError>;

    async fn stop(&self) -> Result<(), ChannelSendError>;

    async fn send(&self, intent: ChannelOutboundIntent)
    -> Result<MessageReceipt, ChannelSendError>;

    /// Validate canonical typed content before it enters a durable queue.
    /// Adapters that support rich content should override this alongside
    /// `send_typed`; the default accepts only the explicit legacy projection.
    fn validate_typed_send(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<(), ChannelSendError> {
        message
            .to_legacy_message()
            .map(|_| ())
            .map_err(|_| unsupported_content_error())
    }

    async fn mark_read(
        &self,
        _conversation: &tinychannels_bus::channel::ConversationRef,
        _provider_message_id: &str,
    ) -> Result<(), ChannelSendError> {
        Err(ChannelSendError::unsupported(
            "provider does not support read receipts",
        ))
    }

    /// Send while preserving partial-delivery information. The default wraps
    /// the legacy receipt-only result; adapters that can report an
    /// undelivered tail may override it without changing existing callers.
    async fn send_outcome(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ChannelSendError> {
        self.send(intent)
            .await
            .map(|receipt| tinychannels_bus::channel::ChannelSendOutcome {
                receipt,
                error: None,
            })
    }

    /// Send a canonical typed message. Older adapters retain a safe text-only
    /// projection by default; provider implementations override this method
    /// when they support rich content or voice.
    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ChannelSendError> {
        let text = message
            .parts
            .iter()
            .map(|part| match part {
                tinychannels_bus::OutboundContentPart::Content {
                    part:
                        tinychannels_bus::ContentPart::Text {
                            text,
                            markdown_dialect: None,
                        },
                } => Ok(text.as_str()),
                tinychannels_bus::OutboundContentPart::Content {
                    part:
                        tinychannels_bus::ContentPart::Text {
                            markdown_dialect: Some(_),
                            ..
                        },
                } => Err(unsupported_content_error()),
                _ => Err(unsupported_content_error()),
            })
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        let intent = ChannelOutboundIntent {
            idempotency_key: message.idempotency_key.clone(),
            channel_id: message.channel_id.clone(),
            conversation_id: message.conversation_id.clone(),
            channel_ref: message.channel_ref.clone(),
            conversation_ref: message.conversation_ref.clone(),
            reply_to_id: message.reply_to_id.clone(),
            thread_id: message.thread_id.clone(),
            turn_id: message.turn_id.clone(),
            payload: tinychannels_bus::OutboundPayload::Text { text },
            ..Default::default()
        };
        self.send_outcome(intent).await
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError>;
}

fn unsupported_content_error() -> ChannelSendError {
    ChannelSendError::unsupported("adapter does not support this typed content")
}

/// Optional setup/config extension.
#[async_trait]
pub trait ChannelSetup: Send + Sync {
    async fn validate_setup(&self, input: Value) -> Result<Value, ChannelSendError>;
}

/// Optional directory extension.
#[async_trait]
pub trait ChannelDirectory: Send + Sync {
    async fn list_conversations(&self, query: Option<&str>)
    -> Result<Vec<Value>, ChannelSendError>;
}

/// Optional target resolver extension.
#[async_trait]
pub trait ChannelResolver: Send + Sync {
    async fn resolve_target(&self, target: &str) -> Result<Value, ChannelSendError>;
}

/// Optional typing indicator extension.
#[async_trait]
pub trait ChannelTyping: Send + Sync {
    async fn start_typing(&self, conversation_id: &str) -> Result<(), ChannelSendError>;
    async fn stop_typing(&self, conversation_id: &str) -> Result<(), ChannelSendError>;
}

/// Optional reaction extension.
#[async_trait]
pub trait ChannelReaction: Send + Sync {
    async fn send_reaction(
        &self,
        conversation_id: &str,
        message_id: &str,
        reaction: &str,
    ) -> Result<(), ChannelSendError>;
}

/// Temporary adapter for providers with scoped, removable reactions. The
/// current portable trait has a smaller surface; keeping this extension
/// preserves provider behavior while callers migrate.
#[async_trait]
pub trait LegacyChannelReaction: Send + Sync {
    async fn send_reaction(
        &self,
        channel: &tinychannels_bus::channel::ChannelRef,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError>;
}

/// Optional edit extension.
#[async_trait]
pub trait ChannelEdit: Send + Sync {
    async fn edit(
        &self,
        receipt: &MessageReceipt,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError>;
}

/// Optional delete/unsend extension.
#[async_trait]
pub trait ChannelDelete: Send + Sync {
    async fn delete(&self, receipt: &MessageReceipt) -> Result<(), ChannelSendError>;
}

/// Optional streaming draft extension.
#[async_trait]
pub trait ChannelStreamingDraft: Send + Sync {
    async fn start_draft(
        &self,
        _intent: ChannelOutboundIntent,
    ) -> Result<Option<String>, ChannelSendError> {
        Err(ChannelSendError::unsupported(
            "provider does not implement draft start",
        ))
    }

    async fn update_draft(
        &self,
        _draft_id: &str,
        _intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        Err(ChannelSendError::unsupported(
            "provider does not implement draft update",
        ))
    }

    async fn finalize_draft(
        &self,
        _draft_id: &str,
        _intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        Err(ChannelSendError::unsupported(
            "provider does not implement draft finalization",
        ))
    }

    async fn upsert_draft(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        let _ = (previous, intent);
        Err(ChannelSendError::unsupported(
            "provider does not implement draft upsert",
        ))
    }
}

/// Provider-specific rich-content sender retained during the adapter port.
#[async_trait]
pub trait ChannelTypedSend: Send + Sync {
    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ChannelSendError>;
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
