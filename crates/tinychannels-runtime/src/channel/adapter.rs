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
        Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
            message: "inbound events are not supported by this sink".into(),
            unsupported: true,
            retryable: false,
            ..Default::default()
        })
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

    /// Capabilities declared by this configured provider instance. The
    /// default is conservative, so older adapters do not accidentally claim
    /// operations they have not implemented.
    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        tinychannels_bus::ChannelCapabilities::unknown()
    }

    fn receive_ack_policy(&self) -> ChannelReceiveAckPolicy {
        ChannelReceiveAckPolicy::AfterReceiveRecord
    }

    async fn start(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError>;

    async fn stop(&self) -> Result<(), ChannelSendError>;

    async fn send(&self, intent: ChannelOutboundIntent)
    -> Result<MessageReceipt, ChannelSendError>;

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

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError>;
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
    async fn upsert_draft(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError>;
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
