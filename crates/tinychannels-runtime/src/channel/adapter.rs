//! Adapter traits for direct providers, relay connectors, and host bridges.

use crate::channel::{ChannelDescriptor, ChannelOutboundIntent, ChannelSendError, MessageReceipt};
use crate::controllers::ChannelAccountSnapshot;
use async_trait::async_trait;
use serde_json::Value;

pub use tinychannels_bus::channel::adapter::*;

/// Sink used by adapters to deliver normalized inbound envelopes.
#[async_trait]
pub trait ChannelInboundSink: Send + Sync {
    /// Publish an ordered ingress item. Messages and lifecycle events share
    /// this one seam so providers cannot reorder events through side queues.
    async fn push(&self, ingress: tinychannels_bus::ChannelIngress)
    -> Result<(), ChannelSendError>;
}

/// Base channel adapter contract.
#[async_trait]
pub trait ChannelAdapter: Send + Sync {
    fn descriptor(&self) -> ChannelDescriptor;

    /// Stable authenticated account identity learned by this live adapter,
    /// when the provider exposes one. Hosts use it to scope durable media and
    /// fail closed while the identity is unknown or changes mid-session.
    fn media_account_identity(&self) -> Option<String> {
        None
    }

    /// Capability facts for this configured account, sourced from its provider.
    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        tinychannels_bus::ChannelCapabilities::unknown()
    }

    fn proactive_target(&self) -> Option<String> {
        None
    }

    fn receive_ack_policy(&self) -> ChannelReceiveAckPolicy {
        ChannelReceiveAckPolicy::AfterReceiveRecord
    }

    /// Whether raw provider webhook delivery is implemented by this adapter.
    fn supports_webhook_delivery(&self) -> bool {
        false
    }

    /// Authenticate and ingest exact raw request bytes through the provider.
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

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<crate::channel::ChannelSendOutcome, ChannelSendError>;

    /// Mark one provider message as read when this account exposes native
    /// read receipts. Adapters without that operation fail explicitly.
    async fn mark_read(
        &self,
        _conversation: &tinychannels_bus::channel::ConversationRef,
        _provider_message_id: &str,
    ) -> Result<(), ChannelSendError> {
        Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
            message: "provider does not support read receipts".into(),
            unsupported: true,
            retryable: false,
            ..Default::default()
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
    /// Begin a provider-owned draft lifecycle from a typed outbound intent.
    /// `None` is a valid marker for pipelines that stream internally and only
    /// publish their final response. Implementations that do not own this
    /// lifecycle return an explicit unsupported error.
    async fn start_draft(
        &self,
        _intent: ChannelOutboundIntent,
    ) -> Result<Option<String>, ChannelSendError> {
        Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
            message: "provider does not implement typed draft start".into(),
            unsupported: true,
            retryable: false,
            ..Default::default()
        })
    }

    /// Update the provider-owned draft identified by `draft_id`.
    async fn update_draft(
        &self,
        _draft_id: &str,
        _intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
            message: "provider does not implement typed draft update".into(),
            unsupported: true,
            retryable: false,
            ..Default::default()
        })
    }

    /// Finalize the provider-owned draft, preserving provider-specific
    /// overflow, completion, and send behavior.
    async fn finalize_draft(
        &self,
        _draft_id: &str,
        _intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
            message: "provider does not implement typed draft finalization".into(),
            unsupported: true,
            retryable: false,
            ..Default::default()
        })
    }

    async fn upsert_draft(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError>;
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;

/// Typed send executor for providers whose rich message surface is native.
#[async_trait]
pub trait ChannelTypedSend: Send + Sync {
    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<crate::channel::ChannelSendOutcome, ChannelSendError>;
}
