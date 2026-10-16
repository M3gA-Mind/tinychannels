use serde::{Deserialize, Serialize};

/// A message received from or sent to a channel
///
/// Serde-derived because this type crosses the bus: the module forwards it to
/// the host's `DeliverInbound` callback. Field names are the wire contract —
/// renaming one is a decode failure at the far end, not a compile error.
///
/// `Default` is derived so a caller that fills only some fields can spread
/// `..Default::default()` and keep compiling when an optional field is added.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ChannelMessage {
    pub id: String,
    pub sender: String,
    pub reply_target: String,
    pub content: String,
    pub channel: String,
    pub timestamp: u64,
    /// Platform thread identifier (e.g. Slack `ts`, Discord thread ID).
    /// When set, replies should be posted as threaded responses.
    pub thread_ts: Option<String>,
    /// The sender's display name as the platform gives it (WhatsApp push
    /// name, Telegram first and last name, Signal profile name, ...). Never
    /// a saved contact name: no channel exposes one. `None` when the
    /// platform gives none. Optional on the wire, so a host or module built
    /// before this field still decodes the message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
}

/// Message to send through a channel
///
/// Serde-derived for the same reason as [`ChannelMessage`]: it is the payload
/// of the module's `SendMessage` member.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SendMessage {
    pub content: String,
    pub recipient: String,
    pub subject: Option<String>,
    /// Platform thread identifier for threaded replies (e.g. Slack `thread_ts`).
    pub thread_ts: Option<String>,
    /// Source message identifier for a provider reply.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "replyToId")]
    pub reply_to_id: Option<String>,
    /// Caller-provided or generated idempotency key for retry-safe delivery.
    pub idempotency_key: Option<String>,
}

impl SendMessage {
    /// Create a new message with content and recipient
    pub fn new(content: impl Into<String>, recipient: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            recipient: recipient.into(),
            subject: None,
            thread_ts: None,
            reply_to_id: None,
            idempotency_key: None,
        }
    }

    /// Create a new message with content, recipient, and subject
    pub fn with_subject(
        content: impl Into<String>,
        recipient: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            content: content.into(),
            recipient: recipient.into(),
            subject: Some(subject.into()),
            thread_ts: None,
            reply_to_id: None,
            idempotency_key: None,
        }
    }

    /// Set the thread identifier for threaded replies.
    pub fn in_thread(mut self, thread_ts: Option<String>) -> Self {
        self.thread_ts = thread_ts;
        self
    }

    /// Set an explicit idempotency key for retry-safe delivery.
    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        let idempotency_key = idempotency_key.into();
        self.idempotency_key = (!idempotency_key.trim().is_empty()).then_some(idempotency_key);
        self
    }
}

use async_trait::async_trait;

/// Provider-facing channel operations shared by the runtime and every host.
///
/// This is part of the contract crate so the runtime and hosts implement one
/// trait. The crate stays free of I/O: `async-trait` is a proc macro and
/// `tokio` is limited to the `sync` feature for the `listen` channel type.
///
/// `listen` remains available to focused test doubles and legacy local
/// adapters. Production module-backed channels receive inbound traffic through
/// their identity-scoped callback object instead.
#[async_trait]
pub trait Channel: Send + Sync {
    /// Stable provider name (for example, `telegram`).
    fn name(&self) -> &str;

    /// Configured default target for recipient-less proactive sends.
    fn proactive_target(&self) -> Option<String> {
        None
    }

    /// Deliver one message through the configured provider.
    async fn send(&self, message: &SendMessage) -> anyhow::Result<()>;

    /// Deliver canonical typed content. Migrated providers override this; the
    /// default is the explicit text-only one-release compatibility projection.
    async fn send_typed(
        &self,
        message: &crate::channel::ChannelOutboundMessage,
    ) -> anyhow::Result<()> {
        let content = message.legacy_text()?;
        self.send(&SendMessage {
            content,
            recipient: message.conversation_id.clone(),
            subject: None,
            thread_ts: message.thread_id.clone(),
            reply_to_id: message.reply_to_id.clone(),
            idempotency_key: Some(message.idempotency_key.clone()),
        })
        .await
    }

    /// Legacy listener seam used by isolated test doubles.
    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()>;

    /// Whether the configured provider is currently healthy.
    async fn health_check(&self) -> bool {
        true
    }

    /// Start a provider typing indicator.
    async fn start_typing(&self, _recipient: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// Stop a provider typing indicator.
    async fn stop_typing(&self, _recipient: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// Start a module-owned periodic refresh and return its opaque lifecycle
    /// handle. Implementations without provider-side refresh support return
    /// `None`; the host does not create a local refresh timer.
    async fn begin_typing_refresh(
        &self,
        _recipient: &str,
        _interval_secs: u64,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    /// Stop one refresh lifecycle by its opaque handle. Handles let concurrent
    /// turns type in the same conversation without canceling each other's
    /// provider task.
    async fn end_typing_refresh(&self, _handle: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// Instance-specific product interaction capabilities.
    fn capabilities(&self) -> crate::capabilities::ChannelCapabilities {
        crate::capabilities::capabilities_for(self.name())
    }

    /// Whether native emoji reactions are supported.
    fn supports_reactions(&self) -> bool {
        false
    }

    /// Whether progressive draft updates are supported.
    fn supports_draft_updates(&self) -> bool {
        false
    }

    /// Send the initial progressive draft and return its provider message id.
    async fn send_draft(&self, _message: &SendMessage) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    /// Update a progressive draft.
    async fn update_draft(
        &self,
        _recipient: &str,
        _message_id: &str,
        _text: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Finalize a progressive draft.
    async fn finalize_draft(
        &self,
        _recipient: &str,
        _message_id: &str,
        _text: &str,
        _thread_ts: Option<&str>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Convert and send one message using the owning provider's idempotency
    /// contract. Module-backed implementations override this to perform
    /// `PrepareOutboundIntent` and `SendChannelIntent` through TinyBus.
    async fn send_idempotent(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.send(message).await
    }
}

/// Extension helpers for host delivery consumers.
#[async_trait]
pub trait ChannelSendExt: Channel {
    /// Send one message with the provider's deterministic idempotency handling.
    async fn send_with_outbound_intent(&self, message: &SendMessage) -> anyhow::Result<()> {
        Channel::send_idempotent(self, message).await
    }
}

impl<T: Channel + ?Sized> ChannelSendExt for T {}

#[cfg(test)]
#[path = "traits_tests.rs"]
mod tests;
