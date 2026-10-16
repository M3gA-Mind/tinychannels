use serde::{Deserialize, Serialize};

/// Legacy v21 message projection retained for one release during v22 migration.
/// New `DeliverInbound` callbacks carry [`crate::ChannelInboundEnvelope`].
///
/// Serde field names remain the v21 wire contract for compatibility shims.
#[deprecated(
    since = "0.1.34",
    note = "v21 compatibility shim; use ChannelInboundEnvelope for canonical v22 inbound content"
)]
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
    /// Provider message identifier this inbound message replies to.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "replyToId")]
    pub reply_to_id: Option<String>,
    /// Independent provider topic identifier where supported.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "topicId")]
    pub topic_id: Option<String>,
    /// The sender's display name as the platform gives it (WhatsApp push
    /// name, Telegram first and last name, Signal profile name, ...). Never
    /// a saved contact name: no channel exposes one. `None` when the
    /// platform gives none. Optional on the wire, so a host or module built
    /// before this field still decodes the message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
}

/// Legacy v21 text-only send projection retained for one release.
/// Canonical sends use [`crate::ChannelOutboundMessage`] through
/// `SendChannelIntent`.
#[deprecated(
    since = "0.1.34",
    note = "v21 compatibility shim; use ChannelOutboundMessage for canonical v22 sends"
)]
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SendMessage {
    pub content: String,
    pub recipient: String,
    pub subject: Option<String>,
    /// Platform thread identifier for threaded replies (e.g. Slack `thread_ts`).
    pub thread_ts: Option<String>,
    /// Independent provider topic identifier where supported.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "topicId")]
    pub topic_id: Option<String>,
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
            topic_id: None,
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
            topic_id: None,
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
    #[deprecated(
        since = "0.1.34",
        note = "v21 text-only provider adapter; implement send_typed for canonical v22 content"
    )]
    async fn send(&self, message: &SendMessage) -> anyhow::Result<()>;

    /// Deliver canonical typed content. Migrated providers override this; the
    /// default is the explicit text-only one-release compatibility projection.
    async fn send_typed(
        &self,
        message: &crate::channel::ChannelOutboundMessage,
    ) -> anyhow::Result<()> {
        self.send(&message.to_legacy_message()?).await
    }

    /// Legacy listener seam used by isolated test doubles.
    #[deprecated(
        since = "0.1.34",
        note = "v21 listener adapter; use typed DeliverInbound callbacks in v22 modules"
    )]
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
        let mut capabilities = crate::capabilities::ChannelCapabilities::unknown();
        capabilities.remote_control = self.supports_remote_control();
        capabilities.chat_approvals = self.supports_chat_approvals();
        capabilities.history_key_ignores_thread = self.history_key_ignores_thread();
        capabilities.progressive_edits = self.supports_draft_updates();
        capabilities.reactions = self.supports_reactions();
        capabilities.content.parts = vec!["text".into()];
        capabilities.content.text = crate::capabilities::ContentCapability {
            inbound: true,
            outbound: true,
            ..Default::default()
        };
        capabilities.actions.supported = vec!["send".into()];
        capabilities
    }

    /// Whether remote-control commands are supported by this configured instance.
    fn supports_remote_control(&self) -> bool {
        false
    }

    /// Whether approval prompts can be returned to this configured instance.
    fn supports_chat_approvals(&self) -> bool {
        false
    }

    /// Whether this provider treats a thread marker as a reply target only.
    fn history_key_ignores_thread(&self) -> bool {
        false
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
