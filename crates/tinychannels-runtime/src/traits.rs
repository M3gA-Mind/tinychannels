//! In-process provider seams and legacy send helpers.
// V21 DTO use is confined to the compatibility traits in this module.
#![allow(deprecated)]

use tinychannels_bus::ChannelOutboundMessage;
pub use tinychannels_bus::{ChannelMessage, SendMessage};

use async_trait::async_trait;

/// Provider-facing channel operations shared by the runtime and every host.
///
/// This provider-facing runtime seam is implemented by channel providers and
/// consumed by hosts. Runtime dependencies stay in `tinychannels-runtime`, not
/// the data-only contract crate.
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
    async fn send_typed(&self, message: &ChannelOutboundMessage) -> anyhow::Result<()> {
        self.send(&message.to_legacy_message()?).await
    }

    /// Validate a typed send before it enters the provider actor. The default
    /// validates the one-release text-only projection; native typed providers
    /// should override this with their own content and addressing checks.
    fn validate_typed_send(&self, message: &ChannelOutboundMessage) -> anyhow::Result<()> {
        Ok(message.to_legacy_message().map(|_| ())?)
    }

    /// Send canonical content and return the provider receipt when the actual
    /// provider response supplies message IDs. Legacy adapters remain usable;
    /// they return an empty receipt rather than guessing an ID.
    async fn send_typed_with_receipt(
        &self,
        message: &ChannelOutboundMessage,
    ) -> anyhow::Result<crate::channel::ChannelSendOutcome> {
        self.send_typed(message).await?;
        Ok(crate::channel::ChannelSendOutcome::default())
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
        capabilities.progressive_edits = self.supports_progressive_edits();
        capabilities.streaming_reply_pipeline = self.supports_draft_updates();
        capabilities.reactions = self.supports_reactions();
        capabilities.addressing.reply_to_id = self.supports_reply_to_id();
        capabilities.addressing.thread_id = self.supports_thread_id();
        capabilities.addressing.topic_id = self.supports_topic_id();
        capabilities.content.parts = vec!["text".into()];
        capabilities.content.text = crate::capabilities::ContentCapability {
            inbound: true,
            outbound: true,
            ..Default::default()
        };
        capabilities.actions.supported = vec!["send".into()];
        if capabilities.progressive_edits {
            capabilities.actions.supported.push("draft-update".into());
        }
        if self.supports_edit() {
            capabilities.actions.supported.push("edit".into());
        }
        if capabilities.addressing.reply_to_id || capabilities.addressing.thread_id {
            capabilities.actions.supported.push("reply".into());
        }
        if capabilities.addressing.thread_id {
            capabilities.actions.supported.push("thread-reply".into());
        }
        if capabilities.reactions {
            capabilities
                .actions
                .supported
                .extend(["react".into(), "reactions".into()]);
        }
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

    /// Whether progressive draft operations are needed for the provider's
    /// streaming reply pipeline. This does not necessarily mean that the
    /// provider displays those drafts to the user.
    fn supports_draft_updates(&self) -> bool {
        false
    }

    /// Whether the provider can visibly update a progressive reply while a
    /// turn is running. Defaults to the legacy draft-update declaration.
    fn supports_progressive_edits(&self) -> bool {
        self.supports_draft_updates()
    }

    /// Whether an existing sent message can be edited independently of draft streaming.
    fn supports_edit(&self) -> bool {
        false
    }

    /// Whether outbound messages may address a source message directly.
    fn supports_reply_to_id(&self) -> bool {
        false
    }

    /// Whether outbound messages may address a conversation thread.
    fn supports_thread_id(&self) -> bool {
        false
    }

    /// Whether provider conversations can be addressed by an independent topic identifier.
    fn supports_topic_id(&self) -> bool {
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

/// A webhook delivery refused before ingestion because its signature, body,
/// or configured account scope is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookRejected(pub String);

impl std::fmt::Display for WebhookRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WebhookRejected {}

/// Runtime idempotency construction for the legacy send DTO.
pub trait SendMessageExt {
    fn with_deterministic_idempotency_key(self, channel_id: impl AsRef<str>) -> Self;
}
impl SendMessageExt for SendMessage {
    /// Generate a deterministic idempotency key from this legacy send shape.
    fn with_deterministic_idempotency_key(mut self, channel_id: impl AsRef<str>) -> Self {
        let intent = crate::channel::outbound_intent_from_send_message(channel_id.as_ref(), &self);
        self.idempotency_key = Some(intent.idempotency_key);
        self
    }
}

#[cfg(test)]
#[path = "traits_tests.rs"]
mod tests;
