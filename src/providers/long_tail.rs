//! Shared v2 adapter surface for the text-only long-tail providers (Lark,
//! Mattermost, IRC, QQ, DingTalk and Yuanbao).
//!
//! Each provider keeps its own transport and typed listener; this module gives
//! it the portable adapter traits so a configured account is described,
//! validated and driven through `ChannelAdapter` like Telegram and Slack. A
//! provider states what it supports once, in `LongTail::declared`, and every
//! operation it does not implement answers with an explicit `unsupported`
//! error instead of pretending to succeed.

use crate::traits::{Channel, SendMessage};
use std::collections::BTreeMap;
use tinychannels_bus::channel::{
    ChannelDescriptor, ChannelRef, ChannelSendError, ChannelSendOutcome, ConversationRef,
    MessageReceipt, MessageReceiptPart, MessageReceiptPartKind, SendErrorKind,
};
use tinychannels_bus::controllers::{ChannelAccountSnapshot, ChannelAccountState};
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ChannelCapabilities, ChannelOutboundIntent,
    ChannelOutboundMessage, ContentCapabilities, ContentCapability,
};

/// What a long-tail provider supports beyond plain text send.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TextSpec {
    /// A typing indicator can be started and stopped.
    pub typing: bool,
    /// Outbound messages may carry a thread or reference marker.
    pub thread: bool,
    /// The host must use the streaming reply parser although nothing is
    /// edited in place (the reply is delivered once, at finalize).
    pub streaming_pipeline: bool,
}

/// Capability facts for a text-only provider: remote control and chat
/// approvals, text in and out, and exactly the actions `spec` enables.
pub(crate) fn text_capabilities(spec: TextSpec) -> ChannelCapabilities {
    let mut actions = vec!["send".to_owned()];
    if spec.typing {
        actions.push("typing".into());
    }
    if spec.thread {
        actions.extend(["reply".to_owned(), "thread-reply".to_owned()]);
    }
    ChannelCapabilities {
        remote_control: true,
        chat_approvals: true,
        streaming_reply_pipeline: spec.streaming_pipeline,
        addressing: AddressingCapabilities {
            thread_id: spec.thread,
            ..Default::default()
        },
        content: ContentCapabilities {
            parts: vec!["text".into()],
            text: ContentCapability {
                inbound: true,
                outbound: true,
                ..Default::default()
            },
            ..Default::default()
        },
        actions: ActionCapabilities {
            supported: actions,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The complete typed adapter surface required by a native long-tail provider.
/// This deliberately does not inherit the legacy `Channel` transport trait.
/// The module keeps one typed object per configured account.
pub trait LongTailNative:
    crate::channel::ChannelAdapter
    + crate::channel::ChannelTypedSend
    + crate::channel::ChannelTyping
    + crate::channel::ChannelReaction
    + crate::channel::ChannelEdit
    + crate::channel::ChannelDelete
    + crate::channel::ChannelStreamingDraft
{
    /// Expose the configured native instance to provider-specific conformance
    /// setup. This is only compiled into tests so production adapters do not
    /// gain a downcast surface.
    #[cfg(test)]
    fn conformance_any(&self) -> &dyn std::any::Any;
}

impl<T> LongTailNative for T where
    T: crate::channel::ChannelAdapter
        + crate::channel::ChannelTypedSend
        + crate::channel::ChannelTyping
        + crate::channel::ChannelReaction
        + crate::channel::ChannelEdit
        + crate::channel::ChannelDelete
        + crate::channel::ChannelStreamingDraft
{
    #[cfg(test)]
    fn conformance_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A provider that is also a v2 text adapter.
#[async_trait::async_trait]
pub(crate) trait LongTail: Channel {
    /// Stable provider id (`mattermost`).
    const ID: &'static str;
    /// Human-readable name (`Mattermost`).
    const DISPLAY: &'static str;

    /// The facts this configured account declares.
    fn declared(&self) -> ChannelCapabilities;

    /// Provider-specific descriptor metadata for capabilities that depend on
    /// host services installed on this configured account.
    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        Vec::new()
    }

    /// Whether the typing indicator is implemented.
    fn typing_supported(&self) -> bool {
        false
    }

    /// Final provider-specific addressing applied to the text projection.
    fn address(&self, _message: &mut SendMessage) {}

    /// Run a provider-owned typed listener when this long-tail provider has
    /// migrated away from the module's legacy `Channel::listen` bridge.
    async fn start_listener(
        &self,
        _sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        // Until a provider opts into the typed listener path, the configured
        // module keeps owning its existing legacy listener.
        Ok(())
    }

    /// Stop a provider-owned typed listener and release its connection.
    async fn stop_listener(&self) -> Result<(), ChannelSendError> {
        Ok(())
    }

    /// Send one text message and return the platform message ids the provider
    /// actually reported. Providers whose API answers with an id override
    /// this; the default reports none, so the receipt stays empty rather than
    /// guessing.
    async fn send_reporting_ids(&self, message: &SendMessage) -> anyhow::Result<Vec<String>> {
        self.send(message).await.map(|()| Vec::new())
    }

    /// Declared providers with canonical media support implement typed sends
    /// through the shared scoped-media planner. Text-only providers retain the
    /// conservative legacy projection.
    async fn send_typed_message(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        send_text_typed::<Self>(self, message).await
    }
}

pub(crate) fn unsupported<T: LongTail + ?Sized>(operation: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: format!("{} does not support {operation}", T::DISPLAY),
        unsupported: true,
        ..Default::default()
    }
}

fn check_channel<T: LongTail + ?Sized>(
    channel_id: &str,
    channel_ref: Option<&tinychannels_bus::ChannelRef>,
) -> Result<(), ChannelSendError> {
    let id = tinychannels_bus::provider_id(channel_id);
    if id != T::ID || channel_ref.is_some_and(|reference| reference.id != T::ID) {
        return Err(ChannelSendError {
            kind: SendErrorKind::Forbidden,
            message: format!("channel does not match {}", T::DISPLAY),
            ..Default::default()
        });
    }
    Ok(())
}

pub(crate) fn descriptor<T: LongTail + ?Sized>(provider: &T) -> ChannelDescriptor {
    let mut metadata = BTreeMap::from([("adapterMode".into(), "native-v2".into())]);
    for (key, value) in provider.descriptor_extra() {
        metadata.insert(key.into(), value.into());
    }
    ChannelDescriptor {
        id: T::ID.into(),
        display_name: T::DISPLAY.into(),
        metadata,
        ..Default::default()
    }
}

pub(crate) async fn send_typed<T: LongTail + ?Sized>(
    provider: &T,
    message: &ChannelOutboundMessage,
) -> Result<ChannelSendOutcome, ChannelSendError> {
    check_channel::<T>(&message.channel_id, message.channel_ref.as_ref())?;
    provider.send_typed_message(message).await
}

async fn send_text_typed<T: LongTail + ?Sized>(
    provider: &T,
    message: &ChannelOutboundMessage,
) -> Result<ChannelSendOutcome, ChannelSendError> {
    if message.conversation_id.trim().is_empty() {
        return Err(ChannelSendError {
            kind: SendErrorKind::BadFormat,
            message: "conversation address is empty".into(),
            ..Default::default()
        });
    }
    let capabilities = provider.declared();
    if message.reply_to_id.is_some() && !capabilities.addressing.reply_to_id {
        return Err(unsupported::<T>("reply addressing"));
    }
    let has_thread = message.thread_id.is_some()
        || message
            .conversation_ref
            .as_ref()
            .is_some_and(|conversation| {
                conversation.thread_id.is_some() || conversation.topic_id.is_some()
            });
    if has_thread && !capabilities.addressing.thread_id {
        return Err(unsupported::<T>("thread addressing"));
    }
    // Only plain text is representable; anything richer is refused before any
    // request is made.
    let mut legacy = message
        .to_legacy_message()
        .map_err(|_| unsupported::<T>("typed content other than plain text"))?;
    provider.address(&mut legacy);
    tracing::debug!(
        channel = T::ID,
        chars = legacy.content.chars().count(),
        "[long-tail] sending text"
    );
    let ids = provider
        .send_reporting_ids(&legacy)
        .await
        .map_err(|error| {
            tracing::debug!(channel = T::ID, %error, "[long-tail] send failed");
            // The request may have reached the platform; never claim a retry
            // is safe.
            ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: format!("{} send failed", T::DISPLAY),
                sent_before_error: true,
                ..Default::default()
            }
        })?;
    Ok(ChannelSendOutcome {
        receipt: receipt::<T>(message, ids),
        error: None,
    })
}

/// The receipt for a delivered text message. It carries exactly the ids the
/// provider reported; a provider that reports none yields an empty receipt.
fn receipt<T: LongTail + ?Sized>(
    message: &ChannelOutboundMessage,
    ids: Vec<String>,
) -> MessageReceipt {
    if ids.is_empty() {
        return MessageReceipt::default();
    }
    let channel = message.channel_ref.clone().unwrap_or(ChannelRef {
        id: T::ID.into(),
        account_id: None,
    });
    let conversation = message
        .conversation_ref
        .clone()
        .unwrap_or_else(|| ConversationRef {
            id: message.conversation_id.clone(),
            ..Default::default()
        });
    MessageReceipt {
        idempotency_key: Some(message.idempotency_key.clone()),
        channel: Some(channel),
        conversation: Some(conversation),
        primary_platform_message_id: ids.first().cloned(),
        parts: ids
            .iter()
            .enumerate()
            .map(|(index, id)| MessageReceiptPart {
                platform_message_id: id.clone(),
                kind: MessageReceiptPartKind::Text,
                index,
                thread_id: message.thread_id.clone(),
                reply_to_id: message.reply_to_id.clone(),
                ..Default::default()
            })
            .collect(),
        platform_message_ids: ids,
        thread_id: message.thread_id.clone(),
        reply_to_id: message.reply_to_id.clone(),
        ..Default::default()
    }
}

pub(crate) async fn send_intent<T: LongTail + ?Sized>(
    provider: &T,
    intent: ChannelOutboundIntent,
) -> Result<ChannelSendOutcome, ChannelSendError> {
    check_channel::<T>(&intent.channel_id, intent.channel_ref.as_ref())?;
    send_typed(provider, &ChannelOutboundMessage::from_intent(&intent)).await
}

pub(crate) fn status<T: LongTail + ?Sized>() -> ChannelAccountSnapshot {
    ChannelAccountSnapshot {
        channel_id: T::ID.into(),
        states: vec![
            ChannelAccountState::Configured,
            ChannelAccountState::Enabled,
        ],
        health_state: Some("unknown".into()),
        ..Default::default()
    }
}

pub(crate) async fn typing<T: LongTail + ?Sized>(
    provider: &T,
    conversation_id: &str,
    start: bool,
) -> Result<(), ChannelSendError> {
    if !provider.typing_supported() {
        return Err(unsupported::<T>("typing indicators"));
    }
    let result = if start {
        provider.start_typing(conversation_id).await
    } else {
        provider.stop_typing(conversation_id).await
    };
    result.map_err(|error| {
        tracing::debug!(channel = T::ID, %error, "[long-tail] typing failed");
        ChannelSendError {
            kind: SendErrorKind::Transient,
            message: format!("{} typing indicator failed", T::DISPLAY),
            retryable: true,
            ..Default::default()
        }
    })
}

/// Implement the portable adapter traits for a [`LongTail`] provider.
macro_rules! impl_long_tail_adapter {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl $crate::channel::ChannelAdapter for $ty {
            fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
                $crate::providers::long_tail::descriptor(self)
            }
            fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
                $crate::providers::long_tail::LongTail::declared(self)
            }
            fn proactive_target(&self) -> Option<String> {
                $crate::traits::Channel::proactive_target(self)
            }
            async fn start(
                &self,
                sink: &(dyn $crate::channel::ChannelInboundSink + Send + Sync),
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::long_tail::LongTail::start_listener(self, sink).await
            }
            async fn stop(&self) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::long_tail::LongTail::stop_listener(self).await
            }
            async fn send(
                &self,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<
                tinychannels_bus::channel::ChannelSendOutcome,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::long_tail::send_intent(self, intent).await
            }
            async fn status(
                &self,
            ) -> Result<
                tinychannels_bus::controllers::ChannelAccountSnapshot,
                tinychannels_bus::channel::ChannelSendError,
            > {
                Ok($crate::providers::long_tail::status::<$ty>())
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelTypedSend for $ty {
            async fn send_typed(
                &self,
                message: &tinychannels_bus::ChannelOutboundMessage,
            ) -> Result<
                tinychannels_bus::channel::ChannelSendOutcome,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::long_tail::send_typed(self, message).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelTyping for $ty {
            async fn start_typing(
                &self,
                conversation_id: &str,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::long_tail::typing(self, conversation_id, true).await
            }
            async fn stop_typing(
                &self,
                conversation_id: &str,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::long_tail::typing(self, conversation_id, false).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelReaction for $ty {
            async fn send_reaction(
                &self,
                _channel: &tinychannels_bus::channel::ChannelRef,
                _conversation: &tinychannels_bus::channel::ConversationRef,
                _provider_message_id: &str,
                _reaction: &tinychannels_bus::channel::ChannelReaction,
                _remove: bool,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                Err($crate::providers::long_tail::unsupported::<$ty>(
                    "reactions",
                ))
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelEdit for $ty {
            async fn edit(
                &self,
                _receipt: &tinychannels_bus::channel::MessageReceipt,
                _intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<
                tinychannels_bus::channel::MessageReceipt,
                tinychannels_bus::channel::ChannelSendError,
            > {
                Err($crate::providers::long_tail::unsupported::<$ty>(
                    "message edits",
                ))
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelDelete for $ty {
            async fn delete(
                &self,
                _receipt: &tinychannels_bus::channel::MessageReceipt,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                Err($crate::providers::long_tail::unsupported::<$ty>(
                    "message deletion",
                ))
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelStreamingDraft for $ty {
            async fn upsert_draft(
                &self,
                _previous: Option<&tinychannels_bus::channel::MessageReceipt>,
                _intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<
                tinychannels_bus::channel::MessageReceipt,
                tinychannels_bus::channel::ChannelSendError,
            > {
                Err($crate::providers::long_tail::unsupported::<$ty>(
                    "in-place draft updates",
                ))
            }
        }
    };
}
pub(crate) use impl_long_tail_adapter;

#[cfg(test)]
#[path = "long_tail_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "long_tail_conformance_tests.rs"]
mod conformance_tests;
