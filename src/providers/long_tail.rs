//! Shared v2 adapter surface for the text-only long-tail providers (Lark,
//! Mattermost, IRC, QQ, DingTalk and Yuanbao).
//!
//! Each provider keeps its own transport and typed listener; this module gives
//! it the portable adapter traits so a configured account is described,
//! validated and driven through `ChannelAdapter` like Telegram and Slack. A
//! provider states what it supports once, in `LongTail::declared`, and every
//! operation it does not implement answers with an explicit `unsupported`
//! error instead of pretending to succeed.

use std::collections::BTreeMap;
use tinychannels_bus::channel::{
    ChannelDescriptor, ChannelSendError, ChannelSendOutcome, SendErrorKind,
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
    + crate::channel::LegacyChannelReaction
    + crate::channel::ChannelEdit
    + crate::channel::ChannelDelete
    + crate::channel::ChannelStreamingDraft
{
}

impl<T> LongTailNative for T where
    T: crate::channel::ChannelAdapter
        + crate::channel::ChannelTypedSend
        + crate::channel::ChannelTyping
        + crate::channel::LegacyChannelReaction
        + crate::channel::ChannelEdit
        + crate::channel::ChannelDelete
        + crate::channel::ChannelStreamingDraft
{
}

/// The listener and descriptor hooks used by the long-tail v2 adapter macro.
/// Sending is implemented on the typed outbound port; this contract does not
/// inherit or call the legacy `Channel` transport trait.
#[async_trait::async_trait]
pub(crate) trait LongTail: Send + Sync {
    /// Stable provider id (`mattermost`).
    const ID: &'static str;
    /// Human-readable name (`Mattermost`).
    const DISPLAY: &'static str;

    /// The facts this configured account declares.
    fn declared(&self) -> ChannelCapabilities;

    /// Configured default recipient, if this provider has one.
    fn proactive_target(&self) -> Option<String> {
        None
    }

    /// Provider-specific descriptor metadata for capabilities that depend on
    /// host services installed on this configured account.
    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        Vec::new()
    }

    /// Probe the configured provider when it has a bounded native health
    /// check. Providers without one retain an honest unknown state.
    async fn health_state(&self) -> &'static str {
        "unknown"
    }

    /// Whether the typing indicator is implemented.
    fn typing_supported(&self) -> bool {
        false
    }

    /// Start or stop a provider-native typing indicator.
    async fn set_typing(
        &self,
        _conversation_id: &str,
        _start: bool,
    ) -> Result<(), ChannelSendError> {
        Err(unsupported::<Self>("typing indicators"))
    }

    /// Run the provider-owned typed listener.
    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError>;

    /// Stop a provider-owned typed listener and release its connection.
    async fn stop_listener(&self) -> Result<(), ChannelSendError>;

    /// Send a typed message using the provider's native transport.
    async fn send_typed_message(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError>;

    /// Send or remove a platform-native reaction when this provider exposes it.
    async fn react(
        &self,
        _channel: &tinychannels_bus::ChannelRef,
        _conversation: &tinychannels_bus::ConversationRef,
        _provider_message_id: &str,
        _reaction: &tinychannels_bus::channel::ChannelReaction,
        _remove: bool,
    ) -> Result<(), ChannelSendError> {
        Err(unsupported::<Self>("reactions"))
    }

    /// Edit a previously returned provider receipt when supported.
    async fn edit(
        &self,
        _receipt: &tinychannels_bus::channel::MessageReceipt,
        _intent: ChannelOutboundIntent,
    ) -> Result<tinychannels_bus::channel::MessageReceipt, ChannelSendError> {
        Err(unsupported::<Self>("message edits"))
    }

    /// Delete a previously returned provider receipt when supported.
    async fn delete(
        &self,
        _receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> Result<(), ChannelSendError> {
        Err(unsupported::<Self>("message deletion"))
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

pub(super) fn check_channel<T: LongTail + ?Sized>(
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
    let result = provider.set_typing(conversation_id, start).await;
    result.map_err(|error| {
        tracing::debug!(channel = T::ID, ?error, "[long-tail] typing failed");
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
                $crate::providers::long_tail::LongTail::proactive_target(self)
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
                tinychannels_bus::channel::MessageReceipt,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::long_tail::send_intent(self, intent)
                    .await
                    .map(|outcome| outcome.receipt)
            }
            async fn status(
                &self,
            ) -> Result<
                tinychannels_bus::controllers::ChannelAccountSnapshot,
                tinychannels_bus::channel::ChannelSendError,
            > {
                let mut snapshot = $crate::providers::long_tail::status::<$ty>();
                snapshot.health_state = Some(
                    $crate::providers::long_tail::LongTail::health_state(self)
                        .await
                        .into(),
                );
                Ok(snapshot)
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
        impl $crate::channel::LegacyChannelReaction for $ty {
            async fn send_reaction(
                &self,
                channel: &tinychannels_bus::channel::ChannelRef,
                conversation: &tinychannels_bus::channel::ConversationRef,
                provider_message_id: &str,
                reaction: &tinychannels_bus::channel::ChannelReaction,
                remove: bool,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::long_tail::check_channel::<$ty>(
                    &<$ty as $crate::providers::long_tail::LongTail>::ID,
                    Some(channel),
                )?;
                $crate::providers::long_tail::LongTail::react(
                    self,
                    channel,
                    conversation,
                    provider_message_id,
                    reaction,
                    remove,
                )
                .await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelEdit for $ty {
            async fn edit(
                &self,
                receipt: &tinychannels_bus::channel::MessageReceipt,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<
                tinychannels_bus::channel::MessageReceipt,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::long_tail::LongTail::edit(self, receipt, intent).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelDelete for $ty {
            async fn delete(
                &self,
                receipt: &tinychannels_bus::channel::MessageReceipt,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::long_tail::LongTail::delete(self, receipt).await
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
