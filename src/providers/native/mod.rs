//! Shared plumbing for the native v2 adapters (Signal, iMessage, Linq, Email).
//!
//! A provider implements [`NativeProvider`] (how to deliver one resolved step,
//! optionally typing, reactions, edits and deletes) and invokes
//! [`native_v2_adapter!`] to get the whole adapter trait family
//! (`ChannelAdapter`, `ChannelTypedSend`, `ChannelTyping`, `ChannelReaction`,
//! `ChannelEdit`, `ChannelDelete`, `ChannelStreamingDraft`). Everything a
//! provider does not implement answers an explicit, non-retryable
//! `unsupported` error before any platform I/O, which is what the conformance
//! kit requires of an undeclared feature.

pub(crate) mod capabilities;
pub(crate) mod edit;
pub(crate) mod inbound;
pub(crate) mod outbound;

pub(crate) use edit::{delete, edit, upsert_draft};
pub(crate) use outbound::{Sent, Step, Target, to_anyhow};

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{
    ChannelReaction, ChannelSendError, ConversationRef, SendErrorKind,
};

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
pub(crate) use outbound::{FileStep, Limits};

/// An explicit, non-retryable "this adapter does not do that" result.
pub(crate) fn unsupported(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        unsupported: true,
        ..Default::default()
    }
}

/// Rejected before any request reached the platform.
pub(crate) fn pre_dispatch(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        ..Default::default()
    }
}

/// A typed send failure carried through `anyhow` so legacy `Channel` callers
/// can recover the structured error by downcast.
#[derive(Debug)]
pub(crate) struct NativeFailure(pub(crate) ChannelSendError);

impl std::fmt::Display for NativeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

impl std::error::Error for NativeFailure {}

/// The host ports every native adapter can use, and its inbound dedupe window.
#[derive(Clone, Default)]
pub(crate) struct HostPorts {
    pub(crate) media_repository: Option<Arc<dyn MediaRepository>>,
    pub(crate) voice_media_store: Option<Arc<dyn VoiceMediaStore>>,
    pub(crate) transcriber: Option<Arc<dyn Transcriber>>,
    pub(crate) voice_synthesis: bool,
    pub(crate) events: Option<Arc<dyn EventSink>>,
    /// Remembered inbound keys so a redelivered event is dropped.
    seen: Arc<Mutex<VecDeque<String>>>,
}

/// Inbound keys remembered per adapter.
const SEEN_WINDOW: usize = 2048;

impl HostPorts {
    /// Record `key`; false when it was already seen.
    pub(crate) fn mark_seen(&self, key: String) -> bool {
        let mut seen = self.seen.lock();
        if seen.contains(&key) {
            return false;
        }
        if seen.len() >= SEEN_WINDOW {
            seen.pop_front();
        }
        seen.push_back(key);
        true
    }
}

/// A message addressed for an edit or delete.
pub(crate) struct Address {
    pub(crate) conversation: ConversationRef,
    pub(crate) message_id: String,
}

/// What a native provider supplies to the shared adapter plumbing.
#[async_trait]
pub(crate) trait NativeProvider: Send + Sync {
    /// Lowercase channel id (`"signal"`).
    fn id(&self) -> &'static str;
    /// Human name used in error messages (`"Signal"`).
    fn display(&self) -> &'static str;
    fn limits(&self) -> Limits;
    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>>;
    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>>;
    fn voice_synthesis(&self) -> bool;
    fn declared_capabilities(&self) -> ChannelCapabilities;
    /// Extra descriptor metadata (for example `voiceOut=attachment`).
    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        Vec::new()
    }
    fn proactive_target(&self) -> Option<String> {
        None
    }
    /// Send one resolved step and report the message ids the platform issued.
    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError>;

    async fn set_typing(&self, _conversation: &str, _on: bool) -> Result<(), ChannelSendError> {
        Err(unsupported(&format!(
            "{} does not support typing indicators",
            self.display()
        )))
    }

    async fn react(
        &self,
        _conversation: &ConversationRef,
        _provider_message_id: &str,
        _reaction: &ChannelReaction,
        _remove: bool,
    ) -> Result<(), ChannelSendError> {
        Err(unsupported(&format!(
            "{} does not support reactions",
            self.display()
        )))
    }

    /// Whether `edit_text` works; checked before a draft sends its first message.
    fn can_edit(&self) -> bool {
        false
    }

    /// Whether streaming drafts (create, edit repeatedly, finalize) are declared.
    fn can_draft(&self) -> bool {
        false
    }

    async fn edit_text(&self, _address: &Address, _text: &str) -> Result<(), ChannelSendError> {
        Err(unsupported(&format!(
            "{} does not support edits",
            self.display()
        )))
    }

    async fn delete_message(&self, _address: &Address) -> Result<(), ChannelSendError> {
        Err(unsupported(&format!(
            "{} does not support deleting messages",
            self.display()
        )))
    }
}

/// Implement the v2 adapter trait family for a [`NativeProvider`].
macro_rules! native_v2_adapter {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl $crate::channel::ChannelAdapter for $ty {
            fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
                let mut metadata = std::collections::BTreeMap::from([(
                    "adapterMode".to_owned(),
                    "native-v2".to_owned(),
                )]);
                for (key, value) in
                    $crate::providers::native::NativeProvider::descriptor_extra(self)
                {
                    metadata.insert(key.to_owned(), value.to_owned());
                }
                tinychannels_bus::channel::ChannelDescriptor {
                    id: $crate::providers::native::NativeProvider::id(self).into(),
                    display_name: $crate::providers::native::NativeProvider::display(self).into(),
                    metadata,
                    ..Default::default()
                }
            }

            fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
                $crate::providers::native::NativeProvider::declared_capabilities(self)
            }

            fn proactive_target(&self) -> Option<String> {
                $crate::providers::native::NativeProvider::proactive_target(self)
            }

            async fn start(
                &self,
                _sink: &(dyn $crate::channel::ChannelInboundSink + Send + Sync),
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                // The configured module's running task owns the single inbound loop.
                Ok(())
            }

            async fn stop(&self) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                Ok(())
            }

            async fn send(
                &self,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<
                tinychannels_bus::channel::ChannelSendOutcome,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::native::outbound::send_typed(
                    self,
                    &tinychannels_bus::ChannelOutboundMessage::from_intent(&intent),
                )
                .await
            }

            async fn status(
                &self,
            ) -> Result<
                tinychannels_bus::controllers::ChannelAccountSnapshot,
                tinychannels_bus::channel::ChannelSendError,
            > {
                Ok(tinychannels_bus::controllers::ChannelAccountSnapshot {
                    channel_id: $crate::providers::native::NativeProvider::id(self).into(),
                    states: vec![
                        tinychannels_bus::controllers::ChannelAccountState::Configured,
                        tinychannels_bus::controllers::ChannelAccountState::Enabled,
                    ],
                    health_state: Some("unknown".into()),
                    ..Default::default()
                })
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
                $crate::providers::native::outbound::send_typed(self, message).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelTyping for $ty {
            async fn start_typing(
                &self,
                conversation_id: &str,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::NativeProvider::set_typing(self, conversation_id, true)
                    .await
            }

            async fn stop_typing(
                &self,
                conversation_id: &str,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::NativeProvider::set_typing(self, conversation_id, false)
                    .await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelReaction for $ty {
            async fn send_reaction(
                &self,
                channel: &tinychannels_bus::channel::ChannelRef,
                conversation: &tinychannels_bus::channel::ConversationRef,
                provider_message_id: &str,
                reaction: &tinychannels_bus::channel::ChannelReaction,
                remove: bool,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::edit::check_channel(self, channel)?;
                $crate::providers::native::NativeProvider::react(
                    self,
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
                $crate::providers::native::edit(self, receipt, &intent).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelDelete for $ty {
            async fn delete(
                &self,
                receipt: &tinychannels_bus::channel::MessageReceipt,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::delete(self, receipt).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::channel::ChannelStreamingDraft for $ty {
            async fn upsert_draft(
                &self,
                previous: Option<&tinychannels_bus::channel::MessageReceipt>,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<
                tinychannels_bus::channel::MessageReceipt,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::native::upsert_draft(self, previous, intent).await
            }
        }
    };
}

pub(crate) use native_v2_adapter;

#[cfg(test)]
#[path = "fixtures_tests.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
