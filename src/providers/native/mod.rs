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
pub(crate) use outbound::{Sent, Step, Target};

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};
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
#[cfg(test)]
pub(crate) struct NativeFailure(pub(crate) ChannelSendError);

#[cfg(test)]
impl std::fmt::Display for NativeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

#[cfg(test)]
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
    seen: Arc<Mutex<SeenState>>,
}

#[derive(Default)]
struct SeenState {
    accepted: VecDeque<String>,
    pending: HashSet<String>,
}

/// A dedupe reservation commits only after the provider's ingress sink accepts
/// the event. Dropping it (including when a listener future is cancelled)
/// makes the event eligible for retry.
pub(crate) struct SeenReservation {
    seen: Arc<Mutex<SeenState>>,
    key: String,
    active: bool,
}

pub(crate) enum SeenReservationResult {
    /// The event was already accepted and can be safely suppressed.
    Accepted,
    /// Another dispatch owns the key, but has not been accepted yet.
    Pending,
    /// This dispatch owns the key until it commits or is dropped.
    Reserved(SeenReservation),
}

impl SeenReservation {
    pub(crate) fn commit(mut self) {
        let mut state = self.seen.lock();
        state.pending.remove(&self.key);
        remember_seen(&mut state, self.key.clone());
        self.active = false;
    }
}

impl Drop for SeenReservation {
    fn drop(&mut self) {
        if self.active {
            self.seen.lock().pending.remove(&self.key);
        }
    }
}

/// Inbound keys remembered per adapter.
const SEEN_WINDOW: usize = 2048;

impl HostPorts {
    /// Whether `key` was recorded earlier, without recording it.
    pub(crate) fn has_seen(&self, key: &str) -> bool {
        self.seen.lock().accepted.iter().any(|seen| seen == key)
    }

    /// Record `key`; false when it was already seen.
    pub(crate) fn mark_seen(&self, key: String) -> bool {
        let mut state = self.seen.lock();
        if state.accepted.contains(&key) || state.pending.contains(&key) {
            return false;
        }
        remember_seen(&mut state, key);
        true
    }

    /// Reserve a key while an ingress write is in progress. Only a completed
    /// write calls [`SeenReservation::commit`]; cancellation and rejection
    /// release the reservation through `Drop`.
    pub(crate) fn reserve_seen(&self, key: String) -> SeenReservationResult {
        let mut state = self.seen.lock();
        if state.accepted.contains(&key) {
            return SeenReservationResult::Accepted;
        }
        if !state.pending.insert(key.clone()) {
            return SeenReservationResult::Pending;
        }
        SeenReservationResult::Reserved(SeenReservation {
            seen: Arc::clone(&self.seen),
            key,
            active: true,
        })
    }
}

fn remember_seen(state: &mut SeenState, key: String) {
    if state.accepted.contains(&key) {
        return;
    }
    if state.accepted.len() >= SEEN_WINDOW {
        state.accepted.pop_front();
    }
    state.accepted.push_back(key);
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
    /// Whether accompanying text is preserved as a voice caption instead of a
    /// separate ordered text message.
    fn voice_caption_supported(&self) -> bool {
        true
    }
    fn declared_capabilities(&self) -> ChannelCapabilities;
    /// Resolve legacy provider-specific address projections before scoped
    /// media is read, so delivery and media authority share one target.
    fn normalized_target(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<Target, ChannelSendError> {
        outbound::target_of(message, self.id(), self.display())
    }
    /// Validate provider-specific address combinations before planning can
    /// resolve opaque host media or perform any network requests.
    fn validate_target(&self, _target: &Target) -> Result<(), ChannelSendError> {
        Ok(())
    }
    /// Validate or normalize every provider-specific step before delivery
    /// starts. This keeps a known-invalid later attachment from following an
    /// already-sent text step.
    async fn prepare_steps(
        &self,
        _target: &Target,
        _steps: &mut [Step],
    ) -> Result<(), ChannelSendError> {
        Ok(())
    }
    /// Extra descriptor metadata (for example `voiceOut=attachment`).
    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        Vec::new()
    }
    fn proactive_target(&self) -> Option<String> {
        None
    }
    /// Send one resolved step and report the message ids the platform issued.
    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError>;

    /// Begin a provider-specific streaming draft lifecycle. Providers that
    /// only support the generic edit/upsert lifecycle keep the default
    /// unsupported result.
    async fn start_draft(
        &self,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<Option<String>, ChannelSendError> {
        Err(unsupported(&format!("{} draft start", self.display())))
    }

    /// Update a provider-specific draft without publishing it.
    async fn update_draft(
        &self,
        _draft_id: &str,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        Err(unsupported(&format!("{} draft update", self.display())))
    }

    /// Publish or otherwise finalize a provider-specific draft.
    async fn finalize_draft(
        &self,
        _draft_id: &str,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        Err(unsupported(&format!(
            "{} draft finalization",
            self.display()
        )))
    }

    /// Optional raw webhook delivery owned by the provider. The default keeps
    /// existing providers on their compatibility ingress until migrated.
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

    /// Provider-owned health state for the typed adapter descriptor. Providers
    /// without a probe remain explicitly unknown.
    async fn health_state(&self) -> &'static str {
        "unknown"
    }

    /// Run a provider-owned typed listener when this native adapter supports
    /// inbound events. Most adapters are outbound-only and retain the no-op.
    async fn start_listener(
        &self,
        _sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        Ok(())
    }

    /// Signal a provider-owned listener to stop.
    async fn stop_listener(&self) -> Result<(), ChannelSendError> {
        Ok(())
    }

    /// Whether one reply travels as a single platform message (email), so the
    /// planned steps are delivered together by [`Self::deliver_bundle`].
    fn bundles_steps(&self) -> bool {
        false
    }

    /// Deliver every planned step as one platform message.
    async fn deliver_bundle(
        &self,
        _target: &Target,
        _steps: &[Step],
    ) -> Result<Vec<Sent>, ChannelSendError> {
        Err(unsupported(&format!(
            "{} does not bundle steps",
            self.display()
        )))
    }

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

    /// Whether provider message deletion is implemented.
    fn can_delete(&self) -> bool {
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

            fn supports_webhook_delivery(&self) -> bool {
                $crate::providers::native::NativeProvider::supports_webhook_delivery(self)
            }

            async fn deliver_webhook(
                &self,
                signature: Option<&str>,
                body: &[u8],
            ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
                $crate::providers::native::NativeProvider::deliver_webhook(self, signature, body)
                    .await
            }

            async fn start(
                &self,
                sink: &(dyn $crate::channel::ChannelInboundSink + Send + Sync),
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::NativeProvider::start_listener(self, sink).await
            }

            async fn stop(&self) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::NativeProvider::stop_listener(self).await
            }

            async fn send(
                &self,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<
                tinychannels_bus::channel::MessageReceipt,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::native::outbound::send_typed(
                    self,
                    &tinychannels_bus::ChannelOutboundMessage::from_intent(&intent),
                )
                .await
                .map(|outcome| outcome.receipt)
            }

            async fn send_typed(
                &self,
                message: &tinychannels_bus::ChannelOutboundMessage,
            ) -> Result<
                tinychannels_bus::channel::ChannelSendOutcome,
                tinychannels_bus::channel::ChannelSendError,
            > {
                $crate::providers::native::outbound::send_typed(self, message).await
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
                    health_state: Some(
                        $crate::providers::native::NativeProvider::health_state(self)
                            .await
                            .into(),
                    ),
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
        impl $crate::channel::LegacyChannelReaction for $ty {
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
            async fn start_draft(
                &self,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<Option<String>, tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::NativeProvider::start_draft(self, intent).await
            }

            async fn update_draft(
                &self,
                draft_id: &str,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::NativeProvider::update_draft(self, draft_id, intent)
                    .await
            }

            async fn finalize_draft(
                &self,
                draft_id: &str,
                intent: tinychannels_bus::ChannelOutboundIntent,
            ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
                $crate::providers::native::NativeProvider::finalize_draft(self, draft_id, intent)
                    .await
            }

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
