//! Native typed attachment delivery for Mattermost's v4 files and posts API.

use super::super::long_tail::LongTail;
use super::{MAX_FILE_BYTES, MAX_VOICE_BYTES, MattermostChannel};
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{Address, Limits, NativeProvider, Sent, Step, Target};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
#[cfg(test)]
use tinychannels_bus::channel::MessageReceiptPartKind;
use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

#[async_trait]
impl NativeProvider for MattermostChannel {
    fn id(&self) -> &'static str {
        "mattermost"
    }

    fn display(&self) -> &'static str {
        "Mattermost"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: 16_383,
            file_bytes: MAX_FILE_BYTES,
            voice_bytes: MAX_VOICE_BYTES,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        false
    }

    fn can_edit(&self) -> bool {
        true
    }

    fn can_delete(&self) -> bool {
        true
    }

    async fn health_state(&self) -> &'static str {
        let healthy = self
            .http_client()
            .get(format!(
                "{}/api/v4/users/me",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        if healthy { "healthy" } else { "unhealthy" }
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    fn normalized_target(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<Target, ChannelSendError> {
        let mut target =
            crate::providers::native::outbound::target_of(message, self.id(), self.display())?;
        if let Some((_, packed_root)) = target.conversation.id.split_once(':') {
            if message
                .conversation_ref
                .as_ref()
                .and_then(|conversation| conversation.thread_id.as_deref())
                .is_some_and(|thread| thread != packed_root)
            {
                return Err(crate::providers::native::pre_dispatch(
                    "Mattermost packed conversation and canonical thread addresses conflict",
                ));
            }
            // Mattermost's deployed legacy address helper preserves an
            // already-packed channel:root recipient instead of appending or
            // replacing its root from the compatibility thread hint.
            target.thread = Some(packed_root.to_owned());
            target.conversation.thread_id = Some(packed_root.to_owned());
        }
        Ok(target)
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        if self.voice_media_store.is_some() {
            vec![("voiceOut", "attachment")]
        } else {
            Vec::new()
        }
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        self.deliver_native(target, step).await
    }

    async fn set_typing(&self, conversation: &str, on: bool) -> Result<(), ChannelSendError> {
        let result = if on {
            self.start_typing_loop(conversation).await
        } else {
            self.stop_typing_loop()
        };
        result.map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "Mattermost typing indicator failed".into(),
            retryable: true,
            ..Default::default()
        })
    }

    async fn react(
        &self,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        self.react_native(conversation, provider_message_id, reaction, remove)
            .await
    }

    async fn edit_text(&self, address: &Address, text: &str) -> Result<(), ChannelSendError> {
        self.edit_native(address, text).await
    }

    async fn delete_message(&self, address: &Address) -> Result<(), ChannelSendError> {
        self.delete_native(address).await
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
