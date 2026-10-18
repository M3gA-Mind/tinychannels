//! Yuanbao's native typed-send implementation over its existing transport.

use super::YuanbaoChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{Limits, NativeProvider, Sent, Step, Target};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{
    ChannelReaction, ChannelSendError, ConversationRef, MessageReceiptPartKind,
};

#[async_trait]
impl NativeProvider for YuanbaoChannel {
    fn id(&self) -> &'static str {
        "yuanbao"
    }

    fn display(&self) -> &'static str {
        "Yuanbao"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: self.config.max_message_length,
            file_bytes: u64::from(self.config.max_media_mb).saturating_mul(1024 * 1024),
            voice_bytes: 0,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.native.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.native.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        false
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        let mut caps = text_capabilities(TextSpec {
            typing: true,
            thread: true,
            streaming_pipeline: true,
        });
        if self.native.media_repository.is_some() {
            caps.content.parts.extend(["image".into(), "file".into()]);
            caps.content.image.outbound = true;
            caps.content.image.max_size_bytes = Some(self.limits().file_bytes);
            caps.content.file.outbound = true;
            caps.content.file.max_size_bytes = Some(self.limits().file_bytes);
        }
        caps
    }

    fn proactive_target(&self) -> Option<String> {
        crate::traits::Channel::proactive_target(self)
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        let recipient = self.resolve_recipient(&target.conversation.id).await;
        let reference = target.thread.as_deref().or(target.reply_to.as_deref());
        let (id, kind) = match step {
            Step::Text(text) => (
                self.outbound
                    .send_text(&recipient, text, reference)
                    .await
                    .map_err(|_| uncertain("text"))?,
                MessageReceiptPartKind::Text,
            ),
            Step::File(file) => {
                let kind = if file.mime.starts_with("image/") {
                    MessageReceiptPartKind::Media
                } else {
                    MessageReceiptPartKind::Media
                };
                let id = self
                    .outbound
                    .send_media_bytes(
                        &recipient,
                        &file.bytes,
                        &file.name,
                        &file.mime,
                        &self.config.app_key,
                        &self.config.bot_id,
                        &self.config.token,
                        &self.config.api_domain,
                        &self.config.route_env,
                        reference,
                    )
                    .await
                    .map_err(|_| uncertain("media"))?;
                (id, kind)
            }
            Step::Voice(_) => {
                return Err(crate::providers::native::unsupported(
                    "Yuanbao has no verified outbound audio message element",
                ));
            }
        };
        Ok(vec![Sent::new(id, kind)])
    }

    async fn set_typing(&self, conversation: &str, on: bool) -> Result<(), ChannelSendError> {
        let result = if on {
            crate::traits::Channel::start_typing(self, conversation).await
        } else {
            crate::traits::Channel::stop_typing(self, conversation).await
        };
        result.map_err(|_| uncertain("typing"))
    }

    async fn react(
        &self,
        _conversation: &ConversationRef,
        _provider_message_id: &str,
        _reaction: &ChannelReaction,
        _remove: bool,
    ) -> Result<(), ChannelSendError> {
        Err(crate::providers::native::unsupported("Yuanbao reactions"))
    }
}

fn uncertain(operation: &str) -> ChannelSendError {
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
        message: format!("Yuanbao {operation} outcome is uncertain"),
        sent_before_error: true,
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
