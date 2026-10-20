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
            file_bytes: u64::from(self.config.max_media_mb)
                .saturating_mul(1024 * 1024)
                .min(u64::from(u32::MAX)),
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

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        self.start_typed_listener(sink).await
    }

    async fn stop_listener(&self) -> Result<(), ChannelSendError> {
        self.stop_typed_listener();
        Ok(())
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        let mut caps = text_capabilities(TextSpec {
            typing: true,
            // Yuanbao's BizMsg protocol has one `ref_msg_id` field. It can
            // reply to a message, but cannot address a distinct thread root.
            thread: false,
            streaming_pipeline: true,
        });
        // Reply references are implemented for group conversations only;
        // direct-message requests are rejected by `validate_target`.
        caps.addressing.reply_to_id = true;
        caps.actions.supported.push("reply".into());
        if self.native.media_repository.is_some() {
            let max_bytes = self.limits().file_bytes;
            caps.content.parts.extend(["image".into(), "file".into()]);
            caps.content.image.outbound = true;
            caps.content.image.max_size_bytes = Some(max_bytes);
            caps.content.file.outbound = true;
            caps.content.file.max_size_bytes = Some(max_bytes);
        }
        // Native voice ingress is published through the supplied
        // ChannelInboundSink, so the adapter only needs the scoped voice
        // store; the legacy EventSink is not part of this path.
        if self.native.voice_media_store.is_some() {
            caps.voice_in.supported = true;
            caps.voice_in.accepted_codecs = vec!["m4a_aac".into(), "mp3".into(), "ogg_opus".into()];
            caps.voice_in.max_size_bytes = Some(self.limits().file_bytes);
        }
        caps
    }

    fn validate_target(&self, target: &Target) -> Result<(), ChannelSendError> {
        // BizMsg only encodes `ref_msg_id` for group sends. C2C messages have
        // no reply-reference field, so reject rather than silently dropping
        // the requested reply address after uploading media.
        if target.reply_to.is_some() && !target.conversation.id.starts_with("g:") {
            return Err(crate::providers::native::unsupported(
                "Yuanbao direct-message reply addressing",
            ));
        }
        Ok(())
    }

    fn proactive_target(&self) -> Option<String> {
        None
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        let recipient = self.resolve_recipient(&target.conversation.id).await;
        let reference = target.reply_to.as_deref();
        let (id, kind) = match step {
            Step::Text(text) => (
                self.outbound
                    .send_text(&recipient, text, reference)
                    .await
                    .map_err(final_send_error)?,
                MessageReceiptPartKind::Text,
            ),
            Step::File(file) => {
                let body = self
                    .outbound
                    .prepare_media_body_with_caption(
                        &file.bytes,
                        &file.name,
                        &file.mime,
                        &self.config.app_key,
                        &self.config.bot_id,
                        &self.config.token,
                        &self.config.api_domain,
                        &self.config.route_env,
                        file.caption.as_deref(),
                    )
                    .await
                    .map_err(|_| upload_not_delivered())?;
                let id = self
                    .outbound
                    .send_body(&recipient, body, reference)
                    .await
                    .map_err(final_send_error)?;
                (id, MessageReceiptPartKind::Media)
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
        self.set_typing_native(conversation, on)
            .await
            .map_err(|_| ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "Yuanbao typing indicator failed".into(),
                retryable: true,
                ..Default::default()
            })
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

fn final_send_error(error: super::super::errors::YuanbaoError) -> ChannelSendError {
    if matches!(error, super::super::errors::YuanbaoError::NotConnected) {
        return ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Transient,
            message: "Yuanbao is not connected; message was not sent".into(),
            retryable: true,
            sent_before_error: false,
            ..Default::default()
        };
    }
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
        message: "Yuanbao send outcome is uncertain".into(),
        sent_before_error: true,
        ..Default::default()
    }
}

fn upload_not_delivered() -> ChannelSendError {
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Transient,
        message: "Yuanbao media preparation failed before message delivery".into(),
        retryable: true,
        sent_before_error: false,
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
