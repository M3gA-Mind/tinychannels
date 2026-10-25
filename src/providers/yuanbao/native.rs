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
use tinychannels_bus::{ChannelOutboundIntent, ChannelOutboundMessage, OutboundPayload};

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
        super::super::capabilities::declared(self)
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

    async fn start_draft(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<Option<String>, ChannelSendError> {
        let message = ChannelOutboundMessage::from_intent(&intent);
        let target = self.normalized_target(&message)?;
        // Yuanbao has no draft transport operation. The marker selects the
        // streaming reply pipeline; content is sent once at finalization.
        Ok(Some(format!("yb-draft:{}", target.conversation.id)))
    }

    async fn update_draft(
        &self,
        _draft_id: &str,
        _intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        // Preserve the provider's historical no-op update semantics. Yuanbao
        // only publishes the accumulated response at finalization.
        Ok(())
    }

    async fn finalize_draft(
        &self,
        _draft_id: &str,
        intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        let text = match &intent.payload {
            OutboundPayload::Text { text } => text.clone(),
            OutboundPayload::Parts { .. } => ChannelOutboundMessage::from_intent(&intent)
                .legacy_text()
                .map_err(|_| {
                    crate::providers::native::unsupported(
                        "Yuanbao streaming draft requires plain text",
                    )
                })?,
            OutboundPayload::NativeChannelData { data } => data
                .get("content")
                .and_then(serde_json::Value::as_str)
                .filter(|_| data.get("subject").is_none_or(serde_json::Value::is_null))
                .map(str::to_owned)
                .ok_or_else(|| {
                    crate::providers::native::unsupported(
                        "Yuanbao streaming draft requires plain text",
                    )
                })?,
            _ => {
                return Err(crate::providers::native::unsupported(
                    "Yuanbao streaming draft requires plain text",
                ));
            }
        };
        let message = ChannelOutboundMessage::from_intent(&intent);
        let target = self.normalized_target(&message)?;
        self.validate_target(&target)?;
        let recipient = self.resolve_recipient(&target.conversation.id).await;
        for chunk in &self.split_message(&text) {
            self.outbound
                .send_text(&recipient, chunk, target.reply_to.as_deref())
                .await
                .map_err(final_send_error)?;
        }
        Ok(())
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
