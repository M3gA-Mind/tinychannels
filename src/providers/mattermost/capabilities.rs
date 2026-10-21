//! Mattermost channel capabilities and typed adapter contract.

use super::*;

#[async_trait]
impl crate::providers::long_tail::LongTail for MattermostChannel {
    const ID: &'static str = "mattermost";
    const DISPLAY: &'static str = "Mattermost";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        let mut capabilities = text_capabilities(TextSpec {
            typing: true,
            thread: true,
            ..Default::default()
        });
        capabilities.reactions = true;
        capabilities
            .actions
            .supported
            .extend(["edit", "delete"].map(String::from));
        if self.media_repository.is_some() {
            capabilities
                .content
                .parts
                .extend(["image", "video", "audio", "file"].map(String::from));
            let outbound = |types: &[&str]| tinychannels_bus::ContentCapability {
                outbound: true,
                accepted_types: types.iter().map(|mime| (*mime).into()).collect(),
                emitted_types: types.iter().map(|mime| (*mime).into()).collect(),
                max_size_bytes: Some(MAX_FILE_BYTES),
                ..Default::default()
            };
            capabilities.content.image =
                outbound(&["image/jpeg", "image/png", "image/gif", "image/webp"]);
            capabilities.content.video = outbound(&["video/mp4", "video/quicktime", "video/webm"]);
            capabilities.content.audio = outbound(&[
                "audio/ogg",
                "audio/mpeg",
                "audio/mp4",
                "audio/wav",
                "audio/webm",
            ]);
            capabilities.content.file = outbound(&[
                "application/pdf",
                "text/plain",
                "text/csv",
                "application/json",
                "application/zip",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ]);
            let inbound = self.events.is_some();
            capabilities.content.image.inbound = inbound;
            capabilities.content.video.inbound = inbound;
            capabilities.content.audio.inbound = inbound;
            capabilities.content.file.inbound = inbound;
        }
        if self.voice_media_store.is_some() {
            capabilities.content.parts.push("voice_note".into());
            capabilities.voice_out = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                MAX_VOICE_BYTES,
                &["m4a_aac", "mp3", "ogg_opus"],
            );
        }
        capabilities
    }

    async fn health_state(&self) -> &'static str {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::providers::native::NativeProvider::health_state(self),
        )
        .await
        {
            Ok(state) => state,
            Err(_) => "unhealthy",
        }
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        crate::providers::native::NativeProvider::descriptor_extra(self)
    }

    fn typing_supported(&self) -> bool {
        true
    }

    async fn set_typing(
        &self,
        conversation_id: &str,
        start: bool,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        let result = if start {
            self.start_typing_loop(conversation_id).await
        } else {
            self.stop_typing_loop()
        };
        result.map_err(|_| tinychannels_bus::channel::ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Transient,
            message: "Mattermost typing indicator failed".into(),
            retryable: true,
            ..Default::default()
        })
    }

    async fn react(
        &self,
        channel: &tinychannels_bus::ChannelRef,
        conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        use crate::providers::native::NativeProvider;

        crate::providers::native::edit::check_channel(self, channel)?;
        NativeProvider::react(self, conversation, provider_message_id, reaction, remove).await
    }

    async fn edit(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<
        tinychannels_bus::channel::MessageReceipt,
        tinychannels_bus::channel::ChannelSendError,
    > {
        crate::providers::native::edit(self, receipt, &intent).await
    }

    async fn delete(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        crate::providers::native::delete(self, receipt).await
    }

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> std::result::Result<(), tinychannels_bus::channel::ChannelSendError> {
        use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

        let (cancel, mut cancellation) = tokio::sync::watch::channel(false);
        {
            let mut active = self.listener_cancel.lock();
            if active.is_some() {
                return Err(ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "Mattermost listener is already running".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            *active = Some(cancel.clone());
        }
        let _registration = ListenerRegistration {
            channel: self,
            cancel,
        };
        self.listen_typed(sink, &mut cancellation).await
    }

    async fn stop_listener(
        &self,
    ) -> std::result::Result<(), tinychannels_bus::channel::ChannelSendError> {
        if let Some(cancel) = self.listener_cancel.lock().as_ref() {
            cancel.send_replace(true);
        }
        Ok(())
    }

    async fn send_typed_message(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<
        tinychannels_bus::channel::ChannelSendOutcome,
        tinychannels_bus::channel::ChannelSendError,
    > {
        crate::providers::native::outbound::send_typed(self, message).await
    }
}
