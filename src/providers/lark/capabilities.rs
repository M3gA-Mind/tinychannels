//! Lark capability declaration and typed provider contract.

use super::*;
use async_trait::async_trait;

#[async_trait]
impl crate::providers::long_tail::LongTail for LarkChannel {
    const ID: &'static str = "lark";
    const DISPLAY: &'static str = "Lark";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        self.native_declared_capabilities(text_capabilities(TextSpec::default()))
    }

    async fn health_state(&self) -> &'static str {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.get_tenant_access_token(),
        )
        .await
        {
            Ok(Ok(_)) => "healthy",
            Ok(Err(_)) | Err(_) => "unhealthy",
        }
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

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        let (mut cancellation, id) = self.begin_listener()?;
        let _registration = LarkListenerRegistration { channel: self, id };
        let result = match self.receive_mode {
            crate::config::LarkReceiveMode::Websocket => {
                tokio::select! {
                    biased;
                    _ = cancellation.changed() => Ok(()),
                    result = self.listen_ws(sink) => result,
                }
            }
            crate::config::LarkReceiveMode::Webhook => {
                self.listen_http_typed(sink, &mut cancellation).await
            }
        };
        result.map_err(|error| {
            tracing::debug!(%error, "Lark typed listener stopped");
            tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "Lark listener stopped".into(),
                retryable: true,
                ..Default::default()
            }
        })
    }

    async fn stop_listener(&self) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.cancel_listener();
        Ok(())
    }
}

impl LarkChannel {
    fn native_declared_capabilities(
        &self,
        mut capabilities: tinychannels_bus::ChannelCapabilities,
    ) -> tinychannels_bus::ChannelCapabilities {
        use tinychannels_bus::ContentCapability;
        const IMAGE_LIMIT: u64 = 10 * 1024 * 1024;
        const FILE_LIMIT: u64 = 30 * 1024 * 1024;
        if self.native.media_repository.is_some() {
            capabilities
                .content
                .parts
                .extend(["image", "video", "audio", "file"].map(str::to_owned));
            let media = |types: &[&str], max_size_bytes| ContentCapability {
                inbound: self.native.events.is_some(),
                outbound: true,
                accepted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
                emitted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
                max_size_bytes: Some(max_size_bytes),
                ..Default::default()
            };
            capabilities.content.image = media(
                &["image/jpeg", "image/png", "image/gif", "image/webp"],
                IMAGE_LIMIT,
            );
            capabilities.content.video = media(&["video/mp4"], FILE_LIMIT);
            let accepted_audio = if self.voice_codec.is_some() {
                vec![
                    "audio/ogg",
                    "audio/opus",
                    "audio/mp4",
                    "audio/mpeg",
                    "audio/wav",
                ]
            } else {
                vec!["audio/ogg", "audio/opus"]
            };
            capabilities.content.audio = ContentCapability {
                inbound: self.native.events.is_some(),
                outbound: true,
                accepted_types: accepted_audio
                    .iter()
                    .map(|mime| (*mime).to_owned())
                    .collect(),
                emitted_types: vec!["audio/opus".into()],
                max_size_bytes: Some(FILE_LIMIT),
                ..Default::default()
            };
            capabilities.content.file = media(
                &[
                    "application/pdf",
                    "text/plain",
                    "text/csv",
                    "application/json",
                    "application/zip",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ],
                FILE_LIMIT,
            );
        }
        if self.native.voice_media_store.is_some() {
            capabilities.content.parts.push("voice_note".into());
            let accepted_codecs = if self.voice_codec.is_some() {
                &["ogg_opus", "m4a_aac", "mp3", "wav_pcm16"][..]
            } else {
                &["ogg_opus"][..]
            };
            capabilities.voice_out = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                FILE_LIMIT,
                accepted_codecs,
            );
        }
        if self.native.voice_media_store.is_some() && self.native.events.is_some() {
            capabilities.voice_in = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                FILE_LIMIT,
                &["ogg_opus", "m4a_aac", "mp3"],
            );
        }
        capabilities
    }
}
