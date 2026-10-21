//! Native typed media delivery for Lark and Feishu.

use super::LarkChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{FileStep, Limits, NativeProvider, Sent, Step, Target};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::channel::ChannelSendError;
use super::outbound::{known_unsent, uncertain_send};
use super::media::file_type_for_mime;
use tinychannels_bus::{
    ChannelCapabilities, ChannelInboundEnvelope, ChannelRef, ConversationKind, ConversationRef,
};


#[async_trait]
impl NativeProvider for LarkChannel {
    fn id(&self) -> &'static str {
        "lark"
    }

    fn display(&self) -> &'static str {
        "Lark"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: 20_000,
            file_bytes: FILE_LIMIT,
            voice_bytes: FILE_LIMIT,
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
        crate::providers::long_tail::LongTail::declared(self)
    }

    fn validate_target(&self, target: &Target) -> Result<(), ChannelSendError> {
        if target.thread.is_some() || target.reply_to.is_some() {
            return Err(crate::providers::native::unsupported(
                "Lark thread and reply addressing",
            ));
        }
        Ok(())
    }

    async fn prepare_steps(
        &self,
        target: &Target,
        steps: &mut [Step],
    ) -> Result<(), ChannelSendError> {
        super::media::prepare_steps(self, target, steps).await
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        super::outbound::deliver(self, target, step).await
    }

}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
