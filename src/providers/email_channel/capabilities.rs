//! Capability facts for a configured email account.

use super::EmailChannel;
use super::native::{EMAIL_MAX_FILE_BYTES, EMAIL_MAX_VOICE_BYTES};
use crate::providers::native::capabilities::{media_capabilities, voice_capabilities};
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ChannelCapabilities, ContentCapabilities,
    ContentCapability,
};

impl EmailChannel {
    pub(crate) fn declared(&self) -> ChannelCapabilities {
        let media_enabled = self.native.media_repository.is_some();
        let voice_enabled = self.native.voice_media_store.is_some();
        let mut parts: Vec<String> = vec!["text".into()];
        let mut content = ContentCapabilities {
            text: ContentCapability {
                inbound: true,
                outbound: true,
                ..Default::default()
            },
            ..Default::default()
        };
        if media_enabled {
            parts.extend(["image", "video", "audio", "file"].map(String::from));
            media_capabilities(&mut content, EMAIL_MAX_FILE_BYTES);
        }
        if voice_enabled {
            parts.push("voice_note".into());
        }
        content.parts = parts;
        let voice = |synthesis| {
            voice_capabilities(
                voice_enabled,
                synthesis,
                EMAIL_MAX_VOICE_BYTES,
                &["m4a_aac", "mp3", "ogg_opus"],
            )
        };
        ChannelCapabilities {
            remote_control: true,
            chat_approvals: true,
            // `In-Reply-To` carries the reply target and `References` the thread.
            addressing: AddressingCapabilities {
                reply_to_id: true,
                thread_id: true,
                ..Default::default()
            },
            content,
            actions: ActionCapabilities {
                supported: ["send", "reply"].map(String::from).into(),
                ..Default::default()
            },
            voice_in: voice(false),
            voice_out: voice(self.native.voice_synthesis),
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
