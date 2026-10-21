//! Capability facts for a configured iMessage account.

use super::IMessageChannel;
use super::outbound::{IMESSAGE_MAX_FILE_BYTES, IMESSAGE_MAX_VOICE_BYTES};
use crate::providers::native::capabilities::{media_capabilities, voice_capabilities};
use tinychannels_bus::{
    ActionCapabilities, ChannelCapabilities, ContentCapabilities, ContentCapability,
    EventCapabilities,
};

impl IMessageChannel {
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
            media_capabilities(&mut content, IMESSAGE_MAX_FILE_BYTES);
        }
        if voice_enabled {
            parts.push("voice_note".into());
        }
        content.parts = parts;
        let voice = |synthesis| {
            voice_capabilities(
                voice_enabled,
                synthesis,
                IMESSAGE_MAX_VOICE_BYTES,
                &["m4a_aac", "mp3"],
            )
        };
        ChannelCapabilities {
            remote_control: true,
            chat_approvals: true,
            content,
            actions: ActionCapabilities {
                supported: vec!["send".into()],
                ..Default::default()
            },
            voice_in: voice(false),
            voice_out: voice(self.native.voice_synthesis),
            events: EventCapabilities {
                supported: ["reaction_added", "reaction_removed", "read_receipt"]
                    .map(String::from)
                    .into(),
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
