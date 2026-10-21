//! Capability facts derived from the configured Yuanbao ports and protocol.

use super::YuanbaoChannel;
use crate::providers::native::NativeProvider;
use tinychannels_bus::ChannelCapabilities;

pub(super) fn declared(channel: &YuanbaoChannel) -> ChannelCapabilities {
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
    if channel.native.media_repository.is_some() {
        let max_bytes = NativeProvider::limits(channel).file_bytes;
        caps.content.parts.extend(["image".into(), "file".into()]);
        caps.content.image.outbound = true;
        caps.content.image.max_size_bytes = Some(max_bytes);
        caps.content.file.outbound = true;
        caps.content.file.max_size_bytes = Some(max_bytes);
    }
    // Native voice ingress is published through the supplied
    // ChannelInboundSink, so the adapter only needs the scoped voice
    // store; the legacy EventSink is not part of this path.
    if channel.native.voice_media_store.is_some() {
        caps.voice_in.supported = true;
        caps.voice_in.accepted_codecs = vec!["m4a_aac".into(), "mp3".into(), "ogg_opus".into()];
        caps.voice_in.max_size_bytes = Some(NativeProvider::limits(channel).file_bytes);
    }
    caps
}
