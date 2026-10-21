use super::QQChannel;
use super::media::DIRECT_UPLOAD_LIMIT;
use crate::providers::native::capabilities::voice_capabilities;
use tinychannels_bus::{ChannelCapabilities, ContentCapability};

pub(super) fn declared(channel: &QQChannel) -> ChannelCapabilities {
    let mut capabilities = crate::providers::long_tail::text_capabilities(
        crate::providers::long_tail::TextSpec::default(),
    );
    if channel.native.media_repository.is_some() {
        let media = |types: &[&str]| ContentCapability {
            // Typed inbound delivery does not require the legacy EventSink.
            inbound: true,
            outbound: true,
            accepted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
            emitted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
            max_size_bytes: Some(DIRECT_UPLOAD_LIMIT),
            ..Default::default()
        };
        capabilities
            .content
            .parts
            .extend(["image", "video", "file"].into_iter().map(str::to_owned));
        capabilities.content.image = media(&["image/jpeg", "image/png"]);
        capabilities.content.video = media(&["video/mp4"]);
        capabilities.content.file = media(&[
            "application/octet-stream",
            "application/pdf",
            "application/zip",
            "text/plain",
            "text/csv",
            "application/json",
        ]);
    }
    if channel.native.voice_media_store.is_some() {
        capabilities.content.parts.push("voice_note".into());
        capabilities.voice_out = voice_capabilities(
            true,
            false,
            DIRECT_UPLOAD_LIMIT,
            &["ogg_opus", "mp3", "wav_pcm16", "silk"],
        );
    }
    capabilities
}
