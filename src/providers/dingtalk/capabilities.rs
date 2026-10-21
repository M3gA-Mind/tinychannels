use super::DingTalkChannel;
use super::native::{MAX_MEDIA_BYTES, MAX_VOICE_BYTES};
use tinychannels_bus::{ChannelCapabilities, ContentCapability};

pub(super) fn declared(channel: &DingTalkChannel) -> ChannelCapabilities {
    let mut caps = crate::providers::long_tail::text_capabilities(
        crate::providers::long_tail::TextSpec::default(),
    );
    if channel.native.media_repository.is_some() {
        let image = ContentCapability {
            inbound: true,
            accepted_types: vec!["image/jpeg".into(), "image/png".into()],
            max_size_bytes: Some(MAX_MEDIA_BYTES),
            ..Default::default()
        };
        let file = ContentCapability {
            inbound: true,
            outbound: true,
            accepted_types: vec![
                "application/pdf".into(),
                "application/zip".into(),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document".into(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
            ],
            emitted_types: vec![
                "application/pdf".into(),
                "application/zip".into(),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document".into(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
            ],
            max_size_bytes: Some(MAX_MEDIA_BYTES),
            ..Default::default()
        };
        caps.content.parts.extend(["image".into(), "file".into()]);
        caps.content.image = image;
        caps.content.file = file;
    }
    if channel.native.voice_media_store.is_some() {
        caps.content.audio = ContentCapability {
            inbound: true,
            accepted_types: vec![
                "audio/ogg".into(),
                "audio/mpeg".into(),
                "audio/mp4".into(),
                "audio/amr".into(),
            ],
            max_size_bytes: Some(MAX_MEDIA_BYTES),
            ..Default::default()
        };
        caps.voice_in.supported = true;
        caps.voice_in.accepted_codecs = vec![
            "ogg_opus".into(),
            "mp3".into(),
            "m4a_aac".into(),
            "amr_nb".into(),
        ];
        caps.voice_in.max_size_bytes = Some(MAX_MEDIA_BYTES);
        caps.content.parts.push("voice".into());
        caps.voice_out.supported = true;
        caps.voice_out.accepted_codecs = vec!["amr_nb".into()];
        caps.voice_out.max_size_bytes = Some(MAX_VOICE_BYTES);
        caps.voice_out.synthesis = channel.native.voice_synthesis;
    }
    caps
}
