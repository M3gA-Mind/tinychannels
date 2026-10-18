use super::*;

#[test]
fn media_capabilities_cover_every_media_kind() {
    let mut content = ContentCapabilities::default();
    media_capabilities(&mut content, 1024);
    for capability in [
        &content.image,
        &content.video,
        &content.audio,
        &content.file,
    ] {
        assert!(capability.inbound && capability.outbound);
        assert_eq!(capability.max_size_bytes, Some(1024));
        assert!(!capability.accepted_types.is_empty());
    }
}

#[test]
fn voice_capabilities_are_empty_until_enabled() {
    assert!(!voice_capabilities(false, true, 8, &["mp3"]).supported);
    let enabled = voice_capabilities(true, true, 8, &["m4a_aac", "mp3"]);
    assert!(enabled.supported && enabled.synthesis);
    assert_eq!(enabled.max_size_bytes, Some(8));
    assert_eq!(enabled.accepted_codecs, ["m4a_aac", "mp3"]);
    assert_eq!(enabled.transcript_sources, [VoiceTranscriptSource::HostStt]);
}
