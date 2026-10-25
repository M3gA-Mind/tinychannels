use super::*;
use crate::providers::discord::support::*;
use std::sync::Arc;
use tinychannels_bus::{ChannelOutboundMessage, MediaKind, MediaReference};
use wiremock::MockServer;

fn voice_message() -> ChannelOutboundMessage {
    message(vec![])
}

fn target() -> Target {
    crate::providers::discord::outbound::target_for(&voice_message()).unwrap()
}

fn reference(voice: &MemoryVoice, bytes: &[u8], mime: &str) -> MediaReference {
    voice.seed(bytes, mime)
}

#[test]
fn waveforms_are_clamped_and_capped_at_256_samples() {
    let encoded = encode_waveform(&[-1.0, 0.0, 0.5, 1.0, 2.0]);
    assert_eq!(BASE64.decode(encoded).unwrap(), vec![0, 0, 128, 255, 255]);

    let long: Vec<f32> = (0..1000).map(|i| (i % 10) as f32 / 10.0).collect();
    let decoded = BASE64.decode(encode_waveform(&long)).unwrap();
    assert!(decoded.len() <= 256 && decoded.len() > 100);
    // Each bucket keeps its peak so loud moments survive downsampling.
    assert!(*decoded.iter().max().unwrap() >= 230);
    assert_eq!(encode_waveform(&[]), "");
}

#[test]
fn ogg_opus_duration_comes_from_the_last_granule_position() {
    let ms = ogg_opus_duration_ms(OGG_FIXTURE).expect("fixture is Ogg Opus");
    assert!((150..250).contains(&ms), "unexpected duration {ms}");
    assert_eq!(ogg_opus_duration_ms(b"RIFF....WAVE"), None);
    assert_eq!(ogg_opus_duration_ms(b"OggS only"), None);
    // Truncated inside the final page header.
    let cut = OGG_FIXTURE.len() - 1;
    let last = OGG_FIXTURE.windows(4).rposition(|w| w == b"OggS").unwrap();
    assert_eq!(ogg_opus_duration_ms(&OGG_FIXTURE[..last + 8]), None);
    assert!(ogg_opus_duration_ms(&OGG_FIXTURE[..cut]).is_some());
    // An unset granule position (all ones) carries no duration.
    let mut unset = OGG_FIXTURE.to_vec();
    unset[last + 6..last + 14].fill(0xff);
    assert_eq!(ogg_opus_duration_ms(&unset), None);
}

#[test]
fn mime_types_map_to_codec_names() {
    assert_eq!(codec_for_mime("audio/ogg"), Some("ogg_opus"));
    assert_eq!(codec_for_mime("audio/mp4"), Some("m4a_aac"));
    assert_eq!(codec_for_mime("audio/mpeg"), Some("mp3"));
    assert_eq!(codec_for_mime("audio/wav"), Some("wav_pcm16"));
    assert_eq!(codec_for_mime("audio/wave"), Some("wav_pcm16"));
    assert_eq!(codec_for_mime("video/mp4"), None);
}

#[tokio::test]
async fn voice_needs_host_voice_storage() {
    let server = MockServer::start().await;
    let error = channel_for(&server)
        .prepare_voice(
            &voice_message(),
            &target(),
            &MediaReference::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(error.unsupported);
}

#[tokio::test]
async fn an_ogg_reference_is_measured_by_the_codec_but_uploaded_untouched() {
    let server = MockServer::start().await;
    let voice = Arc::new(MemoryVoice::default());
    let codec = Arc::new(FixedCodec::new(false));
    let channel = channel_for(&server)
        .with_voice_media_store(voice.clone())
        .with_voice_codec(codec.clone());
    let media = reference(&voice, OGG_FIXTURE, "audio/ogg");

    let upload = channel
        .prepare_voice(
            &voice_message(),
            &target(),
            &media,
            None,
            Some("ogg_opus"),
            Some(900),
        )
        .await
        .unwrap();

    assert_eq!(upload.bytes, OGG_FIXTURE);
    assert_eq!(upload.duration_secs, 2.5, "codec duration wins");
    assert_eq!(
        upload.waveform_base64,
        encode_waveform(&[0.0, 0.25, 0.5, 1.0])
    );
    assert_eq!(
        codec.calls.lock()[0],
        ("ogg_opus".to_owned(), "ogg_opus".to_owned())
    );
}

#[tokio::test]
async fn non_ogg_audio_is_transcoded_to_ogg_opus() {
    let server = MockServer::start().await;
    let voice = Arc::new(MemoryVoice::default());
    let channel = channel_for(&server)
        .with_voice_media_store(voice.clone())
        .with_voice_codec(Arc::new(FixedCodec::new(false)));
    let media = reference(&voice, b"ID3 not ogg", "audio/mpeg");

    let upload = channel
        .prepare_voice(&voice_message(), &target(), &media, None, None, None)
        .await
        .unwrap();

    assert!(upload.bytes.starts_with(b"OggS"));
}

#[tokio::test]
async fn non_ogg_audio_fails_without_a_working_codec() {
    let server = MockServer::start().await;
    let voice = Arc::new(MemoryVoice::default());
    let media = reference(&voice, b"ID3 not ogg", "audio/mpeg");

    let plain = channel_for(&server).with_voice_media_store(voice.clone());
    let error = plain
        .prepare_voice(&voice_message(), &target(), &media, None, None, Some(1000))
        .await
        .unwrap_err();
    assert!(error.message.contains("OGG/Opus"));

    let broken = channel_for(&server)
        .with_voice_media_store(voice.clone())
        .with_voice_codec(Arc::new(FixedCodec::new(true)));
    let error = broken
        .prepare_voice(&voice_message(), &target(), &media, None, None, Some(1000))
        .await
        .unwrap_err();
    assert!(error.message.contains("conversion failed"));

    // Unknown containers cannot even be offered to the codec.
    let odd = reference(&voice, b"????", "audio/x-unknown");
    let with_codec = channel_for(&server)
        .with_voice_media_store(voice)
        .with_voice_codec(Arc::new(FixedCodec::new(false)));
    assert!(
        with_codec
            .prepare_voice(&voice_message(), &target(), &odd, None, None, Some(1000))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ogg_audio_survives_a_codec_failure_with_estimated_metadata() {
    let server = MockServer::start().await;
    let voice = Arc::new(MemoryVoice::default());
    let channel = channel_for(&server)
        .with_voice_media_store(voice.clone())
        .with_voice_codec(Arc::new(FixedCodec::new(true)));
    let media = reference(&voice, OGG_FIXTURE, "audio/ogg");

    let upload = channel
        .prepare_voice(&voice_message(), &target(), &media, None, None, None)
        .await
        .unwrap();

    let expected = ogg_opus_duration_ms(OGG_FIXTURE).unwrap() as f64 / 1000.0;
    assert_eq!(upload.duration_secs, expected);
    // No measured waveform: a flat placeholder keeps the message valid.
    assert_eq!(upload.waveform_base64, encode_waveform(&[0.5; 64]));
}

#[tokio::test]
async fn inline_audio_skips_the_store_read() {
    let server = MockServer::start().await;
    let voice = Arc::new(MemoryVoice::default());
    let channel = channel_for(&server).with_voice_media_store(voice);
    let inline = BASE64.encode(OGG_FIXTURE);

    let upload = channel
        .prepare_voice(
            &voice_message(),
            &target(),
            &MediaReference::default(),
            Some(&inline),
            None,
            Some(1200),
        )
        .await
        .unwrap();
    assert_eq!(upload.duration_secs, 1.2);

    let bad = channel
        .prepare_voice(
            &voice_message(),
            &target(),
            &MediaReference::default(),
            Some("***"),
            None,
            Some(1),
        )
        .await
        .unwrap_err();
    assert!(bad.message.contains("base64"));
}

#[tokio::test]
async fn invalid_voice_inputs_are_rejected_before_dispatch() {
    let server = MockServer::start().await;
    let voice = Arc::new(MemoryVoice::default());
    let channel = channel_for(&server).with_voice_media_store(voice.clone());
    let prepare = |media: MediaReference, inline: Option<String>, duration| {
        let channel = &channel;
        async move {
            channel
                .prepare_voice(
                    &voice_message(),
                    &target(),
                    &media,
                    inline.as_deref(),
                    None,
                    duration,
                )
                .await
        }
    };

    assert!(
        prepare(MediaReference::default(), None, Some(1))
            .await
            .unwrap_err()
            .message
            .contains("host-owned")
    );
    let unknown = MediaReference {
        opaque_id: Some("nope".into()),
        kind: MediaKind::Audio,
        ..Default::default()
    };
    assert!(
        prepare(unknown, None, Some(1))
            .await
            .unwrap_err()
            .message
            .contains("could not be resolved")
    );
    let empty = reference(&voice, b"", "audio/ogg");
    assert!(
        prepare(empty, None, Some(1))
            .await
            .unwrap_err()
            .message
            .contains("size limit")
    );
    let huge = BASE64.encode(vec![0u8; 8 * 1024 * 1024 + 1]);
    assert!(
        prepare(MediaReference::default(), Some(huge), Some(1))
            .await
            .unwrap_err()
            .message
            .contains("size limit")
    );
    // Ogg magic is required even when the caller claims a codec.
    let fake = BASE64.encode(b"not ogg");
    assert!(
        channel
            .prepare_voice(
                &voice_message(),
                &target(),
                &MediaReference::default(),
                Some(&fake),
                Some("ogg_opus"),
                Some(1)
            )
            .await
            .unwrap_err()
            .message
            .contains("OGG/Opus")
    );
    // A scoped read needs the originating turn.
    let media = reference(&voice, OGG_FIXTURE, "audio/ogg");
    let mut no_turn = voice_message();
    no_turn.turn_id = None;
    assert!(
        channel
            .prepare_voice(&no_turn, &target(), &media, None, None, Some(1))
            .await
            .unwrap_err()
            .message
            .contains("turn")
    );
    // Duration must be known or measurable.
    let unmeasurable = BASE64.encode(b"OggS........");
    assert!(
        channel
            .prepare_voice(
                &voice_message(),
                &target(),
                &MediaReference::default(),
                Some(&unmeasurable),
                None,
                None
            )
            .await
            .unwrap_err()
            .message
            .contains("duration")
    );
}

#[tokio::test]
async fn a_caller_supplied_waveform_wins_over_the_codec_measurement() {
    let server = MockServer::start().await;
    let voice = Arc::new(MemoryVoice::default());
    let channel = channel_for(&server)
        .with_voice_media_store(voice.clone())
        .with_voice_codec(Arc::new(FixedCodec::new(false)));
    let media = reference(&voice, OGG_FIXTURE, "audio/ogg");
    let supplied = [1.0_f32, 0.0, 1.0];

    let upload = channel
        .prepare_voice_with_waveform(
            &voice_message(),
            &target(),
            &media,
            None,
            Some("ogg_opus"),
            None,
            Some(&supplied),
        )
        .await
        .unwrap();

    assert_eq!(upload.waveform_base64, encode_waveform(&supplied));
}
