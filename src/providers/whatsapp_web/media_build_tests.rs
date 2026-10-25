use super::*;

fn upload() -> UploadedMedia {
    UploadedMedia {
        url: "https://cdn.invalid/u".into(),
        direct_path: "/d".into(),
        media_key: vec![1; 32],
        file_sha256: vec![3; 32],
        file_enc_sha256: vec![2; 32],
        file_length: 4096,
        media_key_timestamp: 1_700_000_000,
        streaming_sidecar: Some(vec![9, 9]),
    }
}

fn quote() -> Option<wa::ContextInfo> {
    Some(wa::ContextInfo {
        stanza_id: Some("wamid.q".into()),
        ..Default::default()
    })
}

#[test]
fn images_carry_the_cdn_fields_caption_and_quote() {
    let message = build(
        upload(),
        MediaBuild::Image {
            caption: Some("look".into()),
            mimetype: "image/png".into(),
        },
        quote(),
    );
    let image = message.image_message.as_option().unwrap();
    assert_eq!(image.url.as_deref(), Some("https://cdn.invalid/u"));
    assert_eq!(image.direct_path.as_deref(), Some("/d"));
    assert_eq!(image.media_key.as_deref(), Some(&[1u8; 32][..]));
    assert_eq!(image.file_sha256.as_deref(), Some(&[3u8; 32][..]));
    assert_eq!(image.file_enc_sha256.as_deref(), Some(&[2u8; 32][..]));
    assert_eq!(image.file_length, Some(4096));
    assert_eq!(image.media_key_timestamp, Some(1_700_000_000));
    assert_eq!(image.mimetype.as_deref(), Some("image/png"));
    assert_eq!(image.caption.as_deref(), Some("look"));
    assert_eq!(image.context_info.stanza_id.as_deref(), Some("wamid.q"));
}

#[test]
fn videos_keep_the_sidecar_duration_and_mp4_type() {
    let message = build(
        upload(),
        MediaBuild::Video {
            caption: None,
            duration_seconds: Some(12),
        },
        None,
    );
    let video = message.video_message.as_option().unwrap();
    assert_eq!(video.streaming_sidecar.as_deref(), Some(&[9u8, 9][..]));
    assert_eq!(video.seconds, Some(12));
    assert_eq!(video.mimetype.as_deref(), Some("video/mp4"));
    assert!(video.context_info.stanza_id.is_none());
}

#[test]
fn plain_audio_is_not_push_to_talk_and_voice_notes_are() {
    let plain = build(
        upload(),
        MediaBuild::Audio {
            mimetype: "audio/mpeg".into(),
            duration_seconds: Some(3),
        },
        None,
    );
    let audio = plain.audio_message.as_option().unwrap();
    assert_eq!(audio.ptt, None);
    assert_eq!(audio.mimetype.as_deref(), Some("audio/mpeg"));
    assert_eq!(audio.seconds, Some(3));
    assert!(audio.waveform.is_none());

    let voice = build(
        upload(),
        MediaBuild::Voice {
            duration_seconds: Some(2),
            waveform: Some(vec![10, 20, 30]),
        },
        quote(),
    );
    let note = voice.audio_message.as_option().unwrap();
    assert_eq!(note.ptt, Some(true));
    assert_eq!(note.mimetype.as_deref(), Some("audio/ogg; codecs=opus"));
    assert_eq!(note.waveform.as_deref(), Some(&[10u8, 20, 30][..]));
    assert_eq!(note.context_info.stanza_id.as_deref(), Some("wamid.q"));
    assert_eq!(note.streaming_sidecar.as_deref(), Some(&[9u8, 9][..]));
}

#[test]
fn documents_and_stickers_carry_their_type_specific_fields() {
    let document = build(
        upload(),
        MediaBuild::Document {
            mimetype: "application/pdf".into(),
            file_name: Some("a.pdf".into()),
            caption: Some("doc".into()),
        },
        None,
    );
    let document = document.document_message.as_option().unwrap();
    assert_eq!(document.file_name.as_deref(), Some("a.pdf"));
    assert_eq!(document.caption.as_deref(), Some("doc"));
    assert_eq!(document.mimetype.as_deref(), Some("application/pdf"));

    let sticker = build(upload(), MediaBuild::Sticker, quote());
    let sticker = sticker.sticker_message.as_option().unwrap();
    assert_eq!(sticker.mimetype.as_deref(), Some("image/webp"));
    assert_eq!(sticker.file_length, Some(4096));
    assert_eq!(sticker.context_info.stanza_id.as_deref(), Some("wamid.q"));
}
