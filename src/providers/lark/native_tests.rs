use super::*;
use base64::Engine as _;

#[tokio::test]
async fn typed_inbound_strips_feishu_mention_placeholders_from_text() {
    let channel = LarkChannel::new(
        "app".into(),
        "secret".into(),
        "verify".into(),
        None,
        vec!["ou_alice".into()],
    );
    let event = serde_json::json!({
        "sender": {
            "sender_id": {"open_id": "ou_alice"},
            "sender_type": "user"
        },
        "message": {
            "message_id": "mention-1",
            "chat_id": "oc_chat",
            "chat_type": "p2p",
            "message_type": "text",
            "content": serde_json::json!({"text": "hello @_user_1 there"}).to_string()
        }
    });

    let inbound = channel
        .inbound_envelope(&event)
        .await
        .expect("admitted Lark text becomes a typed event");
    assert_eq!(inbound.text, "hello there");
    assert!(matches!(
        inbound.parts.as_slice(),
        [tinychannels_bus::ContentPart::Text { text, .. }] if text == "hello there"
    ));
}

#[test]
fn file_mime_types_map_to_lark_upload_types() {
    for (mime, file_type) in [
        ("application/pdf", "pdf"),
        ("application/msword", "doc"),
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "doc",
        ),
        ("application/vnd.ms-excel", "xls"),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls",
        ),
        ("application/vnd.ms-powerpoint", "ppt"),
        (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "ppt",
        ),
        ("application/octet-stream", "stream"),
    ] {
        assert_eq!(file_type_for_mime(mime), file_type, "{mime}");
    }
}

#[test]
fn ogg_opus_detection_rejects_invalid_headers_and_truncated_segments() {
    let valid = include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg");
    assert!(is_ogg_opus(valid));
    assert!(!is_ogg_opus(b""));
    assert!(!is_ogg_opus(b"OggS"));

    let mut wrong_version = valid.to_vec();
    wrong_version[4] = 1;
    assert!(!is_ogg_opus(&wrong_version));

    let mut truncated_lacing = valid[..27].to_vec();
    truncated_lacing[26] = 1;
    assert!(!is_ogg_opus(&truncated_lacing));

    let mut wrong_packet = valid.to_vec();
    wrong_packet[28..36].copy_from_slice(b"vorbis\0\0");
    assert!(!is_ogg_opus(&wrong_packet));
}

#[test]
fn lark_send_errors_preserve_delivery_certainty() {
    let unsent = known_unsent("upload rejected");
    assert_eq!(
        unsent.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(unsent.retryable);
    assert!(!unsent.sent_before_error);

    let uncertain = uncertain_send("message response lost");
    assert_eq!(
        uncertain.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(uncertain.sent_before_error);
    assert!(!uncertain.retryable);
}

#[test]
fn lark_rejects_threaded_outbound_targets_before_delivery() {
    let channel = super::super::LarkChannel::new(
        "app-id".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    );
    let target = Target {
        channel: ChannelRef {
            id: "lark".into(),
            account_id: None,
        },
        conversation: ConversationRef {
            id: "chat-1".into(),
            ..Default::default()
        },
        reply_to: None,
        thread: Some("thread-1".into()),
    };

    let error = NativeProvider::validate_target(&channel, &target).unwrap_err();

    assert!(error.unsupported);
    assert!(!error.sent_before_error);
}

#[tokio::test]
async fn lark_preflights_audio_that_needs_a_host_codec_before_delivery() {
    let channel = super::super::LarkChannel::new(
        "app-id".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    );
    let target = Target {
        channel: ChannelRef {
            id: "lark".into(),
            account_id: None,
        },
        conversation: ConversationRef {
            id: "chat-1".into(),
            ..Default::default()
        },
        reply_to: None,
        thread: None,
    };
    let mut steps = [Step::File(FileStep {
        kind: MediaKind::Audio,
        bytes: b"wav audio".to_vec(),
        mime: "audio/wav".into(),
        name: "voice.wav".into(),
        caption: None,
        duration_ms: None,
    })];

    let error = NativeProvider::prepare_steps(&channel, &target, &mut steps)
        .await
        .unwrap_err();

    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );
    assert!(!error.sent_before_error);
    assert!(error.message.contains("host voice codec"));
}

#[tokio::test]
async fn lark_preflights_unsupported_image_and_video_formats() {
    let channel = super::super::LarkChannel::new(
        "app-id".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    );
    let target = Target {
        channel: ChannelRef {
            id: "lark".into(),
            account_id: None,
        },
        conversation: ConversationRef {
            id: "chat-1".into(),
            ..Default::default()
        },
        reply_to: None,
        thread: None,
    };

    for (kind, mime, bytes) in [
        (MediaKind::Image, "image/svg+xml", vec![0; 1]),
        (MediaKind::Image, "image/png", vec![0; 10 * 1024 * 1024 + 1]),
        (MediaKind::Video, "video/quicktime", vec![0; 1]),
    ] {
        let mut steps = [Step::File(FileStep {
            kind,
            bytes,
            mime: mime.into(),
            name: "attachment.bin".into(),
            caption: None,
            duration_ms: None,
        })];
        let error = NativeProvider::prepare_steps(&channel, &target, &mut steps)
            .await
            .unwrap_err();

        assert!(!error.sent_before_error);
        assert!(!error.retryable);
    }
}

#[tokio::test]
async fn lark_audio_preparation_rejects_unsupported_and_invalid_sources() {
    use crate::host::{VoiceCodec, VoiceCodecRequest, VoiceCodecResult};

    struct CodecReply(Result<String, &'static str>);

    #[async_trait::async_trait]
    impl VoiceCodec for CodecReply {
        async fn transcode(&self, _request: VoiceCodecRequest) -> anyhow::Result<VoiceCodecResult> {
            let audio_base64 = self
                .0
                .as_ref()
                .map_err(|message| anyhow::anyhow!("{message}"))?;
            Ok(VoiceCodecResult {
                audio_base64: audio_base64.clone(),
                duration_ms: None,
                waveform: None,
            })
        }
    }

    let mut channel = LarkChannel::new(
        "app-id".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    );
    let file = |mime: &str, bytes: &[u8]| FileStep {
        kind: MediaKind::Audio,
        bytes: bytes.to_vec(),
        mime: mime.into(),
        name: "voice.wav".into(),
        caption: None,
        duration_ms: None,
    };

    let unsupported = channel
        .opus_audio(&file("audio/flac", b"audio"))
        .await
        .unwrap_err();
    assert!(unsupported.unsupported);
    assert!(!unsupported.sent_before_error);

    let malformed = channel
        .opus_audio(&file("audio/ogg", b"not an Ogg stream"))
        .await
        .unwrap_err();
    assert_eq!(
        malformed.kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );
    assert!(!malformed.sent_before_error);

    let missing_codec = channel
        .opus_audio(&file("audio/wav", b"wave data"))
        .await
        .unwrap_err();
    assert_eq!(
        missing_codec.kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );

    channel.voice_codec = Some(Arc::new(CodecReply(Err("codec unavailable"))));
    let failed_codec = channel
        .opus_audio(&file("audio/wav", b"wave data"))
        .await
        .unwrap_err();
    assert!(failed_codec.message.contains("conversion failed"));

    channel.voice_codec = Some(Arc::new(CodecReply(Ok("not-base64".into()))));
    let invalid_encoding = channel
        .opus_audio(&file("audio/wav", b"wave data"))
        .await
        .unwrap_err();
    assert!(
        invalid_encoding
            .message
            .contains("invalid or oversized data")
    );

    channel.voice_codec = Some(Arc::new(CodecReply(Ok(
        base64::engine::general_purpose::STANDARD.encode(b"not Ogg Opus"),
    ))));
    let wrong_codec = channel
        .opus_audio(&file("audio/wav", b"wave data"))
        .await
        .unwrap_err();
    assert!(wrong_codec.message.contains("did not return Ogg Opus"));
}

#[tokio::test]
async fn lark_audio_preparation_rejects_files_over_the_provider_limit_before_conversion() {
    let channel = LarkChannel::new(
        "app-id".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    );
    let target = Target {
        channel: ChannelRef {
            id: "lark".into(),
            account_id: None,
        },
        conversation: ConversationRef {
            id: "chat-1".into(),
            ..Default::default()
        },
        reply_to: None,
        thread: None,
    };
    let mut steps = [Step::File(FileStep {
        kind: MediaKind::Audio,
        bytes: vec![0; (super::super::FILE_LIMIT + 1) as usize],
        mime: "audio/wav".into(),
        name: "large.wav".into(),
        caption: None,
        duration_ms: None,
    })];

    let error = NativeProvider::prepare_steps(&channel, &target, &mut steps)
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );
    assert!(!error.sent_before_error);
    assert!(error.message.contains("30 MiB limit"));
}
