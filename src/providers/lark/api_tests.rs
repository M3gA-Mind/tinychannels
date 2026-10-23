//! REST and webhook tests for the Lark channel. A local TLS fixture answers
//! the Feishu/Lark API hosts; the webhook receiver is served on loopback.

#![allow(deprecated)]

use super::*;
use base64::Engine as _;
use tinychannels_bus::SendMessage;

pub(super) struct FixtureVoiceCodec;

#[async_trait::async_trait]
impl crate::host::VoiceCodec for FixtureVoiceCodec {
    async fn transcode(
        &self,
        request: crate::host::VoiceCodecRequest,
    ) -> anyhow::Result<crate::host::VoiceCodecResult> {
        anyhow::ensure!(request.target == "ogg_opus");
        Ok(crate::host::VoiceCodecResult {
            audio_base64: base64::engine::general_purpose::STANDARD
                .encode(include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg")),
            duration_ms: Some(1_000),
            waveform: None,
        })
    }
}

#[test]
fn native_audio_requires_an_ogg_opus_identification_packet() {
    let opus = include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg");
    assert!(super::native::is_ogg_opus(opus));

    let mut vorbis = opus.to_vec();
    let first_segment_len = vorbis[27] as usize;
    assert!(first_segment_len >= 8);
    vorbis[28..36].copy_from_slice(b"vorbis\0\0");
    assert!(!super::native::is_ogg_opus(&vorbis));
    assert!(!super::native::is_ogg_opus(b"not an ogg stream"));
}

#[test]
fn lark_voice_capabilities_do_not_require_unrelated_file_storage() {
    let voice = Arc::new(crate::providers::native::fixtures::StubVoice::default());
    let voice_only = LarkChannel::new(
        "app-id".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    )
    .with_voice_media_store(voice.clone());
    let capabilities = crate::providers::native::NativeProvider::declared_capabilities(&voice_only);

    assert!(capabilities.voice_out.supported);
    assert!(!capabilities.voice_in.supported);

    let voice_with_events = voice_only.with_events(Arc::new(
        crate::providers::native::fixtures::RecordingEvents::delivering(),
    ));
    let capabilities =
        crate::providers::native::NativeProvider::declared_capabilities(&voice_with_events);
    assert!(capabilities.voice_out.supported);
    assert!(capabilities.voice_in.supported);
}

#[tokio::test]
async fn typed_text_with_unsupported_file_caption_fails_before_any_lark_request() {
    use crate::providers::native::fixtures::{StubMedia, stored_media};
    use tinychannels_bus::{
        ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    };

    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t-1")
        } else if request.target == "/open-apis/im/v1/messages?receive_id_type=chat_id" {
            (
                200,
                serde_json::json!({"code": 0, "data": {"message_id": "om-text-1"}}).to_string(),
            )
        } else {
            panic!("unexpected Lark request: {}", request.target)
        }
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Document,
        "application/pdf",
        b"pdf-content",
    ));
    let channel = channel_with(&server, true).with_media_repository(media);
    let message = ChannelOutboundMessage {
        channel_id: "lark".into(),
        conversation_id: "oc_chat_1".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "text-captioned-file".into(),
        parts: vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "send this only if the whole message is valid".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::File {
                    media: MediaReference {
                        opaque_id: Some("opaque-file-1".into()),
                        kind: MediaKind::Document,
                        content_type: Some("application/pdf".into()),
                        ..Default::default()
                    },
                    file_name: Some("report.pdf".into()),
                    caption: Some("unsupported caption".into()),
                },
            },
        ],
    };

    let result = crate::channel::ChannelTypedSend::send_typed(&channel, &message).await;
    assert!(
        server.requests.lock().unwrap().is_empty(),
        "invalid provider-specific attachment must be rejected before text delivery"
    );
    assert!(
        result.is_err(),
        "unsupported file caption should fail the whole send"
    );
}

#[tokio::test]
async fn oversized_lark_image_is_a_non_retryable_preflight_error() {
    use crate::providers::native::fixtures::{StubMedia, stored_media};
    use tinychannels_bus::{
        ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    };

    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t-oversize")
        } else if request.target.starts_with("/open-apis/im/v1/messages?") {
            (
                200,
                r#"{"code":0,"data":{"message_id":"om-text-before-image"}}"#.into(),
            )
        } else {
            panic!(
                "oversized image must not make a request: {}",
                request.target
            )
        }
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Image,
        "image/png",
        &vec![0; 10 * 1024 * 1024 + 1],
    ));
    let channel = channel_with(&server, true).with_media_repository(media);
    let message = ChannelOutboundMessage {
        channel_id: "lark".into(),
        conversation_id: "oc_chat_1".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-oversize-image".into()),
        idempotency_key: "oversize-image".into(),
        parts: vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "text must wait for attachment validation".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::Image {
                    media: MediaReference {
                        opaque_id: Some("oversize-image".into()),
                        kind: MediaKind::Image,
                        content_type: Some("image/png".into()),
                        ..Default::default()
                    },
                    caption: None,
                    alt_text: None,
                },
            },
        ],
    };

    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();

    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );
    assert!(!error.retryable && !error.sent_before_error);
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn rejected_lark_file_upload_stops_before_message_delivery() {
    use crate::providers::native::fixtures::{StubMedia, stored_media};
    use tinychannels_bus::{
        ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    };

    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t-file-rejected")
        } else if request.target == "/open-apis/im/v1/files" {
            (413, r#"{"code":99991663,"msg":"too large"}"#.into())
        } else {
            panic!(
                "rejected file upload must stop before message delivery: {}",
                request.target
            )
        }
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Document,
        "application/pdf",
        b"small-pdf",
    ));
    let channel = channel_with(&server, true).with_media_repository(media);
    let message = ChannelOutboundMessage {
        channel_id: "lark".into(),
        conversation_id: "oc_chat_1".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-file-rejected".into()),
        idempotency_key: "file-rejected".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::File {
                media: MediaReference {
                    opaque_id: Some("file-1".into()),
                    kind: MediaKind::Document,
                    content_type: Some("application/pdf".into()),
                    ..Default::default()
                },
                file_name: Some("report.pdf".into()),
                caption: None,
            },
        }],
    };

    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();

    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(!error.sent_before_error);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].target.starts_with("/open-apis/im/v1/files"));
}

#[tokio::test]
async fn lark_image_upload_rejections_stop_before_message_delivery() {
    use crate::providers::native::fixtures::{StubMedia, stored_media};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinychannels_bus::{
        ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    };

    let upload_attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = upload_attempts.clone();
    let server = HttpsServer::start(move |request| {
        if is_token_request(request) {
            token_ok("t-image-rejected")
        } else if request.target == "/open-apis/im/v1/images" {
            match server_attempts.fetch_add(1, Ordering::SeqCst) {
                0 => (200, r#"{"code":99991663,"msg":"rejected"}"#.into()),
                _ => (200, r#"{"code":0,"data":{}}"#.into()),
            }
        } else {
            panic!(
                "rejected image upload must stop before message delivery: {}",
                request.target
            )
        }
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"small-png"));
    let channel = channel_with(&server, true).with_media_repository(media);
    let message = ChannelOutboundMessage {
        channel_id: "lark".into(),
        conversation_id: "oc_chat_1".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-image-rejected".into()),
        idempotency_key: "image-rejected".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("image-1".into()),
                    kind: MediaKind::Image,
                    content_type: Some("image/png".into()),
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        }],
    };

    for _ in 0..2 {
        let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
            .await
            .unwrap_err();
        assert!(!error.sent_before_error);
    }
    assert_eq!(upload_attempts.load(Ordering::SeqCst), 2);
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| !request.target.starts_with("/open-apis/im/v1/messages"))
    );
}

#[tokio::test]
async fn lark_image_and_message_receipt_failures_keep_delivery_certainty() {
    use crate::providers::native::fixtures::{StubMedia, stored_media};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinychannels_bus::{
        ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    };

    let image_attempts = Arc::new(AtomicUsize::new(0));
    let message_attempts = Arc::new(AtomicUsize::new(0));
    let server_image_attempts = image_attempts.clone();
    let server_message_attempts = message_attempts.clone();
    let server = HttpsServer::start(move |request| {
        if is_token_request(request) {
            token_ok("t-receipt-errors")
        } else if request.target == "/open-apis/im/v1/images" {
            if server_image_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                (503, r#"{"code":99991663}"#.into())
            } else {
                (200, r#"{"code":0,"data":{"image_key":"image-key"}}"#.into())
            }
        } else if request.target.starts_with("/open-apis/im/v1/messages?") {
            match server_message_attempts.fetch_add(1, Ordering::SeqCst) {
                0 => (200, r#"{"code":0,"data":{}}"#.into()),
                1 => (400, r#"{"code":230001,"msg":"rejected"}"#.into()),
                2 => (500, r#"{"code":99991663,"msg":"server error"}"#.into()),
                _ => (200, "not-json".into()),
            }
        } else {
            panic!("unexpected Lark request: {}", request.target)
        }
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"small-png"));
    let channel = channel_with(&server, true).with_media_repository(media);
    let message = ChannelOutboundMessage {
        channel_id: "lark".into(),
        conversation_id: "oc_chat_1".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-receipt-errors".into()),
        idempotency_key: "receipt-errors".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("image-1".into()),
                    kind: MediaKind::Image,
                    content_type: Some("image/png".into()),
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        }],
    };

    let upload_error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert!(!upload_error.sent_before_error);
    let missing_id_error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert!(missing_id_error.sent_before_error);
    let rejected_error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert!(!rejected_error.sent_before_error);
    let server_error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert_eq!(
        server_error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(server_error.sent_before_error);
    assert!(!server_error.retryable);
    let unreadable_error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert_eq!(
        unreadable_error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(unreadable_error.sent_before_error);
    assert_eq!(image_attempts.load(Ordering::SeqCst), 5);
    assert_eq!(
        message_attempts.load(Ordering::SeqCst),
        5,
        "each of the five outbound attempts should issue one message request"
    );
}

#[tokio::test]
async fn lark_host_voice_store_delivers_voice_without_file_repository() {
    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t-voice-only")
        } else if request.target == "/open-apis/im/v1/files" {
            (200, r#"{"code":0,"data":{"file_key":"voice_key"}}"#.into())
        } else if request.target.starts_with("/open-apis/im/v1/messages?") {
            (
                200,
                r#"{"code":0,"data":{"message_id":"om-voice-only"}}"#.into(),
            )
        } else {
            panic!("unexpected Lark request: {}", request.target)
        }
    })
    .await;
    let voice = Arc::new(crate::providers::native::fixtures::StubVoice::default());
    *voice.audio_base64.lock().unwrap() =
        Some(base64::engine::general_purpose::STANDARD.encode(b"mock source audio"));
    let host = crate::host::ChannelHostBuilder::new()
        .voice_media_store(voice)
        .voice_codec(Arc::new(FixtureVoiceCodec))
        .events(Arc::new(
            crate::providers::native::fixtures::RecordingEvents::delivering(),
        ))
        .build();
    let config = tinychannels_bus::ChannelsConfig {
        lark: Some(tinychannels_bus::config::LarkConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            encrypt_key: None,
            verification_token: None,
            allowed_users: vec!["*".into()],
            use_feishu: true,
            receive_mode: Default::default(),
            port: None,
        }),
        ..Default::default()
    };
    let clients = FixtureHttpClients(fixture_client(&server));
    let channel =
        crate::factory::build_long_tail_channel_with_host(&config, &host, &clients, "lark")
            .expect("configured Lark adapter");
    let capabilities = crate::channel::ChannelAdapter::declared_capabilities(channel.as_ref());
    assert!(capabilities.voice_out.supported);
    assert!(capabilities.voice_in.supported);

    let result = crate::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &tinychannels_bus::ChannelOutboundMessage {
            channel_id: "lark".into(),
            conversation_id: "oc_chat_1".into(),
            channel_ref: None,
            conversation_ref: None,
            reply_to_id: None,
            thread_id: None,
            turn_id: Some("turn-voice-only".into()),
            idempotency_key: "voice-only".into(),
            parts: vec![tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("voice-ref-1".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        content_type: Some("audio/mp4".into()),
                        ..Default::default()
                    },
                    codec: Some("m4a_aac".into()),
                    duration_ms: Some(500),
                },
            }],
        },
    )
    .await
    .unwrap();

    assert!(result.error.is_none());
    assert_eq!(
        result.receipt.primary_platform_message_id.as_deref(),
        Some("om-voice-only")
    );
    assert_eq!(server.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn lark_public_reaction_extension_reports_unsupported_without_network_io() {
    let server = HttpsServer::start(|request| {
        panic!(
            "unsupported Lark reaction must not make a request: {}",
            request.target
        )
    })
    .await;
    let channel = channel_with(&server, true);
    let error = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &tinychannels_bus::channel::ChannelRef {
            id: "lark".into(),
            account_id: None,
        },
        &tinychannels_bus::channel::ConversationRef {
            id: "chat-1".into(),
            ..Default::default()
        },
        "message-1",
        &tinychannels_bus::channel::ChannelReaction {
            value: "👍".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await
    .unwrap_err();

    assert!(error.unsupported);
    assert!(!error.retryable);
    assert!(!error.sent_before_error);
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn native_inbound_applies_allowlist_bot_and_group_mention_filters_before_download() {
    let server = HttpsServer::start(|_| (500, "unexpected request".into())).await;
    let mut channel = channel_with(&server, true);
    channel.allowed_users = vec!["ou_allowed".into()];
    let base_event = serde_json::json!({
        "sender": {"sender_type": "user", "sender_id": {"open_id": "ou_allowed"}},
        "message": {
            "message_id": "om-1",
            "chat_id": "oc-1",
            "chat_type": "p2p",
            "message_type": "image",
            "content": "{\"image_key\":\"img-secret-ref\"}"
        }
    });
    let mut forbidden = base_event.clone();
    forbidden["sender"]["sender_id"]["open_id"] = "ou_forbidden".into();
    assert!(channel.inbound_envelope(&forbidden).await.is_none());

    let mut bot = base_event.clone();
    bot["sender"]["sender_type"] = "bot".into();
    assert!(channel.inbound_envelope(&bot).await.is_none());

    let mut unmentioned = base_event;
    unmentioned["message"]["chat_type"] = "group".into();
    unmentioned["message"]["mentions"] = serde_json::json!([]);
    assert!(channel.inbound_envelope(&unmentioned).await.is_none());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn native_inbound_downloads_and_stores_a_file_before_building_the_envelope() {
    let file_bytes = r#"{"note":"attachment"}"#.to_owned();
    let server = HttpsServer::start({
        let file_bytes = file_bytes.clone();
        move |request| {
            if is_token_request(request) {
                token_ok("t-1")
            } else if request.target == "/open-apis/im/v1/messages/om-1/resources/file-1?type=file"
            {
                (200, file_bytes.clone())
            } else {
                (404, String::new())
            }
        }
    })
    .await;
    let media = Arc::new(crate::providers::native::fixtures::StubMedia::default());
    let channel = channel_with(&server, true)
        .with_media_repository(media.clone())
        .with_events(Arc::new(
            crate::providers::native::fixtures::RecordingEvents::delivering(),
        ));
    let event = serde_json::json!({
        "sender": {"sender_type": "user", "sender_id": {"open_id": "ou_alice"}},
        "message": {
            "message_id": "om-1",
            "chat_id": "oc-chat",
            "chat_type": "p2p",
            "message_type": "file",
            "create_time": "1700000000000",
            "content": "{\"file_key\":\"file-1\",\"file_name\":\"note.json\"}"
        }
    });

    let envelope = channel.inbound_envelope(&event).await.unwrap();
    assert_eq!(envelope.message_id, "om-1");
    assert_eq!(envelope.conversation.id, "oc-chat");
    assert_eq!(envelope.timestamp_unix_ms, Some(1_700_000_000_000));
    assert_eq!(envelope.media.len(), 1);
    assert_eq!(
        envelope.media[0].kind,
        tinychannels_bus::MediaKind::Document
    );
    assert!(matches!(
        &envelope.parts[0],
        tinychannels_bus::ContentPart::File { file_name, .. }
            if file_name.as_deref() == Some("note.json")
    ));
    let stored = media.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].bytes, file_bytes.as_bytes());
    assert_eq!(stored[0].metadata.scope.turn_id, "om-1");
    assert_eq!(stored[0].metadata.max_bytes, 30 * 1024 * 1024);
}

#[tokio::test]
async fn lark_voice_store_only_ingests_voice_without_file_repository() {
    let server = HttpsServer::start_with_bytes(
        |request| {
            if is_token_request(request) {
                let (status, body) = token_ok("t-voice-in");
                (status, body.into_bytes())
            } else if request.target
                == "/open-apis/im/v1/messages/om-voice/resources/file-1?type=file"
            {
                (
                    200,
                    include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg").to_vec(),
                )
            } else {
                panic!("unexpected Lark request: {}", request.target)
            }
        },
        "audio/ogg",
    )
    .await;
    let voice = Arc::new(crate::providers::native::fixtures::StubVoice::default());
    let channel = channel_with(&server, true)
        .with_voice_media_store(voice.clone())
        .with_events(Arc::new(
            crate::providers::native::fixtures::RecordingEvents::delivering(),
        ));
    let event = serde_json::json!({
        "sender": {"sender_type": "user", "sender_id": {"open_id": "ou_alice"}},
        "message": {
            "message_id": "om-voice",
            "chat_id": "oc-chat",
            "chat_type": "p2p",
            "message_type": "audio",
            "create_time": "1700000000000",
            "content": "{\"file_key\":\"file-1\",\"duration\":1200}"
        }
    });

    let envelope = channel
        .inbound_envelope(&event)
        .await
        .expect("voice store alone supports Lark voice ingress");

    assert!(matches!(
        &envelope.parts[0],
        tinychannels_bus::ContentPart::VoiceNote { voice_note }
            if voice_note.media.opaque_id.as_deref() == Some("voice-1")
    ));
    let stored = voice.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&stored[0].audio_base64)
            .unwrap(),
        include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg")
    );
}

#[tokio::test]
async fn webhook_ignores_non_message_events_before_media_download() {
    let server = HttpsServer::start(|_| (500, "unexpected request".into())).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = LarkWebhookState {
        verification_token: "verify-token".into(),
        channel: Arc::new(channel_with(&server, true)),
        tx: Some(tx),
        typed_tx: None,
    };
    let payload = serde_json::json!({
        "header": {"token": "verify-token", "event_type": "im.message.deleted_v1"},
        "event": {
            "sender": {"sender_type": "user", "sender_id": {"open_id": "ou_allowed"}},
            "message": {
                "message_id": "om-1",
                "chat_id": "oc-chat",
                "chat_type": "p2p",
                "message_type": "image",
                "content": "{\"image_key\":\"img-1\"}"
            }
        }
    });
    let response = handle_lark_webhook(axum::extract::State(state), axum::Json(payload)).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert!(server.requests.lock().unwrap().is_empty());
}
use crate::providers::http_test_support::{HttpsServer, Request};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Hang guard only; nothing below waits on it in a healthy run.
const GUARD: Duration = Duration::from_secs(10);

/// A client that sends both regions' API hosts to the fixture. The fixture's
/// certificate does not name them, so verification is disabled for this
/// loopback-only test client.
pub(super) fn fixture_client(server: &HttpsServer) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .timeout(GUARD)
        .danger_accept_invalid_certs(true)
        .resolve("open.feishu.cn", server.address)
        .resolve("open.larksuite.com", server.address)
        .build()
        .unwrap()
}

fn channel_with(server: &HttpsServer, use_feishu: bool) -> LarkChannel {
    let mut channel = LarkChannel::new(
        "cli_app".into(),
        "app-secret".into(),
        "verify-token".into(),
        None,
        vec!["*".into()],
    );
    channel.use_feishu = use_feishu;
    channel.http_client = fixture_client(server);
    channel
}

struct FixtureHttpClients(reqwest::Client);

impl crate::factory::HttpClientFactory for FixtureHttpClients {
    fn client_for(&self, _channel: &str) -> reqwest::Client {
        self.0.clone()
    }
}

pub(super) fn is_token_request(request: &Request) -> bool {
    request
        .target
        .ends_with("/auth/v3/tenant_access_token/internal")
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request.headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

pub(super) fn token_ok(token: &str) -> (u16, String) {
    (
        200,
        serde_json::json!({"code": 0, "tenant_access_token": token}).to_string(),
    )
}

fn typed_send_error(error: &anyhow::Error) -> &tinychannels_bus::channel::ChannelSendError {
    &error
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .expect("typed adapter send error")
        .0
}

#[tokio::test]
async fn send_fetches_a_token_once_and_posts_a_text_message() {
    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t-1")
        } else {
            (
                200,
                serde_json::json!({"code": 0, "data": {"message_id": "om-text-1"}}).to_string(),
            )
        }
    })
    .await;
    let channel = channel_with(&server, true);

    crate::provider_test_support::send_v1(&channel, &SendMessage::new("hello lark", "oc_chat"))
        .await
        .unwrap();
    crate::provider_test_support::send_v1(&channel, &SendMessage::new("second", "oc_chat"))
        .await
        .unwrap();
    assert!(
        crate::provider_test_support::is_healthy(&channel).await,
        "cached token stays healthy"
    );

    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3, "the token is fetched once and cached");
    let token = &requests[0];
    assert_eq!(token.method, "POST");
    assert_eq!(
        token.target,
        "/open-apis/auth/v3/tenant_access_token/internal"
    );
    assert_eq!(header(token, "host"), Some("open.feishu.cn"));
    assert_eq!(
        token.body,
        serde_json::json!({"app_id": "cli_app", "app_secret": "app-secret"})
    );

    let send = &requests[1];
    assert_eq!(
        send.target,
        "/open-apis/im/v1/messages?receive_id_type=chat_id"
    );
    assert_eq!(header(send, "authorization"), Some("Bearer t-1"));
    assert_eq!(header(send, "content-type"), Some("application/json"));
    assert_eq!(send.body["receive_id"], "oc_chat");
    assert_eq!(send.body["msg_type"], "text");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(send.body["content"].as_str().unwrap()).unwrap(),
        serde_json::json!({"text": "hello lark"})
    );
    assert_eq!(header(&requests[2], "authorization"), Some("Bearer t-1"));
}

#[tokio::test]
async fn host_aware_factory_sends_typed_image_through_lark_image_upload() {
    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t-image")
        } else if request.target == "/open-apis/im/v1/images" {
            (
                200,
                r#"{"code":0,"data":{"image_key":"img_native_1"}}"#.into(),
            )
        } else if request.target == "/open-apis/im/v1/files" {
            (
                200,
                r#"{"code":0,"data":{"file_key":"file_native_1"}}"#.into(),
            )
        } else if request.target.starts_with("/open-apis/im/v1/messages?") {
            (
                200,
                r#"{"code":0,"data":{"message_id":"om-image-1"}}"#.into(),
            )
        } else {
            (
                404,
                r#"{"code":99991663,"msg":"unexpected request"}"#.into(),
            )
        }
    })
    .await;
    let media = Arc::new(crate::providers::native::fixtures::StubMedia::default());
    let voice = Arc::new(crate::providers::native::fixtures::StubVoice::default());
    *voice.audio_base64.lock().unwrap() =
        Some(base64::engine::general_purpose::STANDARD.encode(b"mock source audio"));
    *media.read.lock().unwrap() = Some(crate::providers::native::fixtures::stored_media(
        tinychannels_bus::MediaKind::Image,
        "image/png",
        b"opaque-scoped-image-bytes",
    ));
    let host = crate::host::ChannelHostBuilder::new()
        .media_repository(media.clone())
        .voice_media_store(voice)
        .voice_codec(Arc::new(FixtureVoiceCodec))
        .build();
    let config = tinychannels_bus::ChannelsConfig {
        lark: Some(tinychannels_bus::config::LarkConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            encrypt_key: None,
            verification_token: None,
            allowed_users: vec!["*".into()],
            use_feishu: true,
            receive_mode: Default::default(),
            port: None,
        }),
        ..Default::default()
    };
    let clients = FixtureHttpClients(fixture_client(&server));
    let channel =
        crate::factory::build_long_tail_channel_with_host(&config, &host, &clients, "lark")
            .expect("Lark config builds through the host-aware factory");
    let capabilities = channel.declared_capabilities();
    assert!(capabilities.content.image.outbound);
    assert!(capabilities.content.file.outbound);
    assert!(capabilities.content.video.outbound);
    assert!(capabilities.content.audio.outbound);
    assert!(capabilities.voice_out.supported);
    let result = crate::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &tinychannels_bus::ChannelOutboundMessage {
            channel_id: "lark".into(),
            conversation_id: "oc_chat_1".into(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "lark".into(),
                account_id: Some("lark:fixture".into()),
            }),
            conversation_ref: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Group,
                id: "oc_chat_1".into(),
                ..Default::default()
            }),
            turn_id: Some("turn-image-1".into()),
            idempotency_key: "typed-image-1".into(),
            reply_to_id: None,
            thread_id: None,
            parts: vec![tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Image {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("opaque-image-1".into()),
                        kind: tinychannels_bus::MediaKind::Image,
                        content_type: Some("image/png".into()),
                        ..Default::default()
                    },
                    caption: None,
                    alt_text: None,
                },
            }],
        },
    )
    .await
    .unwrap();
    assert!(result.error.is_none());
    assert_eq!(
        result.receipt.primary_platform_message_id.as_deref(),
        Some("om-image-1")
    );
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].target, "/open-apis/im/v1/images");
    assert!(
        requests[1]
            .headers
            .to_ascii_lowercase()
            .contains("multipart/form-data")
    );
    assert_eq!(requests[2].body["msg_type"], "image");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(requests[2].body["content"].as_str().unwrap())
            .unwrap()["image_key"],
        "img_native_1"
    );

    *media.read.lock().unwrap() = Some(crate::providers::native::fixtures::stored_media(
        tinychannels_bus::MediaKind::Document,
        "application/pdf",
        b"pdf-bytes",
    ));
    let file_result = crate::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &tinychannels_bus::ChannelOutboundMessage {
            channel_id: "lark".into(),
            conversation_id: "oc_chat_1".into(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "lark".into(),
                account_id: Some("lark:fixture".into()),
            }),
            conversation_ref: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Group,
                id: "oc_chat_1".into(),
                ..Default::default()
            }),
            turn_id: Some("turn-file-1".into()),
            idempotency_key: "typed-file-1".into(),
            reply_to_id: None,
            thread_id: None,
            parts: vec![tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::File {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("opaque-file-1".into()),
                        kind: tinychannels_bus::MediaKind::Document,
                        content_type: Some("application/pdf".into()),
                        ..Default::default()
                    },
                    file_name: Some("report.pdf".into()),
                    caption: None,
                },
            }],
        },
    )
    .await
    .unwrap();
    assert!(file_result.error.is_none());

    *media.read.lock().unwrap() = Some(crate::providers::native::fixtures::stored_media(
        tinychannels_bus::MediaKind::Video,
        "video/mp4",
        b"opaque-scoped-mp4-bytes",
    ));
    let video_result = crate::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &tinychannels_bus::ChannelOutboundMessage {
            channel_id: "lark".into(),
            conversation_id: "oc_chat_1".into(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "lark".into(),
                account_id: Some("lark:fixture".into()),
            }),
            conversation_ref: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Group,
                id: "oc_chat_1".into(),
                ..Default::default()
            }),
            turn_id: Some("turn-video-1".into()),
            idempotency_key: "typed-video-1".into(),
            reply_to_id: None,
            thread_id: None,
            parts: vec![tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Video {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("opaque-video-1".into()),
                        kind: tinychannels_bus::MediaKind::Video,
                        content_type: Some("video/mp4".into()),
                        ..Default::default()
                    },
                    caption: None,
                    duration_ms: None,
                    thumbnail: None,
                },
            }],
        },
    )
    .await
    .unwrap();
    assert!(video_result.error.is_none());

    *media.read.lock().unwrap() = Some(crate::providers::native::fixtures::stored_media(
        tinychannels_bus::MediaKind::Audio,
        "audio/ogg",
        include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg"),
    ));
    let audio_result = crate::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &tinychannels_bus::ChannelOutboundMessage {
            channel_id: "lark".into(),
            conversation_id: "oc_chat_1".into(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "lark".into(),
                account_id: Some("lark:fixture".into()),
            }),
            conversation_ref: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Group,
                id: "oc_chat_1".into(),
                ..Default::default()
            }),
            turn_id: Some("turn-audio-1".into()),
            idempotency_key: "typed-audio-1".into(),
            reply_to_id: None,
            thread_id: None,
            parts: vec![tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Audio {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("opaque-audio-1".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        content_type: Some("audio/ogg".into()),
                        ..Default::default()
                    },
                    codec: Some("ogg_opus".into()),
                    duration_ms: None,
                    title: None,
                    transcript: None,
                },
            }],
        },
    )
    .await
    .unwrap();
    assert!(audio_result.error.is_none());

    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 9);
    assert_eq!(requests[3].target, "/open-apis/im/v1/files");
    assert_eq!(requests[4].body["msg_type"], "file");
    assert_eq!(requests[5].target, "/open-apis/im/v1/files");
    assert_eq!(requests[6].body["msg_type"], "media");
    assert_eq!(requests[7].target, "/open-apis/im/v1/files");
    assert_eq!(requests[8].body["msg_type"], "audio");

    let voice_result = crate::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &tinychannels_bus::ChannelOutboundMessage {
            channel_id: "lark".into(),
            conversation_id: "oc_chat_1".into(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "lark".into(),
                account_id: Some("lark:fixture".into()),
            }),
            conversation_ref: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Group,
                id: "oc_chat_1".into(),
                ..Default::default()
            }),
            turn_id: Some("turn-voice-1".into()),
            idempotency_key: "typed-voice-1".into(),
            reply_to_id: None,
            thread_id: None,
            parts: vec![tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("voice-ref-1".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        content_type: Some("audio/mp4".into()),
                        ..Default::default()
                    },
                    codec: Some("m4a_aac".into()),
                    duration_ms: Some(500),
                },
            }],
        },
    )
    .await
    .unwrap();
    assert!(voice_result.error.is_none());
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 11);
    assert_eq!(requests[9].target, "/open-apis/im/v1/files");
    assert_eq!(requests[10].body["msg_type"], "audio");
}

#[tokio::test]
async fn lark_file_upload_validates_size_and_preserves_audio_duration() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let upload = Arc::new(AtomicUsize::new(0));
    let server = HttpsServer::start({
        let upload = upload.clone();
        move |request| {
            if is_token_request(request) {
                token_ok("t-file-upload")
            } else if request.target == "/open-apis/im/v1/files" {
                if upload.fetch_add(1, Ordering::SeqCst) == 0 {
                    (503, r#"{"code":99991663}"#.into())
                } else {
                    (
                        200,
                        r#"{"code":0,"data":{"file_key":"file-duration"}}"#.into(),
                    )
                }
            } else {
                panic!("unexpected Lark request: {}", request.target)
            }
        }
    })
    .await;
    let channel = channel_with(&server, false);

    for bytes in [&[][..], &vec![0; (super::FILE_LIMIT + 1) as usize][..]] {
        let error = channel
            .upload_file(bytes, "audio.opus", "opus", Some(1_200))
            .await
            .unwrap_err();
        assert!(error.message.contains("empty or exceeds"));
        assert!(!error.sent_before_error);
    }
    assert!(server.requests.lock().unwrap().is_empty());

    let rejected = channel
        .upload_file(b"audio-bytes", "audio.opus", "opus", Some(1_200))
        .await
        .unwrap_err();
    assert!(!rejected.sent_before_error);
    let key = channel
        .upload_file(b"audio-bytes", "audio.opus", "opus", Some(1_200))
        .await
        .unwrap();
    assert_eq!(key, "file-duration");
    let requests = server.requests.lock().unwrap().clone();
    let upload_requests = requests
        .iter()
        .filter(|request| request.target == "/open-apis/im/v1/files")
        .collect::<Vec<_>>();
    assert_eq!(upload_requests.len(), 2);
    assert!(
        String::from_utf8_lossy(&upload_requests[1].raw_body)
            .contains("name=\"duration\"\r\n\r\n1200")
    );
}

#[tokio::test]
async fn send_refreshes_an_expired_token_and_retries_once() {
    let tokens = Arc::new(AtomicUsize::new(0));
    let sends = Arc::new(AtomicUsize::new(0));
    let server = HttpsServer::start({
        let tokens = tokens.clone();
        let sends = sends.clone();
        move |request| {
            if is_token_request(request) {
                let n = tokens.fetch_add(1, Ordering::SeqCst) + 1;
                token_ok(&format!("t-{n}"))
            } else if sends.fetch_add(1, Ordering::SeqCst) == 0 {
                (401, r#"{"code":99991663,"msg":"token expired"}"#.into())
            } else {
                (
                    200,
                    r#"{"code":0,"data":{"message_id":"om-retried"}}"#.into(),
                )
            }
        }
    })
    .await;
    let channel = channel_with(&server, false);

    let result =
        crate::provider_test_support::send_v1(&channel, &SendMessage::new("retry me", "oc_chat"))
            .await;
    assert!(
        result.is_ok(),
        "a refreshed token should allow one retry: {result:?}"
    );

    let requests = server.requests.lock().unwrap().clone();
    let auth: Vec<_> = requests
        .iter()
        .filter(|r| !is_token_request(r))
        .map(|r| header(r, "authorization").unwrap().to_string())
        .collect();
    assert_eq!(auth, ["Bearer t-1", "Bearer t-2"]);
    assert_eq!(header(&requests[0], "host"), Some("open.larksuite.com"));
}

#[tokio::test]
async fn empty_unauthorized_response_still_refreshes_before_retry() {
    let tokens = Arc::new(AtomicUsize::new(0));
    let sends = Arc::new(AtomicUsize::new(0));
    let server = HttpsServer::start({
        let tokens = tokens.clone();
        let sends = sends.clone();
        move |request| {
            if is_token_request(request) {
                let n = tokens.fetch_add(1, Ordering::SeqCst) + 1;
                token_ok(&format!("t-{n}"))
            } else if sends.fetch_add(1, Ordering::SeqCst) == 0 {
                (401, String::new())
            } else {
                (
                    200,
                    r#"{"code":0,"data":{"message_id":"om-after-empty-401"}}"#.into(),
                )
            }
        }
    })
    .await;
    let channel = channel_with(&server, false);

    crate::provider_test_support::send_v1(&channel, &SendMessage::new("retry", "oc_chat"))
        .await
        .expect("the HTTP 401 status alone must trigger the bounded token refresh");

    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(tokens.load(Ordering::SeqCst), 2);
    assert_eq!(sends.load(Ordering::SeqCst), 2);
    assert_eq!(header(&requests[1], "authorization"), Some("Bearer t-1"));
    assert_eq!(header(&requests[3], "authorization"), Some("Bearer t-2"));
}

#[tokio::test]
async fn a_second_token_expired_response_is_known_unsent() {
    let tokens = Arc::new(AtomicUsize::new(0));
    let sends = Arc::new(AtomicUsize::new(0));
    let server = HttpsServer::start({
        let tokens = tokens.clone();
        let sends = sends.clone();
        move |request| {
            if is_token_request(request) {
                let n = tokens.fetch_add(1, Ordering::SeqCst) + 1;
                token_ok(&format!("t-{n}"))
            } else if sends.fetch_add(1, Ordering::SeqCst) == 0 {
                (401, r#"{"code":99991663,"msg":"expired"}"#.into())
            } else {
                (200, r#"{"code":99991663,"msg":"expired again"}"#.into())
            }
        }
    })
    .await;
    let channel = channel_with(&server, false);

    let error = crate::provider_test_support::send_v1(
        &channel,
        &SendMessage::new("do not retry again", "oc_chat"),
    )
    .await
    .unwrap_err();
    let typed = typed_send_error(&error);
    assert_eq!(
        typed.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(!typed.sent_before_error);
    assert!(typed.retryable);
    assert_eq!(tokens.load(Ordering::SeqCst), 2);
    assert_eq!(sends.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn send_reports_failures_with_and_without_a_token_refresh() {
    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t")
        } else {
            (401, "still unauthorized".into())
        }
    })
    .await;
    let channel = channel_with(&server, true);
    let error = crate::provider_test_support::send_v1(&channel, &SendMessage::new("x", "oc_chat"))
        .await
        .unwrap_err();
    let send_error = typed_send_error(&error);
    assert_eq!(
        send_error.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(send_error.retryable);
    assert!(!send_error.sent_before_error);

    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t")
        } else {
            (500, "upstream exploded".into())
        }
    })
    .await;
    let channel = channel_with(&server, true);
    let error = crate::provider_test_support::send_v1(&channel, &SendMessage::new("x", "oc_chat"))
        .await
        .unwrap_err();
    let send_error = typed_send_error(&error);
    assert_eq!(
        send_error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(send_error.sent_before_error);
}

#[tokio::test]
async fn token_errors_fail_send_and_health_check() {
    let server =
        HttpsServer::start(|_| (200, r#"{"code":10003,"msg":"invalid app_secret"}"#.into())).await;
    let channel = channel_with(&server, true);
    assert!(!crate::provider_test_support::is_healthy(&channel).await);
    let error = crate::provider_test_support::send_v1(&channel, &SendMessage::new("x", "oc_chat"))
        .await
        .unwrap_err();
    let send_error = typed_send_error(&error);
    assert_eq!(
        send_error.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(send_error.retryable);
    assert!(!send_error.sent_before_error);

    let server = HttpsServer::start(|_| (200, r#"{"code":0}"#.into())).await;
    let error = channel_with(&server, true)
        .get_tenant_access_token()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing tenant_access_token"));

    let server = HttpsServer::start(|_| (200, r#"{"msg":"no code"}"#.into())).await;
    let error = channel_with(&server, true)
        .get_tenant_access_token()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no code"));
}

#[tokio::test]
async fn ws_endpoint_errors_stop_listen_before_connecting() {
    let server =
        HttpsServer::start(|_| (200, r#"{"code":1000040345,"msg":"bad app"}"#.into())).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let channel = channel_with(&server, true);
    let error = crate::provider_test_support::listen_v1(&channel, tx)
        .await
        .unwrap_err();
    let typed = error
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .expect("typed listener error");
    assert_eq!(
        typed.0.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(typed.0.retryable);
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests[0].target, "/callback/ws/endpoint");
    assert_eq!(header(&requests[0], "locale"), Some("zh"));
    assert_eq!(
        requests[0].body,
        serde_json::json!({"AppID": "cli_app", "AppSecret": "app-secret"})
    );

    let server = HttpsServer::start(|_| (200, r#"{"code":1}"#.into())).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let channel = channel_with(&server, false);
    let error = crate::provider_test_support::listen_v1(&channel, tx)
        .await
        .unwrap_err();
    let typed = error
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .expect("typed listener error");
    assert_eq!(
        typed.0.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(typed.0.retryable);
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(header(&requests[0], "locale"), Some("en"));

    let server = HttpsServer::start(|_| (200, r#"{"code":0}"#.into())).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let channel = channel_with(&server, true);
    let error = crate::provider_test_support::listen_v1(&channel, tx)
        .await
        .unwrap_err();
    let typed = error
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .expect("typed listener error");
    assert_eq!(
        typed.0.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(typed.0.retryable);
}

fn webhook_event(open_id: &str, text: &str) -> serde_json::Value {
    serde_json::json!({
        "header": { "event_type": "im.message.receive_v1", "token": "verify-token" },
        "event": {
            "sender": { "sender_id": { "open_id": open_id } },
            "message": {
                "message_id": "webhook-message-1",
                "message_type": "text",
                "content": serde_json::json!({"text": text}).to_string(),
                "chat_id": "oc_chat",
                "chat_type": "p2p"
            }
        }
    })
}

#[tokio::test]
async fn webhook_mode_requires_a_port() {
    let mut channel = LarkChannel::new("a".into(), "s".into(), "verify-token".into(), None, vec![]);
    channel.receive_mode = crate::config::LarkReceiveMode::Webhook;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let error = crate::provider_test_support::listen_v1(&channel, tx)
        .await
        .unwrap_err();
    let typed = error
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .expect("typed listener error");
    assert_eq!(
        typed.0.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(typed.0.retryable);
}

#[tokio::test]
async fn webhook_server_forwards_verified_events() {
    // Reserve a free port, release it, and hand it to the receiver.
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap().port()
    };
    let mut channel = LarkChannel::new(
        "a".into(),
        "s".into(),
        "verify-token".into(),
        Some(port),
        vec!["ou_alice".into()],
    );
    channel.receive_mode = crate::config::LarkReceiveMode::Webhook;
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let server =
        tokio::spawn(async move { crate::provider_test_support::listen_v1(&channel, tx).await });

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let url = format!("http://127.0.0.1:{port}/lark");
    let deadline = Instant::now() + GUARD;
    let response = loop {
        match client
            .post(&url)
            .json(&webhook_event("ou_alice", "via webhook"))
            .send()
            .await
        {
            Ok(response) => break response,
            Err(error) => {
                assert!(Instant::now() < deadline, "webhook never came up: {error}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "ok");

    let message = tokio::time::timeout(GUARD, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.content, "via webhook");
    assert_eq!(message.reply_target, "oc_chat");

    let forbidden = client
        .post(&url)
        .json(&serde_json::json!({"header": {"token": "wrong"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), 403);
    server.abort();
}

#[tokio::test]
async fn webhook_handler_acknowledges_events_after_the_receiver_closes() {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    let state = LarkWebhookState {
        verification_token: "verify-token".into(),
        channel: Arc::new(LarkChannel::new(
            "a".into(),
            "s".into(),
            "verify-token".into(),
            None,
            vec!["*".into()],
        )),
        tx: Some(tx),
        typed_tx: None,
    };
    let response = handle_lark_webhook(
        axum::extract::State(state),
        axum::Json(webhook_event("ou_bob", "dropped")),
    )
    .await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}

#[test]
fn parse_event_payload_defaults_the_timestamp_to_now() {
    let channel = LarkChannel::new("a".into(), "s".into(), "t".into(), None, vec!["*".into()]);
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let messages = channel.parse_event_payload(&webhook_event("ou_bob", "no time"));
    assert_eq!(messages.len(), 1);
    assert!(messages[0].timestamp >= before);

    let mut post = webhook_event("ou_bob", "");
    post["event"]["message"]["message_type"] = "post".into();
    post["event"]["message"]["content"] = "{}".into();
    assert!(channel.parse_event_payload(&post).is_empty());
}
