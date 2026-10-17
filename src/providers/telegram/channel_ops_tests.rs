use super::*;
use crate::providers::telegram::fixtures::*;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_conformance::AdapterFixture;
use tinychannels_runtime::channel::ChannelAdapter;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct TelegramConformanceSink(Arc<TelegramChannel>);

#[async_trait::async_trait]
impl tinychannels_runtime::adapters::LocalOutboundSink for TelegramConformanceSink {
    async fn send(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<
        tinychannels_bus::channel::MessageReceipt,
        tinychannels_bus::channel::ChannelSendError,
    > {
        let message = tinychannels_bus::ChannelOutboundMessage::from_intent(&intent);
        let outcome = self
            .0
            .send_typed_with_receipt(&message)
            .await
            .map_err(|error| tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                message: error.to_string(),
                ..Default::default()
            })?;
        if let Some(error) = outcome.error {
            return Err(error);
        }
        Ok(outcome.receipt)
    }
}

struct TelegramWireFixture {
    adapter: tinychannels_runtime::adapters::LocalChannelAdapter,
    server: MockServer,
    observations: std::sync::Mutex<Vec<tinychannels_conformance::WireObservation>>,
    request_cursor: std::sync::Mutex<usize>,
    scope: tinychannels_conformance::ExpectedScope,
}

#[async_trait::async_trait]
impl tinychannels_conformance::AdapterFixture for TelegramWireFixture {
    fn adapter(&self) -> &dyn tinychannels_runtime::channel::ChannelAdapter {
        &self.adapter
    }

    fn expected_scope(
        &self,
        _feature: tinychannels_conformance::ConformanceFeature,
    ) -> tinychannels_conformance::ExpectedScope {
        self.scope.clone()
    }

    fn clear_observations(&self) {
        self.observations.lock().unwrap().clear();
    }

    fn observations(&self) -> Vec<tinychannels_conformance::WireObservation> {
        self.observations.lock().unwrap().clone()
    }

    async fn exercise(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
    ) -> Result<
        Option<tinychannels_bus::channel::ChannelSendOutcome>,
        tinychannels_bus::channel::ChannelSendError,
    > {
        use tinychannels_conformance::ConformanceFeature as Feature;
        let payload = match feature {
            Feature::Text => tinychannels_bus::OutboundPayload::Text {
                text: "hello-kit".into(),
            },
            Feature::Image => tinychannels_bus::OutboundPayload::Media {
                text: None,
                media_urls: vec!["https://example.invalid/image.png".into()],
            },
            Feature::VoiceSynthesis => tinychannels_bus::OutboundPayload::Voice {
                voice: tinychannels_bus::OutboundVoice::Synthesize {
                    text: "speak this".into(),
                    voice: None,
                },
            },
            _ => {
                return Err(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
                    message: "fixture does not implement this operation".into(),
                    unsupported: true,
                    ..Default::default()
                });
            }
        };
        let intent = tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "kit-op".into(),
            channel_id: "telegram".into(),
            conversation_id: "100".into(),
            channel_ref: Some(self.scope.channel.clone()),
            conversation_ref: Some(self.scope.conversation.clone()),
            payload,
            ..Default::default()
        };
        let result = self.adapter.send(intent).await;
        let mut requests = self.server.received_requests().await.unwrap_or_default();
        let mut cursor = self.request_cursor.lock().unwrap();
        let request_end = requests.len();
        requests = requests.drain(*cursor..).collect();
        *cursor = request_end;
        for request in requests
            .into_iter()
            .filter(|request| request.url.path().ends_with("/sendMessage"))
        {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let chat = body
                .get("chat_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let conversation = tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: chat.to_owned(),
                ..Default::default()
            };
            let payload_kinds = if body.get("text").is_some() {
                vec!["text".into()]
            } else {
                vec![]
            };
            let wire_id = |field: &str| {
                body.get(field).map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string())
                })
            };
            self.observations
                .lock()
                .unwrap()
                .push(tinychannels_conformance::WireObservation {
                    feature,
                    operation: "send".into(),
                    channel: self.scope.channel.clone(),
                    conversation,
                    reply_to_id: wire_id("reply_to_message_id"),
                    thread_id: wire_id("message_thread_id"),
                    payload_kinds,
                    platform_message_ids: vec!["321".into()],
                    request: body,
                });
        }
        result.map(Some)
    }
}

struct VoiceReadFixture;
struct TypedMediaReadFixture;

struct UncheckedMediaReadFixture;
struct LargeVoiceReadFixture {
    expected_scope: tinychannels_bus::HostVoiceScope,
}

fn outbound_voice_message(
    opaque_id: Option<&str>,
    turn_id: Option<&str>,
) -> tinychannels_bus::ChannelOutboundMessage {
    tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: turn_id.map(str::to_owned),
        idempotency_key: "operation-key".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: opaque_id.map(str::to_owned),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
            },
        }],
    }
}

#[test]
fn telegram_voice_capabilities_require_durable_store_and_host_synthesis_ports() {
    let without_store = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false).capabilities();
    assert!(!without_store.voice_in.supported);
    assert!(!without_store.voice_out.supported);
    assert!(!without_store.voice_out.synthesis);

    let with_storage = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .capabilities();
    assert!(with_storage.voice_in.supported);
    assert_eq!(with_storage.content.parts, ["text", "voice_note"]);
    assert!(with_storage.voice_out.supported);
    assert!(!with_storage.voice_out.synthesis);
    assert_eq!(
        with_storage.voice_out.accepted_codecs,
        ["ogg_opus", "m4a_aac", "mp3"]
    );
    assert_eq!(with_storage.voice_out.max_size_bytes, Some(8 * 1024 * 1024));

    let with_all_ports = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .with_voice_synthesis(true)
        .capabilities();
    assert!(with_all_ports.voice_out.synthesis);
}

#[test]
fn telegram_voice_capabilities_preserve_base_text_and_stream_descriptor() {
    let off = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .capabilities();
    assert!(off.content.text.inbound && off.content.text.outbound);
    assert_eq!(off.content.parts, ["text", "voice_note"]);
    assert!(off.actions.supported.contains(&"send".into()));
    assert!(!off.progressive_edits);
    assert!(!off.streaming_reply_pipeline);

    let on = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_streaming(StreamMode::Partial, 100, true)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .capabilities();
    assert!(on.progressive_edits);
    assert!(on.streaming_reply_pipeline);
    assert!(on.actions.supported.contains(&"draft-update".into()));
}

#[test]
fn telegram_advertises_typed_media_only_with_scoped_storage() {
    let unavailable = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false).capabilities();
    assert!(!unavailable.content.image.outbound);
    assert!(!unavailable.content.file.inbound);

    let configured = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_media_repository(Arc::new(TypedMediaReadFixture))
        .capabilities();
    assert!(configured.content.image.inbound && configured.content.image.outbound);
    assert!(configured.content.video.inbound && configured.content.video.outbound);
    assert!(configured.content.audio.inbound && configured.content.audio.outbound);
    assert!(configured.content.file.inbound && configured.content.file.outbound);
    assert!(configured.content.sticker.inbound);
    assert!(!configured.content.sticker.outbound);
    assert_eq!(
        configured.content.image.accepted_types,
        ["image/jpeg", "image/png", "image/gif", "image/webp"]
    );
    assert_eq!(configured.content.sticker.accepted_types, ["image/webp"]);
    assert_eq!(
        configured.content.image.max_size_bytes,
        Some(10 * 1024 * 1024)
    );
    assert_eq!(
        configured.content.video.max_size_bytes,
        Some(20 * 1024 * 1024)
    );
    assert_eq!(
        configured.content.file.max_size_bytes,
        Some(20 * 1024 * 1024)
    );
    assert_eq!(
        configured.content.audio.emitted_types,
        ["audio/mpeg", "audio/mp4"]
    );
    assert_eq!(configured.content.video.emitted_types, ["video/mp4"]);
}

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for VoiceReadFixture {
    async fn store_voice(
        &self,
        _request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        unreachable!()
    }

    async fn read_voice(
        &self,
        scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        assert_eq!(scope.turn_id, "turn-7");
        assert_eq!(scope.channel.id, "telegram");
        assert_eq!(scope.conversation.id, "100");
        assert_eq!(opaque_id, "host-media-1");
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: "AQIDBA==".into(),
            mime_type: "audio/ogg".into(),
            file_name: Some("reply.ogg".into()),
        })
    }
}

#[async_trait::async_trait]
impl crate::host::MediaRepository for TypedMediaReadFixture {
    async fn store_media(
        &self,
        _request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        unreachable!("outbound media fixture only resolves stored media")
    }

    async fn read_media(
        &self,
        scope: crate::host::MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        assert_eq!(scope.configured_instance, "telegram-main");
        assert_eq!(scope.channel.id, "telegram");
        assert_eq!(scope.channel.account_id.as_deref(), Some("telegram-main"));
        assert_eq!(scope.conversation.id, "-10042");
        assert_eq!(scope.conversation.scope_id.as_deref(), Some("scope-42"));
        assert_eq!(scope.conversation.parent_id.as_deref(), Some("parent-42"));
        assert_eq!(scope.conversation.thread_id.as_deref(), Some("77"));
        assert_eq!(scope.conversation.topic_id.as_deref(), Some("topic-77"));
        assert_eq!(scope.turn_id, "turn-42");
        let (kind, content_type, file_name, bytes) = match opaque_id.as_str() {
            "stored-photo-1" => (
                tinychannels_bus::MediaKind::Image,
                "image/png",
                "photo.png",
                BASE64
                    .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
                    .unwrap(),
            ),
            "oversized-photo" => (
                tinychannels_bus::MediaKind::Image,
                "image/jpeg",
                "large.jpg",
                oversized_jpeg_with_dimensions(),
            ),
            "stored-audio-1" => (
                tinychannels_bus::MediaKind::Audio,
                "audio/mpeg",
                "audio.mp3",
                valid_mp3_frame(),
            ),
            "stored-wav-1" => (
                tinychannels_bus::MediaKind::Audio,
                "audio/wav",
                "audio.wav",
                BASE64.decode("UklGRgAAAABXQVZF").unwrap(),
            ),
            "stored-video-mp4" => (
                tinychannels_bus::MediaKind::Video,
                "video/mp4",
                "clip.mp4",
                b"\0\0\0\x18ftypisom\0\0\0\0isom".to_vec(),
            ),
            "stored-document-pdf" => (
                tinychannels_bus::MediaKind::Document,
                "application/pdf",
                "report.pdf",
                b"%PDF-1.7\nfixture".to_vec(),
            ),
            "stored-video-webm" => (
                tinychannels_bus::MediaKind::Video,
                "video/webm",
                "clip.webm",
                b"\x1a\x45\xdf\xa3fixture".to_vec(),
            ),
            "stored-image-gif" => (
                tinychannels_bus::MediaKind::Image,
                "image/gif",
                "image.gif",
                b"GIF89a".to_vec(),
            ),
            _ => panic!("unexpected typed media id: {opaque_id}"),
        };
        assert!(bytes.len() as u64 <= max_bytes);
        Ok(crate::host::StoredMedia {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some(opaque_id),
                kind,
                content_type: Some(content_type.into()),
                file_name: Some(file_name.into()),
                size_bytes: Some(bytes.len() as u64),
                ..Default::default()
            },
            content_type: content_type.into(),
            file_name: Some(file_name.into()),
            bytes,
        })
    }
}

#[async_trait::async_trait]
impl crate::host::MediaRepository for UncheckedMediaReadFixture {
    async fn store_media(
        &self,
        _request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        unreachable!("outbound media fixture only resolves stored media")
    }

    async fn read_media(
        &self,
        _scope: crate::host::MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        let bytes = BASE64
            .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
            .unwrap();
        assert!(bytes.len() as u64 <= max_bytes);
        Ok(crate::host::StoredMedia {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some(opaque_id),
                kind: tinychannels_bus::MediaKind::Image,
                content_type: Some("image/png".into()),
                size_bytes: Some(bytes.len() as u64),
                ..Default::default()
            },
            content_type: "image/png".into(),
            file_name: Some("photo.png".into()),
            bytes,
        })
    }
}

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for LargeVoiceReadFixture {
    async fn store_voice(
        &self,
        _request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        unreachable!()
    }

    async fn read_voice(
        &self,
        scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        assert_eq!(scope, self.expected_scope);
        assert_eq!(opaque_id, "large-opaque-voice");
        let bytes = vec![0x5a_u8; 129 * 1024];
        assert!(bytes.len() > tinychannels_bus::module_relay::RELAY_FRAME_BYTES);
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: BASE64.encode(bytes),
            mime_type: "audio/ogg".into(),
            file_name: Some("large-reply.ogg".into()),
        })
    }
}

#[tokio::test]
async fn telegram_resolves_and_uploads_voice_larger_than_inline_intent_limit() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 53})),
    )
    .await;
    let expected_conversation = tinychannels_bus::ConversationRef {
        kind: tinychannels_bus::ConversationKind::Dm,
        id: "100".into(),
        scope_id: Some("scope-9".into()),
        parent_id: Some("parent-4".into()),
        thread_id: Some("thread-5".into()),
        topic_id: Some("77".into()),
    };
    let expected_channel = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram:bot-1".into()),
    };
    let expected_scope = tinychannels_bus::HostVoiceScope {
        configured_instance: "telegram:bot-1".into(),
        channel: expected_channel.clone(),
        conversation: expected_conversation.clone(),
        turn_id: "large-voice-origin-turn".into(),
    };
    let channel = channel_for(&server, &["*"])
        .with_voice_media_store(Arc::new(LargeVoiceReadFixture { expected_scope }));
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: Some(expected_channel),
        conversation_ref: Some(expected_conversation),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("large-voice-origin-turn".into()),
        idempotency_key: "send-idempotency-hash".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("large-opaque-voice".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(2_000),
            },
        }],
    };
    let inline = serde_json::to_vec(&message).unwrap();
    assert!(inline.len() < tinychannels_bus::module_relay::RELAY_FRAME_BYTES);
    assert!(!String::from_utf8_lossy(&inline).contains("audio_base64"));

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("53")
    );
    let uploads = raw_bodies(&server, "sendVoice").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("large-reply.ogg"));
    assert!(uploads[0].contains(&"Z".repeat(129 * 1024)));
    assert!(uploads[0].contains("reply_parameters"));
}

#[tokio::test]
async fn typed_voice_resolves_opaque_media_and_sends_real_telegram_voice_receipt() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({
            "message_id": 51
        })),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("host-media-1".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
            },
        }],
    };

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("51")
    );
    assert_eq!(
        outcome.receipt.parts[0].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Voice
    );
    let uploads = raw_bodies(&server, "sendVoice").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("name=\"voice\""));
    assert!(uploads[0].contains("reply.ogg"));
    assert!(uploads[0].contains("\u{1}\u{2}\u{3}\u{4}"));
    assert!(uploads[0].contains("reply_parameters"));
    assert!(uploads[0].contains("\"message_id\":42"));
}

#[tokio::test]
async fn typed_voice_sends_long_transcript_as_receipted_followup_text() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 54})),
    )
    .await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({"message_id": 55})),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let transcript = "transcript ".repeat(110);
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            topic_id: Some("77".into()),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: transcript.clone(),
                    markdown_dialect: None,
                },
            },
            tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("host-media-1".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        ..Default::default()
                    },
                    audio_base64: None,
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1000),
                },
            },
        ],
    };
    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(outcome.error, None);
    assert_eq!(outcome.receipt.platform_message_ids, ["54", "55"]);
    assert_eq!(
        outcome
            .receipt
            .parts
            .iter()
            .map(|part| part.kind)
            .collect::<Vec<_>>(),
        [
            tinychannels_bus::channel::MessageReceiptPartKind::Voice,
            tinychannels_bus::channel::MessageReceiptPartKind::Text,
        ]
    );
    let voice = raw_bodies(&server, "sendVoice").await;
    assert_eq!(voice.len(), 1);
    assert!(
        !voice[0].contains(&transcript),
        "overlong caption is omitted from multipart upload"
    );
    let text = json_bodies(&server, "sendMessage").await;
    assert_eq!(text.len(), 1);
    assert!(text[0]["text"].as_str().unwrap().contains("transcript"));
    assert_eq!(text[0]["message_thread_id"], "77");
    assert_eq!(text[0]["reply_to_message_id"], 42);
}

#[tokio::test]
async fn long_voice_transcript_failure_retains_partial_receipts_without_retry() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 56})),
    )
    .await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .respond_with(ok_with(serde_json::json!({"message_id": 57})))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(400).set_body_string("rejected"),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let transcript = "long transcript ".repeat(400);
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: transcript,
                    markdown_dialect: None,
                },
            },
            tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("host-media-1".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        ..Default::default()
                    },
                    audio_base64: None,
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1000),
                },
            },
        ],
    };
    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["56", "57"]);
    let error = outcome
        .error
        .expect("later transcript chunk failure stays visible");
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    assert_eq!(
        json_bodies(&server, "sendMessage").await.len(),
        2,
        "the operation makes one pass and preserves the accepted text chunk"
    );
    assert_eq!(
        raw_bodies(&server, "sendVoice").await.len(),
        1,
        "ambiguous transcript failure must not repeat the voice upload"
    );
}

#[tokio::test]
async fn typed_voice_keeps_transcript_as_caption_and_sends_one_native_receipt() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 52})),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let mut message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("host-media-1".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
            },
        }],
    };
    message.parts.insert(
        0,
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "recognized words".into(),
                markdown_dialect: None,
            },
        },
    );

    let result = channel.send_typed_with_receipt(&message).await;
    assert!(
        result.is_ok(),
        "mixed transcript+voice must be supported: {result:?}"
    );
    let uploads = raw_bodies(&server, "sendVoice").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("recognized words"));
    assert!(uploads[0].contains("reply_parameters"));
    assert_eq!(
        result
            .unwrap()
            .receipt
            .primary_platform_message_id
            .as_deref(),
        Some("52")
    );
}

#[tokio::test]
async fn typed_voice_rejects_missing_host_storage_reference_and_origin_turn() {
    let server = mock_api().await;
    let message = outbound_voice_message(Some("voice-ref"), Some("turn-1"));
    let missing_store = channel_for(&server, &["*"])
        .send_typed_with_receipt(&message)
        .await
        .unwrap_err();
    assert!(
        missing_store
            .to_string()
            .contains("requires host media resolution")
    );

    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let missing_reference = channel
        .send_typed_with_receipt(&outbound_voice_message(None, Some("turn-1")))
        .await
        .unwrap_err();
    assert!(
        missing_reference
            .to_string()
            .contains("no host-owned opaque reference")
    );

    let missing_turn = channel
        .send_typed_with_receipt(&outbound_voice_message(Some("voice-ref"), None))
        .await
        .unwrap_err();
    assert!(
        missing_turn
            .to_string()
            .contains("requires originating turn identity")
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

/// Hang guard for loops that must terminate on their own.
const GUARD: Duration = Duration::from_secs(20);

fn streaming(server: &MockServer) -> TelegramChannel {
    channel_for(server, &["*"]).with_streaming(StreamMode::Partial, 0, true)
}

#[tokio::test]
async fn send_draft_posts_placeholder_and_returns_message_id() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    let ch = streaming(&server);

    let id = ch
        .send_draft(&SendMessage::new("", "100:7").in_thread(Some("55".into())))
        .await
        .unwrap();

    assert_eq!(id.as_deref(), Some("321"));
    assert_eq!(
        json_bodies(&server, "sendMessage").await,
        vec![serde_json::json!({
            "chat_id": "100",
            "text": "...",
            "message_thread_id": "7",
            "reply_to_message_id": 55,
        })]
    );
    assert!(ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn send_draft_without_message_id_in_result_returns_none() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok_with(serde_json::json!({}))).await;
    let ch = streaming(&server);

    let id = ch
        .send_draft(&SendMessage::new("thinking", "100"))
        .await
        .unwrap();
    assert_eq!(id, None);
    assert_eq!(
        json_bodies(&server, "sendMessage").await[0]["text"],
        "thinking"
    );
}

#[tokio::test]
async fn send_draft_surfaces_api_rejection() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(400).set_body_string("chat not found"),
    )
    .await;
    let ch = streaming(&server);

    let err = ch
        .send_draft(&SendMessage::new("x", "100"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("sendMessage (draft) failed"), "{err}");
    assert!(err.contains("chat not found"), "{err}");
}

fn typed_text(id: &str, text: String) -> tinychannels_bus::ChannelOutboundMessage {
    tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "-10042".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "forged".into(),
            account_id: Some("untrusted-account".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "-10042".into(),
            scope_id: Some("scope".into()),
            parent_id: Some("parent".into()),
            thread_id: Some("77".into()),
            topic_id: Some("topic".into()),
        }),
        reply_to_id: Some("5".into()),
        thread_id: None,
        turn_id: None,
        idempotency_key: id.into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text,
                markdown_dialect: None,
            },
        }],
    }
}

#[tokio::test]
async fn typed_image_upload_reads_scoped_media_and_keeps_ordered_receipts() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({"message_id": 11})),
    )
    .await;
    mount_post(
        &server,
        "sendPhoto",
        ok_with(serde_json::json!({"message_id": 22})),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("ordered-image", "before".into());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "before".into(),
                markdown_dialect: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-photo-1".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("photo caption".into()),
                alt_text: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "after".into(),
                markdown_dialect: None,
            },
        },
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["11", "22", "11"]);
    assert_eq!(outcome.receipt.parts.len(), 3);
    assert_eq!(
        outcome.receipt.parts[1].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Media
    );
    assert_eq!(outcome.receipt.parts[1].thread_id.as_deref(), Some("77"));
    assert_eq!(
        outcome
            .receipt
            .conversation
            .as_ref()
            .and_then(|conversation| conversation.topic_id.as_deref()),
        Some("topic-77")
    );
    assert_eq!(outcome.receipt.parts[1].reply_to_id.as_deref(), Some("5"));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].url.path(), "/bottest-token/sendMessage");
    assert_eq!(requests[1].url.path(), "/bottest-token/sendPhoto");
    assert_eq!(requests[2].url.path(), "/bottest-token/sendMessage");
    let multipart = String::from_utf8_lossy(&requests[1].body);
    assert!(multipart.contains("photo caption"));
    assert!(multipart.contains("name=\"chat_id\"\r\n\r\n-10042"));
    assert!(multipart.contains("name=\"reply_parameters\"\r\n\r\n{\"message_id\":5}"));
    assert!(multipart.contains("name=\"message_thread_id\"\r\n\r\n77"));
    assert!(multipart.contains("photo.png"));
    assert!(multipart.contains("Content-Type: image/png"));
    let expected_bytes = BASE64
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
        .unwrap();
    assert!(
        requests[1]
            .body
            .windows(expected_bytes.len())
            .any(|window| window == expected_bytes)
    );
}

#[tokio::test]
async fn typed_image_rejects_non_numeric_platform_thread_before_upload() {
    let server = mock_api().await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(UncheckedMediaReadFixture));
    let mut message = typed_text("invalid-thread", "caption".into());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        thread_id: None,
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-photo-1".into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }];

    assert!(channel.send_typed_with_receipt(&message).await.is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_media_failure_after_text_keeps_real_partial_ids_and_forbids_retry() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({"message_id": 11})),
    )
    .await;
    mount_post(
        &server,
        "sendPhoto",
        ResponseTemplate::new(400).set_body_string("rejected"),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("partial-image", "before".into());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message
        .parts
        .push(tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-photo-1".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        });

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["11"]);
    let error = outcome.error.expect("partial delivery must be explicit");
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].url.path().ends_with("/sendMessage"));
    assert!(requests[1].url.path().ends_with("/sendPhoto"));
}

#[tokio::test]
async fn configured_local_adapter_accepts_and_delivers_typed_telegram_media() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendPhoto",
        ok_with(serde_json::json!({"message_id": 22})),
    )
    .await;
    let channel = Arc::new(
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture)),
    );
    let account = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    };
    let conversation = tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    };
    let adapter = tinychannels_runtime::adapters::LocalChannelAdapter::new(
        tinychannels_bus::channel::ChannelDescriptor {
            id: account.id.clone(),
            account_id: account.account_id.clone(),
            ..Default::default()
        },
        channel.capabilities(),
        Arc::new(TelegramConformanceSink(channel)),
    );
    let outcome = adapter
        .send(tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "adapter-photo".into(),
            channel_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            channel_ref: Some(account.clone()),
            conversation_ref: Some(conversation.clone()),
            turn_id: Some("turn-42".into()),
            payload: tinychannels_bus::OutboundPayload::Parts {
                parts: vec![tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Image {
                        media: tinychannels_bus::MediaReference {
                            opaque_id: Some("stored-photo-1".into()),
                            kind: tinychannels_bus::MediaKind::Image,
                            ..Default::default()
                        },
                        caption: Some("caption".into()),
                        alt_text: None,
                    },
                }],
            },
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["22"]);
    assert!(outcome.error.is_none());
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].url.path().ends_with("/sendPhoto"));
}

#[tokio::test]
async fn typed_audio_upload_preserves_caption_title_and_duration() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendAudio",
        ok_with(serde_json::json!({"message_id": 44})),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("audio-metadata", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        topic_id: Some("topic-77".into()),
        thread_id: Some("77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Audio {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-audio-1".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            },
            codec: Some("mp3".into()),
            duration_ms: Some(1_250),
            title: Some("Original title".into()),
            transcript: Some("spoken words".into()),
        },
    }];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["44"]);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].url.path().ends_with("/sendAudio"));
    let body = String::from_utf8_lossy(&requests[0].body);
    assert!(body.contains("name=\"caption\"\r\n\r\nspoken words"));
    assert!(body.contains("name=\"title\"\r\n\r\nOriginal title"));
    assert!(body.contains("name=\"duration\"\r\n\r\n2"));
    assert!(body.contains("Content-Type: audio/mpeg"));
}

#[tokio::test]
async fn typed_video_and_file_upload_use_native_multipart_with_real_receipts() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVideo",
        ok_with(serde_json::json!({"message_id": 51})),
    )
    .await;
    mount_post(
        &server,
        "sendDocument",
        ok_with(serde_json::json!({"message_id": 52})),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("video-and-file", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Video {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-video-mp4".into()),
                    kind: tinychannels_bus::MediaKind::Video,
                    ..Default::default()
                },
                caption: Some("video caption".into()),
                duration_ms: Some(2_200),
                thumbnail: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::File {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-document-pdf".into()),
                    kind: tinychannels_bus::MediaKind::Document,
                    ..Default::default()
                },
                file_name: Some("report.pdf".into()),
                caption: Some("file caption".into()),
            },
        },
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["51", "52"]);
    assert_eq!(outcome.receipt.parts.len(), 2);
    assert_eq!(outcome.receipt.parts[0].thread_id.as_deref(), Some("77"));
    assert_eq!(outcome.receipt.parts[1].thread_id.as_deref(), Some("77"));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].url.path().ends_with("/sendVideo"));
    assert!(requests[1].url.path().ends_with("/sendDocument"));
    let video = String::from_utf8_lossy(&requests[0].body);
    assert!(video.contains("Content-Type: video/mp4"));
    assert!(video.contains("name=\"caption\"\r\n\r\nvideo caption"));
    assert!(video.contains("name=\"duration\"\r\n\r\n3"));
    assert!(video.contains("name=\"message_thread_id\"\r\n\r\n77"));
    assert!(
        requests[0]
            .body
            .windows(8)
            .any(|window| window == b"ftypisom")
    );
    let file = String::from_utf8_lossy(&requests[1].body);
    assert!(file.contains("Content-Type: application/pdf"));
    assert!(file.contains("report.pdf"));
    assert!(file.contains("name=\"caption\"\r\n\r\nfile caption"));
    assert!(
        requests[1]
            .body
            .windows(8)
            .any(|window| window == b"%PDF-1.7")
    );
}

#[tokio::test]
async fn unsupported_native_video_and_image_formats_fall_back_to_documents() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendDocument"))
        .up_to_n_times(1)
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true,
            "result": {"message_id": 61}
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendDocument"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true,
            "result": {"message_id": 62}
        })))
        .mount(&server)
        .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("document-fallbacks", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Video {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-video-webm".into()),
                    kind: tinychannels_bus::MediaKind::Video,
                    ..Default::default()
                },
                caption: Some("webm fallback".into()),
                duration_ms: None,
                thumbnail: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-image-gif".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("gif fallback".into()),
                alt_text: None,
            },
        },
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["61", "62"]);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(request.url.path().ends_with("/sendDocument"));
        let body = String::from_utf8_lossy(&request.body);
        assert!(body.contains("name=\"message_thread_id\"\r\n\r\n77"));
    }
    let webm = String::from_utf8_lossy(&requests[0].body);
    assert!(webm.contains("Content-Type: video/webm"));
    assert!(webm.contains("webm fallback"));
    assert!(
        requests[0]
            .body
            .windows(4)
            .any(|window| window == b"\x1a\x45\xdf\xa3")
    );
    let gif = String::from_utf8_lossy(&requests[1].body);
    assert!(gif.contains("Content-Type: image/gif"));
    assert!(gif.contains("gif fallback"));
    assert!(
        requests[1]
            .body
            .windows(6)
            .any(|window| window == b"GIF89a")
    );
}

#[tokio::test]
async fn telegram_rejects_wav_audio_before_native_upload() {
    let server = mock_api().await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("unsupported-wav", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Audio {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-wav-1".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            },
            codec: Some("wav_pcm16".into()),
            duration_ms: None,
            title: None,
            transcript: None,
        },
    }];

    let error = channel.send_typed_with_receipt(&message).await.unwrap_err();

    assert!(error.to_string().contains("MP3 or M4A"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn telegram_photo_over_ten_megabytes_is_rejected_before_http() {
    let server = mock_api().await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("oversized-photo", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("oversized-photo".into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }];

    let error = channel.send_typed_with_receipt(&message).await.unwrap_err();

    assert!(error.to_string().contains("upload method limit"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

fn valid_mp3_frame() -> Vec<u8> {
    // MPEG-1 Layer III, 128 kbps, 44.1 kHz frame (417 bytes).
    let mut bytes = vec![0_u8; 417];
    bytes[..4].copy_from_slice(&[0xff, 0xfb, 0x90, 0x64]);
    bytes
}

fn oversized_jpeg_with_dimensions() -> Vec<u8> {
    let mut bytes = vec![0_u8; 10 * 1024 * 1024 + 1];
    bytes[..11].copy_from_slice(&[
        0xff, 0xd8, 0xff, 0xc0, 0x00, 0x11, 0x08, 0x00, 0x01, 0x00, 0x01,
    ]);
    bytes
}

#[tokio::test]
async fn typed_send_receipt_uses_actual_telegram_ids_and_full_address() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let outcome = ch
        .send_typed_with_receipt(&typed_text("idem", "hello".into()))
        .await
        .unwrap();
    assert_eq!(outcome.error, None);
    assert_eq!(outcome.receipt.platform_message_ids, ["321"]);
    assert_eq!(outcome.receipt.parts[0].edit_token.as_deref(), Some("321"));
    assert_eq!(
        outcome.receipt.parts[0].delete_token.as_deref(),
        Some("321")
    );
    assert_eq!(
        outcome
            .receipt
            .conversation
            .as_ref()
            .unwrap()
            .topic_id
            .as_deref(),
        Some("topic")
    );
    assert_eq!(outcome.receipt.parts[0].reply_to_id.as_deref(), Some("5"));
}

#[tokio::test]
async fn configured_telegram_adapter_passes_mock_wire_conformance() {
    use tinychannels_conformance::{ConformanceFeature, ExpectedScope, check_feature};

    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    let channel = Arc::new(channel_for(&server, &["*"]));
    let scope = ExpectedScope {
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram:test-account".into()),
        },
        conversation: tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        },
        reply_to_id: None,
        thread_id: None,
    };
    let adapter = tinychannels_runtime::adapters::LocalChannelAdapter::new(
        tinychannels_bus::channel::ChannelDescriptor {
            id: scope.channel.id.clone(),
            display_name: "Telegram test account".into(),
            account_id: scope.channel.account_id.clone(),
            ..Default::default()
        },
        channel.capabilities(),
        Arc::new(TelegramConformanceSink(channel)),
    );
    let fixture = TelegramWireFixture {
        adapter,
        server,
        observations: std::sync::Mutex::new(Vec::new()),
        request_cursor: std::sync::Mutex::new(0),
        scope,
    };
    check_feature(&fixture, ConformanceFeature::Text)
        .await
        .unwrap();
    let observation = fixture.observations().pop().unwrap();
    assert_eq!(observation.request["chat_id"], "100");
    assert_eq!(observation.request["text"], "hello-kit");
    assert_eq!(observation.platform_message_ids, ["321"]);

    check_feature(&fixture, ConformanceFeature::VoiceSynthesis)
        .await
        .unwrap();
    assert!(
        fixture.observations().is_empty(),
        "undeclared voice synthesis must not reach the mock platform"
    );
    check_feature(&fixture, ConformanceFeature::Image)
        .await
        .unwrap();
    assert!(fixture.observations().is_empty());
}

#[tokio::test]
async fn typed_send_receipt_keeps_ordered_multiple_text_parts() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 654 })),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let mut message = typed_text("multi-text", "first".into());
    message.conversation_ref = None;
    message.conversation_id = "600".into();
    message.thread_id = Some("18".into());
    message
        .parts
        .push(tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "second".into(),
                markdown_dialect: None,
            },
        });

    let outcome = ch.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(
        json_bodies(&server, "sendMessage").await[0]["text"],
        "first\nsecond"
    );
    assert_eq!(outcome.receipt.platform_message_ids, ["654"]);
    assert_eq!(
        outcome.receipt.idempotency_key.as_deref(),
        Some("multi-text")
    );
    assert_eq!(outcome.receipt.parts[0].edit_token.as_deref(), Some("654"));
    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(bodies[0]["chat_id"], "600");
    assert_eq!(bodies[0]["message_thread_id"], "18");
    assert_eq!(outcome.receipt.thread_id.as_deref(), Some("18"));
}

#[tokio::test]
async fn typed_send_receipt_applies_tool_call_tag_sanitization() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 655 })),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let message = typed_text(
        "sanitize",
        "before<tool_call>hidden</tool_call>[REACTION:👍|321] [IMAGE:https://example.test/image.png]".into(),
    );

    let outcome = ch.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.error, None);
    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(
        bodies[0]["text"],
        "before[REACTION:👍|321] [IMAGE:https://example.test/image.png]"
    );
    assert_eq!(bodies[0]["message_id"], serde_json::Value::Null);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "typed markers remain text, not operations"
    );
    assert_eq!(outcome.receipt.platform_message_ids, ["655"]);
}

#[tokio::test]
async fn typed_send_rejects_non_numeric_reply_reference_before_provider_io() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);
    let mut message = typed_text("bad-reply-id", "hello".into());
    message.reply_to_id = Some("reply:legacy".into());

    let error = ch.send_typed_with_receipt(&message).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("reply_to_id must be a numeric message ID")
    );
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[test]
#[allow(
    deprecated,
    reason = "the normalization fallback is specifically for the v21 provider adapter"
)]
fn inbound_envelope_keeps_chat_channel_and_legacy_identity_fallbacks() {
    let legacy = ChannelMessage {
        id: "telegram_-100_7".into(),
        sender: "legacy-sender".into(),
        sender_name: Some("Legacy display".into()),
        reply_target: "-100:topic-4".into(),
        content: "legacy body".into(),
        channel: "telegram".into(),
        ..Default::default()
    };
    let channel_update = serde_json::json!({
        "message": {
            "message_id": 7,
            "chat": {"type":"channel"},
            "sender_chat": {"id": 9001},
            "reply_to_message": {"message_id": 3}
        }
    });
    let channel = TelegramChannel::inbound_envelope_from_update(&channel_update, &legacy, None);
    assert_eq!(
        channel.conversation.kind,
        tinychannels_bus::ConversationKind::Channel
    );
    assert_eq!(channel.conversation.id, "-100");
    assert_eq!(channel.sender.id, "9001");
    assert_eq!(channel.reply_to_id.as_deref(), Some("3"));
    assert_eq!(channel.sender.name.as_deref(), Some("Legacy display"));

    let unknown_update = serde_json::json!({
        "message": {
            "chat": {"id": 17, "type":"unknown"},
            "from": {}
        }
    });
    let unknown = TelegramChannel::inbound_envelope_from_update(&unknown_update, &legacy, None);
    assert_eq!(
        unknown.conversation.kind,
        tinychannels_bus::ConversationKind::Unknown
    );
    assert_eq!(unknown.conversation.id, "17");
    assert_eq!(unknown.sender.id, "legacy-sender");
}

#[test]
fn voice_ingress_keeps_audio_part_beside_provider_transcript() {
    let legacy = ChannelMessage {
        id: "telegram_100_9".into(),
        sender: "alice".into(),
        reply_target: "100".into(),
        content: "recognized words".into(),
        channel: "telegram".into(),
        ..Default::default()
    };
    let update = serde_json::json!({
        "message": {
            "message_id": 9,
            "chat": {"id": 100, "type": "private"},
            "from": {"id": 8},
            "voice": {
                "file_id": "opaque-provider-file-id",
                "file_unique_id": "opaque-unique-id",
                "file_size": 16,
                "mime_type": "audio/ogg",
                "duration": 3
            }
        }
    });

    let mut voice_note = tinychannels_bus::VoiceNote {
        media: tinychannels_bus::MediaReference {
            opaque_id: Some("store-owned-voice-7".into()),
            content_type: Some("audio/ogg".into()),
            kind: tinychannels_bus::MediaKind::Audio,
            ..Default::default()
        },
        duration_ms: Some(3_000),
        ..Default::default()
    };
    voice_note.transcript = Some(tinychannels_bus::VoiceTranscript {
        text: legacy.content.clone(),
        language: None,
        state: tinychannels_bus::VoiceTranscriptState::Complete,
        source: tinychannels_bus::VoiceTranscriptSource::Provider,
    });
    let envelope =
        TelegramChannel::inbound_envelope_from_update(&update, &legacy, Some(voice_note));

    assert_eq!(envelope.text, "recognized words");
    assert_eq!(envelope.parts.len(), 2);
    assert!(matches!(
        envelope.parts[0],
        tinychannels_bus::ContentPart::Text { .. }
    ));
    let tinychannels_bus::ContentPart::VoiceNote { voice_note } = &envelope.parts[1] else {
        panic!("voice ingress must retain a typed audio part")
    };
    assert_eq!(voice_note.media.content_type.as_deref(), Some("audio/ogg"));
    assert_eq!(voice_note.duration_ms, Some(3_000));
    assert_eq!(
        voice_note
            .transcript
            .as_ref()
            .map(|transcript| transcript.text.as_str()),
        Some("recognized words")
    );
    assert_eq!(
        voice_note
            .transcript
            .as_ref()
            .map(|transcript| transcript.source),
        Some(tinychannels_bus::VoiceTranscriptSource::Provider)
    );
    assert_ne!(
        voice_note.media.url.as_deref(),
        Some("opaque-provider-file-id"),
        "provider-private file IDs are not durable media URLs"
    );
}

#[tokio::test]
async fn typed_send_keeps_delivered_chunk_ids_when_later_chunk_fails() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .respond_with(ok_with(serde_json::json!({ "message_id": 321 })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(400).set_body_string("rejected"),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let mut message = typed_text("idem", "x".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 50));
    message
        .parts
        .push(tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "tail".into(),
                markdown_dialect: None,
            },
        });
    let outcome = ch.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["321"]);
    assert_eq!(outcome.receipt.parts.len(), 1);
    assert!(outcome.error.as_ref().is_some_and(|error| !error.retryable));
    assert!(outcome.error.as_ref().unwrap().sent_before_error);
    assert_eq!(
        outcome.error.as_ref().unwrap().continuation_message_ids,
        ["321"]
    );
    let public_error = serde_json::to_string(&outcome.error).unwrap();
    assert!(
        !public_error.contains(TOKEN),
        "bot token must not enter structured errors"
    );
}

#[tokio::test]
async fn typed_send_without_provider_message_id_is_ambiguous_and_not_retried() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok_with(serde_json::json!({}))).await;
    let ch = channel_for(&server, &["*"]);
    let outcome = ch
        .send_typed_with_receipt(&typed_text("missing-id", "hello".into()))
        .await
        .unwrap();
    let error = outcome.error.expect("missing ID cannot report success");
    assert!(
        error.sent_before_error,
        "Telegram accepted the request, so retry safety is unknown"
    );
    assert!(outcome.receipt.platform_message_ids.is_empty());
    assert!(error.message.contains("usable message ID"));
    assert!(!error.message.contains(TOKEN));
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 1);
}

#[tokio::test]
async fn typed_send_connection_refusal_is_definitely_not_sent_and_sanitized() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = format!("http://{address}");
    let outcome = ch
        .send_typed_with_receipt(&typed_text("transport-error", "hello".into()))
        .await
        .unwrap();
    let error = outcome
        .error
        .expect("transport failure must not become success");
    assert!(
        !error.sent_before_error,
        "connection refusal precedes dispatch"
    );
    assert!(error.message.contains("request failed"));
    assert!(!error.message.contains(TOKEN));
    assert!(!serde_json::to_string(&error).unwrap().contains(TOKEN));
}

#[tokio::test]
async fn accepted_request_disconnect_remains_ambiguous() {
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or_default();
            if bytes.len() >= header_end + 4 + content_length {
                return bytes;
            }
        }
        bytes
    });
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = format!("http://{address}");

    let outcome = ch
        .send_typed_with_receipt(&typed_text("disconnected", "hello".into()))
        .await
        .unwrap();
    let request = accepted.await.unwrap();
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap();
    let body = &request[header_end + 4..];
    assert_eq!(body.len(), content_length);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(body).unwrap()["text"],
        "hello"
    );
    let error = outcome.error.expect("server accepted then disconnected");
    assert!(error.sent_before_error);
    assert!(outcome.receipt.platform_message_ids.is_empty());
}

#[tokio::test]
async fn fallback_connection_refusal_after_markdown_rejection_is_not_sent() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let first_request = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or_default();
            if bytes.len() >= header_end + 4 + content_length {
                let body = br#"{"description":"Bad Request: can't parse entities"}"#;
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.write_all(body).await.unwrap();
                return bytes;
            }
        }
        bytes
    });
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = format!("http://{address}");

    let outcome = ch
        .send_typed_with_receipt(&typed_text("fallback-refused", "hello".into()))
        .await
        .unwrap();
    let request = first_request.await.unwrap();
    assert!(request.windows(10).any(|window| window == b"parse_mode"));
    let error = outcome.error.expect("fallback connection is refused");
    assert!(!error.sent_before_error);
    assert!(outcome.receipt.platform_message_ids.is_empty());
}

#[tokio::test]
async fn update_draft_edits_message_and_records_edit_time() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);

    ch.update_draft("100:3", "12", "partial").await.unwrap();

    assert_eq!(
        json_bodies(&server, "editMessageText").await,
        vec![serde_json::json!({ "chat_id": "100", "message_id": 12, "text": "partial" })]
    );
    assert!(ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn update_draft_truncates_oversized_text_to_limit() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);
    let text = "é".repeat(TELEGRAM_MAX_MESSAGE_LENGTH);

    ch.update_draft("100", "12", &text).await.unwrap();

    let sent = json_bodies(&server, "editMessageText").await[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(sent.len() <= TELEGRAM_MAX_MESSAGE_LENGTH);
    assert_eq!(
        sent.len(),
        TELEGRAM_MAX_MESSAGE_LENGTH,
        "two-byte chars fill the limit"
    );
}

#[tokio::test]
async fn update_draft_rejection_is_swallowed_and_not_recorded() {
    let server = mock_api().await;
    mount_post(
        &server,
        "editMessageText",
        ResponseTemplate::new(400).set_body_string("message is not modified"),
    )
    .await;
    let ch = streaming(&server);

    ch.update_draft("100", "12", "same").await.unwrap();
    assert!(!ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn update_draft_invalid_message_id_skips_network() {
    let server = mock_api().await;
    let ch = streaming(&server);
    ch.update_draft("100", "abc", "text").await.unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn finalize_draft_edits_with_markdown_and_clears_rate_limit() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);
    ch.last_draft_edit
        .lock()
        .insert("100".into(), std::time::Instant::now());

    ch.finalize_draft("100", "8", "done <tool_call>{}</tool_call>", None)
        .await
        .unwrap();

    let bodies = json_bodies(&server, "editMessageText").await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["parse_mode"], "Markdown");
    assert_eq!(bodies[0]["text"], "done");
    assert!(ch.last_draft_edit.lock().is_empty());
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn finalize_draft_retries_plain_edit_when_markdown_rejected() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("editMessageText")))
        .and(body_partial_json(
            serde_json::json!({ "parse_mode": "Markdown" }),
        ))
        .respond_with(ResponseTemplate::new(400))
        .with_priority(1)
        .mount(&server)
        .await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100", "8", "final *text", None)
        .await
        .unwrap();

    let bodies = json_bodies(&server, "editMessageText").await;
    assert_eq!(bodies.len(), 2);
    assert!(bodies[1].get("parse_mode").is_none());
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn finalize_draft_falls_back_to_new_message_when_edits_fail() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ResponseTemplate::new(400)).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100:4", "8", "final", Some("77"))
        .await
        .unwrap();

    assert_eq!(json_bodies(&server, "editMessageText").await.len(), 2);
    let sent = json_bodies(&server, "sendMessage").await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["text"], "final");
    assert_eq!(sent[0]["message_thread_id"], "4");
    assert_eq!(sent[0]["reply_to_message_id"], 77);
}

#[tokio::test]
async fn finalize_draft_short_text_with_invalid_id_sends_new_message() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100", "nope", "final", None)
        .await
        .unwrap();

    assert!(json_bodies(&server, "editMessageText").await.is_empty());
    assert_eq!(sent_texts(&server).await, vec!["final".to_string()]);
}

#[tokio::test]
async fn finalize_draft_oversized_text_deletes_draft_and_sends_chunks() {
    let server = mock_api().await;
    mount_post(&server, "deleteMessage", ok_with(serde_json::json!(true))).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);
    let text = "y".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 1);

    ch.finalize_draft("100", "8", &text, None).await.unwrap();

    assert_eq!(
        json_bodies(&server, "deleteMessage").await,
        vec![serde_json::json!({ "chat_id": "100", "message_id": 8 })]
    );
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

#[tokio::test]
async fn finalize_draft_oversized_text_with_invalid_id_sends_chunks_only() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);
    let text = "y".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 1);

    ch.finalize_draft("100", "bad", &text, None).await.unwrap();

    assert!(json_bodies(&server, "deleteMessage").await.is_empty());
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

#[tokio::test]
async fn send_reaction_marker_targets_explicit_id_then_sends_reply_text() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new("[REACTION:👍|99] Great point", "100:2").in_thread(Some("5".into())))
        .await
        .unwrap();

    let reactions = json_bodies(&server, "setMessageReaction").await;
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0]["message_id"], 99);
    assert_eq!(reactions[0]["chat_id"], "100");
    let sent = json_bodies(&server, "sendMessage").await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["text"], "Great point");
    assert_eq!(sent[0]["message_thread_id"], "2");
    assert_eq!(sent[0]["reply_to_message_id"], 5);
}

#[tokio::test]
async fn send_reaction_only_marker_uses_parent_id_and_sends_no_text() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new("[REACTION:🔥]", "100").in_thread(Some("12".into())))
        .await
        .unwrap();

    let reactions = json_bodies(&server, "setMessageReaction").await;
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0]["message_id"], 12);
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn send_reaction_marker_without_target_is_ignored() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new("[REACTION:🔥]", "100"))
        .await
        .unwrap();

    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn send_with_attachment_markers_sends_text_then_attachments() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    mount_post(&server, "sendPhoto", ok()).await;
    mount_post(&server, "sendDocument", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new(
        "Here you go [IMAGE:https://cdn.example/a.png] [DOCUMENT:https://cdn.example/b.pdf]",
        "100",
    ))
    .await
    .unwrap();

    assert_eq!(sent_texts(&server).await, vec!["Here you go".to_string()]);
    assert_eq!(
        json_bodies(&server, "sendPhoto").await[0]["photo"],
        "https://cdn.example/a.png"
    );
    assert_eq!(
        json_bodies(&server, "sendDocument").await[0]["document"],
        "https://cdn.example/b.pdf"
    );
}

#[tokio::test]
async fn send_attachment_marker_only_skips_text_message() {
    let server = mock_api().await;
    mount_post(&server, "sendVideo", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new(
        "[VIDEO:https://cdn.example/v.mp4]",
        "100",
    ))
    .await
    .unwrap();

    assert!(json_bodies(&server, "sendMessage").await.is_empty());
    assert_eq!(json_bodies(&server, "sendVideo").await.len(), 1);
}

#[tokio::test]
async fn send_bare_existing_path_uploads_it_as_attachment() {
    let server = mock_api().await;
    mount_post(&server, "sendDocument", ok()).await;
    let ch = channel_for(&server, &["*"]);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("report.pdf");
    std::fs::write(&file, b"%PDF").unwrap();

    ch.send(&SendMessage::new(file.to_str().unwrap(), "100"))
        .await
        .unwrap();

    assert!(json_bodies(&server, "sendMessage").await.is_empty());
    let uploads = raw_bodies(&server, "sendDocument").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("report.pdf"));
}

#[tokio::test]
async fn health_check_reflects_get_me_status() {
    let server = mock_api().await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "username": "b" })))
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);
    assert!(ch.health_check().await);

    let failing = mock_api().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&failing)
        .await;
    assert!(!channel_for(&failing, &["*"]).health_check().await);

    let mut dead = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    dead.api_base = dead_base();
    assert!(!dead.health_check().await);
}

#[tokio::test]
async fn start_typing_sends_immediately_and_reuses_task_for_same_recipient() {
    let server = mock_api().await;
    mount_post(&server, "sendChatAction", ok_with(serde_json::json!(true))).await;
    let ch = channel_for(&server, &["*"]);

    ch.start_typing("100:6").await.unwrap();
    // The background refresher's first iteration posts right away; wait for it.
    tokio::time::timeout(GUARD, async {
        while json_bodies(&server, "sendChatAction").await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typing refresher posted");
    let first_task_id = ch.typing_handle.lock().as_ref().unwrap().handle.id();

    ch.start_typing("100:6").await.unwrap();
    assert_eq!(
        ch.typing_handle.lock().as_ref().unwrap().handle.id(),
        first_task_id,
        "same recipient keeps the running refresher"
    );

    let bodies = json_bodies(&server, "sendChatAction").await;
    assert!(bodies.iter().all(|b| b["chat_id"] == "100"
        && b["action"] == "typing"
        && b["message_thread_id"] == "6"));

    ch.stop_typing("100:6").await.unwrap();
    assert!(ch.typing_handle.lock().is_none());
}

#[tokio::test]
async fn typing_refresh_reports_acceptance_rejection_and_transport_error() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/accept"))
        .respond_with(ok_with(serde_json::json!(true)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/reject"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": false })))
        .mount(&server)
        .await;
    let client = reqwest::Client::new();
    let body = TelegramChannel::typing_body_for_recipient("100");

    let accept = format!("{}/accept", server.uri());
    let reject = format!("{}/reject", server.uri());
    let dead = format!("{}/x", dead_base());
    assert!(TelegramChannel::refresh_typing_once(&client, &accept, &body, "100").await);
    assert!(!TelegramChannel::refresh_typing_once(&client, &reject, &body, "100").await);
    assert!(!TelegramChannel::refresh_typing_once(&client, &dead, &body, "100").await);

    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 2);
    let sent: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(
        sent,
        serde_json::json!({ "chat_id": "100", "action": "typing" })
    );
}

#[test]
fn telegram_advertises_reaction_support() {
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    assert!(ch.supports_reactions());
}

fn fast_backoff(mut ch: TelegramChannel) -> TelegramChannel {
    ch.poll_error_backoff = Duration::from_millis(1);
    ch.poll_conflict_backoff = Duration::from_millis(1);
    ch
}

/// Mount a one-shot `getUpdates` response, consumed in mount order.
async fn mount_get_updates_once(server: &MockServer, priority: u8, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path(api_path("getUpdates")))
        .respond_with(response)
        .up_to_n_times(1)
        .with_priority(priority)
        .mount(server)
        .await;
}

fn updates(result: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": true, "result": result }))
}

#[test]
fn telegram_v2_ingress_preserves_plain_and_forum_addresses_without_legacy_topic_guessing() {
    let legacy = ChannelMessage {
        id: "telegram_100_5".into(),
        sender: "alice".into(),
        reply_target: "100:88".into(),
        content: "message".into(),
        channel: "telegram".into(),
        timestamp: 12,
        thread_ts: Some("5".into()),
        ..Default::default()
    };
    let plain = TelegramChannel::inbound_envelope_from_update(
        &serde_json::json!({"message":{"message_id":5,"date":12,"chat":{"id":100,"type":"private"},"from":{"id":7}}}),
        &legacy,
        None,
    );
    assert_eq!(plain.message_id, "telegram_100_5");
    assert_eq!(
        plain.conversation.kind,
        tinychannels_bus::ConversationKind::Dm
    );
    assert_eq!(plain.conversation.id, "100");
    assert_eq!(plain.conversation.topic_id, None);
    assert_eq!(plain.conversation.thread_id, None);
    assert_eq!(plain.sender.id, "7");
    assert_eq!(plain.raw.as_ref().unwrap()["providerMessageId"], "5");

    let forum = TelegramChannel::inbound_envelope_from_update(
        &serde_json::json!({"message":{"message_id":5,"date":12,"message_thread_id":88,"chat":{"id":100,"type":"supergroup"},"from":{"id":7}}}),
        &legacy,
        None,
    );
    assert_eq!(
        forum.conversation.kind,
        tinychannels_bus::ConversationKind::Group
    );
    assert_eq!(forum.conversation.id, "100");
    assert_eq!(forum.conversation.topic_id.as_deref(), Some("88"));
    assert_eq!(forum.conversation.thread_id, None);
}

#[tokio::test]
async fn listen_recovers_from_poll_errors_and_dispatches_updates() {
    let server = mock_api().await;
    // Poll sequence: unparseable body, generic API error, 409 without webhook,
    // 409 webhook conflict that deleteWebhook clears, then a real batch.
    mount_get_updates_once(
        &server,
        1,
        ResponseTemplate::new(200).set_body_string("<html>"),
    )
    .await;
    mount_get_updates_once(
        &server,
        2,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 500, "description": "Internal"
        })),
    )
    .await;
    mount_get_updates_once(
        &server,
        3,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "Conflict: terminated by other getUpdates request"
        })),
    )
    .await;
    mount_get_updates_once(
        &server,
        4,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "Conflict: can't use getUpdates method while webhook is active"
        })),
    )
    .await;
    mount_post(&server, "deleteWebhook", ok_with(serde_json::json!(true))).await;
    mount_get_updates_once(
        &server,
        5,
        updates(serde_json::json!([
            {
                "update_id": 11,
                "message": {
                    "message_id": 5, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 8, "username": "mallory" }, "text": "let me in"
                }
            },
            {
                "update_id": 12,
                "message": {
                    "message_id": 5, "message_thread_id": 88,
                    "chat": { "id": 100, "type": "supergroup" },
                    "from": { "id": 7, "username": "alice" }, "text": "original hello"
                }
            },
            {
                "update_id": 13,
                "edited_message": {
                    "message_id": 5, "edit_date": 1234,
                    "message_thread_id": 88,
                    "chat": { "id": 100, "type": "supergroup" },
                    "from": { "id": 7, "username": "alice" }, "text": "edited hello"
                }
            },
            {
                "update_id": 14,
                "edited_message": {
                    "message_id": 5, "edit_date": 1235,
                    "chat": { "id": 100, "type": "supergroup" },
                    "from": { "id": 8, "username": "mallory" }, "text": "unauthorized edit"
                }
            },
            {
                "update_id": 15,
                "message_reaction": {
                    "chat": { "id": 100, "type": "supergroup" }, "message_id": 5, "message_thread_id": 88,
                    "user": { "id": 7, "username": "alice" },
                    "new_reaction": [{ "type": "emoji", "emoji": "❤" }]
                }
            },
        ])),
    )
    .await;
    // Next poll re-delivers 12 (deduped) and a new message that the closed
    // receiver can no longer take, which ends the loop.
    mount_get_updates_once(
        &server,
        6,
        updates(serde_json::json!([
            {
                "update_id": 12,
                "message": {
                    "message_id": 6, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "hello bot"
                }
            },
            {
                "update_id": 15,
                "message": {
                    "message_id": 9, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "second"
                }
            }
        ])),
    )
    .await;
    // Any later poll delivers yet another update, so the loop ends on the
    // closed receiver no matter how far it ran ahead of the test.
    Mock::given(method("POST"))
        .and(path(api_path("getUpdates")))
        .respond_with(updates(serde_json::json!([
            {
                "update_id": 16,
                "message": {
                    "message_id": 10, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "third"
                }
            }
        ])))
        .with_priority(10)
        .mount(&server)
        .await;
    mount_post(&server, "sendMessage", ok()).await;

    let events = Arc::new(RecordingEvents::default());
    let ch = Arc::new(fast_backoff(
        channel_for(&server, &["alice"]).with_events(events.clone()),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn({
        let ch = ch.clone();
        async move { ch.listen(tx).await }
    });

    let first = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("message dispatched")
        .expect("channel open");
    assert_eq!(first.content, "original hello");
    assert_eq!(first.sender, "alice");
    drop(rx);

    tokio::time::timeout(GUARD, listener)
        .await
        .expect("listen returns once receiver is gone")
        .unwrap()
        .unwrap();

    let recorded = events.events.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].1, "message_updated");
    assert_eq!(recorded[0].2["type"], "message_updated");
    assert_eq!(recorded[0].2["sourceEventId"], "13");
    assert_eq!(recorded[0].2["conversation"]["id"], "100");
    assert_eq!(recorded[0].2["messageId"], "telegram_100_5");
    assert_eq!(recorded[0].2["providerMessageId"], "5");
    assert_eq!(recorded[0].2["conversation"]["kind"], "group");
    assert_eq!(recorded[0].2["conversation"]["topicId"], "88");
    assert_eq!(recorded[0].2["parts"][0]["text"], "edited hello");
    assert_eq!(recorded[1].1, "reaction_added");
    assert_eq!(recorded[1].2["type"], "reaction_added");
    assert_eq!(recorded[1].2["sourceEventId"], "15");
    assert_eq!(
        recorded[1].2["eventId"],
        "telegram:update:15:reaction:added:100:5:❤"
    );
    assert_eq!(recorded[1].2["messageId"], "telegram_100_5");
    assert_eq!(recorded[1].2["providerMessageId"], "5");
    assert_eq!(recorded[1].2["conversation"]["id"], "100");
    assert_eq!(recorded[1].2["conversation"]["topicId"], "88");
    assert_eq!(recorded[1].2["actor"]["id"], "7");
    assert_eq!(recorded[1].2["reaction"]["value"], "❤");
    assert_eq!(json_bodies(&server, "deleteWebhook").await.len(), 1);
    // The unauthorized sender got the approval prompt.
    let prompts = sent_texts(&server).await;
    assert_eq!(prompts.len(), 1);
    assert!(prompts[0].contains("operator approval"), "{prompts:?}");
    // Offsets advance past each delivered batch.
    let polls = json_bodies(&server, "getUpdates").await;
    assert!(polls.len() >= 6, "{polls:?}");
    assert_eq!(polls[0]["offset"], 0);
    assert!(polls.iter().any(|poll| poll["offset"] == 16));
}

#[derive(Default)]
struct RejectingInboundEvents(AtomicUsize);

#[async_trait::async_trait]
impl crate::host::EventSink for RejectingInboundEvents {
    async fn publish(
        &self,
        _domain: &str,
        _kind: &str,
        _payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> anyhow::Result<bool> {
        if matches!(ingress, tinychannels_bus::ChannelIngress::Inbound(_)) {
            self.0.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("fixture ingress delivery failed");
        }
        Ok(true)
    }
}

#[tokio::test]
async fn listen_does_not_duplicate_inbound_after_ordered_sink_failure() {
    let server = mock_api().await;
    mount_get_updates_once(
        &server,
        1,
        updates(serde_json::json!([{
            "update_id": 21,
            "message": {
                "message_id": 7,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 8, "username": "alice"},
                "text": "one delivery attempt"
            }
        }])),
    )
    .await;
    let events = Arc::new(RejectingInboundEvents::default());
    let channel = Arc::new(fast_backoff(
        channel_for(&server, &["alice"]).with_events(events.clone()),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn({
        let channel = channel.clone();
        async move { channel.listen(tx).await }
    });

    let delivered_on_legacy_path = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("listener must finish after the ingress callback fails");
    assert!(
        delivered_on_legacy_path.is_none(),
        "an ambiguous callback failure must not submit the same update through the legacy queue"
    );
    let result = tokio::time::timeout(GUARD, listener)
        .await
        .expect("listener must return after callback failure")
        .unwrap();
    assert!(result.is_err());
    assert_eq!(events.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn listen_backs_off_when_webhook_delete_fails_and_on_transport_error() {
    let server = mock_api().await;
    mount_get_updates_once(
        &server,
        1,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "webhook is active"
        })),
    )
    .await;
    mount_post(&server, "deleteWebhook", ResponseTemplate::new(500)).await;
    mount_get_updates_once(
        &server,
        2,
        updates(serde_json::json!([
            {
                "update_id": 1,
                "message": {
                    "message_id": 2, "chat": { "id": 5, "type": "group" },
                    "from": { "id": 3, "username": "alice" }, "text": "hey @my_bot ping"
                }
            }
        ])),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "username": "my_bot" })))
        .mount(&server)
        .await;

    let mut ch = fast_backoff(channel_for(&server, &["alice"]));
    ch.mention_only = true;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let ch = Arc::new(ch);
    let listener = tokio::spawn({
        let ch = ch.clone();
        async move { ch.listen(tx).await }
    });

    let msg = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("message dispatched")
        .expect("channel open");
    assert_eq!(
        msg.content, "hey ping",
        "mention stripped in mention-only group"
    );
    assert_eq!(json_bodies(&server, "deleteWebhook").await.len(), 1);
    listener.abort();

    // Transport failures back off and retry rather than ending the loop.
    let mut dead = fast_backoff(TelegramChannel::new("tok".into(), vec!["*".into()], true));
    dead.api_base = dead_base();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let outcome = tokio::time::timeout(Duration::from_millis(300), dead.listen(tx)).await;
    assert!(
        outcome.is_err(),
        "listen keeps retrying on transport errors"
    );
    assert!(dead.bot_username.lock().is_none());
}
