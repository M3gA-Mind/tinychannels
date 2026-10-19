use super::*;
use crate::providers::native::NativeProvider;
use serde_json::json;
use tinychannels_bus::channel::{ConversationKind, ConversationRef, SendErrorKind};

fn target(kind: ConversationKind, id: &str) -> Target {
    Target {
        channel: tinychannels_bus::ChannelRef {
            id: "dingtalk".into(),
            account_id: Some("account-1".into()),
        },
        conversation: ConversationRef {
            kind,
            id: id.into(),
            ..Default::default()
        },
        reply_to: None,
        thread: None,
    }
}

#[tokio::test]
async fn direct_send_uses_staff_id_and_provider_receipt() {
    use crate::providers::http_test_support::HttpsServer;

    let server = HttpsServer::start_with_bytes(
        |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/oauth2/accessToken") => {
                (200, json!({"accessToken":"token"}).to_string().into_bytes())
            }
            ("POST", "/v1.0/robot/oToMessages/batchSend") => {
                assert_eq!(request.body["robotCode"], "robot-from-callback");
                assert_eq!(request.body["userIds"], json!(["alice-staff-id"]));
                (
                    200,
                    json!({"processQueryKey":"provider-message-91"})
                        .to_string()
                        .into_bytes(),
                )
            }
            _ => panic!("unexpected DingTalk request: {request:?}"),
        },
        "application/json",
    )
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice-staff-id".into()],
        server.client.clone(),
    );
    channel.robot_contexts.write().await.insert(
        conversation_context_key(ConversationKind::Group, "alice-staff-id"),
        json!({"robotCode":"wrong-group-robot","senderId":"wrong-group-user"}),
    );
    channel.robot_contexts.write().await.insert(
        conversation_context_key(ConversationKind::Dm, "alice-staff-id"),
        json!({
            "robotCode":"robot-from-callback",
            "senderId":"$:union-user-id",
            "senderStaffId":"alice-staff-id"
        }),
    );
    let sent = channel
        .deliver(
            &target(ConversationKind::Dm, "alice-staff-id"),
            &Step::Text("hello".into()),
        )
        .await
        .unwrap();

    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].id, "provider-message-91");
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.target == "/v1.0/robot/oToMessages/batchSend")
            .count(),
        1
    );
}

#[tokio::test]
async fn typed_send_rejects_missing_admission_or_robot_context_before_http() {
    use crate::providers::http_test_support::HttpsServer;
    use tinychannels_bus::{ChannelOutboundMessage, ContentPart, OutboundContentPart};
    use tinychannels_runtime::channel::ChannelTypedSend;

    let server = HttpsServer::start(|request| {
        panic!("missing admitted context must fail before HTTP: {request:?}")
    })
    .await;
    for (kind, id, context) in [
        (ConversationKind::Group, "missing-context", None),
        (ConversationKind::Group, "missing-robot", Some(json!({}))),
    ] {
        let channel = DingTalkChannel::with_http_client(
            "app-key".into(),
            "app-secret".into(),
            vec!["alice".into()],
            server.client.clone(),
        );
        if let Some(context) = context {
            channel
                .robot_contexts
                .write()
                .await
                .insert(conversation_context_key(kind, id), context);
        }
        let message = ChannelOutboundMessage {
            channel_id: "dingtalk".into(),
            conversation_id: id.into(),
            channel_ref: None,
            conversation_ref: Some(ConversationRef {
                kind,
                id: id.into(),
                ..Default::default()
            }),
            reply_to_id: None,
            thread_id: None,
            turn_id: Some("turn-admission-check".into()),
            idempotency_key: format!("admission-{id}"),
            parts: vec![OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "hello".into(),
                    markdown_dialect: None,
                },
            }],
        };
        let error = channel
            .send_typed(&message)
            .await
            .expect_err("unadmitted conversation cannot receive a provider send");
        assert_eq!(error.kind, SendErrorKind::BadFormat, "{id}");
        assert!(!error.sent_before_error, "{id}");
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn group_file_upload_sends_the_uploaded_media_id_and_real_receipt() {
    use crate::providers::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::{StubMedia, stored_media};
    use tinychannels_bus::{
        ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    };
    use tinychannels_runtime::channel::ChannelTypedSend;

    const FILE_BYTES: &[u8] = b"%PDF-1.7\nactual fixture bytes\n";

    let server = HttpsServer::start_with_bytes(
        |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/oauth2/accessToken") => {
                (200, json!({"accessToken":"token"}).to_string().into_bytes())
            }
            ("POST", target) if target.starts_with("/media/upload?") => (
                200,
                json!({"media_id":"uploaded-file-12"})
                    .to_string()
                    .into_bytes(),
            ),
            ("POST", "/v1.0/robot/groupMessages/send") => {
                assert_eq!(request.body["robotCode"], "group-robot");
                assert_eq!(request.body["openConversationId"], "open-group-12");
                assert_eq!(request.body["msgKey"], "sampleFile");
                let params: serde_json::Value = serde_json::from_str(
                    request.body["msgParam"]
                        .as_str()
                        .expect("msgParam is JSON text"),
                )
                .unwrap();
                assert_eq!(params["mediaId"], "uploaded-file-12");
                assert_eq!(params["fileName"], "report.pdf");
                assert_eq!(params["fileType"], "pdf");
                (
                    200,
                    json!({"processQueryKey":"group-receipt-12"})
                        .to_string()
                        .into_bytes(),
                )
            }
            _ => panic!("unexpected DingTalk request: {request:?}"),
        },
        "application/json",
    )
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Document,
        "application/pdf",
        FILE_BYTES,
    ));
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_media_repository(media.clone());
    channel.robot_contexts.write().await.insert(
        conversation_context_key(ConversationKind::Group, "open-group-12"),
        json!({"robotCode":"group-robot"}),
    );

    let message = ChannelOutboundMessage {
        channel_id: "dingtalk".into(),
        conversation_id: "open-group-12".into(),
        channel_ref: None,
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Group,
            id: "open-group-12".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-file-upload".into()),
        idempotency_key: "file-upload".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::File {
                media: MediaReference {
                    opaque_id: Some("stored-file-12".into()),
                    kind: MediaKind::Document,
                    content_type: Some("application/pdf".into()),
                    ..Default::default()
                },
                file_name: Some("report.pdf".into()),
                caption: None,
            },
        }],
    };

    let sent = channel.send_typed(&message).await;
    assert!(
        sent.is_ok(),
        "typed send failed: {:?}; observed requests: {:#?}",
        sent.as_ref().err(),
        server.requests.lock().unwrap()
    );
    let sent = sent.unwrap();
    assert_eq!(
        sent.receipt.primary_platform_message_id.as_deref(),
        Some("group-receipt-12")
    );
    assert_eq!(sent.receipt.parts[0].kind, MessageReceiptPartKind::Media);
    assert_eq!(media.scopes.lock().unwrap()[0].turn_id, "turn-file-upload");
    let requests = server.requests.lock().unwrap();
    let upload = requests
        .iter()
        .find(|request| request.target.starts_with("/media/upload?"))
        .expect("file upload request must be observed");
    assert!(
        upload
            .headers
            .to_ascii_lowercase()
            .contains("multipart/form-data")
    );
    assert!(
        upload
            .raw_body
            .windows(FILE_BYTES.len())
            .any(|window| window == FILE_BYTES)
    );
    assert!(String::from_utf8_lossy(&upload.raw_body).contains("report.pdf"));
    assert_eq!(requests.len(), 3);
}

struct ReadableAmrVoice(Vec<u8>);

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for ReadableAmrVoice {
    async fn store_voice(
        &self,
        _request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        anyhow::bail!("this fixture only serves outbound voice")
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        use base64::Engine as _;

        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(&self.0),
            mime_type: "audio/amr".into(),
            file_name: Some("reply.amr".into()),
        })
    }
}

fn voice_message(conversation_id: &str) -> tinychannels_bus::ChannelOutboundMessage {
    use tinychannels_bus::{
        ChannelOutboundMessage, MediaKind, MediaReference, OutboundContentPart, OutboundVoice,
    };

    ChannelOutboundMessage {
        channel_id: "dingtalk".into(),
        conversation_id: conversation_id.into(),
        channel_ref: None,
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Group,
            id: conversation_id.into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-amr-voice".into()),
        idempotency_key: "amr-voice-send".into(),
        parts: vec![OutboundContentPart::Voice {
            voice: OutboundVoice::Media {
                media: MediaReference {
                    opaque_id: Some("opaque-amr-voice".into()),
                    kind: MediaKind::Audio,
                    content_type: Some("audio/amr".into()),
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("amr_nb".into()),
                // Verify this stored metadata is forwarded. Codec-derived
                // duration is tested at the TinyVoice module boundary.
                duration_ms: Some(1_240),
                waveform: None,
            },
        }],
    }
}

#[tokio::test]
async fn typed_voice_send_forwards_amr_bytes_and_declared_duration() {
    use crate::providers::http_test_support::HttpsServer;
    use tinychannels_bus::channel::MessageReceiptPartKind;
    use tinychannels_runtime::channel::ChannelTypedSend;

    const AMR_BYTES: &[u8] = include_bytes!("../../../tests/fixtures/dingtalk/amrnb_mode0.amr");

    let server = HttpsServer::start_with_bytes(
        |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/oauth2/accessToken") => {
                (200, json!({"accessToken":"token"}).to_string().into_bytes())
            }
            ("POST", target) if target.starts_with("/media/upload?") => {
                assert!(
                    target.contains("type=voice"),
                    "voice must use the voice upload type"
                );
                assert!(
                    request
                        .headers
                        .to_ascii_lowercase()
                        .contains("multipart/form-data")
                );
                assert!(
                    request
                        .raw_body
                        .windows(AMR_BYTES.len())
                        .any(|w| w == AMR_BYTES)
                );
                assert!(String::from_utf8_lossy(&request.raw_body).contains("reply.amr"));
                (
                    200,
                    json!({"media_id":"uploaded-amr-voice"})
                        .to_string()
                        .into_bytes(),
                )
            }
            ("POST", "/v1.0/robot/groupMessages/send") => {
                assert_eq!(request.body["msgKey"], "sampleAudio");
                let params: serde_json::Value = serde_json::from_str(
                    request.body["msgParam"]
                        .as_str()
                        .expect("msgParam is JSON text"),
                )
                .unwrap();
                assert_eq!(params["mediaId"], "uploaded-amr-voice");
                assert_eq!(params["duration"], "1240");
                (
                    200,
                    json!({"processQueryKey":"amr-voice-receipt"})
                        .to_string()
                        .into_bytes(),
                )
            }
            _ => panic!("unexpected DingTalk request: {request:?}"),
        },
        "application/json",
    )
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_voice_media_store(Arc::new(ReadableAmrVoice(AMR_BYTES.to_vec())));
    channel.robot_contexts.write().await.insert(
        conversation_context_key(ConversationKind::Group, "open-group-amr"),
        json!({"robotCode":"group-robot"}),
    );

    let sent = channel
        .send_typed(&voice_message("open-group-amr"))
        .await
        .expect("AMR voice should be uploaded and sent as sampleAudio");

    assert_eq!(
        sent.receipt.primary_platform_message_id.as_deref(),
        Some("amr-voice-receipt")
    );
    assert_eq!(sent.receipt.parts[0].kind, MessageReceiptPartKind::Voice);
    assert_eq!(server.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn typed_voice_send_preserves_transcript_as_an_ordered_text_message() {
    use crate::providers::http_test_support::HttpsServer;
    use tinychannels_bus::channel::MessageReceiptPartKind;
    use tinychannels_bus::{ContentPart, OutboundContentPart};
    use tinychannels_runtime::channel::ChannelTypedSend;

    const AMR_BYTES: &[u8] = include_bytes!("../../../tests/fixtures/dingtalk/amrnb_mode0.amr");
    let server = HttpsServer::start_with_bytes(
        |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/oauth2/accessToken") => {
                (200, json!({"accessToken":"token"}).to_string().into_bytes())
            }
            ("POST", target) if target.starts_with("/media/upload?") => {
                assert!(target.contains("type=voice"));
                assert!(
                    request
                        .raw_body
                        .windows(AMR_BYTES.len())
                        .any(|w| w == AMR_BYTES)
                );
                (
                    200,
                    json!({"media_id":"voice-media-id"})
                        .to_string()
                        .into_bytes(),
                )
            }
            ("POST", "/v1.0/robot/groupMessages/send") => {
                let params: serde_json::Value = serde_json::from_str(
                    request.body["msgParam"]
                        .as_str()
                        .expect("msgParam is JSON text"),
                )
                .unwrap();
                match request.body["msgKey"].as_str() {
                    Some("sampleText") => {
                        assert_eq!(params["content"], "voice transcript");
                        (
                            200,
                            json!({"processQueryKey":"text-receipt"})
                                .to_string()
                                .into_bytes(),
                        )
                    }
                    Some("sampleAudio") => {
                        assert_eq!(params["mediaId"], "voice-media-id");
                        assert_eq!(params["duration"], "1240");
                        (
                            200,
                            json!({"processQueryKey":"voice-receipt"})
                                .to_string()
                                .into_bytes(),
                        )
                    }
                    other => panic!("unexpected DingTalk message template: {other:?}"),
                }
            }
            _ => panic!("unexpected DingTalk request: {request:?}"),
        },
        "application/json",
    )
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_voice_media_store(Arc::new(ReadableAmrVoice(AMR_BYTES.to_vec())));
    channel.robot_contexts.write().await.insert(
        conversation_context_key(ConversationKind::Group, "open-group-amr"),
        json!({"robotCode":"group-robot"}),
    );
    let mut message = voice_message("open-group-amr");
    message.parts.insert(
        0,
        OutboundContentPart::Content {
            part: ContentPart::Text {
                text: "voice transcript".into(),
                markdown_dialect: None,
            },
        },
    );

    let sent = channel
        .send_typed(&message)
        .await
        .expect("DingTalk should preserve transcript and voice as separate messages");

    assert_eq!(sent.receipt.parts.len(), 2);
    assert_eq!(sent.receipt.parts[0].kind, MessageReceiptPartKind::Text);
    assert_eq!(sent.receipt.parts[0].platform_message_id, "text-receipt");
    assert_eq!(sent.receipt.parts[1].kind, MessageReceiptPartKind::Voice);
    assert_eq!(sent.receipt.parts[1].platform_message_id, "voice-receipt");

    let mut reverse_order = voice_message("open-group-amr");
    reverse_order.parts.push(OutboundContentPart::Content {
        part: ContentPart::Text {
            text: "voice transcript".into(),
            markdown_dialect: None,
        },
    });
    let sent = channel
        .send_typed(&reverse_order)
        .await
        .expect("DingTalk should preserve transcript following the voice part");
    assert_eq!(sent.receipt.parts.len(), 2);
    assert_eq!(sent.receipt.parts[0].kind, MessageReceiptPartKind::Voice);
    assert_eq!(sent.receipt.parts[0].platform_message_id, "voice-receipt");
    assert_eq!(sent.receipt.parts[1].kind, MessageReceiptPartKind::Text);
    assert_eq!(sent.receipt.parts[1].platform_message_id, "text-receipt");

    let send_order = server
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.target == "/v1.0/robot/groupMessages/send")
        .map(|request| request.body["msgKey"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        send_order,
        ["sampleText", "sampleAudio", "sampleAudio", "sampleText"]
    );
}

struct LoadedVoiceReplySynthesizer(Vec<u8>);

#[async_trait::async_trait]
impl crate::host::SpeechSynthesizer for LoadedVoiceReplySynthesizer {
    async fn synthesize(
        &self,
        _request: crate::host::SpeechRequest,
    ) -> anyhow::Result<crate::host::SpeechResult> {
        use base64::Engine as _;

        Ok(crate::host::SpeechResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(&self.0),
            mime_type: "audio/wav".into(),
            duration_ms: Some(8_000),
            ..Default::default()
        })
    }
}

struct LoadedTinyVoiceCodec(tinybus::Connection);

#[async_trait::async_trait]
impl crate::host::VoiceCodec for LoadedTinyVoiceCodec {
    async fn transcode(
        &self,
        request: crate::host::VoiceCodecRequest,
    ) -> anyhow::Result<crate::host::VoiceCodecResult> {
        let proxy = self
            .0
            .proxy(
                tinyvoice_bus::BUS_NAME,
                tinyvoice_bus::OBJECT_PATH,
                tinyvoice_bus::BUS_NAME,
            )
            .unwrap();
        let output: tinyvoice_bus::codec::CodecOutput = proxy
            .call(
                tinyvoice_bus::names::methods::CODEC_TRANSCODE,
                (tinyvoice_bus::codec::CodecTranscodeRequest {
                    audio_base64: request.audio_base64,
                    from: match request.from.as_str() {
                        "wav_pcm16" => tinyvoice_bus::codec::AudioCodec::WavPcm16,
                        _ => anyhow::bail!("unexpected source codec"),
                    },
                    target: match request.target.as_str() {
                        "amr_nb" => tinyvoice_bus::codec::AudioCodec::AmrNb,
                        _ => anyhow::bail!("unexpected target codec"),
                    },
                },),
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let info: tinyvoice_bus::codec::CodecInfo = proxy
            .call(
                tinyvoice_bus::names::methods::CODEC_PROBE,
                (tinyvoice_bus::codec::CodecInput {
                    audio_base64: output.audio_base64.clone(),
                },),
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        anyhow::ensure!(info.codec == tinyvoice_bus::codec::AudioCodec::AmrNb);
        Ok(crate::host::VoiceCodecResult {
            audio_base64: output.audio_base64,
            duration_ms: Some(info.duration_ms),
            waveform: None,
        })
    }
}

struct ScopedVoiceFixture(Arc<crate::host::ScopedMediaStore>);

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for ScopedVoiceFixture {
    async fn store_voice(
        &self,
        request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        use base64::Engine as _;
        use tinychannels_bus::{HostVoiceMediaDirection, MediaKind};

        anyhow::ensure!(request.direction == HostVoiceMediaDirection::Outbound);
        let bytes =
            base64::engine::general_purpose::STANDARD.decode(request.audio_base64.as_bytes())?;
        let media = self
            .0
            .store(crate::host::MediaStoreRequest {
                metadata: crate::host::MediaMetadata {
                    scope: request.scope,
                    kind: MediaKind::Audio,
                    content_type: request.mime_type,
                    file_name: request.file_name,
                    duration_ms: request.duration_ms,
                    codec: request.codec,
                    voice_direction: Some(request.direction),
                    dedupe_key: None,
                    max_bytes: 2 * 1024 * 1024,
                },
                bytes,
            })
            .await?;
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media,
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        use base64::Engine as _;

        let stored = self.0.read(scope, &opaque_id, 2 * 1024 * 1024).await?;
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(stored.bytes),
            mime_type: stored.content_type,
            file_name: stored.file_name,
        })
    }
}

#[tokio::test]
#[ignore = "requires a built TinyVoice module; set TINYVOICE_MODULE_ARTIFACT and TINYVOICE_MODULE_SHA256"]
async fn loaded_tinyvoice_scoped_reply_reaches_dingtalk_voice_upload_and_receipt() {
    use crate::host::{VoiceReplyConstraints, prepare_voice_reply};
    use crate::providers::http_test_support::HttpsServer;
    use base64::Engine as _;
    use tinybus::{broker::Broker, module::ModuleHost, transport::memory::MemoryBus};
    use tinychannels_bus::channel::{ConversationKind, ConversationRef, MessageReceiptPartKind};
    use tinychannels_bus::{
        ChannelOutboundMessage, ChannelRef, ContentPart, MediaKind, OutboundContentPart,
        OutboundVoice,
    };
    use tinychannels_runtime::channel::ChannelTypedSend;
    use tinystoragedrivers_core::{MemoryStorage, Scope};

    let artifact = std::env::var("TINYVOICE_MODULE_ARTIFACT").expect("TinyVoice module path");
    let artifact_bytes = std::fs::read(&artifact).expect("read TinyVoice module artifact");
    let expected_sha = std::env::var("TINYVOICE_MODULE_SHA256").expect("TinyVoice artifact SHA");
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&artifact_bytes)),
        expected_sha
    );

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let module_host = ModuleHost::new(broker);
    assert_eq!(
        module_host.load_file(&artifact).unwrap().name,
        "tinyvoice-module"
    );
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    let backend = MemoryStorage::new()
        .for_scope(&Scope::new("module:dingtalk-voice-integration").unwrap())
        .unwrap();
    let scoped_media = Arc::new(crate::host::ScopedMediaStore::new(
        Arc::clone(backend.documents()),
        Arc::clone(backend.blobs()),
    ));
    let voice_store = Arc::new(ScopedVoiceFixture(scoped_media.clone()));
    let scope = tinychannels_bus::HostVoiceScope {
        configured_instance: "dingtalk:robot-1".into(),
        channel: ChannelRef {
            id: "dingtalk".into(),
            account_id: Some("dingtalk:robot-1".into()),
        },
        conversation: ConversationRef {
            kind: ConversationKind::Group,
            id: "open-group-loaded-amr".into(),
            ..Default::default()
        },
        turn_id: "loaded-amr-turn".into(),
    };
    let accepted_codecs = vec!["amr_nb".to_owned()];
    let prepared = prepare_voice_reply(
        scope.clone(),
        "A measured response".into(),
        None,
        &LoadedVoiceReplySynthesizer(dingtalk_synthesis_wav()),
        &LoadedTinyVoiceCodec(connection.clone()),
        voice_store.as_ref(),
        VoiceReplyConstraints {
            accepted_codecs: &accepted_codecs,
            max_size_bytes: Some(2 * 1024 * 1024),
            max_duration_ms: Some(10_000),
        },
    )
    .await
    .expect("shared voice preparation should encode and persist through TinyVoice");
    assert_eq!(prepared.note.codec.as_deref(), Some("amr_nb"));
    assert_eq!(
        prepared.note.media.content_type.as_deref(),
        Some("audio/amr")
    );
    assert_eq!(prepared.note.media.file_name.as_deref(), Some("voice.amr"));
    let media_id = prepared
        .note
        .media
        .opaque_id
        .clone()
        .expect("scoped opaque voice id");
    let stored = scoped_media
        .read(scope.clone(), &media_id, 2 * 1024 * 1024)
        .await
        .expect("the scoped store resolves the exact outgoing voice");
    assert!(stored.bytes.starts_with(b"#!AMR\n"));
    assert_eq!(stored.file_name.as_deref(), Some("voice.amr"));
    let codec_proxy = connection
        .proxy(
            tinyvoice_bus::BUS_NAME,
            tinyvoice_bus::OBJECT_PATH,
            tinyvoice_bus::BUS_NAME,
        )
        .unwrap();
    let measured: tinyvoice_bus::codec::CodecInfo = codec_proxy
        .call(
            tinyvoice_bus::names::methods::CODEC_PROBE,
            (tinyvoice_bus::codec::CodecInput {
                audio_base64: base64::engine::general_purpose::STANDARD.encode(&stored.bytes),
            },),
        )
        .await
        .unwrap();
    assert_eq!(measured.codec, tinyvoice_bus::codec::AudioCodec::AmrNb);
    assert_eq!(prepared.note.duration_ms, Some(measured.duration_ms));
    let expected_bytes = Arc::new(stored.bytes);
    let expected_duration = measured.duration_ms.to_string();
    let sent_parts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sent_parts_capture = sent_parts.clone();
    let bytes_capture = expected_bytes.clone();
    let duration_capture = expected_duration.clone();
    let server = HttpsServer::start_with_bytes(
        move |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/oauth2/accessToken") => {
                (200, json!({"accessToken":"token"}).to_string().into_bytes())
            }
            ("POST", target) if target.starts_with("/media/upload?") => {
                assert!(target.contains("type=voice"));
                assert!(
                    request
                        .raw_body
                        .windows(bytes_capture.len())
                        .any(|bytes| bytes == bytes_capture.as_slice())
                );
                assert!(String::from_utf8_lossy(&request.raw_body).contains("voice.amr"));
                (
                    200,
                    json!({"media_id":"loaded-amr-media"})
                        .to_string()
                        .into_bytes(),
                )
            }
            ("POST", "/v1.0/robot/groupMessages/send") => {
                let key = request.body["msgKey"].as_str().unwrap().to_owned();
                let params: serde_json::Value =
                    serde_json::from_str(request.body["msgParam"].as_str().unwrap()).unwrap();
                if key == "sampleAudio" {
                    assert_eq!(params["mediaId"], "loaded-amr-media");
                    assert_eq!(params["duration"], duration_capture.as_str());
                } else {
                    assert_eq!(key, "sampleText");
                    assert_eq!(params["content"], "A measured response");
                }
                sent_parts_capture.lock().unwrap().push(key.clone());
                (
                    200,
                    json!({"processQueryKey":format!("receipt-{key}")})
                        .to_string()
                        .into_bytes(),
                )
            }
            _ => panic!("unexpected DingTalk request: {request:?}"),
        },
        "application/json",
    )
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec![],
        server.client.clone(),
    )
    .with_voice_media_store(voice_store);
    channel.robot_contexts.write().await.insert(
        conversation_context_key(ConversationKind::Group, &scope.conversation.id),
        json!({"robotCode":"group-robot"}),
    );
    let message = ChannelOutboundMessage {
        channel_id: "dingtalk".into(),
        conversation_id: scope.conversation.id.clone(),
        channel_ref: Some(scope.channel.clone()),
        conversation_ref: Some(scope.conversation.clone()),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some(scope.turn_id.clone()),
        idempotency_key: "loaded-amr-send".into(),
        parts: vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "A measured response".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Voice {
                voice: OutboundVoice::Media {
                    media: prepared.note.media,
                    audio_base64: None,
                    codec: prepared.note.codec,
                    duration_ms: prepared.note.duration_ms,
                    waveform: None,
                },
            },
        ],
    };
    let sent = channel
        .send_typed(&message)
        .await
        .expect("DingTalk native voice upload/send");
    assert_eq!(*sent_parts.lock().unwrap(), ["sampleText", "sampleAudio"]);
    assert_eq!(sent.receipt.parts.len(), 2);
    assert_eq!(sent.receipt.parts[0].kind, MessageReceiptPartKind::Text);
    assert_eq!(sent.receipt.parts[1].kind, MessageReceiptPartKind::Voice);
    assert_eq!(
        sent.receipt.parts[1].platform_message_id.as_deref(),
        Some("receipt-sampleAudio")
    );
    let requests = server.requests.lock().unwrap();
    let upload = requests
        .iter()
        .find(|request| request.target.starts_with("/media/upload?"))
        .unwrap();
    assert!(
        upload
            .raw_body
            .windows(expected_bytes.len())
            .any(|bytes| bytes == expected_bytes.as_slice())
    );
    assert_eq!(expected_duration, measured.duration_ms.to_string());
    assert!(requests.len() >= 5);
}

fn dingtalk_synthesis_wav() -> Vec<u8> {
    let sample_rate = 16_000u32;
    let count = sample_rate as usize * 8;
    let data_len = count * 2;
    let mut wav = Vec::with_capacity(44 + data_len);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36u32 + data_len as u32).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&(sample_rate * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(data_len as u32).to_le_bytes());
    for sample in 0..count {
        let value = ((sample as f32 * 440.0 * std::f32::consts::TAU / sample_rate as f32).sin()
            * 12_000.0) as i16;
        wav.extend_from_slice(&value.to_le_bytes());
    }
    wav
}

#[tokio::test]
async fn typed_voice_over_two_mib_is_rejected_before_platform_requests() {
    use crate::providers::http_test_support::HttpsServer;
    use tinychannels_runtime::channel::ChannelTypedSend;

    let server = HttpsServer::start(|request| {
        panic!("oversized DingTalk voice must be rejected before HTTP: {request:?}")
    })
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_voice_media_store(Arc::new(ReadableAmrVoice(vec![0; 2 * 1024 * 1024 + 1])));

    let error = channel
        .send_typed(&voice_message("open-group-amr"))
        .await
        .expect_err("DingTalk voice upload is limited to 2 MiB");
    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(
        !error.unsupported,
        "AMR is supported; rejection must come from the provider size bound: {error:?}"
    );
    assert!(!error.sent_before_error);
    assert!(server.requests.lock().unwrap().is_empty());
}

#[test]
fn typed_voice_capabilities_advertise_amr_when_host_ports_are_available() {
    use crate::providers::native::fixtures::RecordingEvents;

    let channel = DingTalkChannel::new("app-key".into(), "app-secret".into(), vec![])
        .with_voice_media_store(Arc::new(ReadableAmrVoice(Vec::new())))
        .with_events(Arc::new(RecordingEvents::delivering()));
    let caps = NativeProvider::declared_capabilities(&channel);

    assert!(caps.voice_in.supported);
    assert!(
        caps.voice_in
            .accepted_codecs
            .iter()
            .any(|codec| codec == "amr_nb")
    );
    assert!(
        caps.content
            .audio
            .accepted_types
            .iter()
            .any(|mime| mime == "audio/amr")
    );
    assert!(
        caps.content
            .audio
            .accepted_types
            .contains(&"audio/ogg".into())
    );
    assert!(
        caps.content
            .audio
            .accepted_types
            .contains(&"audio/mpeg".into())
    );
    assert!(
        caps.content
            .audio
            .accepted_types
            .contains(&"audio/mp4".into())
    );
    assert!(caps.voice_out.supported);
    assert_eq!(caps.voice_out.accepted_codecs, ["amr_nb"]);
    assert_eq!(caps.voice_out.max_size_bytes, Some(2 * 1024 * 1024));
    assert!(caps.voice_in.accepted_codecs.contains(&"ogg_opus".into()));
    assert_eq!(caps.voice_in.max_size_bytes, Some(10 * 1024 * 1024));
}

#[tokio::test]
async fn direct_send_rejection_lists_override_a_success_shaped_receipt() {
    use crate::providers::http_test_support::HttpsServer;

    for (field, error_kind, retryable) in [
        ("flowControlledStaffIdList", SendErrorKind::Transient, true),
        ("invalidStaffIdList", SendErrorKind::BadFormat, false),
        ("filteredStaffIdList", SendErrorKind::BadFormat, false),
    ] {
        let server = HttpsServer::start_with_bytes(
            move |request| match (request.method.as_str(), request.target.as_str()) {
                ("POST", "/v1.0/oauth2/accessToken") => {
                    (200, json!({"accessToken":"token"}).to_string().into_bytes())
                }
                ("POST", "/v1.0/robot/oToMessages/batchSend") => {
                    assert_eq!(request.body["userIds"], json!(["alice-staff-id"]));
                    let mut response = json!({"processQueryKey":"provider-query-1"});
                    response[field] = json!(["alice-staff-id"]);
                    (200, response.to_string().into_bytes())
                }
                _ => panic!("unexpected DingTalk request: {request:?}"),
            },
            "application/json",
        )
        .await;
        let channel = DingTalkChannel::with_http_client(
            "app-key".into(),
            "app-secret".into(),
            vec!["alice-staff-id".into()],
            server.client.clone(),
        );
        channel.robot_contexts.write().await.insert(
            conversation_context_key(ConversationKind::Dm, "alice-staff-id"),
            json!({
                "robotCode":"robot-from-callback",
                "senderId":"$:union-user-id",
                "senderStaffId":"alice-staff-id"
            }),
        );
        let error = channel
            .deliver(
                &target(ConversationKind::Dm, "alice-staff-id"),
                &Step::Text("hello".into()),
            )
            .await
            .err()
            .expect("a provider rejection must not return a receipt");
        assert_eq!(error.kind, error_kind, "{field}");
        assert_eq!(error.retryable, retryable, "{field}");
        assert!(!error.sent_before_error, "{field}");
    }
}

#[tokio::test]
async fn final_send_server_error_is_reported_as_ambiguous() {
    use crate::providers::http_test_support::HttpsServer;

    let server = HttpsServer::start_with_bytes(
        |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/oauth2/accessToken") => {
                (200, json!({"accessToken":"token"}).to_string().into_bytes())
            }
            ("POST", "/v1.0/robot/groupMessages/send") => (500, b"retry later".to_vec()),
            _ => panic!("unexpected DingTalk request: {request:?}"),
        },
        "application/json",
    )
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    );
    channel.robot_contexts.write().await.insert(
        conversation_context_key(ConversationKind::Group, "conversation-1"),
        json!({"robotCode":"robot-from-callback"}),
    );
    let error = channel
        .deliver(
            &target(ConversationKind::Group, "conversation-1"),
            &Step::Text("hello".into()),
        )
        .await
        .err()
        .expect("a provider rejection must not return a receipt");

    assert_eq!(error.kind, SendErrorKind::Unknown);
    assert!(error.sent_before_error);
    assert!(!error.retryable);
}

#[tokio::test]
async fn token_and_upload_server_errors_are_retryable_before_message_delivery() {
    use crate::providers::http_test_support::HttpsServer;

    for failing_path in ["token", "upload"] {
        let server = HttpsServer::start_with_bytes(
            move |request| match (request.method.as_str(), request.target.as_str()) {
                ("POST", "/v1.0/oauth2/accessToken") if failing_path == "token" => {
                    (503, b"temporarily unavailable".to_vec())
                }
                ("POST", "/v1.0/oauth2/accessToken") => {
                    (200, json!({"accessToken":"token"}).to_string().into_bytes())
                }
                ("POST", target) if target.starts_with("/media/upload?") => {
                    assert_eq!(failing_path, "upload");
                    (503, b"temporarily unavailable".to_vec())
                }
                _ => panic!("message send must not follow an earlier failure: {request:?}"),
            },
            "application/json",
        )
        .await;
        let channel = DingTalkChannel::with_http_client(
            "app-key".into(),
            "app-secret".into(),
            vec!["alice".into()],
            server.client.clone(),
        );
        channel.robot_contexts.write().await.insert(
            conversation_context_key(ConversationKind::Group, "conversation-1"),
            json!({"robotCode":"robot-from-callback"}),
        );
        let step = if failing_path == "token" {
            Step::Text("hello".into())
        } else {
            Step::File(FileStep {
                bytes: b"pdf-bytes".to_vec(),
                mime: "application/pdf".into(),
                name: "report.pdf".into(),
                caption: None,
                duration_ms: None,
                #[cfg(feature = "lark")]
                kind: tinychannels_bus::MediaKind::Document,
            })
        };
        let error = channel
            .deliver(&target(ConversationKind::Group, "conversation-1"), &step)
            .await
            .err()
            .expect("a pre-send failure must not return a receipt");
        assert_eq!(error.kind, SendErrorKind::Transient, "{failing_path}");
        assert!(error.retryable, "{failing_path}");
        assert!(!error.sent_before_error, "{failing_path}");
        assert!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| { !request.target.starts_with("/v1.0/robot/groupMessages/send") })
        );
        let requests = server.requests.lock().unwrap();
        if failing_path == "token" {
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].target, "/v1.0/oauth2/accessToken");
        } else {
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].target, "/v1.0/oauth2/accessToken");
            assert!(requests[1].target.starts_with("/media/upload?"));
        }
    }
}

#[tokio::test]
async fn invalid_upload_receipts_never_dispatch_a_group_message() {
    use crate::providers::http_test_support::HttpsServer;

    for upload_body in [b"not-json".as_slice(), br#"{"media_id":""}"#] {
        let upload_body = upload_body.to_vec();
        let server = HttpsServer::start_with_bytes(
            move |request| match (request.method.as_str(), request.target.as_str()) {
                ("POST", "/v1.0/oauth2/accessToken") => {
                    (200, json!({"accessToken":"token"}).to_string().into_bytes())
                }
                ("POST", target) if target.starts_with("/media/upload?") => {
                    (200, upload_body.clone())
                }
                _ => panic!("invalid upload receipt must stop before message send: {request:?}"),
            },
            "application/json",
        )
        .await;
        let channel = DingTalkChannel::with_http_client(
            "app-key".into(),
            "app-secret".into(),
            vec!["alice".into()],
            server.client.clone(),
        );
        channel.robot_contexts.write().await.insert(
            conversation_context_key(ConversationKind::Group, "conversation-1"),
            json!({"robotCode":"robot-from-callback"}),
        );
        let error = channel
            .deliver(
                &target(ConversationKind::Group, "conversation-1"),
                &Step::File(FileStep {
                    bytes: b"%PDF fixture".to_vec(),
                    mime: "application/pdf".into(),
                    name: "report.pdf".into(),
                    caption: None,
                    duration_ms: None,
                    #[cfg(feature = "lark")]
                    kind: tinychannels_bus::MediaKind::Document,
                }),
            )
            .await
            .err()
            .expect("an upload without a provider media ID cannot be sent");
        assert_eq!(
            error.kind,
            SendErrorKind::BadFormat,
            "{}; observed requests: {:#?}",
            error.message,
            server.requests.lock().unwrap()
        );
        assert!(!error.sent_before_error);
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn file_caption_is_rejected_before_any_send_step() {
    let channel = DingTalkChannel::new("app-key".into(), "app-secret".into(), vec![]);
    let mut steps = [
        Step::Text("preceding text".into()),
        Step::File(FileStep {
            bytes: b"file".to_vec(),
            mime: "application/pdf".into(),
            name: "report.pdf".into(),
            caption: Some("caption that App Bot cannot preserve".into()),
            duration_ms: None,
            #[cfg(feature = "lark")]
            kind: tinychannels_bus::MediaKind::Document,
        }),
    ];

    let error = channel
        .prepare_steps(
            &target(ConversationKind::Group, "conversation-1"),
            &mut steps,
        )
        .await
        .unwrap_err();
    assert!(error.unsupported);
    assert!(!error.sent_before_error);
}

#[tokio::test]
async fn typed_mixed_text_and_captioned_file_fails_before_platform_requests() {
    use crate::providers::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::{StubMedia, stored_media};
    use tinychannels_bus::{
        ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    };
    use tinychannels_runtime::channel::ChannelTypedSend;

    let server = HttpsServer::start(|request| {
        panic!("caption preflight must not make platform requests: {request:?}")
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Document,
        "application/pdf",
        b"pdf-bytes",
    ));
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec![],
        server.client.clone(),
    )
    .with_media_repository(media);
    let message = ChannelOutboundMessage {
        channel_id: "dingtalk".into(),
        conversation_id: "open-conversation-1".into(),
        channel_ref: None,
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Group,
            id: "open-conversation-1".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-caption-check".into()),
        idempotency_key: "caption-check".into(),
        parts: vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "must not send before validation".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::File {
                    media: MediaReference {
                        opaque_id: Some("file-1".into()),
                        kind: MediaKind::Document,
                        content_type: Some("application/pdf".into()),
                        ..Default::default()
                    },
                    file_name: Some("report.pdf".into()),
                    caption: Some("DingTalk cannot preserve this caption".into()),
                },
            },
        ],
    };

    let error = channel.send_typed(&message).await.unwrap_err();
    assert!(error.unsupported);
    assert!(!error.sent_before_error);
    assert!(server.requests.lock().unwrap().is_empty());
}
