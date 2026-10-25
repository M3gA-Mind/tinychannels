use super::*;
use crate::channel::{ChannelAdapter, ChannelInboundSink};
use crate::providers::native::NativeProvider;
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::ChannelIngress;
use tinychannels_bus::channel::{ConversationKind, ConversationRef, SendErrorKind};

const PUBLIC_LISTENER_GUARD: std::time::Duration = std::time::Duration::from_secs(8);

#[derive(Clone)]
struct RecordingInboundSink(tokio::sync::mpsc::UnboundedSender<ChannelIngress>);

#[async_trait::async_trait]
impl ChannelInboundSink for RecordingInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.0
            .send(ingress)
            .map_err(|_| tinychannels_bus::channel::ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "test sink closed".into(),
                retryable: true,
                ..Default::default()
            })
    }
}

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

#[test]
fn unsupported_attachment_types_are_reported_as_unsupported() {
    let error = validate_file(&FileStep {
        #[cfg(feature = "lark")]
        kind: tinychannels_bus::MediaKind::Image,
        mime: "image/png".into(),
        name: "diagram.png".into(),
        bytes: b"image".to_vec(),
        caption: None,
        duration_ms: None,
    })
    .expect_err("DingTalk App Bot does not send images through its file operation");

    assert!(error.unsupported);
    assert!(!error.retryable);
    assert!(!error.sent_before_error);
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
async fn v1_subject_is_a_markdown_title_not_part_of_dingtalk_body() {
    use crate::providers::http_test_support::HttpsServer;
    use crate::traits::SendMessage;

    let server = HttpsServer::start(|request| {
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/session-webhook");
        (200, r#"{"errcode":0}"#.into())
    })
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    );
    channel.session_webhooks.write().await.insert(
        "chat-1".into(),
        "https://api.dingtalk.com/session-webhook".into(),
    );

    crate::provider_test_support::send_v1(
        &channel,
        &SendMessage::with_subject("deployment complete", "chat-1", "Release status"),
    )
    .await
    .expect("the configured DingTalk webhook accepts the typed projection");

    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].body,
        json!({
            "msgtype": "markdown",
            "markdown": { "title": "Release status", "text": "deployment complete" }
        })
    );
}

#[tokio::test]
async fn session_webhook_errors_are_sanitized_and_keep_delivery_uncertainty() {
    use crate::providers::http_test_support::HttpsServer;

    for (status, expected_kind, expected_sent) in [
        (403, SendErrorKind::BadFormat, false),
        (503, SendErrorKind::Unknown, true),
    ] {
        let server = HttpsServer::start(move |request| {
            assert_eq!(request.target, "/session-webhook");
            (
                status,
                "sensitive provider response with secret-token".into(),
            )
        })
        .await;
        let channel = DingTalkChannel::with_http_client(
            "app-key".into(),
            "app-secret".into(),
            vec!["alice".into()],
            server.client.clone(),
        );
        channel.session_webhooks.write().await.insert(
            "chat-1".into(),
            "https://api.dingtalk.com/session-webhook".into(),
        );

        let error = channel
            .deliver(
                &target(ConversationKind::Group, "chat-1"),
                &Step::Text("reply".into()),
            )
            .await
            .err()
            .expect("non-success webhook status must not produce success");

        assert_eq!(error.kind, expected_kind);
        assert_eq!(error.sent_before_error, expected_sent);
        assert!(!error.retryable);
        assert!(!error.message.contains("sensitive"));
        assert!(!error.message.contains("secret-token"));
    }
}

#[tokio::test]
async fn session_webhook_rejects_nonzero_application_error_codes() {
    use crate::providers::http_test_support::HttpsServer;

    let server = HttpsServer::start(|request| {
        assert_eq!(request.target, "/session-webhook");
        (
            200,
            r#"{"errcode":310000,"errmsg":"secret-token rejected"}"#.into(),
        )
    })
    .await;
    let channel = DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    );
    channel.session_webhooks.write().await.insert(
        "chat-1".into(),
        "https://api.dingtalk.com/session-webhook".into(),
    );

    let error = channel
        .deliver(
            &target(ConversationKind::Group, "chat-1"),
            &Step::Text("reply".into()),
        )
        .await
        .err()
        .expect("HTTP 200 does not mean the webhook accepted the message");

    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(!error.sent_before_error);
    assert!(!error.retryable);
    assert!(!error.message.contains("secret-token"));
}

#[tokio::test]
async fn session_webhook_unverifiable_success_is_reported_as_uncertain() {
    use crate::providers::http_test_support::HttpsServer;

    for body in ["not-json", r#"{"errmsg":"secret-token"}"#] {
        let body = body.to_owned();
        let server = HttpsServer::start(move |request| {
            assert_eq!(request.target, "/session-webhook");
            (200, body.clone())
        })
        .await;
        let channel = DingTalkChannel::with_http_client(
            "app-key".into(),
            "app-secret".into(),
            vec!["alice".into()],
            server.client.clone(),
        );
        channel.session_webhooks.write().await.insert(
            "chat-1".into(),
            "https://api.dingtalk.com/session-webhook".into(),
        );

        let error = channel
            .deliver(
                &target(ConversationKind::Group, "chat-1"),
                &Step::Text("reply".into()),
            )
            .await
            .err()
            .expect("a success status without a success code is ambiguous");

        assert_eq!(error.kind, SendErrorKind::Unknown);
        assert!(error.sent_before_error);
        assert!(!error.retryable);
        assert!(!error.message.contains("secret-token"));
    }
}

#[tokio::test]
async fn typed_send_rejects_missing_admission_or_robot_context_before_http() {
    use crate::providers::http_test_support::HttpsServer;
    use tinychannels_bus::{ChannelOutboundMessage, ContentPart, OutboundContentPart};

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
                codec: Some("amr_nb".into()),
                // Verify this stored metadata is forwarded. Codec-derived
                // duration is tested at the TinyVoice module boundary.
                duration_ms: Some(1_240),
            },
        }],
    }
}

#[tokio::test]
async fn typed_voice_send_forwards_amr_bytes_and_declared_duration() {
    use crate::providers::http_test_support::HttpsServer;
    use tinychannels_bus::channel::MessageReceiptPartKind;

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
            // Deliberately disagree with the encoded audio. The native send
            // must use TinyVoice's measured duration, not this TTS hint.
            duration_ms: Some(7_777),
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
    use sha2::Digest as _;
    use tinybus::{broker::Broker, module::ModuleHost, transport::memory::MemoryBus};
    use tinychannels_bus::channel::{ConversationKind, ConversationRef, MessageReceiptPartKind};
    use tinychannels_bus::{
        ChannelOutboundMessage, ChannelRef, ContentPart, OutboundContentPart, OutboundVoice,
    };
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
    use tinystoragedrivers_types::Scope;

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
    assert_ne!(measured.duration_ms, 7_777);
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
                let content_type = request
                    .headers
                    .lines()
                    .find(|line| line.to_ascii_lowercase().starts_with("content-type:"))
                    .expect("multipart content type");
                let boundary = content_type
                    .split_once("boundary=")
                    .expect("multipart boundary")
                    .1
                    .trim()
                    .trim_matches('"');
                let media_header = b"filename=\"voice.amr\"";
                let media_header_at = request
                    .raw_body
                    .windows(media_header.len())
                    .position(|window| window == media_header)
                    .expect("multipart voice filename");
                let payload_start = request.raw_body[media_header_at..]
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|offset| media_header_at + offset + 4)
                    .expect("multipart voice headers");
                let end_marker = format!("\r\n--{boundary}");
                let payload_end = request.raw_body[payload_start..]
                    .windows(end_marker.len())
                    .position(|window| window == end_marker.as_bytes())
                    .map(|offset| payload_start + offset)
                    .expect("multipart closing boundary");
                assert_eq!(
                    &request.raw_body[payload_start..payload_end],
                    bytes_capture.as_slice(),
                    "uploaded AMR must exactly match the bytes read from scoped storage"
                );
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
                    codec: prepared.note.codec,
                    duration_ms: prepared.note.duration_ms,
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
    assert_eq!(
        sent.receipt.parts[0].platform_message_id,
        "receipt-sampleText"
    );
    assert_eq!(sent.receipt.parts[1].kind, MessageReceiptPartKind::Voice);
    assert_eq!(
        sent.receipt.parts[1].platform_message_id,
        "receipt-sampleAudio"
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.target.starts_with("/media/upload?"))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.target == "/v1.0/robot/groupMessages/send")
            .map(|request| request.body["msgKey"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["sampleText", "sampleAudio"]
    );
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

#[tokio::test]
async fn public_adapter_start_owns_dingtalk_stream_until_stop_and_restart() {
    use crate::providers::http_test_support::HttpsServer;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_hdr_async, tungstenite::Message};

    fn callback(
        frame_id: &str,
        provider_id: &str,
        staff_id: &str,
        text: &str,
    ) -> tokio_tungstenite::tungstenite::Message {
        let data = json!({
            "msgId": provider_id,
            "msgtype": "text",
            "robotCode": "robot-account-1",
            "senderStaffId": staff_id,
            "senderId": "$:union-user-1",
            "conversationType": "2",
            "conversationId": "open-group-1",
            "text": {"content": text}
        })
        .to_string();
        tokio_tungstenite::tungstenite::Message::Text(
            json!({
                "type": "CALLBACK",
                "headers": {
                    "messageId": frame_id,
                    "topic": "/v1.0/im/bot/messages/get"
                },
                "data": data
            })
            .to_string()
            .into(),
        )
    }

    async fn read_ack(
        websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        message_id: &str,
    ) {
        let response = tokio::time::timeout(PUBLIC_LISTENER_GUARD, websocket.next())
            .await
            .expect("DingTalk Stream acknowledgement should arrive")
            .expect("DingTalk Stream connection should remain open")
            .expect("DingTalk Stream acknowledgement should be valid WebSocket data");
        let acknowledgement: serde_json::Value =
            serde_json::from_str(response.to_text().expect("acknowledgement is text")).unwrap();
        assert_eq!(acknowledgement["code"], 200);
        assert_eq!(acknowledgement["headers"]["messageId"], message_id);
    }

    let stream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream_address = stream_listener.local_addr().unwrap();
    let ticket_counter = Arc::new(AtomicUsize::new(0));
    let response_counter = ticket_counter.clone();
    let gateway = HttpsServer::start(move |request| {
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/v1.0/gateway/connections/open");
        assert_eq!(
            request.body,
            json!({
                "clientId": "app-key",
                "clientSecret": "app-secret",
                "subscriptions": [{
                    "type": "CALLBACK",
                    "topic": "/v1.0/im/bot/messages/get"
                }]
            })
        );
        let sequence = response_counter.fetch_add(1, Ordering::SeqCst) + 1;
        (
            200,
            json!({
                "endpoint": format!("ws://{stream_address}/"),
                "ticket": format!("stream-ticket-{sequence}")
            })
            .to_string(),
        )
    })
    .await;

    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let mock_stream = tokio::spawn(async move {
        for (session, ticket, accepted_id, accepted_text) in [
            (
                1_u8,
                "stream-ticket-1",
                "provider-message-1",
                "first stream message",
            ),
            (
                2_u8,
                "stream-ticket-2",
                "provider-message-2",
                "after restart",
            ),
        ] {
            let (socket, _) = tokio::time::timeout(PUBLIC_LISTENER_GUARD, stream_listener.accept())
                .await
                .expect("DingTalk adapter should connect to the local Stream gateway")
                .unwrap();
            #[allow(clippy::result_large_err)]
            let mut websocket = tokio::time::timeout(
                PUBLIC_LISTENER_GUARD,
                accept_hdr_async(
                    socket,
                    move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                          response| {
                        let expected_query = format!("ticket={ticket}");
                        assert_eq!(request.uri().query(), Some(expected_query.as_str()));
                        Ok(response)
                    },
                ),
            )
            .await
            .expect("DingTalk Stream WebSocket handshake should complete")
            .unwrap();

            websocket
                .send(Message::Text(
                    json!({
                        "type": "SYSTEM",
                        "headers": {
                            "messageId": format!("ping-{session}"),
                            "topic": "ping"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let pong = tokio::time::timeout(PUBLIC_LISTENER_GUARD, websocket.next())
                .await
                .expect("DingTalk Stream SYSTEM ping should be acknowledged")
                .unwrap()
                .unwrap();
            let pong: serde_json::Value = serde_json::from_str(pong.to_text().unwrap()).unwrap();
            assert_eq!(pong["code"], 200);
            assert_eq!(pong["headers"]["messageId"], format!("ping-{session}"));
            ready_tx.send(session).unwrap();

            if session == 1 {
                websocket
                    .send(callback(
                        "frame-denied",
                        "provider-denied",
                        "mallory",
                        "filtered",
                    ))
                    .await
                    .unwrap();
                websocket
                    .send(callback(
                        "frame-accepted",
                        accepted_id,
                        "alice",
                        accepted_text,
                    ))
                    .await
                    .unwrap();
                read_ack(&mut websocket, "frame-accepted").await;
                websocket
                    .send(callback(
                        "frame-duplicate",
                        accepted_id,
                        "alice",
                        "duplicate must not publish",
                    ))
                    .await
                    .unwrap();
                read_ack(&mut websocket, "frame-duplicate").await;
                websocket
                    .send(callback(
                        "frame-barrier",
                        "provider-barrier",
                        "alice",
                        "after duplicate",
                    ))
                    .await
                    .unwrap();
                read_ack(&mut websocket, "frame-barrier").await;
            } else {
                websocket
                    .send(callback(
                        "frame-restarted",
                        accepted_id,
                        "alice",
                        accepted_text,
                    ))
                    .await
                    .unwrap();
                read_ack(&mut websocket, "frame-restarted").await;
            }

            let closed = tokio::time::timeout(PUBLIC_LISTENER_GUARD, websocket.next())
                .await
                .expect("DingTalk stop should close its Stream WebSocket");
            assert!(matches!(closed, None | Some(Ok(Message::Close(_)))));
        }
    });

    let channel = Arc::new(DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);

    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let session = tokio::select! {
        result = &mut listener => panic!("DingTalk adapter start returned before Stream ownership: {result:?}"),
        session = tokio::time::timeout(PUBLIC_LISTENER_GUARD, ready_rx.recv()) => session
            .expect("DingTalk Stream handshake should finish within the guard")
            .expect("mock Stream readiness sender remains connected"),
    };
    assert_eq!(session, 1);
    let duplicate_start = tokio::time::timeout(PUBLIC_LISTENER_GUARD, adapter.start(&sink))
        .await
        .expect("duplicate DingTalk start should reject promptly")
        .expect_err("a second listener must not replace the active Stream owner");
    assert_eq!(duplicate_start.kind, SendErrorKind::Forbidden);

    let first = tokio::time::timeout(PUBLIC_LISTENER_GUARD, rx.recv())
        .await
        .expect("admitted DingTalk callback should reach the supplied typed sink")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(first) = first else {
        panic!("DingTalk callbacks must arrive as typed inbound ingress");
    };
    assert_eq!(first.message_id, "provider-message-1");
    assert_eq!(first.sender.id, "alice");
    assert_eq!(first.conversation.id, "open-group-1");
    assert_eq!(first.text, "first stream message");

    let barrier = tokio::time::timeout(PUBLIC_LISTENER_GUARD, rx.recv())
        .await
        .expect("barrier callback should follow the accepted DingTalk callback")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(barrier) = barrier else {
        panic!("DingTalk callbacks must arrive as typed inbound ingress");
    };
    assert_eq!(barrier.message_id, "provider-barrier");
    assert_eq!(barrier.sender.id, "alice");
    assert_eq!(barrier.conversation.id, "open-group-1");
    assert_eq!(barrier.text, "after duplicate");

    adapter.stop().await.unwrap();
    tokio::time::timeout(PUBLIC_LISTENER_GUARD, &mut listener)
        .await
        .expect("DingTalk stop must join the Stream listener")
        .expect("DingTalk listener must not panic")
        .expect("DingTalk listener must stop cleanly");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    let mut restarted = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let session = tokio::select! {
        result = &mut restarted => panic!("DingTalk restart returned before Stream ownership: {result:?}"),
        session = tokio::time::timeout(PUBLIC_LISTENER_GUARD, ready_rx.recv()) => session
            .expect("restarted DingTalk Stream handshake should finish within the guard")
            .expect("mock Stream readiness sender remains connected"),
    };
    assert_eq!(session, 2);
    let restarted_ingress = tokio::time::timeout(PUBLIC_LISTENER_GUARD, rx.recv())
        .await
        .expect("restarted DingTalk Stream should publish admitted callbacks")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(restarted_ingress) = restarted_ingress else {
        panic!("DingTalk callbacks must arrive as typed inbound ingress");
    };
    assert_eq!(restarted_ingress.message_id, "provider-message-2");
    assert_eq!(restarted_ingress.sender.id, "alice");
    assert_eq!(restarted_ingress.conversation.id, "open-group-1");
    assert_eq!(restarted_ingress.text, "after restart");

    adapter.stop().await.unwrap();
    tokio::time::timeout(PUBLIC_LISTENER_GUARD, &mut restarted)
        .await
        .expect("DingTalk stop must join the restarted Stream listener")
        .expect("restarted DingTalk listener must not panic")
        .expect("restarted DingTalk listener must stop cleanly");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    tokio::time::timeout(PUBLIC_LISTENER_GUARD, mock_stream)
        .await
        .expect("mock DingTalk Stream gateway should observe both stops")
        .expect("mock Stream gateway task must not panic");
}

#[tokio::test]
async fn cancelled_listener_does_not_open_gateway_after_registration() {
    use crate::providers::http_test_support::HttpsServer;

    let server = HttpsServer::start(|request| {
        panic!("a cancelled DingTalk listener must not register: {request:?}")
    })
    .await;
    let channel = Arc::new(DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel.clone();
    let (id, cancellation) = channel.begin_listener().unwrap();
    let _registration = super::super::ListenerRegistration {
        channel: &channel,
        id,
    };
    adapter.stop().await.unwrap();

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);
    tokio::time::timeout(
        PUBLIC_LISTENER_GUARD,
        channel.listen_owned(&sink, Some(cancellation)),
    )
    .await
    .expect("a pre-cancelled DingTalk listener must return promptly")
    .expect("cancellation before registration is a clean stop");
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn public_adapter_does_not_ack_or_deduplicate_rejected_typed_callbacks() {
    use crate::providers::http_test_support::HttpsServer;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_hdr_async, tungstenite::Message};

    struct RejectOnceSink {
        attempts: AtomicUsize,
        accepted: tokio::sync::mpsc::UnboundedSender<ChannelIngress>,
    }

    #[async_trait::async_trait]
    impl ChannelInboundSink for RejectOnceSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            ingress: ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(tinychannels_bus::channel::ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "temporary typed sink failure".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            self.accepted
                .send(ingress)
                .map_err(|_| tinychannels_bus::channel::ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "test receiver closed".into(),
                    retryable: true,
                    ..Default::default()
                })
        }
    }

    fn callback(frame_id: &str) -> Message {
        let data = json!({
            "msgId": "provider-message-retry",
            "msgtype": "text",
            "robotCode": "robot-account-1",
            "senderStaffId": "alice",
            "senderId": "$:union-user-1",
            "conversationType": "2",
            "conversationId": "open-group-1",
            "text": {"content": "retry after sink rejection"}
        })
        .to_string();
        Message::Text(
            json!({
                "type": "CALLBACK",
                "headers": {
                    "messageId": frame_id,
                    "topic": "/v1.0/im/bot/messages/get"
                },
                "data": data
            })
            .to_string()
            .into(),
        )
    }

    let stream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream_address = stream_listener.local_addr().unwrap();
    let ticket_counter = Arc::new(AtomicUsize::new(0));
    let response_counter = ticket_counter.clone();
    let gateway = HttpsServer::start(move |request| {
        assert_eq!(request.method, "POST");
        match request.target.as_str() {
            "/v1.0/gateway/connections/open" => {
                let sequence = response_counter.fetch_add(1, Ordering::SeqCst) + 1;
                (
                    200,
                    json!({
                        "endpoint": format!("ws://{stream_address}/"),
                        "ticket": format!("retry-ticket-{sequence}")
                    })
                    .to_string(),
                )
            }
            other => panic!("unexpected DingTalk request: {other}"),
        }
    })
    .await;

    let mock_stream = tokio::spawn(async move {
        for session in 1..=2 {
            let (socket, _) = tokio::time::timeout(PUBLIC_LISTENER_GUARD, stream_listener.accept())
                .await
                .expect("DingTalk should reconnect after sink rejection")
                .unwrap();
            #[allow(clippy::result_large_err)]
            let mut websocket = tokio::time::timeout(
                PUBLIC_LISTENER_GUARD,
                accept_hdr_async(
                    socket,
                    move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                          response| {
                        let expected_query = format!("ticket=retry-ticket-{session}");
                        assert_eq!(request.uri().query(), Some(expected_query.as_str()));
                        Ok(response)
                    },
                ),
            )
            .await
            .expect("DingTalk Stream handshake should complete")
            .unwrap();
            websocket
                .send(Message::Text(
                    json!({
                        "type": "SYSTEM",
                        "headers": {
                            "messageId": format!("ping-{session}"),
                            "topic": "ping"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let pong = tokio::time::timeout(PUBLIC_LISTENER_GUARD, websocket.next())
                .await
                .expect("DingTalk should answer the stream ping")
                .unwrap()
                .unwrap();
            let pong: serde_json::Value = serde_json::from_str(pong.to_text().unwrap()).unwrap();
            assert_eq!(pong["headers"]["messageId"], format!("ping-{session}"));

            websocket
                .send(callback(if session == 1 {
                    "frame-rejected"
                } else {
                    "frame-retried"
                }))
                .await
                .unwrap();
            if session == 1 {
                loop {
                    let received = tokio::time::timeout(PUBLIC_LISTENER_GUARD, websocket.next())
                        .await
                        .expect("rejected publication should terminate its stream");
                    match received {
                        Some(Ok(Message::Text(text))) => {
                            let response: serde_json::Value =
                                serde_json::from_str(text.as_ref()).unwrap();
                            assert_ne!(
                                response["headers"]["messageId"], "frame-rejected",
                                "a rejected typed callback must not receive a success ACK"
                            );
                        }
                        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                        _ => {}
                    }
                }
            } else {
                let response = tokio::time::timeout(PUBLIC_LISTENER_GUARD, websocket.next())
                    .await
                    .expect("accepted callback should receive a stream ACK")
                    .expect("DingTalk connection remains open")
                    .expect("ACK frame should be valid");
                let response: serde_json::Value =
                    serde_json::from_str(response.to_text().unwrap()).unwrap();
                assert_eq!(response["code"], 200);
                assert_eq!(response["headers"]["messageId"], "frame-retried");
                let closed = tokio::time::timeout(PUBLIC_LISTENER_GUARD, websocket.next())
                    .await
                    .expect("stop should close the restarted stream");
                assert!(matches!(closed, None | Some(Ok(Message::Close(_)))));
            }
        }
    });

    let channel = Arc::new(DingTalkChannel::with_http_client(
        "app-key".into(),
        "app-secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (accepted, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(RejectOnceSink {
        attempts: AtomicUsize::new(0),
        accepted,
    });

    let rejected = tokio::time::timeout(PUBLIC_LISTENER_GUARD, adapter.start(sink.as_ref()))
        .await
        .expect("rejected typed publication should release the listener")
        .unwrap_err();
    assert_eq!(rejected.kind, SendErrorKind::Transient);
    assert!(rejected.retryable);
    assert!(rx.is_empty());

    let restarted = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(sink.as_ref()).await }
    });
    let accepted = tokio::time::timeout(PUBLIC_LISTENER_GUARD, rx.recv())
        .await
        .expect("same provider message should be retried after rejection")
        .expect("test receiver remains connected");
    let ChannelIngress::Inbound(accepted) = accepted else {
        panic!("DingTalk callbacks must arrive as typed inbound ingress");
    };
    assert_eq!(accepted.message_id, "provider-message-retry");
    assert_eq!(accepted.sender.id, "alice");
    assert_eq!(accepted.conversation.id, "open-group-1");
    assert_eq!(accepted.text, "retry after sink rejection");

    adapter.stop().await.unwrap();
    tokio::time::timeout(PUBLIC_LISTENER_GUARD, restarted)
        .await
        .expect("stop must join the restarted listener")
        .expect("listener task must not panic")
        .expect("explicit stop should be clean");
    tokio::time::timeout(PUBLIC_LISTENER_GUARD, mock_stream)
        .await
        .expect("mock Stream server should observe both connection ends")
        .expect("mock Stream task must not panic");
}
