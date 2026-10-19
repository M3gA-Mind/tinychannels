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

    const AMR_BYTES: &[u8] = b"#!AMR\nprovider AMR voice bytes";

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
    assert!(caps.voice_out.supported);
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
