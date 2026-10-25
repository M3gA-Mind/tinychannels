use super::*;
use crate::channel::ChannelAdapter;
use crate::traits::SendMessage;

struct DingTalkInboundRecorder(
    tokio::sync::mpsc::UnboundedSender<tinychannels_bus::ChannelIngress>,
);

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for DingTalkInboundRecorder {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.0
            .send(ingress)
            .map_err(|_| tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "DingTalk test sink is closed".into(),
                retryable: true,
                ..Default::default()
            })
    }
}

fn dingtalk_inbound_sink() -> (
    std::sync::Arc<DingTalkInboundRecorder>,
    tokio::sync::mpsc::UnboundedReceiver<tinychannels_bus::ChannelIngress>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (std::sync::Arc::new(DingTalkInboundRecorder(tx)), rx)
}

#[test]
fn test_name() {
    let ch = DingTalkChannel::new("id".into(), "secret".into(), vec![]);
    assert_eq!(ch.descriptor().id, "dingtalk");
}

#[test]
fn test_user_allowed_wildcard() {
    let ch = DingTalkChannel::new("id".into(), "secret".into(), vec!["*".into()]);
    assert!(ch.is_user_allowed("anyone"));
}

#[test]
fn test_user_allowed_specific() {
    let ch = DingTalkChannel::new("id".into(), "secret".into(), vec!["user123".into()]);
    assert!(ch.is_user_allowed("user123"));
    assert!(!ch.is_user_allowed("other"));
}

#[test]
fn test_user_denied_empty() {
    let ch = DingTalkChannel::new("id".into(), "secret".into(), vec![]);
    assert!(!ch.is_user_allowed("anyone"));
}

#[test]
fn test_config_serde() {
    let toml_str = r#"
client_id = "app_id_123"
client_secret = "secret_456"
allowed_users = ["user1", "*"]
"#;
    let config: crate::config::DingTalkConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.client_id, "app_id_123");
    assert_eq!(config.client_secret, "secret_456");
    assert_eq!(config.allowed_users, vec!["user1", "*"]);
}

#[test]
fn test_config_serde_defaults() {
    let toml_str = r#"
client_id = "id"
client_secret = "secret"
"#;
    let config: crate::config::DingTalkConfig = toml::from_str(toml_str).unwrap();
    assert!(config.allowed_users.is_empty());
}

#[test]
fn parse_stream_data_supports_string_payload() {
    let frame = serde_json::json!({
        "data": "{\"text\":{\"content\":\"hello\"}}"
    });
    let parsed = DingTalkChannel::parse_stream_data(&frame).unwrap();
    assert_eq!(
        parsed.get("text").and_then(|v| v.get("content")),
        Some(&serde_json::json!("hello"))
    );
}

#[test]
fn parse_stream_data_supports_object_payload() {
    let frame = serde_json::json!({
        "data": {"text": {"content": "hello"}}
    });
    let parsed = DingTalkChannel::parse_stream_data(&frame).unwrap();
    assert_eq!(
        parsed.get("text").and_then(|v| v.get("content")),
        Some(&serde_json::json!("hello"))
    );
}

#[test]
fn resolve_chat_id_handles_numeric_group_conversation_type() {
    let data = serde_json::json!({
        "conversationType": 2,
        "conversationId": "cid-group",
    });
    let chat_id = DingTalkChannel::resolve_chat_id(&data, "staff-1");
    assert_eq!(chat_id, "cid-group");
}

#[tokio::test]
async fn gateway_registration_and_webhook_replies_preserve_payloads_and_errors() {
    use super::super::http_test_support::HttpsServer;
    use serde_json::json;
    use wiremock::matchers::{body_json, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = HttpsServer::start(|request| {
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/v1.0/gateway/connections/open");
        assert_eq!(request.body, json!({"clientId": "id", "clientSecret": "secret", "subscriptions": [{"type": "CALLBACK", "topic": DINGTALK_BOT_CALLBACK_TOPIC}]}));
        (200, json!({"endpoint": "ws://localhost/stream", "ticket": "ticket"}).to_string())
    }).await;
    let channel = DingTalkChannel::with_http_client(
        "id".into(),
        "secret".into(),
        vec![],
        server.client.clone(),
    );
    let gateway = channel.register_connection().await.unwrap();
    assert_eq!(
        (gateway.endpoint.as_str(), gateway.ticket.as_str()),
        ("ws://localhost/stream", "ticket")
    );
    assert!(crate::provider_test_support::is_healthy(&channel).await);
    assert!(
        crate::provider_test_support::send_v1(&channel, &SendMessage::new("reply", "unknown"))
            .await
            .unwrap_err()
            .to_string()
            .contains("admitted App Bot conversation")
    );

    let webhook = MockServer::start().await;
    channel
        .session_webhooks
        .write()
        .await
        .insert("room".into(), webhook.uri());
    Mock::given(method("POST"))
        .and(body_json(
            json!({"msgtype": "markdown", "markdown": {"title": "OpenHuman", "text": "reply"}}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"errcode": 0})))
        .expect(1)
        .mount(&webhook)
        .await;
    crate::provider_test_support::send_v1(&channel, &SendMessage::new("reply", "room"))
        .await
        .unwrap();
    let mut reply = SendMessage::new("custom", "room");
    reply.subject = Some("Subject".into());
    Mock::given(method("POST"))
        .and(body_json(
            json!({"msgtype": "markdown", "markdown": {"title": "Subject", "text": "custom"}}),
        ))
        .respond_with(ResponseTemplate::new(403).set_body_string("denied"))
        .expect(1)
        .mount(&webhook)
        .await;
    let error = crate::provider_test_support::send_v1(&channel, &reply)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("DingTalk webhook reply was rejected"));
    assert!(!error.contains("denied"));
    webhook.verify().await;
    channel
        .session_webhooks
        .write()
        .await
        .insert("room".into(), "http://127.0.0.1:0".into());
    assert!(
        crate::provider_test_support::send_v1(&channel, &reply)
            .await
            .is_err()
    );

    for (status, body) in [(403, "denied"), (200, "invalid json")] {
        let server = HttpsServer::start(move |_| (status, body.into())).await;
        let channel = DingTalkChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec![],
            server.client.clone(),
        );
        assert!(channel.register_connection().await.is_err());
        assert!(!crate::provider_test_support::is_healthy(&channel).await);
    }
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:0").unwrap())
        .build()
        .unwrap();
    let channel = DingTalkChannel::with_http_client("id".into(), "secret".into(), vec![], client);
    assert!(!crate::provider_test_support::is_healthy(&channel).await);
}

#[tokio::test]
async fn stream_listener_acknowledges_pings_routes_chats_and_stores_reply_webhooks() {
    use super::super::http_test_support::HttpsServer;
    use serde_json::json;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gateway = HttpsServer::start(move |_| {
        (
            200,
            json!({"endpoint": format!("ws://{address}/"), "ticket": "test-ticket"}).to_string(),
        )
    })
    .await;
    let channel = Arc::new(DingTalkChannel::with_http_client(
        "id".into(),
        "secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    ));
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let websocket = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        // Tungstenite fixes the callback error type to an unboxed HTTP response.
        #[allow(clippy::result_large_err)]
        let mut ws = tokio_tungstenite::accept_hdr_async(
            socket,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert_eq!(request.uri().query(), Some("ticket=test-ticket"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        ws.send(Message::Binary(vec![1, 2].into())).await.unwrap();
        ws.send(Message::Text("invalid json".into())).await.unwrap();
        ws.send(Message::Text(json!({"type": "OTHER"}).to_string().into()))
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"type": "SYSTEM", "headers": {"messageId": "ping-id"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let pong: serde_json::Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
        assert_eq!(
            pong,
            json!({"code": 200, "headers": {"contentType": "application/json", "messageId": "ping-id"}, "message": "OK", "data": ""})
        );
        for frame in [
            json!({"type": "CALLBACK"}),
            json!({"type": "CALLBACK", "data": "invalid json"}),
            json!({"type": "CALLBACK", "data": 123}),
            json!({"type": "CALLBACK", "data": {"text": {"content": "   "}}}),
            json!({"type": "CALLBACK", "data": {"senderStaffId": "mallory", "text": {"content": "blocked"}}}),
        ] {
            ws.send(Message::Text(frame.to_string().into()))
                .await
                .unwrap();
        }
        for (kind, conversation_type, id, content) in [
            ("EVENT", "2", "group", " first "),
            ("CALLBACK", "1", "private", "second"),
        ] {
            let data = json!({"msgId": id, "senderStaffId": "alice", "conversationType": conversation_type, "conversationId": "group-id", "text": {"content": content}, "sessionWebhook": format!("https://example.test/{id}")});
            let data = if kind == "EVENT" {
                data
            } else {
                json!(data.to_string())
            };
            ws.send(Message::Text(
                json!({"type": kind, "headers": {"messageId": id}, "data": data})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let ack: serde_json::Value =
                serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
            assert_eq!(ack["headers"]["messageId"], id);
            assert_eq!(ack["code"], 200);
        }
        release_rx.await.unwrap();
        // A callback rejected by the supplied sink must not be acknowledged;
        // DingTalk can retry it after the host has recovered.
        ws.send(Message::Text(json!({"type": "CALLBACK", "data": {"msgId": "closed", "senderStaffId": "alice", "text": {"content": "closed"}}}).to_string().into())).await.unwrap();
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
            .await
            .expect("rejected typed delivery should close the stream");
        assert!(matches!(
            terminal,
            None | Some(Err(_)) | Some(Ok(Message::Close(_)))
        ));
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listening_channel = channel.clone();
    let listening = tokio::spawn(async move {
        crate::provider_test_support::listen_v1(listening_channel.as_ref(), tx).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(
            (
                first.sender.as_str(),
                first.reply_target.as_str(),
                first.content.as_str(),
                first.channel.as_str()
            ),
            ("alice", "group-id", "first", "dingtalk")
        );
        assert_eq!(first.id, "group");
        assert!(first.timestamp > 0);
        assert_eq!(
            (second.reply_target.as_str(), second.content.as_str()),
            ("group-id", "second")
        );
        drop(rx);
        release_tx.send(()).unwrap();
        assert!(listening.await.unwrap().is_err());
        websocket.await.unwrap();
    })
    .await
    .unwrap();
    let webhooks = channel.session_webhooks.read().await;
    assert_eq!(
        webhooks.get("group-id").map(String::as_str),
        Some("https://example.test/group")
    );
    assert_eq!(
        webhooks.get("alice").map(String::as_str),
        Some("https://example.test/private")
    );
}

#[tokio::test]
async fn stream_file_callback_downloads_and_publishes_typed_ingress_once() {
    use super::super::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::StubMedia;
    use serde_json::json;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpsServer::start_with_bytes(
        move |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/gateway/connections/open") => (
                200,
                json!({"endpoint": format!("ws://{address}/"), "ticket": "test-ticket"})
                    .to_string()
                    .into_bytes(),
            ),
            ("POST", "/v1.0/oauth2/accessToken") => (
                200,
                json!({"accessToken": "app-token"}).to_string().into_bytes(),
            ),
            ("POST", "/v1.0/robot/messageFiles/download") => {
                assert_eq!(request.body["robotCode"], "robot-7");
                assert_eq!(request.body["downloadCode"], "temporary-download-code");
                (
                    200,
                    json!({"downloadUrl": "https://api.dingtalk.com/media/file"})
                        .to_string()
                        .into_bytes(),
                )
            }
            ("GET", "/media/file") => (200, b"provider-file-bytes".to_vec()),
            _ => (404, Vec::new()),
        },
        "application/pdf",
    )
    .await;
    let media = Arc::new(StubMedia::default());
    let (sink, mut ingress_rx) = dingtalk_inbound_sink();
    let channel = Arc::new(
        DingTalkChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec!["alice".into()],
            server.client.clone(),
        )
        .with_attachment_http_client(server.client.clone())
        .with_media_repository(media.clone()),
    );
    let websocket = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        #[allow(clippy::result_large_err)]
        let mut ws = tokio_tungstenite::accept_hdr_async(
            socket,
            |_request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                Ok(response)
            },
        )
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({
                "type": "CALLBACK",
                "headers": {"messageId": "stream-frame-17"},
                "data": {
                    "msgId": "provider-message-42",
                    "msgtype": "file",
                    "robotCode": "robot-7",
                    "senderStaffId": "alice",
                    "senderId": "union-alice",
                    "conversationType": "2",
                    "conversationId": "open-conversation-9",
                    "content": {
                        "downloadCode": "temporary-download-code",
                        "fileName": "report.pdf"
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        let response = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
            .await
            .expect("file callback should be acknowledged after typed ingress")
            .unwrap()
            .unwrap();
        let ack: serde_json::Value = serde_json::from_str(&response.to_string()).unwrap();
        assert_eq!(ack["code"], 200);
        assert_eq!(ack["headers"]["messageId"], "stream-frame-17");
        ws.close(None).await.unwrap();
    });
    let listening_channel = channel.clone();
    let listening_sink = sink.clone();
    let listening =
        tokio::spawn(async move { listening_channel.start(listening_sink.as_ref()).await });

    websocket.await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(3), listening)
            .await
            .expect("closing the Stream fixture should end listen")
            .unwrap()
            .is_err()
    );
    let Some(tinychannels_bus::ChannelIngress::Inbound(ingress)) = ingress_rx.try_recv().ok()
    else {
        panic!("typed DingTalk callback should reach the supplied sink");
    };
    assert_eq!(ingress.message_id, "provider-message-42");
    assert_eq!(ingress.conversation.id, "open-conversation-9");
    assert_eq!(ingress.sender.id, "alice");
    assert!(matches!(
        &ingress.parts[0],
        tinychannels_bus::ContentPart::File {
            file_name: Some(name),
            ..
        } if name == "report.pdf"
    ));
    assert!(
        ingress_rx.try_recv().is_err(),
        "one callback yields one typed event"
    );
    let stored = media.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].bytes, b"provider-file-bytes");
    assert_eq!(
        stored[0].metadata.scope.conversation.id,
        "open-conversation-9"
    );
    let requests = server.requests.lock().unwrap();
    assert!(requests.iter().any(|request| {
        request.method == "POST" && request.target == "/v1.0/robot/messageFiles/download"
    }));
}

#[tokio::test]
async fn stream_audio_callback_stores_provider_voice_and_acknowledges_frame() {
    use super::super::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::StubVoice;
    use base64::Engine as _;
    use serde_json::json;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpsServer::start_with_bytes(
        move |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/gateway/connections/open") => (
                200,
                json!({"endpoint": format!("ws://{address}/"), "ticket": "test-ticket"})
                    .to_string()
                    .into_bytes(),
            ),
            ("POST", "/v1.0/oauth2/accessToken") => (
                200,
                json!({"accessToken": "app-token"}).to_string().into_bytes(),
            ),
            ("POST", "/v1.0/robot/messageFiles/download") => {
                assert_eq!(request.body["robotCode"], "robot-audio");
                assert_eq!(request.body["downloadCode"], "voice-download-code");
                (
                    200,
                    json!({"downloadUrl": "https://api.dingtalk.com/media/voice"})
                        .to_string()
                        .into_bytes(),
                )
            }
            ("GET", "/media/voice") => (
                200,
                include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg").to_vec(),
            ),
            _ => panic!("unexpected DingTalk request: {request:?}"),
        },
        "audio/ogg",
    )
    .await;
    let voice = Arc::new(StubVoice::default());
    let (sink, mut ingress_rx) = dingtalk_inbound_sink();
    let channel = Arc::new(
        DingTalkChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec!["staff-a".into()],
            server.client.clone(),
        )
        .with_attachment_http_client(server.client.clone())
        .with_voice_media_store(voice.clone()),
    );
    assert!(channel.declared_capabilities().voice_in.supported);

    let websocket = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        #[allow(clippy::result_large_err)]
        let mut ws = tokio_tungstenite::accept_hdr_async(
            socket,
            |_request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                Ok(response)
            },
        )
        .await
        .unwrap();
        let data = json!({
            "msgId": "voice-message-8",
            "msgtype": "audio",
            "robotCode": "robot-audio",
            "senderStaffId": "staff-a",
            "conversationType": "1",
            "conversationId": "dm-a",
            "content": {
                "downloadCode": "voice-download-code",
                "recognition": "turn left"
            }
        });
        ws.send(Message::Text(
            json!({"type":"CALLBACK", "headers":{"messageId":"voice-frame-8"}, "data":data.clone()})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let response = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
            .await
            .expect("audio callback should be acknowledged after typed ingress")
            .unwrap()
            .unwrap();
        let ack: serde_json::Value = serde_json::from_str(&response.to_string()).unwrap();
        assert_eq!(ack["code"], 200);
        assert_eq!(ack["headers"]["messageId"], "voice-frame-8");
        ws.send(Message::Text(
            json!({"type":"CALLBACK", "headers":{"messageId":"voice-frame-9"}, "data":data})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let duplicate_ack = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
            .await
            .expect("a duplicate provider event must be acknowledged")
            .unwrap()
            .unwrap();
        let duplicate_ack: serde_json::Value =
            serde_json::from_str(&duplicate_ack.to_string()).unwrap();
        assert_eq!(duplicate_ack["code"], 200);
        assert_eq!(duplicate_ack["headers"]["messageId"], "voice-frame-9");
        ws.close(None).await.unwrap();
    });
    let listening_channel = channel.clone();
    let listening_sink = sink.clone();
    let listening =
        tokio::spawn(async move { listening_channel.start(listening_sink.as_ref()).await });

    websocket.await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(3), listening)
            .await
            .expect("closing the Stream fixture should end listen")
            .unwrap()
            .is_err()
    );
    let Some(tinychannels_bus::ChannelIngress::Inbound(inbound)) = ingress_rx.try_recv().ok()
    else {
        panic!("typed DingTalk audio callback should reach the supplied sink");
    };
    assert_eq!(inbound.message_id, "voice-message-8");
    assert_eq!(inbound.conversation.id, "dm-a");
    let [tinychannels_bus::ContentPart::VoiceNote { voice_note }] = inbound.parts.as_slice() else {
        panic!("DingTalk audio must be accepted as typed voice content");
    };
    assert_eq!(voice_note.transcript.as_ref().unwrap().text, "turn left");
    assert_eq!(
        voice_note.transcript.as_ref().unwrap().source,
        tinychannels_bus::VoiceTranscriptSource::Provider
    );
    assert!(ingress_rx.try_recv().is_err());

    let stored = voice.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].mime_type, "audio/ogg");
    assert_eq!(
        stored[0].duration_ms, None,
        "the official callback fixture supplies no duration"
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&stored[0].audio_base64)
            .unwrap(),
        include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg")
    );
}

#[tokio::test]
async fn unauthorized_attachment_callback_is_not_downloaded_or_published() {
    use super::super::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::StubMedia;
    use serde_json::json;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpsServer::start_with_bytes(
        move |request| match (request.method.as_str(), request.target.as_str()) {
            ("POST", "/v1.0/gateway/connections/open") => (
                200,
                json!({"endpoint": format!("ws://{address}/"), "ticket": "test-ticket"})
                    .to_string()
                    .into_bytes(),
            ),
            _ => panic!("unauthorized attachment must not cause HTTP requests: {request:?}"),
        },
        "application/json",
    )
    .await;
    let media = Arc::new(StubMedia::default());
    let (sink, mut ingress_rx) = dingtalk_inbound_sink();
    let channel = Arc::new(
        DingTalkChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec!["alice".into()],
            server.client.clone(),
        )
        .with_attachment_http_client(server.client.clone())
        .with_media_repository(media.clone()),
    );
    let websocket = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        #[allow(clippy::result_large_err)]
        let mut ws = tokio_tungstenite::accept_hdr_async(
            socket,
            |_request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                Ok(response)
            },
        )
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({
                "type": "CALLBACK",
                "headers": {"messageId": "unauthorized-frame"},
                "data": {
                    "msgId": "unauthorized-provider-message",
                    "msgtype": "file",
                    "robotCode": "robot-7",
                    "senderStaffId": "mallory",
                    "conversationType": "2",
                    "conversationId": "open-conversation-9",
                    "content": {"downloadCode": "secret-download-code", "fileName": "report.pdf"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        ws.close(None).await.unwrap();
    });
    let listening_channel = channel.clone();
    let listening_sink = sink.clone();
    let listening =
        tokio::spawn(async move { listening_channel.start(listening_sink.as_ref()).await });
    websocket.await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(3), listening)
            .await
            .expect("closing the fixture should end listen")
            .unwrap()
            .is_err()
    );
    assert!(ingress_rx.try_recv().is_err());
    assert!(media.stored.lock().unwrap().is_empty());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "only gateway registration is expected");
    assert_eq!(requests[0].target, "/v1.0/gateway/connections/open");
}
