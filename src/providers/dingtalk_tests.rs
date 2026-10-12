use super::*;

#[test]
fn test_name() {
    let ch = DingTalkChannel::new("id".into(), "secret".into(), vec![]);
    assert_eq!(ch.name(), "dingtalk");
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
    assert!(channel.health_check().await);
    assert!(
        channel
            .send(&SendMessage::new("reply", "unknown"))
            .await
            .unwrap_err()
            .to_string()
            .contains("No session webhook")
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
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&webhook)
        .await;
    channel
        .send(&SendMessage::new("reply", "room"))
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
    assert!(
        channel
            .send(&reply)
            .await
            .unwrap_err()
            .to_string()
            .contains("denied")
    );
    webhook.verify().await;
    channel
        .session_webhooks
        .write()
        .await
        .insert("room".into(), "http://127.0.0.1:0".into());
    assert!(channel.send(&reply).await.is_err());

    for (status, body) in [(403, "denied"), (200, "invalid json")] {
        let server = HttpsServer::start(move |_| (status, body.into())).await;
        let channel = DingTalkChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec![],
            server.client.clone(),
        );
        assert!(channel.register_connection().await.is_err());
        assert!(!channel.health_check().await);
    }
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:0").unwrap())
        .build()
        .unwrap();
    let channel = DingTalkChannel::with_http_client("id".into(), "secret".into(), vec![], client);
    assert!(!channel.health_check().await);
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
            let data = json!({"senderStaffId": "alice", "conversationType": conversation_type, "conversationId": "group-id", "text": {"content": content}, "sessionWebhook": format!("https://example.test/{id}")});
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
        // The next valid message makes the listener observe the closed receiver.
        ws.send(Message::Text(json!({"type": "CALLBACK", "data": {"senderStaffId": "alice", "text": {"content": "closed"}}}).to_string().into())).await.unwrap();
        let ack: serde_json::Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
        assert_eq!(ack["headers"]["messageId"], "");
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listening_channel = channel.clone();
    let listening = tokio::spawn(async move { listening_channel.listen(tx).await });
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
        assert!(Uuid::parse_str(&first.id).is_ok());
        assert!(first.timestamp > 0);
        assert_eq!(
            (second.reply_target.as_str(), second.content.as_str()),
            ("alice", "second")
        );
        drop(rx);
        release_tx.send(()).unwrap();
        assert!(
            listening
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("stream ended")
        );
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
