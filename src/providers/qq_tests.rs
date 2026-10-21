#![allow(deprecated)]

use super::*;
use crate::channel::ChannelAdapter;
use tinychannels_bus::SendMessage;

#[test]
fn test_name() {
    let ch = QQChannel::new("id".into(), "secret".into(), vec![]);
    assert_eq!(ch.descriptor().id, "qq");
}

#[test]
fn test_user_allowed_wildcard() {
    let ch = QQChannel::new("id".into(), "secret".into(), vec!["*".into()]);
    assert!(ch.is_user_allowed("anyone"));
}

#[test]
fn test_user_allowed_specific() {
    let ch = QQChannel::new("id".into(), "secret".into(), vec!["user123".into()]);
    assert!(ch.is_user_allowed("user123"));
    assert!(!ch.is_user_allowed("other"));
}

#[test]
fn test_user_denied_empty() {
    let ch = QQChannel::new("id".into(), "secret".into(), vec![]);
    assert!(!ch.is_user_allowed("anyone"));
}

#[tokio::test]
async fn test_dedup() {
    let ch = QQChannel::new("id".into(), "secret".into(), vec![]);
    assert!(!ch.is_duplicate("msg1").await);
    assert!(ch.is_duplicate("msg1").await);
    assert!(!ch.is_duplicate("msg2").await);
}

#[tokio::test]
async fn test_dedup_empty_id() {
    let ch = QQChannel::new("id".into(), "secret".into(), vec![]);
    // Empty IDs should never be considered duplicates
    assert!(!ch.is_duplicate("").await);
    assert!(!ch.is_duplicate("").await);
}

#[test]
fn test_config_serde() {
    let toml_str = r#"
app_id = "12345"
app_secret = "secret_abc"
allowed_users = ["user1"]
"#;
    let config: crate::config::QQConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.app_id, "12345");
    assert_eq!(config.app_secret, "secret_abc");
    assert_eq!(config.allowed_users, vec!["user1"]);
}

#[test]
fn ensure_https_accepts_https_urls() {
    assert!(ensure_https("https://api.example.com").is_ok());
    assert!(ensure_https("https://api.sgroup.qq.com/v1").is_ok());
}

#[test]
fn ensure_https_rejects_http_and_other_schemes() {
    assert!(ensure_https("http://example.com").is_err());
    assert!(ensure_https("ws://example.com").is_err());
    assert!(ensure_https("ftp://example.com").is_err());
    assert!(ensure_https("").is_err());
    assert!(ensure_https("example.com").is_err());
}

#[test]
fn api_base_and_auth_url_are_https_constants() {
    assert!(QQ_API_BASE.starts_with("https://"));
    assert!(QQ_AUTH_URL.starts_with("https://"));
}

#[test]
fn new_constructor_stores_fields() {
    let ch = QQChannel::new("a".into(), "b".into(), vec!["u1".into()]);
    assert_eq!(ch.app_id, "a");
    assert_eq!(ch.app_secret, "b");
    assert_eq!(ch.allowed_users, vec!["u1".to_string()]);
}

#[tokio::test]
async fn oauth_cache_gateway_and_send_preserve_auth_and_routing() {
    use super::super::http_test_support::HttpsServer;
    let send_ids = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let response_ids = send_ids.clone();
    let server = HttpsServer::start(move |request| {
        if request.target == "/app/getAppAccessToken" {
            assert_eq!(request.method, "POST");
            assert_eq!(
                request.body,
                json!({"appId": "id", "clientSecret": "secret"})
            );
            return (
                200,
                json!({"access_token": "token", "expires_in": "7200"}).to_string(),
            );
        }
        assert!(request.headers.contains("authorization: QQBot token"));
        if request.target == "/gateway" {
            assert_eq!(request.method, "GET");
            return (200, json!({"url": "ws://localhost/gateway"}).to_string());
        }
        assert_eq!(request.method, "POST");
        assert_eq!(request.body, json!({"content": "hello", "msg_type": 0}));
        if request.target.contains("blocked") {
            (403, "denied".into())
        } else {
            let id = response_ids.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            (200, json!({"id": format!("qq-message-{id}")}).to_string())
        }
    })
    .await;
    let channel =
        QQChannel::with_http_client("id".into(), "secret".into(), vec![], server.client.clone());
    assert_eq!(channel.get_token().await.unwrap(), "token");
    assert_eq!(channel.get_token().await.unwrap(), "token");
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    *channel.token_cache.write().await = Some(("expired".into(), 0));
    assert_eq!(channel.get_token().await.unwrap(), "token");
    assert!(crate::provider_test_support::is_healthy(&channel).await);
    assert_eq!(
        channel.get_gateway_url("token").await.unwrap(),
        "ws://localhost/gateway"
    );
    for (index, recipient) in ["group:room", "user:alice", "bob"].into_iter().enumerate() {
        let outcome =
            crate::provider_test_support::send_v1(&channel, &SendMessage::new("hello", recipient))
                .await
                .unwrap();
        assert_eq!(
            outcome.receipt.primary_platform_message_id.as_deref(),
            Some(format!("qq-message-{}", index + 1).as_str())
        );
    }
    let denied =
        crate::provider_test_support::send_v1(&channel, &SendMessage::new("hello", "user:blocked"))
            .await
            .unwrap_err()
            .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
            .expect("typed QQ errors retain their structured classification")
            .0
            .clone();
    assert_eq!(denied.kind, crate::channel::SendErrorKind::Forbidden);
    assert!(!denied.sent_before_error);
    assert!(!denied.message.contains("denied"));
    let requests = server.requests.lock().unwrap();
    for target in [
        "/v2/groups/room/messages",
        "/v2/users/alice/messages",
        "/v2/users/bob/messages",
    ] {
        assert!(requests.iter().any(|r| r.target == target));
    }
}

#[tokio::test]
async fn oauth_and_gateway_reject_invalid_responses_and_network_failures() {
    use super::super::http_test_support::HttpsServer;
    for (status, body) in [(403, "denied"), (200, "invalid json"), (200, "{}")] {
        let server = HttpsServer::start(move |_| (status, body.into())).await;
        let channel = QQChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec![],
            server.client.clone(),
        );
        assert!(channel.fetch_access_token().await.is_err());
        assert!(!crate::provider_test_support::is_healthy(&channel).await);
        assert!(channel.get_gateway_url("token").await.is_err());
    }
    for (expires, lifetime) in [
        (json!(123), 7140),
        (json!("invalid"), 7140),
        (json!("20"), 0),
    ] {
        let server = HttpsServer::start(move |_| {
            (
                200,
                json!({"access_token": "token", "expires_in": expires}).to_string(),
            )
        })
        .await;
        let channel = QQChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec![],
            server.client.clone(),
        );
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (token, expiry) = channel.fetch_access_token().await.unwrap();
        assert_eq!(token, "token");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!((before + lifetime..=now + lifetime).contains(&expiry));
    }
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:0").unwrap())
        .build()
        .unwrap();
    let channel = QQChannel::with_http_client("id".into(), "secret".into(), vec![], client);
    assert!(!crate::provider_test_support::is_healthy(&channel).await);
    assert!(channel.get_gateway_url("token").await.is_err());
    assert!(
        crate::provider_test_support::send_v1(&channel, &SendMessage::new("hello", "alice"))
            .await
            .is_err()
    );
    *channel.token_cache.write().await = Some(("token".into(), u64::MAX));
    assert!(
        crate::provider_test_support::send_v1(&channel, &SendMessage::new("hello", "alice"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn dedup_evicts_half_at_capacity_and_remembers_the_new_message() {
    let channel = QQChannel::new("id".into(), "secret".into(), vec![]);
    channel
        .dedup
        .write()
        .await
        .extend((0..DEDUP_CAPACITY).map(|i| format!("message-{i}")));
    assert!(!channel.is_duplicate("new").await);
    assert_eq!(channel.dedup.read().await.len(), DEDUP_CAPACITY / 2 + 1);
    assert!(channel.is_duplicate("new").await);
}

#[tokio::test]
async fn gateway_listener_identifies_heartbeats_filters_and_routes_private_and_group_messages() {
    use super::super::http_test_support::HttpsServer;
    // Exercise explicit reconnect, invalid-session, close, and both closed-receiver routes.
    for finish in [7, 9, -1, -2, -3] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let gateway = HttpsServer::start(move |request| {
            if request.target == "/app/getAppAccessToken" {
                (200, json!({"access_token": "token"}).to_string())
            } else {
                (200, json!({"url": format!("ws://{address}/")}).to_string())
            }
        })
        .await;
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let websocket = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let identify: serde_json::Value =
                serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
            assert_eq!(identify["op"], 2);
            assert_eq!(identify["d"]["token"], "QQBot token");
            assert_eq!(identify["d"]["intents"], (1_u64 << 25) | (1_u64 << 30));
            // The timer immediately emits a heartbeat with no sequence.
            let heartbeat: serde_json::Value =
                serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
            assert_eq!(heartbeat, json!({"op": 1, "d": null}));
            ws.send(Message::Text(json!({"op": 1, "s": 42}).to_string().into()))
                .await
                .unwrap();
            let heartbeat: serde_json::Value =
                serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
            assert_eq!(heartbeat, json!({"op": 1, "d": 42}));
            ws.send(Message::Binary(vec![1].into())).await.unwrap();
            ws.send(Message::Text("invalid json".into())).await.unwrap();
            for frame in [
                json!({"op": 11}),
                json!({"op": 0}),
                json!({"op": 0, "t": "OTHER", "d": {}}),
                json!({"t": "C2C_MESSAGE_CREATE", "d": {"id": "blank", "content": "   "}}),
                json!({"t": "C2C_MESSAGE_CREATE", "d": {"id": "blocked", "content": "ignore", "author": {"user_openid": "mallory"}}}),
                json!({"t": "GROUP_AT_MESSAGE_CREATE", "d": {"id": "group-blank"}}),
                json!({"t": "GROUP_AT_MESSAGE_CREATE", "d": {"id": "group-blocked", "content": "ignore"}}),
                json!({"t": "C2C_MESSAGE_CREATE", "d": {"id": "private", "content": " first ", "author": {"id": "alice"}}}),
                json!({"t": "C2C_MESSAGE_CREATE", "d": {"id": "private", "content": "duplicate", "author": {"user_openid": "alice"}}}),
                json!({"t": "GROUP_AT_MESSAGE_CREATE", "d": {"id": "group", "content": "second", "group_openid": "room", "author": {"member_openid": "alice"}}}),
                json!({"t": "GROUP_AT_MESSAGE_CREATE", "d": {"id": "group", "content": "duplicate", "author": {"member_openid": "alice"}}}),
            ] {
                ws.send(Message::Text(frame.to_string().into()))
                    .await
                    .unwrap();
            }
            release_rx.await.unwrap();
            let frame = match finish {
                -1 => {
                    ws.close(None).await.unwrap();
                    return;
                }
                -2 => {
                    json!({"t": "C2C_MESSAGE_CREATE", "d": {"id": "closed", "content": "closed", "author": {"user_openid": "alice"}}})
                }
                -3 => {
                    json!({"t": "GROUP_AT_MESSAGE_CREATE", "d": {"id": "closed", "content": "closed", "group_openid": "room", "author": {"member_openid": "alice"}}})
                }
                op => json!({"op": op}),
            };
            ws.send(Message::Text(frame.to_string().into()))
                .await
                .unwrap();
            // Keep the transport alive until the listener handles the final frame.
            let _ = ws.next().await;
        });
        let channel = QQChannel::with_http_client(
            "id".into(),
            "secret".into(),
            vec!["alice".into()],
            gateway.client.clone(),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let listening =
            tokio::spawn(
                async move { crate::provider_test_support::listen_v1(&channel, tx).await },
            );
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
                ("alice", "user:alice", "first", "qq")
            );
            assert_eq!(
                (
                    second.sender.as_str(),
                    second.reply_target.as_str(),
                    second.content.as_str()
                ),
                ("alice", "group:room", "second")
            );
            assert_eq!(first.id, "private");
            assert_eq!(second.id, "group");
            assert!(first.timestamp > 0);
            drop(rx);
            release_tx.send(()).unwrap();
            let error = tokio::time::timeout(std::time::Duration::from_secs(2), listening)
                .await
                .unwrap_or_else(|_| panic!("QQ listener did not finish for close case {finish}"))
                .unwrap()
                .unwrap_err();
            let error = error
                .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
                .expect("typed listener failures retain their structured classification");
            assert_eq!(error.0.kind, crate::channel::SendErrorKind::Transient);
            assert!(error.0.retryable);
            tokio::time::timeout(std::time::Duration::from_secs(2), websocket)
                .await
                .unwrap_or_else(|_| {
                    panic!("QQ mock websocket did not finish for close case {finish}")
                })
                .unwrap();
        })
        .await
        .unwrap();
    }
}
