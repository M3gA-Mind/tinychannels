//! REST and webhook tests for the Lark channel. A local TLS fixture answers
//! the Feishu/Lark API hosts; the webhook receiver is served on loopback.

use super::*;
use crate::providers::http_test_support::{HttpsServer, Request};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Hang guard only; nothing below waits on it in a healthy run.
const GUARD: Duration = Duration::from_secs(10);

/// A client that sends both regions' API hosts to the fixture. The fixture's
/// certificate does not name them, so verification is disabled for this
/// loopback-only test client.
fn fixture_client(server: &HttpsServer) -> reqwest::Client {
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

fn is_token_request(request: &Request) -> bool {
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

fn token_ok(token: &str) -> (u16, String) {
    (
        200,
        serde_json::json!({"code": 0, "tenant_access_token": token}).to_string(),
    )
}

#[tokio::test]
async fn send_fetches_a_token_once_and_posts_a_text_message() {
    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t-1")
        } else {
            (200, r#"{"code":0}"#.into())
        }
    })
    .await;
    let channel = channel_with(&server, true);

    channel
        .send(&SendMessage::new("hello lark", "oc_chat"))
        .await
        .unwrap();
    channel
        .send(&SendMessage::new("second", "oc_chat"))
        .await
        .unwrap();
    assert!(channel.health_check().await, "cached token stays healthy");

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
    assert_eq!(send.target, "/open-apis/im/v1/messages?receive_id_type=chat_id");
    assert_eq!(header(send, "authorization"), Some("Bearer t-1"));
    assert_eq!(
        header(send, "content-type"),
        Some("application/json; charset=utf-8")
    );
    assert_eq!(send.body["receive_id"], "oc_chat");
    assert_eq!(send.body["msg_type"], "text");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(send.body["content"].as_str().unwrap())
            .unwrap(),
        serde_json::json!({"text": "hello lark"})
    );
    assert_eq!(header(&requests[2], "authorization"), Some("Bearer t-1"));
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
                (401, r#"{"code":99991663}"#.into())
            } else {
                (200, r#"{"code":0}"#.into())
            }
        }
    })
    .await;
    let channel = channel_with(&server, false);

    channel
        .send(&SendMessage::new("retry me", "oc_chat"))
        .await
        .unwrap();

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
async fn send_reports_failures_with_and_without_a_token_refresh() {
    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t")
        } else {
            (401, "still unauthorized".into())
        }
    })
    .await;
    let error = channel_with(&server, true)
        .send(&SendMessage::new("x", "oc_chat"))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Lark send failed after token refresh: still unauthorized"
    );

    let server = HttpsServer::start(|request| {
        if is_token_request(request) {
            token_ok("t")
        } else {
            (500, "upstream exploded".into())
        }
    })
    .await;
    let error = channel_with(&server, true)
        .send(&SendMessage::new("x", "oc_chat"))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "Lark send failed: upstream exploded");
}

#[tokio::test]
async fn token_errors_fail_send_and_health_check() {
    let server = HttpsServer::start(|_| {
        (
            200,
            r#"{"code":10003,"msg":"invalid app_secret"}"#.into(),
        )
    })
    .await;
    let channel = channel_with(&server, true);
    assert!(!channel.health_check().await);
    let error = channel
        .send(&SendMessage::new("x", "oc_chat"))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Lark tenant_access_token failed: invalid app_secret"
    );

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
    let server = HttpsServer::start(|_| (200, r#"{"code":1000040345,"msg":"bad app"}"#.into()))
        .await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let error = channel_with(&server, true).listen(tx).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "Lark WS endpoint failed: code=1000040345 msg=bad app"
    );
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests[0].target, "/callback/ws/endpoint");
    assert_eq!(header(&requests[0], "locale"), Some("zh"));
    assert_eq!(
        requests[0].body,
        serde_json::json!({"AppID": "cli_app", "AppSecret": "app-secret"})
    );

    let server = HttpsServer::start(|_| (200, r#"{"code":1}"#.into())).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let error = channel_with(&server, false).listen(tx).await.unwrap_err();
    assert_eq!(error.to_string(), "Lark WS endpoint failed: code=1 msg=(none)");
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(header(&requests[0], "locale"), Some("en"));

    let server = HttpsServer::start(|_| (200, r#"{"code":0}"#.into())).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let error = channel_with(&server, true).listen(tx).await.unwrap_err();
    assert_eq!(error.to_string(), "Lark WS endpoint: empty data");
}

fn webhook_event(open_id: &str, text: &str) -> serde_json::Value {
    serde_json::json!({
        "header": { "event_type": "im.message.receive_v1", "token": "verify-token" },
        "event": {
            "sender": { "sender_id": { "open_id": open_id } },
            "message": {
                "message_type": "text",
                "content": serde_json::json!({"text": text}).to_string(),
                "chat_id": "oc_chat"
            }
        }
    })
}

#[tokio::test]
async fn webhook_mode_requires_a_port() {
    let mut channel = LarkChannel::new(
        "a".into(),
        "s".into(),
        "verify-token".into(),
        None,
        vec![],
    );
    channel.receive_mode = crate::config::LarkReceiveMode::Webhook;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let error = channel.listen(tx).await.unwrap_err();
    assert!(error.to_string().contains("requires `port`"), "{error}");
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
    let server = tokio::spawn(async move { channel.listen(tx).await });

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
                assert!(
                    Instant::now() < deadline,
                    "webhook never came up: {error}"
                );
                tokio::task::yield_now().await;
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
        tx,
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
