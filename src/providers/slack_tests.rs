use super::*;

#[test]
fn slack_channel_name() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec![]);
    assert_eq!(ch.name(), "slack");
}

#[test]
fn slack_channel_with_channel_id() {
    let ch = SlackChannel::new("xoxb-fake".into(), Some("C12345".into()), vec![]);
    assert_eq!(ch.channel_id, Some("C12345".to_string()));
}

#[test]
fn empty_allowlist_denies_everyone() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec![]);
    assert!(!ch.is_user_allowed("U12345"));
    assert!(!ch.is_user_allowed("anyone"));
}

#[test]
fn wildcard_allows_everyone() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec!["*".into()]);
    assert!(ch.is_user_allowed("U12345"));
}

#[test]
fn specific_allowlist_filters() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec!["U111".into(), "U222".into()]);
    assert!(ch.is_user_allowed("U111"));
    assert!(ch.is_user_allowed("U222"));
    assert!(!ch.is_user_allowed("U333"));
}

#[test]
fn allowlist_exact_match_not_substring() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec!["U111".into()]);
    assert!(!ch.is_user_allowed("U1111"));
    assert!(!ch.is_user_allowed("U11"));
}

#[test]
fn allowlist_empty_user_id() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec!["U111".into()]);
    assert!(!ch.is_user_allowed(""));
}

#[test]
fn allowlist_case_sensitive() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec!["U111".into()]);
    assert!(ch.is_user_allowed("U111"));
    assert!(!ch.is_user_allowed("u111"));
}

#[test]
fn allowlist_wildcard_and_specific() {
    let ch = SlackChannel::new("xoxb-fake".into(), None, vec!["U111".into(), "*".into()]);
    assert!(ch.is_user_allowed("U111"));
    assert!(ch.is_user_allowed("anyone"));
}

// Message ID edge cases.

#[test]
fn slack_message_id_format_includes_channel_and_ts() {
    // Verify that message IDs follow the format: slack_{channel_id}_{ts}
    let ts = "1234567890.123456";
    let channel_id = "C12345";
    let expected_id = format!("slack_{channel_id}_{ts}");
    assert_eq!(expected_id, "slack_C12345_1234567890.123456");
}

#[test]
fn slack_message_id_is_deterministic() {
    // Same channel_id + same ts = same ID (prevents duplicates after restart)
    let ts = "1234567890.123456";
    let channel_id = "C12345";
    let id1 = format!("slack_{channel_id}_{ts}");
    let id2 = format!("slack_{channel_id}_{ts}");
    assert_eq!(id1, id2);
}

#[test]
fn slack_message_id_different_ts_different_id() {
    // Different timestamps produce different IDs
    let channel_id = "C12345";
    let id1 = format!("slack_{channel_id}_1234567890.123456");
    let id2 = format!("slack_{channel_id}_1234567890.123457");
    assert_ne!(id1, id2);
}

#[test]
fn slack_message_id_different_channel_different_id() {
    // Different channels produce different IDs even with same ts
    let ts = "1234567890.123456";
    let id1 = format!("slack_C12345_{ts}");
    let id2 = format!("slack_C67890_{ts}");
    assert_ne!(id1, id2);
}

#[test]
fn slack_message_id_no_uuid_randomness() {
    // Verify format doesn't contain random UUID components
    let ts = "1234567890.123456";
    let channel_id = "C12345";
    let id = format!("slack_{channel_id}_{ts}");
    assert!(!id.contains('-')); // No UUID dashes
    assert!(id.starts_with("slack_"));
}

#[test]
fn inbound_thread_ts_prefers_explicit_thread_ts() {
    let msg = serde_json::json!({
        "ts": "123.002",
        "thread_ts": "123.001"
    });

    let thread_ts = SlackChannel::inbound_thread_ts(&msg, "123.002");
    assert_eq!(thread_ts.as_deref(), Some("123.001"));
}

#[test]
fn inbound_thread_ts_falls_back_to_ts() {
    let msg = serde_json::json!({
        "ts": "123.001"
    });

    let thread_ts = SlackChannel::inbound_thread_ts(&msg, "123.001");
    assert_eq!(thread_ts.as_deref(), Some("123.001"));
}

#[test]
fn inbound_thread_ts_none_when_ts_missing() {
    let msg = serde_json::json!({});

    let thread_ts = SlackChannel::inbound_thread_ts(&msg, "");
    assert_eq!(thread_ts, None);
}

#[tokio::test]
async fn api_requests_preserve_auth_threads_and_report_http_and_slack_errors() {
    use super::super::http_test_support::HttpsServer;
    use serde_json::json;

    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/api/auth.test" => (200, json!({"user_id": "bot"}).to_string()),
        "/api/chat.postMessage" => match request.body["text"].as_str().unwrap() {
            "http-error" => (403, "permission denied".into()),
            "app-error" => (
                200,
                json!({"ok": false, "error": "channel_not_found"}).to_string(),
            ),
            "unknown-error" => (200, json!({"ok": false}).to_string()),
            "invalid-json" => (200, "not json".into()),
            _ => (200, json!({"ok": true}).to_string()),
        },
        _ => panic!("unexpected request: {request:?}"),
    })
    .await;
    let channel =
        SlackChannel::with_http_client("test-token".into(), None, vec![], server.client.clone());
    assert_eq!(channel.get_bot_user_id().await.as_deref(), Some("bot"));
    assert!(channel.health_check().await);
    let mut reply = SendMessage::new("reply", "room");
    reply.thread_ts = Some("123.001".into());
    channel.send(&reply).await.unwrap();
    for (text, error) in [
        ("http-error", "permission denied"),
        ("app-error", "channel_not_found"),
        ("unknown-error", "unknown"),
    ] {
        assert!(
            channel
                .send(&SendMessage::new(text, "room"))
                .await
                .unwrap_err()
                .to_string()
                .contains(error)
        );
    }
    channel
        .send(&SendMessage::new("invalid-json", "room"))
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 7);
    assert!(
        requests
            .iter()
            .all(|r| r.headers.contains("authorization: Bearer test-token"))
    );
    assert_eq!(requests[2].method, "POST");
    assert_eq!(
        requests[2].body,
        json!({"channel": "room", "text": "reply", "thread_ts": "123.001"})
    );
    assert!(requests[3].body.get("thread_ts").is_none());
}

#[tokio::test]
async fn identity_health_and_listen_handle_unavailable_or_invalid_api_responses() {
    use super::super::http_test_support::HttpsServer;

    for body in ["invalid json", "{}"] {
        let server = HttpsServer::start(move |_| (503, body.into())).await;
        let channel =
            SlackChannel::with_http_client("token".into(), None, vec![], server.client.clone());
        assert!(channel.get_bot_user_id().await.is_none());
        assert!(!channel.health_check().await);
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        assert!(
            channel
                .listen(tx)
                .await
                .unwrap_err()
                .to_string()
                .contains("channel_id required")
        );
    }
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:0").unwrap())
        .build()
        .unwrap();
    let channel = SlackChannel::with_http_client("token".into(), None, vec![], client);
    assert!(channel.get_bot_user_id().await.is_none());
    assert!(!channel.health_check().await);
    assert!(
        channel
            .send(&SendMessage::new("hello", "room"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn history_listener_filters_messages_orders_delivery_and_advances_cursor() {
    use super::super::http_test_support::HttpsServer;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let polls = AtomicUsize::new(0);
    let server = HttpsServer::start(move |request| {
        if request.target == "/api/auth.test" {
            return (200, json!({"user_id": "bot"}).to_string());
        }
        assert!(request.target.starts_with("/api/conversations.history?"));
        match polls.fetch_add(1, Ordering::SeqCst) {
            0 => (200, "invalid json".into()),
            1 => (200, "{}".into()),
            2 => {
                assert!(!request.target.contains("oldest="));
                (
                    200,
                    json!({"messages": [
                        {"ts": "003", "user": "alice", "text": "second", "thread_ts": "001"},
                        {"ts": "002", "user": "alice", "text": "first"},
                        {"ts": "001", "user": "alice", "text": ""},
                        {"ts": "001", "user": "mallory", "text": "blocked"},
                        {"ts": "001", "user": "bot", "text": "own message"},
                        {}
                    ]})
                    .to_string(),
                )
            }
            _ => {
                assert!(request.target.contains("oldest=003"));
                (
                    200,
                    json!({"messages": [
                        {"ts": "004", "user": "alice", "text": "receiver closed"},
                        {"ts": "003", "user": "alice", "text": "duplicate"}
                    ]})
                    .to_string(),
                )
            }
        }
    })
    .await;
    let channel = SlackChannel::with_http_client(
        "token".into(),
        Some("room".into()),
        vec!["alice".into()],
        server.client.clone(),
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn(async move { channel.listen(tx).await });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(
            (
                first.id.as_str(),
                first.sender.as_str(),
                first.reply_target.as_str(),
                first.content.as_str(),
                first.channel.as_str()
            ),
            ("slack_room_002", "alice", "room", "first", "slack")
        );
        assert_eq!(first.thread_ts.as_deref(), Some("002"));
        assert_eq!(second.content, "second");
        assert_eq!(second.thread_ts.as_deref(), Some("001"));
        assert!(first.timestamp > 0);
        drop(rx);
        listener.await.unwrap().unwrap();
    })
    .await
    .unwrap();
}
