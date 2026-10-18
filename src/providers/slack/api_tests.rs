use super::*;
use crate::providers::slack::fixtures::*;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

#[tokio::test]
async fn json_calls_carry_the_bot_token_and_return_the_ok_body() {
    let server = mock_api().await;
    mount(&server, "chat.postMessage", ok(json!({"ts": "1.1"}))).await;
    let channel = channel_for(&server, &[]);
    let value = channel
        .api_json("chat.postMessage", &json!({"text": "x"}))
        .await
        .unwrap();
    assert_eq!(value["ts"], "1.1");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests[0].headers.get("authorization").unwrap(),
        "Bearer bot-test-token"
    );
}

#[tokio::test]
async fn form_calls_encode_their_fields() {
    let server = mock_api().await;
    mount(&server, "files.getUploadURLExternal", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    channel
        .api_form(
            "files.getUploadURLExternal",
            &[("filename", "a b.txt"), ("length", "3")],
        )
        .await
        .unwrap();
    let body = bodies(&server, "files.getUploadURLExternal").await;
    assert_eq!(body[0]["filename"], "a b.txt");
    assert_eq!(body[0]["length"], "3");
}

#[tokio::test]
async fn failures_are_classified_by_transport_status_body_and_slack_code() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/api/limited"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
        .mount(&server)
        .await;
    mount_status(&server, "forbidden", 403, json!({})).await;
    Mock::given(method("POST"))
        .and(path("/api/garbled"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;
    mount(
        &server,
        "refused",
        json!({"ok": false, "error": "channel_not_found"}),
    )
    .await;
    mount(&server, "anonymous", json!({"ok": false})).await;
    let channel = channel_for(&server, &[]);
    let call = |name: &'static str| {
        let channel = &channel;
        async move { channel.api_json(name, &json!({})).await.unwrap_err() }
    };
    assert_eq!(
        call("limited").await,
        ApiFailure::Http {
            status: 429,
            retry_after: Some(7.0)
        }
    );
    assert_eq!(
        call("forbidden").await,
        ApiFailure::Http {
            status: 403,
            retry_after: None
        }
    );
    assert_eq!(call("garbled").await, ApiFailure::Malformed);
    assert_eq!(
        call("refused").await,
        ApiFailure::Slack {
            code: "channel_not_found".into()
        }
    );
    assert_eq!(
        call("anonymous").await,
        ApiFailure::Slack {
            code: "unknown".into()
        }
    );
    let dead = SlackChannel::new("t".into(), None, vec![]).with_api_base("http://127.0.0.1:1/api");
    assert_eq!(
        dead.api_json("x", &json!({})).await.unwrap_err(),
        ApiFailure::Transport
    );
    assert_eq!(
        dead.api_form("x", &[]).await.unwrap_err(),
        ApiFailure::Transport
    );
}

#[test]
fn error_mapping_distinguishes_retryable_uncertain_and_permanent_failures() {
    let slack = |code: &str| send_error(&ApiFailure::Slack { code: code.into() }, "send");
    let http = |status| {
        send_error(
            &ApiFailure::Http {
                status,
                retry_after: Some(2.0),
            },
            "send",
        )
    };

    let limited = slack("ratelimited");
    assert_eq!(
        (limited.kind, limited.retryable, limited.sent_before_error),
        (SendErrorKind::RateLimited, true, false)
    );
    let http_limited = http(429);
    assert_eq!(
        (
            http_limited.kind,
            http_limited.retryable,
            http_limited.retry_after
        ),
        (SendErrorKind::RateLimited, true, Some(2.0))
    );
    for code in [
        "internal_error",
        "service_unavailable",
        "fatal_error",
        "request_timeout",
    ] {
        let error = slack(code);
        assert_eq!(
            (error.kind, error.retryable),
            (SendErrorKind::Transient, true),
            "{code}"
        );
    }
    let missing = slack("channel_not_found");
    assert!(missing.chat_level_not_found && missing.kind == SendErrorKind::NotFound);
    for code in ["message_not_found", "thread_not_found", "file_not_found"] {
        assert_eq!(slack(code).kind, SendErrorKind::NotFound);
    }
    assert_eq!(slack("msg_too_long").kind, SendErrorKind::TooLong);
    for code in [
        "no_text",
        "invalid_arguments",
        "invalid_blocks",
        "invalid_blocks_format",
        "invalid_name",
        "invalid_array_arg",
    ] {
        assert_eq!(slack(code).kind, SendErrorKind::BadFormat, "{code}");
    }
    for code in [
        "not_in_channel",
        "invalid_auth",
        "not_authed",
        "account_inactive",
        "token_revoked",
        "token_expired",
        "missing_scope",
        "is_archived",
        "cant_delete_message",
        "cant_update_message",
        "edit_window_closed",
        "restricted_action",
    ] {
        assert_eq!(slack(code).kind, SendErrorKind::Forbidden, "{code}");
    }
    assert_eq!(slack("surprise").kind, SendErrorKind::Unknown);
    assert!(!slack("surprise").sent_before_error);

    assert_eq!(http(503).kind, SendErrorKind::Transient);
    assert!(http(503).sent_before_error && !http(503).retryable);
    assert_eq!(http(401).kind, SendErrorKind::Forbidden);
    assert_eq!(http(403).kind, SendErrorKind::Forbidden);
    assert_eq!(http(404).kind, SendErrorKind::NotFound);
    assert_eq!(http(418).kind, SendErrorKind::Unknown);

    let transport = send_error(&ApiFailure::Transport, "send");
    assert!(transport.sent_before_error && !transport.retryable);
    let garbled = send_error(&ApiFailure::Malformed, "send");
    assert!(garbled.sent_before_error && garbled.message.contains("unreadable"));
    assert!(!transport.message.contains("bot-test-token"));
}

#[test]
fn unsupported_and_pre_dispatch_errors_never_claim_a_send() {
    let unsupported = unsupported("nope");
    assert!(unsupported.unsupported && !unsupported.retryable && !unsupported.sent_before_error);
    let rejected = pre_dispatch("bad");
    assert!(!rejected.unsupported && !rejected.sent_before_error);
}

#[tokio::test]
async fn the_bot_user_id_is_looked_up_once_and_cached() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let channel = channel_for(&server, &[]);
    assert_eq!(channel.bot_user_id().await.as_deref(), Some("UBOT"));
    assert_eq!(channel.bot_user_id().await.as_deref(), Some("UBOT"));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn an_unreadable_identity_lookup_yields_none() {
    let server = mock_api().await;
    let down = channel_for(&server, &[]);
    assert!(
        down.bot_user_id().await.is_none(),
        "404 body is not JSON with a user_id"
    );
    Mock::given(method("GET"))
        .and(path("/api/auth.test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    assert!(down.bot_user_id().await.is_none());
    let dead = SlackChannel::new("t".into(), None, vec![]).with_api_base("http://127.0.0.1:1/api");
    assert!(dead.bot_user_id().await.is_none());
}
