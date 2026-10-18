use super::*;
use crate::providers::discord::support::*;
use serde_json::json;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn nonces_are_stable_short_and_distinct_per_message() {
    let first = nonce("key", 0).unwrap();
    assert_eq!(first, nonce("key", 0).unwrap());
    assert_eq!(first.len(), 24);
    assert_ne!(first, nonce("key", 1).unwrap());
    assert_ne!(first, nonce("other", 0).unwrap());
    assert_eq!(nonce("", 0), None);
}

#[test]
fn pre_dispatch_and_unsupported_errors_never_claim_a_send() {
    let rejected = pre_dispatch("nope");
    assert!(!rejected.sent_before_error && !rejected.retryable && !rejected.unsupported);
    let missing = unsupported("no");
    assert!(missing.unsupported && !missing.sent_before_error && !missing.retryable);
    assert_eq!(DiscordError(rejected).to_string(), "nope");
}

#[test]
fn status_codes_map_to_the_shared_error_taxonomy() {
    use reqwest::StatusCode as S;
    let limited = error_for_status(
        "send",
        S::TOO_MANY_REQUESTS,
        None,
        r#"{"retry_after": 2.5}"#,
    );
    assert_eq!(limited.kind, SendErrorKind::RateLimited);
    assert!(limited.retryable && !limited.sent_before_error);
    assert_eq!(limited.retry_after, Some(2.5));
    // The header is the fallback when the body has no retry_after.
    let header = error_for_status("send", S::TOO_MANY_REQUESTS, Some(4.0), "");
    assert_eq!(header.retry_after, Some(4.0));

    let rejected = error_for_status("send", S::UNAUTHORIZED, None, "401 Unauthorized");
    assert_eq!(rejected.kind, SendErrorKind::Forbidden);
    let text = rejected.message.to_lowercase();
    assert!(
        !(text.contains("401") && text.contains("unauthorized")),
        "must not trip the host's session-expiry classifier"
    );
    assert_eq!(
        error_for_status("send", S::FORBIDDEN, None, "").kind,
        SendErrorKind::Forbidden
    );

    let channel = error_for_status("send", S::NOT_FOUND, None, r#"{"code": 10003}"#);
    assert_eq!(channel.kind, SendErrorKind::NotFound);
    assert!(channel.chat_level_not_found);
    let message = error_for_status("edit", S::NOT_FOUND, None, r#"{"code": 10008}"#);
    assert_eq!(message.kind, SendErrorKind::NotFound);
    assert!(!message.chat_level_not_found);

    let long = error_for_status(
        "send",
        S::BAD_REQUEST,
        None,
        r#"{"code": 50035, "errors": {"content": "Must be 2000 or fewer in length."}}"#,
    );
    assert_eq!(long.kind, SendErrorKind::TooLong);
    assert_eq!(
        error_for_status("send", S::BAD_REQUEST, None, "{}").kind,
        SendErrorKind::BadFormat
    );

    let server_error = error_for_status("send", S::BAD_GATEWAY, None, "x".repeat(900).as_str());
    assert_eq!(server_error.kind, SendErrorKind::Transient);
    assert!(server_error.retryable && server_error.sent_before_error);
    assert!(server_error.message.len() < 500, "bodies are bounded");

    let other = error_for_status("send", S::IM_A_TEAPOT, None, "");
    assert_eq!(other.kind, SendErrorKind::Unknown);
    assert!(!other.retryable);
}

#[tokio::test]
async fn endpoint_urls_encode_segments_and_keep_the_base_path() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let url = channel
        .endpoint_url(&["channels", "1", "reactions", "👍", "@me"])
        .unwrap();
    assert_eq!(url.path(), "/channels/1/reactions/%F0%9F%91%8D/@me");

    let prefixed = DiscordChannel::new("t".into(), None, None, vec![], false, false)
        .with_api_base("http://h.invalid/api/v10/");
    assert_eq!(
        prefixed.endpoint_url(&["channels", "9"]).unwrap().as_str(),
        "http://h.invalid/api/v10/channels/9"
    );
    // A segment cannot climb out of its slot.
    let url = channel.endpoint_url(&["channels", "../x"]).unwrap();
    assert_eq!(url.path(), "/channels/..%2Fx");

    let broken = DiscordChannel::new("t".into(), None, None, vec![], false, false)
        .with_api_base("not a url");
    assert!(broken.endpoint_url(&["a"]).is_err());
    let opaque = DiscordChannel::new("t".into(), None, None, vec![], false, false)
        .with_api_base("mailto:x@y");
    assert!(opaque.endpoint_url(&["a"]).is_err());
}

#[tokio::test]
async fn call_returns_null_for_empty_bodies_and_errors_for_garbage() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>"))
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let url = channel.endpoint_url(&["x"]).unwrap();

    let empty = channel
        .call(
            reqwest::Method::DELETE,
            url.clone(),
            Payload::None,
            "delete",
            true,
        )
        .await
        .unwrap();
    assert_eq!(empty, serde_json::Value::Null);

    let garbage = channel
        .call(reqwest::Method::GET, url, Payload::None, "get", true)
        .await
        .unwrap_err();
    assert!(garbage.sent_before_error && !garbage.retryable);
    assert!(garbage.message.contains("unreadable"));
}

#[tokio::test]
async fn call_reports_whether_a_transport_failure_may_have_reached_discord() {
    // Nothing listens on the port: the connection never opens.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let channel =
        DiscordChannel::new("t".into(), None, None, vec![], false, false).with_api_base(dead);
    let url = channel.endpoint_url(&["x"]).unwrap();

    let refused = channel
        .call(
            reqwest::Method::POST,
            url,
            Payload::Json(json!({})),
            "send",
            false,
        )
        .await
        .unwrap_err();

    assert_eq!(refused.kind, SendErrorKind::Transient);
    assert!(refused.retryable && !refused.sent_before_error);
}

#[tokio::test]
async fn a_bad_attachment_content_type_never_leaves_the_process() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let url = channel.endpoint_url(&["x"]).unwrap();
    let error = channel
        .call(
            reqwest::Method::POST,
            url,
            Payload::Multipart {
                payload: json!({}),
                uploads: vec![Upload {
                    file_name: "f".into(),
                    content_type: "not a mime".into(),
                    bytes: vec![1],
                }],
            },
            "send",
            false,
        )
        .await
        .unwrap_err();
    assert!(error.message.contains("content type"));
    assert!(!error.sent_before_error);
    assert!(requests(&server, "POST").await.is_empty());
}
