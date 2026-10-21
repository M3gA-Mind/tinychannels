use super::*;
use crate::channel::ChannelTyping;
use crate::traits::SendMessage;
use serde_json::json;

// Helper: create a channel with mention_only=false (legacy behavior).
fn make_channel(allowed: Vec<String>, thread_replies: bool) -> MattermostChannel {
    MattermostChannel::new(
        "url".into(),
        "token".into(),
        None,
        allowed,
        thread_replies,
        false,
    )
}

// Helper: create a channel with mention_only=true.
fn make_mention_only_channel() -> MattermostChannel {
    MattermostChannel::new(
        "url".into(),
        "token".into(),
        None,
        vec!["*".into()],
        true,
        true,
    )
}

#[test]
fn mattermost_url_trimming() {
    let ch = MattermostChannel::new(
        "https://mm.example.com/".into(),
        "token".into(),
        None,
        vec![],
        false,
        false,
    );
    assert_eq!(ch.base_url, "https://mm.example.com");
}

#[test]
fn mattermost_allowlist_wildcard() {
    let ch = make_channel(vec!["*".into()], false);
    assert!(ch.is_user_allowed("any-id"));
}

#[test]
fn mattermost_parse_post_basic() {
    let ch = make_channel(vec!["*".into()], true);
    let post = json!({
        "id": "post123",
        "user_id": "user456",
        "message": "hello world",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789")
        .unwrap();
    assert_eq!(msg.sender, "user456");
    assert_eq!(msg.content, "hello world");
    assert_eq!(msg.reply_target, "chan789:post123"); // Default threaded reply
}

#[test]
fn mattermost_accepts_file_only_posts_for_typed_ingress() {
    let channel = make_channel(vec!["*".into()], true);
    let post = json!({
        "id": "post-media-only",
        "user_id": "user456",
        "message": "",
        "create_at": 1_600_000_000_000_i64,
        "root_id": "",
        "file_ids": ["file-1"]
    });
    let message = channel
        .parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789")
        .unwrap();
    assert!(message.content.is_empty());
    assert_eq!(message.reply_target, "chan789:post-media-only");
}

#[test]
fn mattermost_parse_post_thread_replies_enabled() {
    let ch = make_channel(vec!["*".into()], true);
    let post = json!({
        "id": "post123",
        "user_id": "user456",
        "message": "hello world",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789")
        .unwrap();
    assert_eq!(msg.reply_target, "chan789:post123"); // Threaded reply
}

#[test]
fn mattermost_parse_post_thread() {
    let ch = make_channel(vec!["*".into()], false);
    let post = json!({
        "id": "post123",
        "user_id": "user456",
        "message": "reply",
        "create_at": 1_600_000_000_000_i64,
        "root_id": "root789"
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789")
        .unwrap();
    assert_eq!(msg.reply_target, "chan789:root789"); // Stays in the thread
}

#[test]
fn mattermost_parse_post_ignore_self() {
    let ch = make_channel(vec!["*".into()], false);
    let post = json!({
        "id": "post123",
        "user_id": "bot123",
        "message": "my own message",
        "create_at": 1_600_000_000_000_i64
    });

    let msg =
        ch.parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789");
    assert!(msg.is_none());
}

#[test]
fn mattermost_parse_post_ignore_old() {
    let ch = make_channel(vec!["*".into()], false);
    let post = json!({
        "id": "post123",
        "user_id": "user456",
        "message": "old message",
        "create_at": 1_400_000_000_000_i64
    });

    let msg =
        ch.parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789");
    assert!(msg.is_none());
}

#[test]
fn mattermost_parse_post_no_thread_when_disabled() {
    let ch = make_channel(vec!["*".into()], false);
    let post = json!({
        "id": "post123",
        "user_id": "user456",
        "message": "hello world",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789")
        .unwrap();
    assert_eq!(msg.reply_target, "chan789"); // No thread suffix
}

#[test]
fn mattermost_existing_thread_always_threads() {
    // Even with thread_replies=false, replies to existing threads stay in the thread
    let ch = make_channel(vec!["*".into()], false);
    let post = json!({
        "id": "post123",
        "user_id": "user456",
        "message": "reply in thread",
        "create_at": 1_600_000_000_000_i64,
        "root_id": "root789"
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "botname", 1_500_000_000_000_i64, "chan789")
        .unwrap();
    assert_eq!(msg.reply_target, "chan789:root789"); // Stays in existing thread
}

// ── mention_only tests ────────────────────────────────────────

#[test]
fn mention_only_skips_message_without_mention() {
    let ch = make_mention_only_channel();
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "hello everyone",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch.parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1");
    assert!(msg.is_none());
}

#[test]
fn mention_only_accepts_message_with_at_mention() {
    let ch = make_mention_only_channel();
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "@mybot what is the weather?",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1")
        .unwrap();
    assert_eq!(msg.content, "what is the weather?");
}

#[test]
fn mention_only_strips_mention_and_trims() {
    let ch = make_mention_only_channel();
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "  @mybot  run status  ",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1")
        .unwrap();
    assert_eq!(msg.content, "run status");
}

#[test]
fn mention_only_rejects_empty_after_stripping() {
    let ch = make_mention_only_channel();
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "@mybot",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch.parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1");
    assert!(msg.is_none());
}

#[test]
fn mention_only_case_insensitive() {
    let ch = make_mention_only_channel();
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "@MyBot hello",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1")
        .unwrap();
    assert_eq!(msg.content, "hello");
}

#[test]
fn mention_only_detects_metadata_mentions() {
    // Even without @username in text, metadata.mentions should trigger.
    let ch = make_mention_only_channel();
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "hey check this out",
        "create_at": 1_600_000_000_000_i64,
        "root_id": "",
        "metadata": {
            "mentions": ["bot123"]
        }
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1")
        .unwrap();
    // Content is preserved as-is since no @username was in the text to strip.
    assert_eq!(msg.content, "hey check this out");
}

#[test]
fn mention_only_word_boundary_prevents_partial_match() {
    let ch = make_mention_only_channel();
    // "@mybotextended" should NOT match "@mybot" because it extends the username.
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "@mybotextended hello",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch.parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1");
    assert!(msg.is_none());
}

#[test]
fn mention_only_mention_in_middle_of_text() {
    let ch = make_mention_only_channel();
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "hey @mybot how are you?",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1")
        .unwrap();
    assert_eq!(msg.content, "hey   how are you?");
}

#[test]
fn mention_only_disabled_passes_all_messages() {
    // With mention_only=false (default), messages pass through unfiltered.
    let ch = make_channel(vec!["*".into()], true);
    let post = json!({
        "id": "post1",
        "user_id": "user1",
        "message": "no mention here",
        "create_at": 1_600_000_000_000_i64,
        "root_id": ""
    });

    let msg = ch
        .parse_mattermost_post(&post, "bot123", "mybot", 1_500_000_000_000_i64, "chan1")
        .unwrap();
    assert_eq!(msg.content, "no mention here");
}

// ── contains_bot_mention_mm unit tests ────────────────────────

#[test]
fn contains_mention_text_at_end() {
    let post = json!({});
    assert!(contains_bot_mention_mm(
        "hello @mybot",
        "bot123",
        "mybot",
        &post
    ));
}

#[test]
fn contains_mention_text_at_start() {
    let post = json!({});
    assert!(contains_bot_mention_mm(
        "@mybot hello",
        "bot123",
        "mybot",
        &post
    ));
}

#[test]
fn contains_mention_text_alone() {
    let post = json!({});
    assert!(contains_bot_mention_mm("@mybot", "bot123", "mybot", &post));
}

#[test]
fn no_mention_different_username() {
    let post = json!({});
    assert!(!contains_bot_mention_mm(
        "@otherbot hello",
        "bot123",
        "mybot",
        &post
    ));
}

#[test]
fn no_mention_partial_username() {
    let post = json!({});
    // "mybot" is a prefix of "mybotx" — should NOT match
    assert!(!contains_bot_mention_mm(
        "@mybotx hello",
        "bot123",
        "mybot",
        &post
    ));
}

#[test]
fn mention_detects_later_valid_mention_after_partial_prefix() {
    let post = json!({});
    assert!(contains_bot_mention_mm(
        "@mybotx ignore this, but @mybot handle this",
        "bot123",
        "mybot",
        &post
    ));
}

#[test]
fn mention_followed_by_punctuation() {
    let post = json!({});
    // "@mybot," — comma is not alphanumeric/underscore/dash/dot, so it's a boundary
    assert!(contains_bot_mention_mm(
        "@mybot, hello",
        "bot123",
        "mybot",
        &post
    ));
}

#[test]
fn mention_via_metadata_only() {
    let post = json!({
        "metadata": { "mentions": ["bot123"] }
    });
    assert!(contains_bot_mention_mm(
        "no at mention",
        "bot123",
        "mybot",
        &post
    ));
}

#[test]
fn no_mention_empty_username_no_metadata() {
    let post = json!({});
    assert!(!contains_bot_mention_mm("hello world", "bot123", "", &post));
}

// ── normalize_mattermost_content unit tests ───────────────────

#[test]
fn normalize_strips_and_trims() {
    let post = json!({});
    let result = normalize_mattermost_content("  @mybot  do stuff  ", "bot123", "mybot", &post);
    assert_eq!(result.as_deref(), Some("do stuff"));
}

#[test]
fn normalize_returns_none_for_no_mention() {
    let post = json!({});
    let result = normalize_mattermost_content("hello world", "bot123", "mybot", &post);
    assert!(result.is_none());
}

#[test]
fn normalize_returns_none_when_only_mention() {
    let post = json!({});
    let result = normalize_mattermost_content("@mybot", "bot123", "mybot", &post);
    assert!(result.is_none());
}

#[test]
fn normalize_preserves_text_for_metadata_mention() {
    let post = json!({
        "metadata": { "mentions": ["bot123"] }
    });
    let result = normalize_mattermost_content("check this out", "bot123", "mybot", &post);
    assert_eq!(result.as_deref(), Some("check this out"));
}

#[test]
fn normalize_strips_multiple_mentions() {
    let post = json!({});
    let result =
        normalize_mattermost_content("@mybot hello @mybot world", "bot123", "mybot", &post);
    assert_eq!(result.as_deref(), Some("hello   world"));
}

#[test]
fn normalize_keeps_partial_username_mentions() {
    let post = json!({});
    let result =
        normalize_mattermost_content("@mybot hello @mybotx world", "bot123", "mybot", &post);
    assert_eq!(result.as_deref(), Some("hello @mybotx world"));
}

fn mock_channel(base_url: String) -> MattermostChannel {
    MattermostChannel::with_http_client(
        base_url,
        "test-token".into(),
        Some("room".into()),
        vec!["alice".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
}

#[tokio::test]
async fn rest_requests_preserve_auth_thread_routing_and_server_errors() {
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let channel = mock_channel(format!("{}/", server.uri()));
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "bot-id", "username": "bot-name"
        })))
        .expect(2)
        .mount(&server)
        .await;
    assert_eq!(
        channel.get_bot_identity().await,
        ("bot-id".into(), "bot-name".into())
    );
    assert!(crate::provider_test_support::is_healthy(&channel).await);
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(
            json!({"channel_id": "room", "root_id": "root", "message": "reply"}),
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "reply-post"})))
        .expect(1)
        .mount(&server)
        .await;
    crate::provider_test_support::send_v1(&channel, &SendMessage::new("reply", "room:root"))
        .await
        .unwrap();
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .and(body_json(
            json!({"channel_id": "room", "message": "top-level"}),
        ))
        .respond_with(ResponseTemplate::new(403).set_body_string("permission denied"))
        .expect(1)
        .mount(&server)
        .await;
    let error =
        crate::provider_test_support::send_v1(&channel, &SendMessage::new("top-level", "room"))
            .await
            .unwrap_err();
    let error = error
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .expect("typed send errors preserve their structured classification")
        .0
        .clone();
    assert_eq!(error.kind, crate::channel::SendErrorKind::Forbidden);
    assert!(!error.sent_before_error);
    assert!(!error.message.contains("permission denied"));
    server.verify().await;
    server.reset().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;
    assert_eq!(
        channel.get_bot_identity().await,
        (String::new(), String::new())
    );
    assert!(!crate::provider_test_support::is_healthy(&channel).await);
    let unreachable = mock_channel("http://127.0.0.1:0".into());
    assert_eq!(
        unreachable.get_bot_identity().await,
        (String::new(), String::new())
    );
    assert!(!crate::provider_test_support::is_healthy(&unreachable).await);
    assert!(
        crate::provider_test_support::send_v1(&unreachable, &SendMessage::new("hello", "room"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typing_replaces_previous_loop_and_stops_cleanly() {
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let channel = mock_channel(server.uri());
    for (recipient, body) in [
        (
            "room:root",
            json!({"channel_id": "room", "parent_id": "root"}),
        ),
        ("room", json!({"channel_id": "room"})),
    ] {
        server.reset().await;
        Mock::given(method("POST"))
            .and(path("/api/v4/users/me/typing"))
            .and(header("authorization", "Bearer test-token"))
            .and(body_json(body))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        channel.start_typing(recipient).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while server.received_requests().await.unwrap().is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        server.verify().await;
    }
    channel.stop_typing("room").await.unwrap();
    assert!(channel.typing_handle.lock().is_none());
    channel.stop_typing("room").await.unwrap();
}

#[tokio::test]
async fn listener_recovers_from_invalid_json_and_delivers_posts_in_time_order() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let channel = mock_channel(server.uri());
    Mock::given(path("/api/v4/users/me"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id": "bot", "username": "assistant"})),
        )
        .mount(&server)
        .await;
    let polls = AtomicUsize::new(0);
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/room/posts"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(move |_: &wiremock::Request| {
            match polls.fetch_add(1, Ordering::SeqCst) {
                0 => ResponseTemplate::new(200).set_body_string("invalid json"),
                1 => ResponseTemplate::new(200).set_body_json(json!({})),
                _ => ResponseTemplate::new(200).set_body_json(json!({"posts": {
                    "later": {"id": "later", "user_id": "alice", "message": "second", "create_at": i64::MAX - 1},
                    "earlier": {"id": "earlier", "user_id": "alice", "message": "first", "create_at": i64::MAX - 2},
                    "blocked": {"id": "blocked", "user_id": "mallory", "message": "ignore", "create_at": i64::MAX - 3}
                }})),
            }
        })
        .mount(&server).await;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener =
        tokio::spawn(async move { crate::provider_test_support::listen_v1(&channel, tx).await });
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!((first.content.as_str(), first.reply_target.as_str()), ("first", "room"));
        assert_eq!(first.thread_ts.as_deref(), Some("earlier"));
        assert_eq!((second.content.as_str(), second.reply_target.as_str()), ("second", "room"));
        assert_eq!(second.thread_ts.as_deref(), Some("later"));
        // A newer post on the next poll must terminate listening when the receiver closes.
        server.reset().await;
        Mock::given(path("/api/v4/channels/room/posts"))
            .and(query_param("since", (i64::MAX - 1).to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"posts": {
                "last": {"id": "last", "user_id": "alice", "message": "closed", "create_at": i64::MAX}
            }})))
            .expect(1).mount(&server).await;
        drop(rx);
        let error = listener.await.unwrap().unwrap_err();
        let error = error
            .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
            .expect("typed sink rejection remains a structured listener error");
        assert_eq!(error.0.kind, crate::channel::SendErrorKind::Transient);
        assert!(error.0.retryable);
        server.verify().await;
    }).await.unwrap();
}
