use super::*;
use crate::providers::telegram::fixtures::*;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

#[test]
fn new_with_empty_allowlist_arms_pairing_with_live_code() {
    let ch = TelegramChannel::new("tok".into(), vec![" ".into(), "@".into()], false);
    assert!(ch.pairing.is_some(), "blank entries normalize to empty list");
    assert!(ch.pairing_code_active());
    assert!(ch.allowed_users.read().unwrap().is_empty());
}

#[test]
fn new_with_allowlist_normalizes_entries_and_disables_pairing() {
    let ch = TelegramChannel::new("tok".into(), vec![" @alice ".into(), "42".into()], true);
    assert!(ch.pairing.is_none());
    assert!(!ch.pairing_code_active());
    assert_eq!(
        *ch.allowed_users.read().unwrap(),
        vec!["alice".to_string(), "42".to_string()]
    );
    assert!(ch.mention_only);
}

#[test]
fn builders_install_injected_capabilities() {
    let events = std::sync::Arc::new(RecordingEvents::default());
    let allowlist = std::sync::Arc::new(RecordingAllowlist::default());
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false)
        .with_http_client(reqwest::Client::new())
        .with_transcriber(FixedTranscriber::ok("hi"))
        .with_allowlist(allowlist)
        .with_events(events)
        .with_chat_id(Some(" 99 ".into()));
    assert!(ch.transcriber.is_some());
    assert!(ch.allowlist.is_some());
    assert!(ch.events.is_some());
    assert_eq!(ch.chat_id.as_deref(), Some("99"));
}

#[test]
fn parse_message_id_trims_and_rejects_garbage() {
    assert_eq!(TelegramChannel::parse_message_id(Some(" 17 ")), Some(17));
    assert_eq!(TelegramChannel::parse_message_id(Some("x")), None);
    assert_eq!(TelegramChannel::parse_message_id(None), None);
}

#[test]
fn extract_bind_code_requires_a_code_argument() {
    assert_eq!(TelegramChannel::extract_bind_code("/bind"), None);
    assert_eq!(TelegramChannel::extract_bind_code(""), None);
    assert_eq!(TelegramChannel::extract_bind_code("/bind 123"), Some("123"));
}

#[test]
fn is_start_command_rejects_blank_text() {
    assert!(!TelegramChannel::is_start_command("   "));
}

#[test]
fn track_update_id_evicts_oldest_entry_past_cache_size() {
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    let cap = i64::try_from(TELEGRAM_RECENT_UPDATE_CACHE_SIZE).unwrap();
    for id in 1..=cap + 1 {
        assert!(ch.track_update_id(id));
    }
    let window = ch.recent_updates.lock();
    assert_eq!(window.recent_order.len(), TELEGRAM_RECENT_UPDATE_CACHE_SIZE);
    assert!(!window.recent_lookup.contains(&1), "oldest id evicted");
    assert!(window.recent_lookup.contains(&(cap + 1)));
}

#[test]
fn add_allowed_identity_runtime_ignores_blank_and_duplicates() {
    let ch = TelegramChannel::new("tok".into(), vec!["alice".into()], false);
    ch.add_allowed_identity_runtime("  @ ");
    ch.add_allowed_identity_runtime("@alice");
    ch.add_allowed_identity_runtime("@bob");
    assert_eq!(
        *ch.allowed_users.read().unwrap(),
        vec!["alice".to_string(), "bob".to_string()]
    );
}

#[tokio::test]
async fn delete_webhook_posts_drop_pending_false_and_reports_ok() {
    let server = mock_api().await;
    mount_post(&server, "deleteWebhook", ok_with(serde_json::json!(true))).await;
    let ch = channel_for(&server, &["*"]);

    assert!(ch.delete_webhook_for_long_polling().await);
    let bodies = json_bodies(&server, "deleteWebhook").await;
    assert_eq!(
        bodies,
        vec![serde_json::json!({ "drop_pending_updates": false })]
    );
}

#[tokio::test]
async fn delete_webhook_reports_false_on_transport_error() {
    let mut ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    ch.set_api_base_for_tests(dead_base());
    assert!(!ch.delete_webhook_for_long_polling().await);
}

#[tokio::test]
async fn telegram_api_ok_rejects_http_errors_ok_false_and_non_json() {
    let server = mock_api().await;
    Mock::given(path("/status"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    Mock::given(path("/okfalse"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 400, "description": "Bad Request"
        })))
        .mount(&server)
        .await;
    Mock::given(path("/okfalse-bare"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;
    Mock::given(path("/html"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>"))
        .mount(&server)
        .await;
    Mock::given(path("/good"))
        .respond_with(ok())
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    for (route, expected) in [
        ("/status", false),
        ("/okfalse", false),
        ("/okfalse-bare", false),
        ("/html", false),
        ("/good", true),
    ] {
        let resp = client
            .get(format!("{}{route}", server.uri()))
            .send()
            .await
            .unwrap();
        assert_eq!(
            TelegramChannel::telegram_api_ok(resp).await,
            expected,
            "route {route}"
        );
    }
}

#[tokio::test]
async fn get_bot_username_fetches_once_then_serves_cache() {
    let server = mock_api().await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "username": "my_bot" })))
        .expect(1)
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);

    assert_eq!(ch.get_bot_username().await.as_deref(), Some("my_bot"));
    assert_eq!(ch.get_bot_username().await.as_deref(), Some("my_bot"));
    assert_eq!(ch.bot_username.lock().as_deref(), Some("my_bot"));
    server.verify().await;
}

#[tokio::test]
async fn fetch_bot_username_fails_on_http_error_status() {
    let server = mock_api().await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);

    let err = ch.fetch_bot_username().await.unwrap_err().to_string();
    assert!(err.contains("Failed to fetch bot info"), "{err}");
    assert_eq!(ch.get_bot_username().await, None);
    assert!(ch.bot_username.lock().is_none());
}

#[tokio::test]
async fn fetch_bot_username_fails_when_username_missing() {
    let server = mock_api().await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "id": 1 })))
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);

    let err = ch.fetch_bot_username().await.unwrap_err().to_string();
    assert!(err.contains("Bot username not found"), "{err}");
}

#[test]
fn track_update_id_rejects_stale_and_duplicate_ids() {
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    assert!(ch.track_update_id(10));
    assert!(!ch.track_update_id(10), "duplicate");
    assert!(!ch.track_update_id(9), "stale");
    assert!(ch.track_update_id(11));
}
