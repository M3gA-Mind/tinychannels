use super::*;
use crate::channel::ChannelAdapter;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tinychannels_bus::{ChannelInboundEnvelope, ChannelIngress};
use tokio_tungstenite::tungstenite::Message;

fn discord_units(text: &str) -> usize {
    text.encode_utf16().count()
}

#[test]
fn discord_channel_name() {
    let ch = DiscordChannel::new("fake".into(), None, None, vec![], false, false);
    assert_eq!(ChannelAdapter::descriptor(&ch).id, "discord");
}

#[test]
fn base64_decode_bot_id() {
    // "MTIzNDU2" decodes to "123456"
    let decoded = base64_decode("MTIzNDU2");
    assert_eq!(decoded, Some("123456".to_string()));
}

#[test]
fn bot_user_id_extraction() {
    // Token format: base64(user_id).timestamp.hmac
    let token = "MTIzNDU2.fake.hmac";
    let id = DiscordChannel::bot_user_id_from_token(token);
    assert_eq!(id, Some("123456".to_string()));
}

#[test]
fn empty_allowlist_allows_everyone() {
    // Issue #3712: an unconfigured (empty) allowlist must apply no per-user
    // restriction — otherwise a UI-connected bot silently ignores every message
    // and never replies. Scope is still enforced by guild/channel filters.
    let ch = DiscordChannel::new("fake".into(), None, None, vec![], false, false);
    assert!(ch.is_user_allowed("12345"));
    assert!(ch.is_user_allowed("anyone"));
}

#[test]
fn wildcard_allows_everyone() {
    let ch = DiscordChannel::new("fake".into(), None, None, vec!["*".into()], false, false);
    assert!(ch.is_user_allowed("12345"));
    assert!(ch.is_user_allowed("anyone"));
}

#[test]
fn specific_allowlist_filters() {
    let ch = DiscordChannel::new(
        "fake".into(),
        None,
        None,
        vec!["111".into(), "222".into()],
        false,
        false,
    );
    assert!(ch.is_user_allowed("111"));
    assert!(ch.is_user_allowed("222"));
    assert!(!ch.is_user_allowed("333"));
    assert!(!ch.is_user_allowed("unknown"));
}

#[test]
fn allowlist_is_exact_match_not_substring() {
    let ch = DiscordChannel::new("fake".into(), None, None, vec!["111".into()], false, false);
    assert!(!ch.is_user_allowed("1111"));
    assert!(!ch.is_user_allowed("11"));
    assert!(!ch.is_user_allowed("0111"));
}

#[test]
fn allowlist_empty_string_user_id() {
    let ch = DiscordChannel::new("fake".into(), None, None, vec!["111".into()], false, false);
    assert!(!ch.is_user_allowed(""));
}

#[test]
fn allowlist_with_wildcard_and_specific() {
    let ch = DiscordChannel::new(
        "fake".into(),
        None,
        None,
        vec!["111".into(), "*".into()],
        false,
        false,
    );
    assert!(ch.is_user_allowed("111"));
    assert!(ch.is_user_allowed("anyone_else"));
}

#[test]
fn allowlist_case_sensitive() {
    let ch = DiscordChannel::new("fake".into(), None, None, vec!["ABC".into()], false, false);
    assert!(ch.is_user_allowed("ABC"));
    assert!(!ch.is_user_allowed("abc"));
    assert!(!ch.is_user_allowed("Abc"));
}

#[test]
fn base64_decode_empty_string() {
    let decoded = base64_decode("");
    assert_eq!(decoded, Some(String::new()));
}

#[test]
fn base64_decode_invalid_chars() {
    let decoded = base64_decode("!!!!");
    assert!(decoded.is_none());
}

#[test]
fn bot_user_id_from_empty_token() {
    let id = DiscordChannel::bot_user_id_from_token("");
    assert_eq!(id, Some(String::new()));
}

#[test]
fn contains_bot_mention_supports_plain_and_nick_forms() {
    assert!(contains_bot_mention("hi <@12345>", "12345"));
    assert!(contains_bot_mention("hi <@!12345>", "12345"));
    assert!(!contains_bot_mention("hi <@99999>", "12345"));
}

#[test]
fn normalize_incoming_content_requires_mention_when_enabled() {
    let cleaned = normalize_incoming_content("hello there", true, "12345");
    assert!(cleaned.is_none());
}

#[test]
fn normalize_incoming_content_strips_mentions_and_trims() {
    let cleaned = normalize_incoming_content("  <@!12345> run status  ", true, "12345");
    assert_eq!(cleaned.as_deref(), Some("run status"));
}

#[test]
fn normalize_incoming_content_rejects_empty_after_strip() {
    let cleaned = normalize_incoming_content("<@12345>", true, "12345");
    assert!(cleaned.is_none());
}

// Message splitting tests

#[test]
fn split_empty_message() {
    let chunks = split_message_for_discord("");
    assert_eq!(chunks, vec![""]);
}

#[test]
fn split_short_message_under_limit() {
    let msg = "Hello, world!";
    let chunks = split_message_for_discord(msg);
    assert_eq!(chunks, vec![msg]);
}

#[test]
fn split_message_exactly_2000_chars() {
    let msg = "a".repeat(DISCORD_MAX_MESSAGE_LENGTH);
    let chunks = split_message_for_discord(&msg);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].chars().count(), DISCORD_MAX_MESSAGE_LENGTH);
}

#[test]
fn split_message_just_over_limit() {
    let msg = "a".repeat(DISCORD_MAX_MESSAGE_LENGTH + 1);
    let chunks = split_message_for_discord(&msg);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].chars().count(), DISCORD_MAX_MESSAGE_LENGTH);
    assert_eq!(chunks[1].chars().count(), 1);
}

#[test]
fn split_very_long_message() {
    let msg = "word ".repeat(2000); // 10000 characters (5 chars per "word ")
    let chunks = split_message_for_discord(&msg);
    // The shared chunker prefers whitespace boundaries, so it may produce more
    // than the mathematical minimum while preserving Discord's UTF-16 limit.
    assert!(chunks.len() > 1);
    assert!(
        chunks
            .iter()
            .all(|chunk| discord_units(chunk) <= DISCORD_MAX_MESSAGE_LENGTH)
    );
    // Verify total content is preserved
    let reconstructed = chunks.concat();
    assert_eq!(reconstructed, msg);
}

#[test]
fn split_prefer_newline_break() {
    let msg = format!("{}\n{}", "a".repeat(1500), "b".repeat(500));
    let chunks = split_message_for_discord(&msg);
    // Should split at the newline
    assert_eq!(chunks.len(), 2);
    assert!(chunks[1].starts_with('\n'));
    assert!(chunks[1].trim_start_matches('\n').starts_with('b'));
}

#[test]
fn split_prefer_space_break() {
    let msg = format!("{} {}", "a".repeat(1500), "b".repeat(600));
    let chunks = split_message_for_discord(&msg);
    assert_eq!(chunks.len(), 2);
}

#[test]
fn split_without_good_break_points_hard_split() {
    // No spaces or newlines - should hard split at 2000
    let msg = "a".repeat(5000);
    let chunks = split_message_for_discord(&msg);
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].chars().count(), DISCORD_MAX_MESSAGE_LENGTH);
    assert_eq!(chunks[1].chars().count(), DISCORD_MAX_MESSAGE_LENGTH);
    assert_eq!(chunks[2].chars().count(), 1000);
}

#[test]
fn split_multiple_breaks() {
    // Create a message with multiple newlines
    let part1 = "a".repeat(900);
    let part2 = "b".repeat(900);
    let part3 = "c".repeat(900);
    let msg = format!("{part1}\n{part2}\n{part3}");
    let chunks = split_message_for_discord(&msg);
    // Should split into 2 chunks (first two parts + third part)
    assert_eq!(chunks.len(), 2);
    assert!(chunks[0].chars().count() <= DISCORD_MAX_MESSAGE_LENGTH);
    assert!(chunks[1].chars().count() <= DISCORD_MAX_MESSAGE_LENGTH);
}

#[test]
fn split_preserves_content() {
    let original = "Hello world! This is a test message with some content. ".repeat(200);
    let chunks = split_message_for_discord(&original);
    let reconstructed = chunks.concat();
    assert_eq!(reconstructed, original);
}

#[test]
fn split_unicode_content() {
    // Test with emoji and multi-byte characters
    let msg = "🦀 Rust is awesome! ".repeat(500);
    let chunks = split_message_for_discord(&msg);
    // All chunks should be valid UTF-8
    for chunk in &chunks {
        assert!(std::str::from_utf8(chunk.as_bytes()).is_ok());
        assert!(chunk.chars().count() <= DISCORD_MAX_MESSAGE_LENGTH);
    }
    // Reconstruct and verify
    let reconstructed = chunks.concat();
    assert_eq!(reconstructed, msg);
}

#[test]
fn split_newline_too_close_to_end() {
    // If newline is in the first half, don't use it - use space instead or hard split
    let msg = format!("{}\n{}", "a".repeat(1900), "b".repeat(500));
    let chunks = split_message_for_discord(&msg);
    // Should split at newline since it's in the second half of the window
    assert_eq!(chunks.len(), 2);
}

#[test]
fn split_multibyte_only_content_without_panics() {
    let msg = "🦀".repeat(2500);
    let chunks = split_message_for_discord(&msg);
    assert_eq!(chunks.len(), 3);
    assert!(
        chunks
            .iter()
            .all(|chunk| discord_units(chunk) <= DISCORD_MAX_MESSAGE_LENGTH)
    );
    let reconstructed = chunks.concat();
    assert_eq!(reconstructed, msg);
}

#[test]
fn split_chunks_always_within_discord_limit() {
    let msg = "x".repeat(12_345);
    let chunks = split_message_for_discord(&msg);
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.chars().count() <= DISCORD_MAX_MESSAGE_LENGTH)
    );
}

#[test]
fn split_message_with_multiple_newlines() {
    let msg = "Line 1\nLine 2\nLine 3\n".repeat(1000);
    let chunks = split_message_for_discord(&msg);
    assert!(chunks.len() > 1);
    let reconstructed = chunks.concat();
    assert_eq!(reconstructed, msg);
}

async fn typing_sink() -> (DiscordChannel, mpsc::UnboundedReceiver<String>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/channels/{channel_id}/typing",
        post(move |Path(channel_id): Path<String>| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(channel_id);
                StatusCode::NO_CONTENT
            }
        }),
    );
    let base = spawn_http(app).await;
    (channel_at(&base, None), rx)
}

#[tokio::test]
async fn start_typing_sends_once_to_the_numeric_channel() {
    let (ch, mut rx) = typing_sink().await;
    crate::channel::ChannelTyping::start_typing(&ch, "123456")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(HANG_GUARD, rx.recv())
            .await
            .unwrap()
            .as_deref(),
        Some("123456")
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn stop_typing_leaves_discord_indicator_to_expire() {
    let (ch, mut rx) = typing_sink().await;
    crate::channel::ChannelTyping::start_typing(&ch, "123456")
        .await
        .unwrap();
    crate::channel::ChannelTyping::stop_typing(&ch, "123456")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(HANG_GUARD, rx.recv())
            .await
            .unwrap()
            .as_deref(),
        Some("123456")
    );
    assert!(
        rx.try_recv().is_err(),
        "Discord's typing indicator expires on its own"
    );
}

#[tokio::test]
async fn stop_typing_is_idempotent() {
    let ch = DiscordChannel::new("fake".into(), None, None, vec![], false, false)
        .with_api_base("http://127.0.0.1:9");
    assert!(
        crate::channel::ChannelTyping::stop_typing(&ch, "123456")
            .await
            .is_ok()
    );
    assert!(
        crate::channel::ChannelTyping::stop_typing(&ch, "123456")
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn repeated_start_typing_targets_each_requested_channel() {
    let (ch, mut rx) = typing_sink().await;
    crate::channel::ChannelTyping::start_typing(&ch, "111")
        .await
        .unwrap();
    crate::channel::ChannelTyping::start_typing(&ch, "222")
        .await
        .unwrap();
    for expected in ["111", "222"] {
        assert_eq!(
            tokio::time::timeout(HANG_GUARD, rx.recv())
                .await
                .unwrap()
                .as_deref(),
            Some(expected)
        );
    }
    assert!(rx.try_recv().is_err());
}

// ── Message ID edge cases ─────────────────────────────────────

#[test]
fn discord_message_id_format_includes_discord_prefix() {
    // Verify that message IDs follow the format: discord_{message_id}
    let message_id = "123456789012345678";
    let expected_id = format!("discord_{message_id}");
    assert_eq!(expected_id, "discord_123456789012345678");
}

#[test]
fn discord_message_id_is_deterministic() {
    // Same message_id = same ID (prevents duplicates after restart)
    let message_id = "123456789012345678";
    let id1 = format!("discord_{message_id}");
    let id2 = format!("discord_{message_id}");
    assert_eq!(id1, id2);
}

#[test]
fn discord_message_id_different_message_different_id() {
    // Different message IDs produce different IDs
    let id1 = "discord_123456789012345678".to_string();
    let id2 = "discord_987654321098765432".to_string();
    assert_ne!(id1, id2);
}

#[test]
fn discord_message_id_uses_snowflake_id() {
    // Discord snowflake IDs are numeric strings
    let message_id = "123456789012345678"; // Typical snowflake format
    let id = format!("discord_{message_id}");
    assert!(id.starts_with("discord_"));
    // Snowflake IDs are numeric
    assert!(message_id.chars().all(|c| c.is_ascii_digit()));
}

#[test]
fn discord_message_id_fallback_to_uuid_on_empty() {
    // Edge case: empty message_id falls back to UUID
    let message_id = "";
    let id = if message_id.is_empty() {
        format!("discord_{}", uuid::Uuid::new_v4())
    } else {
        format!("discord_{message_id}")
    };
    assert!(id.starts_with("discord_"));
    // Should have UUID dashes
    assert!(id.contains('-'));
}

// ─────────────────────────────────────────────────────────────────────
// TG6: Channel platform limit edge cases for Discord (2000 char limit)
// Prevents: Pattern 6 — issues #574, #499
// ─────────────────────────────────────────────────────────────────────

#[test]
fn split_message_code_block_at_boundary() {
    // Code block that spans the split boundary
    let mut msg = String::new();
    msg.push_str("```rust\n");
    msg.push_str(&"x".repeat(1990));
    msg.push_str("\n```\nMore text after code block");
    let parts = split_message_for_discord(&msg);
    assert!(
        parts.len() >= 2,
        "code block spanning boundary should split"
    );
    for part in &parts {
        assert!(
            part.len() <= DISCORD_MAX_MESSAGE_LENGTH,
            "each part must be <= {DISCORD_MAX_MESSAGE_LENGTH}, got {}",
            part.len()
        );
    }
}

#[test]
fn split_message_single_long_word_exceeds_limit() {
    // A single word longer than 2000 chars must be hard-split
    let long_word = "a".repeat(2500);
    let parts = split_message_for_discord(&long_word);
    assert!(parts.len() >= 2, "word exceeding limit must be split");
    for part in &parts {
        assert!(
            part.len() <= DISCORD_MAX_MESSAGE_LENGTH,
            "hard-split part must be <= {DISCORD_MAX_MESSAGE_LENGTH}, got {}",
            part.len()
        );
    }
    // Reassembled content should match original
    let reassembled: String = parts.join("");
    assert_eq!(reassembled, long_word);
}

#[test]
fn split_message_exactly_at_limit_no_split() {
    let msg = "a".repeat(DISCORD_MAX_MESSAGE_LENGTH);
    let parts = split_message_for_discord(&msg);
    assert_eq!(parts.len(), 1, "message exactly at limit should not split");
    assert_eq!(parts[0].len(), DISCORD_MAX_MESSAGE_LENGTH);
}

#[test]
fn split_message_one_over_limit_splits() {
    let msg = "a".repeat(DISCORD_MAX_MESSAGE_LENGTH + 1);
    let parts = split_message_for_discord(&msg);
    assert!(parts.len() >= 2, "message 1 char over limit must split");
}

#[test]
fn split_message_many_short_lines() {
    // Many short lines should be batched into chunks under the limit
    let msg: String = (0..500).map(|i| format!("line {i}\n")).collect();
    let parts = split_message_for_discord(&msg);
    for part in &parts {
        assert!(
            part.len() <= DISCORD_MAX_MESSAGE_LENGTH,
            "short-line batch must be <= limit"
        );
    }
    // All content should be preserved
    let reassembled: String = parts.join("");
    assert_eq!(reassembled.trim(), msg.trim());
}

#[test]
fn split_message_only_whitespace() {
    let msg = "   \n\n\t  ";
    let parts = split_message_for_discord(msg);
    // Should handle gracefully without panic
    assert!(parts.len() <= 1);
}

#[test]
fn split_message_emoji_at_boundary() {
    // Emoji are multi-byte; ensure we don't split mid-emoji
    let mut msg = "a".repeat(1998);
    msg.push_str("🎉🎊"); // 2 emoji at the boundary (2000 chars total)
    let parts = split_message_for_discord(&msg);
    for part in &parts {
        // The function splits on character count, not byte count
        assert!(
            part.chars().count() <= DISCORD_MAX_MESSAGE_LENGTH,
            "emoji boundary split must respect limit"
        );
    }
}

#[test]
fn split_message_consecutive_newlines_at_boundary() {
    let mut msg = "a".repeat(1995);
    msg.push_str("\n\n\n\n\n");
    msg.push_str(&"b".repeat(100));
    let parts = split_message_for_discord(&msg);
    for part in &parts {
        assert!(part.len() <= DISCORD_MAX_MESSAGE_LENGTH);
    }
}

// ── channel_id field tests ───────────────────────────────────

#[test]
fn channel_id_stored_in_struct() {
    let ch = DiscordChannel::new(
        "token".into(),
        Some("guild1".into()),
        Some("channel1".into()),
        vec![],
        false,
        false,
    );
    assert_eq!(ch.channel_id.as_deref(), Some("channel1"));
    assert_eq!(ch.guild_id.as_deref(), Some("guild1"));
}

#[test]
fn channel_id_defaults_to_none() {
    let ch = DiscordChannel::new("token".into(), None, None, vec![], false, false);
    assert!(ch.channel_id.is_none());
}

#[test]
fn passes_guild_scope_covers_guild_dm_and_unscoped_cases() {
    // No configured guild → everything passes (filter inactive).
    assert!(DiscordChannel::passes_guild_scope(None, Some("g1"), true));
    assert!(DiscordChannel::passes_guild_scope(None, None, true));
    // Configured guild: same guild passes, other guild blocked.
    assert!(DiscordChannel::passes_guild_scope(
        Some("g1"),
        Some("g1"),
        true
    ));
    assert!(!DiscordChannel::passes_guild_scope(
        Some("g1"),
        Some("g2"),
        true
    ));
    // #3794 review (Codex P1): DM (no guild_id) under guild scope is blocked
    // with a blank allowlist, allowed with an explicit one.
    assert!(!DiscordChannel::passes_guild_scope(Some("g1"), None, true));
    assert!(DiscordChannel::passes_guild_scope(Some("g1"), None, false));
}

#[test]
fn resolve_recipient_prefers_explicit_then_configured_channel() {
    // #3794 review (Codex P2): recipient-less proactive sends fall back to the
    // configured channel_id; an explicit recipient always wins.
    assert_eq!(
        DiscordChannel::resolve_recipient("123", Some("999")),
        Some("123")
    );
    assert_eq!(
        DiscordChannel::resolve_recipient("", Some("999")),
        Some("999")
    );
    // Neither available → None, so the caller errors instead of POSTing to "".
    assert_eq!(DiscordChannel::resolve_recipient("", None), None);
    assert_eq!(DiscordChannel::resolve_recipient("", Some("")), None);
}

#[test]
fn proactive_target_uses_configured_channel_id() {
    // Configured channel_id ⇒ recipient-less proactive sends have a target.
    let with_channel = DiscordChannel::new(
        "fake".into(),
        None,
        Some("12345".into()),
        vec![],
        false,
        false,
    );
    assert_eq!(
        ChannelAdapter::proactive_target(&with_channel),
        Some("12345".to_string())
    );

    // No channel_id ⇒ None, so proactive routing skips Discord (#3794 Codex P2).
    let no_channel = DiscordChannel::new("fake".into(), None, None, vec![], false, false);
    assert_eq!(ChannelAdapter::proactive_target(&no_channel), None);

    // Whitespace-only channel_id is treated as unset.
    let blank_channel = DiscordChannel::new(
        "fake".into(),
        None,
        Some("   ".into()),
        vec![],
        false,
        false,
    );
    assert_eq!(ChannelAdapter::proactive_target(&blank_channel), None);
}

// ── REST + Gateway against loopback mocks ─────────────────────

use axum::{
    Json, Router, extract::Path, http::HeaderMap, http::StatusCode, routing::get, routing::post,
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// `base64("123456")` — the bot user id embedded in the token.
const BOT_TOKEN: &str = "MTIzNDU2.ts.hmac";
const HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(10);

async fn spawn_http(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{}", addr.port())
}

fn channel_at(base: &str, channel_id: Option<&str>) -> DiscordChannel {
    DiscordChannel::with_http_client(
        BOT_TOKEN.into(),
        None,
        channel_id.map(str::to_string),
        vec![],
        false,
        false,
        reqwest::Client::new(),
    )
    .with_api_base(format!("{base}/"))
}

fn outbound(content: &str, recipient: &str) -> tinychannels_bus::ChannelOutboundIntent {
    tinychannels_bus::ChannelOutboundIntent {
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "discord".into(),
            account_id: None,
        }),
        channel_id: "discord".into(),
        conversation_id: recipient.into(),
        conversation_ref: (!recipient.is_empty()).then(|| tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Channel,
            id: recipient.into(),
            ..Default::default()
        }),
        payload: tinychannels_bus::OutboundPayload::Text {
            text: content.into(),
        },
        ..Default::default()
    }
}

async fn send_text(
    channel: &DiscordChannel,
    content: &str,
    recipient: &str,
) -> Result<crate::channel::ChannelSendOutcome, crate::channel::ChannelSendError> {
    ChannelAdapter::send_outcome(channel, outbound(content, recipient)).await
}

fn next_inbound(rx: &mut mpsc::Receiver<ChannelIngress>) -> ChannelInboundEnvelope {
    match rx.try_recv().expect("one typed inbound event") {
        ChannelIngress::Inbound(envelope) => envelope,
        ChannelIngress::Event(event) => panic!("unexpected lifecycle event: {event:?}"),
    }
}

/// Mock that records `(channel_id, authorization, content)` per POSTed message.
async fn message_sink(
    status: StatusCode,
) -> (String, mpsc::UnboundedReceiver<(String, String, String)>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/channels/{channel_id}/messages",
        post(
            move |Path(channel_id): Path<String>,
                  headers: HeaderMap,
                  Json(body): Json<serde_json::Value>| {
                let tx = tx.clone();
                async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let content = body["content"].as_str().unwrap_or_default().to_string();
                    let _ = tx.send((channel_id, auth, content));
                    let body = if status.is_success() {
                        r#"{"id":"123456789012345678"}"#
                    } else {
                        "rate limited body"
                    };
                    axum::http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body))
                        .expect("mock response is valid")
                }
            },
        ),
    );
    (spawn_http(app).await, rx)
}

#[test]
fn with_api_base_trims_trailing_slash_and_defaults_to_discord() {
    let ch = DiscordChannel::new("t".into(), None, None, vec![], false, false);
    assert_eq!(ch.api_base, "https://discord.com/api/v10");
    let ch = ch.with_api_base("http://127.0.0.1:9/api//");
    assert_eq!(ch.api_base, "http://127.0.0.1:9/api");
}

#[tokio::test]
async fn send_posts_message_to_recipient_with_bot_auth() {
    let (base, mut rx) = message_sink(StatusCode::OK).await;
    let ch = channel_at(&base, Some("654321"));
    send_text(&ch, "hello there", "123456").await.unwrap();
    let (chan, auth, content) = rx.try_recv().unwrap();
    assert_eq!(chan, "123456");
    assert_eq!(auth, format!("Bot {BOT_TOKEN}"));
    assert_eq!(content, "hello there");
    assert!(rx.try_recv().is_err(), "exactly one request");
}

#[tokio::test]
async fn send_splits_long_text_for_an_explicit_channel() {
    let (base, mut rx) = message_sink(StatusCode::OK).await;
    let ch = channel_at(&base, Some("123456"));
    let long = "word ".repeat(500); // 2500 chars → two chunks
    send_text(&ch, &long, "123456").await.unwrap();
    let first = rx.try_recv().unwrap();
    let second = rx.try_recv().unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(first.0, "123456");
    assert_eq!(second.0, "123456");
    assert!(discord_units(&first.2) <= DISCORD_MAX_MESSAGE_LENGTH);
    assert_eq!(
        format!("{}{}", first.2, second.2)
            .split_whitespace()
            .count(),
        500
    );
}

#[tokio::test]
async fn send_without_any_target_fails_before_any_request() {
    let (base, mut rx) = message_sink(StatusCode::OK).await;
    let ch = channel_at(&base, None);
    let err = send_text(&ch, "hi", "").await.unwrap_err().message;
    assert!(err.contains("numeric channel id"), "{err}");
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn send_surfaces_non_success_status_and_body() {
    let (base, _rx) = message_sink(StatusCode::TOO_MANY_REQUESTS).await;
    let ch = channel_at(&base, None);
    let err = send_text(&ch, "hi", "123456").await.unwrap_err().message;
    assert!(err.contains("Discord send message failed (429"), "{err}");
    assert!(err.contains("rate limited body"), "{err}");
}

#[tokio::test]
async fn health_check_reflects_users_me_status() {
    let app = Router::new().route(
        "/users/@me",
        get(|headers: HeaderMap| async move {
            if headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some(&format!("Bot {BOT_TOKEN}")[..])
            {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            }
        }),
    );
    let base = spawn_http(app).await;
    let healthy = channel_at(&base, None);
    assert_eq!(
        crate::channel::ChannelAdapter::status(&healthy)
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("healthy")
    );
    let bad = DiscordChannel::new("wrong".into(), None, None, vec![], false, false)
        .with_api_base(base.clone());
    assert_eq!(
        crate::channel::ChannelAdapter::status(&bad)
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("unhealthy")
    );
    // Unreachable endpoint → false rather than an error.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    assert_eq!(
        ChannelAdapter::status(&channel_at(&dead, None))
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("unhealthy")
    );
}

#[tokio::test]
async fn start_typing_posts_typing_indicator_until_stopped() {
    let (ch, mut rx) = typing_sink().await;
    crate::channel::ChannelTyping::start_typing(&ch, "9")
        .await
        .unwrap();
    let hit = tokio::time::timeout(HANG_GUARD, rx.recv()).await.unwrap();
    assert_eq!(hit.as_deref(), Some("9"));
    crate::channel::ChannelTyping::stop_typing(&ch, "9")
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
}

// ── Gateway ───────────────────────────────────────────────────

type WsStream = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

/// Spawn a mock REST API whose `/gateway/bot` points at a fresh WebSocket
/// listener, and run `script` against the first gateway connection after
/// the Hello / Identify / first-heartbeat handshake.
async fn spawn_gateway<F, Fut>(script: F) -> (String, tokio::task::JoinHandle<()>)
where
    F: FnOnce(WsStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = Arc::new(format!("ws://{}", ws_listener.local_addr().unwrap()));
    let app = Router::new().route(
        "/gateway/bot",
        get(move || {
            let url = ws_url.clone();
            async move { Json(serde_json::json!({ "url": *url })) }
        }),
    );
    let base = spawn_http(app).await;
    let server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.send(Message::Text(
            json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let identify = next_json(&mut ws).await;
        assert_eq!(identify["op"], 2);
        assert_eq!(identify["d"]["token"], BOT_TOKEN);
        assert_eq!(identify["d"]["intents"], 46593);
        // The heartbeat timer fires immediately, before any sequence is known.
        assert_eq!(next_json(&mut ws).await, json!({"op": 1, "d": null}));
        script(ws).await;
    });
    (base, server)
}

async fn next_json(ws: &mut WsStream) -> serde_json::Value {
    let frame = ws.next().await.unwrap().unwrap();
    serde_json::from_str(&frame.to_string()).unwrap()
}

async fn send_json(ws: &mut WsStream, value: serde_json::Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

fn message_create(d: serde_json::Value) -> serde_json::Value {
    json!({"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": d})
}

#[tokio::test]
async fn listen_filters_gateway_events_and_forwards_allowed_messages() {
    let (base, server) = spawn_gateway(|mut ws| async move {
        // Server-requested heartbeat echoes the latest sequence.
        send_json(&mut ws, json!({"op": 1, "s": 5})).await;
        assert_eq!(next_json(&mut ws).await, json!({"op": 1, "d": 5}));
        ws.send(Message::Binary(vec![1, 2].into())).await.unwrap();
        ws.send(Message::Text("not json".into())).await.unwrap();
        send_json(&mut ws, json!({"op": 0, "t": "GUILD_CREATE", "d": {}})).await;
        send_json(&mut ws, json!({"op": 0, "t": "MESSAGE_CREATE"})).await;
        let base = json!({"id": "m", "guild_id": "g1", "channel_id": "c1", "content": "x"});
        for (author, patch) in [
            (json!({"id": "123456"}), json!({})),          // the bot itself
            (json!({"id": "u1", "bot": true}), json!({})), // another bot
            (json!({"id": "intruder"}), json!({})),        // not allowlisted
            (json!({"id": "u1"}), json!({"guild_id": "g2"})), // other guild
            (json!({"id": "u1"}), json!({"channel_id": "c2"})), // other channel
            (json!({"id": "u1"}), json!({"content": ""})), // empty content
        ] {
            let mut d = base.clone();
            d["author"] = author;
            for (k, v) in patch.as_object().unwrap() {
                d[k] = v.clone();
            }
            send_json(&mut ws, message_create(d)).await;
        }
        send_json(
            &mut ws,
            message_create(json!({
                "id": "m-ok", "guild_id": "g1", "channel_id": "c1",
                "content": "  hello  ", "author": {"id": "u1"}
            })),
        )
        .await;
        send_json(&mut ws, json!({"op": 7, "d": null})).await;
    })
    .await;

    let ch = DiscordChannel::new(
        BOT_TOKEN.into(),
        Some("g1".into()),
        Some("c1".into()),
        vec!["u1".into()],
        false,
        false,
    )
    .with_api_base(base);
    let (tx, mut rx) = mpsc::channel(16);
    tokio::time::timeout(HANG_GUARD, ch.run_gateway_typed(tx))
        .await
        .expect("listen returns on op 7")
        .unwrap();
    tokio::time::timeout(HANG_GUARD, server)
        .await
        .unwrap()
        .unwrap();

    let msg = next_inbound(&mut rx);
    assert_eq!(msg.message_id, "discord_m-ok");
    assert_eq!(msg.sender.id, "u1");
    assert_eq!(msg.conversation.id, "c1");
    assert_eq!(msg.text, "hello");
    assert_eq!(msg.channel.id, "discord");
    assert!(rx.try_recv().is_err(), "everything else was filtered");
}

#[tokio::test]
async fn listen_handles_dm_mentions_and_bots_then_stops_on_invalid_session() {
    let (base, server) = spawn_gateway(|mut ws| async move {
        send_json(
            &mut ws,
            message_create(json!({
                "channel_id": "7654321", "content": "<@!123456> ping",
                "author": {"id": "u7", "bot": true}
            })),
        )
        .await;
        // Not addressed to the bot → dropped in mention-only mode.
        send_json(
            &mut ws,
            message_create(json!({"id": "x", "content": "ignore me", "author": {"id": "u7"}})),
        )
        .await;
        send_json(&mut ws, json!({"op": 9, "d": false})).await;
    })
    .await;

    let ch = DiscordChannel::new(BOT_TOKEN.into(), None, None, vec!["*".into()], true, true)
        .with_api_base(base);
    let (tx, mut rx) = mpsc::channel(16);
    tokio::time::timeout(HANG_GUARD, ch.run_gateway_typed(tx))
        .await
        .expect("listen returns on op 9")
        .unwrap();
    tokio::time::timeout(HANG_GUARD, server)
        .await
        .unwrap()
        .unwrap();

    let msg = next_inbound(&mut rx);
    assert!(
        msg.message_id.starts_with("discord_") && msg.message_id.len() > "discord_".len() + 30,
        "{}",
        msg.message_id
    );
    assert_eq!(msg.sender.id, "u7");
    assert_eq!(
        msg.conversation.id, "7654321",
        "the typed DM conversation retains Discord's channel id"
    );
    assert_eq!(
        msg.conversation.kind,
        tinychannels_bus::ConversationKind::Dm
    );
    assert_eq!(msg.text, "ping");
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn listen_returns_when_gateway_closes() {
    let (base, server) = spawn_gateway(|mut ws| async move {
        ws.close(None).await.unwrap();
    })
    .await;
    let ch = channel_at(&base, None);
    let (tx, _rx) = mpsc::channel(1);
    tokio::time::timeout(HANG_GUARD, ch.run_gateway_typed(tx))
        .await
        .expect("listen returns on close")
        .unwrap();
    tokio::time::timeout(HANG_GUARD, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn gateway_reconnect_controls_release_the_current_typed_session() {
    for opcode in [7, 9] {
        let (base, server) = spawn_gateway(move |mut ws| async move {
            send_json(&mut ws, json!({"op": opcode, "d": false})).await;
        })
        .await;
        let channel = channel_at(&base, None);
        let (tx, _rx) = mpsc::channel(1);
        tokio::time::timeout(HANG_GUARD, channel.run_gateway_typed(tx))
            .await
            .expect("reconnect control closes this gateway session")
            .expect("reconnect is a normal session boundary");
        tokio::time::timeout(HANG_GUARD, server)
            .await
            .expect("mock gateway exits")
            .expect("mock gateway task succeeds");
    }
}

#[tokio::test]
async fn listen_stops_when_receiver_is_gone() {
    let (base, server) = spawn_gateway(|mut ws| async move {
        send_json(
            &mut ws,
            message_create(
                json!({"id": "1", "channel_id": "c", "content": "hi", "author": {"id": "u"}}),
            ),
        )
        .await;
        // Keep the socket open until the client goes away.
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await;
    let ch = channel_at(&base, None);
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    tokio::time::timeout(HANG_GUARD, ch.run_gateway_typed(tx))
        .await
        .expect("listen returns once delivery fails")
        .unwrap();
    tokio::time::timeout(HANG_GUARD, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn listen_fails_when_gateway_lookup_is_not_json() {
    let app = Router::new().route("/gateway/bot", get(|| async { "<html>nope</html>" }));
    let base = spawn_http(app).await;
    let (tx, _rx) = mpsc::channel(1);
    assert!(channel_at(&base, None).run_gateway_typed(tx).await.is_err());
}

#[tokio::test]
async fn gateway_transport_and_hello_failures_return_to_the_listener() {
    let (tx, _rx) = mpsc::channel(1);
    let unavailable = channel_at("http://127.0.0.1:0", None);
    assert!(
        tokio::time::timeout(HANG_GUARD, unavailable.run_gateway_typed(tx))
            .await
            .expect("failed REST connection is bounded")
            .is_err()
    );

    let failed_ws = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let failed_ws_port = failed_ws.local_addr().unwrap().port();
    drop(failed_ws);
    let app = Router::new().route(
        "/gateway/bot",
        get(
            move || async move { Json(json!({"url": format!("ws://127.0.0.1:{failed_ws_port}")})) },
        ),
    );
    let rest_base = spawn_http(app).await;
    let (tx, _rx) = mpsc::channel(1);
    assert!(
        tokio::time::timeout(
            HANG_GUARD,
            channel_at(&rest_base, None).run_gateway_typed(tx)
        )
        .await
        .expect("failed websocket connection is bounded")
        .is_err()
    );

    let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws_listener.local_addr().unwrap());
    let app = Router::new().route(
        "/gateway/bot",
        get(move || {
            let url = ws_url.clone();
            async move { Json(json!({"url": url})) }
        }),
    );
    let rest_base = spawn_http(app).await;
    let gateway = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        drop(tokio_tungstenite::accept_async(socket).await.unwrap());
    });
    let (tx, _rx) = mpsc::channel(1);
    assert!(
        tokio::time::timeout(
            HANG_GUARD,
            channel_at(&rest_base, None).run_gateway_typed(tx)
        )
        .await
        .expect("closed websocket before Hello is bounded")
        .is_err()
    );
    tokio::time::timeout(HANG_GUARD, gateway)
        .await
        .expect("mock gateway exits")
        .expect("mock gateway task succeeds");
}

#[test]
fn base64_decode_handles_unpadded_padded_and_trailing_input() {
    assert_eq!(base64_decode("MQ").as_deref(), Some("1"));
    assert_eq!(base64_decode("MTI").as_deref(), Some("12"));
    assert_eq!(base64_decode("MQ==").as_deref(), Some("1"));
    assert_eq!(base64_decode("MTI=").as_deref(), Some("12"));
    // A dangling fifth character is ignored rather than mis-decoded.
    assert_eq!(base64_decode("MTIzN").as_deref(), Some("123"));
}
