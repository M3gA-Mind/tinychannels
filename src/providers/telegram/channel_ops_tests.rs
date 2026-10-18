use super::*;
use crate::providers::telegram::fixtures::*;
use std::sync::Arc;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Hang guard for loops that must terminate on their own.
const GUARD: Duration = Duration::from_secs(20);

fn streaming(server: &MockServer) -> TelegramChannel {
    channel_for(server, &["*"]).with_streaming(StreamMode::Partial, 0, true)
}

#[tokio::test]
async fn send_draft_posts_placeholder_and_returns_message_id() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    let ch = streaming(&server);

    let id = ch
        .send_draft(&SendMessage::new("", "100:7").in_thread(Some("55".into())))
        .await
        .unwrap();

    assert_eq!(id.as_deref(), Some("321"));
    assert_eq!(
        json_bodies(&server, "sendMessage").await,
        vec![serde_json::json!({
            "chat_id": "100",
            "text": "...",
            "message_thread_id": "7",
            "reply_to_message_id": 55,
        })]
    );
    assert!(ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn send_draft_without_message_id_in_result_returns_none() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok_with(serde_json::json!({}))).await;
    let ch = streaming(&server);

    let id = ch
        .send_draft(&SendMessage::new("thinking", "100"))
        .await
        .unwrap();
    assert_eq!(id, None);
    assert_eq!(
        json_bodies(&server, "sendMessage").await[0]["text"],
        "thinking"
    );
}

#[tokio::test]
async fn send_draft_surfaces_api_rejection() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(400).set_body_string("chat not found"),
    )
    .await;
    let ch = streaming(&server);

    let err = ch
        .send_draft(&SendMessage::new("x", "100"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("sendMessage (draft) failed"), "{err}");
    assert!(err.contains("chat not found"), "{err}");
}

#[tokio::test]
async fn update_draft_edits_message_and_records_edit_time() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);

    ch.update_draft("100:3", "12", "partial").await.unwrap();

    assert_eq!(
        json_bodies(&server, "editMessageText").await,
        vec![serde_json::json!({ "chat_id": "100", "message_id": 12, "text": "partial" })]
    );
    assert!(ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn update_draft_truncates_oversized_text_to_limit() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);
    let text = "é".repeat(TELEGRAM_MAX_MESSAGE_LENGTH);

    ch.update_draft("100", "12", &text).await.unwrap();

    let sent = json_bodies(&server, "editMessageText").await[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(sent.len() <= TELEGRAM_MAX_MESSAGE_LENGTH);
    assert_eq!(
        sent.len(),
        TELEGRAM_MAX_MESSAGE_LENGTH,
        "two-byte chars fill the limit"
    );
}

#[tokio::test]
async fn update_draft_rejection_is_swallowed_and_not_recorded() {
    let server = mock_api().await;
    mount_post(
        &server,
        "editMessageText",
        ResponseTemplate::new(400).set_body_string("message is not modified"),
    )
    .await;
    let ch = streaming(&server);

    ch.update_draft("100", "12", "same").await.unwrap();
    assert!(!ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn update_draft_invalid_message_id_skips_network() {
    let server = mock_api().await;
    let ch = streaming(&server);
    ch.update_draft("100", "abc", "text").await.unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn finalize_draft_edits_with_markdown_and_clears_rate_limit() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);
    ch.last_draft_edit
        .lock()
        .insert("100".into(), std::time::Instant::now());

    ch.finalize_draft("100", "8", "done <tool_call>{}</tool_call>", None)
        .await
        .unwrap();

    let bodies = json_bodies(&server, "editMessageText").await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["parse_mode"], "Markdown");
    assert_eq!(bodies[0]["text"], "done");
    assert!(ch.last_draft_edit.lock().is_empty());
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn finalize_draft_retries_plain_edit_when_markdown_rejected() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("editMessageText")))
        .and(body_partial_json(
            serde_json::json!({ "parse_mode": "Markdown" }),
        ))
        .respond_with(ResponseTemplate::new(400))
        .with_priority(1)
        .mount(&server)
        .await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100", "8", "final *text", None)
        .await
        .unwrap();

    let bodies = json_bodies(&server, "editMessageText").await;
    assert_eq!(bodies.len(), 2);
    assert!(bodies[1].get("parse_mode").is_none());
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn finalize_draft_falls_back_to_new_message_when_edits_fail() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ResponseTemplate::new(400)).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100:4", "8", "final", Some("77"))
        .await
        .unwrap();

    assert_eq!(json_bodies(&server, "editMessageText").await.len(), 2);
    let sent = json_bodies(&server, "sendMessage").await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["text"], "final");
    assert_eq!(sent[0]["message_thread_id"], "4");
    assert_eq!(sent[0]["reply_to_message_id"], 77);
}

#[tokio::test]
async fn finalize_draft_short_text_with_invalid_id_sends_new_message() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100", "nope", "final", None)
        .await
        .unwrap();

    assert!(json_bodies(&server, "editMessageText").await.is_empty());
    assert_eq!(sent_texts(&server).await, vec!["final".to_string()]);
}

#[tokio::test]
async fn finalize_draft_oversized_text_deletes_draft_and_sends_chunks() {
    let server = mock_api().await;
    mount_post(&server, "deleteMessage", ok_with(serde_json::json!(true))).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);
    let text = "y".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 1);

    ch.finalize_draft("100", "8", &text, None).await.unwrap();

    assert_eq!(
        json_bodies(&server, "deleteMessage").await,
        vec![serde_json::json!({ "chat_id": "100", "message_id": 8 })]
    );
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

#[tokio::test]
async fn finalize_draft_oversized_text_with_invalid_id_sends_chunks_only() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);
    let text = "y".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 1);

    ch.finalize_draft("100", "bad", &text, None).await.unwrap();

    assert!(json_bodies(&server, "deleteMessage").await.is_empty());
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

#[tokio::test]
async fn send_reaction_marker_targets_explicit_id_then_sends_reply_text() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new("[REACTION:👍|99] Great point", "100:2").in_thread(Some("5".into())))
        .await
        .unwrap();

    let reactions = json_bodies(&server, "setMessageReaction").await;
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0]["message_id"], 99);
    assert_eq!(reactions[0]["chat_id"], "100");
    let sent = json_bodies(&server, "sendMessage").await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["text"], "Great point");
    assert_eq!(sent[0]["message_thread_id"], "2");
    assert_eq!(sent[0]["reply_to_message_id"], 5);
}

#[tokio::test]
async fn send_reaction_only_marker_uses_parent_id_and_sends_no_text() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new("[REACTION:🔥]", "100").in_thread(Some("12".into())))
        .await
        .unwrap();

    let reactions = json_bodies(&server, "setMessageReaction").await;
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0]["message_id"], 12);
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn send_reaction_marker_without_target_is_ignored() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new("[REACTION:🔥]", "100"))
        .await
        .unwrap();

    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn send_with_attachment_markers_sends_text_then_attachments() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    mount_post(&server, "sendPhoto", ok()).await;
    mount_post(&server, "sendDocument", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new(
        "Here you go [IMAGE:https://cdn.example/a.png] [DOCUMENT:https://cdn.example/b.pdf]",
        "100",
    ))
    .await
    .unwrap();

    assert_eq!(sent_texts(&server).await, vec!["Here you go".to_string()]);
    assert_eq!(
        json_bodies(&server, "sendPhoto").await[0]["photo"],
        "https://cdn.example/a.png"
    );
    assert_eq!(
        json_bodies(&server, "sendDocument").await[0]["document"],
        "https://cdn.example/b.pdf"
    );
}

#[tokio::test]
async fn send_attachment_marker_only_skips_text_message() {
    let server = mock_api().await;
    mount_post(&server, "sendVideo", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send(&SendMessage::new(
        "[VIDEO:https://cdn.example/v.mp4]",
        "100",
    ))
    .await
    .unwrap();

    assert!(json_bodies(&server, "sendMessage").await.is_empty());
    assert_eq!(json_bodies(&server, "sendVideo").await.len(), 1);
}

#[tokio::test]
async fn send_bare_existing_path_uploads_it_as_attachment() {
    let server = mock_api().await;
    mount_post(&server, "sendDocument", ok()).await;
    let ch = channel_for(&server, &["*"]);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("report.pdf");
    std::fs::write(&file, b"%PDF").unwrap();

    ch.send(&SendMessage::new(file.to_str().unwrap(), "100"))
        .await
        .unwrap();

    assert!(json_bodies(&server, "sendMessage").await.is_empty());
    let uploads = raw_bodies(&server, "sendDocument").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("report.pdf"));
}

#[tokio::test]
async fn health_check_reflects_get_me_status() {
    let server = mock_api().await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "username": "b" })))
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);
    assert!(ch.health_check().await);

    let failing = mock_api().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&failing)
        .await;
    assert!(!channel_for(&failing, &["*"]).health_check().await);

    let mut dead = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    dead.api_base = dead_base();
    assert!(!dead.health_check().await);
}

#[tokio::test]
async fn start_typing_sends_immediately_and_reuses_task_for_same_recipient() {
    let server = mock_api().await;
    mount_post(&server, "sendChatAction", ok_with(serde_json::json!(true))).await;
    let ch = channel_for(&server, &["*"]);

    ch.start_typing("100:6").await.unwrap();
    // The background refresher's first iteration posts right away; wait for it.
    tokio::time::timeout(GUARD, async {
        while json_bodies(&server, "sendChatAction").await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typing refresher posted");
    let first_task_id = ch.typing_handle.lock().as_ref().unwrap().handle.id();

    ch.start_typing("100:6").await.unwrap();
    assert_eq!(
        ch.typing_handle.lock().as_ref().unwrap().handle.id(),
        first_task_id,
        "same recipient keeps the running refresher"
    );

    let bodies = json_bodies(&server, "sendChatAction").await;
    assert!(bodies.iter().all(|b| b["chat_id"] == "100"
        && b["action"] == "typing"
        && b["message_thread_id"] == "6"));

    ch.stop_typing("100:6").await.unwrap();
    assert!(ch.typing_handle.lock().is_none());
}

#[tokio::test]
async fn typing_refresh_reports_acceptance_rejection_and_transport_error() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/accept"))
        .respond_with(ok_with(serde_json::json!(true)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/reject"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": false })))
        .mount(&server)
        .await;
    let client = reqwest::Client::new();
    let body = TelegramChannel::typing_body_for_recipient("100");

    let accept = format!("{}/accept", server.uri());
    let reject = format!("{}/reject", server.uri());
    let dead = format!("{}/x", dead_base());
    assert!(TelegramChannel::refresh_typing_once(&client, &accept, &body, "100").await);
    assert!(!TelegramChannel::refresh_typing_once(&client, &reject, &body, "100").await);
    assert!(!TelegramChannel::refresh_typing_once(&client, &dead, &body, "100").await);

    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 2);
    let sent: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(
        sent,
        serde_json::json!({ "chat_id": "100", "action": "typing" })
    );
}

#[test]
fn telegram_advertises_reaction_support() {
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    assert!(ch.supports_reactions());
}

fn fast_backoff(mut ch: TelegramChannel) -> TelegramChannel {
    ch.poll_error_backoff = Duration::from_millis(1);
    ch.poll_conflict_backoff = Duration::from_millis(1);
    ch
}

/// Mount a one-shot `getUpdates` response, consumed in mount order.
async fn mount_get_updates_once(server: &MockServer, priority: u8, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path(api_path("getUpdates")))
        .respond_with(response)
        .up_to_n_times(1)
        .with_priority(priority)
        .mount(server)
        .await;
}

fn updates(result: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": true, "result": result }))
}

#[tokio::test]
async fn listen_recovers_from_poll_errors_and_dispatches_updates() {
    let server = mock_api().await;
    // Poll sequence: unparseable body, generic API error, 409 without webhook,
    // 409 webhook conflict that deleteWebhook clears, then a real batch.
    mount_get_updates_once(
        &server,
        1,
        ResponseTemplate::new(200).set_body_string("<html>"),
    )
    .await;
    mount_get_updates_once(
        &server,
        2,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 500, "description": "Internal"
        })),
    )
    .await;
    mount_get_updates_once(
        &server,
        3,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "Conflict: terminated by other getUpdates request"
        })),
    )
    .await;
    mount_get_updates_once(
        &server,
        4,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "Conflict: can't use getUpdates method while webhook is active"
        })),
    )
    .await;
    mount_post(&server, "deleteWebhook", ok_with(serde_json::json!(true))).await;
    mount_get_updates_once(
        &server,
        5,
        updates(serde_json::json!([
            {
                "update_id": 10,
                "message_reaction": {
                    "chat": { "id": 100 }, "message_id": 4,
                    "user": { "id": 7, "username": "alice" },
                    "new_reaction": [{ "type": "emoji", "emoji": "❤" }]
                }
            },
            {
                "update_id": 11,
                "message": {
                    "message_id": 5, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 8, "username": "mallory" }, "text": "let me in"
                }
            },
            {
                "update_id": 12,
                "message": {
                    "message_id": 6, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "hello bot"
                }
            }
        ])),
    )
    .await;
    // Next poll re-delivers 12 (deduped) and a new message that the closed
    // receiver can no longer take, which ends the loop.
    mount_get_updates_once(
        &server,
        6,
        updates(serde_json::json!([
            {
                "update_id": 12,
                "message": {
                    "message_id": 6, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "hello bot"
                }
            },
            {
                "update_id": 13,
                "message": {
                    "message_id": 9, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "second"
                }
            }
        ])),
    )
    .await;
    // Any later poll delivers yet another update, so the loop ends on the
    // closed receiver no matter how far it ran ahead of the test.
    Mock::given(method("POST"))
        .and(path(api_path("getUpdates")))
        .respond_with(updates(serde_json::json!([
            {
                "update_id": 14,
                "message": {
                    "message_id": 10, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "third"
                }
            }
        ])))
        .with_priority(10)
        .mount(&server)
        .await;
    mount_post(&server, "sendMessage", ok()).await;

    let events = Arc::new(RecordingEvents::default());
    let ch = Arc::new(fast_backoff(
        channel_for(&server, &["alice"]).with_events(events.clone()),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn({
        let ch = ch.clone();
        async move { ch.listen(tx).await }
    });

    let first = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("message dispatched")
        .expect("channel open");
    assert_eq!(first.content, "hello bot");
    assert_eq!(first.sender, "alice");
    drop(rx);

    tokio::time::timeout(GUARD, listener)
        .await
        .expect("listen returns once receiver is gone")
        .unwrap()
        .unwrap();

    let recorded = events.events.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].1, "reaction_received");
    assert_eq!(recorded[0].2["target_message_id"], "telegram_100_4");
    assert_eq!(recorded[0].2["emoji"], "❤");
    assert_eq!(json_bodies(&server, "deleteWebhook").await.len(), 1);
    // The unauthorized sender got the approval prompt.
    let prompts = sent_texts(&server).await;
    assert_eq!(prompts.len(), 1);
    assert!(prompts[0].contains("operator approval"), "{prompts:?}");
    // Offsets advance past each delivered batch.
    let polls = json_bodies(&server, "getUpdates").await;
    assert!(polls.len() >= 6, "{polls:?}");
    assert_eq!(polls[0]["offset"], 0);
    assert_eq!(polls[5]["offset"], 13);
}

#[tokio::test]
async fn listen_backs_off_when_webhook_delete_fails_and_on_transport_error() {
    let server = mock_api().await;
    mount_get_updates_once(
        &server,
        1,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "webhook is active"
        })),
    )
    .await;
    mount_post(&server, "deleteWebhook", ResponseTemplate::new(500)).await;
    mount_get_updates_once(
        &server,
        2,
        updates(serde_json::json!([
            {
                "update_id": 1,
                "message": {
                    "message_id": 2, "chat": { "id": 5, "type": "group" },
                    "from": { "id": 3, "username": "alice" }, "text": "hey @my_bot ping"
                }
            }
        ])),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "username": "my_bot" })))
        .mount(&server)
        .await;

    let mut ch = fast_backoff(channel_for(&server, &["alice"]));
    ch.mention_only = true;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let ch = Arc::new(ch);
    let listener = tokio::spawn({
        let ch = ch.clone();
        async move { ch.listen(tx).await }
    });

    let msg = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("message dispatched")
        .expect("channel open");
    assert_eq!(
        msg.content, "hey ping",
        "mention stripped in mention-only group"
    );
    assert_eq!(json_bodies(&server, "deleteWebhook").await.len(), 1);
    listener.abort();

    // Transport failures back off and retry rather than ending the loop.
    let mut dead = fast_backoff(TelegramChannel::new("tok".into(), vec!["*".into()], true));
    dead.api_base = dead_base();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let outcome = tokio::time::timeout(Duration::from_millis(300), dead.listen(tx)).await;
    assert!(
        outcome.is_err(),
        "listen keeps retrying on transport errors"
    );
    assert!(dead.bot_username.lock().is_none());
}

#[tokio::test]
async fn loopback_bot_api_retries_poll_then_routes_status_reply() {
    let api = BotApiFake::start().await;
    // The current poll loop retries this API error using poll_error_backoff;
    // retry_after is part of the realistic response shape, not a delay assertion.
    api.fail_next_poll(
        429,
        serde_json::json!({
            "ok": false,
            "error_code": 429,
            "description": "Too Many Requests",
            "parameters": { "retry_after": 1 }
        }),
    );
    api.inject(serde_json::json!({
        "update_id": 1,
        "message": {
            "message_id": 41,
            "chat": { "id": 777, "type": "private" },
            "from": { "id": 7, "username": "alice" },
            "text": "/status"
        }
    }));

    let mut channel = api.channel(&["alice"]);
    channel.poll_error_backoff = Duration::from_millis(1);
    let channel = Arc::new(channel);
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn({
        let channel = Arc::clone(&channel);
        async move { channel.listen(tx).await }
    });

    let inbound = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("poll recovers from rate limit")
        .expect("injected command is dispatched");
    assert_eq!(inbound.content, "/status");
    assert_eq!(inbound.sender, "alice");
    assert_eq!(inbound.reply_target, "777");
    assert_eq!(inbound.thread_ts.as_deref(), Some("41"));

    // This is the channel-facing part of the host's /status handler: it
    // turns the received command into a provider reply in the originating chat.
    channel
        .send(
            &SendMessage::new("Thread: none\nProvider: openai", inbound.reply_target)
                .in_thread(inbound.thread_ts),
        )
        .await
        .unwrap();
    assert_eq!(
        api.sent(),
        vec![(
            "sendMessage".to_string(),
            serde_json::json!({
                "chat_id": "777",
                "text": "Thread: none\nProvider: openai",
                "parse_mode": "Markdown",
                "reply_to_message_id": 41
            })
        )]
    );

    // A second injected message lets listen exit cleanly when the receiver is
    // dropped, proving the loop processed a subsequent poll after the 429.
    drop(rx);
    api.inject(serde_json::json!({
        "update_id": 2,
        "message": {
            "message_id": 42,
            "chat": { "id": 777, "type": "private" },
            "from": { "id": 7, "username": "alice" },
            "text": "stop"
        }
    }));
    tokio::time::timeout(GUARD, listener)
        .await
        .expect("listener stops after receiver closes")
        .unwrap()
        .unwrap();
}
