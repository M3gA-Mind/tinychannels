use super::*;
use crate::host::MediaRepository as _;
use crate::providers::telegram::fixtures::*;
use base64::Engine;
use std::sync::Arc;
use tinystoragedrivers_api::StorageBackend as _;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn private_text(from: serde_json::Value, text: &str) -> serde_json::Value {
    serde_json::json!({
        "update_id": 1,
        "message": {
            "message_id": 50,
            "chat": { "id": 777, "type": "private" },
            "from": from,
            "text": text,
        }
    })
}

fn voice_update(
    chat_type: &str,
    caption: Option<&str>,
    file_size: Option<u64>,
) -> serde_json::Value {
    let mut voice = serde_json::json!({
        "file_id": "voice-id",
        "file_unique_id": "uniq",
        "mime_type": "audio/ogg",
    });
    if let Some(size) = file_size {
        voice["file_size"] = serde_json::json!(size);
    }
    let mut message = serde_json::json!({
        "message_id": 60,
        "chat": { "id": 888, "type": chat_type },
        "from": { "id": 9, "username": "alice", "first_name": "Alice" },
        "voice": voice,
    });
    if let Some(caption) = caption {
        message["caption"] = serde_json::json!(caption);
    }
    serde_json::json!({ "update_id": 3, "message": message })
}

/// Serve `getFile` and the file download for the fixture voice note.
async fn mount_voice_download(server: &MockServer, bytes: Vec<u8>) {
    mount_post(
        server,
        "getFile",
        ok_with(serde_json::json!({ "file_path": "voice/v.oga", "file_size": bytes.len() })),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("/file/bot{TOKEN}/voice/v.oga")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(server)
        .await;
}

#[derive(Clone, Default)]
struct RecordingVoiceStore(Arc<std::sync::Mutex<Vec<tinychannels_bus::HostVoiceScope>>>);

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for RecordingVoiceStore {
    async fn store_voice(
        &self,
        request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        self.0.lock().unwrap().push(request.scope);
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("host-voice-reference".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            },
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        unreachable!("inbound voice fixture only stores media")
    }
}

#[derive(Clone, Default)]
struct RecordingMediaRepository(Arc<std::sync::Mutex<Vec<crate::host::MediaStoreRequest>>>);

#[async_trait::async_trait]
impl crate::host::MediaRepository for RecordingMediaRepository {
    async fn store_media(
        &self,
        request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        let reference = tinychannels_bus::MediaReference {
            opaque_id: Some("tinychannels_media_v1_fixture".into()),
            content_type: Some(request.metadata.content_type.clone()),
            file_name: request.metadata.file_name.clone(),
            size_bytes: Some(request.bytes.len() as u64),
            duration_ms: request.metadata.duration_ms,
            kind: request.metadata.kind,
            ..Default::default()
        };
        self.0.lock().unwrap().push(request);
        Ok(reference)
    }

    async fn read_media(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
        _max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        unreachable!("Telegram ingress fixture only stores media")
    }
}

#[derive(Clone)]
struct ScopedMediaRepository {
    store: Arc<crate::host::ScopedMediaStore>,
    scopes: Arc<std::sync::Mutex<Vec<crate::host::MediaScope>>>,
}

impl ScopedMediaRepository {
    fn new() -> Self {
        let storage = tinystoragedrivers_testkit::MemoryBackend::new()
            .for_scope(
                &tinystoragedrivers_types::Scope::new("module:tinychannels@telegram").unwrap(),
            )
            .unwrap();
        Self {
            store: Arc::new(crate::host::ScopedMediaStore::new(
                Arc::clone(storage.documents()),
                Arc::clone(storage.blobs()),
            )),
            scopes: Arc::default(),
        }
    }
}

#[async_trait::async_trait]
impl crate::host::MediaRepository for ScopedMediaRepository {
    async fn store_media(
        &self,
        mut request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        request.metadata.scope.configured_instance = "telegram-main".into();
        request.metadata.scope.channel.id = "telegram".into();
        request.metadata.scope.channel.account_id = Some("telegram-main".into());
        self.scopes
            .lock()
            .unwrap()
            .push(request.metadata.scope.clone());
        self.store.store(request).await
    }

    async fn read_media(
        &self,
        scope: crate::host::MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        self.store.read(scope, &opaque_id, max_bytes).await
    }
}

// ── typing ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn typing_action_retries_without_thread_when_thread_scoped_action_rejected() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("sendChatAction")))
        .and(wiremock::matchers::body_partial_json(
            serde_json::json!({ "message_thread_id": "4" }),
        ))
        .respond_with(ResponseTemplate::new(400).set_body_string("TOPIC_CLOSED"))
        .with_priority(1)
        .mount(&server)
        .await;
    mount_post(&server, "sendChatAction", ok_with(serde_json::json!(true))).await;
    let ch = channel_for(&server, &["*"]);

    assert!(ch.send_typing_action_once("100:4").await.unwrap());

    let bodies = json_bodies(&server, "sendChatAction").await;
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0]["message_thread_id"], "4");
    assert_eq!(
        bodies[1],
        serde_json::json!({ "chat_id": "100", "action": "typing" })
    );
}

#[tokio::test]
async fn typing_action_fallback_rejection_is_tolerated() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendChatAction",
        ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": false })),
    )
    .await;
    let ch = channel_for(&server, &["*"]);

    assert!(!ch.send_typing_action_once("100:4").await.unwrap());
    assert!(!ch.send_typing_action_once("100").await.unwrap());

    // Thread-scoped: original + fallback; plain chat: no fallback.
    assert_eq!(json_bodies(&server, "sendChatAction").await.len(), 3);
}

#[tokio::test]
async fn typing_action_transport_error_is_tolerated() {
    let mut ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    ch.api_base = dead_base();
    assert!(ch.send_typing_action_once("100:4").await.is_err());
}

// ── mention parsing ──────────────────────────────────────────────────────

#[test]
fn mention_spans_ignore_embedded_and_bare_at_signs() {
    assert!(TelegramChannel::find_bot_mention_spans("hi @bot", "@").is_empty());
    assert!(TelegramChannel::find_bot_mention_spans("mail a@bot.com", "bot").is_empty());
    assert!(TelegramChannel::find_bot_mention_spans("@ @! @", "bot").is_empty());
    assert_eq!(
        TelegramChannel::find_bot_mention_spans("é @Bot!", "bot"),
        vec![(3, 7)]
    );
}

#[test]
fn incoming_context_in_mention_only_group_needs_source_and_known_bot_name() {
    let mut ch = TelegramChannel::new("tok".into(), vec!["*".into()], true);
    ch.mention_only = true;
    let message = serde_json::json!({
        "message_id": 1,
        "chat": { "id": 5, "type": "supergroup" },
        "from": { "id": 2, "username": "alice" },
    });
    assert!(ch.parse_incoming_message_context(&message, None).is_none());
    assert!(
        ch.parse_incoming_message_context(&message, Some("@bot hi"))
            .is_none(),
        "unknown bot username cannot be matched"
    );
    *ch.bot_username.lock() = Some("bot".into());
    assert!(
        ch.parse_incoming_message_context(&message, Some("hi"))
            .is_none()
    );
    let ctx = ch
        .parse_incoming_message_context(&message, Some("@bot hi"))
        .unwrap();
    assert_eq!(ctx.mention_text.as_deref(), Some("hi"));
}

#[test]
fn incoming_context_falls_back_to_numeric_id_and_requires_chat() {
    let ch = TelegramChannel::new("tok".into(), vec!["42".into()], false);
    let ctx = ch
        .parse_incoming_message_context(
            &serde_json::json!({
                "message_id": 3,
                "chat": { "id": 5 },
                "from": { "id": 42, "last_name": "Lovelace" },
                "reply_to_message": { "message_id": 2 },
            }),
            Some("x"),
        )
        .unwrap();
    assert_eq!(ctx.sender_identity, "42");
    assert_eq!(ctx.sender_name.as_deref(), Some("Lovelace"));
    assert!(
        ch.parse_incoming_message_context(&serde_json::json!({ "from": { "id": 42 } }), Some("x"))
            .is_none()
    );
    assert_eq!(TelegramChannel::sender_display_name(None), None);
    assert_eq!(
        TelegramChannel::sender_display_name(Some(&serde_json::json!({ "username": "u" }))),
        Some("u".into())
    );
}

#[test]
fn parse_update_message_drops_whitespace_only_text() {
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    let update = private_text(serde_json::json!({ "id": 1, "username": "a" }), "   ");
    assert!(ch.parse_update_message(&update).is_none());
}

#[test]
fn parse_update_reaction_requires_chat_message_and_emoji() {
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    assert!(
        ch.parse_update_reaction(&serde_json::json!({ "message_reaction": { "message_id": 1 } }))
            .is_none()
    );
    assert!(
        ch.parse_update_reaction(
            &serde_json::json!({ "message_reaction": { "chat": { "id": 1 } } })
        )
        .is_none()
    );
    let anonymous = ch
        .parse_update_reaction(&serde_json::json!({ "message_reaction": {
            "chat": { "id": 1 }, "message_id": 2,
            "new_reaction": [{ "type": "custom_emoji" }, { "emoji": "🎉" }]
        }}))
        .unwrap();
    assert_eq!(anonymous.sender, "unknown");
    assert_eq!(anonymous.emoji, "🎉");

    let restricted = TelegramChannel::new("tok".into(), vec!["7".into()], false);
    let by_id = restricted
        .parse_update_reaction(&serde_json::json!({ "message_reaction": {
            "chat": { "id": 1 }, "message_id": 2,
            "user": { "id": 7, "username": "notlisted" },
            "new_reaction": [{ "emoji": "👍" }]
        }}))
        .unwrap();
    assert_eq!(by_id.sender, "notlisted", "allowed via numeric id");
}

#[test]
fn parse_update_reactions_preserves_all_changes_and_custom_emoji_identity() {
    let ch = TelegramChannel::new("tok".into(), vec!["7".into()], false);
    let update = serde_json::json!({
        "update_id": 91,
        "message_reaction": {
            "chat": {"id": -10, "type": "supergroup"},
            "message_id": 12,
            "message_thread_id": 77,
            "user": {"id": 7, "username": "alice"},
            "old_reaction": [{"type":"emoji", "emoji":"A"}],
            "new_reaction": [
                {"type":"custom_emoji", "custom_emoji_id":"custom-2"},
                {"type":"emoji", "emoji":"B"}
            ]
        }
    });
    let changes = ch.parse_update_reactions(&update);
    assert_eq!(changes.len(), 3);
    assert!(!changes[0].added);
    assert_eq!(changes[0].emoji, "A");
    assert!(changes[1..].iter().all(|change| change.added));
    assert_eq!(changes[1].custom_emoji_id.as_deref(), Some("custom-2"));
    assert_eq!(changes[2].emoji, "B");
    assert_eq!(
        changes[0].conversation_kind,
        tinychannels_bus::ConversationKind::Group
    );
    assert_eq!(changes[0].thread_id.as_deref(), Some("77"));
    let unchanged = serde_json::json!({"message_reaction": {
        "chat":{"id":-10,"type":"supergroup"},"message_id":12,"user":{"id":7},
        "old_reaction":[{"type":"emoji","emoji":"A"}],
        "new_reaction":[{"type":"emoji","emoji":"A"}]
    }});
    assert!(ch.parse_update_reactions(&unchanged).is_empty());
}

#[test]
fn anonymous_chat_reactions_keep_actor_identity_and_native_paid_reaction() {
    let ch = TelegramChannel::new("tok".into(), vec!["-100".into()], false);
    let changes = ch.parse_update_reactions(&serde_json::json!({
        "update_id": 92,
        "message_reaction": {
            "chat": {"id": -10, "type": "channel"}, "message_id": 8,
            "actor_chat": {"id": -100, "title": "moderators"},
            "old_reaction": [],
            "new_reaction": [{"type": "paid"}]
        }
    }));
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].sender_id, "-100");
    assert_eq!(changes[0].sender, "moderators");
    assert_eq!(changes[0].emoji, "paid");
    assert_eq!(changes[0].custom_emoji_id, None);

    let denied = TelegramChannel::new("tok".into(), vec!["alice".into()], false);
    assert!(
        denied
            .parse_update_reactions(&serde_json::json!({
                "message_reaction": {
                    "chat": {"id": -10}, "message_id": 8,
                    "actor_chat": {"id": -100, "title": "moderators"},
                    "old_reaction": [], "new_reaction": [{"type": "paid"}]
                }
            }))
            .is_empty(),
        "anonymous actors still obey the configured allowlist"
    );
}

#[test]
fn redact_bot_token_is_identity_for_empty_token() {
    let ch = TelegramChannel::new(String::new(), vec!["*".into()], false);
    assert_eq!(ch.redact_bot_token("keep /bot/ as is"), "keep /bot/ as is");
}

// ── unauthorized-message handling ───────────────────────────────────────

#[tokio::test]
async fn unauthorized_ignores_non_message_unsupported_and_chatless_updates() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["alice"]);

    ch.handle_unauthorized_message(&serde_json::json!({ "edited_message": {} }))
        .await;
    ch.handle_unauthorized_message(&serde_json::json!({ "message": { "photo": [] } }))
        .await;
    ch.handle_unauthorized_message(&serde_json::json!({
        "message": { "text": "hi", "from": { "id": 3 } }
    }))
    .await;

    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn unauthorized_handler_is_silent_for_allowed_sender() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["alice"]);
    ch.handle_unauthorized_message(&private_text(
        serde_json::json!({ "id": 1, "username": "alice" }),
        "hi",
    ))
    .await;
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn unauthorized_handler_suppresses_prompt_on_restart_race() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["alice"]);
    ch.allowed_users.write().unwrap().clear();
    ch.handle_unauthorized_message(&private_text(
        serde_json::json!({ "id": 2, "username": "mallory" }),
        "hi",
    ))
    .await;
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn start_command_pairs_first_private_sender_and_persists() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let store = Arc::new(RecordingAllowlist::default());
    let ch = channel_for(&server, &[]).with_allowlist(store.clone());
    assert!(ch.pairing_code_active());

    ch.handle_unauthorized_message(&private_text(
        serde_json::json!({ "id": 31, "username": "owner" }),
        "/start",
    ))
    .await;

    assert!(ch.is_user_allowed("31"));
    assert!(!ch.pairing_code_active(), "bind code consumed by /start");
    assert_eq!(
        *store.persisted.lock().unwrap(),
        vec![("telegram".to_string(), "31".to_string())]
    );
    let texts = sent_texts(&server).await;
    assert_eq!(texts.len(), 1);
    assert!(texts[0].contains("You're all set"), "{texts:?}");
}

#[tokio::test]
async fn start_command_reports_persist_failure_but_keeps_runtime_access() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let store = Arc::new(RecordingAllowlist {
        fail: true,
        ..Default::default()
    });
    let ch = channel_for(&server, &[]).with_allowlist(store);

    ch.handle_unauthorized_message(&private_text(
        serde_json::json!({ "username": "owner" }),
        "/start@my_bot",
    ))
    .await;

    assert!(ch.is_user_allowed("owner"));
    let texts = sent_texts(&server).await;
    assert!(texts[0].contains("couldn't save it"), "{texts:?}");
}

#[tokio::test]
async fn start_command_without_identity_asks_sender_to_retry() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &[]);

    ch.handle_unauthorized_message(&private_text(serde_json::json!({}), "/start"))
        .await;

    assert!(ch.allowlist_is_empty());
    assert!(ch.pairing_code_active());
    let texts = sent_texts(&server).await;
    assert!(texts[0].contains("Could not identify your Telegram account from /start"));
}

#[tokio::test]
async fn bind_with_valid_code_allowlists_sender() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &[]);
    let code = ch.pairing.as_ref().unwrap().pairing_code().unwrap();
    // A group chat skips the /start onboarding branch.
    let mut update = private_text(
        serde_json::json!({ "id": 64, "username": "op" }),
        &format!("/bind {code}"),
    );
    update["message"]["chat"]["type"] = serde_json::json!("group");

    ch.handle_unauthorized_message(&update).await;

    assert!(ch.is_user_allowed("64"));
    assert!(sent_texts(&server).await[0].contains("You're all set"));
}

#[tokio::test]
async fn bind_with_valid_code_but_no_identity_is_rejected() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &[]);
    let code = ch.pairing.as_ref().unwrap().pairing_code().unwrap();

    ch.handle_unauthorized_message(&private_text(
        serde_json::json!({}),
        &format!("/bind {code}"),
    ))
    .await;

    assert!(ch.allowlist_is_empty());
    assert!(sent_texts(&server).await[0].contains(r"Could not identify your Telegram account\."));
}

#[tokio::test]
async fn bind_with_wrong_code_reports_invalid_then_lockout() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &[]);
    let wrong = serde_json::json!({ "id": 5, "username": "guess" });

    for _ in 0..crate::security::MAX_PAIR_ATTEMPTS {
        ch.handle_unauthorized_message(&private_text(wrong.clone(), "/bind 000000x"))
            .await;
    }
    ch.handle_unauthorized_message(&private_text(wrong, "/bind 000000x"))
        .await;

    let texts = sent_texts(&server).await;
    let max = usize::try_from(crate::security::MAX_PAIR_ATTEMPTS).unwrap();
    assert_eq!(texts.len(), max + 1);
    assert!(
        texts[..max]
            .iter()
            .all(|t| t.contains("Invalid binding code"))
    );
    assert!(
        texts[max].contains("Too many invalid attempts"),
        "{texts:?}"
    );
    assert!(ch.allowlist_is_empty());
}

#[tokio::test]
async fn bind_without_active_pairing_points_at_config() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["alice"]);

    ch.handle_unauthorized_message(&private_text(
        serde_json::json!({ "id": 6, "username": "bob" }),
        "/bind 123456",
    ))
    .await;

    assert!(sent_texts(&server).await[0].contains("pairing is not active"));
}

#[tokio::test]
async fn pending_pairing_prompt_advertises_start_in_private_chat_and_debounces() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &[]);
    let update = private_text(serde_json::json!({ "id": 8, "username": "x" }), "hello");

    ch.handle_unauthorized_message(&update).await;
    ch.handle_unauthorized_message(&update).await;

    let texts = sent_texts(&server).await;
    assert_eq!(texts.len(), 1, "second prompt debounced");
    assert!(texts[0].contains("send /start"), "{texts:?}");
}

#[tokio::test]
async fn pending_pairing_prompt_in_group_omits_start_hint() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &[]);
    let mut update = private_text(serde_json::json!({ "id": 8 }), "/start");
    update["message"]["chat"]["type"] = serde_json::json!("supergroup");

    ch.handle_unauthorized_message(&update).await;

    assert!(ch.allowlist_is_empty(), "group /start never onboards");
    let texts = sent_texts(&server).await;
    assert_eq!(texts.len(), 1);
    assert!(texts[0].contains("requires operator approval"), "{texts:?}");
}

// ── voice ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn voice_update_parser_preserves_caption_without_claiming_transcription() {
    let server = mock_api().await;
    let stt = FixedTranscriber::ok("  spoken words  ");
    let ch = channel_for(&server, &["alice"]).with_transcriber(stt.clone());

    let msg = ch
        .parse_update_message_or_voice(&voice_update("private", Some("note"), Some(3)))
        .await
        .expect("voice message parsed");

    assert_eq!(msg.content, "note");
    assert_eq!(msg.sender, "alice");
    assert_eq!(msg.thread_ts.as_deref(), Some("60"));
    let requests = stt.requests.lock().unwrap().clone();
    assert!(
        requests.is_empty(),
        "STT enrichment belongs to shared ingress"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn ingested_voice_note_carries_the_host_transcript() {
    let server = mock_api().await;
    mount_voice_download(&server, vec![1, 2, 3]).await;
    let stt = FixedTranscriber::ok("  spoken words  ");
    let channel = channel_for(&server, &["alice"])
        .with_voice_media_store(Arc::new(RecordingVoiceStore::default()))
        .with_transcriber(stt.clone());
    let update = voice_update("private", None, Some(3));
    let message = channel
        .parse_update_message_or_voice(&update)
        .await
        .expect("voice message parsed");

    let note = channel
        .voice_note_for_ingress(&update, &message)
        .await
        .unwrap()
        .expect("durable voice reference");

    let transcript = note.transcript.expect("transcript attached at ingress");
    assert_eq!(
        transcript.state,
        tinychannels_bus::VoiceTranscriptState::Complete
    );
    assert_eq!(transcript.text.trim(), "spoken words");
    assert_eq!(stt.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn stored_voice_scope_matches_normalized_ingress_turn_and_conversation() {
    let server = mock_api().await;
    mount_voice_download(&server, vec![1, 2, 3]).await;
    let store = RecordingVoiceStore::default();
    let channel = channel_for(&server, &["alice"]).with_voice_media_store(Arc::new(store.clone()));
    let update = voice_update("supergroup", Some("caption"), Some(3));
    let message = channel
        .parse_update_message_or_voice(&update)
        .await
        .expect("voice message is accepted independently of STT");
    let voice_note = channel
        .voice_note_for_ingress(&update, &message)
        .await
        .unwrap()
        .expect("durable voice reference");
    let envelope =
        TelegramChannel::inbound_envelope_from_update(&update, &message, Some(voice_note));

    let scopes = store.0.lock().unwrap();
    assert_eq!(scopes.len(), 1);
    let scope = &scopes[0];
    assert_eq!(scope.channel.id, envelope.channel.id);
    assert_eq!(scope.channel.account_id, envelope.channel.account_id);
    assert_eq!(scope.conversation, envelope.conversation);
    assert_eq!(scope.turn_id, envelope.message_id);
    assert_eq!(scope.turn_id, "telegram_888_60");
}

#[tokio::test]
async fn stored_voice_scope_preserves_telegram_forum_topic_identity() {
    let server = mock_api().await;
    mount_voice_download(&server, vec![1, 2, 3]).await;
    let store = RecordingVoiceStore::default();
    let channel = channel_for(&server, &["alice"]).with_voice_media_store(Arc::new(store.clone()));
    let mut update = voice_update("supergroup", Some("caption"), Some(3));
    update["message"]["message_thread_id"] = serde_json::json!(42);
    let message = channel
        .parse_update_message_or_voice(&update)
        .await
        .expect("voice message is accepted independently of STT");
    let voice_note = channel
        .voice_note_for_ingress(&update, &message)
        .await
        .unwrap()
        .expect("durable voice reference");
    let envelope =
        TelegramChannel::inbound_envelope_from_update(&update, &message, Some(voice_note));

    let scopes = store.0.lock().unwrap();
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[0].conversation, envelope.conversation);
    assert_eq!(scopes[0].conversation.thread_id, None);
    assert_eq!(scopes[0].conversation.topic_id.as_deref(), Some("42"));
}

#[tokio::test]
async fn telegram_photo_download_enters_ordered_typed_media_ingress() {
    let server = mock_api().await;
    mount_post(
        &server,
        "getFile",
        ok_with(serde_json::json!({
            "file_path": "photos/p.jpg",
            "file_size": 3
        })),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("/file/bot{TOKEN}/photos/p.jpg")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/jpeg")
                .set_body_bytes(vec![0xff, 0xd8, 0xff]),
        )
        .mount(&server)
        .await;
    let repository = RecordingMediaRepository::default();
    let channel =
        channel_for(&server, &["alice"]).with_media_repository(Arc::new(repository.clone()));
    let update = serde_json::json!({
        "update_id": 9,
        "message": {
            "message_id": 60,
            "chat": { "id": 888, "type": "supergroup" },
            "from": { "id": 9, "username": "alice" },
            "caption": "look",
            "photo": [{
                "file_id": "photo-file",
                "file_unique_id": "photo-unique",
                "file_size": 3
            }]
        }
    });
    let message = channel
        .parse_update_message_or_voice(&update)
        .await
        .expect("photo update should parse as a channel message");
    let media = channel
        .media_parts_for_ingress(&update, &message)
        .await
        .unwrap();
    let envelope = TelegramChannel::inbound_envelope_with_media(&update, &message, None, media);
    assert!(matches!(
        envelope.parts.as_slice(),
        [
            tinychannels_bus::ContentPart::Text { text, .. },
            tinychannels_bus::ContentPart::Image { media, .. }
        ] if text == "look" && media.content_type.as_deref() == Some("image/jpeg")
    ));
    let stored = repository.0.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].bytes, vec![0xff, 0xd8, 0xff]);
    assert_eq!(stored[0].metadata.scope.conversation.id, "888");
    assert_eq!(stored[0].metadata.scope.conversation.topic_id, None);
    assert_eq!(stored[0].metadata.scope.turn_id, "telegram_888_60");
    assert_eq!(
        stored[0].metadata.dedupe_key.as_deref(),
        Some("telegram:telegram_888_60:photo")
    );
}

#[tokio::test]
async fn telegram_ingress_preserves_location_venue_and_contact_as_typed_parts() {
    let server = mock_api().await;
    let channel = channel_for(&server, &["alice"]);
    let updates = [
        serde_json::json!({
            "update_id": 51,
            "message": {
                "message_id": 61,
                "date": 1_700_000_000,
                "chat": {"id": -100, "type": "supergroup"},
                "message_thread_id": 7,
                "from": {"id": 9, "username": "alice"},
                "location": {
                    "latitude": 29.3,
                    "longitude": 47.9,
                    "horizontal_accuracy": 12.5,
                    "live_period": 60
                }
            }
        }),
        serde_json::json!({
            "update_id": 52,
            "message": {
                "message_id": 62,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 10, "username": "alice"},
                "venue": {
                    "location": {"latitude": 1.0, "longitude": 2.0},
                    "title": "Office",
                    "address": "Main Street"
                }
            }
        }),
        serde_json::json!({
            "update_id": 53,
            "message": {
                "message_id": 63,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 11, "username": "alice"},
                "contact": {
                    "phone_number": "+1234",
                    "first_name": "Ada",
                    "last_name": "Lovelace"
                }
            }
        }),
    ];

    for (index, update) in updates.iter().enumerate() {
        let message = channel
            .parse_update_message_or_voice(update)
            .await
            .expect("structured location/contact update is accepted");
        let envelope = TelegramChannel::inbound_envelope_from_update(update, &message, None);
        match index {
            0 => assert!(
                matches!(envelope.parts.as_slice(), [tinychannels_bus::ContentPart::Location {
                latitude, longitude, accuracy_meters: Some(12.5), live_until_unix_ms: Some(1_700_000_060_000), ..
            }] if *latitude == 29.3 && *longitude == 47.9)
            ),
            1 => assert!(
                matches!(envelope.parts.as_slice(), [tinychannels_bus::ContentPart::Location {
                latitude, longitude, label: Some(label), address: Some(address), ..
            }] if *latitude == 1.0 && *longitude == 2.0 && label == "Office" && address == "Main Street")
            ),
            2 => assert!(
                matches!(envelope.parts.as_slice(), [tinychannels_bus::ContentPart::Contact {
                display_name, phone_numbers, ..
            }] if display_name == "Ada Lovelace" && phone_numbers[0].value == "+1234")
            ),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn telegram_media_only_update_persists_into_scoped_blobs() {
    let server = mock_api().await;
    let png = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
        .unwrap();
    mount_post(
        &server,
        "getFile",
        ok_with(serde_json::json!({
            "file_path": "photos/one.png",
            "file_size": png.len()
        })),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("/file/bot{TOKEN}/photos/one.png")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(png.clone()),
        )
        .mount(&server)
        .await;

    let repository = ScopedMediaRepository::new();
    let channel =
        channel_for(&server, &["alice"]).with_media_repository(Arc::new(repository.clone()));
    let update = serde_json::json!({
        "update_id": 10,
        "message": {
            "message_id": 61,
            "chat": { "id": 888, "type": "supergroup" },
            "message_thread_id": 42,
            "from": { "id": 9, "username": "alice" },
            "photo": [{
                "file_id": "photo-file",
                "file_unique_id": "photo-unique",
                "file_size": png.len()
            }]
        }
    });
    let message = channel
        .parse_update_message_or_voice(&update)
        .await
        .expect("media-only update should become a channel message");
    let parts = channel
        .media_parts_for_ingress(&update, &message)
        .await
        .unwrap();
    let envelope = TelegramChannel::inbound_envelope_with_media(&update, &message, None, parts);
    let [tinychannels_bus::ContentPart::Image { media, .. }] = envelope.parts.as_slice() else {
        panic!("media-only update should emit its typed image part");
    };
    assert!(envelope.text.is_empty());
    assert!(
        media
            .opaque_id
            .as_deref()
            .unwrap()
            .starts_with("tinychannels_media_v1_")
    );
    assert_eq!(envelope.media.as_slice(), std::slice::from_ref(media));
    assert_eq!(media.path, None);
    assert_eq!(media.url, None);
    assert!(!media.opaque_id.as_deref().unwrap().contains("/file/bot"));

    let scope = repository.scopes.lock().unwrap()[0].clone();
    assert_eq!(scope.configured_instance, "telegram-main");
    assert_eq!(scope.channel.account_id.as_deref(), Some("telegram-main"));
    assert_eq!(scope.conversation.topic_id.as_deref(), Some("42"));
    assert_eq!(scope.turn_id, envelope.message_id);
    let stored = repository
        .read_media(scope, media.opaque_id.clone().unwrap(), 1024)
        .await
        .unwrap();
    assert_eq!(stored.bytes, png);
    assert_eq!(stored.content_type, "image/png");
}

#[tokio::test]
async fn telegram_document_download_infers_type_and_name_from_getfile_path() {
    let server = mock_api().await;
    let pdf = b"%PDF-1.7\n1 0 obj\nfixture".to_vec();
    mount_post(
        &server,
        "getFile",
        ok_with(serde_json::json!({
            "file_path": "documents/spec.pdf",
            "file_size": pdf.len()
        })),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("/file/bot{TOKEN}/documents/spec.pdf")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(pdf.clone()),
        )
        .mount(&server)
        .await;

    let repository = ScopedMediaRepository::new();
    let channel =
        channel_for(&server, &["alice"]).with_media_repository(Arc::new(repository.clone()));
    let update = serde_json::json!({
        "update_id": 12,
        "message": {
            "message_id": 62,
            "chat": { "id": 888, "type": "supergroup" },
            "from": { "id": 9, "username": "alice" },
            "document": { "file_id": "document-file", "file_unique_id": "document-unique" }
        }
    });
    let message = channel
        .parse_update_message_or_voice(&update)
        .await
        .expect("document-only update is accepted");
    let parts = channel
        .media_parts_for_ingress(&update, &message)
        .await
        .unwrap();
    let envelope = TelegramChannel::inbound_envelope_with_media(&update, &message, None, parts);
    let [
        tinychannels_bus::ContentPart::File {
            media, file_name, ..
        },
    ] = envelope.parts.as_slice()
    else {
        panic!("downloaded Telegram document should be retained as a typed file");
    };
    assert_eq!(file_name.as_deref(), Some("spec.pdf"));
    assert_eq!(media.file_name.as_deref(), Some("spec.pdf"));
    assert_eq!(media.content_type.as_deref(), Some("application/pdf"));

    let scope = repository.scopes.lock().unwrap()[0].clone();
    let stored = repository
        .read_media(scope, media.opaque_id.clone().unwrap(), 1024)
        .await
        .unwrap();
    assert_eq!(stored.bytes, pdf);
    assert_eq!(stored.content_type, "application/pdf");
}

#[tokio::test]
async fn telegram_audio_mime_hint_precedes_ambiguous_download_extension() {
    let server = mock_api().await;
    let mp4_audio = b"\0\0\0\x18ftypM4A \0\0\0\0fixture".to_vec();
    mount_post(
        &server,
        "getFile",
        ok_with(serde_json::json!({
            "file_path": "audio/recording.mp4",
            "file_size": mp4_audio.len()
        })),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("/file/bot{TOKEN}/audio/recording.mp4")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(mp4_audio.clone()),
        )
        .mount(&server)
        .await;

    let repository = ScopedMediaRepository::new();
    let channel =
        channel_for(&server, &["alice"]).with_media_repository(Arc::new(repository.clone()));
    let update = serde_json::json!({
        "update_id": 13,
        "message": {
            "message_id": 63,
            "chat": { "id": 888, "type": "supergroup" },
            "from": { "id": 9, "username": "alice" },
            "audio": {
                "file_id": "audio-file",
                "file_unique_id": "audio-unique",
                "mime_type": "audio/mp4"
            }
        }
    });
    let message = channel
        .parse_update_message_or_voice(&update)
        .await
        .expect("audio-only update is accepted");
    let parts = channel
        .media_parts_for_ingress(&update, &message)
        .await
        .unwrap();
    let envelope = TelegramChannel::inbound_envelope_with_media(&update, &message, None, parts);
    let [tinychannels_bus::ContentPart::Audio { media, .. }] = envelope.parts.as_slice() else {
        panic!("audio update should remain typed audio");
    };
    assert_eq!(media.file_name.as_deref(), Some("recording.mp4"));
    assert_eq!(media.content_type.as_deref(), Some("audio/mp4"));
    let scope = repository.scopes.lock().unwrap()[0].clone();
    let stored = repository
        .read_media(scope, media.opaque_id.clone().unwrap(), 1024)
        .await
        .unwrap();
    assert_eq!(stored.bytes, mp4_audio);
    assert_eq!(stored.content_type, "audio/mp4");
}

#[tokio::test]
async fn text_update_bypasses_voice_parser() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["*"]);
    let msg = ch
        .parse_update_message_or_voice(&private_text(
            serde_json::json!({ "id": 1, "username": "a" }),
            "plain",
        ))
        .await
        .unwrap();
    assert_eq!(msg.content, "plain");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn update_without_text_or_voice_yields_nothing() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["*"]);
    let update = serde_json::json!({
        "update_id": 1,
        "message": { "message_id": 1, "chat": { "id": 1 }, "sticker": {} }
    });
    assert!(ch.parse_update_message_or_voice(&update).await.is_none());
    assert!(TelegramChannel::parse_update_voice_attachment(&update).is_none());
}

#[tokio::test]
async fn voice_without_file_id_is_ignored() {
    let update = serde_json::json!({ "message": { "voice": { "file_id": "  " } } });
    assert!(TelegramChannel::parse_update_voice_attachment(&update).is_none());
}

// ── voice download failures ──────────────────────────────────────────────

async fn download_error(server: &MockServer) -> String {
    channel_for(server, &["*"])
        .download_telegram_voice_file("voice-id", Some("uniq"))
        .await
        .unwrap_err()
        .to_string()
}

#[tokio::test]
async fn download_reports_get_file_http_failure_with_token_redacted() {
    let server = mock_api().await;
    mount_post(
        &server,
        "getFile",
        ResponseTemplate::new(400).set_body_string(format!("bad token {TOKEN}")),
    )
    .await;
    let err = download_error(&server).await;
    assert!(err.contains("Telegram getFile failed"), "{err}");
    assert!(!err.contains(TOKEN), "{err}");
}

#[tokio::test]
async fn download_rejects_malformed_get_file_payloads() {
    for (body, expected) in [
        (serde_json::json!("not-an-object"), "missing result"),
        (
            serde_json::json!({ "ok": false, "description": "file is too big" }),
            "ok=false: file is too big",
        ),
        (serde_json::json!({ "ok": true }), "missing result"),
        (
            serde_json::json!({ "ok": true, "result": { "file_path": " " } }),
            "missing file_path",
        ),
        (
            serde_json::json!({ "ok": true, "result": {
                "file_path": "a.oga", "file_size": TELEGRAM_MAX_VOICE_FILE_BYTES + 1
            } }),
            "reported voice file too large",
        ),
    ] {
        let server = mock_api().await;
        let template = if body.is_string() {
            ResponseTemplate::new(200).set_body_string("{not json")
        } else {
            ResponseTemplate::new(200).set_body_json(body)
        };
        mount_post(&server, "getFile", template).await;
        let err = download_error(&server).await;
        let expected = if err.contains("invalid JSON") {
            "invalid JSON"
        } else {
            expected
        };
        assert!(err.contains(expected), "{err} (wanted {expected})");
    }
}

#[tokio::test]
async fn download_reports_file_fetch_failure_and_oversized_content_length() {
    let server = mock_api().await;
    mount_post(
        &server,
        "getFile",
        ok_with(serde_json::json!({ "file_path": "voice/v.oga" })),
    )
    .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404).set_body_string("gone"))
        .mount(&server)
        .await;
    let err = download_error(&server).await;
    assert!(err.contains("voice download failed"), "{err}");
    assert!(err.contains("gone"), "{err}");

    let big = oversized_file_server().await;
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = big;
    let err = ch
        .download_telegram_voice_file("voice-id", None)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("voice download too large"), "{err}");
}

#[tokio::test]
async fn download_reports_transport_failure_with_token_redacted() {
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = dead_base();
    let err = ch
        .download_telegram_voice_file("voice-id", None)
        .await
        .unwrap_err()
        .to_string();
    assert!(!err.contains(TOKEN), "{err}");
}

/// A raw HTTP server whose `getFile` answers normally but whose file download
/// advertises a Content-Length above the voice cap (without sending a body).
async fn oversized_file_server() -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let mut read = 0;
                loop {
                    let n = socket.read(&mut buf[read..]).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    read += n;
                    let head = String::from_utf8_lossy(&buf[..read]).to_string();
                    let Some(end) = head.find("\r\n\r\n") else {
                        continue;
                    };
                    let len = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if read < end + 4 + len {
                        continue;
                    }
                    let response = if head.starts_with("POST") {
                        let body = r#"{"ok":true,"result":{"file_path":"voice/big.oga"}}"#;
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                    } else {
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            TELEGRAM_MAX_VOICE_FILE_BYTES + 1
                        )
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                    return;
                }
            });
        }
    });
    format!("http://{addr}")
}

#[test]
fn normalize_incoming_content_without_mention_collapses_whitespace() {
    assert_eq!(
        TelegramChannel::normalize_incoming_content("  a \n b  ", "bot").as_deref(),
        Some("a b")
    );
    assert_eq!(
        TelegramChannel::normalize_incoming_content("   ", "bot"),
        None
    );
}
