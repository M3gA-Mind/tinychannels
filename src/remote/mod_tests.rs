//! Remote control against an in-memory host.

use std::collections::HashMap;
use std::sync::Mutex;

use super::session_store::{BINDINGS_COLLECTION, BUSY_COLLECTION};
use super::session_store::{RemoteSessionStore, chat_key};
use super::*;
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

#[derive(Default)]
struct FakeHost {
    history: Mutex<HashMap<String, usize>>,
    threads: Mutex<Vec<RemoteThreadSummary>>,
    created: Mutex<Vec<NewRemoteThread>>,
    invalidated: Mutex<Vec<String>>,
    fail_list: bool,
    fail_create: bool,
}

#[async_trait]
impl RemoteControlHost for FakeHost {
    fn route(&self, _sender_key: &str) -> RemoteRoute {
        RemoteRoute {
            provider: "openai".into(),
            model: "gpt-5".into(),
        }
    }
    fn history_len(&self, sender_key: &str) -> usize {
        self.history
            .lock()
            .unwrap()
            .get(sender_key)
            .copied()
            .unwrap_or(0)
    }
    fn clear_history(&self, sender_key: &str) {
        self.history.lock().unwrap().remove(sender_key);
    }
    async fn list_threads(&self) -> anyhow::Result<Vec<RemoteThreadSummary>> {
        if self.fail_list {
            anyhow::bail!("disk on fire");
        }
        Ok(self.threads.lock().unwrap().clone())
    }
    async fn create_thread(&self, thread: NewRemoteThread) -> anyhow::Result<()> {
        if self.fail_create {
            anyhow::bail!("read-only");
        }
        self.created.lock().unwrap().push(thread);
        Ok(())
    }
    async fn invalidate_thread(&self, thread_id: &str) {
        self.invalidated.lock().unwrap().push(thread_id.into());
    }
}

fn store() -> RemoteSessionStore {
    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("tenant:remote-tests").unwrap())
        .unwrap();
    RemoteSessionStore::new(std::sync::Arc::clone(storage.documents()))
}

fn ctx(channel: &str, sessions: &RemoteSessionStore) -> RemoteCommandContext {
    RemoteCommandContext {
        channel: channel.into(),
        reply_target: "chat-1".into(),
        sender_key: format!("{channel}_alice_chat-1"),
        sessions: sessions.clone(),
    }
}

fn thread(id: &str, title: &str, at: &str) -> RemoteThreadSummary {
    RemoteThreadSummary {
        id: id.into(),
        title: title.into(),
        message_count: 3,
        last_message_at: at.into(),
    }
}

#[test]
fn parses_commands_across_mentions_and_case() {
    assert_eq!(
        parse_remote_command(" /STATUS@OpenHumanBot now "),
        Some(RemoteCommand::Status)
    );
    assert_eq!(
        parse_remote_command("/sessions"),
        Some(RemoteCommand::Sessions)
    );
    assert_eq!(parse_remote_command("/new"), Some(RemoteCommand::New));
    assert_eq!(parse_remote_command("/help"), Some(RemoteCommand::Help));
    assert_eq!(parse_remote_command("/model"), None);
    assert_eq!(parse_remote_command("status"), None);
    assert_eq!(parse_remote_command(""), None);
}

#[test]
fn renderers_include_their_inputs() {
    assert!(build_remote_help_response().contains("`/status`"));
    assert!(format_session_line("", "thread-1", 2, true).starts_with("→ `thread-1`"));
    assert!(format_session_line("Title", "t", 2, false).starts_with("  `Title`"));
    assert!(build_new_session_response("Today", "thread-1").contains("thread-1"));
    assert!(
        build_status_response("Thread: none", "openai", "gpt-5", 3, true).contains("in progress")
    );
    assert!(build_status_response("Thread: none", "openai", "gpt-5", 3, false).contains("idle"));
}

#[tokio::test]
async fn new_then_status_on_any_chat_provider() {
    for channel in ["telegram", "discord", "slack"] {
        let sessions = store();
        let host = FakeHost::default();
        let c = ctx(channel, &sessions);
        host.history.lock().unwrap().insert(c.sender_key.clone(), 4);

        let before = execute_remote_command(&host, &c, RemoteCommand::Status).await;
        assert!(before.contains("none — send `/new`"), "{channel}: {before}");
        assert!(before.contains("In-memory turns: 4"));

        let reply = execute_remote_command(&host, &c, RemoteCommand::New).await;
        let created = host.created.lock().unwrap().clone();
        assert_eq!(created.len(), 1);
        assert_eq!(
            created[0].labels,
            vec![channel.to_string(), "remote".into()]
        );
        assert!(reply.contains(&created[0].id));
        assert_eq!(host.history_len(&c.sender_key), 0, "history cleared");
        assert_eq!(
            *host.invalidated.lock().unwrap(),
            vec![created[0].id.clone()]
        );

        let after = execute_remote_command(&host, &c, RemoteCommand::Status).await;
        assert!(after.contains(&created[0].id), "{channel}: {after}");
        assert!(after.contains("Provider: `openai`"));
    }
}

#[tokio::test]
async fn new_session_titles_use_the_channel_display_name() {
    let sessions = store();
    let host = FakeHost::default();
    let now = chrono::DateTime::parse_from_rfc3339("2026-09-29T15:04:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    new_session(&host, &ctx("telegram", &sessions), now).await;
    new_session(&host, &ctx("slack", &sessions), now).await;
    let titles: Vec<_> = host
        .created
        .lock()
        .unwrap()
        .iter()
        .map(|t| t.title.clone())
        .collect();
    assert_eq!(titles, ["Telegram Sep 29 3:04 PM", "Slack Sep 29 3:04 PM"]);
}

#[tokio::test]
async fn new_session_reports_create_failure_without_binding() {
    let sessions = store();
    let host = FakeHost {
        fail_create: true,
        ..Default::default()
    };
    let c = ctx("discord", &sessions);
    let reply = execute_remote_command(&host, &c, RemoteCommand::New).await;
    assert_eq!(reply, "Failed to create session: read-only");
    assert!(
        sessions
            .binding("discord", "chat-1")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sessions_lists_recent_first_and_marks_active() {
    let sessions = store();
    let host = FakeHost::default();
    let c = ctx("telegram", &sessions);
    assert!(
        execute_remote_command(&host, &c, RemoteCommand::Sessions)
            .await
            .contains("No conversation threads yet")
    );
    execute_remote_command(&host, &c, RemoteCommand::New).await;
    let active = host.created.lock().unwrap()[0].id.clone();
    {
        let mut threads = host.threads.lock().unwrap();
        threads.push(thread("old", "Old", "2026-01-01"));
        threads.push(thread(&active, "Active", "2026-03-01"));
        for i in 0..10 {
            threads.push(thread(&format!("filler-{i}"), "F", "2025-01-01"));
        }
    }
    let listing = execute_remote_command(&host, &c, RemoteCommand::Sessions).await;
    let lines: Vec<_> = listing.lines().collect();
    assert_eq!(lines.len(), 3 + SESSIONS_LIST_LIMIT);
    assert!(lines[3].starts_with("→ `Active`"));
    assert!(lines[4].contains("`Old`"));

    let failing = FakeHost {
        fail_list: true,
        ..Default::default()
    };
    assert_eq!(
        execute_remote_command(&failing, &c, RemoteCommand::Sessions).await,
        "Could not list sessions: disk on fire"
    );
}

#[tokio::test]
async fn help_needs_no_state() {
    let sessions = store();
    let reply = execute_remote_command(
        &FakeHost::default(),
        &ctx("irc", &sessions),
        RemoteCommand::Help,
    )
    .await;
    assert_eq!(reply, build_remote_help_response());
}

#[tokio::test]
async fn busy_state_is_per_channel_and_chat() {
    let sessions = store();
    mark_turn(&sessions, "discord", "chat-1", true).await;
    let busy = sessions.is_busy("discord", "chat-1").await.unwrap();
    let other_channel = sessions.is_busy("telegram", "chat-1").await.unwrap();
    assert!(busy && !other_channel);
    let status = execute_remote_command(
        &FakeHost::default(),
        &ctx("discord", &sessions),
        RemoteCommand::Status,
    )
    .await;
    assert!(status.contains("in progress"));
    mark_turn(&sessions, "discord", "chat-1", false).await;
    assert!(!sessions.is_busy("discord", "chat-1").await.unwrap());
}

#[tokio::test]
async fn legacy_telegram_bindings_are_adopted_once_and_left_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("state/telegram_remote_sessions.json");
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    let legacy_raw = r#"{"bindings":{"chat-9":{"thread_id":"thread-old","sender_key":"telegram_bob_chat-9","updated_at":"2026-01-01T00:00:00Z","title":"Old"}},"busy_reply_targets":{"chat-9":true}}"#;
    std::fs::write(&legacy, legacy_raw).unwrap();

    let sessions = store();
    assert_eq!(sessions.adopt_legacy(dir.path()).await.unwrap(), 1);
    let binding = sessions
        .binding("telegram", "chat-9")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.thread_id, "thread-old");
    assert!(
        !sessions.is_busy("telegram", "chat-9").await.unwrap(),
        "busy is not adopted"
    );
    assert!(
        sessions
            .binding("discord", "chat-9")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(std::fs::read_to_string(&legacy).unwrap(), legacy_raw);
    assert!(
        !dir.path()
            .join("state/channel_remote_sessions.json")
            .exists()
    );

    // The import runs once: a later change to the legacy file, or a binding the
    // chat has since moved to, is never overwritten.
    sessions
        .set_binding("telegram", "chat-9", "thread-new".into(), "k".into(), None)
        .await
        .unwrap();
    std::fs::write(&legacy, legacy_raw.replace("thread-old", "thread-stale")).unwrap();
    assert_eq!(sessions.adopt_legacy(dir.path()).await.unwrap(), 0);
    assert_eq!(
        sessions
            .binding("telegram", "chat-9")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "thread-new"
    );
}

#[tokio::test]
async fn shared_legacy_file_wins_over_the_telegram_only_file() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let binding = |thread: &str| {
        format!(
            r#"{{"thread_id":"{thread}","sender_key":"k","updated_at":"2026-01-01T00:00:00Z"}}"#
        )
    };
    std::fs::write(
        state.join("telegram_remote_sessions.json"),
        format!(r#"{{"bindings":{{"c":{}}}}}"#, binding("telegram-only")),
    )
    .unwrap();
    std::fs::write(
        state.join("channel_remote_sessions.json"),
        format!(r#"{{"bindings":{{"telegram:c":{}}}}}"#, binding("shared")),
    )
    .unwrap();
    let sessions = store();
    sessions.adopt_legacy(dir.path()).await.unwrap();
    assert_eq!(
        sessions
            .binding("telegram", "c")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "shared"
    );
}

#[tokio::test]
async fn corrupt_legacy_file_is_skipped_instead_of_failing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state/channel_remote_sessions.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "{not json").unwrap();
    let sessions = store();
    assert_eq!(sessions.adopt_legacy(dir.path()).await.unwrap(), 0);
    assert!(sessions.binding("telegram", "x").await.unwrap().is_none());
    assert_eq!(chat_key("slack", "C1"), "slack/C1");
}

#[tokio::test]
async fn a_corrupt_legacy_file_is_imported_once_it_is_repaired() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state/channel_remote_sessions.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "{not json").unwrap();
    let sessions = store();
    assert_eq!(sessions.adopt_legacy(dir.path()).await.unwrap(), 0);

    write_legacy_binding(dir.path(), "telegram:c", "thread-repaired");
    assert_eq!(sessions.adopt_legacy(dir.path()).await.unwrap(), 1);
    assert_eq!(
        sessions
            .binding("telegram", "c")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "thread-repaired"
    );
    // Now complete: the marker is written and later calls are no-ops.
    assert_eq!(sessions.adopt_legacy(dir.path()).await.unwrap(), 0);
}

#[tokio::test]
async fn unavailable_storage_degrades_status_and_fails_new_without_losing_help() {
    let sessions = RemoteSessionStore::unavailable();
    let host = FakeHost::default();
    let c = ctx("telegram", &sessions);
    assert_eq!(
        execute_remote_command(&host, &c, RemoteCommand::Help).await,
        build_remote_help_response()
    );
    let status = execute_remote_command(&host, &c, RemoteCommand::Status).await;
    assert!(status.contains("none"), "{status}");
    let new = execute_remote_command(&host, &c, RemoteCommand::New).await;
    assert!(new.contains("failed to persist the chat binding"), "{new}");
    assert!(sessions.binding("telegram", "chat-1").await.is_err());
}

fn write_legacy_binding(dir: &std::path::Path, key: &str, thread: &str) {
    let path = dir.join("state/channel_remote_sessions.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        format!(
            r#"{{"bindings":{{"{key}":{{"thread_id":"{thread}","sender_key":"k","updated_at":"2026-01-01T00:00:00Z"}}}}}}"#
        ),
    )
    .unwrap();
}

#[tokio::test]
async fn a_binding_already_in_storage_is_not_replaced_by_the_import() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_binding(dir.path(), "telegram:c", "thread-legacy");
    let sessions = store();
    sessions
        .set_binding("telegram", "c", "thread-live".into(), "k".into(), None)
        .await
        .unwrap();
    assert_eq!(sessions.adopt_legacy(dir.path()).await.unwrap(), 0);
    assert_eq!(
        sessions
            .binding("telegram", "c")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "thread-live"
    );
}

#[tokio::test]
async fn two_concurrent_imports_leave_one_binding_and_both_succeed() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_binding(dir.path(), "telegram:c", "thread-legacy");
    let sessions = store();
    let (a, b) = tokio::join!(
        sessions.adopt_legacy(dir.path()),
        sessions.adopt_legacy(dir.path())
    );
    assert!(a.is_ok() && b.is_ok());
    assert_eq!(
        sessions
            .binding("telegram", "c")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "thread-legacy"
    );
}

#[tokio::test]
async fn marking_a_turn_on_unavailable_storage_is_advisory_and_never_panics() {
    super::session_store::mark_turn(&RemoteSessionStore::unavailable(), "telegram", "c", true)
        .await;
}

#[tokio::test]
async fn bindings_and_busy_flags_are_stored_in_the_v2_shape_under_key_ids() {
    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("tenant:remote-tests").unwrap())
        .unwrap();
    let docs = std::sync::Arc::clone(storage.documents());
    let sessions = RemoteSessionStore::new(std::sync::Arc::clone(&docs));
    sessions
        .set_binding("discord", "room/1:2", "th".into(), "sk".into(), None)
        .await
        .unwrap();
    sessions
        .set_busy("discord", "room/1:2", true)
        .await
        .unwrap();

    let id = chat_key("discord", "room/1:2");
    assert_eq!(id, "discord/room%2F1:2");
    let binding = docs.get(BINDINGS_COLLECTION, &id).await.unwrap().unwrap();
    assert_eq!(
        binding.doc,
        serde_json::json!({ "thread_id": "th", "sender_key": "sk", "title": null })
    );
    assert_eq!(binding.meta.schema, 2);
    let busy = docs.get(BUSY_COLLECTION, &id).await.unwrap().unwrap();
    assert_eq!(busy.doc, serde_json::json!({ "busy": true }));
    assert!(sessions.is_busy("discord", "room/1:2").await.unwrap());
}
