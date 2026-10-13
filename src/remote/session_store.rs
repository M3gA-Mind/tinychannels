//! Chat → thread bindings and busy state for remote control, on the document
//! port.
//!
//! One store serves every channel. Documents are keyed by `<channel>:<reply
//! target>`, so the same chat id on two providers never collides. The store
//! holds a [`DocumentStore`] handle and nothing else: a linked host passes its
//! own, the loaded module passes the storage-bus client, and neither opens a
//! file.
//!
//! Isolation is the handle's job: bindings are keyed by channel and reply
//! target only, so a host serving several workspaces must give each its own
//! scoped [`DocumentStore`] (and therefore its own store and legacy marker).
//! `workspace_dir` is only the source of the one-time legacy import.
//!
//! Hosts that predate the port wrote `state/channel_remote_sessions.json`
//! (and, before that, a Telegram-only `state/telegram_remote_sessions.json`
//! keyed by bare reply target). [`RemoteSessionStore::adopt_legacy`] imports
//! those bindings once. The legacy files are read, never modified, and a
//! binding already in storage always wins over an imported one.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tinystoragedrivers_core::{CollectionSpec, DocumentStore, DocumentStoreExt, Precondition};

const LEGACY_STORE_FILE: &str = "state/channel_remote_sessions.json";
const LEGACY_TELEGRAM_STORE_FILE: &str = "state/telegram_remote_sessions.json";
const LOG_PREFIX: &str = "[channel-remote]";

/// Collection of chat → thread bindings.
pub const BINDINGS_COLLECTION: &str = "channel_remote_bindings";
/// Collection of chats with a turn in flight.
pub const BUSY_COLLECTION: &str = "channel_remote_busy";
/// Collection holding the one-time legacy import marker.
pub const META_COLLECTION: &str = "channel_remote_meta";
const LEGACY_MARKER_ID: &str = "legacy-import";

/// A chat's binding to a host conversation thread.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatBinding {
    pub thread_id: String,
    pub sender_key: String,
    pub updated_at: String,
    /// Title captured at `/new` time so `/status` can show it without listing
    /// every thread.
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct LegacyStoreFile {
    #[serde(default)]
    bindings: HashMap<String, ChatBinding>,
}

/// Store key for one chat on one channel.
pub fn chat_key(channel: &str, reply_target: &str) -> String {
    format!("{channel}:{reply_target}")
}

/// The remote-control session store over one scoped document handle.
#[derive(Clone, Debug)]
pub struct RemoteSessionStore {
    docs: Option<Arc<dyn DocumentStore>>,
    ready: Arc<tokio::sync::OnceCell<()>>,
}

impl RemoteSessionStore {
    /// A store over `docs`. Collections are declared on first use.
    #[must_use]
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            docs: Some(docs),
            ready: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// A store for a host that provides no storage: every operation fails with
    /// "storage unavailable", which the command flow reports to the chat (and
    /// `/help` never touches).
    #[must_use]
    pub fn unavailable() -> Self {
        Self {
            docs: None,
            ready: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    async fn docs(&self) -> anyhow::Result<&Arc<dyn DocumentStore>> {
        let docs = self
            .docs
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("host storage is unavailable"))?;
        self.ready
            .get_or_init(|| async {
                for name in [BINDINGS_COLLECTION, BUSY_COLLECTION, META_COLLECTION] {
                    crate::storage::ensure_collection(docs, CollectionSpec::new(name)).await;
                }
            })
            .await;
        Ok(docs)
    }

    /// The binding for one chat, if any.
    pub async fn binding(
        &self,
        channel: &str,
        reply_target: &str,
    ) -> anyhow::Result<Option<ChatBinding>> {
        let found = self
            .docs()
            .await?
            .get_as::<ChatBinding>(BINDINGS_COLLECTION, &chat_key(channel, reply_target))
            .await
            .map_err(|e| anyhow::anyhow!(crate::storage::describe(&e)))?;
        Ok(found.map(|v| v.doc))
    }

    /// Bind a chat to a thread, replacing any earlier binding.
    pub async fn set_binding(
        &self,
        channel: &str,
        reply_target: &str,
        thread_id: String,
        sender_key: String,
        title: Option<String>,
    ) -> anyhow::Result<()> {
        let binding = ChatBinding {
            thread_id,
            sender_key,
            updated_at: chrono::Utc::now().to_rfc3339(),
            title,
        };
        self.docs()
            .await?
            .put_as(
                BINDINGS_COLLECTION,
                &chat_key(channel, reply_target),
                &binding,
                Precondition::None,
            )
            .await
            .map_err(|e| anyhow::anyhow!(crate::storage::describe(&e)))?;
        Ok(())
    }

    /// Record whether a turn is running for a chat.
    pub async fn set_busy(
        &self,
        channel: &str,
        reply_target: &str,
        busy: bool,
    ) -> anyhow::Result<()> {
        let id = chat_key(channel, reply_target);
        let docs = self.docs().await?;
        let result = if busy {
            let doc =
                serde_json::json!({ "busy": true, "updated_at": chrono::Utc::now().to_rfc3339() });
            docs.put(BUSY_COLLECTION, &id, doc, Precondition::None)
                .await
                .map(|_| ())
        } else {
            docs.delete(BUSY_COLLECTION, &id, Precondition::None)
                .await
                .map(|_| ())
        };
        result.map_err(|e| anyhow::anyhow!(crate::storage::describe(&e)))
    }

    /// Whether a turn is recorded as running for a chat.
    pub async fn is_busy(&self, channel: &str, reply_target: &str) -> anyhow::Result<bool> {
        let found = self
            .docs()
            .await?
            .get(BUSY_COLLECTION, &chat_key(channel, reply_target))
            .await
            .map_err(|e| anyhow::anyhow!(crate::storage::describe(&e)))?;
        Ok(found.is_some())
    }

    /// Import bindings from the legacy JSON files under `workspace_dir`, once.
    ///
    /// Idempotent: a marker document records completion, and each binding is
    /// written with `Absent`, so a binding made through the port is never
    /// overwritten. Busy flags are per-process turn state and do not carry
    /// over. Returns how many bindings were imported.
    pub async fn adopt_legacy(&self, workspace_dir: &Path) -> anyhow::Result<usize> {
        let docs = self.docs().await?;
        let marker = docs
            .get(META_COLLECTION, LEGACY_MARKER_ID)
            .await
            .map_err(|e| anyhow::anyhow!(crate::storage::describe(&e)))?;
        if marker.is_some() {
            return Ok(0);
        }
        let mut legacy: HashMap<String, ChatBinding> = HashMap::new();
        // The Telegram-only file is older; the shared file wins on overlap.
        for (file, prefix) in [
            (LEGACY_TELEGRAM_STORE_FILE, true),
            (LEGACY_STORE_FILE, false),
        ] {
            let path = workspace_dir.join(file);
            let Some(parsed) = read_legacy(&path)? else {
                continue;
            };
            for (key, binding) in parsed.bindings {
                let key = if prefix {
                    chat_key("telegram", &key)
                } else {
                    key
                };
                legacy.insert(key, binding);
            }
        }
        let mut imported = 0;
        for (key, binding) in legacy {
            let doc = serde_json::to_value(&binding)?;
            match docs
                .put(BINDINGS_COLLECTION, &key, doc, Precondition::Absent)
                .await
            {
                Ok(_) => imported += 1,
                Err(error)
                    if matches!(
                        error.kind(),
                        tinystoragedrivers_core::ErrorKind::Conflict
                            | tinystoragedrivers_core::ErrorKind::AlreadyExists
                    ) => {}
                Err(error) => return Err(anyhow::anyhow!(crate::storage::describe(&error))),
            }
        }
        docs.put(
            META_COLLECTION,
            LEGACY_MARKER_ID,
            serde_json::json!({ "imported": imported }),
            Precondition::Absent,
        )
        .await
        .or_else(|error| match error.kind() {
            tinystoragedrivers_core::ErrorKind::Conflict
            | tinystoragedrivers_core::ErrorKind::AlreadyExists => {
                Ok(tinystoragedrivers_core::Version(0))
            }
            _ => Err(anyhow::anyhow!(crate::storage::describe(&error))),
        })?;
        tracing::info!(
            "{LOG_PREFIX} adopted {imported} legacy binding(s) from {}",
            workspace_dir.display()
        );
        Ok(imported)
    }
}

fn read_legacy(path: &Path) -> anyhow::Result<Option<LegacyStoreFile>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)?;
    Ok(Some(serde_json::from_str(&raw).unwrap_or_else(|error| {
        tracing::warn!(
            "{LOG_PREFIX} corrupt legacy session file at {}: {error}; skipping",
            path.display()
        );
        LegacyStoreFile::default()
    })))
}

/// Record whether a turn is running for a chat, for `/status`. Failures are
/// logged, never returned: busy state is advisory.
pub async fn mark_turn(store: &RemoteSessionStore, channel: &str, reply_target: &str, busy: bool) {
    if let Err(error) = store.set_busy(channel, reply_target, busy).await {
        tracing::warn!(
            "{LOG_PREFIX} failed to persist busy={busy} channel={channel} reply_target={reply_target}: {error}"
        );
    }
}
