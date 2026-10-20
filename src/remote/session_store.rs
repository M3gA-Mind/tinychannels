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

use serde::Deserialize;
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, DocumentStoreExt, Key, Precondition, WriteOptions,
};

use crate::storage::schema::{self, SCHEMA_VERSION};

const LEGACY_STORE_FILE: &str = "state/channel_remote_sessions.json";
const LEGACY_TELEGRAM_STORE_FILE: &str = "state/telegram_remote_sessions.json";
const LOG_PREFIX: &str = "[channel-remote]";

/// Collection of chat → thread bindings.
pub const BINDINGS_COLLECTION: &str = schema::REMOTE_BINDINGS;
/// Collection of chats with a turn in flight.
pub const BUSY_COLLECTION: &str = schema::REMOTE_BUSY;
/// Collection holding the one-time legacy import marker.
pub const META_COLLECTION: &str = schema::REMOTE_META;
const LEGACY_MARKER_ID: &str = "legacy-import";

pub use tinychannels_bus::storage::ChatBinding;

#[derive(Debug, Default, Deserialize)]
struct LegacyStoreFile {
    #[serde(default)]
    bindings: HashMap<String, ChatBinding>,
}

/// Document id for one chat on one channel: [`Key::of`] over
/// `[channel, reply_target]`, so the same chat id on two providers never
/// collides and a target holding `:` or `/` stays one unambiguous key.
pub fn chat_key(channel: &str, reply_target: &str) -> String {
    Key::of([channel, reply_target]).map_or_else(
        // Two parts always make a key; the fallback only keeps this infallible.
        |_| format!("{channel}/{reply_target}"),
        |key| key.as_str().to_owned(),
    )
}

/// The write options every v2 document is stamped with.
fn write_options() -> WriteOptions {
    WriteOptions::new().schema(SCHEMA_VERSION)
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
            title,
        };
        self.docs()
            .await?
            .put_with(
                BINDINGS_COLLECTION,
                &chat_key(channel, reply_target),
                serde_json::to_value(&binding)?,
                Precondition::None,
                write_options(),
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
            docs.put_with(
                BUSY_COLLECTION,
                &id,
                serde_json::json!({ "busy": true }),
                Precondition::None,
                write_options(),
            )
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
        // (channel, reply target) -> binding.
        let mut legacy: HashMap<(String, String), ChatBinding> = HashMap::new();
        let mut unreadable = false;
        // The Telegram-only file is older; the shared file wins on overlap.
        for (file, prefix) in [
            (LEGACY_TELEGRAM_STORE_FILE, true),
            (LEGACY_STORE_FILE, false),
        ] {
            let path = workspace_dir.join(file);
            let Some(parsed) = read_legacy(&path)? else {
                continue;
            };
            let Some(parsed) = parsed else {
                // Unreadable now; leave the marker unwritten so a repaired
                // file is imported by a later call.
                unreadable = true;
                continue;
            };
            for (key, binding) in parsed.bindings {
                let chat = if prefix {
                    Some(("telegram".to_owned(), key))
                } else {
                    key.split_once(':')
                        .map(|(channel, target)| (channel.to_owned(), target.to_owned()))
                };
                match chat {
                    Some(chat) => {
                        legacy.insert(chat, binding);
                    }
                    None => tracing::warn!(
                        "{LOG_PREFIX} legacy binding key is not <channel>:<reply target>; skipped"
                    ),
                }
            }
        }
        let mut imported = 0;
        for ((channel, target), binding) in legacy {
            let doc = serde_json::to_value(&binding)?;
            match docs
                .put_with(
                    BINDINGS_COLLECTION,
                    &chat_key(&channel, &target),
                    doc,
                    Precondition::Absent,
                    write_options(),
                )
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
        if unreadable {
            tracing::warn!(
                "{LOG_PREFIX} legacy session file unreadable; import not marked complete, will retry"
            );
            return Ok(imported);
        }
        docs.put_with(
            META_COLLECTION,
            LEGACY_MARKER_ID,
            serde_json::json!({ "imported": imported }),
            Precondition::Absent,
            write_options(),
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

/// `Ok(None)`: no file. `Ok(Some(None))`: the file exists but is not valid
/// JSON (logged). `Err`: the file could not be read.
fn read_legacy(path: &Path) -> anyhow::Result<Option<Option<LegacyStoreFile>>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)?;
    Ok(Some(match serde_json::from_str(&raw) {
        Ok(parsed) => Some(parsed),
        Err(error) => {
            tracing::warn!(
                "{LOG_PREFIX} corrupt legacy session file at {}: {error}; skipping",
                path.display()
            );
            None
        }
    }))
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
