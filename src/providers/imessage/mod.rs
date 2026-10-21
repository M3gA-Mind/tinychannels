//! iMessage on the v2 adapter contract (macOS: `chat.db` in, `osascript` out).
//!
//! Inbound traffic is polled from the Messages database: text, attachments,
//! Apple audio messages (converted to AAC with `afconvert`, then stored as
//! voice notes), tapbacks (reactions) and read state of what we sent. AppleScript
//! cannot tapback, edit, unsend, type or thread, so those are declared
//! unsupported; sends return the message guid read back from `chat.db`.

mod capabilities;
mod db;
mod inbound;
mod listener;
mod outbound;

pub(super) use db::{fetch_new_messages, get_max_rowid};
pub(super) use outbound::{escape_applescript, is_valid_chat_guid, is_valid_imessage_target};

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
#[cfg(test)]
use crate::traits::ChannelMessage;
#[cfg(test)]
use anyhow::Context as _;
use directories::UserDirs;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
#[cfg(test)]
use tokio::sync::mpsc;
use tokio::sync::watch;

type ListenerCancellationSlot = Arc<StdMutex<Option<(u64, watch::Sender<bool>)>>>;

struct ListenerRegistration {
    slot: ListenerCancellationSlot,
    generation: u64,
}

impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        let mut active = self.slot.lock().unwrap_or_else(|error| error.into_inner());
        if active
            .as_ref()
            .is_some_and(|(generation, _)| *generation == self.generation)
        {
            *active = None;
        }
    }
}

#[derive(Clone, Copy)]
struct ListenerCursor {
    inbound_rowid: i64,
    read_date: i64,
    read_rowid: i64,
}

/// iMessage channel using macOS `AppleScript` bridge.
/// Polls the Messages database for new messages and sends replies via `osascript`.
#[derive(Clone)]
pub struct IMessageChannel {
    allowed_contacts: Vec<String>,
    poll_interval: Duration,
    /// Overrides `~/Library/Messages/chat.db`.
    db_path: Option<PathBuf>,
    /// The program and leading arguments that run an `AppleScript`;
    /// `-e <script>` is appended.
    script_command: Vec<String>,
    /// The program that converts an Apple audio message to AAC; it is called
    /// as `<program> -f m4af -d aac <in> <out>` (macOS `afconvert`).
    converter_command: Vec<String>,
    /// How long a send waits for Messages to record the message it made.
    resolve_timeout: Duration,
    native: NativeState,
    listener_cancel: ListenerCancellationSlot,
    listener_generation: Arc<AtomicU64>,
    listener_cursor: Arc<StdMutex<Option<ListenerCursor>>>,
}

impl IMessageChannel {
    pub fn new(allowed_contacts: Vec<String>) -> Self {
        Self {
            allowed_contacts,
            poll_interval: Duration::from_secs(3),
            db_path: None,
            script_command: vec!["osascript".to_string()],
            converter_command: vec!["afconvert".to_string()],
            resolve_timeout: Duration::from_secs(5),
            native: NativeState::default(),
            listener_cancel: Arc::new(StdMutex::new(None)),
            listener_generation: Arc::new(AtomicU64::new(0)),
            listener_cursor: Arc::new(StdMutex::new(None)),
        }
    }

    /// Scoped media storage; enables typed attachments in both directions.
    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Durable voice storage; enables audio messages in both directions.
    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Host speech-to-text for inbound audio messages.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    /// Enable shared final-turn synthesis when the host provides TTS, a codec
    /// and durable voice storage.
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.native.voice_synthesis = enabled;
        self
    }

    /// Ordered ingress for typed inbound messages and lifecycle events.
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// The Messages database: the override, else under the home directory.
    fn messages_db_path(&self) -> Option<PathBuf> {
        if let Some(path) = &self.db_path {
            return Some(path.clone());
        }
        UserDirs::new().map(|u| u.home_dir().join("Library/Messages/chat.db"))
    }

    /// Healthy only on macOS with a Messages database present.
    fn is_healthy_on(&self, os: &str) -> bool {
        if os != "macos" {
            return false;
        }
        self.messages_db_path().is_some_and(|path| path.exists())
    }
}

impl IMessageChannel {
    /// Run the listener and optionally signal after its startup cursor is fixed.
    /// The readiness sender is used by tests to synchronize first-message
    /// delivery with the same cursor boundary as the production listener.
    #[cfg(test)]
    async fn listen_with_ready(
        &self,
        tx: mpsc::Sender<ChannelMessage>,
        ready: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> anyhow::Result<()> {
        tracing::info!("iMessage channel listening (AppleScript bridge)...");

        // Query the Messages SQLite database for new messages
        // The database is at ~/Library/Messages/chat.db
        let db_path = self
            .messages_db_path()
            .context("Cannot find home directory")?;

        if !db_path.exists() {
            anyhow::bail!(
                "Messages database not found at {}. Ensure Messages.app is set up and Full Disk Access is granted.",
                db_path.display()
            );
        }

        // Open a persistent read-only connection instead of creating
        // a new one on every 3-second poll cycle.
        let path = db_path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || db::open_read_only(&path)).await??;

        // Track the newest inbound ROWID and read time already seen (shuttle
        // conn in and out).
        let (mut conn, initial_rowid, initial_read) =
            tokio::task::spawn_blocking(move || -> anyhow::Result<(Connection, i64, i64)> {
                let rowid = db::max_inbound_rowid(&conn)?;
                let read = db::max_date_read(&conn).unwrap_or(0);
                Ok((conn, rowid, read))
            })
            .await??;
        let mut last_rowid = initial_rowid;
        let mut last_read = initial_read;
        if let Some(ready) = ready {
            let _ = ready.send(());
        }

        loop {
            tokio::time::sleep(self.poll_interval).await;

            let (since, read_since) = (last_rowid, last_read);
            let (returned_conn, rows, reads) = tokio::task::spawn_blocking(move || {
                let rows = db::inbound_since(&conn, since);
                let reads = db::read_since(&conn, read_since);
                (conn, rows, reads)
            })
            .await
            .context("iMessage poll worker join error")?;
            conn = returned_conn;

            match reads {
                Ok(reads) => {
                    for read in reads {
                        last_read = last_read.max(read.date_read);
                        self.dispatch_read(read).await;
                    }
                }
                Err(e) => tracing::debug!("iMessage read-state poll error: {e}"),
            }
            match rows {
                Ok(rows) => {
                    for row in rows {
                        last_rowid = last_rowid.max(row.rowid);
                        if !self.dispatch_row(row, &tx).await {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("iMessage poll error: {e}");
                }
            }
        }
    }
}

crate::providers::outbound::native_v2_adapter!(IMessageChannel);

#[cfg(test)]
#[path = "fixtures_tests.rs"]
mod fixtures;

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod conformance;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
