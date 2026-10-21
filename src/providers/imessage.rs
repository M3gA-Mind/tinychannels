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
mod native;

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
use crate::traits::ChannelMessage;
use anyhow::Context as _;
use directories::UserDirs;
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

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

/// The newest-first poll: inbound messages with text, newer than `?1`.
const NEW_MESSAGES_SQL: &str = "SELECT m.ROWID, h.id, m.text \
     FROM message m \
     JOIN handle h ON m.handle_id = h.ROWID \
     WHERE m.ROWID > ?1 \
     AND m.is_from_me = 0 \
     AND m.text IS NOT NULL \
     ORDER BY m.ROWID ASC \
     LIMIT 20";

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

    fn is_contact_allowed(&self, sender: &str) -> bool {
        if self.allowed_contacts.iter().any(|u| u == "*") {
            return true;
        }
        self.allowed_contacts
            .iter()
            .any(|u| u.eq_ignore_ascii_case(sender))
    }
}

/// Escape a string for safe interpolation into `AppleScript`.
///
/// This prevents injection attacks by escaping:
/// - Backslashes (`\` → `\\`)
/// - Double quotes (`"` → `\"`)
/// - Newlines (`\n` → `\\n`, `\r` → `\\r`) to prevent code injection via line breaks
fn escape_applescript(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Validate that a target looks like a valid phone number or email address.
///
/// This is a defense-in-depth measure to reject obviously malicious targets
/// before they reach `AppleScript` interpolation.
///
/// Valid patterns:
/// - Phone: starts with `+` followed by digits (with optional spaces/dashes)
/// - Email: contains `@` with alphanumeric chars on both sides
fn is_valid_imessage_target(target: &str) -> bool {
    let target = target.trim();
    if target.is_empty() {
        return false;
    }

    // Phone number: +1234567890 or +1 234-567-8900
    if target.starts_with('+') {
        let digits_only: String = target.chars().filter(char::is_ascii_digit).collect();
        // Must have at least 7 digits (shortest valid phone numbers)
        return digits_only.len() >= 7 && digits_only.len() <= 15;
    }

    // Email: simple validation (contains @ with chars on both sides)
    if let Some(at_pos) = target.find('@') {
        let local = &target[..at_pos];
        let domain = &target[at_pos + 1..];

        // Local part: non-empty, alphanumeric + common email chars
        let local_valid = !local.is_empty()
            && local
                .chars()
                .all(|c| c.is_alphanumeric() || "._+-".contains(c));

        // Domain: non-empty, contains a dot, alphanumeric + dots/hyphens
        let domain_valid = !domain.is_empty()
            && domain.contains('.')
            && domain
                .chars()
                .all(|c| c.is_alphanumeric() || ".-".contains(c));

        return local_valid && domain_valid;
    }

    false
}

/// A Messages chat guid such as `iMessage;+;chat123456789`.
fn is_valid_chat_guid(target: &str) -> bool {
    let mut parts = target.split(';');
    let (Some(service), Some(style), Some(id), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    matches!(service, "iMessage" | "SMS" | "any")
        && matches!(style, "+" | "-")
        && !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '+' | '@'))
}

impl IMessageChannel {
    /// Run the listener and optionally signal after its startup cursor is fixed.
    /// The readiness sender is used by tests to synchronize first-message
    /// delivery with the same cursor boundary as the production listener.
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
        let conn = tokio::task::spawn_blocking(move || open_read_only(&path)).await??;

        // Track the newest inbound ROWID and read time already seen (shuttle
        // conn in and out).
        let (mut conn, initial_rowid, initial_read) =
            tokio::task::spawn_blocking(move || -> anyhow::Result<(Connection, i64, i64)> {
                let rowid = max_inbound_rowid(&conn)?;
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

/// Opens the Messages database read-only.
fn open_read_only(path: &Path) -> anyhow::Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

/// The newest inbound ROWID, or 0 for an empty table.
fn max_inbound_rowid(conn: &Connection) -> anyhow::Result<i64> {
    let mut stmt = conn.prepare("SELECT MAX(ROWID) FROM message WHERE is_from_me = 0")?;
    let rowid: Option<i64> = stmt.query_row([], |row| row.get(0))?;
    Ok(rowid.unwrap_or(0))
}

/// Inbound messages newer than `since_rowid`, oldest first, at most 20.
/// The rowid is bound as a parameter (CWE-89 prevention).
fn query_new_messages(
    conn: &Connection,
    since_rowid: i64,
) -> anyhow::Result<Vec<(i64, String, String)>> {
    let mut stmt = conn.prepare(NEW_MESSAGES_SQL)?;
    let rows = stmt.query_map([since_rowid], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

#[allow(dead_code)]
/// Get the current max ROWID from the messages table.
/// Uses rusqlite with parameterized queries for security (CWE-89 prevention).
async fn get_max_rowid(db_path: &Path) -> anyhow::Result<i64> {
    let path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || max_inbound_rowid(&open_read_only(&path)?)).await?
}

/// Fetch messages newer than `since_rowid`.
/// Uses rusqlite with parameterized queries for security (CWE-89 prevention).
/// The `since_rowid` parameter is bound safely, preventing SQL injection.
#[allow(dead_code)]
async fn fetch_new_messages(
    db_path: &Path,
    since_rowid: i64,
) -> anyhow::Result<Vec<(i64, String, String)>> {
    let path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || query_new_messages(&open_read_only(&path)?, since_rowid))
        .await?
}

crate::providers::native::native_v2_adapter!(IMessageChannel);

#[cfg(test)]
#[path = "imessage/fixtures_tests.rs"]
mod fixtures;

#[cfg(test)]
#[path = "imessage/conformance_tests.rs"]
mod conformance;

#[cfg(test)]
#[path = "imessage_tests.rs"]
mod tests;
