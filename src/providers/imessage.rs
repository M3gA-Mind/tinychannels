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
mod native;

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
use crate::traits::{Channel, ChannelMessage, SendMessage};
use anyhow::Context as _;
use async_trait::async_trait;
use directories::UserDirs;
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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

#[async_trait]
impl Channel for IMessageChannel {
    fn name(&self) -> &str {
        "imessage"
    }

    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.declared()
    }

    async fn send_typed_with_receipt(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<crate::channel::ChannelSendOutcome> {
        crate::providers::native::outbound::send_typed(self, message)
            .await
            .map_err(crate::providers::native::to_anyhow)
    }

    fn supports_remote_control(&self) -> bool {
        true
    }
    fn supports_chat_approvals(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        // Defense-in-depth: validate target format before any interpolation
        if !is_valid_imessage_target(&message.recipient) {
            anyhow::bail!(
                "Invalid iMessage target: must be a phone number (+1234567890) or email (user@example.com)"
            );
        }

        // SECURITY: Escape both message AND target to prevent AppleScript injection
        // See: CWE-78 (OS Command Injection)
        let escaped_msg = escape_applescript(&message.content);
        let escaped_target = escape_applescript(&message.recipient);

        let script = format!(
            r#"tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetBuddy to participant "{escaped_target}" of targetService
    send "{escaped_msg}" to targetBuddy
end tell"#
        );

        let (program, leading_args) = self
            .script_command
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("no AppleScript runner configured"))?;
        let output = tokio::process::Command::new(program)
            .args(leading_args)
            .arg("-e")
            .arg(&script)
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("iMessage send failed: {stderr}");
        }

        Ok(())
    }

    async fn listen(&self, tx: mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
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

    async fn health_check(&self) -> bool {
        self.is_healthy_on(std::env::consts::OS)
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
