//! Read-only queries over the Messages `chat.db`: inbound messages with their
//! attachments and tapbacks, and read state of messages we sent.
//!
//! The shell's `imessage_scanner` (`crates/openhuman-app/src/imessage_scanner/`)
//! also reads `chat.db`, but it runs in the desktop process and ingests history
//! into memory, while this reader runs inside the channels module and follows
//! new messages for a live conversation. The module cannot depend on the app
//! and the app must not depend on the module's implementation, so each keeps
//! its own small reader over the same documented schema.

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::Path;

/// Seconds between the Unix epoch and the Apple epoch (2001-01-01).
const APPLE_EPOCH_OFFSET_SECS: i64 = 978_307_200;

/// One attachment row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachmentRow {
    pub(crate) path: Option<String>,
    pub(crate) mime_type: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) bytes: i64,
}

/// One inbound message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InboundRow {
    pub(crate) rowid: i64,
    pub(crate) guid: String,
    pub(crate) sender: String,
    pub(crate) text: Option<String>,
    pub(crate) date: i64,
    pub(crate) audio_message: bool,
    pub(crate) associated_guid: Option<String>,
    pub(crate) associated_type: i64,
    /// The group chat's guid, `None` for a direct conversation.
    pub(crate) group_guid: Option<String>,
    pub(crate) attachments: Vec<AttachmentRow>,
}

/// One of our messages the recipient has read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadRow {
    pub(crate) rowid: i64,
    pub(crate) guid: String,
    pub(crate) recipient: String,
    pub(crate) date_read: i64,
}

const INBOUND_SQL: &str = "SELECT m.ROWID, m.guid, h.id, m.text, m.date, \
     COALESCE(m.is_audio_message, 0), m.associated_message_guid, \
     COALESCE(m.associated_message_type, 0), \
     (SELECT c.guid FROM chat c JOIN chat_message_join j ON j.chat_id = c.ROWID \
        WHERE j.message_id = m.ROWID AND c.style = 43 LIMIT 1) \
     FROM message m JOIN handle h ON m.handle_id = h.ROWID \
     WHERE m.ROWID > ?1 AND m.is_from_me = 0 \
     ORDER BY m.ROWID ASC LIMIT 20";

const ATTACHMENTS_SQL: &str = "SELECT a.filename, a.mime_type, a.transfer_name, \
     COALESCE(a.total_bytes, 0) \
     FROM attachment a JOIN message_attachment_join j ON j.attachment_id = a.ROWID \
     WHERE j.message_id = ?1 ORDER BY a.ROWID ASC";

const READ_SQL: &str = "SELECT m.ROWID, m.guid, h.id, m.date_read \
     FROM message m JOIN handle h ON m.handle_id = h.ROWID \
     WHERE m.is_from_me = 1 AND m.date_read > 0 \
       AND (m.date_read > ?1 OR (m.date_read = ?1 AND m.ROWID > ?2)) \
     ORDER BY m.date_read ASC, m.ROWID ASC LIMIT 20";

/// Inbound messages newer than `since_rowid`, oldest first, at most 20.
pub(crate) fn inbound_since(
    conn: &Connection,
    since_rowid: i64,
) -> anyhow::Result<Vec<InboundRow>> {
    let mut stmt = conn.prepare(INBOUND_SQL)?;
    let mut rows = stmt
        .query_map([since_rowid], |row| {
            Ok(InboundRow {
                rowid: row.get(0)?,
                guid: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                sender: row.get(2)?,
                text: row.get(3)?,
                date: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
                audio_message: row.get::<_, i64>(5)? != 0,
                associated_guid: row.get(6)?,
                associated_type: row.get(7)?,
                group_guid: row.get(8)?,
                attachments: Vec::new(),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut attachments = conn.prepare(ATTACHMENTS_SQL)?;
    for row in &mut rows {
        row.attachments = attachments
            .query_map([row.rowid], |attachment| {
                Ok(AttachmentRow {
                    path: attachment.get(0)?,
                    mime_type: attachment.get(1)?,
                    name: attachment.get(2)?,
                    bytes: attachment.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
    }
    Ok(rows)
}

/// The newest `date_read` among our sent messages, or 0.
#[cfg(test)]
pub(crate) fn max_date_read(conn: &Connection) -> anyhow::Result<i64> {
    let value: Option<i64> = conn.query_row(
        "SELECT MAX(date_read) FROM message WHERE is_from_me = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(value.unwrap_or(0))
}

/// The newest read receipt cursor, ordered by `(date_read, ROWID)`.
pub(crate) fn max_read_cursor(conn: &Connection) -> anyhow::Result<(i64, i64)> {
    let cursor = conn
        .query_row(
            "SELECT date_read, ROWID FROM message WHERE is_from_me = 1 AND date_read > 0 \
             ORDER BY date_read DESC, ROWID DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(cursor.unwrap_or((0, 0)))
}

/// Our sent messages read after `since`, oldest read first.
#[cfg(test)]
pub(crate) fn read_since(conn: &Connection, since: i64) -> anyhow::Result<Vec<ReadRow>> {
    read_since_cursor(conn, since, i64::MAX)
}

/// Our sent messages after a composite receipt cursor, oldest first.
pub(crate) fn read_since_cursor(
    conn: &Connection,
    since_date: i64,
    since_rowid: i64,
) -> anyhow::Result<Vec<ReadRow>> {
    let mut stmt = conn.prepare(READ_SQL)?;
    let rows = stmt.query_map([since_date, since_rowid], |row| {
        Ok(ReadRow {
            rowid: row.get(0)?,
            guid: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            recipient: row.get(2)?,
            date_read: row.get(3)?,
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

/// The newest message we sent after `baseline`: `(rowid, guid)`.
pub(crate) fn newest_sent_after(
    conn: &Connection,
    baseline: i64,
) -> anyhow::Result<Option<(i64, String)>> {
    let mut stmt = conn.prepare(
        "SELECT ROWID, guid FROM message WHERE is_from_me = 1 AND ROWID > ?1 \
         ORDER BY ROWID DESC LIMIT 1",
    )?;
    let found = stmt
        .query_map([baseline], |row| {
            Ok((
                row.get(0)?,
                row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            ))
        })?
        .next()
        .transpose()?;
    Ok(found)
}

/// The newest ROWID among messages we sent, or 0.
pub(crate) fn max_sent_rowid(conn: &Connection) -> anyhow::Result<i64> {
    let value: Option<i64> = conn.query_row(
        "SELECT MAX(ROWID) FROM message WHERE is_from_me = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(value.unwrap_or(0))
}

/// Unix milliseconds for a Messages `date` column. Newer macOS stores Apple
/// epoch nanoseconds, older releases seconds.
pub(crate) fn unix_ms(date: i64) -> Option<u64> {
    if date <= 0 {
        return None;
    }
    let seconds = if date > 1_000_000_000_000 {
        date / 1_000_000_000
    } else {
        date
    };
    let millis = (date.rem_euclid(1_000_000_000) / 1_000_000).max(0);
    let unix_seconds = seconds + APPLE_EPOCH_OFFSET_SECS;
    let fraction = if date > 1_000_000_000_000 { millis } else { 0 };
    u64::try_from(unix_seconds * 1000 + fraction).ok()
}

/// The tapback a Messages `associated_message_type` encodes:
/// `(emoji, removed)`.
pub(crate) fn tapback(kind: i64) -> Option<(&'static str, bool)> {
    let emoji = |offset: i64| match offset {
        0 => Some("❤️"),
        1 => Some("👍"),
        2 => Some("👎"),
        3 => Some("😂"),
        4 => Some("‼️"),
        5 => Some("❓"),
        _ => None,
    };
    match kind {
        2000..=2005 => emoji(kind - 2000).map(|e| (e, false)),
        3000..=3005 => emoji(kind - 3000).map(|e| (e, true)),
        _ => None,
    }
}

/// The guid of the message an `associated_message_guid` points at
/// (`p:0/GUID`, `bp:GUID` or a bare guid).
pub(crate) fn target_guid(associated: &str) -> &str {
    let associated = associated.rsplit('/').next().unwrap_or(associated);
    associated.strip_prefix("bp:").unwrap_or(associated)
}

#[cfg(test)]
#[path = "db_tests.rs"]
pub(crate) mod tests;

/// The newest-first poll: inbound messages with text, newer than `?1`.
const NEW_MESSAGES_SQL: &str = "SELECT m.ROWID, h.id, m.text \
     FROM message m \
     JOIN handle h ON m.handle_id = h.ROWID \
     WHERE m.ROWID > ?1 \
     AND m.is_from_me = 0 \
     AND m.text IS NOT NULL \
     ORDER BY m.ROWID ASC \
     LIMIT 20";

/// Opens the Messages database read-only.
pub(super) fn open_read_only(path: &Path) -> anyhow::Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

/// The newest inbound ROWID, or 0 for an empty table.
pub(super) fn max_inbound_rowid(conn: &Connection) -> anyhow::Result<i64> {
    let mut stmt = conn.prepare("SELECT MAX(ROWID) FROM message WHERE is_from_me = 0")?;
    let rowid: Option<i64> = stmt.query_row([], |row| row.get(0))?;
    Ok(rowid.unwrap_or(0))
}

/// Inbound messages newer than `since_rowid`, oldest first, at most 20.
/// The rowid is bound as a parameter (CWE-89 prevention).
pub(super) fn query_new_messages(
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
pub(super) async fn get_max_rowid(db_path: &Path) -> anyhow::Result<i64> {
    let path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || max_inbound_rowid(&open_read_only(&path)?)).await?
}

/// Fetch messages newer than `since_rowid`.
/// Uses rusqlite with parameterized queries for security (CWE-89 prevention).
/// The `since_rowid` parameter is bound safely, preventing SQL injection.
#[allow(dead_code)]
pub(super) async fn fetch_new_messages(
    db_path: &Path,
    since_rowid: i64,
) -> anyhow::Result<Vec<(i64, String, String)>> {
    let path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || query_new_messages(&open_read_only(&path)?, since_rowid))
        .await?
}
