use super::*;
use rusqlite::Connection;

/// A `chat.db` with the columns the adapter reads.
pub(crate) fn schema() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    create_schema(&conn);
    conn
}

pub(crate) fn create_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT NOT NULL);
         CREATE TABLE message (
            ROWID INTEGER PRIMARY KEY, guid TEXT, handle_id INTEGER, text TEXT,
            is_from_me INTEGER DEFAULT 0, date INTEGER, is_audio_message INTEGER DEFAULT 0,
            associated_message_guid TEXT, associated_message_type INTEGER DEFAULT 0,
            date_read INTEGER DEFAULT 0);
         CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, style INTEGER);
         CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
         CREATE TABLE attachment (ROWID INTEGER PRIMARY KEY, filename TEXT, mime_type TEXT,
            transfer_name TEXT, total_bytes INTEGER);
         CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
         INSERT INTO handle VALUES (1, '+15551111');",
    )
    .unwrap();
}

fn insert(conn: &Connection, rowid: i64, from_me: i64, text: Option<&str>) {
    conn.execute(
        "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date) \
         VALUES (?1, ?2, 1, ?3, ?4, 700000000000000000)",
        rusqlite::params![rowid, format!("G{rowid}"), text, from_me],
    )
    .unwrap();
}

#[test]
fn inbound_rows_carry_attachments_audio_and_group_chats() {
    let conn = schema();
    insert(&conn, 1, 0, Some("hello"));
    insert(&conn, 2, 0, None);
    insert(&conn, 3, 1, Some("mine"));
    conn.execute_batch(
        "UPDATE message SET is_audio_message = 1 WHERE ROWID = 2;
         INSERT INTO attachment VALUES (1, '~/a.caf', 'audio/x-caf', 'Audio Message.caf', 9);
         INSERT INTO message_attachment_join VALUES (2, 1);
         INSERT INTO chat VALUES (1, 'iMessage;+;chat1', 43), (2, 'iMessage;-;+1555', 45);
         INSERT INTO chat_message_join VALUES (1, 1), (2, 2);",
    )
    .unwrap();
    let rows = inbound_since(&conn, 0).unwrap();
    assert_eq!(rows.len(), 2, "our own messages are not inbound");
    assert_eq!(rows[0].group_guid.as_deref(), Some("iMessage;+;chat1"));
    assert_eq!(rows[0].text.as_deref(), Some("hello"));
    assert!(!rows[0].audio_message && rows[0].attachments.is_empty());
    assert!(rows[1].audio_message && rows[1].group_guid.is_none());
    assert_eq!(
        rows[1].attachments[0].mime_type.as_deref(),
        Some("audio/x-caf")
    );
    assert_eq!(rows[1].attachments[0].bytes, 9);
    assert_eq!(inbound_since(&conn, 1).unwrap().len(), 1);
    assert!(inbound_since(&conn, 2).unwrap().is_empty());
}

#[test]
fn read_state_is_followed_by_date_read() {
    let conn = schema();
    insert(&conn, 1, 1, Some("a"));
    insert(&conn, 2, 1, Some("b"));
    assert_eq!(max_date_read(&conn).unwrap(), 0);
    conn.execute_batch(
        "UPDATE message SET date_read = 100 WHERE ROWID = 1;
         UPDATE message SET date_read = 200 WHERE ROWID = 2;",
    )
    .unwrap();
    assert_eq!(max_date_read(&conn).unwrap(), 200);
    let rows = read_since(&conn, 100).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].guid, "G2");
    assert_eq!(rows[0].recipient, "+15551111");
    assert_eq!(read_since(&conn, 0).unwrap().len(), 2);
}

#[test]
fn read_receipt_cursor_orders_equal_timestamps_by_rowid_and_pages_them() {
    let conn = schema();
    for rowid in 1..=22 {
        insert(&conn, rowid, 1, Some("sent"));
    }
    conn.execute(
        "UPDATE message SET date_read = 100 WHERE is_from_me = 1",
        [],
    )
    .unwrap();

    let first = read_since_cursor(&conn, 0, 0).unwrap();
    assert_eq!(first.len(), 20);
    assert_eq!(first.first().unwrap().rowid, 1);
    assert_eq!(first.last().unwrap().rowid, 20);
    let next = read_since_cursor(&conn, 100, 20).unwrap();
    assert_eq!(next.len(), 2);
    assert_eq!(next[0].rowid, 21);
    assert_eq!(next[1].rowid, 22);
    assert_eq!(max_read_cursor(&conn).unwrap(), (100, 22));
}

#[test]
fn sent_messages_are_found_after_a_baseline() {
    let conn = schema();
    assert_eq!(max_sent_rowid(&conn).unwrap(), 0);
    assert!(newest_sent_after(&conn, 0).unwrap().is_none());
    insert(&conn, 4, 1, Some("sent"));
    insert(&conn, 5, 0, Some("received"));
    assert_eq!(max_sent_rowid(&conn).unwrap(), 4);
    assert_eq!(
        newest_sent_after(&conn, 0).unwrap(),
        Some((4, "G4".to_owned()))
    );
    assert!(newest_sent_after(&conn, 4).unwrap().is_none());
}

#[test]
fn dates_convert_from_apple_epoch_nanoseconds_and_seconds() {
    // 1001.5 seconds after 2001-01-01, in nanoseconds.
    assert_eq!(unix_ms(1_001_500_000_000), Some(978_308_201_500));
    assert_eq!(unix_ms(10), Some(978_307_210_000));
    assert_eq!(unix_ms(0), None);
    assert_eq!(unix_ms(-5), None);
}

#[test]
fn tapbacks_map_to_emoji_and_direction() {
    assert_eq!(tapback(2001), Some(("👍", false)));
    assert_eq!(tapback(3003), Some(("😂", true)));
    assert_eq!(tapback(2005), Some(("❓", false)));
    assert_eq!(tapback(2006), None);
    assert_eq!(tapback(0), None);
    assert_eq!(tapback(1000), None);
}

#[test]
fn associated_guids_drop_their_prefixes() {
    assert_eq!(target_guid("p:0/ABC"), "ABC");
    assert_eq!(target_guid("bp:ABC"), "ABC");
    assert_eq!(target_guid("ABC"), "ABC");
}
