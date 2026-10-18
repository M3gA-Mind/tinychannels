use super::*;
use tokio::time::timeout;

#[test]
fn creates_with_contacts() {
    let ch = IMessageChannel::new(vec!["+1234567890".into()]);
    assert_eq!(ch.allowed_contacts.len(), 1);
    assert_eq!(ch.poll_interval, Duration::from_secs(3));
}

#[test]
fn creates_with_empty_contacts() {
    let ch = IMessageChannel::new(vec![]);
    assert!(ch.allowed_contacts.is_empty());
}

#[test]
fn wildcard_allows_anyone() {
    let ch = IMessageChannel::new(vec!["*".into()]);
    assert!(ch.is_contact_allowed("+1234567890"));
    assert!(ch.is_contact_allowed("random@icloud.com"));
    assert!(ch.is_contact_allowed(""));
}

#[test]
fn specific_contact_allowed() {
    let ch = IMessageChannel::new(vec!["+1234567890".into(), "user@icloud.com".into()]);
    assert!(ch.is_contact_allowed("+1234567890"));
    assert!(ch.is_contact_allowed("user@icloud.com"));
}

#[test]
fn unknown_contact_denied() {
    let ch = IMessageChannel::new(vec!["+1234567890".into()]);
    assert!(!ch.is_contact_allowed("+9999999999"));
    assert!(!ch.is_contact_allowed("hacker@evil.com"));
}

#[test]
fn contact_case_insensitive() {
    let ch = IMessageChannel::new(vec!["User@iCloud.com".into()]);
    assert!(ch.is_contact_allowed("user@icloud.com"));
    assert!(ch.is_contact_allowed("USER@ICLOUD.COM"));
}

#[test]
fn empty_allowlist_denies_all() {
    let ch = IMessageChannel::new(vec![]);
    assert!(!ch.is_contact_allowed("+1234567890"));
    assert!(!ch.is_contact_allowed("anyone"));
}

#[test]
fn name_returns_imessage() {
    let ch = IMessageChannel::new(vec![]);
    assert_eq!(ch.name(), "imessage");
}

#[test]
fn wildcard_among_others_still_allows_all() {
    let ch = IMessageChannel::new(vec!["+111".into(), "*".into(), "+222".into()]);
    assert!(ch.is_contact_allowed("totally-unknown"));
}

#[test]
fn contact_with_spaces_exact_match() {
    let ch = IMessageChannel::new(vec!["  spaced  ".into()]);
    assert!(ch.is_contact_allowed("  spaced  "));
    assert!(!ch.is_contact_allowed("spaced"));
}

// ══════════════════════════════════════════════════════════
// AppleScript Escaping Tests (CWE-78 Prevention)
// ══════════════════════════════════════════════════════════

#[test]
fn escape_applescript_double_quotes() {
    assert_eq!(escape_applescript(r#"hello "world""#), r#"hello \"world\""#);
}

#[test]
fn escape_applescript_backslashes() {
    assert_eq!(escape_applescript(r"path\to\file"), r"path\\to\\file");
}

#[test]
fn escape_applescript_mixed() {
    assert_eq!(
        escape_applescript(r#"say "hello\" world"#),
        r#"say \"hello\\\" world"#
    );
}

#[test]
fn escape_applescript_injection_attempt() {
    // This is the exact attack vector from the security report
    let malicious = r#"" & do shell script "id" & ""#;
    let escaped = escape_applescript(malicious);
    // After escaping, the quotes should be escaped and not break out
    assert_eq!(escaped, r#"\" & do shell script \"id\" & \""#);
    // Verify all quotes are now escaped (preceded by backslash)
    // The escaped string should not have any unescaped quotes (quote not preceded by backslash)
    let chars: Vec<char> = escaped.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c == '"' {
            // Every quote must be preceded by a backslash
            assert!(
                i > 0 && chars[i - 1] == '\\',
                "Found unescaped quote at position {i}"
            );
        }
    }
}

#[test]
fn escape_applescript_empty_string() {
    assert_eq!(escape_applescript(""), "");
}

#[test]
fn escape_applescript_no_special_chars() {
    assert_eq!(escape_applescript("hello world"), "hello world");
}

#[test]
fn escape_applescript_unicode() {
    assert_eq!(escape_applescript("hello 🦀 world"), "hello 🦀 world");
}

#[test]
fn escape_applescript_newlines_escaped() {
    assert_eq!(escape_applescript("line1\nline2"), "line1\\nline2");
    assert_eq!(escape_applescript("line1\rline2"), "line1\\rline2");
    assert_eq!(escape_applescript("line1\r\nline2"), "line1\\r\\nline2");
}

// ══════════════════════════════════════════════════════════
// Target Validation Tests
// ══════════════════════════════════════════════════════════

#[test]
fn valid_phone_number_simple() {
    assert!(is_valid_imessage_target("+1234567890"));
}

#[test]
fn valid_phone_number_with_country_code() {
    assert!(is_valid_imessage_target("+14155551234"));
}

#[test]
fn valid_phone_number_with_spaces() {
    assert!(is_valid_imessage_target("+1 415 555 1234"));
}

#[test]
fn valid_phone_number_with_dashes() {
    assert!(is_valid_imessage_target("+1-415-555-1234"));
}

#[test]
fn valid_phone_number_international() {
    assert!(is_valid_imessage_target("+447911123456")); // UK
    assert!(is_valid_imessage_target("+81312345678")); // Japan
}

#[test]
fn valid_email_simple() {
    assert!(is_valid_imessage_target("user@example.com"));
}

#[test]
fn valid_email_with_subdomain() {
    assert!(is_valid_imessage_target("user@mail.example.com"));
}

#[test]
fn valid_email_with_plus() {
    assert!(is_valid_imessage_target("user+tag@example.com"));
}

#[test]
fn valid_email_with_dots() {
    assert!(is_valid_imessage_target("first.last@example.com"));
}

#[test]
fn valid_email_icloud() {
    assert!(is_valid_imessage_target("user@icloud.com"));
    assert!(is_valid_imessage_target("user@me.com"));
}

#[test]
fn invalid_target_empty() {
    assert!(!is_valid_imessage_target(""));
    assert!(!is_valid_imessage_target("   "));
}

#[test]
fn invalid_target_no_plus_prefix() {
    // Phone numbers must start with +
    assert!(!is_valid_imessage_target("1234567890"));
}

#[test]
fn invalid_target_too_short_phone() {
    // Less than 7 digits
    assert!(!is_valid_imessage_target("+123456"));
}

#[test]
fn invalid_target_too_long_phone() {
    // More than 15 digits
    assert!(!is_valid_imessage_target("+1234567890123456"));
}

#[test]
fn invalid_target_email_no_at() {
    assert!(!is_valid_imessage_target("userexample.com"));
}

#[test]
fn invalid_target_email_no_domain() {
    assert!(!is_valid_imessage_target("user@"));
}

#[test]
fn invalid_target_email_no_local() {
    assert!(!is_valid_imessage_target("@example.com"));
}

#[test]
fn invalid_target_email_no_dot_in_domain() {
    assert!(!is_valid_imessage_target("user@localhost"));
}

#[test]
fn invalid_target_injection_attempt() {
    // The exact attack vector from the security report
    assert!(!is_valid_imessage_target(r#"" & do shell script "id" & ""#));
}

#[test]
fn invalid_target_applescript_injection() {
    // Various injection attempts
    assert!(!is_valid_imessage_target(r#"test" & quit"#));
    assert!(!is_valid_imessage_target(r"test\ndo shell script"));
    assert!(!is_valid_imessage_target("test\"; malicious code; \""));
}

#[test]
fn invalid_target_special_chars() {
    assert!(!is_valid_imessage_target("user<script>@example.com"));
    assert!(!is_valid_imessage_target("user@example.com; rm -rf /"));
}

#[test]
fn invalid_target_null_byte() {
    assert!(!is_valid_imessage_target("user\0@example.com"));
}

#[test]
fn invalid_target_newline() {
    assert!(!is_valid_imessage_target("user\n@example.com"));
}

#[test]
fn target_with_leading_trailing_whitespace_trimmed() {
    // Should trim and validate
    assert!(is_valid_imessage_target("  +1234567890  "));
    assert!(is_valid_imessage_target("  user@example.com  "));
}

// ══════════════════════════════════════════════════════════
// SQLite/rusqlite Database Tests (CWE-89 Prevention)
// ══════════════════════════════════════════════════════════

/// Helper to create a temporary test database with Messages schema
fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("chat.db");

    let conn = Connection::open(&db_path).unwrap();
    db::tests::create_schema(&conn);
    // The adapter's own fixtures seed a handle; these tests insert their own.
    conn.execute("DELETE FROM handle", []).unwrap();

    (dir, db_path)
}

#[tokio::test]
async fn get_max_rowid_empty_database() {
    let (_dir, db_path) = create_test_db();
    let result = get_max_rowid(&db_path).await;
    assert!(result.is_ok());
    // Empty table returns 0 (NULL coalesced)
    assert_eq!(result.unwrap(), 0);
}

#[tokio::test]
async fn get_max_rowid_with_messages() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (100, 1, 'Hello', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (200, 1, 'World', 0)",
            [],
        )
        .unwrap();
        // This one is from_me=1, should be ignored
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (300, 1, 'Sent', 1)",
            [],
        )
        .unwrap();
    }

    let result = get_max_rowid(&db_path).await.unwrap();
    // Should return 200, not 300 (ignores is_from_me=1)
    assert_eq!(result, 200);
}

#[tokio::test]
async fn get_max_rowid_nonexistent_database() {
    let path = std::path::Path::new("/nonexistent/path/chat.db");
    let result = get_max_rowid(path).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn fetch_new_messages_empty_database() {
    let (_dir, db_path) = create_test_db();
    let result = fetch_new_messages(&db_path, 0).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
async fn fetch_new_messages_returns_correct_data() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (2, 'user@example.com')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'First message', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 2, 'Second message', 0)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0],
        (10, "+1234567890".to_string(), "First message".to_string())
    );
    assert_eq!(
        result[1],
        (
            20,
            "user@example.com".to_string(),
            "Second message".to_string()
        )
    );
}

#[tokio::test]
async fn fetch_new_messages_filters_by_rowid() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Old message', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, 'New message', 0)",
            []
        ).unwrap();
    }

    // Fetch only messages after ROWID 15
    let result = fetch_new_messages(&db_path, 15).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, 20);
    assert_eq!(result[0].2, "New message");
}

#[tokio::test]
async fn fetch_new_messages_excludes_sent_messages() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Received', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, 'Sent by me', 1)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "Received");
}

#[tokio::test]
async fn fetch_new_messages_excludes_null_text() {
    let (_dir, db_path) = create_test_db();

    // Insert test data
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Has text', 0)",
            []
        ).unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, NULL, 0)",
            [],
        )
        .unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "Has text");
}

#[tokio::test]
async fn fetch_new_messages_respects_limit() {
    let (_dir, db_path) = create_test_db();

    // Insert 25 messages (limit is 20)
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        for i in 1..=25 {
            conn.execute(
                &format!("INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES ({i}, 1, 'Message {i}', 0)"),
                []
            ).unwrap();
        }
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 20); // Limited to 20
    assert_eq!(result[0].0, 1); // First message
    assert_eq!(result[19].0, 20); // 20th message
}

#[tokio::test]
async fn fetch_new_messages_ordered_by_rowid_asc() {
    let (_dir, db_path) = create_test_db();

    // Insert messages out of order
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (30, 1, 'Third', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'First', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (20, 1, 'Second', 0)",
            [],
        )
        .unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, 10);
    assert_eq!(result[1].0, 20);
    assert_eq!(result[2].0, 30);
}

#[tokio::test]
async fn fetch_new_messages_nonexistent_database() {
    let path = std::path::Path::new("/nonexistent/path/chat.db");
    let result = fetch_new_messages(path, 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn fetch_new_messages_handles_special_characters() {
    let (_dir, db_path) = create_test_db();

    // Insert message with special characters (potential SQL injection patterns)
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Hello \"world'' OR 1=1; DROP TABLE message;--', 0)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    // The special characters should be preserved, not interpreted as SQL
    assert!(result[0].2.contains("DROP TABLE"));
}

#[tokio::test]
async fn fetch_new_messages_handles_unicode() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Hello 🦀 世界 مرحبا', 0)",
            []
        ).unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "Hello 🦀 世界 مرحبا");
}

#[tokio::test]
async fn fetch_new_messages_handles_empty_text() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, '', 0)",
            [],
        )
        .unwrap();
    }

    let result = fetch_new_messages(&db_path, 0).await.unwrap();
    // Empty string is NOT NULL, so it's included
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "");
}

#[tokio::test]
async fn fetch_new_messages_negative_rowid_edge_case() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Test', 0)",
            [],
        )
        .unwrap();
    }

    // Negative rowid should still work (fetch all messages with ROWID > -1)
    let result = fetch_new_messages(&db_path, -1).await.unwrap();
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn fetch_new_messages_large_rowid_edge_case() {
    let (_dir, db_path) = create_test_db();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+1234567890')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (10, 1, 'Test', 0)",
            [],
        )
        .unwrap();
    }

    // Very large rowid should return empty (no messages after this)
    let result = fetch_new_messages(&db_path, i64::MAX - 1).await.unwrap();
    assert!(result.is_empty());
}

// ══════════════════════════════════════════════════════════
// Channel behaviour behind the db-path and script-runner seams
// ══════════════════════════════════════════════════════════

const HANG_GUARD: Duration = Duration::from_secs(20);

fn channel_on(db_path: &Path, contacts: &[&str]) -> IMessageChannel {
    let mut channel = IMessageChannel::new(contacts.iter().map(|c| c.to_string()).collect());
    channel.db_path = Some(db_path.to_path_buf());
    channel.poll_interval = Duration::from_millis(5);
    channel
}

/// A runner that writes its `-e` script to `out` and exits with `code`.
fn fake_runner(out: &Path, code: i32) -> Vec<String> {
    vec![
        "sh".to_string(),
        "-c".to_string(),
        format!(
            "printf '%s' \"$2\" > '{}'; echo 'runner said no' >&2; exit {code}",
            out.display()
        ),
        "fake-osascript".to_string(),
    ]
}

#[tokio::test]
async fn send_runs_the_escaped_script_through_the_runner() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("script.txt");
    let mut channel = IMessageChannel::new(vec![]);
    channel.script_command = fake_runner(&out, 0);

    channel
        .send(&SendMessage::new("say \"hi\"", "+15555550123"))
        .await
        .unwrap();

    let script = std::fs::read_to_string(&out).unwrap();
    assert!(script.contains("tell application \"Messages\""), "{script}");
    assert!(script.contains("participant \"+15555550123\""), "{script}");
    assert!(
        script.contains("send \"say \\\"hi\\\"\" to targetBuddy"),
        "{script}"
    );
}

#[tokio::test]
async fn send_reports_the_runner_stderr_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut channel = IMessageChannel::new(vec![]);
    channel.script_command = fake_runner(&dir.path().join("script.txt"), 1);

    let error = channel
        .send(&SendMessage::new("hello", "user@example.com"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("iMessage send failed"), "{error}");
    assert!(error.contains("runner said no"), "{error}");
}

#[tokio::test]
async fn send_rejects_an_invalid_target_before_running_anything() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("script.txt");
    let mut channel = IMessageChannel::new(vec![]);
    channel.script_command = fake_runner(&out, 0);

    let error = channel
        .send(&SendMessage::new("hello", "not a target"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Invalid iMessage target"), "{error}");
    assert!(!out.exists(), "the runner must not be invoked");
}

#[tokio::test]
async fn send_without_a_runner_is_an_error() {
    let mut channel = IMessageChannel::new(vec![]);
    channel.script_command = Vec::new();
    assert!(
        channel
            .send(&SendMessage::new("hello", "+15555550123"))
            .await
            .is_err()
    );
}

#[test]
fn the_default_database_lives_under_the_home_directory() {
    let channel = IMessageChannel::new(vec![]);
    if let Some(path) = channel.messages_db_path() {
        assert!(path.ends_with("Library/Messages/chat.db"), "{path:?}");
    }
    assert_eq!(channel.script_command, vec!["osascript".to_string()]);
}

#[test]
fn health_requires_macos_and_an_existing_database() {
    let (_dir, db_path) = create_test_db();
    let present = channel_on(&db_path, &[]);
    assert!(present.is_healthy_on("macos"));
    assert!(!present.is_healthy_on("linux"));

    let missing = channel_on(&db_path.with_file_name("absent.db"), &[]);
    assert!(!missing.is_healthy_on("macos"));
}

#[tokio::test]
async fn health_check_reflects_the_host_os() {
    let (_dir, db_path) = create_test_db();
    let channel = channel_on(&db_path, &[]);
    assert_eq!(
        channel.health_check().await,
        std::env::consts::OS == "macos"
    );
}

#[tokio::test]
async fn listen_fails_when_the_database_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let channel = channel_on(&dir.path().join("chat.db"), &["*"]);
    let (tx, _rx) = mpsc::channel(1);
    let error = channel.listen(tx).await.unwrap_err().to_string();
    assert!(error.contains("Messages database not found"), "{error}");
}

fn insert_message(conn: &Connection, rowid: i64, handle: i64, text: &str) {
    conn.execute(
        "INSERT INTO message (ROWID, handle_id, text, is_from_me) VALUES (?1, ?2, ?3, 0)",
        rusqlite::params![rowid, handle, text],
    )
    .unwrap();
}

/// Keeps adding one round of messages (a blocked sender, a blank text, then
/// an allowed one) until the listener delivers something, and returns it.
/// The listener only reports rows newer than its startup baseline, so the
/// rounds keep coming until one lands after it.
async fn next_delivery(
    writer: &Connection,
    rowid: &mut i64,
    rx: &mut mpsc::Receiver<ChannelMessage>,
) -> ChannelMessage {
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    loop {
        for (handle, text) in [(2, "from a stranger"), (1, "   "), (1, "hello")] {
            *rowid += 1;
            insert_message(writer, *rowid, handle, text);
        }
        if let Ok(Some(message)) = timeout(Duration::from_millis(200), rx.recv()).await {
            return message;
        }
        assert!(tokio::time::Instant::now() < deadline, "no delivery");
    }
}

#[tokio::test]
async fn listen_delivers_only_allowed_non_blank_messages() {
    let (_dir, db_path) = create_test_db();
    let writer = Connection::open(&db_path).unwrap();
    writer
        .execute_batch(
            "INSERT INTO handle (ROWID, id) VALUES (1, '+15555550123');
             INSERT INTO handle (ROWID, id) VALUES (2, '+19999999999');",
        )
        .unwrap();
    insert_message(&writer, 1, 1, "before startup");
    let mut rowid = 1;

    let channel = channel_on(&db_path, &["+15555550123"]);
    let (tx, mut rx) = mpsc::channel(8);
    let listen = tokio::spawn(async move { channel.listen(tx).await });

    let first = next_delivery(&writer, &mut rowid, &mut rx).await;
    assert_eq!(first.sender, "+15555550123");
    assert_eq!(first.reply_target, "+15555550123");
    assert_eq!(first.content, "hello");
    assert_eq!(first.channel, "imessage");
    assert_ne!(first.id, "1", "rows before startup are never replayed");
    assert!(first.timestamp > 0);

    // The receiver going away ends the listener cleanly.
    drop(rx);
    for _ in 0..3 {
        rowid += 1;
        insert_message(&writer, rowid, 1, "after close");
    }
    let result = timeout(HANG_GUARD, listen)
        .await
        .expect("listen returns once the receiver is gone")
        .unwrap();
    assert!(result.is_ok(), "{result:?}");
}

/// A poll that fails (here: the `handle` table is missing) is logged and
/// retried rather than ending the listener.
#[tokio::test]
async fn listen_survives_failing_polls() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("chat.db");
    let writer = Connection::open(&db_path).unwrap();
    writer
        .execute_batch(
            "CREATE TABLE message (
                ROWID INTEGER PRIMARY KEY, guid TEXT, handle_id INTEGER, text TEXT,
                is_from_me INTEGER DEFAULT 0, date INTEGER, is_audio_message INTEGER DEFAULT 0,
                associated_message_guid TEXT, associated_message_type INTEGER DEFAULT 0,
                date_read INTEGER DEFAULT 0
            );",
        )
        .unwrap();

    let channel = channel_on(&db_path, &["*"]);
    let (tx, mut rx) = mpsc::channel(8);
    let listen = tokio::spawn(async move { channel.listen(tx).await });

    // Give the listener time to fail a few polls; the assertions below hold
    // whether or not it did.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!listen.is_finished(), "a failing poll must not end listen");

    writer
        .execute_batch(
            "CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT NOT NULL);
             CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, style INTEGER);
             CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
             CREATE TABLE attachment (ROWID INTEGER PRIMARY KEY, filename TEXT, mime_type TEXT,
                transfer_name TEXT, total_bytes INTEGER);
             CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
             INSERT INTO handle (ROWID, id) VALUES (1, 'a@example.com');
             INSERT INTO handle (ROWID, id) VALUES (2, 'b@example.com');",
        )
        .unwrap();
    let mut rowid = 0;
    let delivered = next_delivery(&writer, &mut rowid, &mut rx).await;
    // With a wildcard allowlist the stranger is delivered too; blanks never are.
    assert!(!delivered.content.trim().is_empty());
    listen.abort();
}
