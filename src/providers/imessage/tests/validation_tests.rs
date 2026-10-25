use super::*;

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
    assert_eq!(ch.descriptor().id, "imessage");
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
