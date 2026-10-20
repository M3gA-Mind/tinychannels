use super::parse_rfc3339_ms;

#[test]
fn rfc3339_timestamps_parse_to_unix_milliseconds() {
    assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(
        parse_rfc3339_ms("2026-09-29T15:04:00Z"),
        Some(1_790_694_240_000)
    );
    assert_eq!(
        parse_rfc3339_ms("2026-09-29T17:04:00.250+02:00"),
        Some(1_790_694_240_250)
    );
    assert_eq!(
        parse_rfc3339_ms("2024-02-29T00:00:00-01:00"),
        Some(1_709_168_400_000)
    );
    assert_eq!(parse_rfc3339_ms("not a time"), None);
    assert_eq!(parse_rfc3339_ms("2026-13-01T00:00:00Z"), None);
}
