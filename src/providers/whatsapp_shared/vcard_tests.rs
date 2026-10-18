use super::*;

fn card() -> ContactCard {
    ContactCard {
        display_name: "Ada; Lovelace, Countess".into(),
        phones: vec![
            ContactMethod {
                value: "+1 (555) 010-0100".into(),
                label: Some("work".into()),
            },
            ContactMethod {
                value: "ext".into(),
                label: Some("EVIL;X=1".into()),
            },
        ],
        emails: vec![ContactMethod {
            value: "ada@example.invalid".into(),
            label: None,
        }],
        organization: Some("Analytical Engines".into()),
    }
}

#[test]
fn written_cards_escape_values_and_whitelist_type_tokens() {
    let body = write(&card());
    assert!(body.starts_with("BEGIN:VCARD\r\nVERSION:3.0\r\n"));
    assert!(body.contains("FN:Ada\\; Lovelace\\, Countess\r\n"));
    assert!(body.contains("TEL;type=work;waid=15550100100:+1 (555) 010-0100\r\n"));
    // A hostile label falls back to a safe token and a digit-less number has no waid.
    assert!(body.contains("TEL;type=CELL:ext\r\n"));
    assert!(!body.contains("EVIL"));
    assert!(body.contains("EMAIL;type=INTERNET:ada@example.invalid\r\n"));
    assert!(body.contains("ORG:Analytical Engines\r\n"));
    assert!(body.ends_with("END:VCARD\r\n"));
}

#[test]
fn read_round_trips_a_written_card() {
    let read = read(&write(&card()));
    assert_eq!(read.display_name, "Ada; Lovelace, Countess");
    assert_eq!(read.phones.len(), 2);
    assert_eq!(read.phones[0].value, "+1 (555) 010-0100");
    assert_eq!(read.phones[0].label.as_deref(), Some("work"));
    assert_eq!(read.emails[0].value, "ada@example.invalid");
    assert_eq!(read.organization.as_deref(), Some("Analytical Engines"));
}

#[test]
fn read_falls_back_to_the_structured_name_and_unfolds_lines() {
    let body = "BEGIN:VCARD\nVERSION:3.0\nN:Lovelace;Ada;;;\nTEL;TYPE=CELL:+4412\n 3456\nORG:Engines;Division\nX-CUSTOM:ignored\nnot a property\nEND:VCARD";
    let read = read(body);
    assert_eq!(read.display_name, "Ada Lovelace");
    assert_eq!(read.phones[0].value, "+44123456");
    assert_eq!(read.organization.as_deref(), Some("Engines Division"));
}

#[test]
fn escapes_round_trip_including_trailing_backslash() {
    assert_eq!(unescape("a\\nb\\\\c\\"), "a\nb\\c\\");
    assert_eq!(escape("a\r\nb"), "a\\nb");
}
