use super::*;

#[test]
fn email_send_wire_uses_camel_case_and_preserves_attachment_bytes() {
    let request = EmailSendRequest {
        config: EmailConfig {
            smtp_host: "127.0.0.1".into(),
            smtp_port: 2525,
            smtp_tls: false,
            username: "user@example.test".into(),
            password: "secret".into(),
            from_address: "sender@example.test".into(),
            ..EmailConfig::default()
        },
        recipient: "recipient@example.test".into(),
        subject: "report".into(),
        body: "body".into(),
        attachments: vec![EmailAttachment {
            filename: "report.bin".into(),
            content_type: "application/octet-stream".into(),
            bytes: vec![0, 1, 255],
        }],
    };
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(value["config"]["smtp_host"], "127.0.0.1");
    assert_eq!(value["config"]["from_address"], "sender@example.test");
    assert_eq!(
        value["attachments"][0]["contentType"],
        "application/octet-stream"
    );
    assert_eq!(
        value["attachments"][0]["bytes"],
        serde_json::json!([0, 1, 255])
    );
    let decoded: EmailSendRequest = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.attachments[0].bytes, [0, 1, 255]);
}

#[test]
fn email_send_schema_exposes_the_complete_attachment_request() {
    let schema = schemars::schema_for!(EmailSendRequest);
    let value = serde_json::to_value(schema).unwrap();
    let text = serde_json::to_string(&value).unwrap();
    for field in [
        "config",
        "recipient",
        "subject",
        "body",
        "attachments",
        "contentType",
    ] {
        assert!(text.contains(field), "schema omitted {field}");
    }
}

#[test]
fn email_upload_wire_keeps_chunk_payloads_bounded_and_camel_case() {
    let chunk = EmailUploadChunk {
        handle: EmailUploadHandle("opaque".into()),
        attachment_index: 2,
        sequence: 7,
        bytes: vec![0, 255],
    };
    let value = serde_json::to_value(&chunk).unwrap();
    assert_eq!(value["attachmentIndex"], 2);
    assert_eq!(value["sequence"], 7);
    assert_eq!(value["bytes"], serde_json::json!([0, 255]));
    let decoded: EmailUploadChunk = serde_json::from_value(value).unwrap();
    assert_eq!(decoded, chunk);
    assert_eq!(EMAIL_UPLOAD_CHUNK_BYTES, 512 * 1024);
    assert_eq!(EMAIL_UPLOAD_MAX_CHUNKS, 128);
}
