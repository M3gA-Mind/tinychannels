use super::*;

#[test]
fn unsupported_error_marker_preserves_the_public_struct_layout() {
    let legacy: ChannelSendError = serde_json::from_value(serde_json::json!({
        "kind": "bad_format",
        "message": "unsupported operation"
    }))
    .expect("decode pre-flag error");
    assert!(!legacy.is_unsupported());

    let unsupported = ChannelSendError::unsupported("voice output");
    assert!(unsupported.is_unsupported());
    assert_eq!(unsupported.kind, SendErrorKind::BadFormat);
    assert_eq!(unsupported.message, "unsupported operation: voice output");
    let encoded = serde_json::to_value(unsupported).expect("encode unsupported error");
    assert_eq!(encoded["message"], "unsupported operation: voice output");
    assert!(encoded.get("unsupported").is_none());
}
