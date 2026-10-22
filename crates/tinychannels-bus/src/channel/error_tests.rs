use super::*;

#[test]
fn unsupported_error_flag_is_backward_compatible_on_the_wire() {
    let legacy: ChannelSendError = serde_json::from_value(serde_json::json!({
        "kind": "bad_format",
        "message": "unsupported operation"
    }))
    .expect("decode pre-flag error");
    assert!(!legacy.unsupported);

    let encoded = serde_json::to_value(ChannelSendError::default()).expect("encode default");
    assert!(encoded.get("unsupported").is_none());

    let unsupported = ChannelSendError {
        unsupported: true,
        ..Default::default()
    };
    let encoded = serde_json::to_value(unsupported).expect("encode unsupported error");
    assert_eq!(encoded["unsupported"], true);
}
