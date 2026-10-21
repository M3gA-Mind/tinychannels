// Scoped allowance for the one-release v21 wire DTO compatibility surface.
#![allow(deprecated)]

use super::*;

#[test]
fn runtime_reexports_keep_exact_bus_dto_identity() {
    let message: tinychannels_bus::SendMessage = traits::SendMessage::new("hi", "room");
    let bus_message: tinychannels_bus::traits::SendMessage = message.clone();
    assert_eq!(
        std::any::type_name_of_val(&bus_message),
        std::any::type_name::<tinychannels_bus::SendMessage>()
    );
    let intent = channel::outbound_intent_from_send_message("telegram", &message);
    assert!(intent.idempotency_key.starts_with("legacy-send:telegram:"));
    let envelope: tinychannels_bus::ChannelInboundEnvelope =
        channel::ChannelInboundEnvelope::default();
    let projected: tinychannels_bus::ChannelMessage =
        channel::legacy_message_from_inbound_envelope(&envelope, 7).unwrap();
    assert_eq!(projected.timestamp, 7);
}

#[test]
fn persisted_legacy_idempotency_digest_remains_byte_identical() {
    let intent = channel::outbound_intent_from_legacy_message(
        "telegram",
        serde_json::json!({"text":"hi", "recipient":"room"}),
    );
    assert_eq!(
        intent.idempotency_key,
        "legacy-send:telegram:348ff41bc06b76435e69051366e46001710bb3590c128b6e88c97e42d5dfb30e"
    );
}

#[test]
fn relay_auth_rejects_malformed_tokens_headers_and_hex() {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    for raw in [
        vec![255],
        b"no-colons".to_vec(),
        b"payload:bad:signature".to_vec(),
    ] {
        assert_eq!(relay::verify_token_at(&b64.encode(raw), &["key"], 1), None);
    }
    for signature in ["", "a", "zz"] {
        assert!(!relay::verify_signature("payload", signature, &["key"]));
    }
    let signature = relay::sign("payload", "key").to_uppercase();
    assert!(relay::verify_signature("payload", &signature, &["key"]));
    assert!(!relay::verify_delivery_signature_at(
        "body",
        None,
        Some("sig"),
        &["key"],
        300,
        1
    ));
    assert!(!relay::verify_delivery_signature_at(
        "body",
        Some("invalid"),
        Some("sig"),
        &["key"],
        300,
        1
    ));
    assert!(!relay::verify_delivery_signature(
        "body",
        None,
        None,
        &["key"]
    ));
}

#[test]
fn relay_auth_rejects_negative_replay_window() {
    let body = "body";
    let timestamp = "1000";
    let signature = relay::sign(&format!("{timestamp}.{body}"), "key");

    assert!(!relay::verify_delivery_signature_at(
        body,
        Some(timestamp),
        Some(&signature),
        &["key"],
        -1,
        1000,
    ));
}
