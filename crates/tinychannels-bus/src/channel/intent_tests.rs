use super::*;

#[test]
fn v21_persisted_media_file_and_voice_payloads_remain_readable() {
    let base = serde_json::json!({
        "idempotencyKey": "delivery-1",
        "channelId": "telegram",
        "conversationId": "room",
        "replyToId": null,
        "threadId": null,
        "durability": "required"
    });

    let mut media = base.clone();
    media["payload"] = serde_json::json!({
        "kind": "media",
        "text": "caption",
        "media_urls": ["https://example.test/image.png"]
    });
    assert!(matches!(
        serde_json::from_value::<ChannelOutboundIntent>(media)
            .unwrap()
            .payload,
        OutboundPayload::Media { ref media_urls, .. }
            if media_urls == &["https://example.test/image.png"]
    ));

    let mut files = base.clone();
    files["payload"] = serde_json::json!({
        "kind": "files",
        "file_urls": ["https://example.test/file.pdf"]
    });
    assert!(matches!(
        serde_json::from_value::<ChannelOutboundIntent>(files)
            .unwrap()
            .payload,
        OutboundPayload::Files { ref file_urls }
            if file_urls == &["https://example.test/file.pdf"]
    ));

    let mut voice = base;
    voice["payload"] = serde_json::json!({
        "kind": "voice",
        "media_url": "https://example.test/voice.ogg"
    });
    let parsed: ChannelOutboundIntent = serde_json::from_value(voice).unwrap();
    let OutboundPayload::Voice {
        voice: OutboundVoice::Media { media, .. },
    } = parsed.payload
    else {
        panic!("legacy voice URL must decode as typed media voice");
    };
    assert_eq!(media.url.as_deref(), Some("https://example.test/voice.ogg"));

    let encoded = serde_json::to_value(parsed_payload_for_serialization()).unwrap();
    assert!(encoded.get("mediaUrls").is_some());
    assert!(encoded.get("media_urls").is_none());
}

#[test]
fn outbound_turn_scope_is_optional_and_survives_typed_adaptation() {
    let legacy = serde_json::json!({
        "idempotencyKey": "old",
        "channelId": "telegram",
        "conversationId": "room",
        "replyToId": null,
        "threadId": null,
        "durability": "required",
        "turnId": "turn-42",
        "payload": { "kind": "text", "text": "hello" }
    });
    let intent: ChannelOutboundIntent = serde_json::from_value(legacy).unwrap();
    let typed = ChannelOutboundMessage::from_intent(&intent);
    let encoded_typed = serde_json::to_value(typed).unwrap();
    assert_eq!(encoded_typed["turnId"], "turn-42");
    let encoded = serde_json::to_value(intent).unwrap();
    assert_eq!(encoded["turnId"], "turn-42");
}

#[test]
fn outbound_turn_scope_remains_optional_for_legacy_payloads() {
    let intent: ChannelOutboundIntent = serde_json::from_value(serde_json::json!({
        "idempotencyKey": "old",
        "channelId": "telegram",
        "conversationId": "room",
        "replyToId": null,
        "threadId": null,
        "durability": "required",
        "payload": { "kind": "text", "text": "hello" }
    }))
    .unwrap();
    assert!(
        serde_json::to_value(intent)
            .unwrap()
            .get("turnId")
            .is_none()
    );
}

fn parsed_payload_for_serialization() -> OutboundPayload {
    OutboundPayload::Media {
        text: None,
        media_urls: vec!["https://example.test/new.png".into()],
    }
}
