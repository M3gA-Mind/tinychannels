use super::*;

#[test]
fn transcription_request_keeps_legacy_default_and_carries_optional_scope() {
    let legacy = HostTranscriptionRequest::default();
    assert!(legacy.scope.is_none());

    let scoped = HostTranscriptionRequest {
        scope: Some(HostVoiceScope {
            configured_instance: "telegram:primary".into(),
            channel: ChannelRef {
                id: "telegram".into(),
                account_id: Some("primary".into()),
            },
            conversation: ConversationRef {
                id: "chat-1".into(),
                ..Default::default()
            },
            turn_id: "turn-1".into(),
        }),
        audio_base64: "AA==".into(),
        ..Default::default()
    };
    let value = serde_json::to_value(scoped).unwrap();
    assert_eq!(value["scope"]["configuredInstance"], "telegram:primary");
    assert_eq!(value["scope"]["conversation"]["id"], "chat-1");
}

#[test]
fn voice_media_direction_defaults_to_inbound() {
    let request: HostStoreVoiceMediaRequest = serde_json::from_value(serde_json::json!({
        "scope": {"configuredInstance": "discord:primary"},
        "audioBase64": "AA==",
        "mimeType": "audio/ogg"
    }))
    .unwrap();
    assert_eq!(request.direction, HostVoiceMediaDirection::Inbound);
}
