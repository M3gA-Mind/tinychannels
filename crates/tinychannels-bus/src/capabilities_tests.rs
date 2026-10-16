use super::*;

#[test]
fn v2_capabilities_round_trip_with_voice_and_typed_limits() {
    let mut capabilities = ChannelCapabilities::unknown();
    capabilities.voice_in = VoiceCapabilities {
        supported: true,
        accepted_codecs: vec!["ogg/opus".into()],
        max_duration_ms: Some(60_000),
        native_waveform: true,
        ..Default::default()
    };
    capabilities.content.parts.push("voice_note".into());
    capabilities.content.image.max_size_bytes = Some(10_000);

    let json = serde_json::to_vec(&capabilities).unwrap();
    let decoded: ChannelCapabilities = serde_json::from_slice(&json).unwrap();
    assert_eq!(decoded, capabilities);
    assert_eq!(ChannelCapabilities::VERSION, 2);
}

#[test]
fn provider_identifier_strips_instance_address() {
    assert_eq!(provider_id("tg:1"), "telegram");
    assert_eq!(provider_id("slack:C1"), "slack");
    assert_eq!(provider_id("email"), "email");
}

#[test]
fn channel_message_action_names_are_unique() {
    let names = crate::channel::CHANNEL_MESSAGE_ACTION_NAMES;
    let unique: std::collections::BTreeSet<_> = names.iter().copied().collect();
    assert_eq!(unique.len(), names.len());
}
