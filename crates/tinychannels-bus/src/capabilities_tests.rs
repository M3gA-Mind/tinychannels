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

#[test]
fn legacy_presentation_projection_preserves_rich_button_constraints_and_edit_semantics() {
    let mut capabilities = ChannelCapabilities::unknown();
    capabilities.progressive_edits = true;
    capabilities.content.button_presentation = ButtonPresentationCapabilities {
        max_actions_per_row: Some(4),
        max_rows: Some(3),
        max_label_length: Some(32),
        max_value_bytes: Some(512),
        supports_styles: true,
        supports_disabled: true,
        supports_layout_hints: true,
    };
    // Streaming drafts do not imply a separately supported edit action.
    let static_projection = crate::channel::ChannelStaticCapabilities::from(&capabilities);
    let presentation = crate::channel::ChannelPresentationCapabilities::from(&capabilities);
    assert!(!static_projection.edit);
    assert!(static_projection.block_streaming);
    assert!(!presentation.supports_edit);
    assert_eq!(presentation.max_actions_per_row, Some(4));
    assert_eq!(presentation.max_rows, Some(3));
    assert_eq!(presentation.max_label_length, Some(32));
    assert_eq!(presentation.max_value_bytes, Some(512));
    assert!(
        presentation.supports_styles
            && presentation.supports_disabled
            && presentation.supports_layout_hints
    );
}
