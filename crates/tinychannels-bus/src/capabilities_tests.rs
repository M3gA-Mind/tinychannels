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
    capabilities.text.length_unit = LengthUnit::Utf8Bytes;
    capabilities.text.markdown_dialects = vec![MarkdownDialect::SlackMrkdwn];

    let json = serde_json::to_vec(&capabilities).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(value["text"]["lengthUnit"], "utf8_bytes");
    assert_eq!(value["text"]["markdownDialects"][0], "slack_mrkdwn");
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

#[test]
fn legacy_presentation_keeps_v1_enum_spellings_on_the_wire() {
    for (length_unit, length_wire) in [
        (LengthUnit::Characters, "characters"),
        (LengthUnit::Utf8Bytes, "utf8-bytes"),
        (LengthUnit::Utf16Units, "utf16-units"),
    ] {
        for (markdown_dialect, markdown_wire) in [
            (MarkdownDialect::Plain, "plain"),
            (MarkdownDialect::Markdown, "markdown"),
            (MarkdownDialect::Html, "html"),
            (MarkdownDialect::SlackMrkdwn, "slack-mrkdwn"),
            (MarkdownDialect::DiscordMarkdown, "discord-markdown"),
            (MarkdownDialect::TelegramMarkdownV2, "telegram-markdown-v2"),
        ] {
            let presentation = crate::channel::ChannelPresentationCapabilities {
                length_unit,
                markdown_dialect,
                ..Default::default()
            };
            let value = serde_json::to_value(&presentation).unwrap();
            assert_eq!(value["lengthUnit"], length_wire);
            assert_eq!(value["markdownDialect"], markdown_wire);
            let decoded: crate::channel::ChannelPresentationCapabilities =
                serde_json::from_value(value).unwrap();
            assert_eq!(decoded.length_unit, length_unit);
            assert_eq!(decoded.markdown_dialect, markdown_dialect);
        }
    }
    let schema = schemars::schema_for!(crate::channel::ChannelPresentationCapabilities);
    let schema_json = serde_json::to_string(&schema).unwrap();
    for legacy_value in [
        "characters",
        "utf8-bytes",
        "utf16-units",
        "slack-mrkdwn",
        "discord-markdown",
        "telegram-markdown-v2",
    ] {
        assert!(
            schema_json.contains(legacy_value),
            "schema lacks {legacy_value}"
        );
    }
}
