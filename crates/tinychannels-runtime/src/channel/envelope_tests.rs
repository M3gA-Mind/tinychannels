#![allow(deprecated)]

use super::*;

fn legacy_message(channel: &str, timestamp: u64) -> ChannelMessage {
    ChannelMessage {
        id: "message-1".into(),
        sender: "sender-1".into(),
        reply_target: "conversation-1".into(),
        content: "legacy text".into(),
        channel: channel.into(),
        timestamp,
        topic_id: None,
        reply_to_id: Some("reply-1".into()),
        thread_ts: Some(" thread-1 ".into()),
        sender_name: Some("Sender".into()),
    }
}

#[test]
fn media_projection_aligns_partial_metadata_and_transcription_indexes() {
    let projected = InboundMediaPayload::from_media(&[
        MediaReference {
            path: Some("/tmp/one".into()),
            content_type: Some("image/png".into()),
            transcribed: true,
            ..Default::default()
        },
        MediaReference {
            url: Some("https://example.test/two".into()),
            kind: MediaKind::Audio,
            ..Default::default()
        },
    ]);

    assert_eq!(projected.media_path.as_deref(), Some("/tmp/one"));
    assert_eq!(projected.media_url.as_deref(), Some("/tmp/one"));
    assert_eq!(projected.media_type.as_deref(), Some("image/png"));
    assert_eq!(
        projected.media_paths,
        Some(vec!["/tmp/one".into(), "".into()])
    );
    assert_eq!(
        projected.media_urls,
        Some(vec!["/tmp/one".into(), "https://example.test/two".into()])
    );
    assert_eq!(
        projected.media_types,
        Some(vec!["image/png".into(), "audio".into()])
    );
    assert_eq!(projected.media_transcribed_indexes, Some(vec![0]));
    assert!(InboundMediaPayload::from_media(&[]).media_paths.is_none());
}

#[test]
fn legacy_projection_normalizes_timestamps_and_telegram_topic_threads() {
    let seconds = inbound_envelope_from_legacy_message(&legacy_message("telegram:account", 42));
    assert_eq!(seconds.timestamp_unix_ms, Some(42_000));
    assert_eq!(seconds.conversation.thread_id, None);
    assert_eq!(seconds.conversation.topic_id.as_deref(), Some("thread-1"));
    assert_eq!(seconds.sender.name.as_deref(), Some("Sender"));
    assert_eq!(seconds.reply_to_id.as_deref(), Some("reply-1"));

    let millis = inbound_envelope_from_legacy_message(&legacy_message("slack", 100_000_000_000));
    assert_eq!(millis.timestamp_unix_ms, Some(100_000_000_000));
    assert_eq!(millis.conversation.thread_id.as_deref(), Some("thread-1"));
    assert_eq!(millis.conversation.topic_id, None);
}

#[test]
fn legacy_projection_accepts_text_and_complete_voice_transcripts_only() {
    let mut envelope = inbound_envelope_from_legacy_message(&legacy_message("slack", 42));
    envelope.parts = vec![
        ContentPart::Text {
            text: "first".into(),
            markdown_dialect: Some("commonmark".into()),
        },
        ContentPart::VoiceNote {
            voice_note: VoiceNote {
                transcript: Some(VoiceTranscript {
                    text: "spoken words".into(),
                    language: None,
                    state: VoiceTranscriptState::Complete,
                    source: VoiceTranscriptSource::HostStt,
                }),
                ..Default::default()
            },
        },
    ];
    let legacy = legacy_message_from_inbound_envelope(&envelope, 777).unwrap();
    assert_eq!(legacy.content, "first\nspoken words");
    assert_eq!(legacy.timestamp, 777);

    envelope.parts = vec![ContentPart::VoiceNote {
        voice_note: VoiceNote {
            transcript: Some(VoiceTranscript {
                text: "pending words".into(),
                language: None,
                state: VoiceTranscriptState::Pending,
                source: VoiceTranscriptSource::Provider,
            }),
            ..Default::default()
        },
    }];
    assert!(legacy_message_from_inbound_envelope(&envelope, 1).is_err());

    envelope.parts = vec![ContentPart::Image {
        media: Default::default(),
        caption: None,
        alt_text: None,
    }];
    assert!(legacy_message_from_inbound_envelope(&envelope, 1).is_err());
}

#[test]
fn typed_projection_keeps_text_when_rich_content_has_no_text_view() {
    let mut envelope = inbound_envelope_from_legacy_message(&legacy_message("slack", 42));
    envelope.parts = vec![ContentPart::Image {
        media: Default::default(),
        caption: None,
        alt_text: None,
    }];
    assert_eq!(
        typed_message_from_inbound_envelope(&envelope, 123).content,
        "legacy text"
    );

    envelope.parts = vec![];
    assert_eq!(
        typed_message_from_inbound_envelope(&envelope, 123).content,
        "legacy text"
    );
}
