use super::*;
use tinychannels_bus::channel::{
    VoiceNote, VoiceTranscript, VoiceTranscriptSource, VoiceTranscriptState,
};
use tinychannels_bus::web::QueueMode;

fn start() -> WebChatStart {
    WebChatStart {
        client_id: "client-7".into(),
        thread_id: "thread-9".into(),
        request_id: "request-2".into(),
        text: "hello".into(),
        ..Default::default()
    }
}

fn image() -> ContentPart {
    ContentPart::Image {
        media: MediaReference {
            opaque_id: Some("media-1".into()),
            content_type: Some("image/png".into()),
            ..Default::default()
        },
        caption: None,
        alt_text: None,
    }
}

fn voice(transcript: Option<&str>) -> ContentPart {
    voice_in_state(transcript, VoiceTranscriptState::Complete)
}

fn voice_in_state(transcript: Option<&str>, state: VoiceTranscriptState) -> ContentPart {
    ContentPart::VoiceNote {
        voice_note: VoiceNote {
            media: MediaReference {
                opaque_id: Some("voice-1".into()),
                ..Default::default()
            },
            transcript: transcript.map(|text| VoiceTranscript {
                text: text.into(),
                language: None,
                state,
                source: VoiceTranscriptSource::HostStt,
            }),
            ..Default::default()
        },
    }
}

#[test]
fn text_submission_maps_addressing_and_authority() {
    let envelope = normalize_chat_start(&start(), Some("acct")).unwrap();
    assert_eq!(envelope.channel.id, "web");
    assert_eq!(envelope.channel.account_id.as_deref(), Some("acct"));
    assert_eq!(envelope.message_id, "request-2");
    assert_eq!(envelope.conversation.id, "thread-9");
    assert_eq!(envelope.conversation.thread_id.as_deref(), Some("thread-9"));
    assert_eq!(envelope.sender.id, "client-7");
    assert_eq!(envelope.text, "hello");
    assert_eq!(envelope.parts.len(), 1);
    assert!(!envelope.access.command_authorized);
    assert_eq!(envelope.raw.as_ref().unwrap()["queueMode"], "interrupt");
}

#[test]
fn anonymous_submissions_do_not_authorize_commands() {
    let envelope = normalize_chat_start(&start(), None).unwrap();
    assert!(!envelope.access.command_authorized);
}

#[test]
fn client_input_cannot_choose_the_authenticated_sender() {
    let parsed: WebChatStart = serde_json::from_str(
        r#"{"clientId":"client-7","threadId":"thread-9","requestId":"request-2","text":"hello","senderId":"forged-user","commandAuthorized":true}"#,
    )
    .unwrap();
    assert_eq!(parsed.sender_id, None);
    assert!(!parsed.command_authorized);
    let envelope = normalize_chat_start(&parsed, None).unwrap();
    assert_eq!(envelope.sender.id, "client-7");
    assert!(!envelope.access.command_authorized);
}

#[test]
fn command_authority_requires_a_host_authenticated_sender() {
    let mut authenticated = start();
    authenticated.sender_id = Some("user-1".into());
    authenticated.command_authorized = true;
    assert!(
        normalize_chat_start(&authenticated, None)
            .unwrap()
            .access
            .command_authorized
    );

    authenticated.sender_id = None;
    assert!(
        !normalize_chat_start(&authenticated, None)
            .unwrap()
            .access
            .command_authorized
    );
}

#[test]
fn authenticated_sender_overrides_client_id_and_blank_falls_back() {
    let mut with_user = start();
    with_user.sender_id = Some("user-1".into());
    with_user.sender_name = Some("Ada".into());
    let envelope = normalize_chat_start(&with_user, None).unwrap();
    assert_eq!(envelope.sender.id, "user-1");
    assert_eq!(envelope.sender.name.as_deref(), Some("Ada"));

    with_user.sender_id = Some("  ".into());
    assert_eq!(
        normalize_chat_start(&with_user, None).unwrap().sender.id,
        "client-7"
    );
}

#[test]
fn attachments_become_ordered_typed_parts_and_media_refs() {
    let mut submission = start();
    submission.attachments = vec![image(), voice(None)];
    let envelope = normalize_chat_start(&submission, None).unwrap();
    assert_eq!(envelope.parts.len(), 3);
    assert!(matches!(envelope.parts[0], ContentPart::Text { .. }));
    assert!(matches!(envelope.parts[1], ContentPart::Image { .. }));
    assert!(matches!(envelope.parts[2], ContentPart::VoiceNote { .. }));
    let ids: Vec<_> = envelope
        .media
        .iter()
        .filter_map(|media| media.opaque_id.as_deref())
        .collect();
    assert_eq!(ids, ["media-1", "voice-1"]);
    assert_eq!(envelope.text, "hello");
}

#[test]
fn voice_only_submission_uses_the_transcript_as_text() {
    let mut submission = start();
    submission.text = String::new();
    submission.attachments = vec![voice(Some("what time is it"))];
    let envelope = normalize_chat_start(&submission, None).unwrap();
    assert_eq!(envelope.text, "what time is it");
    assert_eq!(envelope.parts.len(), 1);

    submission.attachments = vec![voice(None)];
    let envelope = normalize_chat_start(&submission, None).unwrap();
    assert_eq!(envelope.text, "");
    assert_eq!(envelope.parts.len(), 1);
}

#[test]
fn a_pending_transcript_is_not_promoted_to_text() {
    let mut submission = start();
    submission.text = String::new();
    submission.attachments = vec![voice_in_state(
        Some("partial"),
        VoiceTranscriptState::Pending,
    )];
    assert_eq!(normalize_chat_start(&submission, None).unwrap().text, "");
}

#[test]
fn typed_text_wins_over_a_voice_transcript() {
    let mut submission = start();
    submission.attachments = vec![voice(Some("spoken"))];
    assert_eq!(
        normalize_chat_start(&submission, None).unwrap().text,
        "hello"
    );
}

#[test]
fn file_video_and_audio_attachments_are_accepted() {
    let media = MediaReference::default();
    let mut submission = start();
    submission.attachments = vec![
        ContentPart::File {
            media: media.clone(),
            file_name: Some("a.pdf".into()),
            caption: None,
        },
        ContentPart::Video {
            media: media.clone(),
            caption: None,
            duration_ms: None,
            thumbnail: None,
        },
        ContentPart::Audio {
            media,
            codec: None,
            duration_ms: None,
            title: None,
            transcript: None,
        },
    ];
    assert_eq!(
        normalize_chat_start(&submission, None).unwrap().media.len(),
        3
    );
}

#[test]
fn malformed_submissions_are_rejected_as_bad_format() {
    let mut no_thread = start();
    no_thread.thread_id = " ".into();
    let mut no_request = start();
    no_request.request_id.clear();
    let mut no_client = start();
    no_client.client_id.clear();
    let mut empty = start();
    empty.text = "  ".into();
    let mut non_media = start();
    non_media.attachments = vec![ContentPart::Location {
        latitude: 1.0,
        longitude: 2.0,
        label: None,
        address: None,
        accuracy_meters: None,
        live_until_unix_ms: None,
    }];
    for bad in [no_thread, no_request, no_client, empty, non_media] {
        let error = normalize_chat_start(&bad, None).unwrap_err();
        assert_eq!(error.kind, SendErrorKind::BadFormat);
        assert!(!error.retryable);
    }
}

#[test]
fn sender_identity_is_host_assigned_after_wire_deserialization() {
    let parsed: WebChatStart = serde_json::from_str(
        r#"{"clientId":"client-7","threadId":"thread-9","requestId":"request-2","text":"hello","senderId":"attacker"}"#,
    )
    .unwrap();
    assert_eq!(parsed.sender_id, None);

    let mut authenticated = parsed;
    authenticated.sender_id = Some("user-1".into());
    assert_eq!(
        normalize_chat_start(&authenticated, None)
            .unwrap()
            .sender
            .id,
        "user-1"
    );
}

#[test]
fn chat_start_deserializes_from_the_camel_case_wire() {
    let parsed: WebChatStart = serde_json::from_str(
        r#"{"clientId":"c","threadId":"t","requestId":"r","text":"hi","queueMode":"steer","speakReply":true}"#,
    )
    .unwrap();
    assert_eq!(parsed.queue_mode, QueueMode::Steer);
    assert_eq!(parsed.speak_reply, Some(true));
    let envelope = normalize_chat_start(&parsed, None).unwrap();
    assert_eq!(envelope.raw.unwrap()["queueMode"], "steer");
}
