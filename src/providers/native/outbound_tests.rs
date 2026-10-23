use super::*;
use crate::providers::native::fixtures::*;
use std::sync::Arc;
use tinychannels_bus::{ChannelOutboundMessage, MediaReference};

fn message(parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "fake".into(),
        conversation_id: "c1".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn".into()),
        idempotency_key: "k".into(),
        parts,
    }
}

fn text(body: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: body.into(),
            markdown_dialect: None,
        },
    }
}

fn media(kind: MediaKind, opaque: Option<&str>) -> MediaReference {
    MediaReference {
        opaque_id: opaque.map(str::to_owned),
        kind,
        ..Default::default()
    }
}

fn image(opaque: Option<&str>) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Image {
            media: media(MediaKind::Image, opaque),
            caption: Some("cap".into()),
            alt_text: None,
        },
    }
}

fn voice(opaque: Option<&str>) -> OutboundContentPart {
    OutboundContentPart::Voice {
        voice: OutboundVoice::Media {
            media: media(MediaKind::Audio, opaque),
            codec: None,
            duration_ms: None,
        },
    }
}

fn with_media(read: Option<tinychannels_bus::MediaKind>, bytes: &[u8]) -> FakeProvider {
    let repository = Arc::new(StubMedia::default());
    if let Some(kind) = read {
        *repository.read.lock().unwrap() = Some(stored_media(kind, "image/png", bytes));
    }
    let mut provider = FakeProvider {
        media: Some(repository),
        ..Default::default()
    };
    provider.caps.content.image.outbound = true;
    provider
}

fn with_voice(audio: Option<&[u8]>) -> FakeProvider {
    let store = Arc::new(StubVoice::default());
    *store.audio_base64.lock().unwrap() =
        audio.map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes));
    let mut provider = FakeProvider {
        voice: Some(store),
        ..Default::default()
    };
    provider.caps.voice_out.supported = audio.is_some();
    provider
}

async fn plan_of(
    provider: &FakeProvider,
    message: &ChannelOutboundMessage,
) -> Result<Vec<Step>, ChannelSendError> {
    let target = target_of(message, "fake", "Fake")?;
    plan(provider, message, &target).await
}

#[tokio::test]
async fn text_is_chunked_and_stripped_of_tool_call_tags() {
    let provider = FakeProvider::default();
    let steps = plan_of(
        &provider,
        &message(vec![text("one two three four five six seven")]),
    )
    .await
    .unwrap();
    assert!(steps.len() >= 2);
    assert!(
        steps
            .iter()
            .all(|s| matches!(s, Step::Text(t) if t.chars().count() <= 20))
    );
    // Blank text is not a step; a message with nothing else is an error.
    let error = plan_of(&provider, &message(vec![text("  ")])).await;
    assert!(error.is_err());
    assert!(plan_of(&provider, &message(vec![])).await.is_err());
}

#[tokio::test]
async fn unsupported_content_kinds_are_explicit() {
    let provider = FakeProvider::default();
    let poll = OutboundContentPart::Content {
        part: ContentPart::Poll {
            question: "q".into(),
            options: vec![],
            multiple_choice: false,
            closes_at_unix_ms: None,
            provider_poll_id: None,
        },
    };
    assert!(
        plan_of(&provider, &message(vec![poll]))
            .await
            .unwrap_err()
            .unsupported
    );
    // Media without storage.
    assert!(
        plan_of(&provider, &message(vec![image(Some("m"))]))
            .await
            .unwrap_err()
            .unsupported
    );
    // Voice without storage or synthesis.
    assert!(
        plan_of(&provider, &message(vec![voice(Some("v"))]))
            .await
            .unwrap_err()
            .unsupported
    );
    let synth = OutboundContentPart::Voice {
        voice: OutboundVoice::Synthesize {
            text: "x".into(),
            voice: None,
        },
    };
    assert!(
        plan_of(&provider, &message(vec![synth.clone()]))
            .await
            .unwrap_err()
            .unsupported
    );
    let synthesizing = FakeProvider {
        synthesis: true,
        ..Default::default()
    };
    let error = plan_of(&synthesizing, &message(vec![synth]))
        .await
        .unwrap_err();
    assert!(!error.unsupported && error.message.contains("shared module runtime"));
}

#[tokio::test]
async fn undeclared_media_kinds_are_rejected_before_host_storage_reads() {
    let repository = Arc::new(StubMedia::default());
    let provider = FakeProvider {
        media: Some(repository.clone()),
        ..Default::default()
    };
    let unsupported_parts = [
        OutboundContentPart::Content {
            part: ContentPart::Video {
                media: media(MediaKind::Video, Some("video-ref")),
                caption: None,
                duration_ms: None,
                thumbnail: None,
            },
        },
        OutboundContentPart::Content {
            part: ContentPart::Audio {
                media: media(MediaKind::Audio, Some("audio-ref")),
                codec: Some("ogg_opus".into()),
                duration_ms: None,
                title: None,
                transcript: None,
            },
        },
    ];

    for part in unsupported_parts {
        let error = plan_of(&provider, &message(vec![part])).await.unwrap_err();
        assert!(error.unsupported, "{}", error.message);
    }
    assert!(repository.scopes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn media_is_resolved_and_validated_before_any_delivery() {
    let provider = with_media(Some(MediaKind::Image), b"png");
    let steps = plan_of(&provider, &message(vec![image(Some("m"))]))
        .await
        .unwrap();
    let Step::File(file) = &steps[0] else {
        panic!("expected a file step");
    };
    assert_eq!(file.bytes, b"png");
    assert_eq!(file.caption.as_deref(), Some("cap"));
    assert_eq!(file.name, "report.pdf");

    for (provider, parts) in [
        // No opaque reference.
        (
            with_media(Some(MediaKind::Image), b"png"),
            vec![image(None)],
        ),
        // Nothing stored.
        (with_media(None, b""), vec![image(Some("m"))]),
        // Kind mismatch.
        (
            with_media(Some(MediaKind::Video), b"png"),
            vec![image(Some("m"))],
        ),
        // Empty and oversize payloads.
        (
            with_media(Some(MediaKind::Image), b""),
            vec![image(Some("m"))],
        ),
        (
            with_media(Some(MediaKind::Image), &[0u8; 17]),
            vec![image(Some("m"))],
        ),
    ] {
        let error = plan_of(&provider, &message(parts)).await.unwrap_err();
        assert!(!error.unsupported, "{}", error.message);
    }

    // A turn identity is required to read scoped media.
    let mut anonymous = message(vec![image(Some("m"))]);
    anonymous.turn_id = None;
    let error = plan_of(&provider, &anonymous).await.unwrap_err();
    assert!(error.message.contains("turn identity"));
}

#[tokio::test]
async fn voice_clips_become_captioned_voice_steps() {
    let provider = with_voice(Some(b"voice"));
    let steps = plan_of(
        &provider,
        &message(vec![text("transcript"), voice(Some("v"))]),
    )
    .await
    .unwrap();
    assert_eq!(steps.len(), 1);
    let Step::Voice(file) = &steps[0] else {
        panic!("expected a voice step");
    };
    assert_eq!(file.caption.as_deref(), Some("transcript"));
    assert_eq!(file.name, "reply.m4a");

    let captionless = plan_of(&provider, &message(vec![voice(Some("v"))]))
        .await
        .unwrap();
    assert!(matches!(&captionless[0], Step::Voice(f) if f.caption.is_none()));

    for (provider, parts) in [
        (with_voice(Some(b"voice")), vec![voice(None)]),
        (with_voice(None), vec![voice(Some("v"))]),
        (with_voice(Some(&[0u8; 9])), vec![voice(Some("v"))]),
        (
            with_voice(Some(b"voice")),
            vec![voice(Some("a")), voice(Some("b"))],
        ),
        (
            with_voice(Some(b"voice")),
            vec![image(Some("m")), voice(Some("v"))],
        ),
    ] {
        assert!(plan_of(&provider, &message(parts)).await.is_err());
    }
}

#[test]
fn targets_need_a_conversation_and_prefer_explicit_threads() {
    let mut empty = message(vec![text("x")]);
    empty.conversation_id = " ".into();
    assert!(target_of(&empty, "fake", "Fake").is_err());

    let mut threaded = message(vec![text("x")]);
    threaded.thread_id = Some("t1".into());
    threaded.reply_to_id = Some("r1".into());
    let target = target_of(&threaded, "fake", "Fake").unwrap();
    assert_eq!(target.thread.as_deref(), Some("t1"));
    assert_eq!(target.reply_to.as_deref(), Some("r1"));
    assert_eq!(target.channel.id, "fake");

    let mut topic = message(vec![text("x")]);
    topic.conversation_ref = Some(ConversationRef {
        id: "c2".into(),
        topic_id: Some("topic".into()),
        ..Default::default()
    });
    assert_eq!(
        target_of(&topic, "fake", "Fake").unwrap().thread.as_deref(),
        Some("topic")
    );
}

#[tokio::test]
async fn sends_assemble_one_receipt_with_per_part_tokens() {
    let provider = FakeProvider::default();
    let outcome = send_typed(&provider, &message(vec![text("hello")]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["m1"]);
    assert_eq!(outcome.receipt.edit_token.as_deref(), Some("m1"));
    assert_eq!(outcome.receipt.delete_token.as_deref(), Some("m1"));
    assert!(outcome.error.is_none());
    assert_eq!(
        outcome.receipt.parts[0]
            .raw
            .as_ref()
            .unwrap()
            .channel
            .as_deref(),
        Some("fake")
    );

    let multi = send_typed(
        &provider,
        &message(vec![text("one two three four five six seven")]),
    )
    .await
    .unwrap();
    assert!(multi.receipt.parts.len() >= 2);
    assert!(
        multi.receipt.edit_token.is_none(),
        "several parts have no single token"
    );
}

#[tokio::test]
async fn a_failure_after_the_first_part_is_a_partial_send() {
    let provider = FakeProvider {
        fail_after: Some(1),
        ..Default::default()
    };
    let outcome = send_typed(
        &provider,
        &message(vec![text("one two three four five six seven")]),
    )
    .await
    .unwrap();
    assert_eq!(outcome.receipt.parts.len(), 1);
    let error = outcome.error.unwrap();
    assert!(error.sent_before_error && !error.retryable);

    let first_fails = FakeProvider {
        fail_after: Some(0),
        ..Default::default()
    };
    assert!(
        send_typed(&first_fails, &message(vec![text("x")]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn threads_and_replies_need_declared_addressing() {
    let mut provider = FakeProvider::default();
    provider.caps.addressing.thread_id = false;
    provider.caps.addressing.reply_to_id = false;
    let mut threaded = message(vec![text("x")]);
    threaded.thread_id = Some("t".into());
    assert!(
        send_typed(&provider, &threaded)
            .await
            .unwrap_err()
            .unsupported
    );
    let mut reply = message(vec![text("x")]);
    reply.reply_to_id = Some("r".into());
    assert!(send_typed(&provider, &reply).await.unwrap_err().unsupported);
    assert!(provider.delivered.lock().unwrap().is_empty());
}

#[test]
fn typed_failures_survive_anyhow() {
    let error = to_anyhow(super::super::unsupported("nope"));
    let failure = error.downcast_ref::<NativeFailure>().unwrap();
    assert!(failure.0.unsupported);
}

#[tokio::test]
async fn a_bundling_provider_receives_every_step_in_one_delivery() {
    let provider = FakeProvider {
        bundle: true,
        ..Default::default()
    };
    let outcome = send_typed(&provider, &message(vec![text("one"), text("two")]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["bundle-1"]);
    assert_eq!(provider.delivered.lock().unwrap().as_slice(), ["bundle:2"]);
    assert!(outcome.error.is_none());
}
