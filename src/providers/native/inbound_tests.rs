use super::*;
use crate::providers::native::fixtures::*;
use tinychannels_bus::{ChannelEventKind, ConversationKind, VoiceTranscript};

fn conversation() -> ConversationRef {
    ConversationRef {
        kind: ConversationKind::Dm,
        id: "c1".into(),
        ..Default::default()
    }
}

fn file(mime: &str, voice: bool) -> InboundFile {
    InboundFile {
        bytes: b"bytes".to_vec(),
        mime: mime.into(),
        name: Some("a.bin".into()),
        duration_ms: Some(1200),
        voice,
        dedupe_key: "k".into(),
        transcript: None,
    }
}

struct Rig {
    media: Arc<StubMedia>,
    media_dyn: Arc<dyn MediaRepository>,
    stt_dyn: Arc<dyn Transcriber>,
}

fn rig(media: StubMedia, stt: StubTranscriber) -> Rig {
    let media = Arc::new(media);
    Rig {
        media_dyn: media.clone(),
        media,
        stt_dyn: Arc::new(stt),
    }
}

impl Rig {
    fn ingest(&self, with_stt: bool) -> Ingest<'_> {
        Ingest {
            provider: "fake",
            channel: ChannelRef {
                id: "fake".into(),
                account_id: None,
            },
            repository: Some(&self.media_dyn),
            transcriber: with_stt.then_some(&self.stt_dyn),
            file_bytes: 16,
            voice_bytes: 8,
        }
    }
}

fn voice_note(part: &ContentPart) -> &VoiceNote {
    match part {
        ContentPart::VoiceNote { voice_note } => voice_note,
        _ => panic!("expected voice note part"),
    }
}

#[test]
fn mime_helpers_classify_and_name_codecs() {
    assert_eq!(kind_for_mime("image/png"), MediaKind::Image);
    assert_eq!(kind_for_mime("video/mp4"), MediaKind::Video);
    assert_eq!(kind_for_mime("audio/ogg"), MediaKind::Audio);
    assert_eq!(kind_for_mime("application/pdf"), MediaKind::Document);
    assert_eq!(normalize_mime(" Audio/MP4 ; codecs=aac"), "audio/mp4");
    assert_eq!(codec_for("audio/amr").as_deref(), Some("amr_nb"));
    assert_eq!(codec_for("audio/mp4").as_deref(), Some("m4a_aac"));
    assert_eq!(codec_for("audio/wav"), None);
}

#[tokio::test]
async fn attachments_are_stored_as_typed_parts_in_received_order() {
    let rig = rig(Default::default(), Default::default());
    let mut ingested = Ingested::default();
    for (mime, is_voice) in [
        ("image/png", false),
        ("audio/mp4", true),
        ("application/pdf", false),
    ] {
        rig.ingest(false)
            .add(&mut ingested, &conversation(), "m1", file(mime, is_voice))
            .await;
    }
    assert!(matches!(ingested.parts[0], ContentPart::Image { .. }));
    assert!(matches!(ingested.parts[1], ContentPart::VoiceNote { .. }));
    assert!(matches!(ingested.parts[2], ContentPart::File { .. }));
    let stored = rig.media.stored.lock().unwrap();
    assert_eq!(stored.len(), 3);
    assert_eq!(stored[1].metadata.codec.as_deref(), Some("m4a_aac"));
    assert_eq!(
        stored[1].metadata.voice_direction,
        Some(HostVoiceMediaDirection::Inbound)
    );
    let note = voice_note(&ingested.parts[1]);
    assert_eq!(note.codec.as_deref(), Some("m4a_aac"));
    assert_eq!(note.duration_ms, Some(1200));
    assert!(note.media.url.is_none());
    assert!(note.media.path.is_none());
    assert!(note.media.local_cache_path.is_none());
}

#[tokio::test]
async fn attachments_are_typed_and_unsupported_ones_are_skipped() {
    let rig = rig(Default::default(), Default::default());
    let mut ingested = Ingested::default();
    for mime in ["image/png", "video/mp4", "audio/mpeg", "application/pdf"] {
        rig.ingest(false)
            .add(&mut ingested, &conversation(), "m1", file(mime, false))
            .await;
    }
    assert!(matches!(ingested.parts[0], ContentPart::Image { .. }));
    assert!(matches!(ingested.parts[1], ContentPart::Video { .. }));
    assert!(matches!(ingested.parts[2], ContentPart::Audio { .. }));
    assert!(matches!(ingested.parts[3], ContentPart::File { .. }));
    assert_eq!(media_of(&ingested.parts).len(), 4);

    let mut empty = file("image/png", false);
    empty.bytes.clear();
    for bad in [file("application/x-msdownload", false), empty] {
        rig.ingest(false)
            .add(&mut ingested, &conversation(), "m1", bad)
            .await;
    }
    assert_eq!(ingested.parts.len(), 4);
}

#[tokio::test]
async fn voice_transcript_is_enriched_without_reordering_the_media_part() {
    let rig = rig(Default::default(), Default::default());
    let mut ingested = Ingested::default();
    let mut clip = file("audio/ogg", true);
    clip.transcript = Some(VoiceTranscript {
        text: "provider transcript".into(),
        language: None,
        state: VoiceTranscriptState::Complete,
        source: VoiceTranscriptSource::Provider,
    });
    rig.ingest(true)
        .add(&mut ingested, &conversation(), "m1", clip)
        .await;
    assert_eq!(voice_note(&ingested.parts[0]).transcript.as_ref().unwrap().text, "provider transcript");

    let mut next = Ingested::default();
    rig.ingest(false)
        .add(&mut next, &conversation(), "m2", file("audio/ogg", true))
        .await;
    assert!(voice_note(&next.parts[0]).transcript.is_none());
}

#[tokio::test]
async fn unusable_voice_media_and_missing_storage_are_skipped() {
    let failing = rig(
        StubMedia {
            fail_store: true,
            ..Default::default()
        },
        Default::default(),
    );
    let mut parts = Ingested::default();
    failing
        .ingest(false)
        .add(&mut parts, &conversation(), "m1", file("audio/mp4", true))
        .await;
    assert!(parts.parts.is_empty());

    let rig = rig(Default::default(), Default::default());
    let mut no_storage = rig.ingest(false);
    no_storage.repository = None;
    no_storage
        .add(&mut parts, &conversation(), "m1", file("audio/mp4", true))
        .await;
    assert!(parts.parts.is_empty());

    let mut oversized = file("audio/mp4", true);
    oversized.bytes = vec![0; 9];
    rig.ingest(false)
        .add(&mut parts, &conversation(), "m1", oversized)
        .await;
    assert!(parts.parts.is_empty());
}

#[test]
fn assembly_keeps_text_first_and_attachment_parts_ordered() {
    let ingested = Ingested {
        parts: vec![
            ContentPart::Text {
                text: "first".into(),
                markdown_dialect: None,
            },
            ContentPart::Text {
                text: "second".into(),
                markdown_dialect: None,
            },
        ],
    };
    let parts = assemble("prefix", Some("md"), ingested);
    assert!(matches!(&parts[0], ContentPart::Text { markdown_dialect: Some(d), .. } if d == "md"));
    assert!(matches!(&parts[1], ContentPart::Text { text, .. } if text == "first"));
    assert!(matches!(&parts[2], ContentPart::Text { text, .. } if text == "second"));
    assert!(assemble("  ", None, Ingested::default()).is_empty());
}

#[tokio::test]
async fn publishing_reports_whether_the_sink_took_the_message() {
    let envelope = ChannelInboundEnvelope::default();
    assert!(!publish("fake", None, envelope.clone()).await);
    let taking: Arc<dyn EventSink> = Arc::new(RecordingEvents::delivering());
    assert!(publish("fake", Some(&taking), envelope.clone()).await);
    let refusing: Arc<dyn EventSink> = Arc::new(RecordingEvents::default());
    assert!(!publish("fake", Some(&refusing), envelope.clone()).await);
    let failing: Arc<dyn EventSink> = Arc::new(RecordingEvents { fail: true, ..Default::default() });
    assert!(!publish("fake", Some(&failing), envelope).await);
}

#[tokio::test]
async fn events_carry_the_provider_channel_and_survive_a_failing_sink() {
    let event = channel_event(
        "fake",
        "e1".into(),
        Some(5),
        Some(conversation()),
        ChannelEventKind::MessageDeleted { message_id: "m".into(), provider_message_id: None },
    );
    assert_eq!(event.channel.id, "fake");
    assert_eq!(event.version, 2);
    let recording = Arc::new(RecordingEvents::default());
    let sink: Arc<dyn EventSink> = recording.clone();
    publish_event("fake", Some(&sink), event.clone()).await;
    assert_eq!(recording.events().len(), 1);
    publish_event("fake", None, event.clone()).await;
    let failing: Arc<dyn EventSink> = Arc::new(RecordingEvents { fail: true, ..Default::default() });
    publish_event("fake", Some(&failing), event).await;
}
