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
    voice: Arc<StubVoice>,
    media_dyn: Arc<dyn MediaRepository>,
    voice_dyn: Arc<dyn VoiceMediaStore>,
    stt_dyn: Arc<dyn Transcriber>,
}

fn rig(media: StubMedia, voice: StubVoice, stt: StubTranscriber) -> Rig {
    let media = Arc::new(media);
    let voice = Arc::new(voice);
    Rig {
        media_dyn: media.clone(),
        voice_dyn: voice.clone(),
        media,
        voice,
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
            voice_store: Some(&self.voice_dyn),
            transcriber: with_stt.then_some(&self.stt_dyn),
            file_bytes: 16,
            voice_bytes: 8,
        }
    }
}

#[test]
fn mime_helpers_classify_and_name_codecs() {
    assert_eq!(kind_for_mime("image/png"), MediaKind::Image);
    assert_eq!(kind_for_mime("video/mp4"), MediaKind::Video);
    assert_eq!(kind_for_mime("audio/ogg"), MediaKind::Audio);
    assert_eq!(kind_for_mime("application/pdf"), MediaKind::Document);
    assert_eq!(normalize_mime(" Audio/MP4 ; codecs=aac"), "audio/mp4");
    for (mime, codec) in [
        ("audio/amr", Some("amr_nb")),
        ("audio/mp4", Some("m4a_aac")),
        ("audio/aac", Some("m4a_aac")),
        ("audio/mpeg", Some("mp3")),
        ("audio/ogg", Some("ogg_opus")),
        ("audio/x-caf", Some("caf")),
        ("audio/wav", None),
    ] {
        assert_eq!(codec_for(mime).as_deref(), codec, "{mime}");
    }
}

#[tokio::test]
async fn amr_voice_is_stored_with_the_native_codec_tag() {
    let rig = rig(Default::default(), Default::default(), Default::default());
    let mut ingest = rig.ingest(false);
    ingest.voice_bytes = 2 * 1024 * 1024;
    let mut amr = file("audio/amr", true);
    amr.bytes = include_bytes!("../../../tests/fixtures/dingtalk/amrnb_mode0.amr").to_vec();
    amr.name = Some("voice.amr".into());

    let mut ingested = Ingested::default();
    ingest
        .add(&mut ingested, &conversation(), "amr-message", amr)
        .await;

    let note = ingested.voice.expect("allowed AMR voice should be stored");
    assert_eq!(note.media.content_type.as_deref(), Some("audio/amr"));
    assert_eq!(note.codec.as_deref(), Some("amr_nb"));
    let stored = rig.voice.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].mime_type, "audio/amr");
    assert_eq!(stored[0].codec.as_deref(), Some("amr_nb"));
    assert_eq!(stored[0].file_name.as_deref(), Some("voice.amr"));
}

#[tokio::test]
async fn files_become_typed_parts_of_the_right_kind() {
    let rig = rig(Default::default(), Default::default(), Default::default());
    let mut ingested = Ingested::default();
    for mime in ["image/png", "video/mp4", "audio/mpeg", "application/pdf"] {
        rig.ingest(false)
            .add(&mut ingested, &conversation(), "m1", file(mime, false))
            .await;
    }
    assert!(matches!(ingested.parts[0], ContentPart::Image { .. }));
    assert!(matches!(
        ingested.parts[1],
        ContentPart::Video {
            duration_ms: Some(1200),
            ..
        }
    ));
    assert!(matches!(ingested.parts[2], ContentPart::Audio { .. }));
    assert!(matches!(ingested.parts[3], ContentPart::File { .. }));
    assert_eq!(rig.media.stored.lock().unwrap().len(), 4);
    assert_eq!(media_of(&ingested.parts).len(), 4);
}

#[tokio::test]
async fn unusable_files_are_skipped_without_failing_the_message() {
    let rig = rig(Default::default(), Default::default(), Default::default());
    let mut ingested = Ingested::default();
    let mut empty = file("image/png", false);
    empty.bytes.clear();
    let mut huge = file("image/png", false);
    huge.bytes = vec![0; 17];
    for bad in [file("application/x-msdownload", false), empty, huge] {
        rig.ingest(false)
            .add(&mut ingested, &conversation(), "m1", bad)
            .await;
    }
    // No repository at all.
    let mut bare = rig.ingest(false);
    bare.repository = None;
    bare.add(
        &mut ingested,
        &conversation(),
        "m1",
        file("image/png", false),
    )
    .await;
    // A repository that cannot store.
    let failing = self::rig(
        StubMedia {
            fail_store: true,
            ..Default::default()
        },
        Default::default(),
        Default::default(),
    );
    failing
        .ingest(false)
        .add(
            &mut ingested,
            &conversation(),
            "m1",
            file("image/png", false),
        )
        .await;
    assert!(ingested.parts.is_empty() && ingested.voice.is_none());
}

#[tokio::test]
async fn voice_notes_are_stored_scrubbed_and_transcribed() {
    let rig = rig(Default::default(), Default::default(), Default::default());
    let mut ingested = Ingested::default();
    rig.ingest(true)
        .add(
            &mut ingested,
            &conversation(),
            "m1",
            file("audio/mp4", true),
        )
        .await;
    let note = ingested.voice.as_ref().unwrap();
    assert_eq!(note.codec.as_deref(), Some("m4a_aac"));
    assert_eq!(note.duration_ms, Some(1200));
    assert!(note.media.url.is_none(), "private provider URLs never leak");
    let transcript = note.transcript.as_ref().unwrap();
    assert_eq!(transcript.source, VoiceTranscriptSource::HostStt);
    assert_eq!(transcript.state, VoiceTranscriptState::Complete);
    assert_eq!(rig.voice.stored.lock().unwrap().len(), 1);

    // A second clip in the same message is ignored.
    rig.ingest(true)
        .add(
            &mut ingested,
            &conversation(),
            "m1",
            file("audio/mp4", true),
        )
        .await;
    assert_eq!(rig.voice.stored.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn provider_transcripts_win_and_missing_stt_leaves_it_to_the_ingress() {
    let rig = rig(Default::default(), Default::default(), Default::default());
    let mut ingested = Ingested::default();
    let mut clip = file("audio/ogg", true);
    clip.transcript = Some(VoiceTranscript {
        text: "provider".into(),
        language: None,
        state: VoiceTranscriptState::Complete,
        source: VoiceTranscriptSource::Provider,
    });
    rig.ingest(true)
        .add(&mut ingested, &conversation(), "m1", clip)
        .await;
    assert_eq!(ingested.voice.unwrap().transcript.unwrap().text, "provider");

    let mut plain = Ingested::default();
    rig.ingest(false)
        .add(&mut plain, &conversation(), "m2", file("audio/ogg", true))
        .await;
    assert!(plain.voice.unwrap().transcript.is_none());
}

#[tokio::test]
async fn transcription_failures_and_blank_results_leave_no_transcript() {
    for stt in [
        StubTranscriber {
            text: "  ",
            fail: false,
        },
        StubTranscriber {
            text: "x",
            fail: true,
        },
    ] {
        let rig = rig(Default::default(), Default::default(), stt);
        let mut ingested = Ingested::default();
        rig.ingest(true)
            .add(
                &mut ingested,
                &conversation(),
                "m1",
                file("audio/mp4", true),
            )
            .await;
        assert!(ingested.voice.unwrap().transcript.is_none());
    }
}

#[tokio::test]
async fn voice_storage_failures_and_missing_references_drop_the_clip() {
    for voice in [
        StubVoice {
            fail_store: true,
            ..Default::default()
        },
        StubVoice {
            empty_reference: true,
            ..Default::default()
        },
    ] {
        let rig = rig(Default::default(), voice, Default::default());
        let mut ingested = Ingested::default();
        rig.ingest(true)
            .add(
                &mut ingested,
                &conversation(),
                "m1",
                file("audio/mp4", true),
            )
            .await;
        assert!(ingested.voice.is_none() && ingested.parts.is_empty());
    }
    // Oversized for a voice clip (limit 8), though fine for a file.
    let rig = rig(Default::default(), Default::default(), Default::default());
    let mut clip = file("audio/mp4", true);
    clip.bytes = vec![0; 12];
    let mut ingested = Ingested::default();
    rig.ingest(true)
        .add(&mut ingested, &conversation(), "m1", clip)
        .await;
    assert!(ingested.voice.is_none());
    // Without a voice store the same clip is plain audio.
    let mut no_voice = rig.ingest(false);
    no_voice.voice_store = None;
    let mut plain = file("audio/mp4", true);
    plain.bytes = vec![0; 12];
    no_voice
        .add(&mut ingested, &conversation(), "m1", plain)
        .await;
    assert!(matches!(ingested.parts[0], ContentPart::Audio { .. }));
}

#[test]
fn parts_are_assembled_text_first_then_voice_then_media() {
    let rig = rig(Default::default(), Default::default(), Default::default());
    let _ = rig;
    let parts = assemble("hi", Some("md"), Ingested::default());
    assert!(matches!(
        &parts[0],
        ContentPart::Text { markdown_dialect: Some(d), .. } if d == "md"
    ));
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
    let failing: Arc<dyn EventSink> = Arc::new(RecordingEvents {
        fail: true,
        ..Default::default()
    });
    assert!(!publish("fake", Some(&failing), envelope).await);
}

#[tokio::test]
async fn events_carry_the_provider_channel_and_survive_a_failing_sink() {
    let kind = ChannelEventKind::MessageDeleted {
        message_id: "m".into(),
        provider_message_id: None,
    };
    let event = channel_event(
        "fake",
        "e1".into(),
        Some(5),
        Some(conversation()),
        kind.clone(),
    );
    assert_eq!(event.channel.id, "fake");
    assert_eq!(event.version, 2);
    let recording = Arc::new(RecordingEvents::default());
    let sink: Arc<dyn EventSink> = recording.clone();
    publish_event("fake", Some(&sink), event.clone()).await;
    assert_eq!(recording.events().len(), 1);
    publish_event("fake", None, event.clone()).await;
    let failing: Arc<dyn EventSink> = Arc::new(RecordingEvents {
        fail: true,
        ..Default::default()
    });
    publish_event("fake", Some(&failing), event).await;
}
