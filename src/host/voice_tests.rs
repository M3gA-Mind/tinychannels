use super::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_bus::{MediaKind, MediaReference};

struct StubTranscriber {
    calls: AtomicUsize,
    result: anyhow::Result<super::super::TranscriptionResult>,
}

#[async_trait]
impl Transcriber for StubTranscriber {
    fn name(&self) -> &str {
        "stub"
    }

    async fn transcribe(
        &self,
        _request: TranscriptionRequest,
    ) -> anyhow::Result<super::super::TranscriptionResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.result {
            Ok(result) => Ok(result.clone()),
            Err(_) => anyhow::bail!("private fixture error"),
        }
    }
}

fn scope() -> HostVoiceScope {
    HostVoiceScope {
        configured_instance: "telegram:bot-1".into(),
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram:bot-1".into()),
        },
        conversation: tinychannels_bus::ConversationRef {
            id: "chat-1".into(),
            topic_id: Some("topic-2".into()),
            ..Default::default()
        },
        turn_id: "message-3".into(),
    }
}

fn stored_voice() -> VoiceNote {
    VoiceNote {
        media: MediaReference {
            opaque_id: Some("store-owned-voice-7".into()),
            kind: MediaKind::Audio,
            content_type: Some("audio/ogg".into()),
            ..Default::default()
        },
        codec: Some("ogg_opus".into()),
        duration_ms: Some(900),
        ..Default::default()
    }
}

fn request() -> TranscriptionRequest {
    TranscriptionRequest {
        audio_base64: "UklGRg==".into(),
        mime_type: Some("audio/ogg".into()),
        file_name: Some("voice.ogg".into()),
        language: None,
    }
}

#[tokio::test]
async fn voice_enrichment_keeps_durable_audio_when_transcription_fails() {
    let mut note = stored_voice();
    let media = note.media.clone();
    let transcriber = Arc::new(StubTranscriber {
        calls: AtomicUsize::new(0),
        result: Err(anyhow::anyhow!("fixture")),
    });

    let status = enrich_voice_note(
        &mut note,
        scope(),
        Some(request()),
        Some(transcriber.clone()),
    )
    .await;

    assert_eq!(status, VoiceEnrichmentStatus::Unavailable);
    assert_eq!(note.media, media);
    let transcript = note.transcript.expect("failure state is retained");
    assert_eq!(transcript.state, VoiceTranscriptState::Unavailable);
    assert!(transcript.text.is_empty());
    assert_eq!(transcriber.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn provider_transcript_wins_without_replacing_stored_audio() {
    let mut note = stored_voice();
    note.transcript = Some(VoiceTranscript {
        text: "provider words".into(),
        language: Some("en".into()),
        state: VoiceTranscriptState::Complete,
        source: VoiceTranscriptSource::Provider,
    });
    let media = note.media.clone();
    let transcriber = Arc::new(StubTranscriber {
        calls: AtomicUsize::new(0),
        result: Ok(super::super::TranscriptionResult {
            text: "host words".into(),
            language: None,
            duration_secs: None,
        }),
    });

    let status = enrich_voice_note(
        &mut note,
        scope(),
        Some(request()),
        Some(transcriber.clone()),
    )
    .await;

    assert_eq!(status, VoiceEnrichmentStatus::ProviderTranscript);
    assert_eq!(note.media, media);
    assert_eq!(note.transcript.as_ref().unwrap().text, "provider words");
    assert_eq!(transcriber.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn completed_host_transcript_survives_replay_without_stt_port() {
    let mut note = stored_voice();
    note.transcript = Some(VoiceTranscript {
        text: "persisted words".into(),
        language: Some("en".into()),
        state: VoiceTranscriptState::Complete,
        source: VoiceTranscriptSource::HostStt,
    });
    let media = note.media.clone();

    let status = enrich_voice_note(&mut note, scope(), None, None).await;

    assert_eq!(status, VoiceEnrichmentStatus::HostTranscription);
    assert_eq!(note.media, media);
    assert_eq!(note.transcript.unwrap().text, "persisted words");
}

#[tokio::test]
async fn host_transcription_attaches_text_to_the_existing_media_reference() {
    let mut note = stored_voice();
    let media = note.media.clone();
    let transcriber = Arc::new(StubTranscriber {
        calls: AtomicUsize::new(0),
        result: Ok(super::super::TranscriptionResult {
            text: "host words".into(),
            language: Some("en".into()),
            duration_secs: Some(1.25),
        }),
    });

    let status = enrich_voice_note(&mut note, scope(), Some(request()), Some(transcriber)).await;

    assert_eq!(status, VoiceEnrichmentStatus::HostTranscription);
    assert_eq!(note.media, media);
    assert_eq!(
        note.duration_ms,
        Some(900),
        "provider duration remains authoritative"
    );
    let transcript = note.transcript.unwrap();
    assert_eq!(transcript.text, "host words");
    assert_eq!(transcript.state, VoiceTranscriptState::Complete);
    assert_eq!(transcript.source, VoiceTranscriptSource::HostStt);
}

#[test]
fn reply_mode_respects_inbound_voice_and_explicit_turn_request() {
    assert!(should_synthesize_reply(VoiceReplyMode::Mirror, true, false));
    assert!(!should_synthesize_reply(
        VoiceReplyMode::Mirror,
        false,
        true
    ));
    assert!(should_synthesize_reply(
        VoiceReplyMode::Always,
        false,
        false
    ));
    assert!(!should_synthesize_reply(VoiceReplyMode::Never, true, true));
    assert!(should_synthesize_reply(
        VoiceReplyMode::SpeakReply,
        false,
        true
    ));
    assert!(!should_synthesize_reply(
        VoiceReplyMode::SpeakReply,
        true,
        false
    ));
}
