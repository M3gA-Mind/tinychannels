use super::*;
use async_trait::async_trait;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_bus::{MediaKind, MediaReference};

struct StubTranscriber {
    calls: AtomicUsize,
    result: anyhow::Result<super::super::TranscriptionResult>,
}

struct StubSynthesizer(anyhow::Result<super::super::SpeechResult>);

#[async_trait]
impl SpeechSynthesizer for StubSynthesizer {
    async fn synthesize(
        &self,
        _request: super::super::SpeechRequest,
    ) -> anyhow::Result<super::super::SpeechResult> {
        self.0
            .as_ref()
            .cloned()
            .map_err(|_| anyhow::anyhow!("fixture synthesis failure"))
    }
}

struct StubCodec {
    result: anyhow::Result<super::super::VoiceCodecResult>,
    requests: Mutex<Vec<super::super::VoiceCodecRequest>>,
}

#[async_trait]
impl VoiceCodec for StubCodec {
    async fn transcode(
        &self,
        request: super::super::VoiceCodecRequest,
    ) -> anyhow::Result<super::super::VoiceCodecResult> {
        self.requests.lock().unwrap().push(request);
        self.result
            .as_ref()
            .cloned()
            .map_err(|_| anyhow::anyhow!("fixture codec failure"))
    }
}

struct StubVoiceStore {
    result: anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult>,
    requests: Mutex<Vec<super::super::VoiceMediaRequest>>,
}

#[async_trait]
impl VoiceMediaStore for StubVoiceStore {
    async fn store_voice(
        &self,
        request: super::super::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        self.requests.lock().unwrap().push(request);
        self.result
            .as_ref()
            .cloned()
            .map_err(|_| anyhow::anyhow!("fixture storage failure"))
    }
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

fn voice_pipeline_stubs(
    source_mime: &str,
    encoded_base64: &str,
    encoded_duration_ms: Option<u64>,
    stored_opaque_id: Option<&str>,
) -> (StubSynthesizer, StubCodec, StubVoiceStore) {
    let synth = StubSynthesizer(Ok(super::super::SpeechResult {
        audio_base64: "AQIDBA==".into(),
        mime_type: source_mime.into(),
        duration_ms: Some(1_250),
        ..Default::default()
    }));
    let codec = StubCodec {
        result: Ok(super::super::VoiceCodecResult {
            audio_base64: encoded_base64.into(),
            duration_ms: encoded_duration_ms,
            waveform: Some(vec![0.2, 0.8]),
        }),
        requests: Mutex::new(Vec::new()),
    };
    let store = StubVoiceStore {
        result: Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media: MediaReference {
                opaque_id: stored_opaque_id.map(str::to_owned),
                url: Some("https://authorized.example/voice".into()),
                path: Some("/private/cache/voice".into()),
                local_cache_path: Some("/private/cache/voice".into()),
                ..Default::default()
            },
            accessible_url: Some("https://authorized.example/voice".into()),
        }),
        requests: Mutex::new(Vec::new()),
    };
    (synth, codec, store)
}

fn constraints(codecs: &[String]) -> VoiceReplyConstraints<'_> {
    VoiceReplyConstraints {
        accepted_codecs: codecs,
        max_size_bytes: None,
        max_duration_ms: None,
    }
}

#[tokio::test]
async fn prepare_voice_reply_transcodes_and_persists_scoped_opaque_audio() {
    let (synth, codec, store) =
        voice_pipeline_stubs("audio/wav", "AQIDBA==", None, Some("opaque-1"));
    let accepted = vec!["ogg_opus".into()];
    let prepared = prepare_voice_reply(
        scope(),
        "spoken response".into(),
        Some("voice-a".into()),
        &synth,
        &codec,
        &store,
        VoiceReplyConstraints {
            accepted_codecs: &accepted,
            max_size_bytes: Some(4),
            max_duration_ms: Some(1_250),
        },
    )
    .await
    .unwrap();

    let codec_request = codec.requests.lock().unwrap()[0].clone();
    assert_eq!(codec_request.from, "wav_pcm16");
    assert_eq!(codec_request.target, "ogg_opus");
    let stored = &store.requests.lock().unwrap()[0];
    assert_eq!(stored.scope.turn_id, "message-3");
    assert_eq!(
        stored.scope.conversation.topic_id.as_deref(),
        Some("topic-2")
    );
    assert_eq!(
        stored.direction,
        tinychannels_bus::HostVoiceMediaDirection::Outbound
    );
    assert_eq!(prepared.note.media.opaque_id.as_deref(), Some("opaque-1"));
    assert_eq!(prepared.note.media.url, None);
    assert_eq!(prepared.note.media.path, None);
    assert_eq!(prepared.note.media.local_cache_path, None);
    assert_eq!(prepared.note.media.size_bytes, Some(4));
    assert_eq!(prepared.note.duration_ms, Some(1_250));
    assert_eq!(prepared.note.waveform, Some(vec![0.2, 0.8]));
    assert_eq!(
        prepared.accessible_url.as_deref(),
        Some("https://authorized.example/voice")
    );
}

#[tokio::test]
async fn prepare_voice_reply_rejects_empty_scope_before_synthesis() {
    let (synth, codec, store) =
        voice_pipeline_stubs("audio/wav", "AQIDBA==", None, Some("opaque-1"));
    let accepted = vec!["ogg_opus".into()];
    let mut invalid_scope = scope();
    invalid_scope.turn_id.clear();
    let error = prepare_voice_reply(
        invalid_scope,
        "spoken response".into(),
        None,
        &synth,
        &codec,
        &store,
        constraints(&accepted),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("requires conversation and turn"));
    assert!(codec.requests.lock().unwrap().is_empty());
    assert!(store.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn prepare_voice_reply_enforces_size_duration_and_durable_reference() {
    let accepted = vec!["ogg_opus".into()];
    let (synth, codec, store) =
        voice_pipeline_stubs("audio/wav", "AQIDBA==", Some(1_251), Some("id"));
    let error = prepare_voice_reply(
        scope(),
        "reply".into(),
        None,
        &synth,
        &codec,
        &store,
        VoiceReplyConstraints {
            accepted_codecs: &accepted,
            max_size_bytes: Some(3),
            max_duration_ms: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("size limit"));

    let (synth, codec, store) =
        voice_pipeline_stubs("audio/wav", "AQIDBA==", Some(1_251), Some("id"));
    let error = prepare_voice_reply(
        scope(),
        "reply".into(),
        None,
        &synth,
        &codec,
        &store,
        VoiceReplyConstraints {
            accepted_codecs: &accepted,
            max_size_bytes: None,
            max_duration_ms: Some(1_250),
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("duration limit"));

    let (synth, codec, store) = voice_pipeline_stubs("audio/wav", "AQIDBA==", None, Some("  "));
    let error = prepare_voice_reply(
        scope(),
        "reply".into(),
        None,
        &synth,
        &codec,
        &store,
        constraints(&accepted),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("no durable opaque reference"));
}

#[tokio::test]
async fn prepare_voice_reply_uses_provider_codec_and_encoded_duration_metadata() {
    let (synth, codec, store) =
        voice_pipeline_stubs("audio/mp4", "AQIDBA==", Some(800), Some("media-2"));
    let accepted = vec!["mp3".into(), "m4a_aac".into()];
    let prepared = prepare_voice_reply(
        scope(),
        "hello".into(),
        None,
        &synth,
        &codec,
        &store,
        VoiceReplyConstraints {
            accepted_codecs: &accepted,
            max_size_bytes: Some(4),
            max_duration_ms: Some(800),
        },
    )
    .await
    .unwrap();

    let codec_request = codec.requests.lock().unwrap()[0].clone();
    assert_eq!(
        (codec_request.from.as_str(), codec_request.target.as_str()),
        ("m4a_aac", "m4a_aac")
    );
    let stored = &store.requests.lock().unwrap()[0];
    assert_eq!(stored.mime_type, "audio/mp4");
    assert_eq!(stored.file_name.as_deref(), Some("voice.m4a"));
    assert_eq!(stored.duration_ms, Some(800));
    assert_eq!(prepared.note.media.opaque_id.as_deref(), Some("media-2"));
    assert_eq!(
        prepared.note.media.content_type.as_deref(),
        Some("audio/mp4")
    );
    assert_eq!(prepared.note.media.codec.as_deref(), Some("m4a_aac"));
    assert_eq!(prepared.note.duration_ms, Some(800));
    assert_eq!(
        prepared.accessible_url.as_deref(),
        Some("https://authorized.example/voice")
    );
}

#[tokio::test]
async fn prepare_voice_reply_can_encode_measured_amr_for_app_bot() {
    let (synth, codec, store) =
        voice_pipeline_stubs("audio/wav", "AQIDBA==", Some(1_240), Some("amr-media"));
    let accepted = vec!["amr_nb".into()];
    let prepared = prepare_voice_reply(
        scope(),
        "hello over App Bot".into(),
        None,
        &synth,
        &codec,
        &store,
        VoiceReplyConstraints {
            accepted_codecs: &accepted,
            max_size_bytes: Some(2 * 1024 * 1024),
            max_duration_ms: None,
        },
    )
    .await
    .unwrap();

    let codec_request = codec.requests.lock().unwrap()[0].clone();
    assert_eq!(codec_request.from, "wav_pcm16");
    assert_eq!(codec_request.target, "amr_nb");
    let stored = &store.requests.lock().unwrap()[0];
    assert_eq!(stored.mime_type, "audio/amr");
    assert_eq!(stored.codec.as_deref(), Some("amr_nb"));
    assert_eq!(stored.file_name.as_deref(), Some("voice.amr"));
    assert_eq!(stored.duration_ms, Some(1_240));
    assert_eq!(prepared.note.media.opaque_id.as_deref(), Some("amr-media"));
    assert_eq!(
        prepared.note.media.content_type.as_deref(),
        Some("audio/amr")
    );
    assert_eq!(prepared.note.media.codec.as_deref(), Some("amr_nb"));
    assert_eq!(prepared.note.duration_ms, Some(1_240));
}

#[tokio::test]
async fn prepare_voice_reply_stops_before_storage_when_host_pipeline_cannot_proceed() {
    let accepted = vec!["ogg_opus".into()];
    let (synth, codec, store) = voice_pipeline_stubs("audio/aac", "AQIDBA==", None, Some("id"));
    let error = prepare_voice_reply(
        scope(),
        "reply".into(),
        None,
        &synth,
        &codec,
        &store,
        constraints(&accepted),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unsupported audio format"));
    assert!(codec.requests.lock().unwrap().is_empty());
    assert!(store.requests.lock().unwrap().is_empty());

    let (synth, codec, store) = voice_pipeline_stubs("audio/wav", "AQIDBA==", None, Some("id"));
    let error = prepare_voice_reply(
        scope(),
        "reply".into(),
        None,
        &synth,
        &codec,
        &store,
        constraints(&[]),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("no supported voice codec"));
    assert!(codec.requests.lock().unwrap().is_empty());
    assert!(store.requests.lock().unwrap().is_empty());

    let (synth, codec, mut store) = voice_pipeline_stubs("audio/wav", "AQIDBA==", None, Some("id"));
    store.result = Err(anyhow::anyhow!("fixture"));
    let error = prepare_voice_reply(
        scope(),
        "reply".into(),
        None,
        &synth,
        &codec,
        &store,
        constraints(&accepted),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("fixture storage failure"));
    assert_eq!(store.requests.lock().unwrap().len(), 1);
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
