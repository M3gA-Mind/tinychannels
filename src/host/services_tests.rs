use super::*;

struct UnsupportedApprovals;

#[async_trait]
impl ApprovalGate for UnsupportedApprovals {}

#[tokio::test]
async fn default_approval_gate_rejects_requests_and_does_not_parse_replies() {
    let gate: &dyn ApprovalGate = &UnsupportedApprovals;
    for reply in ["yes", "no", "1", "arbitrary text", ""] {
        assert_eq!(gate.parse_reply(reply), None);
    }
    let error = gate
        .request(ApprovalAsk {
            id: "approval".into(),
            prompt: "Proceed?".into(),
            choices: vec![],
            session_key: "session".into(),
            timeout_secs: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "interactive approval requests are not supported by this host"
    );
}

struct LegacyIngressSink;

#[async_trait]
impl EventSink for LegacyIngressSink {
    async fn publish(
        &self,
        _domain: &str,
        _kind: &str,
        _payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[allow(
    deprecated,
    reason = "this pins the one-release v21 inbound compatibility default"
)]
async fn legacy_event_sink_declines_v22_inbound_for_message_channel_fallback() {
    let sink = LegacyIngressSink;
    assert!(
        !sink
            .publish_inbound(tinychannels_bus::ChannelMessage::default())
            .await
            .unwrap()
    );
    assert!(
        !sink
            .publish_ingress(tinychannels_bus::ChannelIngress::Inbound(
                tinychannels_bus::ChannelInboundEnvelope::default()
            ))
            .await
            .unwrap()
    );
}

struct LegacyVoicePorts;

#[async_trait]
impl Transcriber for LegacyVoicePorts {
    fn name(&self) -> &str {
        "legacy"
    }

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> anyhow::Result<TranscriptionResult> {
        Ok(TranscriptionResult {
            text: request.file_name.unwrap_or_default(),
            language: request.language,
            duration_secs: Some(0.5),
        })
    }
}

#[async_trait]
impl SpeechSynthesizer for LegacyVoicePorts {
    async fn synthesize(&self, request: SpeechRequest) -> anyhow::Result<SpeechResult> {
        Ok(SpeechResult {
            audio_base64: request.text,
            mime_type: "audio/wav".into(),
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn legacy_voice_ports_delegate_scoped_calls_and_media_resolution_fails_closed() {
    let ports = LegacyVoicePorts;
    let scope = tinychannels_bus::HostVoiceScope::default();
    let transcript = ports
        .transcribe_scoped(
            scope.clone(),
            TranscriptionRequest {
                audio_base64: "bytes".into(),
                mime_type: None,
                file_name: Some("voice.ogg".into()),
                language: Some("en".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(transcript.text, "voice.ogg");
    assert_eq!(transcript.language.as_deref(), Some("en"));
    assert_eq!(transcript.duration_secs, Some(0.5));

    let speech = ports
        .synthesize_scoped(
            scope.clone(),
            SpeechRequest {
                text: "spoken bytes".into(),
                voice: None,
                format: None,
                thread_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(speech.audio_base64, "spoken bytes");
    assert_eq!(speech.mime_type, "audio/wav");

    let store = DefaultVoiceMediaStore;
    let error = store
        .read_voice(scope, "opaque-id".into())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "host voice media resolution is unavailable"
    );
}

struct DefaultVoiceMediaStore;

#[async_trait]
impl VoiceMediaStore for DefaultVoiceMediaStore {
    async fn store_voice(
        &self,
        _request: VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        unreachable!("this test only exercises the default read_voice behavior")
    }
}

struct UnsupportedMediaRepository;

#[async_trait]
impl MediaRepository for UnsupportedMediaRepository {
    async fn store_media(
        &self,
        _request: MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        unreachable!("this test exercises only the optional repository defaults")
    }

    async fn read_media(
        &self,
        _scope: MediaScope,
        _opaque_id: String,
        _max_bytes: u64,
    ) -> anyhow::Result<StoredMedia> {
        unreachable!("this test exercises only the optional repository defaults")
    }
}

#[tokio::test]
async fn media_repository_defaults_reject_streaming_and_deletion() {
    let repository = UnsupportedMediaRepository;
    let stream_error = repository
        .store_media_stream(MediaStreamRequest {
            metadata: crate::host::MediaMetadata {
                scope: MediaScope::default(),
                kind: tinychannels_bus::MediaKind::Unknown,
                content_type: "application/octet-stream".into(),
                file_name: None,
                duration_ms: None,
                codec: None,
                voice_direction: None,
                dedupe_key: None,
                max_bytes: 0,
            },
            reader: Box::pin(futures_util::stream::empty()),
        })
        .await
        .unwrap_err();
    assert_eq!(
        stream_error.to_string(),
        "media repository does not support streamed storage"
    );

    let delete_error = repository
        .delete_media(MediaScope::default(), "opaque-id".into())
        .await
        .unwrap_err();
    assert_eq!(
        delete_error.to_string(),
        "media repository does not support deletion"
    );
}
