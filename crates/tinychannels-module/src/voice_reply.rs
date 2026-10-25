//! Outbound voice replies for module-delivered turns.
//!
//! The host supplies the ports (speech synthesis, TinyVoice transcoding, scoped
//! media storage, the configured reply mode). This module owns the pipeline:
//! apply the reply mode to the turn's inbound facts, synthesize the final text,
//! transcode it to a codec the channel accepts, store it under a host-owned
//! opaque reference and hand the provider an `OutboundVoice::Media` part.
//! Every failure falls back to the text reply that was already going out.

use crate::host::HostChannels;
use std::sync::Arc;
use tinychannels::host::{
    SpeechSynthesizer, VoiceCodec, VoiceMediaStore, VoiceReplyConstraints, VoiceReplyMode,
    prepare_voice_reply, should_synthesize_reply,
};
use tinychannels_bus::{
    ChannelCapabilities, ChannelRef, ConversationRef, HostVoiceReplyMode,
    HostVoiceReplyPolicyRequest, HostVoiceScope, OutboundContentPart, OutboundVoice,
};

/// Whether the latest inbound message of each conversation carried a voice
/// note, so a `mirror` reply can answer in kind. The host's turn id is not the
/// provider message id, so the fact is keyed by conversation and replaced by
/// every later inbound message.
pub(crate) mod inbound_voice {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{LazyLock, Mutex};

    const CAPACITY: usize = 1024;

    pub(super) struct Latest {
        capacity: usize,
        by_conversation: HashMap<String, bool>,
        order: VecDeque<String>,
    }

    impl Latest {
        pub(super) fn new(capacity: usize) -> Self {
            Self {
                capacity,
                by_conversation: HashMap::new(),
                order: VecDeque::new(),
            }
        }

        pub(super) fn record(&mut self, conversation_id: &str, has_voice: bool) {
            if conversation_id.trim().is_empty() {
                return;
            }
            if self
                .by_conversation
                .insert(conversation_id.to_owned(), has_voice)
                .is_none()
            {
                self.order.push_back(conversation_id.to_owned());
                if self.order.len() > self.capacity
                    && let Some(oldest) = self.order.pop_front()
                {
                    self.by_conversation.remove(&oldest);
                }
            }
        }

        pub(super) fn has_voice(&self, conversation_id: &str) -> bool {
            self.by_conversation
                .get(conversation_id)
                .copied()
                .unwrap_or(false)
        }
    }

    static LATEST: LazyLock<Mutex<Latest>> = LazyLock::new(|| Mutex::new(Latest::new(CAPACITY)));

    /// Record whether the newest inbound message in `conversation_id` has voice.
    pub(crate) fn record(conversation_id: &str, has_voice: bool) {
        LATEST
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .record(conversation_id, has_voice);
    }

    pub(crate) fn latest_has_voice(conversation_id: &str) -> bool {
        LATEST
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .has_voice(conversation_id)
    }
}

/// Host speech synthesis bridge; the module is the authenticated source of the
/// scope's provider identity, as for media and transcription.
pub(crate) struct ModuleSynthesizer(pub(crate) HostChannels, pub(crate) String);

#[async_trait::async_trait]
impl tinychannels::host::SpeechSynthesizer for ModuleSynthesizer {
    async fn synthesize(
        &self,
        _request: tinychannels::host::SpeechRequest,
    ) -> anyhow::Result<tinychannels::host::SpeechResult> {
        anyhow::bail!("scoped voice synthesis requires a conversation and turn identity")
    }

    async fn synthesize_scoped(
        &self,
        mut scope: HostVoiceScope,
        request: tinychannels::host::SpeechRequest,
    ) -> anyhow::Result<tinychannels::host::SpeechResult> {
        if scope.conversation.id.trim().is_empty() || scope.turn_id.trim().is_empty() {
            anyhow::bail!("voice callback requires a conversation and turn identity");
        }
        scope.configured_instance.clone_from(&self.1);
        scope.channel.id = tinychannels_bus::provider_id(&self.1).to_owned();
        scope.channel.account_id = Some(self.1.clone());
        let result = self
            .0
            .synthesize_speech(tinychannels_bus::HostSpeechSynthesisRequest {
                scope,
                text: request.text,
                voice: request.voice,
                format: request.format,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(tinychannels::host::SpeechResult {
            audio_base64: result.audio_base64,
            mime_type: result.mime_type,
            duration_ms: result.duration_ms,
            visemes: result.visemes,
        })
    }
}

/// TinyVoice transcoding over the loaded codec module's bus contract.
pub(crate) struct ModuleVoiceCodec(pub(crate) HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::VoiceCodec for ModuleVoiceCodec {
    async fn transcode(
        &self,
        request: tinychannels::host::VoiceCodecRequest,
    ) -> anyhow::Result<tinychannels::host::VoiceCodecResult> {
        use tinyvoice_bus::codec::{AudioCodec, CodecTranscodeRequest};
        let parse = |format: &str| match format {
            "wav_pcm16" | "audio/wav" => Ok(AudioCodec::WavPcm16),
            "ogg_opus" | "audio/ogg" => Ok(AudioCodec::OggOpus),
            "m4a_aac" | "audio/mp4" => Ok(AudioCodec::M4aAac),
            "mp3" | "audio/mpeg" => Ok(AudioCodec::Mp3),
            "amr_nb" | "audio/amr" => Ok(AudioCodec::AmrNb),
            _ => anyhow::bail!("unsupported TinyVoice codec format"),
        };
        let result = self
            .0
            .transcode_voice(CodecTranscodeRequest {
                audio_base64: request.audio_base64,
                from: parse(&request.from)?,
                target: parse(&request.target)?,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(tinychannels::host::VoiceCodecResult {
            audio_base64: result.audio_base64,
            duration_ms: None,
            waveform: None,
        })
    }
}

/// The host's configured reply mode for one scoped turn.
#[async_trait::async_trait]
pub(crate) trait ReplyPolicy: Send + Sync {
    async fn reply_policy(
        &self,
        request: HostVoiceReplyPolicyRequest,
    ) -> anyhow::Result<tinychannels_bus::HostVoiceReplyPolicy>;
}

#[async_trait::async_trait]
impl ReplyPolicy for HostChannels {
    async fn reply_policy(
        &self,
        request: HostVoiceReplyPolicyRequest,
    ) -> anyhow::Result<tinychannels_bus::HostVoiceReplyPolicy> {
        self.voice_reply_policy(request)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

/// The outbound voice pipeline bound to one configured provider instance.
pub(crate) struct SpokenReply {
    pub(crate) instance: String,
    pub(crate) policy: Arc<dyn ReplyPolicy>,
    pub(crate) synthesizer: Arc<dyn SpeechSynthesizer>,
    pub(crate) codec: Arc<dyn VoiceCodec>,
    pub(crate) store: Arc<dyn VoiceMediaStore>,
}

impl SpokenReply {
    /// The voice part to deliver beside `text`, or `None` to send text only:
    /// the channel cannot synthesize, the host policy says not to speak, or any
    /// pipeline stage failed.
    pub(crate) async fn voice_part(
        &self,
        conversation: &ConversationRef,
        turn_id: &str,
        text: &str,
        capabilities: &ChannelCapabilities,
    ) -> Option<OutboundContentPart> {
        if text.trim().is_empty()
            || !capabilities.voice_out.supported
            || !capabilities.voice_out.synthesis
        {
            return None;
        }
        let scope = HostVoiceScope {
            configured_instance: self.instance.clone(),
            channel: ChannelRef {
                id: tinychannels_bus::provider_id(&self.instance).to_owned(),
                account_id: Some(self.instance.clone()),
            },
            conversation: conversation.clone(),
            turn_id: turn_id.to_owned(),
        };
        let inbound_has_voice = inbound_voice::latest_has_voice(&conversation.id);
        let policy = match self
            .policy
            .reply_policy(HostVoiceReplyPolicyRequest {
                scope: scope.clone(),
                inbound_has_voice,
                explicitly_requested: false,
            })
            .await
        {
            Ok(policy) => policy,
            Err(error) => {
                tracing::debug!(channel = %self.instance, "voice reply policy unavailable: {error}");
                return None;
            }
        };
        let mode = match policy.mode {
            HostVoiceReplyMode::Mirror => VoiceReplyMode::Mirror,
            HostVoiceReplyMode::Always => VoiceReplyMode::Always,
            HostVoiceReplyMode::Never => VoiceReplyMode::Never,
            HostVoiceReplyMode::SpeakReply => VoiceReplyMode::SpeakReply,
        };
        if !should_synthesize_reply(mode, inbound_has_voice, false) {
            return None;
        }
        let prepared = prepare_voice_reply(
            scope,
            text.to_owned(),
            None,
            self.synthesizer.as_ref(),
            self.codec.as_ref(),
            self.store.as_ref(),
            VoiceReplyConstraints {
                accepted_codecs: &capabilities.voice_out.accepted_codecs,
                max_size_bytes: capabilities.voice_out.max_size_bytes,
                max_duration_ms: capabilities.voice_out.max_duration_ms,
            },
        )
        .await;
        match prepared {
            Ok(prepared) => Some(OutboundContentPart::Voice {
                voice: OutboundVoice::Media {
                    media: prepared.note.media,
                    codec: prepared.note.codec,
                    duration_ms: prepared.note.duration_ms,
                },
            }),
            Err(error) => {
                tracing::warn!(channel = %self.instance, "voice reply unavailable, sending text only: {error}");
                None
            }
        }
    }
}

/// Stub ports shared by the voice pipeline tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(crate) struct StubPolicy(pub(crate) Option<HostVoiceReplyMode>);

    #[async_trait::async_trait]
    impl ReplyPolicy for StubPolicy {
        async fn reply_policy(
            &self,
            _request: HostVoiceReplyPolicyRequest,
        ) -> anyhow::Result<tinychannels_bus::HostVoiceReplyPolicy> {
            self.0
                .map(|mode| tinychannels_bus::HostVoiceReplyPolicy { mode })
                .ok_or_else(|| anyhow::anyhow!("policy unavailable"))
        }
    }

    #[derive(Default)]
    pub(crate) struct StubSynth {
        pub(crate) calls: AtomicUsize,
        pub(crate) fail: bool,
    }

    #[async_trait::async_trait]
    impl SpeechSynthesizer for StubSynth {
        async fn synthesize(
            &self,
            _request: tinychannels::host::SpeechRequest,
        ) -> anyhow::Result<tinychannels::host::SpeechResult> {
            self.synthesize_scoped(HostVoiceScope::default(), _request)
                .await
        }

        async fn synthesize_scoped(
            &self,
            _scope: HostVoiceScope,
            _request: tinychannels::host::SpeechRequest,
        ) -> anyhow::Result<tinychannels::host::SpeechResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("synthesis failed");
            }
            Ok(tinychannels::host::SpeechResult {
                audio_base64: "YQ==".into(),
                mime_type: "audio/wav".into(),
                duration_ms: Some(900),
                visemes: None,
            })
        }
    }

    pub(crate) struct StubCodec;

    #[async_trait::async_trait]
    impl VoiceCodec for StubCodec {
        async fn transcode(
            &self,
            _request: tinychannels::host::VoiceCodecRequest,
        ) -> anyhow::Result<tinychannels::host::VoiceCodecResult> {
            Ok(tinychannels::host::VoiceCodecResult {
                audio_base64: "b2dn".into(),
                duration_ms: Some(900),
                waveform: None,
            })
        }
    }

    pub(crate) struct StubStore;

    #[async_trait::async_trait]
    impl VoiceMediaStore for StubStore {
        async fn store_voice(
            &self,
            _request: tinychannels::host::VoiceMediaRequest,
        ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
            Ok(tinychannels_bus::HostStoreVoiceMediaResult {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("voice-1".into()),
                    ..Default::default()
                },
                accessible_url: None,
            })
        }
    }

    pub(crate) fn voice_capabilities() -> ChannelCapabilities {
        let mut capabilities = ChannelCapabilities::unknown();
        capabilities.voice_out.supported = true;
        capabilities.voice_out.synthesis = true;
        capabilities.voice_out.accepted_codecs = vec!["ogg_opus".into()];
        capabilities
    }

    pub(crate) fn spoken_with(
        mode: Option<HostVoiceReplyMode>,
        synth: Arc<StubSynth>,
    ) -> SpokenReply {
        SpokenReply {
            instance: "telegram".into(),
            policy: Arc::new(StubPolicy(mode)),
            synthesizer: synth,
            codec: Arc::new(StubCodec),
            store: Arc::new(StubStore),
        }
    }

    pub(crate) fn spoken_reply(mode: HostVoiceReplyMode) -> SpokenReply {
        spoken_with(Some(mode), Arc::new(StubSynth::default()))
    }
}

#[cfg(test)]
#[path = "voice_reply_tests.rs"]
mod tests;
