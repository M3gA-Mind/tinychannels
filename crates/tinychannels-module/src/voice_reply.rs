//! Outbound voice replies for module-delivered turns.
//!
//! The host supplies the ports (speech synthesis, TinyVoice transcoding, scoped
//! media storage, the configured reply mode). This module owns the pipeline:
//! apply the reply mode to the turn's inbound facts, synthesize the final text,
//! transcode it to a codec the channel accepts, store it under a host-owned
//! opaque reference and hand the provider an `OutboundVoice::Media` part.
//! Every failure falls back to the text reply that was already going out.

use crate::host::HostChannels;
use crate::provider::ModuleVoiceMediaStore;
use tinychannels::host::{
    VoiceReplyConstraints, VoiceReplyMode, prepare_voice_reply, should_synthesize_reply,
};
use tinychannels_bus::{
    ChannelCapabilities, ChannelRef, ConversationRef, HostVoiceReplyMode,
    HostVoiceReplyPolicyRequest, HostVoiceScope, OutboundContentPart, OutboundVoice,
};

/// Turns whose inbound message carried a voice note, so a `mirror` reply can
/// answer in kind. The key is the conversation and the inbound message id,
/// which the host uses as the turn id.
pub(crate) mod inbound_voice {
    use std::collections::VecDeque;
    use std::sync::{LazyLock, Mutex};

    const CAPACITY: usize = 1024;
    static TURNS: LazyLock<Mutex<VecDeque<(String, String)>>> =
        LazyLock::new(|| Mutex::new(VecDeque::new()));

    pub(crate) fn record(conversation_id: &str, turn_id: &str) {
        if conversation_id.trim().is_empty() || turn_id.trim().is_empty() {
            return;
        }
        let mut turns = TURNS.lock().unwrap_or_else(|e| e.into_inner());
        let key = (conversation_id.to_owned(), turn_id.to_owned());
        if turns.contains(&key) {
            return;
        }
        if turns.len() >= CAPACITY {
            turns.pop_front();
        }
        turns.push_back(key);
    }

    pub(crate) fn contains(conversation_id: &str, turn_id: &str) -> bool {
        TURNS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|(conversation, turn)| conversation == conversation_id && turn == turn_id)
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

/// The outbound voice pipeline bound to one configured provider instance.
pub(crate) struct SpokenReply {
    pub(crate) host: HostChannels,
    pub(crate) instance: String,
    pub(crate) synthesizer: ModuleSynthesizer,
    pub(crate) codec: ModuleVoiceCodec,
    pub(crate) store: ModuleVoiceMediaStore,
}

impl SpokenReply {
    /// The voice part to deliver beside `text`, or `None` to send text alone:
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
        let inbound_has_voice = inbound_voice::contains(&conversation.id, turn_id);
        let policy = match self
            .host
            .voice_reply_policy(HostVoiceReplyPolicyRequest {
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
            &self.synthesizer,
            &self.codec,
            &self.store,
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

#[cfg(test)]
#[path = "voice_reply_tests.rs"]
mod tests;
