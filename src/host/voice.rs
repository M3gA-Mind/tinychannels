//! Shared inbound voice enrichment and scoped reply-mode decisions.

use super::{
    SpeechRequest, SpeechSynthesizer, Transcriber, TranscriptionRequest, VoiceCodec,
    VoiceCodecRequest, VoiceMediaRequest, VoiceMediaStore,
};
use std::sync::Arc;
use tinychannels_bus::{
    HostVoiceScope, VoiceNote, VoiceTranscript, VoiceTranscriptSource, VoiceTranscriptState,
};

/// Product voice mode supplied by the host for the active scoped turn.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum VoiceReplyMode {
    /// Reply with speech only when the inbound turn included a voice note.
    #[default]
    Mirror,
    /// Always synthesize a spoken reply.
    Always,
    /// Never synthesize a spoken reply.
    Never,
    /// Synthesize only when the host marks this turn as explicitly requested.
    SpeakReply,
}

/// Outcome of attempting to enrich a voice note while preserving its media.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoiceEnrichmentStatus {
    ProviderTranscript,
    HostTranscription,
    Unavailable,
}

/// Keep provider transcript facts when present; otherwise ask the host STT
/// capability and attach its result to the existing audio note.
pub async fn enrich_voice_note(
    note: &mut VoiceNote,
    scope: HostVoiceScope,
    request: Option<TranscriptionRequest>,
    transcriber: Option<Arc<dyn Transcriber>>,
) -> VoiceEnrichmentStatus {
    if let Some(transcript) = note.transcript.as_ref().filter(|transcript| {
        transcript.state == VoiceTranscriptState::Complete && !transcript.text.trim().is_empty()
    }) {
        return if transcript.source == VoiceTranscriptSource::Provider {
            VoiceEnrichmentStatus::ProviderTranscript
        } else {
            VoiceEnrichmentStatus::HostTranscription
        };
    }

    let Some((request, transcriber)) = request.zip(transcriber) else {
        note.transcript = Some(unavailable_transcript());
        return VoiceEnrichmentStatus::Unavailable;
    };

    match transcriber.transcribe_scoped(scope, request).await {
        Ok(result) if !result.text.trim().is_empty() => {
            if note.duration_ms.is_none() {
                note.duration_ms = result.duration_secs.and_then(|seconds| {
                    (seconds.is_finite() && seconds >= 0.0)
                        .then(|| (seconds * 1000.0).round() as u64)
                });
            }
            note.transcript = Some(VoiceTranscript {
                text: result.text,
                language: result.language,
                state: VoiceTranscriptState::Complete,
                source: VoiceTranscriptSource::HostStt,
            });
            VoiceEnrichmentStatus::HostTranscription
        }
        Ok(_) | Err(_) => {
            note.transcript = Some(unavailable_transcript());
            VoiceEnrichmentStatus::Unavailable
        }
    }
}

/// Apply the host-selected reply mode to turn facts.
pub fn should_synthesize_reply(
    mode: VoiceReplyMode,
    inbound_has_voice: bool,
    explicitly_requested: bool,
) -> bool {
    match mode {
        VoiceReplyMode::Mirror => inbound_has_voice,
        VoiceReplyMode::Always => true,
        VoiceReplyMode::Never => false,
        VoiceReplyMode::SpeakReply => explicitly_requested,
    }
}

/// Prepared outbound media and a host-authorized public fallback link.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedVoiceReply {
    pub note: VoiceNote,
    pub accessible_url: Option<String>,
}

/// Provider constraints applied before audio is durably stored.
pub struct VoiceReplyConstraints<'a> {
    pub accepted_codecs: &'a [String],
    pub max_size_bytes: Option<u64>,
    pub max_duration_ms: Option<u64>,
}

/// Run the shared text-to-speech, TinyVoice codec, and host persistence path
/// for one explicitly selected voice reply. Providers only resolve the final
/// opaque media reference and perform their native upload.
pub async fn prepare_voice_reply(
    scope: HostVoiceScope,
    text: String,
    voice: Option<String>,
    synthesizer: &dyn SpeechSynthesizer,
    codec: &dyn VoiceCodec,
    store: &dyn VoiceMediaStore,
    constraints: VoiceReplyConstraints<'_>,
) -> anyhow::Result<PreparedVoiceReply> {
    if scope.conversation.id.trim().is_empty() || scope.turn_id.trim().is_empty() {
        anyhow::bail!("voice synthesis requires conversation and turn identity");
    }
    let synthesized = synthesizer
        .synthesize_scoped(
            scope.clone(),
            SpeechRequest {
                text,
                voice,
                format: None,
                thread_id: scope.conversation.thread_id.clone(),
            },
        )
        .await?;
    let from = match synthesized.mime_type.as_str() {
        "audio/wav" | "audio/wave" => "wav_pcm16",
        "audio/amr" => "amr_nb",
        "audio/ogg" => "ogg_opus",
        "audio/mp4" => "m4a_aac",
        "audio/mpeg" => "mp3",
        _ => anyhow::bail!("synthesizer returned an unsupported audio format"),
    };
    let target = ["ogg_opus", "m4a_aac", "mp3", "wav_pcm16", "amr_nb"]
        .into_iter()
        .find(|codec| {
            constraints
                .accepted_codecs
                .iter()
                .any(|accepted| accepted == codec)
        })
        .ok_or_else(|| anyhow::anyhow!("provider advertises no supported voice codec"))?;
    let encoded = codec
        .transcode(VoiceCodecRequest {
            audio_base64: synthesized.audio_base64,
            from: from.into(),
            target: target.into(),
        })
        .await?;
    let size_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &encoded.audio_base64,
    )?
    .len() as u64;
    if constraints
        .max_size_bytes
        .is_some_and(|limit| size_bytes > limit)
    {
        anyhow::bail!("synthesized voice exceeds provider size limit");
    }
    let duration_ms = encoded.duration_ms.or(synthesized.duration_ms);
    if constraints
        .max_duration_ms
        .zip(duration_ms)
        .is_some_and(|(limit, duration)| duration > limit)
    {
        anyhow::bail!("synthesized voice exceeds provider duration limit");
    }
    let (mime_type, file_name) = match target {
        "ogg_opus" => ("audio/ogg", "voice.ogg"),
        "m4a_aac" => ("audio/mp4", "voice.m4a"),
        "mp3" => ("audio/mpeg", "voice.mp3"),
        "wav_pcm16" => ("audio/wav", "voice.wav"),
        "amr_nb" => ("audio/amr", "voice.amr"),
        _ => unreachable!(),
    };
    let stored = store
        .store_voice(VoiceMediaRequest {
            audio_base64: encoded.audio_base64,
            mime_type: mime_type.into(),
            codec: Some(target.into()),
            duration_ms,
            file_name: Some(file_name.into()),
            direction: tinychannels_bus::HostVoiceMediaDirection::Outbound,
            scope,
        })
        .await?;
    let mut media = stored.media;
    if media
        .opaque_id
        .as_deref()
        .is_none_or(|id| id.trim().is_empty())
    {
        anyhow::bail!("host voice storage returned no durable opaque reference");
    }
    media.url = None;
    media.path = None;
    media.local_cache_path = None;
    media.kind = tinychannels_bus::MediaKind::Audio;
    media.content_type = Some(mime_type.into());
    media.codec = Some(target.into());
    media.duration_ms = duration_ms;
    media.size_bytes = Some(size_bytes);
    let note = VoiceNote {
        media,
        codec: Some(target.into()),
        duration_ms,
        waveform: encoded.waveform,
        push_to_talk: false,
        transcript: None,
    };
    Ok(PreparedVoiceReply {
        note,
        accessible_url: stored.accessible_url,
    })
}

fn unavailable_transcript() -> VoiceTranscript {
    VoiceTranscript {
        text: String::new(),
        language: None,
        state: VoiceTranscriptState::Unavailable,
        source: VoiceTranscriptSource::HostStt,
    }
}

#[cfg(test)]
#[path = "voice_tests.rs"]
mod tests;
