//! Shared inbound voice enrichment and scoped reply-mode decisions.

use super::{Transcriber, TranscriptionRequest};
use std::sync::Arc;
use tinychannels_bus::{
    HostVoiceScope, VoiceNote, VoiceTranscript, VoiceTranscriptSource, VoiceTranscriptState,
};

/// Product voice mode supplied by the host for the active scoped turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoiceReplyMode {
    /// Reply with speech only when the inbound turn included a voice note.
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
    if note.transcript.as_ref().is_some_and(|transcript| {
        transcript.source == VoiceTranscriptSource::Provider
            && transcript.state == VoiceTranscriptState::Complete
            && !transcript.text.trim().is_empty()
    }) {
        return VoiceEnrichmentStatus::ProviderTranscript;
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
