//! Capability helpers shared by the native v2 adapters.

use tinychannels_bus::{
    ContentCapabilities, ContentCapability, VoiceCapabilities, VoiceTranscriptSource,
};

const IMAGE_TYPES: [&str; 4] = ["image/jpeg", "image/png", "image/gif", "image/webp"];
const VIDEO_TYPES: [&str; 3] = ["video/mp4", "video/quicktime", "video/webm"];
const AUDIO_TYPES: [&str; 5] = [
    "audio/ogg",
    "audio/mpeg",
    "audio/mp4",
    "audio/wav",
    "audio/webm",
];
const FILE_TYPES: [&str; 7] = [
    "application/pdf",
    "text/plain",
    "text/csv",
    "application/json",
    "application/zip",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
];

fn media(types: &[&str], max_bytes: u64) -> ContentCapability {
    let owned = types
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect::<Vec<_>>();
    ContentCapability {
        inbound: true,
        outbound: true,
        accepted_types: owned.clone(),
        emitted_types: owned,
        max_size_bytes: Some(max_bytes),
        ..Default::default()
    }
}

/// Declare image, video, audio and file content, both directions.
pub(crate) fn media_capabilities(content: &mut ContentCapabilities, max_bytes: u64) {
    content.image = media(&IMAGE_TYPES, max_bytes);
    content.video = media(&VIDEO_TYPES, max_bytes);
    content.audio = media(&AUDIO_TYPES, max_bytes);
    content.file = media(&FILE_TYPES, max_bytes);
}

/// Voice facts for one direction. Transcripts come from the host's speech to
/// text, either through the adapter's own transcriber or the shared ingress.
pub(crate) fn voice_capabilities(
    enabled: bool,
    synthesis: bool,
    max_bytes: u64,
    codecs: &[&str],
) -> VoiceCapabilities {
    if !enabled {
        return VoiceCapabilities::default();
    }
    VoiceCapabilities {
        supported: true,
        accepted_codecs: codecs.iter().map(|codec| (*codec).to_owned()).collect(),
        max_size_bytes: Some(max_bytes),
        transcript_sources: vec![VoiceTranscriptSource::HostStt],
        synthesis,
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
