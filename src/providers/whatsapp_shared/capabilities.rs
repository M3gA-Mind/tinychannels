//! Capability facts for one configured WhatsApp account.
//!
//! The facts are derived from what the exact adapter and its ports can do:
//! a media kind is advertised only when scoped host storage is present, voice
//! only when the voice store is, and edit, delete and read receipts only for
//! the adapter whose protocol carries them. Nothing here is a hope.

use super::plan::{MAX_VOICE_BYTES, MediaLimits};
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ChannelCapabilities, ContentCapabilities,
    ContentCapability, EventCapabilities, TextCapabilities, VoiceCapabilities,
    VoiceTranscriptSource,
};

const DOCUMENT_TYPES: &[&str] = &[
    "application/pdf",
    "text/plain",
    "text/csv",
    "application/json",
    "application/zip",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
];

/// What one adapter instance offers.
pub(crate) struct CapabilitySpec {
    /// Scoped host media storage is wired.
    pub media: bool,
    /// The host voice store is wired.
    pub voice_store: bool,
    /// Shared final-turn synthesis is available to this account.
    pub voice_synthesis: bool,
    /// Outbound voice notes carry WhatsApp's waveform preview.
    pub native_waveform: bool,
    /// The protocol edits and unsends sent messages.
    pub edit_delete: bool,
    pub limits: MediaLimits,
    /// Longest text one WhatsApp message carries.
    pub text_limit: u64,
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn media_capability(
    enabled: bool,
    accepted: &[&str],
    emitted: &[&str],
    max_size_bytes: u64,
) -> ContentCapability {
    if !enabled {
        return ContentCapability::default();
    }
    ContentCapability {
        inbound: true,
        outbound: true,
        accepted_types: strings(accepted),
        emitted_types: strings(emitted),
        max_size_bytes: Some(max_size_bytes),
        ..Default::default()
    }
}

/// Build the capability facts for `spec`.
pub(crate) fn capabilities(spec: &CapabilitySpec) -> ChannelCapabilities {
    let mut parts = strings(&["text", "location", "contact"]);
    if spec.media {
        parts.extend(strings(&["image", "video", "audio", "file", "sticker"]));
    }
    if spec.voice_store {
        parts.push("voice_note".into());
    }
    let mut actions = strings(&["send", "typing", "mark-read", "react", "reactions", "reply"]);
    if spec.edit_delete {
        actions.extend(strings(&["edit", "delete", "unsend"]));
    }
    let events = strings(&[
        "reaction_added",
        "reaction_removed",
        "read_receipt",
        "delivered_receipt",
    ]);
    let both_ways = ContentCapability {
        inbound: true,
        outbound: true,
        ..Default::default()
    };
    let limits = &spec.limits;
    ChannelCapabilities {
        remote_control: true,
        chat_approvals: true,
        history_key_ignores_thread: true,
        reactions: true,
        addressing: AddressingCapabilities {
            reply_to_id: true,
            ..Default::default()
        },
        content: ContentCapabilities {
            parts,
            text: both_ways.clone(),
            image: media_capability(
                spec.media,
                &["image/jpeg", "image/png", "image/gif", "image/webp"],
                &["image/jpeg", "image/png"],
                limits.image,
            ),
            video: media_capability(
                spec.media,
                &["video/mp4", "video/quicktime", "video/webm"],
                &["video/mp4"],
                limits.video,
            ),
            audio: media_capability(
                spec.media,
                &[
                    "audio/ogg",
                    "audio/mpeg",
                    "audio/mp4",
                    "audio/wav",
                    "audio/webm",
                ],
                &["audio/mpeg", "audio/mp4", "audio/ogg"],
                limits.audio,
            ),
            file: media_capability(spec.media, DOCUMENT_TYPES, DOCUMENT_TYPES, limits.document),
            sticker: media_capability(spec.media, &["image/webp"], &["image/webp"], limits.sticker),
            location: both_ways.clone(),
            contact: both_ways,
            ..Default::default()
        },
        voice_in: if spec.voice_store {
            VoiceCapabilities {
                supported: true,
                max_size_bytes: Some(super::inbound::MAX_INBOUND_VOICE_BYTES),
                transcript_sources: vec![VoiceTranscriptSource::HostStt],
                ..Default::default()
            }
        } else {
            VoiceCapabilities::default()
        },
        voice_out: if spec.voice_store {
            VoiceCapabilities {
                supported: true,
                accepted_codecs: strings(&["ogg_opus"]),
                max_size_bytes: Some(MAX_VOICE_BYTES),
                native_waveform: spec.native_waveform,
                synthesis: spec.voice_synthesis,
                ..Default::default()
            }
        } else {
            VoiceCapabilities::default()
        },
        events: EventCapabilities { supported: events },
        actions: ActionCapabilities {
            supported: actions,
            ..Default::default()
        },
        text: TextCapabilities {
            text_limit: Some(spec.text_limit),
            chunk_limit: Some(spec.text_limit),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
