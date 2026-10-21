//! Capability facts derived from Discord account configuration and host ports.

use super::channel::DiscordChannel;
use super::outbound::MAX_UPLOAD_BYTES;
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ButtonPresentationCapabilities,
    ChannelCapabilities, ContentCapabilities, ContentCapability, EventCapabilities,
    TextCapabilities, VoiceCapabilities, VoiceTranscriptSource,
};

const INLINE_VOICE_BYTES: u64 = 8 * 1024 * 1024;

pub(super) fn declared(channel: &DiscordChannel) -> ChannelCapabilities {
    let media = channel.media_repository.is_some();
    let voice = channel.voice_media_store.is_some();
    let media_capability = |accepted: &[&str], emitted: &[&str]| {
        if media {
            ContentCapability {
                inbound: true,
                outbound: true,
                accepted_types: accepted.iter().map(|t| (*t).to_owned()).collect(),
                emitted_types: emitted.iter().map(|t| (*t).to_owned()).collect(),
                max_size_bytes: Some(MAX_UPLOAD_BYTES),
                max_count: Some(10),
                ..Default::default()
            }
        } else {
            ContentCapability::default()
        }
    };
    let mut parts: Vec<String> = ["text", "buttons", "presentation", "native"]
        .iter()
        .map(|p| (*p).to_owned())
        .collect();
    if media {
        parts.extend(["image", "video", "audio", "file"].map(str::to_owned));
    }
    let mut actions: Vec<String> = [
        "send",
        "typing",
        "delete",
        "edit",
        "draft-update",
        "reply",
        "thread-reply",
        "thread-create",
        "thread-update",
        "thread-list",
        "react",
        "reactions",
    ]
    .iter()
    .map(|a| (*a).to_owned())
    .collect();
    if media {
        actions.push("sendAttachment".into());
    }
    let (voice_in, voice_out) = if voice {
        parts.push("voice_note".into());
        (
            VoiceCapabilities {
                supported: true,
                accepted_codecs: vec!["ogg_opus".into()],
                max_size_bytes: Some(INLINE_VOICE_BYTES),
                native_waveform: true,
                transcript_sources: vec![VoiceTranscriptSource::HostStt],
                ..Default::default()
            },
            VoiceCapabilities {
                supported: true,
                accepted_codecs: vec!["ogg_opus".into()],
                max_size_bytes: Some(INLINE_VOICE_BYTES),
                native_waveform: true,
                normalized_waveform: channel.voice_codec.is_some(),
                synthesis: channel.voice_synthesis,
                ..Default::default()
            },
        )
    } else {
        (VoiceCapabilities::default(), VoiceCapabilities::default())
    };
    ChannelCapabilities {
        remote_control: true,
        chat_approvals: true,
        progressive_edits: true,
        streaming_reply_pipeline: true,
        reactions: true,
        addressing: AddressingCapabilities {
            reply_to_id: true,
            thread_id: true,
            parent_id: true,
            scope_id: true,
            conversation_types: vec!["dm".into(), "channel".into(), "thread".into()],
            ..Default::default()
        },
        content: ContentCapabilities {
            parts,
            text: ContentCapability {
                inbound: true,
                outbound: true,
                ..Default::default()
            },
            image: media_capability(
                &["image/jpeg", "image/png", "image/gif", "image/webp"],
                &["image/jpeg", "image/png", "image/gif", "image/webp"],
            ),
            video: media_capability(
                &["video/mp4", "video/quicktime", "video/webm"],
                &["video/mp4", "video/quicktime", "video/webm"],
            ),
            audio: media_capability(
                &[
                    "audio/ogg",
                    "audio/mpeg",
                    "audio/mp4",
                    "audio/wav",
                    "audio/webm",
                ],
                &[
                    "audio/ogg",
                    "audio/mpeg",
                    "audio/mp4",
                    "audio/wav",
                    "audio/webm",
                ],
            ),
            file: media_capability(
                &[
                    "application/pdf",
                    "text/plain",
                    "text/csv",
                    "application/json",
                    "application/zip",
                ],
                &[
                    "application/pdf",
                    "text/plain",
                    "text/csv",
                    "application/json",
                    "application/zip",
                ],
            ),
            buttons: ContentCapability {
                outbound: true,
                ..Default::default()
            },
            button_presentation: ButtonPresentationCapabilities {
                max_actions_per_row: Some(5),
                max_rows: Some(5),
                max_label_length: Some(80),
                max_value_bytes: Some(100),
                supports_styles: true,
                supports_disabled: true,
                ..Default::default()
            },
            presentation: ContentCapability {
                outbound: true,
                ..Default::default()
            },
            native: ContentCapability {
                outbound: true,
                ..Default::default()
            },
            ..Default::default()
        },
        text: TextCapabilities {
            text_limit: Some(2000),
            length_unit: tinychannels_bus::LengthUnit::Utf16Units,
            markdown_dialects: vec![tinychannels_bus::MarkdownDialect::DiscordMarkdown],
            chunk_limit: Some(2000),
            ..Default::default()
        },
        actions: ActionCapabilities {
            supported: actions,
            ..Default::default()
        },
        voice_in,
        voice_out,
        events: EventCapabilities {
            supported: [
                "button_callback",
                "reaction_added",
                "reaction_removed",
                "outbound_reaction",
                "message_updated",
                "message_deleted",
            ]
            .iter()
            .map(|e| (*e).to_owned())
            .collect(),
        },
        ..Default::default()
    }
}
