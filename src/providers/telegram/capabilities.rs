//! Configured Telegram capability facts.

use super::channel_types::TelegramChannel;

pub(super) fn declared(channel: &TelegramChannel) -> tinychannels_bus::ChannelCapabilities {
    let progressive_edits = channel.supports_progressive_edits();
    let streaming_reply_pipeline = channel.supports_draft_updates();
    let reply_to_id = channel.supports_reply_to_id();
    let thread_id = channel.supports_thread_id();
    let media_enabled = channel.media_repository.is_some();
    let mut actions = vec!["send".into(), "typing".into(), "delete".into()];
    if progressive_edits {
        actions.push("draft-update".into());
    }
    if channel.supports_edit() {
        actions.push("edit".into());
    }
    if reply_to_id || thread_id {
        actions.push("reply".into());
    }
    if thread_id {
        actions.push("thread-reply".into());
    }
    // The module controller also exposes Bot API forum-topic lifecycle
    // operations for configured chats. Telegram cannot list topics.
    actions.extend(["thread-create".into(), "thread-update".into()]);
    actions.extend(["react".into(), "reactions".into()]);
    let mut parts = vec!["text".into()];
    parts.extend([
        "location".into(),
        "contact".into(),
        "buttons".into(),
        "poll".into(),
        "presentation".into(),
    ]);
    let media_capability = |accepted: &[&str], emitted: &[&str], max_size_bytes: u64| {
        if media_enabled {
            tinychannels_bus::ContentCapability {
                inbound: true,
                outbound: true,
                accepted_types: accepted.iter().map(|kind| (*kind).to_owned()).collect(),
                emitted_types: emitted.iter().map(|kind| (*kind).to_owned()).collect(),
                max_size_bytes: Some(max_size_bytes),
                ..Default::default()
            }
        } else {
            tinychannels_bus::ContentCapability::default()
        }
    };
    if media_enabled {
        parts.extend([
            "image".into(),
            "video".into(),
            "audio".into(),
            "file".into(),
            "sticker".into(),
        ]);
    }
    let (voice_in, voice_out) = if channel.voice_media_store.is_some() {
        parts.push("voice_note".into());
        (
            tinychannels_bus::VoiceCapabilities {
                supported: true,
                max_size_bytes: Some(8 * 1024 * 1024),
                transcript_sources: vec![
                    tinychannels_bus::VoiceTranscriptSource::Provider,
                    tinychannels_bus::VoiceTranscriptSource::HostStt,
                ],
                ..Default::default()
            },
            tinychannels_bus::VoiceCapabilities {
                supported: true,
                accepted_codecs: vec!["ogg_opus".into(), "m4a_aac".into(), "mp3".into()],
                max_size_bytes: Some(8 * 1024 * 1024),
                synthesis: channel.voice_synthesis,
                ..Default::default()
            },
        )
    } else {
        (
            tinychannels_bus::VoiceCapabilities::default(),
            tinychannels_bus::VoiceCapabilities::default(),
        )
    };
    tinychannels_bus::ChannelCapabilities {
        remote_control: true,
        chat_approvals: true,
        history_key_ignores_thread: true,
        reactions: true,
        progressive_edits,
        streaming_reply_pipeline,
        addressing: tinychannels_bus::AddressingCapabilities {
            reply_to_id,
            thread_id,
            ..Default::default()
        },
        content: tinychannels_bus::ContentCapabilities {
            parts,
            text: tinychannels_bus::ContentCapability {
                inbound: true,
                outbound: true,
                ..Default::default()
            },
            image: media_capability(
                &["image/jpeg", "image/png", "image/gif", "image/webp"],
                &["image/jpeg", "image/png"],
                10 * 1024 * 1024,
            ),
            video: media_capability(
                &["video/mp4", "video/quicktime", "video/webm"],
                &["video/mp4"],
                20 * 1024 * 1024,
            ),
            audio: media_capability(
                &[
                    "audio/ogg",
                    "audio/mpeg",
                    "audio/mp4",
                    "audio/wav",
                    "audio/webm",
                ],
                &["audio/mpeg", "audio/mp4"],
                20 * 1024 * 1024,
            ),
            file: media_capability(
                &[
                    "application/pdf",
                    "text/plain",
                    "text/csv",
                    "application/json",
                    "application/zip",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ],
                &[
                    "application/pdf",
                    "text/plain",
                    "text/csv",
                    "application/json",
                    "application/zip",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ],
                20 * 1024 * 1024,
            ),
            sticker: if media_enabled {
                tinychannels_bus::ContentCapability {
                    inbound: true,
                    outbound: true,
                    accepted_types: vec!["image/webp".into()],
                    emitted_types: vec!["image/webp".into()],
                    max_size_bytes: Some(20 * 1024 * 1024),
                    ..Default::default()
                }
            } else {
                tinychannels_bus::ContentCapability::default()
            },
            location: tinychannels_bus::ContentCapability {
                inbound: true,
                outbound: true,
                ..Default::default()
            },
            contact: tinychannels_bus::ContentCapability {
                inbound: true,
                outbound: true,
                ..Default::default()
            },
            buttons: tinychannels_bus::ContentCapability {
                outbound: true,
                ..Default::default()
            },
            button_presentation: tinychannels_bus::ButtonPresentationCapabilities {
                max_actions_per_row: Some(8),
                max_rows: Some(100),
                max_label_length: Some(64),
                max_value_bytes: Some(64),
                ..Default::default()
            },
            poll: tinychannels_bus::ContentCapability {
                outbound: true,
                ..Default::default()
            },
            presentation: tinychannels_bus::ContentCapability {
                outbound: true,
                ..Default::default()
            },
            ..Default::default()
        },
        actions: tinychannels_bus::ActionCapabilities {
            supported: actions,
            ..Default::default()
        },
        voice_in,
        voice_out,
        events: tinychannels_bus::EventCapabilities {
            supported: vec![
                "button_callback".into(),
                "reaction_added".into(),
                "reaction_removed".into(),
                "outbound_reaction".into(),
                "message_updated".into(),
            ],
        },
        ..Default::default()
    }
}
