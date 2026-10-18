//! Capability facts for a configured Slack account.

use super::types::{SLACK_MAX_FILE_BYTES, SLACK_MAX_VOICE_BYTES, SlackChannel};
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ButtonPresentationCapabilities,
    ChannelCapabilities, ContentCapabilities, ContentCapability, EventCapabilities,
    VoiceCapabilities, VoiceTranscriptSource,
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

fn media(types: &[&str]) -> ContentCapability {
    let owned = types
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect::<Vec<_>>();
    ContentCapability {
        inbound: true,
        outbound: true,
        accepted_types: owned.clone(),
        emitted_types: owned,
        max_size_bytes: Some(SLACK_MAX_FILE_BYTES),
        ..Default::default()
    }
}

impl SlackChannel {
    pub(crate) fn declared(&self) -> ChannelCapabilities {
        let media_enabled = self.media_repository.is_some();
        let voice_enabled = self.voice_media_store.is_some();
        let mut parts: Vec<String> = ["text", "buttons", "presentation"].map(String::from).into();
        let actions: Vec<String> = [
            "send",
            "typing",
            "draft-update",
            "edit",
            "delete",
            "reply",
            "thread-reply",
            "react",
            "reactions",
        ]
        .map(String::from)
        .into();
        if media_enabled {
            parts.extend(["image", "video", "audio", "file"].map(String::from));
        }
        let voice = |synthesis: bool| {
            if voice_enabled {
                VoiceCapabilities {
                    supported: true,
                    accepted_codecs: ["m4a_aac", "mp3", "ogg_opus"].map(String::from).into(),
                    max_size_bytes: Some(SLACK_MAX_VOICE_BYTES),
                    transcript_sources: vec![
                        VoiceTranscriptSource::Provider,
                        VoiceTranscriptSource::HostStt,
                    ],
                    synthesis,
                    ..Default::default()
                }
            } else {
                VoiceCapabilities::default()
            }
        };
        if voice_enabled {
            parts.push("voice_note".into());
        }
        ChannelCapabilities {
            remote_control: true,
            chat_approvals: true,
            history_key_ignores_thread: false,
            reactions: true,
            progressive_edits: true,
            streaming_reply_pipeline: true,
            addressing: AddressingCapabilities {
                reply_to_id: true,
                thread_id: true,
                ..Default::default()
            },
            content: ContentCapabilities {
                parts,
                text: ContentCapability {
                    inbound: true,
                    outbound: true,
                    ..Default::default()
                },
                image: if media_enabled {
                    media(&IMAGE_TYPES)
                } else {
                    Default::default()
                },
                video: if media_enabled {
                    media(&VIDEO_TYPES)
                } else {
                    Default::default()
                },
                audio: if media_enabled {
                    media(&AUDIO_TYPES)
                } else {
                    Default::default()
                },
                file: if media_enabled {
                    media(&FILE_TYPES)
                } else {
                    Default::default()
                },
                buttons: ContentCapability {
                    outbound: true,
                    ..Default::default()
                },
                button_presentation: ButtonPresentationCapabilities {
                    max_actions_per_row: Some(5),
                    max_rows: Some(24),
                    max_label_length: Some(75),
                    max_value_bytes: Some(255),
                    ..Default::default()
                },
                presentation: ContentCapability {
                    outbound: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            actions: ActionCapabilities {
                supported: actions,
                ..Default::default()
            },
            voice_in: voice(false),
            voice_out: voice(self.voice_synthesis),
            events: EventCapabilities {
                supported: [
                    "button_callback",
                    "reaction_added",
                    "reaction_removed",
                    "outbound_reaction",
                    "message_updated",
                    "message_deleted",
                ]
                .map(String::from)
                .into(),
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
