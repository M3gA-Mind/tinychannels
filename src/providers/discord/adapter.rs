//! Discord on the v2 adapter contract: capability facts and the optional
//! typing, reaction, edit, delete and streaming-draft extensions.

use super::channel::DiscordChannel;
use super::outbound::MAX_UPLOAD_BYTES;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelInboundSink, ChannelReaction,
    ChannelSendError, ChannelSendOutcome, ChannelStreamingDraft, ChannelTypedSend, ChannelTyping,
    MessageReceipt,
};
use async_trait::async_trait;
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ButtonPresentationCapabilities,
    ChannelCapabilities, ChannelOutboundIntent, ChannelOutboundMessage, ContentCapabilities,
    ContentCapability, EventCapabilities, TextCapabilities, VoiceCapabilities,
    VoiceTranscriptSource,
};

const INLINE_VOICE_BYTES: u64 = 8 * 1024 * 1024;

fn wrong_channel(operation: &str) -> ChannelSendError {
    ChannelSendError {
        kind: crate::channel::SendErrorKind::Forbidden,
        message: format!("{operation} channel does not match Discord"),
        retryable: false,
        ..Default::default()
    }
}

impl DiscordChannel {
    /// Capability facts for this configured account. Media and voice are
    /// declared only when the host supplied the storage they need.
    pub(crate) fn v2_capabilities(&self) -> ChannelCapabilities {
        let media = self.media_repository.is_some();
        let voice = self.voice_media_store.is_some();
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
                    normalized_waveform: self.voice_codec.is_some(),
                    synthesis: self.voice_synthesis,
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
}

#[async_trait]
impl ChannelAdapter for DiscordChannel {
    fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        tinychannels_bus::channel::ChannelDescriptor {
            id: "discord".into(),
            display_name: "Discord".into(),
            metadata: std::collections::BTreeMap::from([(
                "adapterMode".into(),
                "native-v2".into(),
            )]),
            ..Default::default()
        }
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.v2_capabilities()
    }

    fn proactive_target(&self) -> Option<String> {
        crate::traits::Channel::proactive_target(self)
    }

    async fn start(
        &self,
        _sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        // The module runtime owns the single gateway session and publishes
        // the same normalized ingress through its event sink.
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        self.send_v2_with_receipt(&ChannelOutboundMessage::from_intent(&intent))
            .await
    }

    async fn status(
        &self,
    ) -> Result<tinychannels_runtime::controllers::ChannelAccountSnapshot, ChannelSendError> {
        let health_state = if self.health_status().await {
            "healthy"
        } else {
            "unhealthy"
        };
        Ok(tinychannels_bus::controllers::ChannelAccountSnapshot {
            channel_id: "discord".into(),
            states: vec![
                tinychannels_bus::controllers::ChannelAccountState::Configured,
                tinychannels_bus::controllers::ChannelAccountState::Enabled,
            ],
            health_state: Some(health_state.into()),
            ..Default::default()
        })
    }
}

#[async_trait]
impl ChannelTypedSend for DiscordChannel {
    async fn send_typed(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        self.send_v2_with_receipt(message).await
    }
}

#[async_trait]
impl ChannelReaction for DiscordChannel {
    async fn send_reaction(
        &self,
        channel: &tinychannels_bus::channel::ChannelRef,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        if tinychannels_bus::provider_id(&channel.id) != "discord" {
            return Err(wrong_channel("reaction"));
        }
        self.send_reaction_v2(conversation, provider_message_id, reaction, remove)
            .await
    }
}

#[async_trait]
impl ChannelEdit for DiscordChannel {
    async fn edit(
        &self,
        receipt: &MessageReceipt,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.edit_message_v2(receipt, &intent).await
    }
}

#[async_trait]
impl ChannelDelete for DiscordChannel {
    async fn delete(&self, receipt: &MessageReceipt) -> Result<(), ChannelSendError> {
        self.delete_message_v2(receipt).await
    }
}

#[async_trait]
impl ChannelTyping for DiscordChannel {
    async fn start_typing(&self, conversation_id: &str) -> Result<(), ChannelSendError> {
        self.send_typing_once(conversation_id).await
    }

    async fn stop_typing(&self, _conversation_id: &str) -> Result<(), ChannelSendError> {
        // Discord's indicator expires on its own and has no stop call.
        Ok(())
    }
}

#[async_trait]
impl ChannelStreamingDraft for DiscordChannel {
    async fn upsert_draft(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        if intent
            .channel_ref
            .as_ref()
            .is_some_and(|c| tinychannels_bus::provider_id(&c.id) != "discord")
        {
            return Err(wrong_channel("draft"));
        }
        self.upsert_draft_v2(previous, intent).await
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
