// Scoped allowance for the one-release v21 wire DTO compatibility surface.
#![allow(deprecated)]

//! Legacy inbound projections.
use crate::channel::types::{ChannelRef, ConversationRef, SenderRef};
use crate::traits::ChannelMessage;
pub use tinychannels_bus::channel::envelope::*;

/// Runtime media projection onto the legacy payload.
pub trait InboundMediaPayloadExt {
    fn from_media(media: &[MediaReference]) -> Self;
}
impl InboundMediaPayloadExt for InboundMediaPayload {
    fn from_media(media: &[MediaReference]) -> Self {
        let transcribed_indexes: Vec<usize> = media
            .iter()
            .enumerate()
            .filter_map(|(index, item)| item.transcribed.then_some(index))
            .collect();
        Self {
            media_path: media.first().and_then(|m| m.path.clone()),
            media_url: media
                .first()
                .and_then(|m| m.url.clone().or_else(|| m.path.clone())),
            media_type: media.first().and_then(media_type),
            media_paths: aligned_strings(media.iter().map(|m| m.path.clone()).collect()),
            media_urls: aligned_strings(
                media
                    .iter()
                    .map(|m| m.url.clone().or_else(|| m.path.clone()))
                    .collect(),
            ),
            media_types: aligned_strings(media.iter().map(media_type).collect()),
            media_transcribed_indexes: (!transcribed_indexes.is_empty())
                .then_some(transcribed_indexes),
        }
    }
}

fn media_type(media: &MediaReference) -> Option<String> {
    media
        .content_type
        .clone()
        .or_else(|| Some(format!("{:?}", media.kind).to_ascii_lowercase()))
}

fn aligned_strings(values: Vec<Option<String>>) -> Option<Vec<String>> {
    values.iter().any(Option::is_some).then(|| {
        values
            .into_iter()
            .map(|value| value.unwrap_or_default())
            .collect()
    })
}

/// Project the legacy host `ChannelMessage` shape into a normalized inbound
/// envelope without inferring provider-specific conversation kind.
pub fn inbound_envelope_from_legacy_message(msg: &ChannelMessage) -> ChannelInboundEnvelope {
    // v21 providers used Unix seconds while a few newer providers already
    // supplied milliseconds. Values below this year-5138 boundary are
    // unambiguously seconds for contemporary channel timestamps.
    const MILLIS_THRESHOLD: u64 = 100_000_000_000;
    let timestamp_unix_ms = Some(if msg.timestamp < MILLIS_THRESHOLD {
        msg.timestamp.saturating_mul(1000)
    } else {
        msg.timestamp
    });
    let thread_ts = msg
        .thread_ts
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    let provider = msg.channel.split(':').next().unwrap_or_default();
    let (thread_id, topic_id) = if matches!(provider, "telegram" | "tg") && msg.topic_id.is_none() {
        // v21 Telegram populated `thread_ts` for its topic. Preserve that
        // historical interpretation only when the v22 topic field is absent.
        (None, thread_ts)
    } else {
        (thread_ts, msg.topic_id.clone())
    };

    ChannelInboundEnvelope {
        channel: ChannelRef {
            id: msg.channel.clone(),
            account_id: None,
        },
        message_id: msg.id.clone(),
        reply_to_id: msg.reply_to_id.clone(),
        timestamp_unix_ms,
        conversation: ConversationRef {
            id: msg.reply_target.clone(),
            thread_id,
            topic_id,
            ..Default::default()
        },
        sender: SenderRef {
            id: msg.sender.clone(),
            name: msg.sender_name.clone(),
            ..Default::default()
        },
        text: msg.content.clone(),
        parts: vec![ContentPart::Text {
            text: msg.content.clone(),
            markdown_dialect: None,
        }],
        ..Default::default()
    }
}

/// Project a normalized inbound envelope back into the legacy host
/// `ChannelMessage` shape used by existing OpenHuman dispatch loops.
pub fn legacy_message_from_inbound_envelope(
    envelope: &ChannelInboundEnvelope,
    timestamp: u64,
) -> anyhow::Result<ChannelMessage> {
    let content = if envelope.parts.is_empty() {
        envelope.text.clone()
    } else {
        let mut text = Vec::with_capacity(envelope.parts.len());
        for part in &envelope.parts {
            match part {
                ContentPart::Text { text: value, .. } => text.push(value.as_str()),
                ContentPart::VoiceNote { voice_note } => {
                    let Some(transcript) = voice_note.transcript.as_ref().filter(|transcript| {
                        transcript.state == VoiceTranscriptState::Complete
                            && !transcript.text.trim().is_empty()
                    }) else {
                        anyhow::bail!(
                            "legacy message cannot represent a voice note without a complete transcript"
                        );
                    };
                    // The prepared result retains the canonical envelope and
                    // its audio reference. This compatibility view carries
                    // the transcript into legacy text dispatch.
                    text.push(transcript.text.as_str());
                }
                _ => anyhow::bail!("legacy message cannot represent non-text inbound content"),
            }
        }
        text.join("\n")
    };
    Ok(ChannelMessage {
        id: envelope.message_id.clone(),
        sender: envelope.sender.id.clone(),
        reply_target: envelope.conversation.id.clone(),
        content,
        channel: envelope.channel.id.clone(),
        timestamp,
        thread_ts: envelope.conversation.thread_id.clone(),
        topic_id: envelope.conversation.topic_id.clone(),
        reply_to_id: envelope.reply_to_id.clone(),
        sender_name: envelope.sender.name.clone(),
    })
}

/// Build a text/routing compatibility view for a host that consumes the
/// preserved typed envelope separately. Rich media remain only in `envelope`;
/// callers that do not consume those parts must use the strict legacy
/// projection above instead.
pub fn typed_message_from_inbound_envelope(
    envelope: &ChannelInboundEnvelope,
    timestamp: u64,
) -> ChannelMessage {
    let content = if envelope.parts.is_empty() {
        envelope.text.clone()
    } else {
        let projected = envelope
            .parts
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text, .. } => Some(text.as_str()),
                ContentPart::VoiceNote { voice_note } => voice_note
                    .transcript
                    .as_ref()
                    .filter(|transcript| {
                        transcript.state == VoiceTranscriptState::Complete
                            && !transcript.text.trim().is_empty()
                    })
                    .map(|transcript| transcript.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if projected.is_empty() {
            envelope.text.clone()
        } else {
            projected.join("\n")
        }
    };
    ChannelMessage {
        id: envelope.message_id.clone(),
        sender: envelope.sender.id.clone(),
        reply_target: envelope.conversation.id.clone(),
        content,
        channel: envelope.channel.id.clone(),
        timestamp,
        thread_ts: envelope.conversation.thread_id.clone(),
        topic_id: envelope.conversation.topic_id.clone(),
        reply_to_id: envelope.reply_to_id.clone(),
        sender_name: envelope.sender.name.clone(),
    }
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
