//! Normalization of a web `chat:start` request into the v2 inbound envelope.

use crate::channel::ChannelSendError;
use serde_json::json;
use tinychannels_bus::channel::VoiceTranscriptState;
use tinychannels_bus::channel::{
    AccessContext, ChannelInboundEnvelope, ChannelRef, ContentPart, ConversationKind,
    ConversationRef, MediaReference, SendErrorKind, SenderDmDecision, SenderRef,
};
use tinychannels_bus::web::WebChatStart;

/// Channel id the in-app chat is registered under.
pub const WEB_CHANNEL_ID: &str = "web";

fn bad(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.to_owned(),
        retryable: false,
        ..Default::default()
    }
}

fn media_of(part: &ContentPart) -> Option<&MediaReference> {
    match part {
        ContentPart::Image { media, .. }
        | ContentPart::Video { media, .. }
        | ContentPart::Audio { media, .. }
        | ContentPart::File { media, .. } => Some(media),
        ContentPart::VoiceNote { voice_note } => Some(&voice_note.media),
        _ => None,
    }
}

/// Build the canonical envelope for a submission.
///
/// Fails with `BadFormat` when addressing is missing, when an attachment is not
/// media (the web composer cannot send polls, buttons or locations), or when the
/// submission carries neither text nor attachments. A voice note without typed
/// text contributes its completed transcript as `text`, which keeps the plain-text
/// projection usable by consumers that ignore `parts`.
pub fn normalize_chat_start(
    start: &WebChatStart,
    account_id: Option<&str>,
) -> Result<ChannelInboundEnvelope, ChannelSendError> {
    if start.client_id.trim().is_empty()
        || start.thread_id.trim().is_empty()
        || start.request_id.trim().is_empty()
    {
        return Err(bad(
            "web chat start requires client_id, thread_id and request_id",
        ));
    }
    let mut parts = Vec::with_capacity(start.attachments.len() + 1);
    let mut media = Vec::new();
    if !start.text.trim().is_empty() {
        parts.push(ContentPart::Text {
            text: start.text.clone(),
            markdown_dialect: None,
        });
    }
    let mut text = start.text.clone();
    for attachment in &start.attachments {
        let Some(reference) = media_of(attachment) else {
            return Err(bad("web attachments must be media or voice notes"));
        };
        media.push(reference.clone());
        if text.trim().is_empty()
            && let ContentPart::VoiceNote { voice_note } = attachment
            && let Some(transcript) = &voice_note.transcript
            && transcript.state == VoiceTranscriptState::Complete
        {
            text.clone_from(&transcript.text);
        }
        parts.push(attachment.clone());
    }
    if parts.is_empty() {
        return Err(bad("web chat start has no text or attachments"));
    }
    let authenticated_sender_id = start
        .sender_id
        .as_deref()
        .filter(|id| !id.trim().is_empty());
    let sender_id = authenticated_sender_id
        .map(str::to_owned)
        .unwrap_or_else(|| start.client_id.clone());
    Ok(ChannelInboundEnvelope {
        channel: ChannelRef {
            id: WEB_CHANNEL_ID.to_owned(),
            account_id: account_id.map(str::to_owned),
        },
        message_id: start.request_id.clone(),
        reply_to_id: None,
        timestamp_unix_ms: start.timestamp_unix_ms,
        conversation: ConversationRef {
            kind: ConversationKind::Dm,
            id: start.thread_id.clone(),
            thread_id: Some(start.thread_id.clone()),
            ..Default::default()
        },
        sender: SenderRef {
            id: sender_id,
            name: start.sender_name.clone(),
            ..Default::default()
        },
        text,
        parts,
        access: AccessContext {
            dm_decision: SenderDmDecision::Allow,
            command_authorized: start.command_authorized && authenticated_sender_id.is_some(),
            ..Default::default()
        },
        media,
        raw: Some(json!({
            "clientId": start.client_id,
            "requestId": start.request_id,
            "queueMode": start.queue_mode.to_string(),
            "speakReply": start.speak_reply,
            "source": start.source,
        })),
    })
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
