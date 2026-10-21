//! Provider-neutral typed outbound sends: plan the parts, resolve scoped media
//! up front (so a bad attachment fails before any text is sent), deliver step by
//! step through the provider, and assemble the receipt.

#[cfg(test)]
use super::NativeFailure;
use super::{NativeProvider, pre_dispatch, unsupported};
use crate::providers::slack::text::{chunk_text, strip_tool_call_tags};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tinychannels_bus::channel::{
    ChannelSendError, ChannelSendOutcome, MessageReceipt, MessageReceiptPart,
    MessageReceiptPartKind, MessageReceiptSourceResult, SendErrorKind,
};
use tinychannels_bus::{
    ChannelOutboundMessage, ChannelRef, ContentPart, ConversationRef, HostVoiceScope, MediaKind,
    OutboundContentPart, OutboundVoice,
};

/// Where a message goes.
pub(crate) struct Target {
    pub(crate) channel: ChannelRef,
    pub(crate) conversation: ConversationRef,
    pub(crate) reply_to: Option<String>,
    pub(crate) thread: Option<String>,
}

/// Size limits a provider enforces on what it sends.
#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) text_chars: usize,
    pub(crate) file_bytes: u64,
    pub(crate) voice_bytes: u64,
}

/// One file ready to upload.
#[derive(Debug)]
pub(crate) struct FileStep {
    /// Semantic bus kind, retained so providers can distinguish an Audio part
    /// from a generic file whose MIME type happens to be audio/*.
    #[cfg(feature = "lark")]
    pub(crate) kind: MediaKind,
    pub(crate) bytes: Vec<u8>,
    pub(crate) mime: String,
    pub(crate) name: String,
    pub(crate) caption: Option<String>,
    /// Measured duration carried by a stored voice note, when present.
    pub(crate) duration_ms: Option<u64>,
}

/// One validated unit of work, in message order.
#[derive(Debug)]
pub(crate) enum Step {
    Text(String),
    File(FileStep),
    Voice(FileStep),
}

/// A message the provider accepted.
pub(crate) struct Sent {
    pub(crate) id: String,
    pub(crate) kind: MessageReceiptPartKind,
    pub(crate) editable: bool,
    pub(crate) deletable: bool,
}

impl Sent {
    pub(crate) fn new(id: impl Into<String>, kind: MessageReceiptPartKind) -> Self {
        Self {
            id: id.into(),
            kind,
            editable: false,
            deletable: false,
        }
    }

    pub(crate) fn editable(mut self, editable: bool) -> Self {
        self.editable = editable;
        self
    }

    pub(crate) fn deletable(mut self, deletable: bool) -> Self {
        self.deletable = deletable;
        self
    }
}

pub(crate) fn target_of(
    message: &ChannelOutboundMessage,
    provider: &str,
    display: &str,
) -> Result<Target, ChannelSendError> {
    let mut conversation = message
        .conversation_ref
        .clone()
        .unwrap_or_else(|| ConversationRef {
            id: message.conversation_id.clone(),
            ..Default::default()
        });
    if conversation.thread_id.is_none() {
        conversation.thread_id = message.thread_id.clone();
    }
    if conversation.id.trim().is_empty() {
        return Err(pre_dispatch(&format!(
            "{display} send requires a conversation identity"
        )));
    }
    let thread = conversation
        .thread_id
        .clone()
        .or_else(|| conversation.topic_id.clone());
    let channel = message.channel_ref.clone().unwrap_or(ChannelRef {
        id: provider.into(),
        account_id: None,
    });
    Ok(Target {
        channel,
        conversation,
        reply_to: message.reply_to_id.clone(),
        thread,
    })
}

fn voice_scope(
    message: &ChannelOutboundMessage,
    target: &Target,
    display: &str,
) -> Result<HostVoiceScope, ChannelSendError> {
    let turn_id = message
        .turn_id
        .clone()
        .filter(|turn_id| !turn_id.trim().is_empty())
        .ok_or_else(|| {
            pre_dispatch(&format!(
                "{display} uploads require the originating turn identity"
            ))
        })?;
    Ok(HostVoiceScope {
        configured_instance: target
            .channel
            .account_id
            .clone()
            .unwrap_or_else(|| target.channel.id.clone()),
        channel: target.channel.clone(),
        conversation: target.conversation.clone(),
        turn_id,
    })
}

fn text_of(part: &OutboundContentPart) -> Option<String> {
    match part {
        OutboundContentPart::Content {
            part: ContentPart::Text { text, .. },
        } => Some(strip_tool_call_tags(text)),
        _ => None,
    }
}

/// Turn the message into resolved steps, or say why the provider cannot send it.
pub(crate) async fn plan<P: NativeProvider + ?Sized>(
    provider: &P,
    message: &ChannelOutboundMessage,
    target: &Target,
) -> Result<Vec<Step>, ChannelSendError> {
    let display = provider.display();
    if message.parts.is_empty() {
        return Err(pre_dispatch(&format!(
            "{display} typed sends require at least one part"
        )));
    }
    let limits = provider.limits();
    let voice_count = message
        .parts
        .iter()
        .filter(|part| matches!(part, OutboundContentPart::Voice { .. }))
        .count();
    let mut steps = Vec::new();
    for part in &message.parts {
        match part {
            OutboundContentPart::Content {
                part: ContentPart::Text { .. },
            } => {
                // Providers without voice captions receive transcript text as
                // its own ordered step before or after the voice upload.
                if (voice_count == 0 || !provider.voice_caption_supported())
                    && let Some(text) = text_of(part).filter(|text| !text.trim().is_empty())
                {
                    for chunk in chunk_text(&text, limits.text_chars) {
                        steps.push(Step::Text(chunk));
                    }
                }
            }
            OutboundContentPart::Content { part: content } => {
                let (kind, media, name, caption) = match content {
                    ContentPart::Image { media, caption, .. } => {
                        (MediaKind::Image, media, "image", caption.clone())
                    }
                    ContentPart::Video { media, caption, .. } => {
                        (MediaKind::Video, media, "video", caption.clone())
                    }
                    ContentPart::Audio { media, title, .. } => {
                        (MediaKind::Audio, media, "audio", title.clone())
                    }
                    ContentPart::File {
                        media,
                        file_name,
                        caption,
                    } => (
                        MediaKind::Document,
                        media,
                        file_name.as_deref().unwrap_or("file"),
                        caption.clone(),
                    ),
                    _ => {
                        return Err(unsupported(&format!(
                            "{display} does not support this content kind"
                        )));
                    }
                };
                let capabilities = provider.declared_capabilities();
                let declared = match kind {
                    MediaKind::Image => capabilities.content.image.outbound,
                    MediaKind::Video => capabilities.content.video.outbound,
                    MediaKind::Audio => capabilities.content.audio.outbound,
                    MediaKind::Document => capabilities.content.file.outbound,
                    MediaKind::Unknown => false,
                };
                if !declared {
                    return Err(unsupported(&format!(
                        "{display} does not support outbound {name} media"
                    )));
                }
                let repository = provider.media_repository().ok_or_else(|| {
                    unsupported(&format!("{display} media needs scoped host storage"))
                })?;
                let opaque = media
                    .opaque_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| {
                        pre_dispatch(&format!(
                            "{display} media has no host-owned opaque reference"
                        ))
                    })?;
                let scope = voice_scope(message, target, display)?;
                let stored = repository
                    .read_media(scope, opaque.to_owned(), limits.file_bytes)
                    .await
                    .map_err(|_| {
                        pre_dispatch(&format!("{display} scoped media could not be read"))
                    })?;
                if stored.media.kind != kind
                    || !crate::host::is_allowed_content_type(kind, &stored.content_type)
                    || stored.bytes.is_empty()
                    || stored.bytes.len() as u64 > limits.file_bytes
                {
                    return Err(pre_dispatch(&format!(
                        "{display} media failed upload validation"
                    )));
                }
                steps.push(Step::File(FileStep {
                    #[cfg(feature = "lark")]
                    kind,
                    mime: stored.content_type,
                    name: stored.file_name.unwrap_or_else(|| name.to_owned()),
                    bytes: stored.bytes,
                    caption: caption.map(|caption| strip_tool_call_tags(&caption)),
                    duration_ms: None,
                }));
            }
            OutboundContentPart::Voice {
                voice:
                    OutboundVoice::Media {
                        media, duration_ms, ..
                    },
            } => {
                if !provider.declared_capabilities().voice_out.supported {
                    return Err(unsupported(&format!(
                        "{display} does not support outbound voice"
                    )));
                }
                let store = provider.voice_media_store().ok_or_else(|| {
                    unsupported(&format!("{display} voice needs host voice storage"))
                })?;
                if voice_count > 1 {
                    return Err(pre_dispatch(&format!(
                        "{display} sends one voice clip per message"
                    )));
                }
                let opaque = media
                    .opaque_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| {
                        pre_dispatch(&format!(
                            "{display} voice media has no host-owned opaque reference"
                        ))
                    })?;
                let caption = message
                    .parts
                    .iter()
                    .filter(|p| !matches!(p, OutboundContentPart::Voice { .. }))
                    .map(text_of)
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        pre_dispatch(&format!(
                            "{display} voice only combines with transcript text"
                        ))
                    })?
                    .join("\n");
                let scope = voice_scope(message, target, display)?;
                let resolved = store
                    .read_voice(scope, opaque.to_owned())
                    .await
                    .map_err(|_| {
                        pre_dispatch(&format!("{display} voice media could not be read"))
                    })?;
                let audio = BASE64
                    .decode(resolved.audio_base64.as_bytes())
                    .ok()
                    .filter(|audio| !audio.is_empty() && audio.len() as u64 <= limits.voice_bytes)
                    .ok_or_else(|| {
                        pre_dispatch(&format!("{display} voice media failed upload validation"))
                    })?;
                let default_name = if resolved.mime_type == "audio/amr" {
                    "voice.amr"
                } else {
                    "voice.m4a"
                };
                steps.push(Step::Voice(FileStep {
                    #[cfg(feature = "lark")]
                    kind: MediaKind::Audio,
                    mime: resolved.mime_type,
                    name: resolved.file_name.unwrap_or_else(|| default_name.into()),
                    bytes: audio,
                    caption: provider
                        .voice_caption_supported()
                        .then_some(caption)
                        .filter(|c| !c.trim().is_empty()),
                    duration_ms: *duration_ms,
                }));
            }
            OutboundContentPart::Voice { .. } => {
                return Err(if provider.voice_synthesis() {
                    pre_dispatch("voice synthesis must be prepared by the shared module runtime")
                } else {
                    unsupported(&format!("{display} voice synthesis is not configured"))
                });
            }
        }
    }
    if steps.is_empty() {
        return Err(pre_dispatch(&format!(
            "{display} message has no sendable content"
        )));
    }
    Ok(steps)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn receipt_part(provider: &str, sent: Sent, index: usize, target: &Target) -> MessageReceiptPart {
    MessageReceiptPart {
        edit_token: sent.editable.then(|| sent.id.clone()),
        delete_token: sent.deletable.then(|| sent.id.clone()),
        raw: Some(MessageReceiptSourceResult {
            channel: Some(provider.into()),
            message_id: Some(sent.id.clone()),
            channel_id: Some(target.conversation.id.clone()),
            conversation_id: Some(target.conversation.id.clone()),
            ..Default::default()
        }),
        platform_message_id: sent.id,
        kind: sent.kind,
        index,
        thread_id: target.thread.clone(),
        reply_to_id: target.reply_to.clone(),
    }
}

/// Send canonical content and return the provider's actual message ids.
pub(crate) async fn send_typed<P: NativeProvider + ?Sized>(
    provider: &P,
    message: &ChannelOutboundMessage,
) -> Result<ChannelSendOutcome, ChannelSendError> {
    let id = provider.id();
    let target = provider.normalized_target(message)?;
    let declared = provider.declared_capabilities();
    if target.thread.is_some() && !declared.addressing.thread_id {
        return Err(unsupported(&format!(
            "{} does not support threads",
            provider.display()
        )));
    }
    if target.reply_to.is_some() && !declared.addressing.reply_to_id {
        return Err(unsupported(&format!(
            "{} does not support reply addressing",
            provider.display()
        )));
    }
    provider.validate_target(&target)?;
    let mut steps = plan(provider, message, &target).await?;
    provider.prepare_steps(&target, &mut steps).await?;
    let mut parts: Vec<MessageReceiptPart> = Vec::new();
    let mut failure = None;
    if provider.bundles_steps() {
        for item in provider.deliver_bundle(&target, &steps).await? {
            let index = parts.len();
            parts.push(receipt_part(id, item, index, &target));
        }
    } else {
        for step in &steps {
            match provider.deliver(&target, step).await {
                Ok(sent) => {
                    for item in sent {
                        let index = parts.len();
                        parts.push(receipt_part(id, item, index, &target));
                    }
                }
                Err(error) if parts.is_empty() => return Err(error),
                Err(_) => {
                    failure = Some(ChannelSendError {
                        kind: SendErrorKind::Unknown,
                        message: format!(
                            "{} sent only part of the typed message; retry is unsafe",
                            provider.display()
                        ),
                        sent_before_error: true,
                        ..Default::default()
                    });
                    break;
                }
            }
        }
    }

    let ids: Vec<String> = parts
        .iter()
        .map(|p| p.platform_message_id.clone())
        .collect();
    let single = (ids.len() == 1).then(|| parts[0].clone());
    tracing::debug!(
        provider = id,
        parts = ids.len(),
        partial = failure.is_some(),
        "[native] typed send complete"
    );
    let receipt = MessageReceipt {
        idempotency_key: Some(message.idempotency_key.clone()),
        channel: Some(target.channel.clone()),
        conversation: Some(target.conversation.clone()),
        primary_platform_message_id: ids.first().cloned(),
        platform_message_ids: ids,
        parts,
        thread_id: target.thread.clone(),
        reply_to_id: message.reply_to_id.clone(),
        edit_token: single.as_ref().and_then(|p| p.edit_token.clone()),
        delete_token: single.as_ref().and_then(|p| p.delete_token.clone()),
        sent_at: now_ms(),
        raw: None,
    };
    Ok(ChannelSendOutcome {
        receipt,
        error: failure,
    })
}

/// Carry a typed failure through `anyhow`; callers recover it by downcast.
#[cfg(test)]
pub(crate) fn to_anyhow(error: ChannelSendError) -> anyhow::Error {
    anyhow::Error::new(NativeFailure(error))
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;
