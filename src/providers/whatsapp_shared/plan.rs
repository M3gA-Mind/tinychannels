//! Turn one typed outbound message into the ordered WhatsApp operations that
//! deliver it.
//!
//! Planning is where every shared rule is enforced, before the first provider
//! call: the message must address this account, thread addressing is refused,
//! stored media is read through the host's scoped ports and checked against
//! the content type and size WhatsApp accepts, and a voice note must be
//! OGG/Opus. A message that fails planning has not touched the network, so
//! the failure is safe to retry.

use super::error::WaFailure;
use super::text::strip_tool_call_tags;
use super::vcard::ContactCard;
use crate::host::{MediaRepository, VoiceMediaStore};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use tinychannels_bus::{
    ChannelOutboundMessage, ChannelRef, ContentPart, ConversationRef, HostVoiceScope, MediaKind,
    MediaReference, OutboundContentPart, OutboundVoice,
};

/// Largest voice note WhatsApp clients play inline.
pub(crate) const MAX_VOICE_BYTES: u64 = 16 * 1024 * 1024;
/// Waveform resolution WhatsApp draws.
pub(crate) const WAVEFORM_BUCKETS: usize = 64;

/// Per-adapter upload ceilings in bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MediaLimits {
    pub image: u64,
    pub video: u64,
    pub audio: u64,
    pub document: u64,
    pub sticker: u64,
    pub caption_chars: usize,
}

/// How a planned attachment is presented in the chat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttachmentKind {
    Image,
    Video,
    Audio,
    Document,
    Sticker,
}

/// One provider operation, in delivery order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlannedPart {
    Text(String),
    Attachment {
        kind: AttachmentKind,
        bytes: Vec<u8>,
        content_type: String,
        file_name: Option<String>,
        caption: Option<String>,
        duration_ms: Option<u64>,
    },
    Location {
        latitude: f64,
        longitude: f64,
        name: Option<String>,
        address: Option<String>,
    },
    Contact(ContactCard),
    Voice {
        bytes: Vec<u8>,
        content_type: String,
        duration_ms: Option<u64>,
        waveform: Option<Vec<u8>>,
    },
}

/// Where a message goes and on whose behalf.
#[derive(Debug, Clone)]
pub(crate) struct OutboundScope {
    pub channel: ChannelRef,
    pub conversation: ConversationRef,
    pub reply_to_id: Option<String>,
    pub turn_id: Option<String>,
}

/// The host ports a plan may read stored media through.
#[derive(Clone, Copy)]
pub(crate) struct PlanPorts<'a> {
    pub media: Option<&'a dyn MediaRepository>,
    pub voices: Option<&'a dyn VoiceMediaStore>,
}

/// Validate the address of `message` for provider `provider`.
pub(crate) fn outbound_scope(
    message: &ChannelOutboundMessage,
    provider: &str,
) -> Result<OutboundScope, WaFailure> {
    let mut conversation = message
        .conversation_ref
        .clone()
        .unwrap_or_else(|| ConversationRef {
            id: message.conversation_id.clone(),
            thread_id: message.thread_id.clone(),
            ..Default::default()
        });
    if conversation.thread_id.is_none() {
        conversation.thread_id.clone_from(&message.thread_id);
    }
    if conversation.id.trim().is_empty() {
        return Err(WaFailure::rejected(
            "WhatsApp send requires a conversation identity",
        ));
    }
    if conversation.thread_id.is_some() || conversation.topic_id.is_some() {
        return Err(WaFailure::unsupported(
            "WhatsApp conversations have no thread or topic addressing",
        ));
    }
    let channel = message.channel_ref.clone().unwrap_or_else(|| ChannelRef {
        id: provider.to_owned(),
        account_id: None,
    });
    if channel.id != provider {
        return Err(WaFailure::rejected(
            "message channel does not match the WhatsApp adapter",
        ));
    }
    Ok(OutboundScope {
        channel,
        conversation,
        reply_to_id: message
            .reply_to_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_owned),
        turn_id: message
            .turn_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_owned),
    })
}

impl OutboundScope {
    /// The scope a host media read is bound to. Reading stored media needs the
    /// configured account and the originating turn.
    fn host_scope(&self) -> Result<HostVoiceScope, WaFailure> {
        let configured_instance = self
            .channel
            .account_id
            .clone()
            .filter(|account| !account.trim().is_empty())
            .ok_or_else(|| {
                WaFailure::rejected("WhatsApp media send requires a configured account")
            })?;
        let turn_id = self.turn_id.clone().ok_or_else(|| {
            WaFailure::rejected("WhatsApp media send requires the originating turn identity")
        })?;
        Ok(HostVoiceScope {
            configured_instance,
            channel: self.channel.clone(),
            conversation: self.conversation.clone(),
            turn_id,
        })
    }
}

fn opaque_id(media: &MediaReference) -> Result<&str, WaFailure> {
    media
        .opaque_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| WaFailure::rejected("WhatsApp media has no host-owned opaque reference"))
}

/// Normalize a MIME header value: drop parameters and case.
pub(crate) fn base_mime(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

struct StoredAttachment {
    bytes: Vec<u8>,
    content_type: String,
    file_name: Option<String>,
}

async fn read_attachment(
    scope: &OutboundScope,
    ports: PlanPorts<'_>,
    expected: MediaKind,
    media: &MediaReference,
    limit: u64,
) -> Result<StoredAttachment, WaFailure> {
    let repository = ports.media.ok_or_else(|| {
        WaFailure::unsupported("WhatsApp media send requires scoped host media storage")
    })?;
    let id = opaque_id(media)?.to_owned();
    let stored = repository
        .read_media(scope.host_scope()?, id, limit)
        .await
        .map_err(|_| WaFailure::rejected("WhatsApp scoped media could not be read"))?;
    let content_type = base_mime(&stored.content_type);
    if stored.media.kind != expected
        || !crate::host::is_allowed_content_type(expected, &content_type)
        || stored.bytes.is_empty()
        || stored.bytes.len() as u64 > limit
    {
        return Err(WaFailure::rejected(
            "WhatsApp media failed upload validation",
        ));
    }
    Ok(StoredAttachment {
        bytes: stored.bytes,
        content_type,
        file_name: stored.file_name,
    })
}

/// Resample a normalized 0.0-1.0 loudness preview to WhatsApp's 64 buckets of
/// 0-100.
pub(crate) fn waveform_bytes(waveform: &[f32]) -> Option<Vec<u8>> {
    if waveform.is_empty() || waveform.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let mut buckets = Vec::with_capacity(WAVEFORM_BUCKETS);
    for bucket in 0..WAVEFORM_BUCKETS {
        let start = bucket * waveform.len() / WAVEFORM_BUCKETS;
        let end = ((bucket + 1) * waveform.len() / WAVEFORM_BUCKETS).max(start + 1);
        let window = &waveform[start.min(waveform.len() - 1)..end.min(waveform.len())];
        let peak = window
            .iter()
            .copied()
            .fold(0.0_f32, f32::max)
            .clamp(0.0, 1.0);
        buckets.push((peak * 100.0).round() as u8);
    }
    Some(buckets)
}

async fn plan_voice(
    scope: &OutboundScope,
    ports: PlanPorts<'_>,
    media: &MediaReference,
    codec: Option<&str>,
    duration_ms: Option<u64>,
    waveform: Option<&[f32]>,
) -> Result<PlannedPart, WaFailure> {
    let store = ports.voices.ok_or_else(|| {
        WaFailure::unsupported("WhatsApp voice notes require host voice media storage")
    })?;
    let id = opaque_id(media)?.to_owned();
    let resolved = store
        .read_voice(scope.host_scope()?, id)
        .await
        .map_err(|_| WaFailure::rejected("WhatsApp scoped voice media could not be read"))?;
    let bytes = BASE64
        .decode(resolved.audio_base64.as_bytes())
        .map_err(|_| WaFailure::rejected("WhatsApp voice media is not valid audio data"))?;
    let content_type = base_mime(&resolved.mime_type);
    let is_ogg = content_type == "audio/ogg" || codec == Some("ogg_opus");
    if !is_ogg || !bytes.starts_with(b"OggS") {
        return Err(WaFailure::unsupported(
            "WhatsApp voice notes must be OGG/Opus audio",
        ));
    }
    if bytes.len() as u64 > MAX_VOICE_BYTES {
        return Err(WaFailure::rejected(
            "WhatsApp voice media exceeds its size limit",
        ));
    }
    Ok(PlannedPart::Voice {
        bytes,
        content_type: "audio/ogg; codecs=opus".into(),
        duration_ms,
        waveform: waveform.and_then(waveform_bytes),
    })
}

fn attachment(
    kind: AttachmentKind,
    stored: StoredAttachment,
    caption: Option<&str>,
    duration_ms: Option<u64>,
    limits: &MediaLimits,
    follow_up: &mut Vec<PlannedPart>,
) -> PlannedPart {
    let caption = caption
        .map(strip_tool_call_tags)
        .map(|caption| caption.trim().to_owned())
        .filter(|caption| !caption.is_empty());
    let caption = match caption {
        // A caption WhatsApp would reject travels as the next message instead.
        Some(caption) if caption.chars().count() > limits.caption_chars => {
            follow_up.push(PlannedPart::Text(caption));
            None
        }
        other => other,
    };
    PlannedPart::Attachment {
        kind,
        bytes: stored.bytes,
        content_type: stored.content_type,
        file_name: stored.file_name,
        caption,
        duration_ms,
    }
}

/// Plan every part of `message`. The result is non-empty or an error.
pub(crate) async fn plan_parts(
    message: &ChannelOutboundMessage,
    scope: &OutboundScope,
    ports: PlanPorts<'_>,
    limits: &MediaLimits,
) -> Result<Vec<PlannedPart>, WaFailure> {
    let mut planned = Vec::new();
    for part in &message.parts {
        let mut follow_up = Vec::new();
        let next = match part {
            OutboundContentPart::Content {
                part: ContentPart::Text { text, .. },
            } => {
                let text = strip_tool_call_tags(text);
                if text.trim().is_empty() {
                    continue;
                }
                PlannedPart::Text(text)
            }
            OutboundContentPart::Content {
                part: ContentPart::Image { media, caption, .. },
            } => {
                let stored =
                    read_attachment(scope, ports, MediaKind::Image, media, limits.image).await?;
                // WhatsApp renders JPEG and PNG as photos; other images keep
                // their bytes as a document instead of being refused.
                let kind = if matches!(stored.content_type.as_str(), "image/jpeg" | "image/png") {
                    AttachmentKind::Image
                } else {
                    AttachmentKind::Document
                };
                attachment(
                    kind,
                    stored,
                    caption.as_deref(),
                    None,
                    limits,
                    &mut follow_up,
                )
            }
            OutboundContentPart::Content {
                part:
                    ContentPart::Video {
                        media,
                        caption,
                        duration_ms,
                        ..
                    },
            } => {
                let stored =
                    read_attachment(scope, ports, MediaKind::Video, media, limits.video).await?;
                let kind = if stored.content_type == "video/mp4" {
                    AttachmentKind::Video
                } else {
                    AttachmentKind::Document
                };
                attachment(
                    kind,
                    stored,
                    caption.as_deref(),
                    *duration_ms,
                    limits,
                    &mut follow_up,
                )
            }
            OutboundContentPart::Content {
                part:
                    ContentPart::Audio {
                        media,
                        transcript,
                        duration_ms,
                        ..
                    },
            } => {
                let stored =
                    read_attachment(scope, ports, MediaKind::Audio, media, limits.audio).await?;
                let kind = if matches!(
                    stored.content_type.as_str(),
                    "audio/mpeg" | "audio/mp4" | "audio/ogg"
                ) {
                    AttachmentKind::Audio
                } else {
                    AttachmentKind::Document
                };
                let planned = attachment(kind, stored, None, *duration_ms, limits, &mut follow_up);
                // Audio messages carry no caption; the transcript follows.
                if let Some(transcript) = transcript
                    .as_deref()
                    .map(strip_tool_call_tags)
                    .filter(|text| !text.trim().is_empty())
                {
                    follow_up.push(PlannedPart::Text(transcript));
                }
                planned
            }
            OutboundContentPart::Content {
                part:
                    ContentPart::File {
                        media,
                        file_name,
                        caption,
                    },
            } => {
                let mut stored =
                    read_attachment(scope, ports, MediaKind::Document, media, limits.document)
                        .await?;
                if let Some(name) = file_name.as_deref().filter(|name| !name.trim().is_empty()) {
                    stored.file_name = Some(name.to_owned());
                }
                attachment(
                    AttachmentKind::Document,
                    stored,
                    caption.as_deref(),
                    None,
                    limits,
                    &mut follow_up,
                )
            }
            OutboundContentPart::Content {
                part: ContentPart::Sticker { media, .. },
            } => {
                let stored =
                    read_attachment(scope, ports, MediaKind::Image, media, limits.sticker).await?;
                if stored.content_type != "image/webp" {
                    return Err(WaFailure::rejected("WhatsApp stickers must be WEBP images"));
                }
                attachment(
                    AttachmentKind::Sticker,
                    stored,
                    None,
                    None,
                    limits,
                    &mut follow_up,
                )
            }
            OutboundContentPart::Content {
                part:
                    ContentPart::Location {
                        latitude,
                        longitude,
                        label,
                        address,
                        ..
                    },
            } => {
                if !latitude.is_finite()
                    || !longitude.is_finite()
                    || !(-90.0..=90.0).contains(latitude)
                    || !(-180.0..=180.0).contains(longitude)
                {
                    return Err(WaFailure::rejected("WhatsApp location is out of range"));
                }
                PlannedPart::Location {
                    latitude: *latitude,
                    longitude: *longitude,
                    name: label.clone().filter(|name| !name.trim().is_empty()),
                    address: address.clone().filter(|address| !address.trim().is_empty()),
                }
            }
            OutboundContentPart::Content {
                part:
                    ContentPart::Contact {
                        display_name,
                        phone_numbers,
                        emails,
                        organization,
                    },
            } => {
                if display_name.trim().is_empty() && phone_numbers.is_empty() {
                    return Err(WaFailure::rejected(
                        "WhatsApp contact needs a name or a phone number",
                    ));
                }
                PlannedPart::Contact(ContactCard {
                    display_name: display_name.clone(),
                    phones: phone_numbers.clone(),
                    emails: emails.clone(),
                    organization: organization.clone(),
                })
            }
            OutboundContentPart::Voice {
                voice:
                    OutboundVoice::Media {
                        media,
                        codec,
                        duration_ms,
                        ..
                    },
            } => plan_voice(scope, ports, media, codec.as_deref(), *duration_ms, None).await?,
            OutboundContentPart::Voice {
                voice: OutboundVoice::Synthesize { .. },
            } => {
                return Err(WaFailure::unsupported(
                    "voice synthesis is prepared by the shared module runtime, not the provider",
                ));
            }
            OutboundContentPart::Content {
                part: ContentPart::VoiceNote { voice_note },
            } => {
                plan_voice(
                    scope,
                    ports,
                    &voice_note.media,
                    voice_note.codec.as_deref(),
                    voice_note.duration_ms,
                    voice_note.waveform.as_deref(),
                )
                .await?
            }
            OutboundContentPart::Content {
                part:
                    ContentPart::Buttons { .. }
                    | ContentPart::Poll { .. }
                    | ContentPart::Presentation { .. }
                    | ContentPart::Native { .. },
            } => {
                return Err(WaFailure::unsupported(
                    "this adapter does not carry buttons, polls, presentations or native data",
                ));
            }
        };
        planned.push(next);
        planned.extend(follow_up);
    }
    if planned.is_empty() {
        return Err(WaFailure::rejected("WhatsApp send has no content"));
    }
    Ok(planned)
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;
