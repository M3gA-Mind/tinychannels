//! Persisting inbound WhatsApp attachments and shaping them into content parts.
//!
//! Provider URLs and handles are private and expire, so an inbound attachment
//! is downloaded by the adapter, stored through the host's scoped ports, and
//! only the host's opaque reference reaches the envelope.

#![cfg_attr(
    not(feature = "whatsapp-web"),
    allow(dead_code, reason = "the Web adapter shares these helpers")
)]

use super::error::WaFailure;
use super::plan::base_mime;
use crate::host::{
    MediaMetadata, MediaRepository, MediaStoreRequest, VoiceMediaRequest, VoiceMediaStore,
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use tinychannels_bus::{
    ChannelRef, ContentPart, ConversationKind, ConversationRef, HostVoiceMediaDirection,
    HostVoiceScope, MediaKind, MediaReference, VoiceNote,
};

/// Largest inbound attachment the adapters download.
pub(crate) const MAX_INBOUND_MEDIA_BYTES: u64 = 20 * 1024 * 1024;
/// Largest inbound voice note stored through the inline host callback.
pub(crate) const MAX_INBOUND_VOICE_BYTES: u64 = 8 * 1024 * 1024;

/// What WhatsApp said an inbound attachment is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InboundKind {
    Image,
    Video,
    Audio,
    Document,
    Sticker,
}

impl InboundKind {
    pub(crate) fn media_kind(self) -> MediaKind {
        match self {
            Self::Image | Self::Sticker => MediaKind::Image,
            Self::Video => MediaKind::Video,
            Self::Audio => MediaKind::Audio,
            Self::Document => MediaKind::Document,
        }
    }

    pub(crate) fn field(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
            Self::Audio => "audio",
            Self::Document => "document",
            Self::Sticker => "sticker",
        }
    }
}

/// The scope an inbound attachment is stored under. The module overwrites the
/// account fields; the adapter supplies the conversation and the message id
/// that the host's later scoped reads resolve against.
pub(crate) fn inbound_scope(
    provider: &str,
    conversation: &ConversationRef,
    message_id: &str,
) -> HostVoiceScope {
    HostVoiceScope {
        configured_instance: String::new(),
        channel: ChannelRef {
            id: provider.to_owned(),
            account_id: None,
        },
        conversation: conversation.clone(),
        turn_id: message_id.to_owned(),
    }
}

/// One downloaded attachment, before storage.
#[derive(Debug)]
pub(crate) struct DownloadedAttachment {
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub file_name: Option<String>,
    pub duration_ms: Option<u64>,
}

/// Store a downloaded attachment through the host and build its content part.
pub(crate) async fn store_attachment(
    repository: &dyn MediaRepository,
    scope: HostVoiceScope,
    kind: InboundKind,
    download: DownloadedAttachment,
    caption: Option<String>,
    dedupe_key: String,
) -> Result<ContentPart, WaFailure> {
    let content_type = base_mime(&download.content_type);
    // The host store only accepts types it can validate; anything else is
    // preserved as an opaque document rather than dropped.
    let (kind, media_kind) =
        if crate::host::is_allowed_content_type(kind.media_kind(), &content_type) {
            (kind, kind.media_kind())
        } else if crate::host::is_allowed_content_type(MediaKind::Document, &content_type) {
            (InboundKind::Document, MediaKind::Document)
        } else {
            return Err(WaFailure::rejected(
                "inbound WhatsApp attachment has an unsupported content type",
            ));
        };
    if download.bytes.is_empty() || download.bytes.len() as u64 > MAX_INBOUND_MEDIA_BYTES {
        return Err(WaFailure::rejected(
            "inbound WhatsApp attachment size is outside its limit",
        ));
    }
    let file_name = download.file_name.clone();
    let duration_ms = download.duration_ms;
    let media = repository
        .store_media(MediaStoreRequest {
            metadata: MediaMetadata {
                scope,
                kind: media_kind,
                content_type,
                file_name: file_name.clone(),
                duration_ms,
                codec: None,
                voice_direction: None,
                dedupe_key: Some(dedupe_key),
                max_bytes: MAX_INBOUND_MEDIA_BYTES,
            },
            bytes: download.bytes,
        })
        .await
        .map_err(|_| WaFailure::uncertain("inbound WhatsApp attachment could not be stored"))?;
    Ok(match kind {
        InboundKind::Image => ContentPart::Image {
            media,
            caption,
            alt_text: None,
        },
        InboundKind::Sticker => ContentPart::Sticker {
            media,
            pack_id: None,
            sticker_id: None,
            emoji: None,
            alt_text: None,
        },
        InboundKind::Video => ContentPart::Video {
            media,
            caption,
            duration_ms,
            thumbnail: None,
        },
        InboundKind::Audio => ContentPart::Audio {
            media,
            codec: None,
            duration_ms,
            title: None,
            transcript: None,
        },
        InboundKind::Document => ContentPart::File {
            media,
            file_name,
            caption,
        },
    })
}

/// Store a push-to-talk voice note and build its envelope part. Transcription
/// is the module's job: the part is returned without a transcript.
pub(crate) async fn store_voice_note(
    store: &dyn VoiceMediaStore,
    scope: HostVoiceScope,
    download: DownloadedAttachment,
    waveform: Option<Vec<f32>>,
) -> Result<VoiceNote, WaFailure> {
    if download.bytes.is_empty() || download.bytes.len() as u64 > MAX_INBOUND_VOICE_BYTES {
        return Err(WaFailure::rejected(
            "inbound WhatsApp voice note size is outside its limit",
        ));
    }
    let mime_type = base_mime(&download.content_type);
    let codec = (mime_type == "audio/ogg").then(|| "ogg_opus".to_owned());
    let stored = store
        .store_voice(VoiceMediaRequest {
            audio_base64: BASE64.encode(&download.bytes),
            mime_type: mime_type.clone(),
            codec: codec.clone(),
            duration_ms: download.duration_ms,
            file_name: download.file_name,
            direction: HostVoiceMediaDirection::Inbound,
            scope,
        })
        .await
        .map_err(|_| WaFailure::uncertain("inbound WhatsApp voice note could not be stored"))?;
    let mut media = stored.media;
    if media
        .opaque_id
        .as_deref()
        .is_none_or(|id| id.trim().is_empty())
    {
        return Err(WaFailure::uncertain(
            "host voice storage returned no durable opaque reference",
        ));
    }
    // Provider handles and URLs are private and expiring; only the host's
    // opaque reference may reach history.
    media.url = None;
    media.path = None;
    media.local_cache_path = None;
    media.kind = MediaKind::Audio;
    media.content_type = Some(mime_type);
    media.codec.clone_from(&codec);
    let duration_ms = media.duration_ms.or(download.duration_ms);
    Ok(VoiceNote {
        media,
        codec,
        duration_ms,
        waveform,
        push_to_talk: true,
        transcript: None,
    })
}

/// The media references an envelope's parts carry, in part order.
pub(crate) fn envelope_media(parts: &[ContentPart]) -> Vec<MediaReference> {
    parts
        .iter()
        .flat_map(|part| match part {
            ContentPart::Image { media, .. }
            | ContentPart::Audio { media, .. }
            | ContentPart::File { media, .. }
            | ContentPart::Sticker { media, .. }
            | ContentPart::Video { media, .. } => vec![media.clone()],
            ContentPart::VoiceNote { voice_note } => vec![voice_note.media.clone()],
            _ => Vec::new(),
        })
        .collect()
}

/// Conversation kind from a WhatsApp chat address.
pub(crate) fn conversation_kind(is_group: bool) -> ConversationKind {
    if is_group {
        ConversationKind::Group
    } else {
        ConversationKind::Dm
    }
}

/// Unix seconds (as the providers send them) to milliseconds.
pub(crate) fn seconds_to_ms(seconds: u64) -> u64 {
    seconds.saturating_mul(1000)
}

/// Decode normalized 0-100 waveform bytes into 0.0-1.0 samples.
pub(crate) fn waveform_samples(bytes: &[u8]) -> Option<Vec<f32>> {
    (!bytes.is_empty()).then(|| {
        bytes
            .iter()
            .map(|byte| f32::from(*byte).min(100.0) / 100.0)
            .collect()
    })
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
