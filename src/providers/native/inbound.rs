//! Provider-neutral inbound media: store a downloaded attachment as typed
//! content, or as a voice note when the platform marks it as one.

use crate::host::{
    EventSink, MediaMetadata, MediaRepository, MediaStoreRequest, Transcriber,
    TranscriptionRequest, VoiceMediaRequest, VoiceMediaStore,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::sync::Arc;
use tinychannels_bus::{
    ChannelEvent, ChannelEventKind, ChannelInboundEnvelope, ChannelIngress, ChannelRef,
    ContentPart, ConversationRef, HostVoiceMediaDirection, HostVoiceScope, MediaKind,
    MediaReference, VoiceNote, VoiceTranscript, VoiceTranscriptSource, VoiceTranscriptState,
};

/// The host ports and limits inbound ingestion needs.
pub(crate) struct Ingest<'a> {
    pub(crate) provider: &'static str,
    pub(crate) channel: ChannelRef,
    pub(crate) repository: Option<&'a Arc<dyn MediaRepository>>,
    pub(crate) voice_store: Option<&'a Arc<dyn VoiceMediaStore>>,
    /// Host speech-to-text; without one the shared ingress transcribes later.
    pub(crate) transcriber: Option<&'a Arc<dyn Transcriber>>,
    pub(crate) file_bytes: u64,
    pub(crate) voice_bytes: u64,
}

/// One downloaded attachment.
pub(crate) struct InboundFile {
    pub(crate) bytes: Vec<u8>,
    pub(crate) mime: String,
    pub(crate) name: Option<String>,
    pub(crate) duration_ms: Option<u64>,
    /// The platform flagged this as a voice note.
    pub(crate) voice: bool,
    /// Stable per-message-and-attachment key so a redelivery deduplicates.
    pub(crate) dedupe_key: String,
    pub(crate) transcript: Option<VoiceTranscript>,
}

/// The typed content a set of attachments produced.
#[derive(Default)]
pub(crate) struct Ingested {
    pub(crate) parts: Vec<ContentPart>,
    pub(crate) voice: Option<VoiceNote>,
}

/// The media kind a MIME type belongs to.
pub(crate) fn kind_for_mime(mime: &str) -> MediaKind {
    match mime.split('/').next() {
        Some("image") => MediaKind::Image,
        Some("video") => MediaKind::Video,
        Some("audio") => MediaKind::Audio,
        _ => MediaKind::Document,
    }
}

/// Bare, lowercase MIME type without parameters.
pub(crate) fn normalize_mime(mime: &str) -> String {
    mime.split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// Codec name the shared voice pipeline knows for an audio MIME type.
pub(crate) fn codec_for(mime: &str) -> Option<String> {
    match mime {
        "audio/amr" => Some("amr_nb".into()),
        "audio/mp4" | "audio/m4a" | "audio/x-m4a" | "audio/aac" => Some("m4a_aac".into()),
        "audio/mpeg" | "audio/mp3" => Some("mp3".into()),
        "audio/ogg" | "audio/opus" => Some("ogg_opus".into()),
        "audio/x-caf" | "audio/caf" => Some("caf".into()),
        _ => None,
    }
}

impl Ingest<'_> {
    fn scope(&self, conversation: &ConversationRef, message_id: &str) -> HostVoiceScope {
        HostVoiceScope {
            configured_instance: String::new(),
            channel: self.channel.clone(),
            conversation: conversation.clone(),
            turn_id: message_id.to_owned(),
        }
    }

    /// Transcribe with the host's speech-to-text, when it has one.
    async fn transcribe(
        &self,
        conversation: &ConversationRef,
        message_id: &str,
        bytes: &[u8],
        mime: &str,
        name: &Option<String>,
    ) -> Option<VoiceTranscript> {
        let transcriber = self.transcriber?;
        let result = transcriber
            .transcribe_scoped(
                self.scope(conversation, message_id),
                TranscriptionRequest {
                    audio_base64: BASE64.encode(bytes),
                    mime_type: Some(mime.to_owned()),
                    file_name: name.clone(),
                    language: None,
                },
            )
            .await;
        match result {
            Ok(result) if !result.text.trim().is_empty() => Some(VoiceTranscript {
                text: result.text,
                language: result.language,
                state: VoiceTranscriptState::Complete,
                source: VoiceTranscriptSource::HostStt,
            }),
            Ok(_) => None,
            Err(error) => {
                tracing::warn!(provider = self.provider, %error, "[native] voice transcription failed");
                None
            }
        }
    }

    /// Store `file`. A file that cannot be stored is logged and skipped so the
    /// message text is still delivered.
    pub(crate) async fn add(
        &self,
        into: &mut Ingested,
        conversation: &ConversationRef,
        message_id: &str,
        file: InboundFile,
    ) {
        let provider = self.provider;
        let mime = normalize_mime(&file.mime);
        let kind = kind_for_mime(&mime);
        if !crate::host::is_allowed_content_type(kind, &mime) {
            tracing::debug!(
                provider,
                "[native] skipping an attachment with an unsupported type"
            );
            return;
        }
        let voice_clip = file.voice && kind == MediaKind::Audio && self.voice_store.is_some();
        let limit = if voice_clip {
            self.voice_bytes
        } else {
            self.file_bytes
        };
        if file.bytes.is_empty() || file.bytes.len() as u64 > limit {
            tracing::warn!(
                provider,
                "[native] skipping an empty or oversized attachment"
            );
            return;
        }
        if voice_clip && let Some(store) = self.voice_store {
            if into.voice.is_some() {
                tracing::debug!(provider, "[native] only the first voice clip is kept");
                return;
            }
            let codec = codec_for(&mime);
            let stored = store
                .store_voice(VoiceMediaRequest {
                    audio_base64: BASE64.encode(&file.bytes),
                    mime_type: mime.clone(),
                    codec: codec.clone(),
                    duration_ms: file.duration_ms,
                    file_name: file.name.clone(),
                    direction: HostVoiceMediaDirection::Inbound,
                    scope: self.scope(conversation, message_id),
                })
                .await;
            let Ok(stored) = stored else {
                tracing::warn!(provider, "[native] voice clip could not be stored");
                return;
            };
            let mut media = stored.media;
            if media
                .opaque_id
                .as_deref()
                .is_none_or(|id| id.trim().is_empty())
            {
                tracing::warn!(provider, "[native] voice store returned no reference");
                return;
            }
            media.url = None;
            media.path = None;
            media.local_cache_path = None;
            media.kind = MediaKind::Audio;
            media.content_type = Some(mime);
            media.codec = codec.clone();
            let transcript = match file.transcript {
                Some(transcript) => Some(transcript),
                None => {
                    self.transcribe(
                        conversation,
                        message_id,
                        &file.bytes,
                        &mime_for(&media),
                        &file.name,
                    )
                    .await
                }
            };
            into.voice = Some(VoiceNote {
                media,
                codec,
                duration_ms: file.duration_ms,
                waveform: None,
                push_to_talk: false,
                transcript,
            });
            return;
        }
        let Some(repository) = self.repository else {
            tracing::debug!(
                provider,
                "[native] no media repository; skipping attachment"
            );
            return;
        };
        let stored = repository
            .store_media(MediaStoreRequest {
                metadata: MediaMetadata {
                    scope: self.scope(conversation, message_id),
                    kind,
                    content_type: mime,
                    file_name: file.name.clone(),
                    duration_ms: file.duration_ms,
                    codec: None,
                    voice_direction: None,
                    dedupe_key: Some(file.dedupe_key),
                    max_bytes: self.file_bytes,
                },
                bytes: file.bytes,
            })
            .await;
        let Ok(media) = stored else {
            tracing::warn!(
                provider,
                "[native] attachment could not be stored as typed media"
            );
            return;
        };
        into.parts.push(match kind {
            MediaKind::Image => ContentPart::Image {
                media,
                caption: None,
                alt_text: None,
            },
            MediaKind::Video => ContentPart::Video {
                media,
                caption: None,
                duration_ms: file.duration_ms,
                thumbnail: None,
            },
            MediaKind::Audio => ContentPart::Audio {
                media,
                codec: None,
                duration_ms: file.duration_ms,
                title: None,
                transcript: None,
            },
            _ => ContentPart::File {
                media,
                file_name: file.name,
                caption: None,
            },
        });
    }
}

/// Every stored media reference in `parts`, in order.
pub(crate) fn media_of(parts: &[ContentPart]) -> Vec<MediaReference> {
    parts
        .iter()
        .flat_map(|part| match part {
            ContentPart::Image { media, .. }
            | ContentPart::Audio { media, .. }
            | ContentPart::File { media, .. }
            | ContentPart::Video { media, .. } => vec![media.clone()],
            ContentPart::VoiceNote { voice_note } => vec![voice_note.media.clone()],
            _ => Vec::new(),
        })
        .collect()
}

/// Text, the voice note and attachments as one ordered part list.
pub(crate) fn assemble(text: &str, markdown: Option<&str>, ingested: Ingested) -> Vec<ContentPart> {
    let mut parts = Vec::new();
    if !text.trim().is_empty() {
        parts.push(ContentPart::Text {
            text: text.to_owned(),
            markdown_dialect: markdown.map(str::to_owned),
        });
    }
    if let Some(voice_note) = ingested.voice {
        parts.push(ContentPart::VoiceNote { voice_note });
    }
    parts.extend(ingested.parts);
    parts
}

/// Publish `envelope` on the ordered ingress path. False when there is no
/// sink or it did not take the message, so the caller can fall back to the
/// legacy channel.
pub(crate) async fn publish(
    provider: &str,
    events: Option<&Arc<dyn EventSink>>,
    envelope: ChannelInboundEnvelope,
) -> bool {
    let Some(events) = events else {
        return false;
    };
    events
        .publish_ingress(ChannelIngress::Inbound(envelope))
        .await
        .unwrap_or_else(|error| {
            tracing::debug!(provider, %error, "[native] failed to deliver inbound through ingress");
            false
        })
}

/// A v2 lifecycle event for `provider` (reactions, receipts, edits).
pub(crate) fn channel_event(
    provider: &str,
    event_id: String,
    source_timestamp: Option<u64>,
    conversation: Option<ConversationRef>,
    event: ChannelEventKind,
) -> ChannelEvent {
    ChannelEvent {
        version: 2,
        source_event_id: None,
        source_timestamp,
        event_id,
        channel: ChannelRef {
            id: provider.into(),
            account_id: None,
        },
        conversation,
        event,
    }
}

/// Publish a lifecycle event; a missing or failing sink only logs.
pub(crate) async fn publish_event(
    provider: &str,
    events: Option<&Arc<dyn EventSink>>,
    event: ChannelEvent,
) {
    let Some(events) = events else {
        return;
    };
    if let Err(error) = events.publish_ingress(ChannelIngress::Event(event)).await {
        tracing::debug!(provider, %error, "[native] failed to deliver channel event");
    }
}

fn mime_for(media: &MediaReference) -> String {
    media
        .content_type
        .clone()
        .unwrap_or_else(|| "audio/mp4".into())
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
