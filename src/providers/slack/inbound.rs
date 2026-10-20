//! Normalization of Slack message events: addressing, senders and attached
//! files (including audio clips) into typed content.

use super::types::{SLACK_MAX_FILE_BYTES, SLACK_MAX_VOICE_BYTES, SlackChannel};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;
use tinychannels_bus::{
    ChannelRef, ContentPart, ConversationKind, ConversationRef, HostVoiceMediaDirection,
    HostVoiceScope, MediaKind, SenderRef, VoiceNote, VoiceTranscript, VoiceTranscriptSource,
    VoiceTranscriptState,
};

pub(crate) fn slack_channel_ref() -> ChannelRef {
    ChannelRef {
        id: "slack".into(),
        account_id: None,
    }
}

/// Slack timestamps (`"1700000000.000200"`) as Unix milliseconds.
pub(crate) fn ts_to_ms(ts: &str) -> Option<u64> {
    let seconds = ts.parse::<f64>().ok()?;
    (seconds.is_finite() && seconds >= 0.0).then_some((seconds * 1000.0) as u64)
}

fn conversation_kind(channel_id: &str, channel_type: Option<&str>) -> ConversationKind {
    match (channel_type, channel_id.chars().next()) {
        (Some("im"), _) | (None, Some('D')) => ConversationKind::Dm,
        (Some("mpim"), _) => ConversationKind::Group,
        (Some("channel" | "group"), _) | (None, Some('C' | 'G')) => ConversationKind::Channel,
        _ => ConversationKind::Unknown,
    }
}

impl SlackChannel {
    /// Thread identifier for an inbound message: the thread root for replies,
    /// the message itself for top-level posts (so replies thread under it).
    pub(crate) fn inbound_thread_ts(msg: &Value, ts: &str) -> Option<String> {
        msg.get("thread_ts")
            .and_then(Value::as_str)
            .or(if ts.is_empty() { None } else { Some(ts) })
            .map(str::to_string)
    }

    pub(crate) fn conversation_ref(
        channel_id: &str,
        channel_type: Option<&str>,
        thread_ts: Option<String>,
    ) -> ConversationRef {
        ConversationRef {
            kind: conversation_kind(channel_id, channel_type),
            id: channel_id.to_owned(),
            thread_id: thread_ts,
            ..Default::default()
        }
    }

    pub(crate) fn sender_ref(user: &Value) -> SenderRef {
        SenderRef {
            id: user
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            name: user
                .get("username")
                .or_else(|| user.get("name"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            ..Default::default()
        }
    }

    fn scope(conversation: &ConversationRef, turn_id: &str) -> HostVoiceScope {
        HostVoiceScope {
            configured_instance: String::new(),
            channel: slack_channel_ref(),
            conversation: conversation.clone(),
            turn_id: turn_id.to_owned(),
        }
    }

    /// Download one private file. Only URLs under the trusted files origin
    /// receive the bot token.
    async fn download_file(&self, url: &str, max_bytes: u64, expect_html: bool) -> Option<Vec<u8>> {
        if !url.starts_with(&self.files_base) {
            tracing::warn!("[slack] refusing to fetch a file from an untrusted origin");
            return None;
        }
        let mut response = self
            .http_client()
            .get(url)
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?;
        let is_html = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/html"));
        if !response.status().is_success()
            || (is_html && !expect_html)
            || response.content_length().is_some_and(|len| len > max_bytes)
        {
            tracing::warn!("[slack] file download was rejected");
            return None;
        }
        let mut bytes = Vec::new();
        while let Ok(Some(chunk)) = response.chunk().await {
            if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
                tracing::warn!("[slack] file exceeds the size limit");
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        (!bytes.is_empty()).then_some(bytes)
    }

    /// Typed media parts and an optional voice note for the files attached to
    /// `event`. A file that cannot be stored is logged and skipped, so the
    /// message text is still delivered.
    pub(crate) async fn media_parts(
        &self,
        event: &Value,
        message_id: &str,
        conversation: &ConversationRef,
    ) -> (Vec<ContentPart>, Option<VoiceNote>) {
        let mut parts = Vec::new();
        let mut voice = None;
        let files = event.get("files").and_then(Value::as_array);
        for file in files.into_iter().flatten() {
            let id = file.get("id").and_then(Value::as_str).unwrap_or("file");
            let mime = file
                .get("mimetype")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            let is_clip = file.get("subtype").and_then(Value::as_str) == Some("slack_audio");
            let kind = match mime.split('/').next() {
                Some("image") => MediaKind::Image,
                Some("video") => MediaKind::Video,
                Some("audio") => MediaKind::Audio,
                _ => MediaKind::Document,
            };
            if !crate::host::is_allowed_content_type(kind, &mime) {
                tracing::debug!(
                    file = id,
                    "[slack] skipping a file with an unsupported type"
                );
                continue;
            }
            let Some(url) = file.get("url_private_download").and_then(Value::as_str) else {
                continue;
            };
            let size = file.get("size").and_then(Value::as_u64).unwrap_or(0);
            let limit = if is_clip && self.voice_media_store.is_some() {
                SLACK_MAX_VOICE_BYTES
            } else {
                SLACK_MAX_FILE_BYTES
            };
            if size > limit {
                tracing::warn!(file = id, "[slack] skipping an oversized file");
                continue;
            }
            let Some(bytes) = self.download_file(url, limit, mime == "text/html").await else {
                continue;
            };
            let name = file.get("name").and_then(Value::as_str).map(str::to_owned);
            let duration_ms = file.get("duration_ms").and_then(Value::as_u64);
            if is_clip && let Some(store) = self.voice_media_store.as_ref() {
                let codec = codec_for(&mime);
                let result = store
                    .store_voice(crate::host::VoiceMediaRequest {
                        audio_base64: BASE64.encode(&bytes),
                        mime_type: mime.clone(),
                        codec: codec.clone(),
                        duration_ms,
                        file_name: name.clone(),
                        direction: HostVoiceMediaDirection::Inbound,
                        scope: Self::scope(conversation, message_id),
                    })
                    .await;
                let Ok(result) = result else {
                    tracing::warn!(file = id, "[slack] voice clip could not be stored");
                    continue;
                };
                let mut media = result.media;
                if media
                    .opaque_id
                    .as_deref()
                    .is_none_or(|v| v.trim().is_empty())
                {
                    tracing::warn!(file = id, "[slack] voice store returned no reference");
                    continue;
                }
                media.url = None;
                media.path = None;
                media.local_cache_path = None;
                media.kind = MediaKind::Audio;
                media.content_type = Some(mime.clone());
                media.codec = codec.clone();
                voice = Some(VoiceNote {
                    media,
                    codec,
                    duration_ms,
                    waveform: None,
                    push_to_talk: false,
                    transcript: provider_transcript(file),
                });
                continue;
            }
            let Some(repository) = self.media_repository.as_ref() else {
                tracing::debug!(file = id, "[slack] no media repository; skipping file");
                continue;
            };
            let stored = repository
                .store_media(crate::host::MediaStoreRequest {
                    metadata: crate::host::MediaMetadata {
                        scope: Self::scope(conversation, message_id),
                        kind,
                        content_type: mime.clone(),
                        file_name: name.clone(),
                        duration_ms,
                        codec: None,
                        voice_direction: None,
                        dedupe_key: Some(format!("slack:{message_id}:{id}")),
                        max_bytes: SLACK_MAX_FILE_BYTES,
                    },
                    bytes,
                })
                .await;
            let Ok(media) = stored else {
                tracing::warn!(file = id, "[slack] file could not be stored as typed media");
                continue;
            };
            parts.push(match kind {
                MediaKind::Image => ContentPart::Image {
                    media,
                    caption: None,
                    alt_text: None,
                },
                MediaKind::Video => ContentPart::Video {
                    media,
                    caption: None,
                    duration_ms,
                    thumbnail: None,
                },
                MediaKind::Audio => ContentPart::Audio {
                    media,
                    codec: None,
                    duration_ms,
                    title: None,
                    transcript: None,
                },
                _ => ContentPart::File {
                    media,
                    file_name: name,
                    caption: None,
                },
            });
        }
        (parts, voice)
    }
}

fn codec_for(mime: &str) -> Option<String> {
    match mime {
        "audio/mp4" => Some("m4a_aac".into()),
        "audio/mpeg" => Some("mp3".into()),
        "audio/ogg" => Some("ogg_opus".into()),
        _ => None,
    }
}

/// Slack's own transcription of an audio clip, when it has finished.
fn provider_transcript(file: &Value) -> Option<VoiceTranscript> {
    let transcription = file.get("transcription")?;
    if transcription.get("status").and_then(Value::as_str) != Some("complete") {
        return None;
    }
    let text = transcription
        .pointer("/preview/content")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())?;
    Some(VoiceTranscript {
        text: text.to_owned(),
        language: transcription
            .get("locale")
            .and_then(Value::as_str)
            .map(str::to_owned),
        state: VoiceTranscriptState::Complete,
        source: VoiceTranscriptSource::Provider,
    })
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
