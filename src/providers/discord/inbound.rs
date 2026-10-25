//! Discord inbound normalization: messages, attachments, voice messages,
//! threads and lifecycle events.

use super::api::DISCORD_API_BASE;
use super::channel::{DiscordChannel, contains_bot_mention, normalize_incoming_content};
use crate::traits::ChannelMessage;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use tinychannels_bus::{
    ChannelEvent, ChannelEventKind, ChannelInboundEnvelope, ChannelIngress, ChannelRef,
    ContentPart, ConversationKind, ConversationRef, MediaKind, SenderRef, VoiceNote,
};
use uuid::Uuid;

/// Message flag set on a voice message (`IS_VOICE_MESSAGE`).
pub(super) const DISCORD_VOICE_MESSAGE_FLAG: u64 = 1 << 13;
/// Largest attachment downloaded into the media store.
pub(super) const MAX_INBOUND_MEDIA_BYTES: u64 = 20 * 1024 * 1024;
/// Voice audio crosses TinyBus inline, so it keeps the inline frame bound.
pub(super) const MAX_VOICE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_ATTACHMENTS_PER_MESSAGE: usize = 10;
const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;

/// What the adapter knows about one channel id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChannelInfo {
    /// Parent channel when this channel is a thread.
    pub(crate) thread_parent: Option<String>,
}

pub(super) fn channel_ref() -> ChannelRef {
    ChannelRef {
        id: "discord".into(),
        account_id: None,
    }
}

/// Unix milliseconds encoded in a Discord snowflake.
pub(super) fn snowflake_unix_ms(id: &str) -> Option<u64> {
    id.parse::<u64>()
        .ok()
        .map(|value| (value >> 22).saturating_add(DISCORD_EPOCH_MS))
}

/// True for the numeric ids Discord uses for channels, messages and users.
pub(super) fn is_snowflake(value: &str) -> bool {
    !value.is_empty() && value.len() <= 20 && value.bytes().all(|b| b.is_ascii_digit())
}

fn str_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

pub(super) fn user_ref(user: &Value) -> SenderRef {
    SenderRef {
        id: str_field(user, "id").unwrap_or_default().to_owned(),
        name: str_field(user, "global_name")
            .filter(|name| !name.is_empty())
            .or_else(|| str_field(user, "username"))
            .map(str::to_owned),
        is_bot: user.get("bot").and_then(Value::as_bool).unwrap_or(false),
        ..Default::default()
    }
}

/// Decide the text a message contributes, or `None` to ignore the message.
///
/// A message with attachments and no text is still a message (a voice
/// message never carries a mention), so it passes when the bot is not
/// mention-gated or the message replies to the bot.
pub(super) fn message_text(
    content: &str,
    mention_only: bool,
    bot_user_id: &str,
    has_attachments: bool,
    replies_to_bot: bool,
) -> Option<String> {
    if content.trim().is_empty() {
        return (has_attachments && (!mention_only || replies_to_bot)).then(String::new);
    }
    normalize_incoming_content(content, mention_only, bot_user_id).or_else(|| {
        // A bare mention that carries attachments still addresses the bot.
        (has_attachments && contains_bot_mention(content, bot_user_id)).then(String::new)
    })
}

fn media_kind_for(content_type: &str) -> MediaKind {
    match content_type.split('/').next().unwrap_or_default() {
        "image" => MediaKind::Image,
        "video" => MediaKind::Video,
        "audio" => MediaKind::Audio,
        _ => MediaKind::Document,
    }
}

fn normalized_content_type(raw: Option<&str>) -> String {
    raw.and_then(|value| value.split(';').next())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "application/octet-stream".into())
}

/// Decode Discord's base64 waveform (0..=255 per sample) into 0.0..=1.0.
pub(super) fn decode_waveform(encoded: &str) -> Option<Vec<f32>> {
    let bytes = BASE64.decode(encoded.as_bytes()).ok()?;
    (!bytes.is_empty()).then(|| bytes.into_iter().map(|b| f32::from(b) / 255.0).collect())
}

impl DiscordChannel {
    /// Record a thread channel's parent from a gateway or REST channel object.
    pub(super) fn remember_thread(&self, channel: &Value) {
        let is_thread = matches!(channel.get("type").and_then(Value::as_u64), Some(10..=12));
        if let (true, Some(id), Some(parent)) = (
            is_thread,
            str_field(channel, "id"),
            str_field(channel, "parent_id"),
        ) {
            self.channel_cache.lock().insert(
                id.to_owned(),
                ChannelInfo {
                    thread_parent: Some(parent.to_owned()),
                },
            );
        }
    }

    /// Resolve whether a guild channel is a thread. Cached; a failed lookup
    /// is treated as a plain channel and retried on the next message.
    pub(super) async fn channel_info(&self, channel_id: &str, in_guild: bool) -> ChannelInfo {
        if !in_guild || !is_snowflake(channel_id) {
            return ChannelInfo::default();
        }
        if let Some(info) = self.channel_cache.lock().get(channel_id) {
            return info.clone();
        }
        let url = format!("{}/channels/{channel_id}", self.api_base);
        let response = self
            .http_client()
            .get(&url)
            .header("Authorization", format!("Bot {}", self.bot_token))
            .send()
            .await;
        let Ok(response) = response else {
            tracing::debug!(channel_id, "[discord] channel lookup transport failed");
            return ChannelInfo::default();
        };
        if !response.status().is_success() {
            tracing::debug!(channel_id, status = %response.status(), "[discord] channel lookup rejected");
            return ChannelInfo::default();
        }
        let Ok(channel) = response.json::<Value>().await else {
            return ChannelInfo::default();
        };
        self.remember_thread(&channel);
        let info = self
            .channel_cache
            .lock()
            .get(channel_id)
            .cloned()
            .unwrap_or_default();
        if info.thread_parent.is_none() {
            self.channel_cache
                .lock()
                .insert(channel_id.to_owned(), info.clone());
        }
        info
    }

    /// Normalized address for a channel id, thread aware.
    pub(super) fn conversation_for(
        channel_id: &str,
        guild_id: Option<&str>,
        info: &ChannelInfo,
    ) -> ConversationRef {
        if let Some(parent) = info.thread_parent.as_deref() {
            return ConversationRef {
                kind: ConversationKind::Thread,
                id: parent.to_owned(),
                scope_id: guild_id.map(str::to_owned),
                parent_id: Some(parent.to_owned()),
                thread_id: Some(channel_id.to_owned()),
                topic_id: None,
            };
        }
        ConversationRef {
            kind: if guild_id.is_some() {
                ConversationKind::Channel
            } else {
                ConversationKind::Dm
            },
            id: channel_id.to_owned(),
            scope_id: guild_id.map(str::to_owned),
            ..Default::default()
        }
    }

    /// Guild, DM and configured-channel scoping shared by every inbound path.
    fn in_scope(&self, guild_id: Option<&str>, channel_id: &str, info: &ChannelInfo) -> bool {
        if !Self::passes_guild_scope(
            self.guild_id.as_deref(),
            guild_id,
            self.allowed_users.is_empty(),
        ) {
            return false;
        }
        match self.channel_id.as_deref() {
            Some(configured) => {
                channel_id == configured || info.thread_parent.as_deref() == Some(configured)
            }
            None => true,
        }
    }

    async fn publish_event(&self, event: ChannelEvent) {
        if let Some(events) = &self.events
            && let Err(error) = events.publish_ingress(ChannelIngress::Event(event)).await
        {
            tracing::debug!(%error, "[discord] failed to deliver channel event");
        }
    }

    async fn publish_event_to(
        &self,
        event: ChannelEvent,
        typed_sink: Option<&tokio::sync::mpsc::Sender<ChannelIngress>>,
    ) {
        if let Some(sink) = typed_sink {
            if let Err(error) = sink.send(ChannelIngress::Event(event)).await {
                tracing::debug!(%error, "[discord] typed Gateway event receiver closed");
            }
        } else {
            self.publish_event(event).await;
        }
    }

    fn actor_allowed(&self, user: &Value, bot_user_id: &str) -> bool {
        let id = str_field(user, "id").unwrap_or_default();
        if id == bot_user_id {
            return false;
        }
        if !self.listen_to_bots && user.get("bot").and_then(Value::as_bool).unwrap_or(false) {
            return false;
        }
        self.is_user_allowed(id)
    }

    /// Handle `MESSAGE_CREATE`. Returns `false` when the legacy receiver is gone.
    #[cfg(test)]
    pub(super) async fn handle_message_create(
        &self,
        d: &Value,
        bot_user_id: &str,
        tx: &tokio::sync::mpsc::Sender<ChannelMessage>,
    ) -> bool {
        self.handle_message_create_to(d, bot_user_id, Some(tx), None)
            .await
    }

    pub(super) async fn handle_message_create_typed(
        &self,
        d: &Value,
        bot_user_id: &str,
        tx: &tokio::sync::mpsc::Sender<ChannelIngress>,
    ) -> bool {
        self.handle_message_create_to(d, bot_user_id, None, Some(tx))
            .await
    }

    async fn handle_message_create_to(
        &self,
        d: &Value,
        bot_user_id: &str,
        legacy_tx: Option<&tokio::sync::mpsc::Sender<ChannelMessage>>,
        typed_tx: Option<&tokio::sync::mpsc::Sender<ChannelIngress>>,
    ) -> bool {
        let author = d.get("author").unwrap_or(&Value::Null);
        let author_id = str_field(author, "id").unwrap_or_default();
        if author_id == bot_user_id {
            return true;
        }
        if !self.listen_to_bots && author.get("bot").and_then(Value::as_bool).unwrap_or(false) {
            return true;
        }
        if !self.is_user_allowed(author_id) {
            tracing::warn!("Discord: ignoring message from unauthorized user: {author_id}");
            return true;
        }
        let guild_id = str_field(d, "guild_id");
        let channel_id = str_field(d, "channel_id").unwrap_or_default();
        let info = self.channel_info(channel_id, guild_id.is_some()).await;
        if !self.in_scope(guild_id, channel_id, &info) {
            return true;
        }

        let attachments = d
            .get("attachments")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let replies_to_bot = d
            .get("referenced_message")
            .and_then(|message| message.get("author"))
            .and_then(|author| str_field(author, "id"))
            == Some(bot_user_id);
        let Some(text) = message_text(
            str_field(d, "content").unwrap_or_default(),
            self.mention_only,
            bot_user_id,
            !attachments.is_empty(),
            replies_to_bot,
        ) else {
            return true;
        };

        let message_id = str_field(d, "id").unwrap_or_default();
        let id = if message_id.is_empty() {
            format!("discord_{}", Uuid::new_v4())
        } else {
            format!("discord_{message_id}")
        };
        let conversation = Self::conversation_for(channel_id, guild_id, &info);
        let (media_parts, voice_note) = self
            .attachment_parts(d, attachments, &conversation, &id)
            .await;
        let transcript = voice_note.as_ref().and_then(|note| {
            note.transcript
                .as_ref()
                .filter(|t| !t.text.trim().is_empty())
                .map(|t| t.text.clone())
        });
        let content = if text.is_empty() {
            transcript.unwrap_or_default()
        } else {
            text.clone()
        };
        if content.trim().is_empty() && media_parts.is_empty() && voice_note.is_none() {
            return true;
        }

        let reply_to_id = d
            .get("message_reference")
            .and_then(|reference| str_field(reference, "message_id"))
            .map(str::to_owned);
        let sender = user_ref(author);
        let reply_target = if channel_id.is_empty() {
            author_id.to_owned()
        } else {
            channel_id.to_owned()
        };
        let message = ChannelMessage {
            id: id.clone(),
            sender: author_id.to_owned(),
            reply_target,
            content: content.clone(),
            channel: "discord".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            topic_id: None,
            reply_to_id: reply_to_id.clone(),
            thread_ts: conversation.thread_id.clone(),
            sender_name: sender.name.clone(),
        };

        let mut parts = Vec::new();
        if !text.is_empty() {
            parts.push(ContentPart::Text {
                text: text.clone(),
                markdown_dialect: None,
            });
        }
        if let Some(note) = voice_note {
            parts.push(ContentPart::VoiceNote { voice_note: note });
        }
        parts.extend(media_parts);
        let envelope = ChannelInboundEnvelope {
            channel: channel_ref(),
            message_id: id,
            reply_to_id,
            timestamp_unix_ms: snowflake_unix_ms(message_id),
            conversation,
            sender,
            text: content,
            media: media_references(&parts),
            parts,
            raw: Some(json!({
                "providerMessageId": message_id,
                "channelId": channel_id,
                "guildId": guild_id,
            })),
            ..Default::default()
        };

        if let Some(typed_tx) = typed_tx {
            return typed_tx
                .send(ChannelIngress::Inbound(envelope))
                .await
                .is_ok();
        }
        let delivered = match &self.events {
            Some(events) => match events
                .publish_ingress(ChannelIngress::Inbound(envelope))
                .await
            {
                Ok(delivered) => delivered,
                Err(error) => {
                    tracing::debug!(%error, "[discord] inbound module ingress failed");
                    false
                }
            },
            None => false,
        };
        if delivered {
            true
        } else if let Some(tx) = legacy_tx {
            tx.send(message).await.is_ok()
        } else {
            false
        }
    }

    /// Download and persist a message's attachments.
    ///
    /// Voice messages become a `VoiceNote` (stored, then transcribed); other
    /// attachments become typed media parts. A failed attachment is logged and
    /// skipped so one bad file never drops the rest of the message.
    async fn attachment_parts(
        &self,
        message: &Value,
        attachments: &[Value],
        conversation: &ConversationRef,
        message_id: &str,
    ) -> (Vec<ContentPart>, Option<VoiceNote>) {
        let is_voice_message = message.get("flags").and_then(Value::as_u64).unwrap_or(0)
            & DISCORD_VOICE_MESSAGE_FLAG
            != 0;
        let scope = tinychannels_bus::HostVoiceScope {
            configured_instance: String::new(),
            channel: channel_ref(),
            conversation: conversation.clone(),
            turn_id: message_id.to_owned(),
        };
        let mut parts = Vec::new();
        let mut voice_note = None;
        for attachment in attachments.iter().take(MAX_ATTACHMENTS_PER_MESSAGE) {
            let Some(url) = str_field(attachment, "url") else {
                continue;
            };
            let attachment_id = str_field(attachment, "id").unwrap_or("attachment");
            if is_voice_message && voice_note.is_none() {
                match self.voice_note_from(attachment, url, &scope).await {
                    Ok(note) => {
                        voice_note = Some(note);
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "[discord] voice message could not be stored");
                        continue;
                    }
                }
            }
            match self
                .media_part_from(attachment, url, &scope, message_id, attachment_id)
                .await
            {
                Ok(Some(part)) => parts.push(part),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, attachment_id, "[discord] attachment could not be stored");
                }
            }
        }
        (parts, voice_note)
    }

    async fn voice_note_from(
        &self,
        attachment: &Value,
        url: &str,
        scope: &tinychannels_bus::HostVoiceScope,
    ) -> anyhow::Result<VoiceNote> {
        let store = self
            .voice_media_store
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("host voice media storage is unavailable"))?;
        let (bytes, _) = self.download_attachment(url, MAX_VOICE_BYTES).await?;
        let duration_ms = attachment
            .get("duration_secs")
            .and_then(Value::as_f64)
            .filter(|secs| secs.is_finite() && *secs >= 0.0)
            .map(|secs| (secs * 1000.0).round() as u64);
        let file_name = str_field(attachment, "filename")
            .unwrap_or("voice-message.ogg")
            .to_owned();
        let audio_base64 = BASE64.encode(&bytes);
        let stored = store
            .store_voice(crate::host::VoiceMediaRequest {
                audio_base64: audio_base64.clone(),
                mime_type: "audio/ogg".into(),
                codec: Some("ogg_opus".into()),
                duration_ms,
                file_name: Some(file_name.clone()),
                direction: tinychannels_bus::HostVoiceMediaDirection::Inbound,
                scope: scope.clone(),
            })
            .await?;
        let mut media = stored.media;
        if media
            .opaque_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
        {
            anyhow::bail!("host voice media storage returned no durable opaque reference");
        }
        // Discord CDN URLs are signed and expire; only the host reference persists.
        media.url = None;
        media.path = None;
        media.local_cache_path = None;
        media.kind = MediaKind::Audio;
        media.content_type = Some("audio/ogg".into());
        media.codec = Some("ogg_opus".into());
        let mut note = VoiceNote {
            media,
            codec: Some("ogg_opus".into()),
            duration_ms,
            waveform: str_field(attachment, "waveform").and_then(decode_waveform),
            push_to_talk: true,
            transcript: None,
        };
        let request = crate::host::TranscriptionRequest {
            audio_base64,
            mime_type: Some("audio/ogg".into()),
            file_name: Some(file_name),
            language: None,
        };
        let status = crate::host::enrich_voice_note(
            &mut note,
            scope.clone(),
            Some(request),
            self.transcriber.clone(),
        )
        .await;
        tracing::debug!(?status, "[discord] voice message enriched");
        Ok(note)
    }

    async fn media_part_from(
        &self,
        attachment: &Value,
        url: &str,
        scope: &tinychannels_bus::HostVoiceScope,
        message_id: &str,
        attachment_id: &str,
    ) -> anyhow::Result<Option<ContentPart>> {
        let Some(repository) = self.media_repository.as_ref() else {
            tracing::debug!("[discord] attachment skipped: no host media repository");
            return Ok(None);
        };
        let content_type = normalized_content_type(str_field(attachment, "content_type"));
        let kind = media_kind_for(&content_type);
        let (bytes, _) = self
            .download_attachment(url, MAX_INBOUND_MEDIA_BYTES)
            .await?;
        let file_name = str_field(attachment, "filename").map(str::to_owned);
        let duration_ms = attachment
            .get("duration_secs")
            .and_then(Value::as_f64)
            .filter(|secs| secs.is_finite() && *secs >= 0.0)
            .map(|secs| (secs * 1000.0).round() as u64);
        let media = repository
            .store_media(crate::host::MediaStoreRequest {
                metadata: crate::host::MediaMetadata {
                    scope: scope.clone(),
                    kind,
                    content_type,
                    file_name: file_name.clone(),
                    duration_ms,
                    codec: None,
                    voice_direction: None,
                    dedupe_key: Some(format!("discord:{message_id}:{attachment_id}")),
                    max_bytes: MAX_INBOUND_MEDIA_BYTES,
                },
                bytes,
            })
            .await?;
        let alt_text = str_field(attachment, "description").map(str::to_owned);
        Ok(Some(match kind {
            MediaKind::Image => ContentPart::Image {
                media,
                caption: None,
                alt_text,
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
                file_name,
                caption: None,
            },
        }))
    }

    /// Bounded, unauthenticated download of a Discord CDN attachment.
    pub(super) async fn download_attachment(
        &self,
        url: &str,
        max_bytes: u64,
    ) -> anyhow::Result<(Vec<u8>, Option<String>)> {
        let parsed = reqwest::Url::parse(url)?;
        let custom_base = self.api_base != DISCORD_API_BASE;
        if parsed.scheme() != "https" && !(custom_base && parsed.scheme() == "http") {
            anyhow::bail!("attachment URL must use https");
        }
        let mut response = self.http_client().get(parsed).send().await?;
        if !response.status().is_success() {
            anyhow::bail!("attachment download failed ({})", response.status());
        }
        if response.content_length().is_some_and(|len| len > max_bytes) {
            anyhow::bail!("attachment exceeds the size limit");
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
                anyhow::bail!("attachment exceeds the size limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            anyhow::bail!("attachment body is empty");
        }
        Ok((bytes, content_type))
    }

    /// `MESSAGE_UPDATE`: a user edited a message.
    #[cfg(test)]
    pub(super) async fn handle_message_update(&self, d: &Value, bot_user_id: &str) {
        self.handle_message_update_to(d, bot_user_id, None).await;
    }

    pub(super) async fn handle_message_update_to(
        &self,
        d: &Value,
        bot_user_id: &str,
        typed_tx: Option<&tokio::sync::mpsc::Sender<ChannelIngress>>,
    ) {
        if self.events.is_none() && typed_tx.is_none() {
            return;
        }
        let Some(edited_at) = str_field(d, "edited_timestamp") else {
            return; // embed unfurls and pins arrive without an edit time
        };
        let Some(author) = d.get("author") else {
            return;
        };
        if !self.actor_allowed(author, bot_user_id) {
            return;
        }
        let (Some(message_id), Some(channel_id), Some(content)) = (
            str_field(d, "id"),
            str_field(d, "channel_id"),
            str_field(d, "content"),
        ) else {
            return;
        };
        let guild_id = str_field(d, "guild_id");
        let info = self.channel_info(channel_id, guild_id.is_some()).await;
        if !self.in_scope(guild_id, channel_id, &info) {
            return;
        }
        let source_timestamp = chrono::DateTime::parse_from_rfc3339(edited_at)
            .ok()
            .and_then(|at| u64::try_from(at.timestamp_millis()).ok());
        self.publish_event_to(
            ChannelEvent {
                version: 2,
                event_id: format!("discord:edit:{message_id}:{edited_at}"),
                source_event_id: Some(message_id.to_owned()),
                source_timestamp,
                channel: channel_ref(),
                conversation: Some(Self::conversation_for(channel_id, guild_id, &info)),
                event: ChannelEventKind::MessageUpdated {
                    message_id: format!("discord_{message_id}"),
                    provider_message_id: Some(message_id.to_owned()),
                    parts: vec![ContentPart::Text {
                        text: content.to_owned(),
                        markdown_dialect: None,
                    }],
                },
            },
            typed_tx,
        )
        .await;
    }

    /// `MESSAGE_DELETE`: a message was removed.
    #[cfg(test)]
    pub(super) async fn handle_message_delete(&self, d: &Value) {
        self.handle_message_delete_to(d, None).await;
    }

    pub(super) async fn handle_message_delete_to(
        &self,
        d: &Value,
        typed_tx: Option<&tokio::sync::mpsc::Sender<ChannelIngress>>,
    ) {
        if self.events.is_none() && typed_tx.is_none() {
            return;
        }
        let (Some(message_id), Some(channel_id)) = (str_field(d, "id"), str_field(d, "channel_id"))
        else {
            return;
        };
        let guild_id = str_field(d, "guild_id");
        let info = self.channel_info(channel_id, guild_id.is_some()).await;
        if !self.in_scope(guild_id, channel_id, &info) {
            return;
        }
        self.publish_event_to(
            ChannelEvent {
                version: 2,
                event_id: format!("discord:delete:{message_id}"),
                source_event_id: Some(message_id.to_owned()),
                source_timestamp: None,
                channel: channel_ref(),
                conversation: Some(Self::conversation_for(channel_id, guild_id, &info)),
                event: ChannelEventKind::MessageDeleted {
                    message_id: format!("discord_{message_id}"),
                    provider_message_id: Some(message_id.to_owned()),
                },
            },
            typed_tx,
        )
        .await;
    }

    /// `MESSAGE_REACTION_ADD` / `MESSAGE_REACTION_REMOVE`.
    #[cfg(test)]
    pub(super) async fn handle_reaction(&self, d: &Value, bot_user_id: &str, added: bool) {
        self.handle_reaction_to(d, bot_user_id, added, None).await;
    }

    pub(super) async fn handle_reaction_to(
        &self,
        d: &Value,
        bot_user_id: &str,
        added: bool,
        typed_tx: Option<&tokio::sync::mpsc::Sender<ChannelIngress>>,
    ) {
        if self.events.is_none() && typed_tx.is_none() {
            return;
        }
        let (Some(user_id), Some(message_id), Some(channel_id)) = (
            str_field(d, "user_id"),
            str_field(d, "message_id"),
            str_field(d, "channel_id"),
        ) else {
            return;
        };
        let member_user = d.get("member").and_then(|member| member.get("user"));
        let actor = member_user.map_or_else(
            || json!({"id": user_id}),
            |user| {
                let mut user = user.clone();
                user["id"] = json!(user_id);
                user
            },
        );
        if !self.actor_allowed(&actor, bot_user_id) {
            return;
        }
        let emoji = d.get("emoji").unwrap_or(&Value::Null);
        let Some(name) = str_field(emoji, "name").filter(|name| !name.is_empty()) else {
            return;
        };
        let custom_id = str_field(emoji, "id").map(str::to_owned);
        let guild_id = str_field(d, "guild_id");
        let info = self.channel_info(channel_id, guild_id.is_some()).await;
        if !self.in_scope(guild_id, channel_id, &info) {
            return;
        }
        let reaction = tinychannels_bus::ChannelReaction {
            value: name.to_owned(),
            custom_emoji_id: custom_id.clone(),
        };
        let sender = user_ref(&actor);
        let (verb, kind) = if added {
            (
                "added",
                ChannelEventKind::ReactionAdded {
                    message_id: format!("discord_{message_id}"),
                    provider_message_id: Some(message_id.to_owned()),
                    actor: sender,
                    reaction,
                },
            )
        } else {
            (
                "removed",
                ChannelEventKind::ReactionRemoved {
                    message_id: format!("discord_{message_id}"),
                    provider_message_id: Some(message_id.to_owned()),
                    actor: sender,
                    reaction,
                },
            )
        };
        self.publish_event_to(
            ChannelEvent {
                version: 2,
                event_id: format!(
                    "discord:reaction:{verb}:{channel_id}:{message_id}:{user_id}:{}",
                    custom_id.as_deref().unwrap_or(name)
                ),
                source_event_id: None,
                source_timestamp: None,
                channel: channel_ref(),
                conversation: Some(Self::conversation_for(channel_id, guild_id, &info)),
                event: kind,
            },
            typed_tx,
        )
        .await;
    }

    /// `INTERACTION_CREATE` for a button press: acknowledge it, then publish
    /// the opaque action id for the host to validate.
    #[cfg(test)]
    pub(super) async fn handle_interaction(&self, d: &Value) {
        self.handle_interaction_to(d, None).await;
    }

    pub(super) async fn handle_interaction_to(
        &self,
        d: &Value,
        typed_tx: Option<&tokio::sync::mpsc::Sender<ChannelIngress>>,
    ) {
        if d.get("type").and_then(Value::as_u64) != Some(3) {
            return;
        }
        let (Some(interaction_id), Some(token)) = (str_field(d, "id"), str_field(d, "token"))
        else {
            return;
        };
        if !is_snowflake(interaction_id)
            || !token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return;
        }
        // Deferred update: the click is acknowledged without a visible reply.
        let ack = self
            .http_client()
            .post(format!(
                "{}/interactions/{interaction_id}/{token}/callback",
                self.api_base
            ))
            .json(&json!({"type": 6}))
            .send()
            .await;
        if ack.map(|r| !r.status().is_success()).unwrap_or(true) {
            tracing::debug!("[discord] interaction acknowledgment failed");
        }
        let user = d
            .get("member")
            .and_then(|member| member.get("user"))
            .or_else(|| d.get("user"));
        let Some(user) = user else {
            return;
        };
        if !self.is_user_allowed(str_field(user, "id").unwrap_or_default()) {
            return;
        }
        let Some(action_id) = d
            .get("data")
            .and_then(|data| str_field(data, "custom_id"))
            .filter(|id| !id.is_empty() && id.len() <= 100)
        else {
            return;
        };
        let message = d.get("message");
        let channel_id = message
            .and_then(|m| str_field(m, "channel_id"))
            .or_else(|| str_field(d, "channel_id"));
        let Some(channel_id) = channel_id else {
            return;
        };
        let guild_id = str_field(d, "guild_id");
        let info = self.channel_info(channel_id, guild_id.is_some()).await;
        if !self.in_scope(guild_id, channel_id, &info) {
            return;
        }
        self.publish_event_to(
            ChannelEvent {
                version: 2,
                event_id: format!("discord:interaction:{interaction_id}"),
                source_event_id: Some(interaction_id.to_owned()),
                source_timestamp: None,
                channel: channel_ref(),
                conversation: Some(Self::conversation_for(channel_id, guild_id, &info)),
                event: ChannelEventKind::ButtonCallback {
                    query_id: interaction_id.to_owned(),
                    action_id: action_id.to_owned(),
                    provider_message_id: message
                        .and_then(|m| str_field(m, "id"))
                        .map(str::to_owned),
                    actor: user_ref(user),
                },
            },
            typed_tx,
        )
        .await;
    }
}

fn media_references(parts: &[ContentPart]) -> Vec<tinychannels_bus::MediaReference> {
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

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
