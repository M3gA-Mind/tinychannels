//! Telegram channel — outbound text chunking, native media uploads, and reaction sending.

use super::channel_types::TelegramChannel;

#[derive(Debug)]
pub(crate) struct PartialTextSendError {
    pub(crate) message_ids: Vec<String>,
    pub(crate) sent_before_error: bool,
    pub(crate) error: tinychannels_bus::channel::ChannelSendError,
}

impl std::fmt::Display for PartialTextSendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.error.message)
    }
}

impl std::error::Error for PartialTextSendError {}
use super::text::{markdown_v2_text, split_message_for_telegram};
use reqwest::multipart::{Form, Part};
use std::path::Path;
use std::time::Duration;

pub(crate) struct TelegramVoiceUpload<'a> {
    pub(crate) method: &'a str,
    pub(crate) field: &'a str,
    pub(crate) chat_id: &'a str,
    pub(crate) thread_id: Option<&'a str>,
    pub(crate) reply_to_id: Option<&'a str>,
    pub(crate) caption: Option<&'a str>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) file_name: &'a str,
}

pub(crate) struct TelegramMediaUpload<'a> {
    pub(crate) method: &'a str,
    pub(crate) field: &'a str,
    pub(crate) chat_id: &'a str,
    pub(crate) thread_id: Option<&'a str>,
    pub(crate) reply_to_id: Option<&'a str>,
    pub(crate) caption: Option<&'a str>,
    pub(crate) title: Option<&'a str>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) content_type: &'a str,
    pub(crate) file_name: &'a str,
}

impl TelegramChannel {
    pub(crate) async fn send_media_bytes_with_id(
        &self,
        upload: TelegramMediaUpload<'_>,
    ) -> anyhow::Result<String> {
        if upload.bytes.is_empty() || upload.bytes.len() > 20 * 1024 * 1024 {
            anyhow::bail!("media upload exceeds the supported inline limit");
        }
        let part = Part::bytes(upload.bytes)
            .file_name(upload.file_name.to_owned())
            .mime_str(upload.content_type)?;
        let mut form = Form::new()
            .text("chat_id", upload.chat_id.to_owned())
            .part(upload.field.to_owned(), part);
        if let Some(thread) = upload.thread_id {
            form = form.text("message_thread_id", thread.to_owned());
        }
        if let Some(reply_to) = upload.reply_to_id {
            let message_id = reply_to
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Telegram reply target is invalid"))?;
            form = form.text(
                "reply_parameters",
                serde_json::json!({"message_id": message_id}).to_string(),
            );
        }
        if let Some(caption) = upload.caption {
            form = form.text("caption", caption.to_owned());
        }
        if let Some(title) = upload.title {
            form = form.text("title", title.to_owned());
        }
        if let Some(duration_ms) = upload.duration_ms {
            form = form.text(
                "duration",
                duration_ms
                    .saturating_add(999)
                    .saturating_div(1000)
                    .to_string(),
            );
        }
        let response = self
            .http_client()
            .post(self.api_url(upload.method))
            .multipart(form)
            .send()
            .await?;
        let status = response.status();
        let body: serde_json::Value = response.json().await?;
        if !status.is_success() || body.get("ok").and_then(serde_json::Value::as_bool) != Some(true)
        {
            anyhow::bail!(
                "Telegram {} returned an unsuccessful or unknown receipt",
                upload.method
            );
        }
        body.get("result")
            .and_then(|result| result.get("message_id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Telegram {} response omitted message receipt",
                    upload.method
                )
            })
    }

    pub(crate) async fn send_voice_bytes_with_id(
        &self,
        upload: TelegramVoiceUpload<'_>,
    ) -> anyhow::Result<String> {
        if upload.bytes.len() > 8 * 1024 * 1024 {
            anyhow::bail!("voice upload exceeds the supported inline limit");
        }
        let part = Part::bytes(upload.bytes).file_name(upload.file_name.to_owned());
        let mut form = Form::new()
            .text("chat_id", upload.chat_id.to_owned())
            .part(upload.field.to_owned(), part);
        if let Some(thread) = upload.thread_id {
            form = form.text("message_thread_id", thread.to_owned());
        }
        if let Some(reply_to) = upload.reply_to_id {
            let message_id = reply_to
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Telegram reply target is invalid"))?;
            form = form.text(
                "reply_parameters",
                serde_json::json!({"message_id": message_id}).to_string(),
            );
        }
        if let Some(caption) = upload.caption {
            form = form.text("caption", caption.to_owned());
        }
        let response = self
            .http_client()
            .post(self.api_url(upload.method))
            .multipart(form)
            .send()
            .await?;
        let status = response.status();
        let body: serde_json::Value = response.json().await?;
        if !status.is_success() || body.get("ok").and_then(serde_json::Value::as_bool) != Some(true)
        {
            anyhow::bail!(
                "Telegram {} returned an unsuccessful or unknown receipt",
                upload.method
            );
        }
        body.get("result")
            .and_then(|result| result.get("message_id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Telegram {} response omitted message receipt",
                    upload.method
                )
            })
    }

    pub(crate) async fn send_message_reaction(
        &self,
        chat_id: &str,
        message_id: i64,
        emoji: &str,
    ) -> anyhow::Result<bool> {
        let emoji = emoji.trim();
        if emoji.is_empty() {
            return Ok(false);
        }

        self.send_message_reaction_type(
            chat_id,
            message_id,
            serde_json::json!({ "type": "emoji", "emoji": emoji }),
            emoji,
        )
        .await
    }

    pub(crate) async fn send_message_custom_reaction(
        &self,
        chat_id: &str,
        message_id: i64,
        custom_emoji_id: &str,
    ) -> anyhow::Result<bool> {
        let custom_emoji_id = custom_emoji_id.trim();
        if custom_emoji_id.is_empty() {
            return Ok(false);
        }

        self.send_message_reaction_type(
            chat_id,
            message_id,
            serde_json::json!({ "type": "custom_emoji", "custom_emoji_id": custom_emoji_id }),
            custom_emoji_id,
        )
        .await
    }

    async fn send_message_reaction_type(
        &self,
        chat_id: &str,
        message_id: i64,
        reaction: serde_json::Value,
        log_value: &str,
    ) -> anyhow::Result<bool> {
        let body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "reaction": [reaction],
            "is_big": false
        });
        let resp = self
            .http_client()
            .post(self.api_url("setMessageReaction"))
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let response = resp.json::<serde_json::Value>().await?;
        if status.is_success() && super::channel_ops::response_status_ok(&response) {
            self.publish_reaction_sent(chat_id, message_id, log_value, true)
                .await;
            tracing::info!(
                chat_id,
                message_id,
                reaction = log_value,
                "Telegram reaction sent"
            );
            return Ok(true);
        }

        tracing::warn!(
            chat_id,
            message_id,
            reaction = log_value,
            status = ?status,
            "Telegram reaction not applied; continuing without failure"
        );
        self.publish_reaction_sent(chat_id, message_id, log_value, false)
            .await;
        Ok(false)
    }

    /// Best-effort fan-out of a reaction-sent event onto the host event sink.
    /// No-op when no [`crate::host::EventSink`] is injected.
    async fn publish_reaction_sent(
        &self,
        chat_id: &str,
        message_id: i64,
        emoji: &str,
        success: bool,
    ) {
        if let Some(events) = &self.events {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let event = tinychannels_bus::ChannelEvent {
                version: 2,
                event_id: format!("telegram:outbound-reaction:{}", uuid::Uuid::new_v4()),
                source_event_id: None,
                source_timestamp: Some(timestamp),
                channel: tinychannels_bus::ChannelRef {
                    id: "telegram".into(),
                    account_id: None,
                },
                conversation: Some(tinychannels_bus::ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Unknown,
                    id: chat_id.to_owned(),
                    ..Default::default()
                }),
                event: tinychannels_bus::ChannelEventKind::OutboundReaction {
                    message_id: format!("telegram_{chat_id}_{message_id}"),
                    provider_message_id: Some(message_id.to_string()),
                    reaction: tinychannels_bus::ChannelReaction {
                        value: emoji.trim().to_owned(),
                        custom_emoji_id: None,
                    },
                    success,
                },
            };
            if let Err(error) = events.publish_channel_event(event).await {
                tracing::debug!(%error, "failed to deliver Telegram outbound reaction event");
            }
        }
    }

    pub(crate) async fn send_text_chunks(
        &self,
        message: &str,
        chat_id: &str,
        thread_id: Option<&str>,
        reply_to_message_id: Option<i64>,
    ) -> anyhow::Result<()> {
        self.send_text_chunks_with_ids(message, chat_id, thread_id, reply_to_message_id)
            .await
            .map(|_| ())
    }

    /// Send text and retain only message IDs returned by Telegram itself.
    pub(crate) async fn send_text_chunks_with_ids(
        &self,
        message: &str,
        chat_id: &str,
        thread_id: Option<&str>,
        reply_to_message_id: Option<i64>,
    ) -> anyhow::Result<Vec<String>> {
        let chunks = split_message_for_telegram(message);
        let mut message_ids = Vec::new();

        for (index, chunk) in chunks.iter().enumerate() {
            let text = chunk.to_string();

            let mut markdown_body = serde_json::json!({
                "chat_id": chat_id,
                "text": markdown_v2_text(&text),
                "parse_mode": "MarkdownV2"
            });

            // Add message_thread_id for forum topic support
            if let Some(tid) = thread_id {
                markdown_body["message_thread_id"] = serde_json::Value::String(tid.to_string());
            }
            if index == 0
                && let Some(parent_id) = reply_to_message_id
            {
                markdown_body["reply_to_message_id"] = serde_json::Value::from(parent_id);
            }

            let markdown_resp = self
                .http_client()
                .post(self.api_url("sendMessage"))
                .json(&markdown_body)
                .send()
                .await
                .map_err(|error| {
                    tracing::debug!("Telegram sendMessage transport failed");
                    let definitely_not_sent = error.is_connect();
                    partial_text_send_error(
                        message_ids.clone(),
                        "Telegram sendMessage request failed".into(),
                        true,
                        !definitely_not_sent,
                    )
                })?;

            if markdown_resp.status().is_success() {
                let response = markdown_resp.json::<serde_json::Value>().await.ok();
                let Some(id) = Self::telegram_result_message_id(response) else {
                    return Err(partial_text_send_error(
                        message_ids,
                        "Telegram accepted send without a usable message ID".into(),
                        false,
                        true,
                    ));
                };
                message_ids.push(id);
                if index < chunks.len() - 1 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                continue;
            }

            let markdown_status = markdown_resp.status();
            let markdown_body = markdown_resp.bytes().await.unwrap_or_default();
            if !is_markdown_parse_rejection(markdown_status, &markdown_body) {
                return Err(partial_text_send_error(
                    message_ids,
                    format!(
                        "Telegram sendMessage rejected with HTTP {}",
                        markdown_status.as_u16()
                    ),
                    markdown_status.is_server_error() || markdown_status.as_u16() == 429,
                    markdown_status.is_server_error(),
                ));
            }
            tracing::warn!(
                status = ?markdown_status,
                "Telegram rejected MarkdownV2 formatting; retrying once without parse_mode"
            );

            let mut plain_body = serde_json::json!({
                "chat_id": chat_id,
                "text": text,
            });

            // Add message_thread_id for forum topic support
            if let Some(tid) = thread_id {
                plain_body["message_thread_id"] = serde_json::Value::String(tid.to_string());
            }
            if index == 0
                && let Some(parent_id) = reply_to_message_id
            {
                plain_body["reply_to_message_id"] = serde_json::Value::from(parent_id);
            }
            let plain_resp = self
                .http_client()
                .post(self.api_url("sendMessage"))
                .json(&plain_body)
                .send()
                .await
                .map_err(|error| {
                    tracing::debug!("Telegram sendMessage fallback transport failed");
                    let definitely_not_sent = error.is_connect();
                    partial_text_send_error(
                        message_ids.clone(),
                        "Telegram sendMessage request failed".into(),
                        true,
                        !definitely_not_sent,
                    )
                })?;

            if !plain_resp.status().is_success() {
                let plain_status = plain_resp.status();
                let _ = plain_resp.bytes().await;
                return Err(partial_text_send_error(
                    message_ids,
                    format!(
                        "Telegram sendMessage rejected with HTTP {}",
                        plain_status.as_u16()
                    ),
                    plain_status.is_server_error() || plain_status.as_u16() == 429,
                    plain_status.is_server_error(),
                ));
            }

            let response = plain_resp.json::<serde_json::Value>().await.ok();
            let Some(id) = Self::telegram_result_message_id(response) else {
                return Err(partial_text_send_error(
                    message_ids,
                    "Telegram accepted send without a usable message ID".into(),
                    false,
                    true,
                ));
            };
            message_ids.push(id);

            if index < chunks.len() - 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        Ok(message_ids)
    }

    fn telegram_result_message_id(response: Option<serde_json::Value>) -> Option<String> {
        response?
            .get("result")?
            .get("message_id")?
            .as_i64()
            .map(|id| id.to_string())
    }

    async fn send_media_by_url(
        &self,
        method: &str,
        media_field: &str,
        chat_id: &str,
        thread_id: Option<&str>,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "chat_id": chat_id,
        });
        body[media_field] = serde_json::Value::String(url.to_string());

        if let Some(tid) = thread_id {
            body["message_thread_id"] = serde_json::Value::String(tid.to_string());
        }

        if let Some(cap) = caption {
            body["caption"] = serde_json::Value::String(cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url(method))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram {method} by URL failed: {err}");
        }

        tracing::info!("Telegram {method} sent to {chat_id}: {url}");
        Ok(())
    }

    /// Send a document/file to a Telegram chat
    pub async fn send_document(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", part);

        if let Some(tid) = thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendDocument"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendDocument failed: {err}");
        }

        tracing::info!("Telegram document sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a document from bytes (in-memory) to a Telegram chat
    pub async fn send_document_bytes(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        file_bytes: Vec<u8>,
        file_name: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", part);

        if let Some(tid) = thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendDocument"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendDocument failed: {err}");
        }

        tracing::info!("Telegram document sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a photo to a Telegram chat
    pub async fn send_photo(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("photo.jpg");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("photo", part);

        if let Some(tid) = thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendPhoto"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendPhoto failed: {err}");
        }

        tracing::info!("Telegram photo sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a photo from bytes (in-memory) to a Telegram chat
    pub async fn send_photo_bytes(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        file_bytes: Vec<u8>,
        file_name: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("photo", part);

        if let Some(tid) = thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendPhoto"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendPhoto failed: {err}");
        }

        tracing::info!("Telegram photo sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a video to a Telegram chat
    pub async fn send_video(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("video.mp4");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("video", part);

        if let Some(tid) = thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendVideo"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendVideo failed: {err}");
        }

        tracing::info!("Telegram video sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send an audio file to a Telegram chat
    pub async fn send_audio(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("audio.mp3");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("audio", part);

        if let Some(tid) = thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendAudio"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendAudio failed: {err}");
        }

        tracing::info!("Telegram audio sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a voice message to a Telegram chat
    pub async fn send_voice(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        file_path: &Path,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("voice.ogg");

        let file_bytes = tokio::fs::read(file_path).await?;
        let part = Part::bytes(file_bytes).file_name(file_name.to_string());

        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part("voice", part);

        if let Some(tid) = thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        if let Some(cap) = caption {
            form = form.text("caption", cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendVoice"))
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendVoice failed: {err}");
        }

        tracing::info!("Telegram voice sent to {chat_id}: {file_name}");
        Ok(())
    }

    /// Send a file by URL (Telegram will download it)
    pub async fn send_document_by_url(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "document": url
        });

        if let Some(tid) = thread_id {
            body["message_thread_id"] = serde_json::Value::String(tid.to_string());
        }

        if let Some(cap) = caption {
            body["caption"] = serde_json::Value::String(cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendDocument"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendDocument by URL failed: {err}");
        }

        tracing::info!("Telegram document (URL) sent to {chat_id}: {url}");
        Ok(())
    }

    /// Send a photo by URL (Telegram will download it)
    pub async fn send_photo_by_url(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "photo": url
        });

        if let Some(tid) = thread_id {
            body["message_thread_id"] = serde_json::Value::String(tid.to_string());
        }

        if let Some(cap) = caption {
            body["caption"] = serde_json::Value::String(cap.to_string());
        }

        let resp = self
            .http_client()
            .post(self.api_url("sendPhoto"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await?;
            anyhow::bail!("Telegram sendPhoto by URL failed: {err}");
        }

        tracing::info!("Telegram photo (URL) sent to {chat_id}: {url}");
        Ok(())
    }

    /// Send a video by URL (Telegram will download it)
    pub async fn send_video_by_url(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        self.send_media_by_url("sendVideo", "video", chat_id, thread_id, url, caption)
            .await
    }

    /// Send an audio file by URL (Telegram will download it)
    pub async fn send_audio_by_url(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        self.send_media_by_url("sendAudio", "audio", chat_id, thread_id, url, caption)
            .await
    }

    /// Send a voice message by URL (Telegram will download it)
    pub async fn send_voice_by_url(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        url: &str,
        caption: Option<&str>,
    ) -> anyhow::Result<()> {
        self.send_media_by_url("sendVoice", "voice", chat_id, thread_id, url, caption)
            .await
    }
}

fn is_markdown_parse_rejection(status: reqwest::StatusCode, body: &[u8]) -> bool {
    if status != reqwest::StatusCode::BAD_REQUEST {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    let Some(description) = value.get("description").and_then(serde_json::Value::as_str) else {
        return false;
    };
    let description = description.to_ascii_lowercase();
    description.contains("can't parse entities")
        || description.contains("can't find end of the entity")
        || description.contains("entity beginning")
}

pub(crate) fn partial_text_send_error(
    message_ids: Vec<String>,
    message: String,
    retryable: bool,
    sent_before_error: bool,
) -> anyhow::Error {
    let sent_before_error = sent_before_error || !message_ids.is_empty();
    anyhow::Error::new(PartialTextSendError {
        sent_before_error,
        error: tinychannels_bus::channel::ChannelSendError {
            kind: if retryable {
                tinychannels_bus::channel::SendErrorKind::Transient
            } else {
                tinychannels_bus::channel::SendErrorKind::Unknown
            },
            message,
            retryable,
            sent_before_error,
            continuation_message_ids: message_ids.clone(),
            ..Default::default()
        },
        message_ids,
    })
}

#[cfg(test)]
#[path = "channel_send_tests.rs"]
mod tests;
