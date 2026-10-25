//! Linq as a [`NativeProvider`]: text and attachment sends, voice clips as
//! audio attachments and typing indicators over the Partner V3 REST API.

use super::LinqChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{
    FileStep, Limits, NativeProvider, Sent, Step, Target, pre_dispatch,
};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde_json::{Value, json};
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};

/// Stay inside the 16 MiB bus frame once base64 encoded.
pub(crate) const LINQ_MAX_FILE_BYTES: u64 = 12 * 1024 * 1024;
/// Voice bytes cross TinyBus inline, so they stay below the frame cap.
pub(crate) const LINQ_MAX_VOICE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const LINQ_TEXT_CHUNK_CHARS: usize = 4000;

/// A transport failure: the request may or may not have been processed.
fn uncertain(operation: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Transient,
        message: format!("Linq {operation} outcome is uncertain"),
        sent_before_error: true,
        ..Default::default()
    }
}

/// Map an HTTP status onto the typed error. Messages never carry response
/// bodies, numbers or tokens.
pub(crate) fn status_error(status: StatusCode, operation: &str) -> ChannelSendError {
    let (kind, retryable, message) = match status.as_u16() {
        429 => (
            SendErrorKind::RateLimited,
            true,
            format!("Linq {operation} was rate limited"),
        ),
        401 | 403 => (
            SendErrorKind::Forbidden,
            false,
            format!("Linq {operation} was not authorized"),
        ),
        404 => (
            SendErrorKind::NotFound,
            false,
            format!("Linq {operation} target was not found"),
        ),
        400 | 413 | 415 | 422 => (
            SendErrorKind::BadFormat,
            false,
            format!("Linq {operation} was rejected"),
        ),
        500..=599 => (
            SendErrorKind::Transient,
            true,
            format!("Linq {operation} failed upstream"),
        ),
        _ => (
            SendErrorKind::Unknown,
            false,
            format!("Linq {operation} failed"),
        ),
    };
    ChannelSendError {
        kind,
        message,
        retryable,
        ..Default::default()
    }
}

/// The id Linq issued for a created message, wherever the response puts it.
fn message_id(response: &Value) -> Option<String> {
    ["/message/id", "/chat/message/id", "/id"]
        .iter()
        .find_map(|pointer| response.pointer(pointer).and_then(Value::as_str))
        .filter(|id| !id.trim().is_empty())
        .map(str::to_owned)
}

/// A chat id or a phone number: nothing that could alter the request path.
fn checked_conversation(conversation: &str) -> Result<&str, ChannelSendError> {
    let valid = !conversation.is_empty()
        && conversation.len() <= 128
        && conversation
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '+' | '.'))
        && !conversation.contains("..");
    if valid {
        Ok(conversation)
    } else {
        Err(pre_dispatch(
            "Linq conversation id is not a chat id or phone number",
        ))
    }
}

impl LinqChannel {
    async fn post_json(
        &self,
        path: &str,
        body: &Value,
        operation: &str,
    ) -> Result<Value, ChannelSendError> {
        let response = self
            .http_client()
            .post(format!("{}{path}", self.api_base))
            .bearer_auth(&self.api_token)
            .json(body)
            .send()
            .await
            .map_err(|_| uncertain(operation))?;
        let status = response.status();
        if !status.is_success() {
            tracing::warn!(operation, status = status.as_u16(), "[linq] request failed");
            return Err(status_error(status, operation));
        }
        response
            .json::<Value>()
            .await
            .map_err(|_| uncertain(operation))
    }

    /// Upload `file` and return the attachment id Linq issued for it.
    async fn upload(&self, file: &FileStep) -> Result<String, ChannelSendError> {
        let created = self
            .post_json(
                "/attachments",
                &json!({
                    "filename": file.name,
                    "content_type": file.mime,
                    "size_bytes": file.bytes.len(),
                }),
                "attachment upload",
            )
            .await
            .map_err(|error| ChannelSendError {
                // Nothing reached a chat yet, so a retry cannot duplicate.
                sent_before_error: false,
                ..error
            })?;
        let (Some(id), Some(url)) = (
            created.get("attachment_id").and_then(Value::as_str),
            created.get("upload_url").and_then(Value::as_str),
        ) else {
            return Err(pre_dispatch("Linq attachment upload returned no target"));
        };
        let mut request = self
            .http_client()
            .put(url)
            .header("Content-Type", &file.mime)
            .body(file.bytes.clone());
        if let Some(headers) = created.get("required_headers").and_then(Value::as_object) {
            for (name, value) in headers {
                if let Some(value) = value.as_str() {
                    request = request.header(name.as_str(), value);
                }
            }
        }
        let response = request.send().await.map_err(|_| ChannelSendError {
            sent_before_error: false,
            ..uncertain("attachment upload")
        })?;
        if !response.status().is_success() {
            return Err(status_error(response.status(), "attachment upload"));
        }
        Ok(id.to_owned())
    }

    /// Send `parts` to `conversation`: an existing chat id, or a phone number
    /// a new chat is opened with.
    async fn send_parts(
        &self,
        conversation: &str,
        parts: Vec<Value>,
        operation: &str,
    ) -> Result<String, ChannelSendError> {
        let message = json!({"parts": parts});
        let existing = self
            .post_json(
                &format!("/chats/{conversation}/messages"),
                &json!({"message": message}),
                operation,
            )
            .await;
        let response = match existing {
            Err(error)
                if error.kind == SendErrorKind::NotFound && conversation.starts_with('+') =>
            {
                self.post_json(
                    "/chats",
                    &json!({"from": self.from_phone, "to": [conversation], "message": message}),
                    operation,
                )
                .await?
            }
            other => other?,
        };
        message_id(&response).ok_or_else(|| ChannelSendError {
            kind: SendErrorKind::Unknown,
            message: format!("Linq {operation} returned no message id"),
            sent_before_error: true,
            ..Default::default()
        })
    }

    async fn typing(&self, conversation: &str, on: bool) -> Result<(), ChannelSendError> {
        let url = format!("{}/chats/{conversation}/typing", self.api_base);
        let request = if on {
            self.http_client().post(url)
        } else {
            self.http_client().delete(url)
        };
        let response = request
            .bearer_auth(&self.api_token)
            .send()
            .await
            .map_err(|_| uncertain("typing"))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(status_error(response.status(), "typing"))
        }
    }
}

#[async_trait]
impl NativeProvider for LinqChannel {
    fn id(&self) -> &'static str {
        "linq"
    }

    fn display(&self) -> &'static str {
        "Linq"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: LINQ_TEXT_CHUNK_CHARS,
            file_bytes: LINQ_MAX_FILE_BYTES,
            voice_bytes: LINQ_MAX_VOICE_BYTES,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.native.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.native.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        self.native.voice_synthesis
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    fn supports_webhook_delivery(&self) -> bool {
        true
    }

    async fn health_state(&self) -> &'static str {
        if self.health_probe().await {
            "healthy"
        } else {
            "unhealthy"
        }
    }

    async fn deliver_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
        self.ingest_webhook(signature, body).await
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        vec![("voiceOut", "attachment")]
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        let conversation = checked_conversation(&target.conversation.id)?;
        let (parts, kind, operation) = match step {
            Step::Text(text) => (
                vec![json!({"type": "text", "value": text})],
                MessageReceiptPartKind::Text,
                "send",
            ),
            Step::File(file) | Step::Voice(file) => {
                let attachment = self.upload(file).await?;
                let mut parts = Vec::new();
                if let Some(caption) = file.caption.as_deref().filter(|c| !c.trim().is_empty()) {
                    parts.push(json!({"type": "text", "value": caption}));
                }
                parts.push(json!({"type": "media", "attachment_id": attachment}));
                let kind = if matches!(step, Step::Voice(_)) {
                    MessageReceiptPartKind::Voice
                } else {
                    MessageReceiptPartKind::Media
                };
                (parts, kind, "attachment send")
            }
        };
        let id = self.send_parts(conversation, parts, operation).await?;
        Ok(vec![Sent::new(id, kind)])
    }

    async fn set_typing(&self, conversation: &str, on: bool) -> Result<(), ChannelSendError> {
        self.typing(checked_conversation(conversation)?, on).await
    }
}

#[cfg(test)]
#[path = "outbound/tests/mod.rs"]
mod tests;
