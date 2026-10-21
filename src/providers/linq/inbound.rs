//! Inbound Linq webhooks: authenticate the exact bytes, then publish typed
//! messages (text, images, files, voice notes) on the ordered ingress path.

use super::LinqChannel;
use super::outbound::{LINQ_MAX_FILE_BYTES, LINQ_MAX_VOICE_BYTES};
use crate::WebhookRejected;
use crate::providers::native::inbound::normalize_mime;
use crate::providers::native::inbound::{
    InboundFile, Ingest, Ingested, assemble, media_of, publish,
};
use crate::traits::ChannelMessage;
use serde_json::{Value, json};
use tinychannels_bus::{
    ChannelInboundEnvelope, ChannelRef, ConversationKind, ConversationRef, SenderRef,
    WebhookDeliveryOutcome,
};
use uuid::Uuid;

fn linq_ref() -> ChannelRef {
    ChannelRef {
        id: "linq".into(),
        account_id: None,
    }
}

fn rejected(reason: &str) -> anyhow::Error {
    anyhow::Error::new(WebhookRejected(reason.to_owned()))
}

/// Split the signature header the host forwards: `t=<unix seconds>,v1=<hex>`,
/// built from Linq's `X-Webhook-Timestamp` and `X-Webhook-Signature` headers.
fn split_signature(header: &str) -> Option<(&str, &str)> {
    let mut timestamp = None;
    let mut signature = None;
    for field in header.split(',') {
        match field.trim().split_once('=') {
            Some(("t", value)) => timestamp = Some(value.trim()),
            Some(("v1", value)) => signature = Some(value.trim()),
            _ => {}
        }
    }
    Some((timestamp?, signature?))
}

/// One media part of an inbound message.
struct MediaPart<'a> {
    url: &'a str,
    mime: String,
    name: Option<String>,
    duration_ms: Option<u64>,
    voice: bool,
}

fn media_part(part: &Value) -> Option<MediaPart<'_>> {
    let kind = part.get("type").and_then(Value::as_str)?;
    if !matches!(
        kind,
        "media" | "image" | "audio" | "video" | "file" | "voice" | "voice_memo" | "audio_message"
    ) {
        return None;
    }
    let url = part
        .get("url")
        .or_else(|| part.get("value"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())?;
    let name = part
        .get("filename")
        .or_else(|| part.get("name"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mime = part
        .get("mime_type")
        .and_then(Value::as_str)
        .map(normalize_mime)
        .filter(|mime| !mime.is_empty())
        .unwrap_or_else(|| "application/octet-stream".into());
    let flagged = part
        .get("voice_memo")
        .or_else(|| part.get("is_voice_memo"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let voice = matches!(kind, "voice" | "voice_memo" | "audio_message")
        || (mime.starts_with("audio/")
            && (flagged
                || name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("Audio Message"))));
    Some(MediaPart {
        url,
        mime,
        name,
        duration_ms: part.get("duration_ms").and_then(Value::as_u64),
        voice,
    })
}

impl LinqChannel {
    /// Media comes from Linq's CDN over https; the API origin is also trusted
    /// so a private gateway can serve its own files.
    fn media_url_allowed(&self, url: &str) -> bool {
        url.starts_with("https://") || url.starts_with(&self.api_base)
    }

    /// Download one media part, bounded by `limit` bytes. The URL is
    /// pre-signed, so no credential is sent with it.
    async fn download(&self, url: &str, limit: u64) -> Option<Vec<u8>> {
        if !self.media_url_allowed(url) {
            tracing::warn!("[linq] skipping media from an untrusted origin");
            return None;
        }
        let mut response = self.http_client().get(url).send().await.ok()?;
        if !response.status().is_success() || response.content_length().is_some_and(|n| n > limit) {
            tracing::warn!("[linq] media download refused or failed");
            return None;
        }
        let mut bytes = Vec::new();
        while let Ok(Some(chunk)) = response.chunk().await {
            if bytes.len() as u64 + chunk.len() as u64 > limit {
                tracing::warn!("[linq] media exceeded the size limit");
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        Some(bytes)
    }

    /// Authenticate and ingest one raw webhook POST.
    pub(super) async fn ingest_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> anyhow::Result<WebhookDeliveryOutcome> {
        let Some(secret) = self.webhook_secret.as_deref() else {
            tracing::warn!("[linq] webhook refused: no webhook secret is configured");
            return Err(rejected("Linq webhook secret is not configured"));
        };
        let (timestamp, signature) = signature
            .and_then(split_signature)
            .ok_or_else(|| rejected("Linq webhook signature is missing"))?;
        let text = std::str::from_utf8(body).map_err(|_| rejected("Linq webhook is not UTF-8"))?;
        if !super::verify_linq_signature(secret, text, timestamp, signature) {
            return Err(rejected("Linq webhook signature is invalid"));
        }
        let payload: Value =
            serde_json::from_str(text).map_err(|_| rejected("Linq webhook is not JSON"))?;
        let mut outcome = WebhookDeliveryOutcome::default();
        if payload.get("event_type").and_then(Value::as_str) != Some("message.received") {
            tracing::debug!("[linq] ignoring a non-message webhook event");
            return Ok(outcome);
        }
        let Some(data) = payload.get("data") else {
            return Ok(outcome);
        };
        if data
            .get("is_from_me")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(outcome);
        }
        let Some(from) = data.get("from").and_then(Value::as_str) else {
            return Ok(outcome);
        };
        let sender = if from.starts_with('+') {
            from.to_owned()
        } else {
            format!("+{from}")
        };
        if !self.is_sender_allowed(&sender) {
            tracing::warn!("[linq] ignoring a message from a sender outside the allowlist");
            return Ok(outcome);
        }
        let message = data.get("message").unwrap_or(&Value::Null);
        let message_id = message
            .get("id")
            .or_else(|| payload.get("event_id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| rejected("Linq webhook carries no message id"))?
            .to_owned();
        let dedupe = format!("linq:{message_id}");
        if self.native.has_seen(&dedupe) {
            outcome.duplicates += 1;
            return Ok(outcome);
        }
        let chat_id = data
            .get("chat_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .unwrap_or(&sender)
            .to_owned();
        let conversation = ConversationRef {
            kind: ConversationKind::Dm,
            id: chat_id,
            ..Default::default()
        };
        let parts = message
            .get("parts")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut texts = Vec::new();
        let mut ingested = Ingested::default();
        let ingest = Ingest {
            provider: "linq",
            channel: linq_ref(),
            repository: self.native.media_repository.as_ref(),
            voice_store: self.native.voice_media_store.as_ref(),
            transcriber: self.native.transcriber.as_ref(),
            file_bytes: LINQ_MAX_FILE_BYTES,
            voice_bytes: LINQ_MAX_VOICE_BYTES,
        };
        for (index, part) in parts.iter().enumerate() {
            if part.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(value) = part.get("value").and_then(Value::as_str) {
                    texts.push(value.to_owned());
                }
                continue;
            }
            let Some(media) = media_part(part) else {
                tracing::debug!("[linq] skipping an unsupported part");
                continue;
            };
            let limit = if media.voice {
                LINQ_MAX_VOICE_BYTES
            } else {
                LINQ_MAX_FILE_BYTES
            };
            let Some(bytes) = self.download(media.url, limit).await else {
                continue;
            };
            ingest
                .add(
                    &mut ingested,
                    &conversation,
                    &message_id,
                    InboundFile {
                        bytes,
                        mime: media.mime,
                        name: media.name,
                        duration_ms: media.duration_ms,
                        voice: media.voice,
                        dedupe_key: format!("{message_id}:{index}"),
                        transcript: None,
                    },
                )
                .await;
        }
        let text = texts.join("\n").trim().to_owned();
        let parts = assemble(&text, None, ingested);
        if parts.is_empty() {
            return Ok(outcome);
        }
        let timestamp_unix_ms = payload
            .get("created_at")
            .and_then(Value::as_str)
            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
            .and_then(|at| u64::try_from(at.timestamp_millis()).ok());
        let envelope = ChannelInboundEnvelope {
            channel: linq_ref(),
            message_id: message_id.clone(),
            reply_to_id: None,
            timestamp_unix_ms,
            conversation,
            sender: SenderRef {
                id: sender,
                ..Default::default()
            },
            text,
            media: media_of(&parts),
            parts,
            raw: Some(json!({"providerMessageId": message_id})),
            ..Default::default()
        };
        if !publish("linq", self.native.events.as_ref(), envelope).await {
            // Not remembered, so Linq's redelivery gets another attempt.
            return Err(anyhow::anyhow!("Linq inbound was not accepted by the host"));
        }
        self.native.mark_seen(dedupe);
        outcome.messages += 1;
        Ok(outcome)
    }
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;

impl LinqChannel {
    fn is_sender_allowed(&self, phone: &str) -> bool {
        self.allowed_senders.iter().any(|n| n == "*" || n == phone)
    }

    /// Parse an incoming webhook payload from Linq and extract messages.
    ///
    /// Linq webhook envelope:
    /// ```json
    /// {
    ///   "api_version": "v3",
    ///   "event_type": "message.received",
    ///   "event_id": "...",
    ///   "created_at": "...",
    ///   "trace_id": "...",
    ///   "data": {
    ///     "chat_id": "...",
    ///     "from": "+1...",
    ///     "recipient_phone": "+1...",
    ///     "is_from_me": false,
    ///     "service": "iMessage",
    ///     "message": {
    ///       "id": "...",
    ///       "parts": [{ "type": "text", "value": "..." }]
    ///     }
    ///   }
    /// }
    /// ```
    pub fn parse_webhook_payload(&self, payload: &serde_json::Value) -> Vec<ChannelMessage> {
        let mut messages = Vec::new();

        // Only handle message.received events
        let event_type = payload
            .get("event_type")
            .and_then(|e| e.as_str())
            .unwrap_or("");
        if event_type != "message.received" {
            tracing::debug!("Linq: skipping non-message event: {event_type}");
            return messages;
        }

        let Some(data) = payload.get("data") else {
            return messages;
        };

        // Skip messages sent by the bot itself
        if data
            .get("is_from_me")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            tracing::debug!("Linq: skipping is_from_me message");
            return messages;
        }

        // Get sender phone number
        let Some(from) = data.get("from").and_then(|f| f.as_str()) else {
            return messages;
        };

        // Normalize to E.164 format
        let normalized_from = if from.starts_with('+') {
            from.to_string()
        } else {
            format!("+{from}")
        };

        // Check allowlist
        if !self.is_sender_allowed(&normalized_from) {
            tracing::warn!(
                "Linq: ignoring message from unauthorized sender: {normalized_from}. \
                Add to allowed_senders in config.toml."
            );
            return messages;
        }

        // Get chat_id for reply routing
        let chat_id = data
            .get("chat_id")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();

        // Extract text from message parts
        let Some(message) = data.get("message") else {
            return messages;
        };

        let Some(parts) = message.get("parts").and_then(|p| p.as_array()) else {
            return messages;
        };

        let content_parts: Vec<String> = parts
            .iter()
            .filter_map(|part| {
                let part_type = part.get("type").and_then(|t| t.as_str())?;
                match part_type {
                    "text" => part
                        .get("value")
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string),
                    // Media parts are typed content on the v2 ingress path
                    // (`ingest_webhook`); this text-only parse never encodes
                    // them into the message body.
                    "media" | "image" => {
                        tracing::debug!("Linq: media part carried by typed ingress, not text");
                        None
                    }
                    _ => {
                        tracing::debug!("Linq: skipping {part_type} part");
                        None
                    }
                }
            })
            .collect();

        if content_parts.is_empty() {
            return messages;
        }

        let content = content_parts.join("\n").trim().to_string();

        if content.is_empty() {
            return messages;
        }

        // Get timestamp from created_at or use current time
        let timestamp = payload
            .get("created_at")
            .and_then(|t| t.as_str())
            .and_then(|t| {
                chrono::DateTime::parse_from_rfc3339(t)
                    .ok()
                    .map(|dt| dt.timestamp().cast_unsigned())
            })
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            });

        // Use chat_id as reply_target so replies go to the right conversation
        let reply_target = if chat_id.is_empty() {
            normalized_from.clone()
        } else {
            chat_id
        };

        messages.push(ChannelMessage {
            id: Uuid::new_v4().to_string(),
            reply_target,
            sender: normalized_from,
            content,
            channel: "linq".to_string(),
            timestamp,
            topic_id: None,
            reply_to_id: None,
            thread_ts: None,
            sender_name: None,
        });

        messages
    }
}

/// Verify a Linq webhook signature.
///
/// Linq signs webhooks with HMAC-SHA256 over `"{timestamp}.{body}"`.
/// The signature is sent in `X-Webhook-Signature` (hex-encoded) and the
/// timestamp in `X-Webhook-Timestamp`. Reject timestamps older than 300s.
pub fn verify_linq_signature(secret: &str, body: &str, timestamp: &str, signature: &str) -> bool {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    // Reject stale timestamps (>300s old)
    if let Ok(ts) = timestamp.parse::<i64>() {
        let now = chrono::Utc::now().timestamp();
        if (now - ts).unsigned_abs() > 300 {
            tracing::warn!("Linq: rejecting stale webhook timestamp ({ts}, now={now})");
            return false;
        }
    } else {
        tracing::warn!("Linq: invalid webhook timestamp: {timestamp}");
        return false;
    }

    // Compute HMAC-SHA256 over "{timestamp}.{body}"
    let message = format!("{timestamp}.{body}");
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(message.as_bytes());
    let signature_hex = signature
        .trim()
        .strip_prefix("sha256=")
        .unwrap_or(signature);
    let Ok(provided) = hex::decode(signature_hex.trim()) else {
        tracing::warn!("Linq: invalid webhook signature format");
        return false;
    };

    // Constant-time comparison via HMAC verify.
    mac.verify_slice(&provided).is_ok()
}
