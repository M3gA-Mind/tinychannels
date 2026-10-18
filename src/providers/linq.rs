//! Linq (iMessage, RCS and SMS through the Linq Partner V3 API) on the v2
//! adapter contract.
//!
//! Outbound text, attachments and voice clips go through the REST API.
//! Inbound traffic is a signed webhook: the host forwards each raw POST to
//! [`Channel::deliver_webhook`], which authenticates the exact bytes, downloads
//! media, stores voice notes and publishes on the ordered ingress path.

mod capabilities;
mod inbound;
mod native;

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
use crate::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

/// Linq channel — uses the Linq Partner V3 API for iMessage, RCS, and SMS.
///
/// This channel operates in webhook mode (push-based) rather than polling.
/// The `listen` method here is a keepalive placeholder; inbound delivery depends on
/// your deployment wiring Linq webhooks to the app.
pub struct LinqChannel {
    api_token: String,
    from_phone: String,
    allowed_senders: Vec<String>,
    http_client: reqwest::Client,
    api_base: String,
    /// Signs webhooks (`X-Webhook-Signature`); deliveries are refused without it.
    webhook_secret: Option<String>,
    native: NativeState,
}

const LINQ_API_BASE: &str = "https://api.linqapp.com/api/partner/v3";

impl LinqChannel {
    pub fn new(api_token: String, from_phone: String, allowed_senders: Vec<String>) -> Self {
        Self {
            api_token,
            from_phone,
            allowed_senders,
            http_client: reqwest::Client::new(),
            api_base: LINQ_API_BASE.to_owned(),
            webhook_secret: None,
            native: NativeState::default(),
        }
    }

    pub fn with_http_client(
        api_token: String,
        from_phone: String,
        allowed_senders: Vec<String>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            api_token,
            from_phone,
            allowed_senders,
            http_client,
            api_base: LINQ_API_BASE.to_owned(),
            webhook_secret: None,
            native: NativeState::default(),
        }
    }

    /// The secret Linq signs webhook deliveries with.
    pub fn with_webhook_secret(mut self, secret: Option<String>) -> Self {
        self.webhook_secret = secret.filter(|secret| !secret.trim().is_empty());
        self
    }

    /// Point the client at another API origin (tests and private gateways).
    pub fn with_api_base(mut self, base: impl Into<String>) -> Self {
        self.api_base = base.into().trim_end_matches('/').to_owned();
        self
    }

    /// Scoped media storage; enables typed attachments in both directions.
    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Durable voice storage; enables voice notes in both directions.
    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Host speech-to-text for inbound voice notes.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    /// Enable shared final-turn synthesis when the host provides TTS, a codec
    /// and durable voice storage.
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.native.voice_synthesis = enabled;
        self
    }

    /// Ordered ingress for typed inbound messages.
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    /// Check if a sender phone number is allowed (E.164 format: +1234567890)
    fn is_sender_allowed(&self, phone: &str) -> bool {
        self.allowed_senders.iter().any(|n| n == "*" || n == phone)
    }

    /// Get the bot's phone number
    pub fn phone_number(&self) -> &str {
        &self.from_phone
    }

    fn media_part_to_image_marker(part: &serde_json::Value) -> Option<String> {
        let source = part
            .get("url")
            .or_else(|| part.get("value"))
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())?;

        let mime_type = part
            .get("mime_type")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .unwrap_or_default()
            .to_ascii_lowercase();

        if !mime_type.starts_with("image/") {
            return None;
        }

        Some(format!("[IMAGE:{source}]"))
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
                    "media" | "image" => {
                        if let Some(marker) = Self::media_part_to_image_marker(part) {
                            Some(marker)
                        } else {
                            tracing::debug!("Linq: skipping unsupported {part_type} part");
                            None
                        }
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

#[async_trait]
impl Channel for LinqChannel {
    fn name(&self) -> &str {
        "linq"
    }

    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.declared()
    }

    async fn send_typed_with_receipt(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<crate::channel::ChannelSendOutcome> {
        crate::providers::native::outbound::send_typed(self, message)
            .await
            .map_err(crate::providers::native::to_anyhow)
    }

    async fn deliver_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
        self.ingest_webhook(signature, body).await
    }

    fn supports_remote_control(&self) -> bool {
        true
    }
    fn supports_chat_approvals(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        // If reply_target looks like a chat_id, send to existing chat.
        // Otherwise create a new chat with the recipient phone number.
        let recipient = &message.recipient;

        let body = serde_json::json!({
            "message": {
                "parts": [{
                    "type": "text",
                    "value": message.content
                }]
            }
        });

        // Try sending to existing chat (recipient is chat_id)
        let url = format!("{}/chats/{recipient}/messages", self.api_base);

        let resp = self
            .http_client()
            .post(&url)
            .bearer_auth(&self.api_token)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if resp.status().is_success() {
            return Ok(());
        }

        // If the chat_id-based send failed with 404, try creating a new chat
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            let new_chat_body = serde_json::json!({
                "from": self.from_phone,
                "to": [recipient],
                "message": {
                    "parts": [{
                        "type": "text",
                        "value": message.content
                    }]
                }
            });

            let create_resp = self
                .http_client()
                .post(format!("{}/chats", self.api_base))
                .bearer_auth(&self.api_token)
                .header("Content-Type", "application/json")
                .json(&new_chat_body)
                .send()
                .await?;

            if !create_resp.status().is_success() {
                let status = create_resp.status();
                let error_body = create_resp.text().await.unwrap_or_default();
                tracing::error!("Linq create chat failed: {status} — {error_body}");
                anyhow::bail!("Linq API error: {status}");
            }

            return Ok(());
        }

        let status = resp.status();
        let error_body = resp.text().await.unwrap_or_default();
        tracing::error!("Linq send failed: {status} — {error_body}");
        anyhow::bail!("Linq API error: {status}");
    }

    async fn listen(&self, _tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        // Linq uses webhooks (push-based), not polling.
        tracing::info!(
            "Linq channel active (webhook mode). \
            Configure Linq to POST webhook events to your deployed HTTPS webhook URL."
        );

        // Keep the task alive — it will be cancelled when the channel shuts down
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }

    async fn health_check(&self) -> bool {
        // Check if we can reach the Linq API
        let url = format!("{}/phonenumbers", self.api_base);

        self.http_client()
            .get(&url)
            .bearer_auth(&self.api_token)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        let url = format!("{}/chats/{recipient}/typing", self.api_base);

        let resp = self
            .http_client()
            .post(&url)
            .bearer_auth(&self.api_token)
            .send()
            .await?;

        if !resp.status().is_success() {
            tracing::debug!("Linq start_typing failed: {}", resp.status());
        }

        Ok(())
    }

    async fn stop_typing(&self, recipient: &str) -> anyhow::Result<()> {
        let url = format!("{}/chats/{recipient}/typing", self.api_base);

        let resp = self
            .http_client()
            .delete(&url)
            .bearer_auth(&self.api_token)
            .send()
            .await?;

        if !resp.status().is_success() {
            tracing::debug!("Linq stop_typing failed: {}", resp.status());
        }

        Ok(())
    }
}

crate::providers::native::native_v2_adapter!(LinqChannel);

#[cfg(test)]
#[path = "linq/fixtures_tests.rs"]
mod fixtures;

#[cfg(test)]
#[path = "linq/conformance_tests.rs"]
mod conformance;

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

#[cfg(test)]
#[path = "linq_tests.rs"]
mod tests;
