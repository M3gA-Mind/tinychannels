use super::{api, ingress, outbound};

use crate::providers::whatsapp_shared::error::WaFailure;
use crate::providers::whatsapp_shared::window::{BoundedMap, SeenSet};
use crate::traits::ChannelMessage;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;
use uuid::Uuid;

/// Refuse to transmit credentials over anything but HTTPS.
pub(super) fn ensure_https(url: &str) -> anyhow::Result<()> {
    if !url.starts_with("https://") {
        anyhow::bail!(
            "Refusing to transmit sensitive data over non-HTTPS URL: URL scheme must be https"
        );
    }
    Ok(())
}

/// `WhatsApp` channel that uses the `WhatsApp` Business Cloud API.
///
/// Native v2 adapter. Outbound goes to the Graph API (text, media, voice,
/// location, contacts, reactions, read receipts, typing). Inbound is
/// webhook-driven: the host forwards each raw POST to the typed
/// [`ChannelAdapter::deliver_webhook`](crate::channel::ChannelAdapter::deliver_webhook), which authenticates it with the Meta app
/// secret before anything is parsed. [`ChannelAdapter::start`](crate::channel::ChannelAdapter::start) only parks.
///
/// # Runtime Negotiation
///
/// This Cloud API channel is automatically selected when `phone_number_id` is set in the config.
/// Use `WhatsAppWebChannel` (with `session_path`) for native Web mode.
pub struct WhatsAppChannel {
    pub(super) access_token: String,
    pub(super) endpoint_id: String,
    pub(super) verify_token: String,
    /// Meta app secret; the key of the `X-Hub-Signature-256` webhook MAC.
    /// Without it no webhook POST is accepted.
    pub(super) app_secret: Option<String>,
    pub(super) allowed_numbers: Vec<String>,
    pub(super) http_client: reqwest::Client,
    pub(super) api_base: String,
    pub(super) media_repository: Option<Arc<dyn crate::host::MediaRepository>>,
    pub(super) voice_media_store: Option<Arc<dyn crate::host::VoiceMediaStore>>,
    pub(super) voice_synthesis: bool,
    pub(super) events: Option<Arc<dyn crate::host::EventSink>>,
    /// Sink of the legacy listener, used when the host cannot take typed ingress.
    pub(super) listener: Mutex<Option<tokio::sync::mpsc::Sender<ChannelMessage>>>,
    /// Provider message ids already published (Meta redelivers).
    pub(super) seen: Mutex<SeenSet>,
    /// Serializes deliveries so one provider id is never published twice by
    /// concurrent redeliveries.
    pub(super) ingest_gate: tokio::sync::Mutex<()>,
    /// Latest inbound message per conversation, the anchor for typing indicators.
    pub(super) anchors: Mutex<BoundedMap>,
}

impl WhatsAppChannel {
    pub fn new(
        access_token: String,
        endpoint_id: String,
        verify_token: String,
        allowed_numbers: Vec<String>,
    ) -> Self {
        Self::with_http_client(
            access_token,
            endpoint_id,
            verify_token,
            allowed_numbers,
            reqwest::Client::new(),
        )
    }

    pub fn with_http_client(
        access_token: String,
        endpoint_id: String,
        verify_token: String,
        allowed_numbers: Vec<String>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            access_token,
            endpoint_id,
            verify_token,
            app_secret: None,
            allowed_numbers,
            http_client,
            api_base: api::resolve_graph_base(std::env::var(api::GRAPH_BASE_ENV).ok()),
            media_repository: None,
            voice_media_store: None,
            voice_synthesis: false,
            events: None,
            listener: Mutex::new(None),
            seen: Mutex::new(SeenSet::new(ingress::SEEN_WINDOW)),
            ingest_gate: tokio::sync::Mutex::new(()),
            anchors: Mutex::new(BoundedMap::new(ingress::ANCHOR_WINDOW)),
        }
    }

    /// Set the Meta app secret that authenticates webhook deliveries.
    #[must_use]
    pub fn with_app_secret(mut self, app_secret: Option<String>) -> Self {
        self.app_secret = app_secret.filter(|secret| !secret.trim().is_empty());
        self
    }

    /// Point a test-support build at a local mock Graph server.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_api_base_for_test(mut self, api_base: &str) -> Self {
        self.api_base = api::resolve_graph_base(Some(api_base.to_owned()));
        self
    }

    /// Scoped host media storage, which unlocks typed media in both directions.
    #[must_use]
    pub fn with_media_repository(mut self, store: Arc<dyn crate::host::MediaRepository>) -> Self {
        self.media_repository = Some(store);
        self
    }

    /// Host voice storage, which unlocks voice notes in both directions.
    #[must_use]
    pub fn with_voice_media_store(mut self, store: Arc<dyn crate::host::VoiceMediaStore>) -> Self {
        self.voice_media_store = Some(store);
        self
    }

    /// Whether the module can prepare synthesized voice replies for this account.
    #[must_use]
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.voice_synthesis = enabled;
        self
    }

    /// Where authenticated deliveries are published.
    #[must_use]
    pub fn with_events(mut self, events: Arc<dyn crate::host::EventSink>) -> Self {
        self.events = Some(events);
        self
    }

    pub(super) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    pub(crate) async fn health_status(&self) -> bool {
        let url = self.graph_url(&self.endpoint_id);
        if api::ensure_secure(&url).is_err() {
            return false;
        }
        self.http_client()
            .get(&url)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map(|response| response.status().is_success())
            .unwrap_or(false)
    }

    /// Check if a phone number is allowed (E.164 format: +1234567890)
    pub(super) fn is_number_allowed(&self, phone: &str) -> bool {
        self.allowed_numbers.iter().any(|n| n == "*" || n == phone)
    }

    /// Get the verify token for webhook verification
    pub fn verify_token(&self) -> &str {
        &self.verify_token
    }

    /// The profile name of the contact `wa_id` in a webhook `value`
    /// (`contacts[].profile.name`, the name the sender set for themselves).
    pub(super) fn contact_name(value: &serde_json::Value, wa_id: &str) -> Option<String> {
        value
            .get("contacts")?
            .as_array()?
            .iter()
            .find(|contact| contact.get("wa_id").and_then(|id| id.as_str()) == Some(wa_id))?
            .get("profile")?
            .get("name")?
            .as_str()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
    }

    /// Parse an incoming webhook payload into text-only legacy messages.
    ///
    /// This is the v21 text projection and does not authenticate anything.
    /// Production ingress is [`ChannelAdapter::deliver_webhook`](crate::channel::ChannelAdapter::deliver_webhook), which verifies the
    /// signature, binds the destination and carries every content kind.
    pub fn parse_webhook_payload(&self, payload: &serde_json::Value) -> Vec<ChannelMessage> {
        let mut messages = Vec::new();

        // WhatsApp Cloud API webhook structure:
        // { "object": "whatsapp_business_account", "entry": [...] }
        let Some(entries) = payload.get("entry").and_then(|e| e.as_array()) else {
            return messages;
        };

        for entry in entries {
            let Some(changes) = entry.get("changes").and_then(|c| c.as_array()) else {
                continue;
            };

            for change in changes {
                let Some(value) = change.get("value") else {
                    continue;
                };

                // Extract messages array
                let Some(msgs) = value.get("messages").and_then(|m| m.as_array()) else {
                    continue;
                };

                for msg in msgs {
                    // Get sender phone number
                    let Some(from) = msg.get("from").and_then(|f| f.as_str()) else {
                        continue;
                    };

                    // Check allowlist
                    let normalized_from = if from.starts_with('+') {
                        from.to_string()
                    } else {
                        format!("+{from}")
                    };

                    if !self.is_number_allowed(&normalized_from) {
                        tracing::warn!(
                            "WhatsApp: ignoring message from unauthorized number: {normalized_from}. \
                            Add to allowed_numbers in config.toml, then configure channels in the web UI."
                        );
                        continue;
                    }

                    // Extract text content (support text messages only for now)
                    let content = if let Some(text_obj) = msg.get("text") {
                        text_obj
                            .get("body")
                            .and_then(|b| b.as_str())
                            .unwrap_or("")
                            .to_string()
                    } else {
                        // Could be image, audio, etc. Skip for now.
                        tracing::debug!("WhatsApp: skipping non-text message from {from}");
                        continue;
                    };

                    if content.is_empty() {
                        continue;
                    }

                    // Get timestamp
                    let timestamp = msg
                        .get("timestamp")
                        .and_then(|t| t.as_str())
                        .and_then(|t| t.parse::<u64>().ok())
                        .unwrap_or_else(|| {
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                        });

                    messages.push(ChannelMessage {
                        id: Uuid::new_v4().to_string(),
                        reply_target: normalized_from.clone(),
                        sender: normalized_from,
                        content,
                        channel: "whatsapp".to_string(),
                        timestamp,
                        topic_id: None,
                        reply_to_id: None,
                        thread_ts: None,
                        sender_name: Self::contact_name(value, from),
                    });
                }
            }
        }

        messages
    }
}

impl WhatsAppChannel {
    async fn mark_read_provider(&self, provider_message_id: &str) -> anyhow::Result<()> {
        if provider_message_id.trim().is_empty() {
            return Err(anyhow::Error::new(WaFailure::rejected(
                "WhatsApp read receipt needs a message id",
            )));
        }
        self.post_messages(&outbound::read_body(provider_message_id, false))
            .await
            .map(|_| ())
            .map_err(anyhow::Error::new)
    }

    async fn start_typing_provider(&self, recipient: &str) -> anyhow::Result<()> {
        let digits = outbound::recipient_digits(recipient)?;
        // The Cloud API shows "typing" only against an inbound message.
        let anchor = self.anchors.lock().get(&format!("+{digits}"));
        let Some(anchor) = anchor else {
            return Err(anyhow::Error::new(WaFailure::rejected(
                "WhatsApp typing needs a recent inbound message from this contact",
            )));
        };
        self.post_messages(&outbound::read_body(&anchor, true))
            .await
            .map(|_| ())
            .map_err(anyhow::Error::new)
    }

    async fn stop_typing_provider(&self) -> anyhow::Result<()> {
        // WhatsApp clears the indicator when a reply arrives or after 25 seconds.
        Ok(())
    }

    async fn send_reaction_provider(
        &self,
        conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::ChannelReaction,
        remove: bool,
    ) -> anyhow::Result<()> {
        if reaction.custom_emoji_id.is_some() {
            return Err(anyhow::Error::new(WaFailure::unsupported(
                "WhatsApp reactions are plain emoji",
            )));
        }
        let to = outbound::recipient_digits(&conversation.id)?;
        let emoji = if remove { "" } else { reaction.value.as_str() };
        if !remove && emoji.trim().is_empty() {
            return Err(anyhow::Error::new(WaFailure::rejected(
                "WhatsApp reaction needs an emoji",
            )));
        }
        self.post_messages(&outbound::reaction_body(&to, provider_message_id, emoji))
            .await
            .map(|_| ())
            .map_err(anyhow::Error::new)
    }

    async fn deliver_webhook_provider(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
        self.ingest_webhook(signature, body)
            .await
            .map_err(|failure| match failure {
                WaFailure::Rejected(message)
                | WaFailure::BadFormat(message)
                | WaFailure::Unsupported(message) => {
                    anyhow::Error::new(tinychannels_runtime::WebhookRejected(message))
                }
                uncertain => anyhow::Error::new(uncertain),
            })
    }

    fn verify_webhook_provider(
        &self,
        mode: &str,
        verify_token: &str,
        challenge: &str,
    ) -> Option<String> {
        self.verify_challenge(mode, verify_token, challenge)
    }
}

/// The failure class of an error produced by the Cloud adapter.
fn failure_of(error: &anyhow::Error) -> WaFailure {
    error
        .downcast_ref::<WaFailure>()
        .cloned()
        .unwrap_or_else(|| WaFailure::uncertain("WhatsApp operation outcome is uncertain"))
}

fn unsupported(message: &str) -> crate::channel::ChannelSendError {
    WaFailure::unsupported(message).into_send_error()
}

#[async_trait]
impl crate::channel::ChannelAdapter for WhatsAppChannel {
    fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        tinychannels_bus::channel::ChannelDescriptor {
            id: "whatsapp".into(),
            display_name: "WhatsApp".into(),
            metadata: std::collections::BTreeMap::from([
                ("adapterMode".into(), "native-v2".into()),
                ("backend".into(), "cloud".into()),
            ]),
            ..Default::default()
        }
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        super::capabilities::declared(self)
    }

    fn supports_webhook_delivery(&self) -> bool {
        true
    }

    async fn deliver_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
        self.deliver_webhook_provider(signature, body).await
    }

    fn supports_webhook_verification(&self) -> bool {
        true
    }

    fn verify_webhook(&self, mode: &str, verify_token: &str, challenge: &str) -> Option<String> {
        self.verify_webhook_provider(mode, verify_token, challenge)
    }

    async fn start(
        &self,
        _sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), crate::channel::ChannelSendError> {
        // Webhook ingress is owned by the module runtime, which forwards each
        // authenticated delivery to `deliver_webhook`.
        Ok(())
    }

    async fn stop(&self) -> Result<(), crate::channel::ChannelSendError> {
        Ok(())
    }

    async fn send(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<crate::channel::MessageReceipt, crate::channel::ChannelSendError> {
        let message = tinychannels_bus::ChannelOutboundMessage::from_intent(&intent);
        self.send_v2(&message)
            .await
            .map(|outcome| outcome.receipt)
            .map_err(WaFailure::into_send_error)
    }

    async fn mark_read(
        &self,
        _conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.mark_read_provider(provider_message_id)
            .await
            .map_err(|error| failure_of(&error).into_send_error())
    }

    async fn status(
        &self,
    ) -> Result<
        tinychannels_runtime::controllers::ChannelAccountSnapshot,
        crate::channel::ChannelSendError,
    > {
        let health_state = if self.health_status().await {
            "healthy"
        } else {
            "unhealthy"
        };
        Ok(tinychannels_bus::controllers::ChannelAccountSnapshot {
            channel_id: "whatsapp".into(),
            states: vec![
                tinychannels_bus::controllers::ChannelAccountState::Configured,
                tinychannels_bus::controllers::ChannelAccountState::Enabled,
            ],
            health_state: Some(health_state.into()),
            ..Default::default()
        })
    }
}

#[async_trait]
impl crate::channel::ChannelTypedSend for WhatsAppChannel {
    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<crate::channel::ChannelSendOutcome, crate::channel::ChannelSendError> {
        self.send_v2(message)
            .await
            .map_err(WaFailure::into_send_error)
    }
}

#[async_trait]
impl crate::channel::ChannelTyping for WhatsAppChannel {
    async fn start_typing(
        &self,
        conversation_id: &str,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.start_typing_provider(conversation_id)
            .await
            .map_err(|error| failure_of(&error).into_send_error())
    }

    async fn stop_typing(
        &self,
        _conversation_id: &str,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.stop_typing_provider()
            .await
            .map_err(|error| failure_of(&error).into_send_error())
    }
}

#[async_trait]
impl crate::channel::LegacyChannelReaction for WhatsAppChannel {
    async fn send_reaction(
        &self,
        channel: &tinychannels_bus::channel::ChannelRef,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), crate::channel::ChannelSendError> {
        if channel.id != "whatsapp" {
            return Err(
                WaFailure::rejected("reaction channel does not match WhatsApp").into_send_error(),
            );
        }
        self.send_reaction_provider(conversation, provider_message_id, reaction, remove)
            .await
            .map_err(|error| failure_of(&error).into_send_error())
    }
}

#[async_trait]
impl crate::channel::ChannelEdit for WhatsAppChannel {
    async fn edit(
        &self,
        _receipt: &tinychannels_bus::channel::MessageReceipt,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<tinychannels_bus::channel::MessageReceipt, crate::channel::ChannelSendError> {
        Err(unsupported(
            "the WhatsApp Cloud API cannot edit a sent message",
        ))
    }
}

#[async_trait]
impl crate::channel::ChannelDelete for WhatsAppChannel {
    async fn delete(
        &self,
        _receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> Result<(), crate::channel::ChannelSendError> {
        Err(unsupported(
            "the WhatsApp Cloud API cannot unsend a message",
        ))
    }
}

#[async_trait]
impl crate::channel::ChannelStreamingDraft for WhatsAppChannel {
    async fn upsert_draft(
        &self,
        _previous: Option<&tinychannels_bus::channel::MessageReceipt>,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<tinychannels_bus::channel::MessageReceipt, crate::channel::ChannelSendError> {
        Err(unsupported(
            "WhatsApp messages cannot be streamed as edited drafts",
        ))
    }
}

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod conformance;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
