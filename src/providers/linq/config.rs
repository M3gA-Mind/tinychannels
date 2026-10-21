//! Configuration and client construction for the Linq provider.

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
use std::sync::Arc;

/// Linq channel — uses the Linq Partner V3 API for iMessage, RCS, and SMS.
///
/// This channel operates in webhook mode (push-based) rather than polling.
/// The `listen` method here is a keepalive placeholder; inbound delivery depends on
/// your deployment wiring Linq webhooks to the app.
pub struct LinqChannel {
    pub(super) api_token: String,
    pub(super) from_phone: String,
    pub(super) allowed_senders: Vec<String>,
    pub(super) http_client: reqwest::Client,
    pub(super) api_base: String,
    /// Signs webhooks (`X-Webhook-Signature`); deliveries are refused without it.
    pub(super) webhook_secret: Option<String>,
    pub(super) native: NativeState,
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

    pub(super) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    pub(super) async fn health_probe(&self) -> bool {
        let url = format!("{}/phonenumbers", self.api_base);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.http_client()
                .get(&url)
                .bearer_auth(&self.api_token)
                .send(),
        )
        .await
        .is_ok_and(|response| response.is_ok_and(|response| response.status().is_success()))
    }

    /// Get the bot's phone number.
    pub fn phone_number(&self) -> &str {
        &self.from_phone
    }
}
