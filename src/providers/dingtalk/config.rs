//! DingTalk account configuration and host service wiring.

use super::inbound::no_redirect_client;
use super::*;

impl DingTalkChannel {
    pub fn new(client_id: String, client_secret: String, allowed_users: Vec<String>) -> Self {
        Self {
            client_id,
            client_secret,
            allowed_users,
            session_webhooks: Arc::new(RwLock::new(HashMap::new())),
            http_client: reqwest::Client::new(),
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            robot_contexts: Arc::new(RwLock::new(HashMap::new())),
            listener: std::sync::Mutex::new(None),
        }
    }

    pub fn with_http_client(
        client_id: String,
        client_secret: String,
        allowed_users: Vec<String>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            allowed_users,
            session_webhooks: Arc::new(RwLock::new(HashMap::new())),
            http_client,
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            robot_contexts: Arc::new(RwLock::new(HashMap::new())),
            listener: std::sync::Mutex::new(None),
        }
    }

    /// Enable native typed media ingress and upload through host-scoped storage.
    pub fn with_media_repository(
        mut self,
        repository: Arc<dyn crate::host::MediaRepository>,
    ) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Attach the host's ordered typed-ingress path.
    pub fn with_events(mut self, events: Arc<dyn crate::host::EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// Enable native voice ingress through the host's scoped voice store.
    pub fn with_voice_media_store(mut self, store: Arc<dyn crate::host::VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Enable shared final-turn synthesis when the host has TTS, a codec,
    /// and durable scoped voice storage.
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.native.voice_synthesis = enabled;
        self
    }

    /// Attach the host's speech-to-text service for media that carries no transcript.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn crate::host::Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    pub(crate) fn with_attachment_http_client(mut self, client: reqwest::Client) -> Self {
        self.attachment_http_client = client;
        self
    }
}
