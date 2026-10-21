//! QQ bot configuration and host service wiring.

use super::*;

impl QQChannel {
    pub fn new(app_id: String, app_secret: String, allowed_users: Vec<String>) -> Self {
        Self {
            app_id,
            app_secret,
            allowed_users,
            token_cache: Arc::new(RwLock::new(None)),
            dedup: Arc::new(RwLock::new(HashSet::new())),
            http_client: reqwest::Client::new(),
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            listener: std::sync::Mutex::new(None),
        }
    }

    pub fn with_http_client(
        app_id: String,
        app_secret: String,
        allowed_users: Vec<String>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            app_id,
            app_secret,
            allowed_users,
            token_cache: Arc::new(RwLock::new(None)),
            dedup: Arc::new(RwLock::new(HashSet::new())),
            http_client,
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            listener: std::sync::Mutex::new(None),
        }
    }

    /// Enable scoped media uploads through the host repository.
    pub fn with_media_repository(
        mut self,
        repository: Arc<dyn crate::host::MediaRepository>,
    ) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Enable native QQ voice messages through the host voice store.
    pub fn with_voice_media_store(mut self, store: Arc<dyn crate::host::VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Attach the host's transcriber for platform-marked inbound voice clips.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn crate::host::Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    /// Attach the host's ordered typed-ingress path.
    pub fn with_events(mut self, events: Arc<dyn crate::host::EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// Replace the no-redirect attachment client, primarily for host-managed
    /// trust roots and deterministic local tests.
    #[cfg(test)]
    pub(crate) fn with_attachment_http_client(mut self, client: reqwest::Client) -> Self {
        self.attachment_http_client = client;
        self
    }
}
