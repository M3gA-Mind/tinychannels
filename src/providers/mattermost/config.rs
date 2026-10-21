//! Mattermost account configuration and host service wiring.

use super::*;

impl MattermostChannel {
    pub fn new(
        base_url: String,
        bot_token: String,
        channel_id: Option<String>,
        allowed_users: Vec<String>,
        thread_replies: bool,
        mention_only: bool,
    ) -> Self {
        Self::with_http_client(
            base_url,
            bot_token,
            channel_id,
            allowed_users,
            thread_replies,
            mention_only,
            reqwest::Client::new(),
        )
    }

    pub fn with_http_client(
        base_url: String,
        bot_token: String,
        channel_id: Option<String>,
        allowed_users: Vec<String>,
        thread_replies: bool,
        mention_only: bool,
        http_client: reqwest::Client,
    ) -> Self {
        // Ensure base_url doesn't have a trailing slash for consistent path joining
        let base_url = base_url.trim_end_matches('/').to_string();
        Self {
            base_url,
            bot_token,
            channel_id,
            allowed_users,
            thread_replies,
            mention_only,
            typing_handle: Mutex::new(None),
            http_client,
            media_repository: None,
            voice_media_store: None,
            transcriber: None,
            events: None,
            listener_cancel: Mutex::new(None),
        }
    }

    /// Attach the host's scoped media repository for native file uploads.
    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.media_repository = Some(repository);
        self
    }

    /// Attach the host's scoped voice store for audio file attachments.
    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.voice_media_store = Some(store);
        self
    }

    /// Attach the host's speech-to-text port for platform-marked voice clips.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.transcriber = Some(transcriber);
        self
    }

    /// Attach the host's ordered typed-ingress path.
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.events = Some(events);
        self
    }
}
