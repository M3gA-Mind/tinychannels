//! Lark account configuration and host service wiring.

use super::*;

impl LarkChannel {
    pub fn new(
        app_id: String,
        app_secret: String,
        verification_token: String,
        port: Option<u16>,
        allowed_users: Vec<String>,
    ) -> Self {
        Self {
            app_id,
            app_secret,
            verification_token,
            port,
            allowed_users,
            use_feishu: true,
            receive_mode: crate::config::LarkReceiveMode::default(),
            http_client: reqwest::Client::new(),
            native: crate::providers::native::HostPorts::default(),
            voice_codec: None,
            tenant_token: Arc::new(RwLock::new(None)),
            ws_seen_ids: Arc::new(RwLock::new(HashMap::new())),
            listener_cancel: Arc::new(StdMutex::new(None)),
        }
    }

    /// Build from `LarkConfig` (preserves `use_feishu` and `receive_mode`).
    pub fn from_config(config: &crate::config::LarkConfig) -> Self {
        let mut ch = Self::new(
            config.app_id.clone(),
            config.app_secret.clone(),
            config.verification_token.clone().unwrap_or_default(),
            config.port,
            config.allowed_users.clone(),
        );
        ch.use_feishu = config.use_feishu;
        ch.receive_mode = config.receive_mode.clone();
        ch
    }

    /// Build from config using the host-selected HTTP client.
    pub fn from_config_with_http_client(
        config: &crate::config::LarkConfig,
        http_client: reqwest::Client,
    ) -> Self {
        let mut channel = Self::from_config(config);
        channel.http_client = http_client;
        channel
    }

    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    pub fn with_voice_codec(mut self, codec: Arc<dyn VoiceCodec>) -> Self {
        self.voice_codec = Some(codec);
        self
    }
}
