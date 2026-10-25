//! Email channel construction and host service configuration.

use super::*;

impl EmailChannel {
    pub fn new(config: EmailConfig) -> Self {
        Self {
            config,
            #[cfg(feature = "email")]
            seen_messages: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(feature = "email")]
            reconnect_backoff: Duration::from_secs(1),
            #[cfg(feature = "email")]
            max_reconnect_backoff: Duration::from_secs(60),
            #[cfg(feature = "email")]
            native: NativeState::default(),
            #[cfg(feature = "email")]
            subjects: native::Subjects::default(),
            #[cfg(feature = "email")]
            listener_cancel: Arc::new(StdMutex::new(None)),
            #[cfg(feature = "email")]
            listener_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Scoped media storage; enables typed attachments in both directions.
    #[cfg(feature = "email")]
    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Durable voice storage; enables voice attachments in both directions.
    #[cfg(feature = "email")]
    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Host speech-to-text for inbound voice attachments.
    #[cfg(feature = "email")]
    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    /// Enable shared final-turn synthesis when the host provides TTS, a codec
    /// and durable voice storage.
    #[cfg(feature = "email")]
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.native.voice_synthesis = enabled;
        self
    }

    /// Ordered ingress for typed inbound messages.
    #[cfg(feature = "email")]
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }
}
