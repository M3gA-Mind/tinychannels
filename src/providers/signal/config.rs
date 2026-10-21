//! Configuration, client setup, and health checks for Signal.

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::sync::watch;

pub(super) type ListenerCancellationSlot =
    Arc<std::sync::Mutex<Option<(u64, watch::Sender<bool>)>>>;

/// Signal channel using signal-cli daemon's native JSON-RPC + SSE API.
///
/// Connects to a running `signal-cli daemon --http <host:port>`.
/// Listens via SSE at `/api/v1/events` and sends via JSON-RPC at
/// `/api/v1/rpc`.
#[derive(Clone)]
pub struct SignalChannel {
    pub(super) http_url: String,
    pub(super) account: String,
    pub(super) group_id: Option<String>,
    pub(super) allowed_from: Vec<String>,
    pub(super) ignore_attachments: bool,
    pub(super) ignore_stories: bool,
    pub(super) http_client: Client,
    pub(super) native: NativeState,
    pub(super) listener_cancel: ListenerCancellationSlot,
    pub(super) listener_generation: Arc<AtomicU64>,
}

impl SignalChannel {
    pub fn new(
        http_url: String,
        account: String,
        group_id: Option<String>,
        allowed_from: Vec<String>,
        ignore_attachments: bool,
        ignore_stories: bool,
    ) -> Self {
        let http_url = http_url.trim_end_matches('/').to_string();
        let http_client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("Signal HTTP client should build");
        Self {
            http_url,
            account,
            group_id,
            allowed_from,
            ignore_attachments,
            ignore_stories,
            http_client,
            native: NativeState::default(),
            listener_cancel: Arc::new(std::sync::Mutex::new(None)),
            listener_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_http_client(
        http_url: String,
        account: String,
        group_id: Option<String>,
        allowed_from: Vec<String>,
        ignore_attachments: bool,
        ignore_stories: bool,
        http_client: Client,
    ) -> Self {
        let http_url = http_url.trim_end_matches('/').to_string();
        Self {
            http_url,
            account,
            group_id,
            allowed_from,
            ignore_attachments,
            ignore_stories,
            http_client,
            native: NativeState::default(),
            listener_cancel: Arc::new(std::sync::Mutex::new(None)),
            listener_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(super) async fn health_probe(&self) -> bool {
        let url = format!("{}/api/v1/check", self.http_url);
        self.http_client()
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
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

    /// Ordered ingress for typed inbound messages and lifecycle events.
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    pub(super) fn http_client(&self) -> Client {
        self.http_client.clone()
    }
}
