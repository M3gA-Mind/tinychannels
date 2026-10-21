//! Slack channel — struct definition, builders and shared constants.

use crate::host::{EventSink, MediaRepository, VoiceMediaStore};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
#[cfg(test)]
use tinychannels_bus::channel::ChannelSendError;

pub(crate) const SLACK_API_BASE: &str = "https://slack.com/api";
pub(crate) const SLACK_FILES_BASE: &str = "https://files.slack.com/";
/// Largest file the adapter downloads or uploads.
pub(crate) const SLACK_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
/// Voice bytes cross TinyBus inline, so they stay below the 16 MiB frame cap.
pub(crate) const SLACK_MAX_VOICE_BYTES: u64 = 8 * 1024 * 1024;
/// Slack truncates `text` at 40 000 characters; chunk well below that.
pub(crate) const SLACK_TEXT_CHUNK_CHARS: usize = 3900;
/// Remembered `channel:ts` pairs, so a redelivered or duplicated event is dropped.
pub(crate) const SLACK_SEEN_WINDOW: usize = 2048;

/// A typed send failure carried through `anyhow` so `Channel::send_typed_with_receipt`
/// callers can recover the structured error.
#[derive(Debug)]
#[cfg(test)]
pub(crate) struct SlackFailure(pub(crate) ChannelSendError);

#[cfg(test)]
impl std::fmt::Display for SlackFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

#[cfg(test)]
impl std::error::Error for SlackFailure {}

/// Slack channel: Socket Mode (or history polling) in, Web API out.
pub struct SlackChannel {
    pub(crate) bot_token: String,
    pub(crate) app_token: Option<String>,
    pub(crate) channel_id: Option<String>,
    pub(crate) allowed_users: Vec<String>,
    pub(crate) http_client: reqwest::Client,
    /// Web API base, without a trailing slash. Overridable for mock servers.
    pub(crate) api_base: String,
    /// Only file URLs under this prefix receive the bot token.
    pub(crate) files_base: String,
    pub(crate) media_repository: Option<Arc<dyn MediaRepository>>,
    pub(crate) voice_media_store: Option<Arc<dyn VoiceMediaStore>>,
    pub(crate) voice_synthesis: bool,
    pub(crate) events: Option<Arc<dyn EventSink>>,
    pub(crate) bot_user_id: Mutex<Option<String>>,
    pub(crate) seen: Mutex<VecDeque<String>>,
    pub(crate) reconnect_backoff: Duration,
    pub(crate) poll_interval: Duration,
    pub(crate) listener_cancel: tokio::sync::watch::Sender<bool>,
    pub(crate) listener_running: AtomicBool,
}

impl SlackChannel {
    pub fn new(bot_token: String, channel_id: Option<String>, allowed_users: Vec<String>) -> Self {
        Self::with_http_client(bot_token, channel_id, allowed_users, reqwest::Client::new())
    }

    pub fn with_http_client(
        bot_token: String,
        channel_id: Option<String>,
        allowed_users: Vec<String>,
        http_client: reqwest::Client,
    ) -> Self {
        let (listener_cancel, _) = tokio::sync::watch::channel(false);
        Self {
            bot_token,
            app_token: None,
            channel_id,
            allowed_users,
            http_client,
            api_base: SLACK_API_BASE.into(),
            files_base: SLACK_FILES_BASE.into(),
            media_repository: None,
            voice_media_store: None,
            voice_synthesis: false,
            events: None,
            bot_user_id: Mutex::new(None),
            seen: Mutex::new(VecDeque::new()),
            reconnect_backoff: Duration::from_secs(2),
            poll_interval: Duration::from_secs(3),
            listener_cancel,
            listener_running: AtomicBool::new(false),
        }
    }

    /// Enable Socket Mode with an app-level (`xapp-`) token.
    pub fn with_app_token(mut self, app_token: Option<String>) -> Self {
        self.app_token = app_token.filter(|token| !token.trim().is_empty());
        self
    }

    /// Point the Web API at another base URL (a mock server). File downloads
    /// are then trusted from that server's origin instead of `files.slack.com`.
    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        let base = api_base.into().trim_end_matches('/').to_owned();
        let origin = base
            .split_once("://")
            .map(|(scheme, rest)| {
                format!("{scheme}://{}/", rest.split('/').next().unwrap_or_default())
            })
            .unwrap_or_else(|| format!("{base}/"));
        self.files_base = origin;
        self.api_base = base;
        self
    }

    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.media_repository = Some(repository);
        self
    }

    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.voice_media_store = Some(store);
        self
    }

    /// Enable shared final-turn synthesis when the host provides TTS, a codec
    /// and durable voice storage.
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.voice_synthesis = enabled;
        self
    }

    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.events = Some(events);
        self
    }

    /// Shorten the reconnect and polling waits (tests).
    pub fn with_timing(mut self, reconnect_backoff: Duration, poll_interval: Duration) -> Self {
        self.reconnect_backoff = reconnect_backoff;
        self.poll_interval = poll_interval;
        self
    }

    pub(crate) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    pub(crate) async fn health_status(&self) -> bool {
        self.http_client()
            .get(self.api_url("auth.test"))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .map(|response| response.status().is_success())
            .unwrap_or(false)
    }

    /// Empty allowlist denies everyone; `"*"` allows everyone.
    pub(crate) fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Record `key`; false when it was already seen.
    pub(crate) fn mark_seen(&self, key: String) -> bool {
        let mut seen = self.seen.lock();
        if seen.contains(&key) {
            return false;
        }
        if seen.len() >= SLACK_SEEN_WINDOW {
            seen.pop_front();
        }
        seen.push_back(key);
        true
    }

    /// Check for a committed delivery without reserving the key.
    pub(crate) fn has_seen(&self, key: &str) -> bool {
        self.seen.lock().iter().any(|seen| seen == key)
    }
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
