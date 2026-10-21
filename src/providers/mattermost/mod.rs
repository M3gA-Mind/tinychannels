mod capabilities;
mod config;
mod inbound;
mod media;
use inbound::{contains_bot_mention_mm, normalize_mattermost_content};
mod outbound;
use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::traits::ChannelMessage;
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;

pub(super) const MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;
pub(super) const MAX_VOICE_BYTES: u64 = 8 * 1024 * 1024;

/// Mattermost channel — polls channel posts via REST API v4.
/// Mattermost is API-compatible with many Slack patterns but uses a dedicated v4 structure.
pub struct MattermostChannel {
    base_url: String, // e.g., https://mm.example.com
    bot_token: String,
    channel_id: Option<String>,
    allowed_users: Vec<String>,
    /// When true (default), replies thread on the original post's root_id.
    /// When false, replies go to the channel root.
    thread_replies: bool,
    /// When true, only respond to messages that @-mention the bot.
    mention_only: bool,
    /// Handle for the background typing-indicator loop (aborted on stop_typing).
    typing_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    http_client: reqwest::Client,
    media_repository: Option<Arc<dyn MediaRepository>>,
    voice_media_store: Option<Arc<dyn VoiceMediaStore>>,
    transcriber: Option<Arc<dyn Transcriber>>,
    events: Option<Arc<dyn EventSink>>,
    listener_cancel: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
}

struct AdmittedPost {
    id: String,
    user_id: String,
    create_at: i64,
    root_id: String,
    content: String,
}

struct ListenerRegistration<'a> {
    channel: &'a MattermostChannel,
    cancel: tokio::sync::watch::Sender<bool>,
}

impl Drop for ListenerRegistration<'_> {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        let mut active = self.channel.listener_cancel.lock();
        if active
            .as_ref()
            .is_some_and(|cancel| cancel.same_channel(&self.cancel))
        {
            *active = None;
        }
    }
}

#[path = "native.rs"]
mod native;

crate::providers::long_tail::impl_long_tail_adapter!(MattermostChannel);

#[cfg(test)]
#[path = "mattermost_tests.rs"]
mod tests;

#[cfg(any(test, debug_assertions))]
#[path = "test_support_tests.rs"]
pub mod test_support;
