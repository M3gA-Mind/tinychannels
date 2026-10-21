mod outbound;
mod inbound;
mod config;
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

impl MattermostChannel {

impl MattermostChannel {
n valid_file_id(file_id: &str) -> bool {
    !file_id.is_empty()
        && file_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

impl MattermostChannel {


impl MattermostChannel {


/// Check whether a Mattermost post contains an @-mention of the bot.
///
/// Checks two sources:
/// 1. Text-based: looks for `@bot_username` in the message body (case-insensitive).
/// 2. Metadata-based: checks the post's `metadata.mentions` array for the bot user ID.
fn contains_bot_mention_mm(
    text: &str,
    bot_user_id: &str,
    bot_username: &str,
    post: &serde_json::Value,
) -> bool {
    // 1. Text-based: @username (case-insensitive, word-boundary aware)
    if !find_bot_mention_spans(text, bot_username).is_empty() {
        return true;
    }

    // 2. Metadata-based: Mattermost may include a "metadata.mentions" array of user IDs.
    if !bot_user_id.is_empty()
        && let Some(mentions) = post
            .get("metadata")
            .and_then(|m| m.get("mentions"))
            .and_then(|m| m.as_array())
        && mentions.iter().any(|m| m.as_str() == Some(bot_user_id))
    {
        return true;
    }

    false
}

fn is_mattermost_username_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.'
}

fn find_bot_mention_spans(text: &str, bot_username: &str) -> Vec<(usize, usize)> {
    if bot_username.is_empty() {
        return Vec::new();
    }

    let mention = format!("@{}", bot_username.to_ascii_lowercase());
    let mention_len = mention.len();
    if mention_len == 0 {
        return Vec::new();
    }

    let mention_bytes = mention.as_bytes();
    let text_bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut index = 0;

    while index + mention_len <= text_bytes.len() {
        let is_match = text_bytes[index] == b'@'
            && text_bytes[index..index + mention_len]
                .iter()
                .zip(mention_bytes.iter())
                .all(|(left, right)| left.eq_ignore_ascii_case(right));

        if is_match {
            let end = index + mention_len;
            let at_boundary = text[end..]
                .chars()
                .next()
                .is_none_or(|next| !is_mattermost_username_char(next));
            if at_boundary {
                spans.push((index, end));
                index = end;
                continue;
            }
        }

        let step = text[index..].chars().next().map_or(1, char::len_utf8);
        index += step;
    }

    spans
}

/// Normalize incoming Mattermost content when `mention_only` is enabled.
///
/// Returns `None` if the message doesn't mention the bot.
/// Returns `Some(cleaned)` with the @-mention stripped and text trimmed.
fn normalize_mattermost_content(
    text: &str,
    bot_user_id: &str,
    bot_username: &str,
    post: &serde_json::Value,
) -> Option<String> {
    let mention_spans = find_bot_mention_spans(text, bot_username);
    let metadata_mentions_bot = !bot_user_id.is_empty()
        && post
            .get("metadata")
            .and_then(|m| m.get("mentions"))
            .and_then(|m| m.as_array())
            .is_some_and(|mentions| mentions.iter().any(|m| m.as_str() == Some(bot_user_id)));

    if mention_spans.is_empty() && !metadata_mentions_bot {
        return None;
    }

    let mut cleaned = text.to_string();
    if !mention_spans.is_empty() {
        let mut result = String::with_capacity(text.len());
        let mut cursor = 0;
        for (start, end) in mention_spans {
            result.push_str(&text[cursor..start]);
            result.push(' ');
            cursor = end;
        }
        result.push_str(&text[cursor..]);
        cleaned = result;
    }

    let cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() {
        return None;
    }

    Some(cleaned)
}

#[async_trait]
impl crate::providers::long_tail::LongTail for MattermostChannel {
    const ID: &'static str = "mattermost";
    const DISPLAY: &'static str = "Mattermost";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        let mut capabilities = text_capabilities(TextSpec {
            typing: true,
            thread: true,
            ..Default::default()
        });
        capabilities.reactions = true;
        capabilities
            .actions
            .supported
            .extend(["edit", "delete"].map(String::from));
        if self.media_repository.is_some() {
            capabilities
                .content
                .parts
                .extend(["image", "video", "audio", "file"].map(String::from));
            let outbound = |types: &[&str]| tinychannels_bus::ContentCapability {
                outbound: true,
                accepted_types: types.iter().map(|mime| (*mime).into()).collect(),
                emitted_types: types.iter().map(|mime| (*mime).into()).collect(),
                max_size_bytes: Some(MAX_FILE_BYTES),
                ..Default::default()
            };
            capabilities.content.image =
                outbound(&["image/jpeg", "image/png", "image/gif", "image/webp"]);
            capabilities.content.video = outbound(&["video/mp4", "video/quicktime", "video/webm"]);
            capabilities.content.audio = outbound(&[
                "audio/ogg",
                "audio/mpeg",
                "audio/mp4",
                "audio/wav",
                "audio/webm",
            ]);
            capabilities.content.file = outbound(&[
                "application/pdf",
                "text/plain",
                "text/csv",
                "application/json",
                "application/zip",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ]);
            let inbound = self.events.is_some();
            capabilities.content.image.inbound = inbound;
            capabilities.content.video.inbound = inbound;
            capabilities.content.audio.inbound = inbound;
            capabilities.content.file.inbound = inbound;
        }
        if self.voice_media_store.is_some() {
            capabilities.content.parts.push("voice_note".into());
            capabilities.voice_out = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                MAX_VOICE_BYTES,
                &["m4a_aac", "mp3", "ogg_opus"],
            );
        }
        capabilities
    }

    async fn health_state(&self) -> &'static str {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::providers::native::NativeProvider::health_state(self),
        )
        .await
        {
            Ok(state) => state,
            Err(_) => "unhealthy",
        }
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        crate::providers::native::NativeProvider::descriptor_extra(self)
    }

    fn typing_supported(&self) -> bool {
        true
    }

    async fn set_typing(
        &self,
        conversation_id: &str,
        start: bool,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        let result = if start {
            self.start_typing_loop(conversation_id).await
        } else {
            self.stop_typing_loop()
        };
        result.map_err(|_| tinychannels_bus::channel::ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Transient,
            message: "Mattermost typing indicator failed".into(),
            retryable: true,
            ..Default::default()
        })
    }

    async fn react(
        &self,
        channel: &tinychannels_bus::ChannelRef,
        conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        use crate::providers::native::NativeProvider;

        crate::providers::native::edit::check_channel(self, channel)?;
        NativeProvider::react(self, conversation, provider_message_id, reaction, remove).await
    }

    async fn edit(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<
        tinychannels_bus::channel::MessageReceipt,
        tinychannels_bus::channel::ChannelSendError,
    > {
        crate::providers::native::edit(self, receipt, &intent).await
    }

    async fn delete(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        crate::providers::native::delete(self, receipt).await
    }

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> std::result::Result<(), tinychannels_bus::channel::ChannelSendError> {
        use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

        let (cancel, mut cancellation) = tokio::sync::watch::channel(false);
        {
            let mut active = self.listener_cancel.lock();
            if active.is_some() {
                return Err(ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "Mattermost listener is already running".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            *active = Some(cancel.clone());
        }
        let _registration = ListenerRegistration {
            channel: self,
            cancel,
        };
        self.listen_typed(sink, &mut cancellation).await
    }

    async fn stop_listener(
        &self,
    ) -> std::result::Result<(), tinychannels_bus::channel::ChannelSendError> {
        if let Some(cancel) = self.listener_cancel.lock().as_ref() {
            cancel.send_replace(true);
        }
        Ok(())
    }

    async fn send_typed_message(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<
        tinychannels_bus::channel::ChannelSendOutcome,
        tinychannels_bus::channel::ChannelSendError,
    > {
        crate::providers::native::outbound::send_typed(self, message).await
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
