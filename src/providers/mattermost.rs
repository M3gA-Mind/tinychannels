use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::traits::{Channel, ChannelMessage, SendMessage};
use anyhow::{Result, bail};
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

    fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    /// Check if a user ID is in the allowlist.
    /// Empty list means deny everyone. "*" means allow everyone.
    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Get the bot's own user ID and username so we can ignore our own messages
    /// and detect @-mentions by username.
    async fn get_bot_identity(&self) -> (String, String) {
        let resp: Option<serde_json::Value> = async {
            self.http_client()
                .get(format!("{}/api/v4/users/me", self.base_url))
                .bearer_auth(&self.bot_token)
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()
        }
        .await;

        let id = resp
            .as_ref()
            .and_then(|v| v.get("id"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        let username = resp
            .as_ref()
            .and_then(|v| v.get("username"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        (id, username)
    }
}

impl MattermostChannel {
    async fn inbound_envelope(
        &self,
        post: &serde_json::Value,
        channel_id: &str,
        message: &ChannelMessage,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        self.inbound_envelope_from_text(post, channel_id, &message.content, false)
            .await
    }

    async fn inbound_envelope_from_text(
        &self,
        post: &serde_json::Value,
        channel_id: &str,
        text: &str,
        native_listener: bool,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        let id = post.get("id")?.as_str()?.to_owned();
        let sender_id = post.get("user_id")?.as_str()?.to_owned();
        let timestamp = post.get("create_at")?.as_i64()?;
        let mut conversation = tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Channel,
            id: channel_id.to_owned(),
            ..Default::default()
        };
        let root_id = post
            .get("root_id")
            .and_then(serde_json::Value::as_str)
            .filter(|root| !root.is_empty());
        conversation.thread_id = root_id
            .map(str::to_owned)
            .or_else(|| self.thread_replies.then(|| id.clone()));

        let mut ingested = crate::providers::native::inbound::Ingested::default();
        let ingest = crate::providers::native::inbound::Ingest {
            provider: "mattermost",
            channel: tinychannels_bus::ChannelRef {
                id: "mattermost".into(),
                account_id: None,
            },
            repository: self.media_repository.as_ref(),
            voice_store: self.voice_media_store.as_ref(),
            transcriber: self.transcriber.as_ref(),
            file_bytes: MAX_FILE_BYTES,
            voice_bytes: MAX_VOICE_BYTES,
        };
        let file_ids = post
            .get("file_ids")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str);
        for file_id in file_ids {
            if self.media_repository.is_none() || (!native_listener && self.events.is_none()) {
                break;
            }
            let Some((mime, name, size)) = self.file_info(post, file_id).await else {
                continue;
            };
            if size.is_some_and(|size| size > MAX_FILE_BYTES) {
                continue;
            }
            let kind = crate::providers::native::inbound::kind_for_mime(&mime);
            if !crate::host::is_allowed_content_type(kind, &mime) {
                continue;
            }
            let Some(bytes) = self.download_file(file_id, MAX_FILE_BYTES).await else {
                continue;
            };
            ingest
                .add(
                    &mut ingested,
                    &conversation,
                    &id,
                    crate::providers::native::inbound::InboundFile {
                        bytes,
                        mime,
                        name,
                        duration_ms: None,
                        // Mattermost file metadata has no voice-note marker;
                        // generic audio attachments remain ordinary audio.
                        voice: false,
                        dedupe_key: format!("mattermost:{id}:{file_id}"),
                        transcript: None,
                    },
                )
                .await;
        }
        let parts = crate::providers::native::inbound::assemble(text, None, ingested);
        if parts.is_empty() {
            return None;
        }
        Some(tinychannels_bus::ChannelInboundEnvelope {
            channel: tinychannels_bus::ChannelRef {
                id: "mattermost".into(),
                account_id: None,
            },
            message_id: id,
            reply_to_id: root_id.map(str::to_owned),
            #[allow(clippy::cast_sign_loss)]
            timestamp_unix_ms: Some(timestamp as u64),
            conversation,
            sender: tinychannels_bus::SenderRef {
                id: sender_id,
                ..Default::default()
            },
            text: text.to_owned(),
            media: crate::providers::native::inbound::media_of(&parts),
            parts,
            ..Default::default()
        })
    }

    async fn file_info(
        &self,
        post: &serde_json::Value,
        file_id: &str,
    ) -> Option<(String, Option<String>, Option<u64>)> {
        if let Some(file) = post
            .pointer("/metadata/files")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|file| file.get("id").and_then(serde_json::Value::as_str) == Some(file_id))
        {
            return Some((
                file.get("mime_type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("application/octet-stream")
                    .to_owned(),
                file.get("name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                file.get("size").and_then(serde_json::Value::as_u64),
            ));
        }
        if !valid_file_id(file_id) {
            return None;
        }
        let info = self
            .http_client()
            .get(format!("{}/api/v4/files/{file_id}/info", self.base_url))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?;
        if !info.status().is_success() {
            return None;
        }
        let info: serde_json::Value = info.json().await.ok()?;
        Some((
            info.get("mime_type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("application/octet-stream")
                .to_owned(),
            info.get("name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            info.get("size").and_then(serde_json::Value::as_u64),
        ))
    }

    async fn download_file(&self, file_id: &str, limit: u64) -> Option<Vec<u8>> {
        if !valid_file_id(file_id) {
            return None;
        }
        let mut response = self
            .http_client()
            .get(format!("{}/api/v4/files/{file_id}", self.base_url))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?;
        if !response.status().is_success()
            || response.content_length().is_some_and(|size| size > limit)
        {
            return None;
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if bytes.len() as u64 + chunk.len() as u64 > limit {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        (!bytes.is_empty()).then_some(bytes)
    }

    async fn listen_typed(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        cancellation: &mut tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<(), tinychannels_bus::channel::ChannelSendError> {
        use tinychannels_bus::ChannelIngress;
        use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

        let channel_id = self.channel_id.as_deref().ok_or_else(|| ChannelSendError {
            kind: SendErrorKind::BadFormat,
            message: "Mattermost channel_id required for listening".into(),
            ..Default::default()
        })?;
        if *cancellation.borrow() {
            return Ok(());
        }
        let (bot_user_id, bot_username) = tokio::select! {
            _ = cancellation.changed() => return Ok(()),
            identity = self.get_bot_identity() => identity,
        };
        #[allow(clippy::cast_possible_truncation)]
        let mut last_create_at = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()) as i64;

        loop {
            tokio::select! {
                _ = cancellation.changed() => return Ok(()),
                _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {}
            }
            let response = tokio::select! {
                _ = cancellation.changed() => return Ok(()),
                response = self.http_client()
                    .get(format!("{}/api/v4/channels/{channel_id}/posts", self.base_url))
                    .bearer_auth(&self.bot_token)
                    .query(&[("since", last_create_at.to_string())])
                    .send() => response,
            };
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!("Mattermost poll error: {error}");
                    continue;
                }
            };
            let data = tokio::select! {
                _ = cancellation.changed() => return Ok(()),
                data = response.json::<serde_json::Value>() => data,
            };
            let data = match data {
                Ok(data) => data,
                Err(error) => {
                    tracing::warn!("Mattermost parse error: {error}");
                    continue;
                }
            };
            let Some(posts) = data.get("posts").and_then(serde_json::Value::as_object) else {
                continue;
            };
            let batch_cursor = last_create_at;
            let mut next_cursor = batch_cursor;
            let mut post_list: Vec<_> = posts.values().collect();
            post_list.sort_by_key(|post| {
                post.get("create_at")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0)
            });
            for post in post_list {
                if *cancellation.borrow() {
                    return Ok(());
                }
                let create_at = post
                    .get("create_at")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(batch_cursor);
                next_cursor = next_cursor.max(create_at);
                let Some(admitted) =
                    self.admitted_post(post, &bot_user_id, &bot_username, batch_cursor)
                else {
                    continue;
                };
                let envelope = tokio::select! {
                    _ = cancellation.changed() => return Ok(()),
                    envelope = self.inbound_envelope_from_text(
                        post,
                        channel_id,
                        &admitted.content,
                        true,
                    ) => envelope,
                };
                let Some(envelope) = envelope else {
                    continue;
                };
                tokio::select! {
                    _ = cancellation.changed() => return Ok(()),
                    result = sink.push(ChannelIngress::Inbound(envelope)) => {
                        result?;
                    }
                }
            }
            last_create_at = next_cursor;
        }
    }

    /// Post one message and return the id Mattermost assigned to it.
    async fn post_message(&self, message: &SendMessage) -> Result<Vec<String>> {
        // Mattermost supports threading via 'root_id'.
        // We pack 'channel_id:root_id' into recipient if it's a thread.
        let (channel_id, root_id) = if let Some((c, r)) = message.recipient.split_once(':') {
            (c, Some(r))
        } else {
            (message.recipient.as_str(), None)
        };

        let mut body_map = serde_json::json!({
            "channel_id": channel_id,
            "message": message.content
        });

        if let Some(root) = root_id {
            body_map.as_object_mut().unwrap().insert(
                "root_id".to_string(),
                serde_json::Value::String(root.to_string()),
            );
        }

        let resp = self
            .http_client()
            .post(format!("{}/api/v4/posts", self.base_url))
            .bearer_auth(&self.bot_token)
            .json(&body_map)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response: {e}>"));
            bail!("Mattermost post failed ({status}): {body}");
        }

        let ids = resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|post| post.get("id").and_then(|id| id.as_str()).map(str::to_owned));
        Ok(ids.into_iter().collect())
    }
}

fn valid_file_id(file_id: &str) -> bool {
    !file_id.is_empty()
        && file_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[async_trait]
impl Channel for MattermostChannel {
    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        crate::providers::long_tail::LongTail::declared(self)
    }

    fn name(&self) -> &str {
        "mattermost"
    }

    fn supports_remote_control(&self) -> bool {
        true
    }

    fn supports_chat_approvals(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> Result<()> {
        self.post_message(message).await.map(|_| ())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> Result<()> {
        let channel_id = self
            .channel_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Mattermost channel_id required for listening"))?;

        let (bot_user_id, bot_username) = self.get_bot_identity().await;
        #[allow(clippy::cast_possible_truncation)]
        let mut last_create_at = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()) as i64;

        tracing::info!("Mattermost channel listening on {}...", channel_id);

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;

            let resp = match self
                .http_client()
                .get(format!(
                    "{}/api/v4/channels/{}/posts",
                    self.base_url, channel_id
                ))
                .bearer_auth(&self.bot_token)
                .query(&[("since", last_create_at.to_string())])
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Mattermost poll error: {e}");
                    continue;
                }
            };

            let data: serde_json::Value = match resp.json().await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("Mattermost parse error: {e}");
                    continue;
                }
            };

            if let Some(posts) = data.get("posts").and_then(|p| p.as_object()) {
                // Process in chronological order
                let mut post_list: Vec<_> = posts.values().collect();
                post_list.sort_by_key(|p| p.get("create_at").and_then(|c| c.as_i64()).unwrap_or(0));

                for post in post_list {
                    let msg = self.parse_mattermost_post(
                        post,
                        &bot_user_id,
                        &bot_username,
                        last_create_at,
                        &channel_id,
                    );
                    let create_at = post
                        .get("create_at")
                        .and_then(|c| c.as_i64())
                        .unwrap_or(last_create_at);
                    last_create_at = last_create_at.max(create_at);

                    let typed_delivered = if let Some(message) = msg.as_ref() {
                        if let Some(envelope) =
                            self.inbound_envelope(post, &channel_id, message).await
                        {
                            crate::providers::native::inbound::publish(
                                "mattermost",
                                self.events.as_ref(),
                                envelope,
                            )
                            .await
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if !typed_delivered
                        && let Some(channel_msg) = msg.filter(|message| !message.content.is_empty())
                        && tx.send(channel_msg).await.is_err()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn health_check(&self) -> bool {
        self.http_client()
            .get(format!("{}/api/v4/users/me", self.base_url))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    async fn start_typing(&self, recipient: &str) -> Result<()> {
        self.start_typing_loop(recipient).await
    }

    async fn stop_typing(&self, _recipient: &str) -> Result<()> {
        self.stop_typing_loop()
    }
}

impl MattermostChannel {
    async fn start_typing_loop(&self, recipient: &str) -> Result<()> {
        self.stop_typing_loop()?;
        let client = self.http_client();
        let token = self.bot_token.clone();
        let base_url = self.base_url.clone();
        let (channel_id, parent_id) = match recipient.split_once(':') {
            Some((channel, parent)) => (channel.to_string(), Some(parent.to_string())),
            None => (recipient.to_string(), None),
        };
        let handle = tokio::spawn(async move {
            let url = format!("{base_url}/api/v4/users/me/typing");
            loop {
                let mut body = serde_json::json!({ "channel_id": channel_id });
                if let Some(ref pid) = parent_id {
                    body.as_object_mut()
                        .unwrap()
                        .insert("parent_id".to_string(), serde_json::json!(pid));
                }
                if let Ok(response) = client
                    .post(&url)
                    .bearer_auth(&token)
                    .json(&body)
                    .send()
                    .await
                    && !response.status().is_success()
                {
                    tracing::debug!(status = %response.status(), "Mattermost typing indicator failed");
                }
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;
            }
        });
        *self.typing_handle.lock() = Some(handle);
        Ok(())
    }

    fn stop_typing_loop(&self) -> Result<()> {
        let mut guard = self.typing_handle.lock();
        if let Some(handle) = guard.take() {
            handle.abort();
        }
        Ok(())
    }
}

impl MattermostChannel {
    fn parse_mattermost_post(
        &self,
        post: &serde_json::Value,
        bot_user_id: &str,
        bot_username: &str,
        last_create_at: i64,
        channel_id: &str,
    ) -> Option<ChannelMessage> {
        let admitted = self.admitted_post(post, bot_user_id, bot_username, last_create_at)?;

        // Reply routing depends on thread_replies config:
        //   - Existing thread (root_id set): always stay in the thread.
        //   - Top-level post + thread_replies=true: thread on the original post.
        //   - Top-level post + thread_replies=false: reply at channel level.
        let reply_target = if !admitted.root_id.is_empty() {
            format!("{}:{}", channel_id, admitted.root_id)
        } else if self.thread_replies {
            format!("{}:{}", channel_id, admitted.id)
        } else {
            channel_id.to_string()
        };

        Some(ChannelMessage {
            id: format!("mattermost_{}", admitted.id),
            sender: admitted.user_id,
            reply_target,
            content: admitted.content,
            channel: "mattermost".to_string(),
            #[allow(clippy::cast_sign_loss)]
            timestamp: (admitted.create_at / 1000) as u64,
            topic_id: None,
            reply_to_id: None,
            thread_ts: None,
            sender_name: None,
        })
    }

    fn admitted_post(
        &self,
        post: &serde_json::Value,
        bot_user_id: &str,
        bot_username: &str,
        last_create_at: i64,
    ) -> Option<AdmittedPost> {
        let id = post
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        let user_id = post
            .get("user_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        let text = post
            .get("message")
            .and_then(|message| message.as_str())
            .unwrap_or("");
        let create_at = post
            .get("create_at")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let root_id = post
            .get("root_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        let has_files = post
            .get("file_ids")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|files| !files.is_empty());
        if user_id == bot_user_id || create_at <= last_create_at || (text.is_empty() && !has_files)
        {
            return None;
        }
        if !self.is_user_allowed(&user_id) {
            tracing::warn!("Mattermost: ignoring message from unauthorized user: {user_id}");
            return None;
        }
        let content = if self.mention_only {
            normalize_mattermost_content(text, bot_user_id, bot_username, post)?
        } else {
            text.to_owned()
        };
        Some(AdmittedPost {
            id,
            user_id,
            create_at,
            root_id,
            content,
        })
    }
}

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
        capabilities.actions.supported.extend(["edit", "delete"].map(String::from));
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
    ) -> Result<tinychannels_bus::channel::MessageReceipt, tinychannels_bus::channel::ChannelSendError>
    {
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

#[path = "mattermost/native.rs"]
mod native;

crate::providers::long_tail::impl_long_tail_adapter!(MattermostChannel);

#[cfg(test)]
#[path = "mattermost_tests.rs"]
mod tests;

#[cfg(any(test, debug_assertions))]
#[path = "mattermost_test_support_tests.rs"]
pub mod test_support;
