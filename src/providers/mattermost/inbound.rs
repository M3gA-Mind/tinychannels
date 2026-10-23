//! Mattermost REST polling, post parsing, and media intake.

use super::*;

impl MattermostChannel {
    pub(super) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    /// Check if a user ID is in the allowlist.
    /// Empty list means deny everyone. "*" means allow everyone.
    pub(super) fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Get the bot's own user ID and username so we can ignore our own messages
    /// and detect @-mentions by username.
    pub(super) async fn get_bot_identity(&self) -> (String, String) {
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
    #[cfg(test)]
    pub(super) async fn inbound_envelope(
        &self,
        post: &serde_json::Value,
        channel_id: &str,
        message: &ChannelMessage,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        self.inbound_envelope_from_text(post, channel_id, &message.content, false)
            .await
    }

    pub(super) async fn inbound_envelope_from_text(
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

    pub(super) async fn listen_typed(
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
                    result = sink.push_ingress(ChannelIngress::Inbound(envelope)) => {
                        result?;
                    }
                }
            }
            last_create_at = next_cursor;
        }
    }
}

pub(super) fn valid_file_id(file_id: &str) -> bool {
    !file_id.is_empty()
        && file_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

impl MattermostChannel {
    pub(super) fn parse_mattermost_post(
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

    pub(super) fn admitted_post(
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
pub(super) fn contains_bot_mention_mm(
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
pub(super) fn normalize_mattermost_content(
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
