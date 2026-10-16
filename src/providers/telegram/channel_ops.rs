//! Telegram channel — `Channel` trait implementation: send, listen, draft streaming, typing.

use super::attachments::{parse_attachment_markers, parse_path_only_attachment};
use super::channel_types::{TelegramChannel, TelegramTypingTask};
use super::text::{TELEGRAM_MAX_MESSAGE_LENGTH, strip_tool_call_tags};
use crate::config::StreamMode;
use crate::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use std::time::Duration;

impl TelegramChannel {
    /// One iteration of the background typing refresher: post the chat action
    /// and log (never fail) on rejection or transport error. Returns whether
    /// Telegram accepted the refresh.
    pub(crate) async fn refresh_typing_once(
        client: &reqwest::Client,
        url: &str,
        body: &serde_json::Value,
        recipient: &str,
    ) -> bool {
        match client.post(url).json(body).send().await {
            Ok(resp) => {
                let accepted = Self::telegram_api_ok(resp).await;
                if !accepted {
                    tracing::warn!(recipient, "Telegram typing refresh rejected");
                }
                accepted
            }
            Err(error) => {
                tracing::warn!(
                    recipient,
                    %error,
                    "Telegram typing refresh request failed"
                );
                false
            }
        }
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn name(&self) -> &str {
        "telegram"
    }

    fn supports_remote_control(&self) -> bool {
        true
    }

    fn supports_chat_approvals(&self) -> bool {
        true
    }

    fn history_key_ignores_thread(&self) -> bool {
        true
    }

    /// Recipient-less proactive sends (cron/heartbeat) deliver to the bot's
    /// configured default `chat_id`. `None` when unconfigured, so proactive
    /// routing skips Telegram rather than letting `send` POST to an empty
    /// `chat_id` (mirrors Discord — #3712 Telegram parity).
    fn proactive_target(&self) -> Option<String> {
        self.chat_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    }

    fn supports_reactions(&self) -> bool {
        true
    }

    fn supports_draft_updates(&self) -> bool {
        self.stream_mode != StreamMode::Off
    }

    async fn send_draft(&self, message: &SendMessage) -> anyhow::Result<Option<String>> {
        if self.stream_mode == StreamMode::Off {
            return Ok(None);
        }

        let (chat_id, thread_id) = Self::parse_reply_target(&message.recipient);
        let parent_message_id = Self::parse_message_id(message.thread_ts.as_deref());
        let initial_text = if message.content.is_empty() {
            "...".to_string()
        } else {
            message.content.clone()
        };

        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "text": initial_text,
        });
        if let Some(tid) = thread_id {
            body["message_thread_id"] = serde_json::Value::String(tid.to_string());
        }
        if let Some(parent_id) = parent_message_id {
            body["reply_to_message_id"] = serde_json::Value::from(parent_id);
        }

        let resp = self
            .client
            .post(self.api_url("sendMessage"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("Telegram sendMessage (draft) failed: {err}");
        }

        let resp_json: serde_json::Value = resp.json().await?;
        let message_id = resp_json
            .get("result")
            .and_then(|r| r.get("message_id"))
            .and_then(|id| id.as_i64())
            .map(|id| id.to_string());

        self.last_draft_edit
            .lock()
            .insert(chat_id.to_string(), std::time::Instant::now());

        Ok(message_id)
    }

    async fn update_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        let (chat_id, _) = Self::parse_reply_target(recipient);

        // Rate-limit edits per chat
        {
            let last_edits = self.last_draft_edit.lock();
            if let Some(last_time) = last_edits.get(&chat_id) {
                let elapsed = u64::try_from(last_time.elapsed().as_millis()).unwrap_or(u64::MAX);
                if elapsed < self.draft_update_interval_ms {
                    return Ok(());
                }
            }
        }

        // Truncate to Telegram limit for mid-stream edits (UTF-8 safe)
        let display_text = if text.len() > TELEGRAM_MAX_MESSAGE_LENGTH {
            let mut end = 0;
            for (idx, ch) in text.char_indices() {
                let next = idx + ch.len_utf8();
                if next > TELEGRAM_MAX_MESSAGE_LENGTH {
                    break;
                }
                end = next;
            }
            &text[..end]
        } else {
            text
        };

        let message_id_parsed = match message_id.parse::<i64>() {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!("Invalid Telegram message_id '{message_id}': {e}");
                return Ok(());
            }
        };

        let body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id_parsed,
            "text": display_text,
        });

        let resp = self
            .client
            .post(self.api_url("editMessageText"))
            .json(&body)
            .send()
            .await?;

        if resp.status().is_success() {
            self.last_draft_edit
                .lock()
                .insert(chat_id.clone(), std::time::Instant::now());
        } else {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();
            tracing::debug!("Telegram editMessageText failed ({status}): {err}");
        }

        Ok(())
    }

    async fn finalize_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
        thread_ts: Option<&str>,
    ) -> anyhow::Result<()> {
        let text = &strip_tool_call_tags(text);
        let (chat_id, thread_id) = Self::parse_reply_target(recipient);
        let parent_message_id = Self::parse_message_id(thread_ts);

        // Clean up rate-limit tracking for this chat
        self.last_draft_edit.lock().remove(&chat_id);

        // If text exceeds limit, delete draft and send as chunked messages
        if text.len() > TELEGRAM_MAX_MESSAGE_LENGTH {
            let msg_id = match message_id.parse::<i64>() {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!("Invalid Telegram message_id '{message_id}': {e}");
                    return self
                        .send_text_chunks(text, &chat_id, thread_id.as_deref(), parent_message_id)
                        .await;
                }
            };

            // Delete the draft
            let _ = self
                .client
                .post(self.api_url("deleteMessage"))
                .json(&serde_json::json!({
                    "chat_id": chat_id,
                    "message_id": msg_id,
                }))
                .send()
                .await;

            // Fall back to chunked send
            return self
                .send_text_chunks(text, &chat_id, thread_id.as_deref(), parent_message_id)
                .await;
        }

        let msg_id = match message_id.parse::<i64>() {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!("Invalid Telegram message_id '{message_id}': {e}");
                return self
                    .send_text_chunks(text, &chat_id, thread_id.as_deref(), parent_message_id)
                    .await;
            }
        };

        // Try editing with Markdown formatting
        let body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": msg_id,
            "text": text,
            "parse_mode": "Markdown",
        });

        let resp = self
            .client
            .post(self.api_url("editMessageText"))
            .json(&body)
            .send()
            .await?;

        if resp.status().is_success() {
            return Ok(());
        }

        // Markdown failed — retry without parse_mode
        let plain_body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": msg_id,
            "text": text,
        });

        let resp = self
            .client
            .post(self.api_url("editMessageText"))
            .json(&plain_body)
            .send()
            .await?;

        if resp.status().is_success() {
            return Ok(());
        }

        // Edit failed entirely — fall back to new message
        tracing::warn!("Telegram finalize_draft edit failed; falling back to sendMessage");
        self.send_text_chunks(text, &chat_id, thread_id.as_deref(), parent_message_id)
            .await
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        // Strip tool_call tags before processing to prevent Markdown parsing failures
        let content = strip_tool_call_tags(&message.content);
        let parent_message_id = Self::parse_message_id(message.thread_ts.as_deref());

        // Parse recipient: "chat_id" or "chat_id:thread_id" format
        let (chat_id, thread_id) = match message.recipient.split_once(':') {
            Some((chat, thread)) => (chat, Some(thread)),
            None => (message.recipient.as_str(), None),
        };

        let (reactionless_content, reaction_marker) = Self::parse_reaction_marker(&content);
        if let Some(reaction_marker) = reaction_marker.as_deref() {
            let (emoji, explicit_target_id) = match reaction_marker.split_once('|') {
                Some((emoji, target)) => (emoji.trim(), Self::parse_message_id(Some(target))),
                None => (reaction_marker.trim(), None),
            };
            let target_message_id = explicit_target_id.or(parent_message_id);
            if let Some(target_id) = target_message_id {
                let _ = self
                    .send_message_reaction(chat_id, target_id, emoji)
                    .await?;
                tracing::debug!(
                    chat_id,
                    target_id,
                    emoji,
                    has_reply = !reactionless_content.is_empty(),
                    "[telegram] reaction sent; continuing to send reply text if present"
                );
            } else {
                tracing::warn!(
                    recipient = message.recipient,
                    marker = reaction_marker,
                    "[telegram] reaction marker ignored: missing target message id"
                );
            }
            // If no text follows the reaction marker, we are done.
            if reactionless_content.trim().is_empty() {
                return Ok(());
            }
        }

        let (text_without_markers, attachments) = parse_attachment_markers(&reactionless_content);

        if !attachments.is_empty() {
            if !text_without_markers.is_empty() {
                self.send_text_chunks(&text_without_markers, chat_id, thread_id, parent_message_id)
                    .await?;
            }

            for attachment in &attachments {
                self.send_attachment(chat_id, thread_id, attachment).await?;
            }

            return Ok(());
        }

        if let Some(attachment) = parse_path_only_attachment(&reactionless_content) {
            self.send_attachment(chat_id, thread_id, &attachment)
                .await?;
            return Ok(());
        }

        self.send_text_chunks(&reactionless_content, chat_id, thread_id, parent_message_id)
            .await
    }

    async fn send_typed_with_receipt(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<tinychannels_bus::channel::ChannelSendOutcome> {
        let text = match message.parts.as_slice() {
            [
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text { text, .. },
                },
            ] => text,
            _ => {
                self.send_typed(message).await?;
                return Ok(tinychannels_bus::channel::ChannelSendOutcome::default());
            }
        };
        let mut conversation =
            message
                .conversation_ref
                .clone()
                .unwrap_or_else(|| tinychannels_bus::ConversationRef {
                    id: message.conversation_id.clone(),
                    thread_id: message.thread_id.clone(),
                    ..Default::default()
                });
        if conversation.thread_id.is_none() {
            conversation.thread_id = message.thread_id.clone();
        }
        let thread_id = conversation
            .thread_id
            .as_deref()
            .or(conversation.topic_id.as_deref())
            .or(message.thread_id.as_deref())
            .map(str::to_owned);
        let reply_to_id = match message.reply_to_id.as_deref() {
            Some(id) => Some(id.parse::<i64>().map_err(|_| {
                anyhow::anyhow!("Telegram reply_to_id must be a numeric message ID")
            })?),
            None => None,
        };
        let (ids, error) = match self
            .send_text_chunks_with_ids(text, &conversation.id, thread_id.as_deref(), reply_to_id)
            .await
        {
            Ok(ids) => (ids, None),
            Err(error) => match error.downcast_ref::<super::channel_send::PartialTextSendError>() {
                Some(partial) => (partial.message_ids.clone(), Some(partial.error.clone())),
                None => return Err(error),
            },
        };
        let parts = ids
            .iter()
            .enumerate()
            .map(
                |(index, id)| tinychannels_bus::channel::MessageReceiptPart {
                    platform_message_id: id.clone(),
                    kind: tinychannels_bus::channel::MessageReceiptPartKind::Text,
                    index,
                    thread_id: thread_id.clone(),
                    reply_to_id: message.reply_to_id.clone(),
                    edit_token: Some(id.clone()),
                    delete_token: Some(id.clone()),
                    raw: Some(tinychannels_bus::channel::MessageReceiptSourceResult {
                        channel: Some("telegram".into()),
                        message_id: Some(id.clone()),
                        chat_id: Some(conversation.id.clone()),
                        ..Default::default()
                    }),
                },
            )
            .collect::<Vec<_>>();
        let single_part = (parts.len() == 1).then(|| parts[0].platform_message_id.clone());
        let receipt = tinychannels_bus::channel::MessageReceipt {
            channel: Some(tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: None,
            }),
            conversation: Some(conversation),
            primary_platform_message_id: parts.first().map(|part| part.platform_message_id.clone()),
            platform_message_ids: ids,
            parts,
            thread_id,
            reply_to_id: message.reply_to_id.clone(),
            edit_token: single_part.clone(),
            delete_token: single_part,
            sent_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            raw: None,
        };
        Ok(tinychannels_bus::channel::ChannelSendOutcome { receipt, error })
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        let mut offset: i64 = 0;

        if self.mention_only {
            let _ = self.get_bot_username().await;
        }

        tracing::info!("Telegram channel listening for messages...");

        loop {
            if self.mention_only {
                let missing_username = self.bot_username.lock().is_none();
                if missing_username {
                    let _ = self.get_bot_username().await;
                }
            }

            let url = self.api_url("getUpdates");
            let body = serde_json::json!({
                "offset": offset,
                "timeout": 30,
                "allowed_updates": ["message", "edited_message", "message_reaction"]
            });

            let resp = match self.http_client().post(&url).json(&body).send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Telegram poll error: {e}");
                    tokio::time::sleep(self.poll_error_backoff).await;
                    continue;
                }
            };

            let data: serde_json::Value = match resp.json().await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("Telegram parse error: {e}");
                    tokio::time::sleep(self.poll_error_backoff).await;
                    continue;
                }
            };

            let ok = data
                .get("ok")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            if !ok {
                let error_code = data
                    .get("error_code")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or_default();
                let description = data
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown Telegram API error");

                if error_code == 409 {
                    let webhook_blocks_polling = description.to_lowercase().contains("webhook");
                    if webhook_blocks_polling {
                        tracing::warn!(
                            "[telegram] getUpdates conflict (409): webhook is active; calling deleteWebhook"
                        );
                        if self.delete_webhook_for_long_polling().await {
                            tracing::info!("[telegram] deleteWebhook ok; retrying getUpdates");
                            continue;
                        }
                        tracing::warn!("[telegram] deleteWebhook did not succeed; backing off");
                    } else {
                        tracing::warn!(
                            "Telegram polling conflict (409): {description}. \
Ensure only one `openhuman` process is using this bot token."
                        );
                    }
                    tokio::time::sleep(self.poll_conflict_backoff).await;
                } else {
                    tracing::warn!(
                        "Telegram getUpdates API error (code={}): {description}",
                        error_code
                    );
                    tokio::time::sleep(self.poll_error_backoff).await;
                }
                continue;
            }

            if let Some(results) = data.get("result").and_then(serde_json::Value::as_array) {
                for update in results {
                    let update_id = update
                        .get("update_id")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or_default();
                    if update_id > 0 && !self.track_update_id(update_id) {
                        continue;
                    }

                    // Advance offset past this update
                    if let Some(uid) = update.get("update_id").and_then(serde_json::Value::as_i64) {
                        offset = uid + 1;
                    }

                    let reactions = self.parse_update_reactions(update);
                    if update.get("message_reaction").is_some() {
                        if !reactions.is_empty() {
                            for reaction in reactions {
                                tracing::info!(
                                    sender = reaction.sender,
                                    reply_target = reaction.reply_target,
                                    target_message_id = reaction.target_message_id,
                                    emoji = reaction.emoji,
                                    "Telegram reaction received"
                                );
                                if let Some(events) = &self.events {
                                    let source_id = reaction.update_id.clone();
                                    let conversation = tinychannels_bus::ConversationRef {
                                        kind: reaction.conversation_kind,
                                        id: reaction.reply_target.clone(),
                                        topic_id: reaction.thread_id.clone(),
                                        ..Default::default()
                                    };
                                    let reaction_kind = if reaction.added {
                                        tinychannels_bus::ChannelEventKind::ReactionAdded {
                                            message_id: format!("telegram_{}_{}", reaction.reply_target, reaction.target_message_id),
                                            provider_message_id: Some(reaction.target_message_id.clone()),
                                            actor: tinychannels_bus::SenderRef {
                                                id: reaction.sender_id.clone(),
                                                name: Some(reaction.sender.clone()),
                                                ..Default::default()
                                            },
                                            reaction: tinychannels_bus::ChannelReaction {
                                                value: reaction.emoji.clone(),
                                                custom_emoji_id: reaction.custom_emoji_id.clone(),
                                            },
                                        }
                                    } else {
                                        tinychannels_bus::ChannelEventKind::ReactionRemoved {
                                            message_id: format!("telegram_{}_{}", reaction.reply_target, reaction.target_message_id),
                                            provider_message_id: Some(reaction.target_message_id.clone()),
                                            actor: tinychannels_bus::SenderRef {
                                                id: reaction.sender_id.clone(),
                                                name: Some(reaction.sender.clone()),
                                                ..Default::default()
                                            },
                                            reaction: tinychannels_bus::ChannelReaction {
                                                value: reaction.emoji.clone(),
                                                custom_emoji_id: reaction.custom_emoji_id.clone(),
                                            },
                                        }
                                    };
                                    let event = tinychannels_bus::ChannelEvent {
                                        version: 2,
                                        event_id: source_id
                                            .as_ref()
                                            .map(|id| {
                                                format!(
                                                    "telegram:update:{id}:reaction:{}:{}:{}:{}",
                                                    if reaction.added {
                                                        "added"
                                                    } else {
                                                        "removed"
                                                    },
                                                    reaction.reply_target,
                                                    reaction.target_message_id,
                                                    reaction.emoji
                                                )
                                            })
                                            .unwrap_or_else(|| {
                                                format!(
                                                    "telegram:reaction:{}:{}:{}:{}:{}",
                                                    reaction.reply_target,
                                                    reaction.target_message_id,
                                                    reaction.sender_id,
                                                    reaction.emoji,
                                                    if reaction.added {
                                                        "added"
                                                    } else {
                                                        "removed"
                                                    }
                                                )
                                            }),
                                        source_event_id: source_id,
                                        source_timestamp: reaction.timestamp,
                                        channel: tinychannels_bus::ChannelRef {
                                            id: "telegram".into(),
                                            account_id: None,
                                        },
                                        conversation: Some(conversation),
                                        event: reaction_kind,
                                    };
                                    if let Err(error) = events.publish_ingress(tinychannels_bus::ChannelIngress::Event(event)).await {
                                        tracing::debug!(%error, "failed to deliver Telegram reaction event");
                                    }
                                }
                            }
                            continue;
                        }
                        continue;
                    }

                    // Telegram edits arrive as a distinct update field. Keep
                    // them out of the inbound-new-message path even when no
                    // host event sink is installed.
                    if let Some(edited) = update.get("edited_message") {
                        if let Some(message) = self.parse_update_message(update) {
                            if let Some(events) = &self.events {
                                let source_id = update
                                    .get("update_id")
                                    .and_then(serde_json::Value::as_i64)
                                    .map(|id| id.to_string());
                                let thread_id = edited
                                    .get("message_thread_id")
                                    .and_then(serde_json::Value::as_i64)
                                    .map(|id| id.to_string());
                                let event = tinychannels_bus::ChannelEvent {
                                    version: 2,
                                    event_id: source_id
                                        .as_ref()
                                        .map(|id| {
                                            format!("telegram:update:{id}:edit:{}", message.id)
                                        })
                                        .unwrap_or_else(|| format!("telegram:edit:{}", message.id)),
                                    source_event_id: source_id,
                                    source_timestamp: edited
                                        .get("edit_date")
                                        .and_then(serde_json::Value::as_u64)
                                        .map(|seconds| seconds.saturating_mul(1000)),
                                    channel: tinychannels_bus::ChannelRef {
                                        id: "telegram".into(),
                                        account_id: None,
                                    },
                                    conversation: Some(tinychannels_bus::ConversationRef {
                                        kind: match edited
                                            .get("chat")
                                            .and_then(|chat| chat.get("type"))
                                            .and_then(serde_json::Value::as_str)
                                        {
                                            Some("private") => {
                                                tinychannels_bus::ConversationKind::Dm
                                            }
                                            Some("group" | "supergroup") => {
                                                tinychannels_bus::ConversationKind::Group
                                            }
                                            Some("channel") => {
                                                tinychannels_bus::ConversationKind::Channel
                                            }
                                            _ => tinychannels_bus::ConversationKind::Unknown,
                                        },
                                        id: message.reply_target.clone(),
                                        topic_id: thread_id,
                                        ..Default::default()
                                    }),
                                    event: tinychannels_bus::ChannelEventKind::MessageUpdated {
                                        message_id: message.id,
                                        provider_message_id: edited.get("message_id").and_then(serde_json::Value::as_i64).map(|id| id.to_string()),
                                        parts: vec![tinychannels_bus::ContentPart::Text {
                                            text: message.content,
                                            markdown_dialect: None,
                                        }],
                                    },
                                };
                                if let Err(error) = events.publish_ingress(tinychannels_bus::ChannelIngress::Event(event)).await {
                                    tracing::debug!(%error, "failed to deliver Telegram edit event");
                                }
                            }
                        }
                        continue;
                    }

                    let Some(msg) = self.parse_update_message_or_voice(update).await else {
                        self.handle_unauthorized_message(update).await;
                        continue;
                    };

                    let delivered_by_event_sink = if let Some(events) = &self.events {
                        match events.publish_inbound(msg.clone()).await {
                            Ok(delivered) => delivered,
                            Err(error) => {
                                tracing::debug!(%error, "failed to deliver Telegram inbound through module ingress");
                                false
                            }
                        }
                    } else {
                        false
                    };
                    if !delivered_by_event_sink && tx.send(msg).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn health_check(&self) -> bool {
        let timeout_duration = Duration::from_secs(5);

        match tokio::time::timeout(
            timeout_duration,
            self.http_client().get(self.api_url("getMe")).send(),
        )
        .await
        {
            Ok(Ok(resp)) => resp.status().is_success(),
            Ok(Err(e)) => {
                tracing::debug!("Telegram health check failed: {e}");
                false
            }
            Err(_) => {
                tracing::debug!("Telegram health check timed out after 5s");
                false
            }
        }
    }

    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        tracing::info!(recipient, "Telegram start_typing invoked");
        // Emit immediately so short model turns still show "typing…"
        self.send_typing_action_once(recipient).await;

        {
            let guard = self.typing_handle.lock();
            if guard
                .as_ref()
                .is_some_and(|task| task.recipient == recipient)
            {
                return Ok(());
            }
        }
        self.stop_typing(recipient).await?;

        let client = self.http_client();
        let url = self.api_url("sendChatAction");
        let recipient_owned = recipient.to_string();
        let recipient_for_log = recipient_owned.clone();
        let body = Self::typing_body_for_recipient(recipient);

        let handle = tokio::spawn(async move {
            loop {
                Self::refresh_typing_once(&client, &url, &body, &recipient_for_log).await;
                // Telegram typing indicator expires after 5s; refresh at 4s
                tokio::time::sleep(Duration::from_secs(4)).await;
            }
        });

        let mut guard = self.typing_handle.lock();
        *guard = Some(TelegramTypingTask {
            recipient: recipient_owned,
            handle,
        });

        Ok(())
    }

    async fn stop_typing(&self, _recipient: &str) -> anyhow::Result<()> {
        tracing::info!("Telegram stop_typing invoked");
        let mut guard = self.typing_handle.lock();
        if let Some(task) = guard.take() {
            task.handle.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "channel_ops_tests.rs"]
mod tests;
