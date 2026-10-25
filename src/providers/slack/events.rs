//! Dispatch of Slack events (messages, edits, deletes, reactions, button
//! presses) onto the host's ordered ingress path.

use super::inbound::{slack_channel_ref, ts_to_ms};
use super::types::SlackChannel;
use crate::channel::{ChannelInboundSink, ChannelSendError};
use crate::traits::ChannelMessage;
use serde_json::Value;
use tinychannels_bus::{
    ChannelEvent, ChannelEventKind, ChannelInboundEnvelope, ChannelIngress, ChannelReaction,
    ContentPart, ConversationRef,
};
use tokio::sync::mpsc::Sender;

/// Whether the listener should keep running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flow {
    Continue,
    Stop,
}

const REACTION_EMOJI: [(&str, &str); 14] = [
    ("thumbsup", "👍"),
    ("+1", "👍"),
    ("thumbsdown", "👎"),
    ("-1", "👎"),
    ("heart", "❤️"),
    ("eyes", "👀"),
    ("white_check_mark", "✅"),
    ("x", "❌"),
    ("tada", "🎉"),
    ("fire", "🔥"),
    ("rocket", "🚀"),
    ("pray", "🙏"),
    ("joy", "😂"),
    ("thinking_face", "🤔"),
];

/// A Slack reaction name as the emoji it is known by, else `:name:`.
pub(crate) fn emoji_for_name(name: &str) -> String {
    REACTION_EMOJI
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, emoji)| (*emoji).to_owned())
        .unwrap_or_else(|| format!(":{name}:"))
}

/// The Slack reaction name for an emoji, `:name:` shortcode or bare name.
pub(crate) fn name_for_emoji(value: &str) -> Option<String> {
    let value = value.trim();
    if let Some((name, _)) = REACTION_EMOJI.iter().find(|(_, emoji)| *emoji == value) {
        return Some((*name).to_owned());
    }
    let name = value.trim_matches(':');
    (!name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '+')))
    .then(|| name.to_owned())
}

pub(crate) fn provider_message_id(channel: &str, ts: &str) -> String {
    format!("slack_{channel}_{ts}")
}

fn str_of<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

impl SlackChannel {
    fn channel_event(
        event_id: String,
        source_timestamp: Option<u64>,
        conversation: Option<ConversationRef>,
        event: ChannelEventKind,
    ) -> ChannelEvent {
        ChannelEvent {
            version: 2,
            source_event_id: None,
            source_timestamp,
            event_id,
            channel: slack_channel_ref(),
            conversation,
            event,
        }
    }

    async fn publish_event(
        &self,
        event: ChannelEvent,
        typed_sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> Result<bool, ChannelSendError> {
        if let Some(sink) = typed_sink {
            sink.push_ingress(ChannelIngress::Event(event)).await?;
            return Ok(true);
        }
        let Some(events) = &self.events else {
            return Ok(false);
        };
        match events.publish_ingress(ChannelIngress::Event(event)).await {
            Ok(delivered) => Ok(delivered),
            Err(error) => {
                tracing::debug!(%error, "[slack] failed to deliver channel event");
                Ok(false)
            }
        }
    }

    /// A message from `channel` is in scope when no default channel is
    /// configured, it is the configured channel, or it is a direct message.
    fn channel_in_scope(&self, channel: &str, channel_type: Option<&str>) -> bool {
        self.channel_id
            .as_deref()
            .is_none_or(|configured| configured == channel || channel_type == Some("im"))
    }

    /// Handle one Events API / history item.
    #[cfg(test)]
    pub(crate) async fn handle_event(
        &self,
        event: &Value,
        tx: Option<&Sender<ChannelMessage>>,
    ) -> Flow {
        self.handle_event_to_sink(event, tx, None)
            .await
            .unwrap_or_else(|error| {
                tracing::debug!(message = %error.message, "[slack] event delivery failed");
                Flow::Continue
            })
    }

    pub(crate) async fn handle_event_typed(
        &self,
        event: &Value,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        self.handle_event_to_sink(event, None, Some(sink))
            .await
            .map(|_| ())
    }

    pub(crate) async fn handle_socket_delivery(
        &self,
        kind: &str,
        payload: &Value,
        envelope_id: Option<&str>,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        let dedupe_key = envelope_id.map(|id| format!("socket-envelope:{id}"));
        if dedupe_key.as_deref().is_some_and(|key| self.has_seen(key)) {
            return Ok(());
        }
        match kind {
            "events_api" => {
                let event = payload.get("event").unwrap_or(&Value::Null);
                self.handle_event_typed(event, sink).await?;
            }
            "interactive" => self.handle_interactive_to_sink(payload, Some(sink)).await?,
            _ => {}
        }
        if let Some(dedupe_key) = dedupe_key {
            self.mark_seen(dedupe_key);
        }
        Ok(())
    }

    async fn handle_event_to_sink(
        &self,
        event: &Value,
        tx: Option<&Sender<ChannelMessage>>,
        typed_sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> Result<Flow, ChannelSendError> {
        match str_of(event, "type") {
            "message" | "app_mention" => match str_of(event, "subtype") {
                "message_changed" => self.handle_changed(event, typed_sink).await?,
                "message_deleted" => self.handle_deleted(event, typed_sink).await?,
                "bot_message" => {}
                _ => return self.handle_message(event, tx, typed_sink).await,
            },
            "reaction_added" => self.handle_reaction(event, true, typed_sink).await?,
            "reaction_removed" => self.handle_reaction(event, false, typed_sink).await?,
            _ => {}
        }
        Ok(Flow::Continue)
    }

    async fn handle_message(
        &self,
        event: &Value,
        tx: Option<&Sender<ChannelMessage>>,
        typed_sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> Result<Flow, ChannelSendError> {
        let channel = str_of(event, "channel");
        let ts = str_of(event, "ts");
        let user = str_of(event, "user");
        let channel_type = event.get("channel_type").and_then(Value::as_str);
        if event.get("bot_id").is_some() || user.is_empty() || channel.is_empty() || ts.is_empty() {
            return Ok(Flow::Continue);
        }
        if self.bot_user_id().await.as_deref() == Some(user) {
            return Ok(Flow::Continue);
        }
        if !self.is_user_allowed(user) {
            tracing::warn!("[slack] ignoring message from an unauthorized user");
            return Ok(Flow::Continue);
        }
        if !self.channel_in_scope(channel, channel_type) {
            return Ok(Flow::Continue);
        }
        let text = str_of(event, "text");
        let has_files = event
            .get("files")
            .and_then(Value::as_array)
            .is_some_and(|files| !files.is_empty());
        let seen_key = format!("{channel}:{ts}");
        if (text.is_empty() && !has_files) || self.has_seen(&seen_key) {
            return Ok(Flow::Continue);
        }
        let thread_ts = Self::inbound_thread_ts(event, ts);
        let message_id = provider_message_id(channel, ts);
        let message = ChannelMessage {
            id: message_id.clone(),
            sender: user.to_owned(),
            reply_target: channel.to_owned(),
            content: text.to_owned(),
            channel: "slack".into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            topic_id: None,
            reply_to_id: None,
            thread_ts: thread_ts.clone(),
            sender_name: None,
        };
        let conversation = Self::conversation_ref(channel, channel_type, thread_ts);
        let (media_parts, voice_note) = self.media_parts(event, &message_id, &conversation).await;
        let mut parts = Vec::new();
        if !text.trim().is_empty() {
            parts.push(ContentPart::Text {
                text: text.to_owned(),
                markdown_dialect: Some("slack_mrkdwn".into()),
            });
        }
        if let Some(voice_note) = voice_note.clone() {
            parts.push(ContentPart::VoiceNote { voice_note });
        }
        parts.extend(media_parts);
        let media = parts
            .iter()
            .flat_map(|part| match part {
                ContentPart::Image { media, .. }
                | ContentPart::Audio { media, .. }
                | ContentPart::File { media, .. }
                | ContentPart::Video { media, .. } => vec![media.clone()],
                ContentPart::VoiceNote { voice_note } => vec![voice_note.media.clone()],
                _ => Vec::new(),
            })
            .collect();
        let envelope = ChannelInboundEnvelope {
            channel: slack_channel_ref(),
            message_id,
            reply_to_id: None,
            timestamp_unix_ms: ts_to_ms(ts),
            conversation,
            sender: tinychannels_bus::SenderRef {
                id: user.to_owned(),
                ..Default::default()
            },
            text: text.to_owned(),
            parts,
            media,
            raw: Some(serde_json::json!({"providerMessageId": ts})),
            ..Default::default()
        };
        let delivered = if let Some(sink) = typed_sink {
            sink.push_ingress(ChannelIngress::Inbound(envelope)).await?;
            true
        } else {
            match &self.events {
                Some(events) => events
                .publish_ingress(ChannelIngress::Inbound(envelope))
                .await
                .unwrap_or_else(|error| {
                    tracing::debug!(%error, "[slack] failed to deliver inbound through ingress");
                    false
                }),
                None => false,
            }
        };
        if !delivered
            && let Some(tx) = tx
            && tx.send(message).await.is_err()
        {
            return Ok(Flow::Stop);
        }
        self.mark_seen(seen_key);
        Ok(Flow::Continue)
    }

    async fn handle_changed(
        &self,
        event: &Value,
        typed_sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> Result<(), ChannelSendError> {
        let channel = str_of(event, "channel");
        let inner = event.get("message").unwrap_or(&Value::Null);
        let (user, ts) = (str_of(inner, "user"), str_of(inner, "ts"));
        if channel.is_empty()
            || ts.is_empty()
            || inner.get("bot_id").is_some()
            || !self.is_user_allowed(user)
            || self.bot_user_id().await.as_deref() == Some(user)
        {
            return Ok(());
        }
        let message_id = provider_message_id(channel, ts);
        let event_ts = str_of(event, "event_ts");
        let conversation = Self::conversation_ref(
            channel,
            event.get("channel_type").and_then(Value::as_str),
            inner
                .get("thread_ts")
                .and_then(Value::as_str)
                .map(str::to_owned),
        );
        let kind = ChannelEventKind::MessageUpdated {
            message_id: message_id.clone(),
            provider_message_id: Some(ts.to_owned()),
            parts: vec![ContentPart::Text {
                text: str_of(inner, "text").to_owned(),
                markdown_dialect: Some("slack_mrkdwn".into()),
            }],
        };
        let event_id = format!("slack:edit:{message_id}:{event_ts}");
        self.publish_event(
            Self::channel_event(
                event_id.clone(),
                ts_to_ms(event_ts),
                Some(conversation),
                kind,
            ),
            typed_sink,
        )
        .await?;
        Ok(())
    }

    async fn handle_deleted(
        &self,
        event: &Value,
        typed_sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> Result<(), ChannelSendError> {
        let channel = str_of(event, "channel");
        let ts = str_of(event, "deleted_ts");
        let author = event
            .pointer("/previous_message/user")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if channel.is_empty()
            || ts.is_empty()
            || !self.is_user_allowed(author)
            || self.bot_user_id().await.as_deref() == Some(author)
        {
            return Ok(());
        }
        let message_id = provider_message_id(channel, ts);
        let conversation = Self::conversation_ref(
            channel,
            event.get("channel_type").and_then(Value::as_str),
            None,
        );
        let kind = ChannelEventKind::MessageDeleted {
            message_id: message_id.clone(),
            provider_message_id: Some(ts.to_owned()),
        };
        let event_ts = str_of(event, "event_ts");
        self.publish_event(
            Self::channel_event(
                format!("slack:delete:{message_id}:{event_ts}"),
                ts_to_ms(event_ts),
                Some(conversation),
                kind,
            ),
            typed_sink,
        )
        .await?;
        Ok(())
    }

    async fn handle_reaction(
        &self,
        event: &Value,
        added: bool,
        typed_sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> Result<(), ChannelSendError> {
        let item = event.get("item").unwrap_or(&Value::Null);
        let (channel, ts) = (str_of(item, "channel"), str_of(item, "ts"));
        let (user, name) = (str_of(event, "user"), str_of(event, "reaction"));
        if str_of(item, "type") != "message"
            || channel.is_empty()
            || ts.is_empty()
            || name.is_empty()
            || !self.is_user_allowed(user)
            || self.bot_user_id().await.as_deref() == Some(user)
        {
            return Ok(());
        }
        let event_ts = str_of(event, "event_ts");
        let direction = if added { "added" } else { "removed" };
        let event_id =
            format!("slack:reaction:{direction}:{channel}:{ts}:{user}:{name}:{event_ts}");
        if self.has_seen(&event_id) {
            return Ok(());
        }
        let message_id = provider_message_id(channel, ts);
        let actor = tinychannels_bus::SenderRef {
            id: user.to_owned(),
            ..Default::default()
        };
        let reaction = ChannelReaction {
            value: emoji_for_name(name),
            custom_emoji_id: None,
        };
        let provider_message_id = Some(ts.to_owned());
        let kind = if added {
            ChannelEventKind::ReactionAdded {
                message_id,
                provider_message_id,
                actor,
                reaction,
            }
        } else {
            ChannelEventKind::ReactionRemoved {
                message_id,
                provider_message_id,
                actor,
                reaction,
            }
        };
        let conversation = Self::conversation_ref(channel, None, None);
        self.publish_event(
            Self::channel_event(
                event_id.clone(),
                ts_to_ms(event_ts),
                Some(conversation),
                kind,
            ),
            typed_sink,
        )
        .await?;
        self.mark_seen(event_id);
        Ok(())
    }

    /// Handle one Socket Mode `interactive` payload (a Block Kit button press).
    #[cfg(test)]
    pub(crate) async fn handle_interactive(&self, payload: &Value) {
        if let Err(error) = self.handle_interactive_to_sink(payload, None).await {
            tracing::debug!(message = %error.message, "[slack] interactive event delivery failed");
        }
    }

    async fn handle_interactive_to_sink(
        &self,
        payload: &Value,
        typed_sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> Result<(), ChannelSendError> {
        if str_of(payload, "type") != "block_actions" {
            return Ok(());
        }
        let user = payload.get("user").unwrap_or(&Value::Null);
        let actor = Self::sender_ref(user);
        let allowed = self.is_user_allowed(&actor.id)
            || actor
                .name
                .as_deref()
                .is_some_and(|name| self.is_user_allowed(name));
        let action = payload.pointer("/actions/0");
        let action_id = action.map(|a| str_of(a, "action_id")).unwrap_or_default();
        let channel = payload
            .pointer("/channel/id")
            .or_else(|| payload.pointer("/container/channel_id"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let message_ts = payload
            .pointer("/message/ts")
            .or_else(|| payload.pointer("/container/message_ts"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let query_id = str_of(payload, "trigger_id");
        if !allowed || action_id.is_empty() || channel.is_empty() || query_id.is_empty() {
            tracing::debug!("[slack] dropped a button press");
            return Ok(());
        }
        let thread_ts = payload
            .pointer("/container/thread_ts")
            .or_else(|| payload.pointer("/message/thread_ts"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let kind = ChannelEventKind::ButtonCallback {
            query_id: query_id.to_owned(),
            action_id: action_id.to_owned(),
            provider_message_id: (!message_ts.is_empty()).then(|| message_ts.to_owned()),
            actor,
        };
        let conversation = Self::conversation_ref(channel, None, thread_ts);
        self.publish_event(
            Self::channel_event(
                format!("slack:callback:{query_id}"),
                None,
                Some(conversation),
                kind,
            ),
            typed_sink,
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "events_tests.rs"]
mod tests;
