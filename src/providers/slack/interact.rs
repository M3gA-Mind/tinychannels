//! Reactions, edits, deletes, typing status and streaming drafts.

use super::api::{ApiFailure, pre_dispatch, send_error, unsupported};
use super::events::name_for_emoji;
use super::outbound::target_of;
use super::text::{strip_tool_call_tags, to_mrkdwn};
use super::types::{SLACK_TEXT_CHUNK_CHARS, SlackChannel};
use serde_json::{Value, json};
use tinychannels_bus::channel::{
    ChannelReaction, ChannelSendError, ConversationRef, MessageReceipt, MessageReceiptPart,
    MessageReceiptPartKind,
};
use tinychannels_bus::{ChannelOutboundIntent, ChannelOutboundMessage, OutboundContentPart};

/// Slack answers these when the requested state already holds.
const IDEMPOTENT: [&str; 2] = ["already_reacted", "no_reaction"];

/// Single text body of an intent, or why it is not a plain edit.
fn plain_text(intent: &ChannelOutboundIntent) -> Result<String, ChannelSendError> {
    let message = ChannelOutboundMessage::from_intent(intent);
    let texts = message
        .parts
        .iter()
        .map(|part| match part {
            OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text { text, .. },
            } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
        .filter(|texts| !texts.is_empty())
        .ok_or_else(|| pre_dispatch("Slack edits and drafts only accept text"))?;
    let text = to_mrkdwn(&strip_tool_call_tags(&texts.join("\n")));
    if text.trim().is_empty() {
        return Err(pre_dispatch("Slack edits and drafts require text"));
    }
    if text.chars().count() > SLACK_TEXT_CHUNK_CHARS {
        return Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::TooLong,
            message: "Slack edit text exceeds the single-message limit".into(),
            ..Default::default()
        });
    }
    Ok(text)
}

/// `(channel, message ts)` addressed by a receipt.
fn receipt_address(receipt: &MessageReceipt) -> Result<(String, String), ChannelSendError> {
    let conversation = receipt
        .conversation
        .as_ref()
        .filter(|c| {
            !c.id.trim().is_empty() && receipt.channel.as_ref().is_none_or(|c| c.id == "slack")
        })
        .ok_or_else(|| pre_dispatch("Slack receipt does not address a Slack conversation"))?;
    let id = receipt
        .primary_platform_message_id
        .as_deref()
        .or_else(|| {
            receipt
                .parts
                .first()
                .map(|part| part.platform_message_id.as_str())
        })
        .ok_or_else(|| pre_dispatch("Slack receipt has no message ID"))?;
    Ok((conversation.id.clone(), id.to_owned()))
}

/// Edit by raw ids, for the legacy draft entry points.
#[cfg(test)]
pub(crate) async fn edit_text_for_legacy(
    channel: &SlackChannel,
    recipient: &str,
    message_id: &str,
    text: &str,
) -> Result<(), ChannelSendError> {
    let text = to_mrkdwn(&strip_tool_call_tags(text));
    if text.trim().is_empty() {
        return Err(pre_dispatch("Slack edits and drafts require text"));
    }
    channel.update_text(recipient, message_id, &text).await
}

impl SlackChannel {
    pub(crate) async fn send_reaction_v2(
        &self,
        conversation: &ConversationRef,
        provider_message_id: &str,
        reaction: &ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        let name = name_for_emoji(&reaction.value)
            .filter(|_| !conversation.id.trim().is_empty() && !provider_message_id.is_empty())
            .ok_or_else(|| pre_dispatch("Slack reaction needs a message and an emoji name"))?;
        let method = if remove {
            "reactions.remove"
        } else {
            "reactions.add"
        };
        let body =
            json!({"channel": conversation.id, "timestamp": provider_message_id, "name": name});
        match self.api_json(method, &body).await {
            Ok(_) => Ok(()),
            Err(ApiFailure::Slack { code }) if IDEMPOTENT.contains(&code.as_str()) => Ok(()),
            Err(failure) => Err(send_error(&failure, "reaction")),
        }
    }

    pub(crate) async fn edit_message_v2(
        &self,
        receipt: &MessageReceipt,
        intent: &ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        let (channel, ts) = receipt_address(receipt)?;
        if intent.channel_ref.as_ref().is_some_and(|c| c.id != "slack")
            || (!intent.conversation_id.is_empty() && intent.conversation_id != channel)
        {
            return Err(pre_dispatch("Slack edit intent does not match its receipt"));
        }
        let text = plain_text(intent)?;
        self.update_text(&channel, &ts, &text).await?;
        Ok(receipt.clone())
    }

    async fn update_text(
        &self,
        channel: &str,
        ts: &str,
        text: &str,
    ) -> Result<(), ChannelSendError> {
        self.api_json(
            "chat.update",
            &json!({"channel": channel, "ts": ts, "text": text}),
        )
        .await
        .map(|_| ())
        .map_err(|failure| send_error(&failure, "edit"))
    }

    pub(crate) async fn delete_message_v2(
        &self,
        receipt: &MessageReceipt,
    ) -> Result<(), ChannelSendError> {
        let (channel, id) = receipt_address(receipt)?;
        // A bare file ID (uploads whose message timestamp Slack did not report).
        let result = if id.contains('.') {
            self.api_json("chat.delete", &json!({"channel": channel, "ts": id}))
                .await
        } else {
            self.api_json("files.delete", &json!({"file": id})).await
        };
        result
            .map(|_| ())
            .map_err(|failure| send_error(&failure, "delete"))
    }

    /// Typing status for an assistant thread. `recipient` is `channel:thread_ts`.
    pub(crate) async fn set_typing_status(
        &self,
        recipient: &str,
        status: &str,
    ) -> Result<(), ChannelSendError> {
        let Some((channel, thread_ts)) = recipient
            .split_once(':')
            .filter(|(c, t)| !c.is_empty() && !t.is_empty())
        else {
            return Err(unsupported(
                "Slack typing needs a thread (channel:thread_ts)",
            ));
        };
        self.api_json(
            "assistant.threads.setStatus",
            &json!({"channel_id": channel, "thread_ts": thread_ts, "status": status}),
        )
        .await
        .map(|_| ())
        .map_err(|failure| send_error(&failure, "typing"))
    }

    /// Create the draft message, or update the one `previous` points at.
    pub(crate) async fn upsert_draft_v2(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        if let Some(previous) = previous {
            return self.edit_message_v2(previous, &intent).await;
        }
        let text = plain_text(&intent)?;
        let message = ChannelOutboundMessage::from_intent(&intent);
        let target = target_of(&message)?;
        let ts = self.post_draft(&target, &text).await?;
        let part = MessageReceiptPart {
            platform_message_id: ts.clone(),
            kind: MessageReceiptPartKind::Text,
            edit_token: Some(ts.clone()),
            delete_token: Some(ts.clone()),
            thread_id: target.thread_ts.clone(),
            ..Default::default()
        };
        Ok(MessageReceipt {
            idempotency_key: Some(message.idempotency_key),
            channel: Some(target.channel),
            conversation: Some(target.conversation),
            primary_platform_message_id: Some(ts.clone()),
            platform_message_ids: vec![ts.clone()],
            parts: vec![part],
            thread_id: target.thread_ts,
            edit_token: Some(ts.clone()),
            delete_token: Some(ts),
            ..Default::default()
        })
    }

    async fn post_draft(
        &self,
        target: &super::outbound::Target,
        text: &str,
    ) -> Result<String, ChannelSendError> {
        let mut body: Value =
            json!({"channel": target.conversation.id, "text": text, "mrkdwn": true});
        if let Some(thread) = &target.thread_ts {
            body["thread_ts"] = thread.clone().into();
        }
        let value = self
            .api_json("chat.postMessage", &body)
            .await
            .map_err(|failure| send_error(&failure, "draft"))?;
        value
            .get("ts")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| send_error(&ApiFailure::Malformed, "draft"))
    }
}

#[cfg(test)]
#[path = "interact_tests.rs"]
mod tests;
