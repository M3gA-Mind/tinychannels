//! Discord message interactions: edit, delete, reactions, typing and
//! edit-in-place streaming drafts.

use super::channel::{DiscordChannel, split_message_for_discord};
use super::http::{Payload, pre_dispatch};
use super::inbound::is_snowflake;
use super::outbound::target_for;
use serde_json::json;
use tinychannels_bus::channel::{
    ChannelSendError, ChannelSendOutcome, MessageReceipt, SendErrorKind,
};
use tinychannels_bus::{
    ChannelOutboundIntent, ChannelOutboundMessage, ChannelReaction, ContentPart, ConversationRef,
    OutboundContentPart, OutboundPayload,
};

const MAX_EDIT_UTF16_UNITS: usize = super::channel::DISCORD_MAX_MESSAGE_LENGTH;

/// Plain text carried by an intent, or `None` when it holds anything else.
pub(super) fn intent_text(payload: &OutboundPayload) -> Option<String> {
    match payload {
        OutboundPayload::Text { text } => Some(text.clone()),
        OutboundPayload::Parts { parts } => {
            let mut texts = Vec::with_capacity(parts.len());
            for part in parts {
                match part {
                    OutboundContentPart::Content {
                        part: ContentPart::Text { text, .. },
                    } => texts.push(text.as_str()),
                    _ => return None,
                }
            }
            (!texts.is_empty()).then(|| texts.join("\n"))
        }
        _ => None,
    }
}

/// Channel id and message id for one receipt part.
fn part_targets(receipt: &MessageReceipt) -> Result<Vec<(String, String)>, ChannelSendError> {
    let conversation = receipt
        .conversation
        .as_ref()
        .ok_or_else(|| pre_dispatch("Discord receipt has no conversation"))?;
    if receipt
        .channel
        .as_ref()
        .is_some_and(|channel| tinychannels_bus::provider_id(&channel.id) != "discord")
    {
        return Err(pre_dispatch("receipt does not belong to Discord"));
    }
    let mut targets = Vec::new();
    for (index, id) in receipt.platform_message_ids.iter().enumerate() {
        let thread = receipt
            .parts
            .get(index)
            .and_then(|part| part.thread_id.clone())
            .or_else(|| receipt.thread_id.clone())
            .or_else(|| conversation.thread_id.clone());
        let channel_id = thread.unwrap_or_else(|| conversation.id.clone());
        if !is_snowflake(&channel_id) || !is_snowflake(id) {
            return Err(pre_dispatch("Discord receipt ids must be numeric"));
        }
        targets.push((channel_id, id.clone()));
    }
    if targets.is_empty()
        && let Some(id) = receipt.primary_platform_message_id.as_deref()
    {
        let channel_id = receipt
            .thread_id
            .clone()
            .unwrap_or_else(|| conversation.id.clone());
        if is_snowflake(&channel_id) && is_snowflake(id) {
            targets.push((channel_id, id.to_owned()));
        }
    }
    if targets.is_empty() {
        return Err(pre_dispatch("Discord receipt has no provider message id"));
    }
    Ok(targets)
}

fn emoji_segment(reaction: &ChannelReaction) -> Result<String, ChannelSendError> {
    if let Some(id) = reaction.custom_emoji_id.as_deref() {
        if !is_snowflake(id) || reaction.value.trim().is_empty() {
            return Err(pre_dispatch(
                "Discord custom emoji needs a name and numeric id",
            ));
        }
        return Ok(format!("{}:{id}", reaction.value.trim_matches(':')));
    }
    if reaction.value.trim().is_empty() {
        return Err(pre_dispatch("Discord reaction needs an emoji"));
    }
    Ok(reaction.value.clone())
}

impl DiscordChannel {
    /// Replace a message's text. Discord rejects more than 2000 characters.
    pub(super) async fn patch_content(
        &self,
        channel_id: &str,
        message_id: &str,
        text: &str,
    ) -> Result<(), ChannelSendError> {
        let url = self.endpoint_url(&["channels", channel_id, "messages", message_id])?;
        self.call(
            reqwest::Method::PATCH,
            url,
            Payload::Json(json!({"content": text, "allowed_mentions": {"parse": ["users"]}})),
            "edit message",
            true,
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn edit_message_v2(
        &self,
        receipt: &MessageReceipt,
        intent: &ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        let text = intent_text(&intent.payload)
            .ok_or_else(|| pre_dispatch("Discord edit only supports plain text"))?;
        let conversation = receipt
            .conversation
            .as_ref()
            .ok_or_else(|| pre_dispatch("Discord receipt has no conversation"))?;
        if intent.conversation_id != conversation.id
            || intent
                .conversation_ref
                .as_ref()
                .is_some_and(|target| target != conversation)
            || intent
                .channel_ref
                .as_ref()
                .zip(receipt.channel.as_ref())
                .is_some_and(|(a, b)| a != b)
        {
            return Err(pre_dispatch(
                "Discord edit scope does not match the receipt",
            ));
        }
        if text.encode_utf16().count() > MAX_EDIT_UTF16_UNITS {
            return Err(ChannelSendError {
                kind: SendErrorKind::TooLong,
                ..pre_dispatch("Discord edits are limited to 2000 characters")
            });
        }
        let (channel_id, message_id) = part_targets(receipt)?
            .into_iter()
            .next()
            .ok_or_else(|| pre_dispatch("Discord receipt has no provider message id"))?;
        self.patch_content(&channel_id, &message_id, &text).await?;
        Ok(receipt.clone())
    }

    /// Delete every message the receipt names.
    pub(crate) async fn delete_message_v2(
        &self,
        receipt: &MessageReceipt,
    ) -> Result<(), ChannelSendError> {
        let targets = part_targets(receipt)?;
        for (channel_id, message_id) in targets {
            let url = self.endpoint_url(&["channels", &channel_id, "messages", &message_id])?;
            match self
                .call(
                    reqwest::Method::DELETE,
                    url,
                    Payload::None,
                    "delete message",
                    true,
                )
                .await
            {
                Ok(_) => {}
                // Already gone is the state the caller asked for.
                Err(error) if error.kind == SendErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(crate) async fn send_reaction_v2(
        &self,
        conversation: &ConversationRef,
        provider_message_id: &str,
        reaction: &ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        let channel_id = conversation
            .thread_id
            .as_deref()
            .unwrap_or(conversation.id.as_str());
        if !is_snowflake(channel_id) || !is_snowflake(provider_message_id) {
            return Err(pre_dispatch("Discord reaction target ids must be numeric"));
        }
        let emoji = emoji_segment(reaction)?;
        let url = self.endpoint_url(&[
            "channels",
            channel_id,
            "messages",
            provider_message_id,
            "reactions",
            &emoji,
            "@me",
        ])?;
        let method = if remove {
            reqwest::Method::DELETE
        } else {
            reqwest::Method::PUT
        };
        self.call(method, url, Payload::None, "reaction", true)
            .await
            .map(|_| ())
    }

    /// One typing indicator (Discord clears it after ten seconds).
    pub(crate) async fn send_typing_once(
        &self,
        conversation_id: &str,
    ) -> Result<(), ChannelSendError> {
        if !is_snowflake(conversation_id) {
            return Err(pre_dispatch("Discord typing target must be numeric"));
        }
        let url = self.endpoint_url(&["channels", conversation_id, "typing"])?;
        self.call(reqwest::Method::POST, url, Payload::None, "typing", true)
            .await
            .map(|_| ())
    }

    /// Create (`previous` is `None`) or edit a streaming draft message.
    /// Drafts show the first 2000 characters; the final edit carries the rest.
    pub(crate) async fn upsert_draft_v2(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        let text = intent_text(&intent.payload)
            .ok_or_else(|| pre_dispatch("Discord draft payload must be text"))?;
        let shown = split_message_for_discord(&text)
            .into_iter()
            .next()
            .unwrap_or_default();
        if shown.trim().is_empty() {
            return Err(pre_dispatch("Discord draft requires text"));
        }
        match previous {
            Some(previous) => {
                let conversation = previous
                    .conversation
                    .as_ref()
                    .ok_or_else(|| pre_dispatch("Discord draft receipt has no conversation"))?;
                if intent.conversation_id != conversation.id {
                    return Err(pre_dispatch(
                        "Discord draft scope does not match the receipt",
                    ));
                }
                let (channel_id, message_id) = part_targets(previous)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| pre_dispatch("Discord draft receipt has no message id"))?;
                self.patch_content(&channel_id, &message_id, &shown).await?;
                Ok(previous.clone())
            }
            None => {
                let mut message = ChannelOutboundMessage::from_intent(&intent);
                message.parts = vec![OutboundContentPart::Content {
                    part: ContentPart::Text {
                        text: shown,
                        markdown_dialect: None,
                    },
                }];
                // Validate addressing early so the error names the draft.
                target_for(&message)?;
                let ChannelSendOutcome { receipt, error } =
                    self.send_v2_with_receipt(&message).await?;
                match error {
                    Some(error) => Err(error),
                    None => Ok(receipt),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "interactions_tests.rs"]
mod tests;
