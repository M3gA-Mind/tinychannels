//! Edits, deletes and streaming drafts over [`NativeProvider`].

use super::outbound::send_typed;
use super::{Address, NativeProvider, pre_dispatch, unsupported};
use crate::providers::slack::text::strip_tool_call_tags;
use tinychannels_bus::channel::{ChannelRef, ChannelSendError, MessageReceipt};
use tinychannels_bus::{
    ChannelOutboundIntent, ChannelOutboundMessage, ContentPart, OutboundContentPart,
};

/// A reaction for another channel is forbidden and never reaches the platform.
pub(crate) fn check_channel<P: NativeProvider + ?Sized>(
    provider: &P,
    channel: &ChannelRef,
) -> Result<(), ChannelSendError> {
    if channel.id == provider.id() {
        return Ok(());
    }
    Err(ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Forbidden,
        message: format!("channel does not match {}", provider.display()),
        ..Default::default()
    })
}

/// Single text body of an intent, or why it is not a plain edit.
fn plain_text<P: NativeProvider + ?Sized>(
    provider: &P,
    intent: &ChannelOutboundIntent,
) -> Result<String, ChannelSendError> {
    let message = ChannelOutboundMessage::from_intent(intent);
    let texts = message
        .parts
        .iter()
        .map(|part| match part {
            OutboundContentPart::Content {
                part: ContentPart::Text { text, .. },
            } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
        .filter(|texts| !texts.is_empty())
        .ok_or_else(|| {
            pre_dispatch(&format!(
                "{} edits and drafts only accept text",
                provider.display()
            ))
        })?;
    let text = strip_tool_call_tags(&texts.join("\n"));
    if text.trim().is_empty() {
        return Err(pre_dispatch(&format!(
            "{} edits and drafts require text",
            provider.display()
        )));
    }
    if text.chars().count() > provider.limits().text_chars {
        return Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::TooLong,
            message: format!(
                "{} edit text exceeds the single-message limit",
                provider.display()
            ),
            ..Default::default()
        });
    }
    Ok(text)
}

fn address_of<P: NativeProvider + ?Sized>(
    provider: &P,
    receipt: &MessageReceipt,
) -> Result<Address, ChannelSendError> {
    let display = provider.display();
    let conversation = receipt
        .conversation
        .as_ref()
        .filter(|c| {
            !c.id.trim().is_empty()
                && receipt
                    .channel
                    .as_ref()
                    .is_none_or(|channel| channel.id == provider.id())
        })
        .ok_or_else(|| {
            pre_dispatch(&format!(
                "{display} receipt does not address a {display} conversation"
            ))
        })?;
    let id = receipt
        .primary_platform_message_id
        .as_deref()
        .or_else(|| {
            receipt
                .parts
                .first()
                .map(|part| part.platform_message_id.as_str())
        })
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| pre_dispatch(&format!("{display} receipt has no message ID")))?;
    Ok(Address {
        conversation: conversation.clone(),
        message_id: id.to_owned(),
    })
}

pub(crate) async fn edit<P: NativeProvider + ?Sized>(
    provider: &P,
    receipt: &MessageReceipt,
    intent: &ChannelOutboundIntent,
) -> Result<MessageReceipt, ChannelSendError> {
    if !provider.can_edit() {
        return Err(unsupported(&format!(
            "{} does not support edits",
            provider.display()
        )));
    }
    let address = address_of(provider, receipt)?;
    if intent
        .channel_ref
        .as_ref()
        .is_some_and(|c| c.id != provider.id())
        || (!intent.conversation_id.is_empty() && intent.conversation_id != address.conversation.id)
    {
        return Err(pre_dispatch(&format!(
            "{} edit intent does not match its receipt",
            provider.display()
        )));
    }
    let text = plain_text(provider, intent)?;
    provider.edit_text(&address, &text).await?;
    Ok(receipt.clone())
}

pub(crate) async fn delete<P: NativeProvider + ?Sized>(
    provider: &P,
    receipt: &MessageReceipt,
) -> Result<(), ChannelSendError> {
    if !provider.can_delete() {
        return Err(unsupported(&format!(
            "{} does not support deleting messages",
            provider.display()
        )));
    }
    let address = address_of(provider, receipt)?;
    provider.delete_message(&address).await
}

/// Create the draft message, or edit the one `previous` points at.
pub(crate) async fn upsert_draft<P: NativeProvider + ?Sized>(
    provider: &P,
    previous: Option<&MessageReceipt>,
    intent: ChannelOutboundIntent,
) -> Result<MessageReceipt, ChannelSendError> {
    if !provider.can_edit() || !provider.can_draft() {
        return Err(unsupported(&format!(
            "{} does not support streaming drafts",
            provider.display()
        )));
    }
    if let Some(previous) = previous {
        return edit(provider, previous, &intent).await;
    }
    plain_text(provider, &intent)?;
    let message = ChannelOutboundMessage::from_intent(&intent);
    let outcome = send_typed(provider, &message).await?;
    Ok(outcome.receipt)
}
