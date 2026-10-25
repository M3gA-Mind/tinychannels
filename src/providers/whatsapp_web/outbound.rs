//! Typed outbound delivery for the WhatsApp Web adapter.

use super::client::OutboundClient;
use super::media_build::MediaBuild;
use super::{WhatsAppWebChannel, failure_of};
use crate::channel::{
    ChannelSendOutcome, MessageReceipt, MessageReceiptPart, MessageReceiptPartKind,
};
use crate::providers::whatsapp_shared::error::WaFailure;
use crate::providers::whatsapp_shared::plan::{
    self, AttachmentKind, MediaLimits, OutboundScope, PlanPorts, PlannedPart, plan_parts,
};
use crate::providers::whatsapp_shared::text::split_text;
use crate::providers::whatsapp_shared::vcard::{self, ContactCard};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::ChannelOutboundMessage;
use tinychannels_bus::channel::MessageReceiptSourceResult;
use whatsapp_rust::Jid;
use whatsapp_rust::wacore_binary::JidExt as _;
use whatsapp_rust::waproto::whatsapp as wa;

/// Longest text one outbound message carries.
pub(super) const TEXT_LIMIT: usize = 16_384;

pub(super) const MEDIA_LIMITS: MediaLimits = MediaLimits {
    image: 16 * 1024 * 1024,
    video: 16 * 1024 * 1024,
    audio: 16 * 1024 * 1024,
    document: 20 * 1024 * 1024,
    sticker: 500 * 1024,
    caption_chars: 1024,
};

/// Quote context for a reply to `reply_to` authored by `participant`.
pub(super) fn reply_context(reply_to: &str, participant: Option<&str>) -> wa::ContextInfo {
    wa::ContextInfo {
        stanza_id: Some(reply_to.to_owned()),
        participant: participant.map(str::to_owned),
        ..Default::default()
    }
}

pub(super) fn text_message(text: &str, context: Option<wa::ContextInfo>) -> wa::Message {
    match context {
        None => wa::Message {
            conversation: Some(text.to_owned()),
            ..Default::default()
        },
        Some(context) => wa::Message {
            extended_text_message: whatsapp_rust::buffa::MessageField::some(
                wa::message::ExtendedTextMessage {
                    text: Some(text.to_owned()),
                    context_info: whatsapp_rust::buffa::MessageField::some(context),
                    ..Default::default()
                },
            ),
            ..Default::default()
        },
    }
}

pub(super) fn location_message(
    latitude: f64,
    longitude: f64,
    name: Option<&str>,
    address: Option<&str>,
    context: Option<wa::ContextInfo>,
) -> wa::Message {
    wa::Message {
        location_message: whatsapp_rust::buffa::MessageField::some(wa::message::LocationMessage {
            degrees_latitude: Some(latitude),
            degrees_longitude: Some(longitude),
            name: name.map(str::to_owned),
            address: address.map(str::to_owned),
            context_info: context
                .map(whatsapp_rust::buffa::MessageField::some)
                .unwrap_or_default(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(super) fn contact_message(card: &ContactCard, context: Option<wa::ContextInfo>) -> wa::Message {
    wa::Message {
        contact_message: whatsapp_rust::buffa::MessageField::some(wa::message::ContactMessage {
            display_name: Some(card.display_name.clone()),
            vcard: Some(vcard::write(card)),
            context_info: context
                .map(whatsapp_rust::buffa::MessageField::some)
                .unwrap_or_default(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn media_build(
    kind: AttachmentKind,
    content_type: &str,
    file_name: Option<String>,
    caption: Option<String>,
    duration_ms: Option<u64>,
) -> MediaBuild {
    let duration_seconds =
        duration_ms.map(|ms| u32::try_from(ms.div_ceil(1000)).unwrap_or(u32::MAX));
    match kind {
        AttachmentKind::Image => MediaBuild::Image {
            caption,
            mimetype: content_type.to_owned(),
        },
        AttachmentKind::Video => MediaBuild::Video {
            caption,
            duration_seconds,
        },
        AttachmentKind::Audio => MediaBuild::Audio {
            mimetype: content_type.to_owned(),
            duration_seconds,
        },
        AttachmentKind::Document => MediaBuild::Document {
            mimetype: content_type.to_owned(),
            file_name,
            caption,
        },
        AttachmentKind::Sticker => MediaBuild::Sticker,
    }
}

fn receipt_part(
    id: &str,
    kind: MessageReceiptPartKind,
    index: usize,
    scope: &OutboundScope,
    reply_to: Option<&str>,
) -> MessageReceiptPart {
    MessageReceiptPart {
        platform_message_id: id.to_owned(),
        kind,
        index,
        thread_id: None,
        reply_to_id: reply_to.map(str::to_owned),
        // Only text can be edited; any message of ours can be unsent.
        edit_token: (kind == MessageReceiptPartKind::Text).then(|| id.to_owned()),
        delete_token: Some(id.to_owned()),
        raw: Some(MessageReceiptSourceResult {
            channel: Some("whatsapp".into()),
            message_id: Some(id.to_owned()),
            conversation_id: Some(scope.conversation.id.clone()),
            ..Default::default()
        }),
    }
}

impl WhatsAppWebChannel {
    /// The JID a send to `conversation` is addressed to, after the account's
    /// outbound policy (allow-list and group provenance) has admitted it.
    pub(super) fn admitted_jid(&self, conversation: &str) -> Result<Jid, WaFailure> {
        if !self.should_allow_outbound(conversation) {
            return Err(WaFailure::rejected(
                "WhatsApp recipient is not allowed for this account",
            ));
        }
        self.recipient_to_jid(conversation)
            .map_err(|_| WaFailure::bad_format("WhatsApp recipient address is invalid"))
    }

    pub(super) fn live_client(&self) -> Result<Arc<dyn OutboundClient>, WaFailure> {
        let client = self
            .connected_client()
            .map_err(|_| WaFailure::rejected("WhatsApp Web client is not connected"))?;
        if !self.identity.observe(
            client
                .linked_account()
                .map(|jid| jid.to_string())
                .as_deref(),
        ) {
            return Err(WaFailure::rejected(
                "the linked WhatsApp account changed; restart the channel",
            ));
        }
        Ok(client)
    }

    async fn send_planned(
        &self,
        client: &dyn OutboundClient,
        to: &Jid,
        part: PlannedPart,
        context: Option<wa::ContextInfo>,
    ) -> (Vec<(String, MessageReceiptPartKind)>, Option<WaFailure>) {
        let mut sent = Vec::new();
        let outcome: Result<(), WaFailure> = async {
            let uncertain = |_error: anyhow::Error| {
                WaFailure::uncertain("WhatsApp Web send outcome is uncertain")
            };
            match part {
                PlannedPart::Text(text) => {
                    let mut context = context;
                    for chunk in split_text(&text, TEXT_LIMIT) {
                        let message = text_message(&chunk, context.take());
                        let id = client
                            .send_message(to.clone(), message)
                            .await
                            .map_err(uncertain)?;
                        sent.push((id, MessageReceiptPartKind::Text));
                    }
                }
                PlannedPart::Attachment {
                    kind,
                    bytes,
                    content_type,
                    file_name,
                    caption,
                    duration_ms,
                } => {
                    let build = media_build(kind, &content_type, file_name, caption, duration_ms);
                    // An upload creates no message: failing it is safe to retry.
                    let message = client
                        .media_message(bytes, build, context)
                        .await
                        .map_err(|_| WaFailure::rejected("WhatsApp Web media upload failed"))?;
                    let id = client
                        .send_message(to.clone(), message)
                        .await
                        .map_err(uncertain)?;
                    let part_kind = if kind == AttachmentKind::Audio {
                        MessageReceiptPartKind::Voice
                    } else {
                        MessageReceiptPartKind::Media
                    };
                    sent.push((id, part_kind));
                }
                PlannedPart::Voice {
                    bytes,
                    duration_ms,
                    waveform,
                    ..
                } => {
                    let build = MediaBuild::Voice {
                        duration_seconds: duration_ms
                            .map(|ms| u32::try_from(ms.div_ceil(1000)).unwrap_or(u32::MAX)),
                        waveform,
                    };
                    let message = client
                        .media_message(bytes, build, context)
                        .await
                        .map_err(|_| WaFailure::rejected("WhatsApp Web voice upload failed"))?;
                    let id = client
                        .send_message(to.clone(), message)
                        .await
                        .map_err(uncertain)?;
                    sent.push((id, MessageReceiptPartKind::Voice));
                }
                PlannedPart::Location {
                    latitude,
                    longitude,
                    name,
                    address,
                } => {
                    let message = location_message(
                        latitude,
                        longitude,
                        name.as_deref(),
                        address.as_deref(),
                        context,
                    );
                    let id = client
                        .send_message(to.clone(), message)
                        .await
                        .map_err(uncertain)?;
                    sent.push((id, MessageReceiptPartKind::Unknown));
                }
                PlannedPart::Contact(card) => {
                    let id = client
                        .send_message(to.clone(), contact_message(&card, context))
                        .await
                        .map_err(uncertain)?;
                    sent.push((id, MessageReceiptPartKind::Unknown));
                }
            }
            Ok(())
        }
        .await;
        (sent, outcome.err())
    }

    /// Deliver `message` and return the provider's own receipt.
    pub(super) async fn send_v2(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, WaFailure> {
        let scope = plan::outbound_scope(message, "whatsapp")?;
        let to = self.admitted_jid(&scope.conversation.id)?;
        let client = self.live_client()?;
        let planned = plan_parts(
            message,
            &scope,
            PlanPorts {
                media: self.media_repository.as_deref(),
                voices: self.voice_media_store.as_deref(),
            },
            &MEDIA_LIMITS,
        )
        .await?;
        let mut parts: Vec<MessageReceiptPart> = Vec::new();
        let mut failure = None;
        for planned_part in planned {
            // Only the first message quotes the source; the rest follow it.
            let reply = if parts.is_empty() {
                scope.reply_to_id.as_deref()
            } else {
                None
            };
            let context = reply.map(|reply_to| {
                let participant = self
                    .authors
                    .lock()
                    .get(reply_to)
                    .or_else(|| (!to.is_group()).then(|| to.to_non_ad_string()));
                reply_context(reply_to, participant.as_deref())
            });
            let (sent, stopped) = self
                .send_planned(client.as_ref(), &to, planned_part, context)
                .await;
            for (position, (id, kind)) in sent.into_iter().enumerate() {
                let index = parts.len();
                // Only the first message of the group quotes the source.
                let quoted = if position == 0 { reply } else { None };
                parts.push(receipt_part(&id, kind, index, &scope, quoted));
            }
            if let Some(error) = stopped {
                if parts.is_empty() {
                    return Err(error);
                }
                failure = Some(error);
                break;
            }
        }
        let ids: Vec<String> = parts
            .iter()
            .map(|part| part.platform_message_id.clone())
            .collect();
        let single = (ids.len() == 1).then(|| ids[0].clone());
        let receipt = MessageReceipt {
            idempotency_key: Some(message.idempotency_key.clone()),
            channel: Some(scope.channel.clone()),
            conversation: Some(scope.conversation.clone()),
            primary_platform_message_id: ids.first().cloned(),
            platform_message_ids: ids,
            parts,
            thread_id: None,
            reply_to_id: scope.reply_to_id.clone(),
            edit_token: single.clone().filter(|_| {
                message.parts.len() == 1
                    && matches!(
                        message.parts.first(),
                        Some(tinychannels_bus::OutboundContentPart::Content {
                            part: tinychannels_bus::ContentPart::Text { .. }
                        })
                    )
            }),
            delete_token: single,
            sent_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            raw: None,
        };
        let error = failure.map(|failure| {
            let mut error = WaFailure::uncertain(format!(
                "WhatsApp send is partial or uncertain; retry is unsafe: {failure}"
            ))
            .into_send_error();
            error.sent_before_error = true;
            error
        });
        Ok(ChannelSendOutcome { receipt, error })
    }

    /// The conversation, message id and kind a single-message receipt names.
    fn receipt_target(
        receipt: &MessageReceipt,
    ) -> Result<(String, String, MessageReceiptPartKind), WaFailure> {
        let conversation = receipt
            .conversation
            .as_ref()
            .map(|conversation| conversation.id.clone())
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| WaFailure::rejected("WhatsApp receipt has no conversation"))?;
        let [part] = receipt.parts.as_slice() else {
            return Err(WaFailure::unsupported(
                "WhatsApp can only change a single-message send",
            ));
        };
        Ok((conversation, part.platform_message_id.clone(), part.kind))
    }

    /// Edit the text of a message we sent.
    pub(super) async fn edit_v2(
        &self,
        receipt: &MessageReceipt,
        intent: &tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<MessageReceipt, WaFailure> {
        let (_, original_id, kind) = Self::receipt_target(receipt)?;
        if kind != MessageReceiptPartKind::Text {
            return Err(WaFailure::unsupported(
                "WhatsApp can only edit text messages",
            ));
        }
        let message = ChannelOutboundMessage::from_intent(intent);
        let [
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text { text, .. },
            },
        ] = message.parts.as_slice()
        else {
            return Err(WaFailure::unsupported(
                "a WhatsApp edit replaces text with text",
            ));
        };
        let text = crate::providers::whatsapp_shared::text::strip_tool_call_tags(text);
        if text.trim().is_empty() || text.chars().count() > TEXT_LIMIT {
            return Err(WaFailure::rejected(
                "WhatsApp edit text is empty or too long",
            ));
        }
        let scope = plan::outbound_scope(&message, "whatsapp")?;
        let client = self.live_client()?;
        let to = self.admitted_jid(&scope.conversation.id)?;
        client
            .edit_message(to, original_id, text_message(&text, None))
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp edit outcome is uncertain"))?;
        // An edit keeps the message's identity, so the receipt keeps its ids.
        let mut edited = receipt.clone();
        edited.sent_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Ok(edited)
    }

    /// Unsend a message we sent, for everyone.
    pub(super) async fn delete_v2(&self, receipt: &MessageReceipt) -> Result<(), WaFailure> {
        let (conversation, original_id, _) = Self::receipt_target(receipt)?;
        let client = self.live_client()?;
        let to = self.admitted_jid(&conversation)?;
        client
            .revoke_message(to, original_id)
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp unsend outcome is uncertain"))
    }

    /// React to an inbound message (an empty reaction removes ours).
    pub(super) async fn react_v2(
        &self,
        conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::ChannelReaction,
        remove: bool,
    ) -> Result<(), WaFailure> {
        if reaction.custom_emoji_id.is_some() {
            return Err(WaFailure::unsupported("WhatsApp reactions are plain emoji"));
        }
        let emoji = if remove { "" } else { reaction.value.as_str() };
        if !remove && emoji.trim().is_empty() {
            return Err(WaFailure::rejected("WhatsApp reaction needs an emoji"));
        }
        if provider_message_id.trim().is_empty() {
            return Err(WaFailure::rejected("WhatsApp reaction needs a message id"));
        }
        let client = self.live_client()?;
        let chat = self.admitted_jid(&conversation.id)?;
        let key = wa::MessageKey {
            remote_jid: Some(chat.to_string()),
            from_me: Some(false),
            id: Some(provider_message_id.to_owned()),
            participant: chat
                .is_group()
                .then(|| self.authors.lock().get(provider_message_id))
                .flatten(),
        };
        client
            .send_reaction(chat, key, emoji)
            .await
            .map(|_| ())
            .map_err(|_| WaFailure::uncertain("WhatsApp reaction outcome is uncertain"))
    }

    /// Send a read receipt for one inbound message.
    pub(super) async fn read_v2(
        &self,
        conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
    ) -> Result<(), WaFailure> {
        if provider_message_id.trim().is_empty() {
            return Err(WaFailure::rejected(
                "WhatsApp read receipt needs a message id",
            ));
        }
        let client = self.live_client()?;
        let chat = self.admitted_jid(&conversation.id)?;
        let sender: Option<Jid> = chat
            .is_group()
            .then(|| self.authors.lock().get(provider_message_id))
            .flatten()
            .and_then(|participant| participant.parse().ok());
        client
            .mark_read(&chat, sender.as_ref(), &[provider_message_id.to_owned()])
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp read receipt outcome is uncertain"))
    }
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::ChannelTypedSend for WhatsAppWebChannel {
    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> std::result::Result<crate::channel::ChannelSendOutcome, crate::channel::ChannelSendError>
    {
        self.send_v2(message)
            .await
            .map_err(WaFailure::into_send_error)
    }
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::ChannelTyping for WhatsAppWebChannel {
    async fn start_typing(
        &self,
        conversation_id: &str,
    ) -> std::result::Result<(), crate::channel::ChannelSendError> {
        self.start_typing_provider(conversation_id)
            .await
            .map_err(|error| failure_of(&error).into_send_error())
    }

    async fn stop_typing(
        &self,
        conversation_id: &str,
    ) -> std::result::Result<(), crate::channel::ChannelSendError> {
        self.stop_typing_provider(conversation_id)
            .await
            .map_err(|error| failure_of(&error).into_send_error())
    }
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::LegacyChannelReaction for WhatsAppWebChannel {
    async fn send_reaction(
        &self,
        channel: &tinychannels_bus::channel::ChannelRef,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> std::result::Result<(), crate::channel::ChannelSendError> {
        if channel.id != "whatsapp" {
            return Err(
                WaFailure::rejected("reaction channel does not match WhatsApp").into_send_error(),
            );
        }
        self.react_v2(conversation, provider_message_id, reaction, remove)
            .await
            .map_err(WaFailure::into_send_error)
    }
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::ChannelEdit for WhatsAppWebChannel {
    async fn edit(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<
        tinychannels_bus::channel::MessageReceipt,
        crate::channel::ChannelSendError,
    > {
        self.edit_v2(receipt, &intent)
            .await
            .map_err(WaFailure::into_send_error)
    }
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::ChannelDelete for WhatsAppWebChannel {
    async fn delete(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> std::result::Result<(), crate::channel::ChannelSendError> {
        self.delete_v2(receipt)
            .await
            .map_err(WaFailure::into_send_error)
    }
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::ChannelStreamingDraft for WhatsAppWebChannel {
    async fn upsert_draft(
        &self,
        _previous: Option<&tinychannels_bus::channel::MessageReceipt>,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<
        tinychannels_bus::channel::MessageReceipt,
        crate::channel::ChannelSendError,
    > {
        Err(
            WaFailure::unsupported("WhatsApp messages cannot be streamed as edited drafts")
                .into_send_error(),
        )
    }
}
