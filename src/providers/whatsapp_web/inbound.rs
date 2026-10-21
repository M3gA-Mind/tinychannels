//! Inbound WhatsApp Web messages: protocol parsing and typed delivery.

use super::{InboundState, WhatsAppWebChannel};
use crate::providers::whatsapp_shared::error::WaFailure;
use crate::providers::whatsapp_shared::inbound::{
    self, DownloadedAttachment, InboundKind, MAX_INBOUND_MEDIA_BYTES, MAX_INBOUND_VOICE_BYTES,
};
use crate::providers::whatsapp_shared::vcard;
use tinychannels_bus::{
    ChannelEvent, ChannelEventKind, ChannelInboundEnvelope, ChannelIngress, ChannelReaction,
    ChannelRef, ContentPart, ConversationRef, SenderRef,
};
use wacore::download::Downloadable;
use wacore::proto_helpers::MessageExt;
use whatsapp_rust::waproto::whatsapp as wa;

/// One attachment found in a message, borrowed from the protocol message.
pub(super) enum Attachment<'a> {
    Image(&'a wa::message::ImageMessage),
    Video(&'a wa::message::VideoMessage),
    Audio(&'a wa::message::AudioMessage),
    Document(&'a wa::message::DocumentMessage),
    Sticker(&'a wa::message::StickerMessage),
}

impl Attachment<'_> {
    fn downloadable(&self) -> &dyn Downloadable {
        match self {
            Self::Image(media) => *media,
            Self::Video(media) => *media,
            Self::Audio(media) => *media,
            Self::Document(media) => *media,
            Self::Sticker(media) => *media,
        }
    }

    fn kind(&self) -> InboundKind {
        match self {
            Self::Image(_) => InboundKind::Image,
            Self::Video(_) => InboundKind::Video,
            Self::Audio(_) => InboundKind::Audio,
            Self::Document(_) => InboundKind::Document,
            Self::Sticker(_) => InboundKind::Sticker,
        }
    }

    fn mime(&self) -> String {
        let mime = match self {
            Self::Image(media) => media.mimetype.as_deref(),
            Self::Video(media) => media.mimetype.as_deref(),
            Self::Audio(media) => media.mimetype.as_deref(),
            Self::Document(media) => media.mimetype.as_deref(),
            Self::Sticker(media) => media.mimetype.as_deref(),
        };
        mime.unwrap_or_default().to_owned()
    }

    fn caption(&self) -> Option<String> {
        match self {
            Self::Image(media) => media.caption.clone(),
            Self::Video(media) => media.caption.clone(),
            Self::Document(media) => media.caption.clone(),
            Self::Audio(_) | Self::Sticker(_) => None,
        }
        .filter(|caption| !caption.trim().is_empty())
    }

    fn file_name(&self) -> Option<String> {
        match self {
            Self::Document(media) => media.file_name.clone(),
            _ => None,
        }
    }

    fn duration_ms(&self) -> Option<u64> {
        match self {
            Self::Video(media) => media.seconds.map(|seconds| u64::from(seconds) * 1000),
            Self::Audio(media) => media.seconds.map(|seconds| u64::from(seconds) * 1000),
            _ => None,
        }
    }
}

/// What one inbound protocol message carries.
#[derive(Default)]
pub(super) struct Parsed<'a> {
    pub text: String,
    pub attachments: Vec<Attachment<'a>>,
    pub location: Option<(f64, f64, Option<String>, Option<String>)>,
    pub contacts: Vec<vcard::ContactCard>,
    /// `(target message id, emoji)`; an empty emoji removes the reaction.
    pub reaction: Option<(String, String)>,
    pub reply_to: Option<String>,
}

/// Read the content of `message`, unwrapping ephemeral and view-once wrappers.
pub(super) fn parse(message: &wa::Message) -> Parsed<'_> {
    let base = message.get_base_message();
    let mut parsed = Parsed {
        text: base.text_content().unwrap_or_default().to_owned(),
        ..Default::default()
    };
    let mut reply = |context: &whatsapp_rust::buffa::MessageField<wa::ContextInfo>| {
        if parsed.reply_to.is_none() {
            parsed.reply_to = context.stanza_id.clone().filter(|id| !id.trim().is_empty());
        }
    };
    reply(&base.extended_text_message.context_info);
    if let Some(media) = base.image_message.as_option() {
        reply(&media.context_info);
        parsed.attachments.push(Attachment::Image(media));
    }
    if let Some(media) = base.video_message.as_option() {
        reply(&media.context_info);
        parsed.attachments.push(Attachment::Video(media));
    }
    if let Some(media) = base.audio_message.as_option() {
        reply(&media.context_info);
        parsed.attachments.push(Attachment::Audio(media));
    }
    if let Some(media) = base.document_message.as_option() {
        reply(&media.context_info);
        parsed.attachments.push(Attachment::Document(media));
    }
    if let Some(media) = base.sticker_message.as_option() {
        reply(&media.context_info);
        parsed.attachments.push(Attachment::Sticker(media));
    }
    if let Some(location) = base.location_message.as_option()
        && let (Some(latitude), Some(longitude)) =
            (location.degrees_latitude, location.degrees_longitude)
        && (-90.0..=90.0).contains(&latitude)
        && (-180.0..=180.0).contains(&longitude)
    {
        reply(&location.context_info);
        parsed.location = Some((
            latitude,
            longitude,
            location.name.clone().filter(|name| !name.trim().is_empty()),
            location
                .address
                .clone()
                .filter(|address| !address.trim().is_empty()),
        ));
    }
    if let Some(contact) = base.contact_message.as_option() {
        reply(&contact.context_info);
        parsed.contacts.push(contact_card(contact));
    }
    if let Some(array) = base.contacts_array_message.as_option() {
        parsed
            .contacts
            .extend(array.contacts.iter().map(contact_card));
    }
    if let Some(reaction) = base.reaction_message.as_option()
        && let Some(target) = reaction.key.id.clone().filter(|id| !id.is_empty())
    {
        parsed.reaction = Some((target, reaction.text.clone().unwrap_or_default()));
    }
    parsed
}

fn contact_card(contact: &wa::message::ContactMessage) -> vcard::ContactCard {
    let mut card = contact
        .vcard
        .as_deref()
        .map(vcard::read)
        .unwrap_or_default();
    if card.display_name.is_empty() {
        card.display_name = contact.display_name.clone().unwrap_or_default();
    }
    card
}

impl InboundState {
    /// Build and publish the typed envelope (or reaction event) for one
    /// allowed inbound message. `Ok(false)` means the host cannot take typed
    /// ingress and the caller should use the legacy message path.
    pub(super) async fn publish_typed(
        &self,
        inbound: &wacore::types::events::InboundMessage,
        conversation_id: &str,
        sender_id: &str,
        sender_name: Option<String>,
        is_group: bool,
    ) -> Result<bool, WaFailure> {
        if self.events.is_none() && self.typed_tx.is_none() {
            return Ok(false);
        }
        let info = &inbound.info;
        let id = if info.id.is_empty() && self.typed_tx.is_some() {
            uuid::Uuid::new_v4().to_string()
        } else {
            info.id.clone()
        };
        if id.is_empty() {
            return Ok(false);
        }
        if self.seen.lock().contains(&id) {
            return Ok(true);
        }
        let conversation = ConversationRef {
            kind: inbound::conversation_kind(is_group),
            id: conversation_id.to_owned(),
            ..Default::default()
        };
        let sender = SenderRef {
            id: sender_id.to_owned(),
            name: sender_name,
            ..Default::default()
        };
        let timestamp = u64::try_from(info.timestamp.timestamp_millis()).ok();
        let parsed = parse(&inbound.message);
        let ingress = if let Some((target, emoji)) = parsed.reaction.clone() {
            let reaction = ChannelReaction {
                value: emoji.clone(),
                custom_emoji_id: None,
            };
            let event = if emoji.is_empty() {
                ChannelEventKind::ReactionRemoved {
                    message_id: target.clone(),
                    provider_message_id: Some(target),
                    actor: sender,
                    reaction,
                }
            } else {
                ChannelEventKind::ReactionAdded {
                    message_id: target.clone(),
                    provider_message_id: Some(target),
                    actor: sender,
                    reaction,
                }
            };
            ChannelIngress::Event(ChannelEvent {
                version: 2,
                event_id: format!("whatsapp:reaction:{id}"),
                source_event_id: Some(id.clone()),
                source_timestamp: timestamp,
                channel: ChannelRef {
                    id: "whatsapp".into(),
                    account_id: None,
                },
                conversation: Some(conversation),
                event,
            })
        } else {
            let scope = inbound::inbound_scope("whatsapp", &conversation, &id);
            let mut parts = Vec::new();
            if !parsed.text.trim().is_empty() {
                parts.push(ContentPart::Text {
                    text: parsed.text.clone(),
                    markdown_dialect: None,
                });
            }
            let mut skipped_caption = None;
            for attachment in &parsed.attachments {
                match self.attachment_part(attachment, &id, scope.clone()).await? {
                    Some(part) => parts.push(part),
                    // An attachment the adapter cannot keep still delivers its words.
                    None => skipped_caption = skipped_caption.or_else(|| attachment.caption()),
                }
            }
            if parsed.text.trim().is_empty()
                && let Some(caption) = skipped_caption
            {
                parts.insert(
                    0,
                    ContentPart::Text {
                        text: caption,
                        markdown_dialect: None,
                    },
                );
            }
            if let Some((latitude, longitude, label, address)) = parsed.location.clone() {
                parts.push(ContentPart::Location {
                    latitude,
                    longitude,
                    label,
                    address,
                    accuracy_meters: None,
                    live_until_unix_ms: None,
                });
            }
            for card in &parsed.contacts {
                parts.push(ContentPart::Contact {
                    display_name: card.display_name.clone(),
                    phone_numbers: card.phones.clone(),
                    emails: card.emails.clone(),
                    organization: card.organization.clone(),
                });
            }
            if parts.is_empty() {
                return Ok(true);
            }
            let text = if parsed.text.trim().is_empty() {
                parsed
                    .attachments
                    .iter()
                    .find_map(Attachment::caption)
                    .unwrap_or_default()
            } else {
                parsed.text.clone()
            };
            let media = inbound::envelope_media(&parts);
            ChannelIngress::Inbound(ChannelInboundEnvelope {
                channel: ChannelRef {
                    id: "whatsapp".into(),
                    account_id: None,
                },
                message_id: id.clone(),
                reply_to_id: parsed.reply_to.clone(),
                timestamp_unix_ms: timestamp,
                conversation,
                sender,
                text,
                parts,
                media,
                raw: Some(serde_json::json!({ "providerMessageId": id })),
                ..Default::default()
            })
        };
        let delivered = if let Some(typed_tx) = &self.typed_tx {
            let (accepted, result) = tokio::sync::oneshot::channel();
            typed_tx
                .send(super::TypedIngressDelivery { ingress, accepted })
                .await
                .map_err(|_| {
                    WaFailure::uncertain("WhatsApp ingress queue is no longer available")
                })?;
            result.await.map_err(|_| {
                WaFailure::uncertain("WhatsApp ingress acknowledgement was cancelled")
            })?
        } else if let Some(sink) = &self.events {
            sink.publish_ingress(ingress).await.map_err(|_| {
                WaFailure::uncertain("WhatsApp ingress could not be delivered to the host")
            })?
        } else {
            false
        };
        if self.typed_tx.is_some() && !delivered {
            return Err(WaFailure::uncertain(
                "WhatsApp ingress was not accepted by the host",
            ));
        }
        if delivered {
            self.seen.lock().insert(id);
        }
        Ok(delivered)
    }

    /// Download and persist one attachment. `Ok(None)` means it cannot be kept
    /// (no storage wired, or permanently unusable content).
    async fn attachment_part(
        &self,
        attachment: &Attachment<'_>,
        message_id: &str,
        scope: tinychannels_bus::HostVoiceScope,
    ) -> Result<Option<ContentPart>, WaFailure> {
        let is_voice = matches!(attachment, Attachment::Audio(audio) if audio.ptt == Some(true));
        let voice_store = self.voice_media_store.as_deref().filter(|_| is_voice);
        let repository = self.media_repository.as_deref();
        if voice_store.is_none() && repository.is_none() {
            return Ok(None);
        }
        let Some(client) = self.client.lock().clone() else {
            return Err(WaFailure::uncertain("WhatsApp Web client is not connected"));
        };
        let limit = if voice_store.is_some() {
            MAX_INBOUND_VOICE_BYTES
        } else {
            MAX_INBOUND_MEDIA_BYTES
        };
        if attachment
            .downloadable()
            .file_length()
            .is_some_and(|length| length > limit)
        {
            tracing::warn!("[whatsapp_web] inbound attachment exceeds its size limit; skipped");
            return Ok(None);
        }
        let bytes = client
            .download(attachment.downloadable())
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp Web attachment download failed"))?;
        let download = DownloadedAttachment {
            bytes,
            content_type: attachment.mime(),
            file_name: attachment.file_name(),
            duration_ms: attachment.duration_ms(),
        };
        if let Some(store) = voice_store {
            let waveform = match attachment {
                Attachment::Audio(audio) => audio
                    .waveform
                    .as_deref()
                    .and_then(inbound::waveform_samples),
                _ => None,
            };
            return match inbound::store_voice_note(store, scope, download, waveform).await {
                Ok(voice_note) => Ok(Some(ContentPart::VoiceNote { voice_note })),
                Err(WaFailure::Rejected(reason)) => {
                    tracing::warn!(reason, "[whatsapp_web] inbound voice note skipped");
                    Ok(None)
                }
                Err(error) => Err(error),
            };
        }
        let Some(repository) = repository else {
            return Ok(None);
        };
        let dedupe_key = format!("whatsapp:{message_id}:{}", attachment.kind().field());
        match inbound::store_attachment(
            repository,
            scope,
            attachment.kind(),
            download,
            attachment.caption(),
            dedupe_key,
        )
        .await
        {
            Ok(part) => Ok(Some(part)),
            Err(WaFailure::Rejected(reason)) => {
                tracing::warn!(reason, "[whatsapp_web] inbound attachment skipped");
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Publish delivery and read receipts for messages we sent.
    pub(super) async fn publish_receipt(&self, receipt: &wacore::types::events::Receipt) {
        use wacore::types::presence::ReceiptType;
        if self.events.is_none() && self.typed_tx.is_none() {
            return;
        }
        if receipt.source.is_from_me {
            return;
        }
        let read = match receipt.r#type {
            ReceiptType::Read => true,
            ReceiptType::Delivered => false,
            _ => return,
        };
        let (sender, _) = WhatsAppWebChannel::sender_identity(&receipt.source.sender, None);
        let chat = receipt.source.chat.to_string();
        let conversation_id = WhatsAppWebChannel::compute_reply_target(&chat, &sender);
        let is_group = WhatsAppWebChannel::is_group_jid(&chat);
        let timestamp = u64::try_from(receipt.timestamp.timestamp_millis()).ok();
        for message_id in &receipt.message_ids {
            let actor = Some(SenderRef {
                id: sender.clone(),
                ..Default::default()
            });
            let (kind, label) = if read {
                (
                    ChannelEventKind::ReadReceipt {
                        message_id: message_id.clone(),
                        actor,
                        timestamp,
                    },
                    "read",
                )
            } else {
                (
                    ChannelEventKind::DeliveredReceipt {
                        message_id: message_id.clone(),
                        actor,
                        timestamp,
                    },
                    "delivered",
                )
            };
            let event = ChannelEvent {
                version: 2,
                event_id: format!("whatsapp:receipt:{message_id}:{label}"),
                source_event_id: Some(message_id.clone()),
                source_timestamp: timestamp,
                channel: ChannelRef {
                    id: "whatsapp".into(),
                    account_id: None,
                },
                conversation: Some(ConversationRef {
                    kind: inbound::conversation_kind(is_group),
                    id: conversation_id.clone(),
                    ..Default::default()
                }),
                event: kind,
            };
            let delivered = if let Some(typed_tx) = &self.typed_tx {
                let (accepted, result) = tokio::sync::oneshot::channel();
                typed_tx
                    .send(super::TypedIngressDelivery {
                        ingress: ChannelIngress::Event(event),
                        accepted,
                    })
                    .await
                    .is_ok()
                    && result.await.unwrap_or(false)
            } else if let Some(sink) = &self.events {
                sink.publish_ingress(ChannelIngress::Event(event)).await.is_ok()
            } else {
                false
            };
            if !delivered {
                tracing::debug!("[whatsapp_web] receipt event could not be delivered");
            }
        }
    }
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
