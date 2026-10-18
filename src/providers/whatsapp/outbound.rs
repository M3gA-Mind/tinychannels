//! Typed outbound delivery for the WhatsApp Business Cloud adapter.

use super::WhatsAppChannel;
use crate::channel::{
    ChannelSendOutcome, MessageReceipt, MessageReceiptPart, MessageReceiptPartKind,
};
use crate::providers::whatsapp_shared::error::WaFailure;
use crate::providers::whatsapp_shared::plan::{
    self, AttachmentKind, OutboundScope, PlanPorts, PlannedPart, plan_parts,
};
use crate::providers::whatsapp_shared::text::split_text;
use crate::providers::whatsapp_shared::vcard::{ContactCard, phone_digits};
use serde_json::{Value, json};
use tinychannels_bus::ChannelOutboundMessage;
use tinychannels_bus::channel::MessageReceiptSourceResult;

/// Longest text body the Cloud API accepts.
pub(super) const TEXT_LIMIT: usize = 4096;

/// The digits the Cloud API addresses a recipient by.
pub(super) fn recipient_digits(conversation_id: &str) -> Result<String, WaFailure> {
    let digits = phone_digits(conversation_id);
    if digits.is_empty() {
        return Err(WaFailure::rejected(
            "WhatsApp conversation id must be a phone number",
        ));
    }
    Ok(digits)
}

fn envelope(to: &str, kind: &str, reply_to: Option<&str>, content: Value) -> Value {
    let mut body = json!({
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to,
        "type": kind,
    });
    body[kind] = content;
    if let Some(reply_to) = reply_to {
        body["context"] = json!({ "message_id": reply_to });
    }
    body
}

pub(super) fn text_body(to: &str, text: &str, reply_to: Option<&str>) -> Value {
    envelope(
        to,
        "text",
        reply_to,
        json!({ "preview_url": false, "body": text }),
    )
}

pub(super) fn media_body(
    to: &str,
    kind: AttachmentKind,
    media_id: &str,
    caption: Option<&str>,
    file_name: Option<&str>,
    reply_to: Option<&str>,
) -> Value {
    let (name, with_caption) = match kind {
        AttachmentKind::Image => ("image", true),
        AttachmentKind::Video => ("video", true),
        AttachmentKind::Audio => ("audio", false),
        AttachmentKind::Document => ("document", true),
        AttachmentKind::Sticker => ("sticker", false),
    };
    let mut content = json!({ "id": media_id });
    if with_caption && let Some(caption) = caption {
        content["caption"] = json!(caption);
    }
    if kind == AttachmentKind::Document
        && let Some(file_name) = file_name
    {
        content["filename"] = json!(file_name);
    }
    envelope(to, name, reply_to, content)
}

pub(super) fn location_body(
    to: &str,
    latitude: f64,
    longitude: f64,
    name: Option<&str>,
    address: Option<&str>,
    reply_to: Option<&str>,
) -> Value {
    let mut content = json!({ "latitude": latitude, "longitude": longitude });
    if let Some(name) = name {
        content["name"] = json!(name);
    }
    if let Some(address) = address {
        content["address"] = json!(address);
    }
    envelope(to, "location", reply_to, content)
}

pub(super) fn contact_body(to: &str, card: &ContactCard, reply_to: Option<&str>) -> Value {
    let kind = |label: Option<&str>, allowed: &[&str], fallback: &str| {
        label
            .map(str::trim)
            .map(str::to_ascii_uppercase)
            .filter(|label| allowed.contains(&label.as_str()))
            .unwrap_or_else(|| fallback.to_owned())
    };
    let name = if card.display_name.trim().is_empty() {
        card.phones
            .first()
            .map(|phone| phone.value.clone())
            .unwrap_or_default()
    } else {
        card.display_name.clone()
    };
    let mut contact = json!({
        "name": { "formatted_name": name, "first_name": name },
    });
    if !card.phones.is_empty() {
        contact["phones"] = Value::Array(
            card.phones
                .iter()
                .map(|phone| {
                    let mut entry = json!({
                        "phone": phone.value,
                        "type": kind(phone.label.as_deref(), &["CELL", "MAIN", "IPHONE", "HOME", "WORK"], "CELL"),
                    });
                    let digits = phone_digits(&phone.value);
                    if !digits.is_empty() {
                        entry["wa_id"] = json!(digits);
                    }
                    entry
                })
                .collect(),
        );
    }
    if !card.emails.is_empty() {
        contact["emails"] = Value::Array(
            card.emails
                .iter()
                .map(|email| {
                    json!({
                        "email": email.value,
                        "type": kind(email.label.as_deref(), &["HOME", "WORK"], "WORK"),
                    })
                })
                .collect(),
        );
    }
    if let Some(company) = card.organization.as_deref().filter(|org| !org.is_empty()) {
        contact["org"] = json!({ "company": company });
    }
    envelope(to, "contacts", reply_to, Value::Array(vec![contact]))
}

pub(super) fn reaction_body(to: &str, message_id: &str, emoji: &str) -> Value {
    envelope(
        to,
        "reaction",
        None,
        json!({ "message_id": message_id, "emoji": emoji }),
    )
}

pub(super) fn read_body(message_id: &str, typing: bool) -> Value {
    let mut body = json!({
        "messaging_product": "whatsapp",
        "status": "read",
        "message_id": message_id,
    });
    if typing {
        body["typing_indicator"] = json!({ "type": "text" });
    }
    body
}

fn attachment_name(kind: AttachmentKind) -> &'static str {
    match kind {
        AttachmentKind::Image => "image",
        AttachmentKind::Video => "video",
        AttachmentKind::Audio => "audio",
        AttachmentKind::Document => "document",
        AttachmentKind::Sticker => "sticker",
    }
}

fn attachment_receipt_kind(kind: AttachmentKind) -> MessageReceiptPartKind {
    match kind {
        AttachmentKind::Audio => MessageReceiptPartKind::Voice,
        _ => MessageReceiptPartKind::Media,
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
        // The Cloud API cannot edit or unsend, so no token is minted.
        edit_token: None,
        delete_token: None,
        raw: Some(MessageReceiptSourceResult {
            channel: Some("whatsapp".into()),
            message_id: Some(id.to_owned()),
            conversation_id: Some(scope.conversation.id.clone()),
            ..Default::default()
        }),
    }
}

impl WhatsAppChannel {
    pub(super) fn media_limits() -> plan::MediaLimits {
        plan::MediaLimits {
            image: 5 * 1024 * 1024,
            video: 16 * 1024 * 1024,
            audio: 16 * 1024 * 1024,
            document: 20 * 1024 * 1024,
            sticker: 500 * 1024,
            caption_chars: 1024,
        }
    }

    /// Send one planned part. Returns every message the provider confirmed and,
    /// separately, the failure that stopped the part, so a text split into
    /// chunks never loses the ids of the chunks that were delivered.
    async fn send_planned(
        &self,
        to: &str,
        part: PlannedPart,
        reply_to: Option<&str>,
    ) -> (Vec<(String, MessageReceiptPartKind)>, Option<WaFailure>) {
        let mut sent = Vec::new();
        let outcome: Result<(), WaFailure> = async {
            match part {
                PlannedPart::Text(text) => {
                    for (index, chunk) in split_text(&text, TEXT_LIMIT).into_iter().enumerate() {
                        let reply = if index == 0 { reply_to } else { None };
                        let answer = self.post_messages(&text_body(to, &chunk, reply)).await?;
                        sent.push((
                            Self::answered_message_id(&answer)?,
                            MessageReceiptPartKind::Text,
                        ));
                    }
                }
                PlannedPart::Attachment {
                    kind,
                    bytes,
                    content_type,
                    file_name,
                    caption,
                    ..
                } => {
                    let upload_name = file_name
                        .clone()
                        .unwrap_or_else(|| attachment_name(kind).into());
                    let media_id = self
                        .upload_media(bytes, &content_type, &upload_name)
                        .await?;
                    let body = media_body(
                        to,
                        kind,
                        &media_id,
                        caption.as_deref(),
                        file_name.as_deref(),
                        reply_to,
                    );
                    let id = Self::answered_message_id(&self.post_messages(&body).await?)?;
                    sent.push((id, attachment_receipt_kind(kind)));
                }
                PlannedPart::Voice {
                    bytes,
                    content_type,
                    ..
                } => {
                    // OGG/Opus uploaded as audio is what WhatsApp renders as a
                    // voice message; the Cloud API has no waveform field.
                    let media_id = self.upload_media(bytes, &content_type, "voice.ogg").await?;
                    let body =
                        media_body(to, AttachmentKind::Audio, &media_id, None, None, reply_to);
                    let id = Self::answered_message_id(&self.post_messages(&body).await?)?;
                    sent.push((id, MessageReceiptPartKind::Voice));
                }
                PlannedPart::Location {
                    latitude,
                    longitude,
                    name,
                    address,
                } => {
                    let body = location_body(
                        to,
                        latitude,
                        longitude,
                        name.as_deref(),
                        address.as_deref(),
                        reply_to,
                    );
                    let id = Self::answered_message_id(&self.post_messages(&body).await?)?;
                    sent.push((id, MessageReceiptPartKind::Unknown));
                }
                PlannedPart::Contact(card) => {
                    let body = contact_body(to, &card, reply_to);
                    let id = Self::answered_message_id(&self.post_messages(&body).await?)?;
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
        let to = recipient_digits(&scope.conversation.id)?;
        let planned = plan_parts(
            message,
            &scope,
            PlanPorts {
                media: self.media_repository.as_deref(),
                voices: self.voice_media_store.as_deref(),
            },
            &Self::media_limits(),
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
            let (sent, stopped) = self.send_planned(&to, planned_part, reply).await;
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
        let receipt = MessageReceipt {
            idempotency_key: Some(message.idempotency_key.clone()),
            channel: Some(scope.channel.clone()),
            conversation: Some(scope.conversation.clone()),
            primary_platform_message_id: ids.first().cloned(),
            platform_message_ids: ids,
            parts,
            thread_id: None,
            reply_to_id: scope.reply_to_id.clone(),
            edit_token: None,
            delete_token: None,
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
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;
