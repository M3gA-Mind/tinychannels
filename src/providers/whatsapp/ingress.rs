//! Authenticated webhook ingress for the WhatsApp Business Cloud adapter.
//!
//! Meta POSTs every inbound message, reaction and delivery status to one
//! webhook. The subscription handshake's verify token proves nothing about a
//! POST, so each delivery is authenticated here, against the exact bytes
//! received, with the app secret (`X-Hub-Signature-256`) before anything is
//! parsed. A delivery is also bound to the configured number: a payload for a
//! different `phone_number_id` is refused rather than routed.
//!
//! Redelivery is expected (Meta retries until it sees a 2xx), so accepted
//! message ids are remembered in a bounded window and published once.

use super::WhatsAppChannel;
use crate::providers::whatsapp_shared::error::WaFailure;
use crate::providers::whatsapp_shared::inbound::{
    self, InboundKind, MAX_INBOUND_MEDIA_BYTES, MAX_INBOUND_VOICE_BYTES,
};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::Sha256;
use tinychannels_bus::{
    ChannelEvent, ChannelEventKind, ChannelInboundEnvelope, ChannelIngress, ChannelReaction,
    ChannelRef, ContactMethod, ContentPart, ConversationRef, SenderRef, WebhookDeliveryOutcome,
};

/// Largest webhook body accepted (Meta batches several messages per POST).
pub(super) const MAX_WEBHOOK_BYTES: usize = 2 * 1024 * 1024;
/// How many accepted ids are remembered for duplicate suppression.
pub(super) const SEEN_WINDOW: usize = 4096;
/// How many conversations remember their latest inbound message id.
pub(super) const ANCHOR_WINDOW: usize = 512;

/// Check `X-Hub-Signature-256` against the received bytes. Missing,
/// malformed and wrong signatures are indistinguishable to the caller.
pub(super) fn verify_signature(
    app_secret: &str,
    header: Option<&str>,
    body: &[u8],
) -> Result<(), WaFailure> {
    let denied = || WaFailure::rejected("WhatsApp webhook signature is not valid");
    let provided = header
        .map(str::trim)
        .and_then(|value| value.strip_prefix("sha256="))
        .and_then(|value| hex::decode(value.trim()).ok())
        .ok_or_else(denied)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(app_secret.as_bytes()).map_err(|_| denied())?;
    mac.update(body);
    // `verify_slice` compares in constant time.
    mac.verify_slice(&provided).map_err(|_| denied())
}

/// One normalized item extracted from a delivery.
enum Item {
    Message(Box<ChannelInboundEnvelope>),
    Event(Box<ChannelEvent>),
}

fn timestamp_ms(value: &Value) -> Option<u64> {
    value
        .get("timestamp")
        .and_then(|timestamp| {
            timestamp
                .as_str()
                .and_then(|text| text.parse::<u64>().ok())
                .or_else(|| timestamp.as_u64())
        })
        .map(inbound::seconds_to_ms)
}

fn plus(wa_id: &str) -> String {
    format!("+{}", wa_id.trim_start_matches('+'))
}

fn text_of<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().filter(|text| !text.trim().is_empty())
}

impl WhatsAppChannel {
    /// Answer the subscription handshake. Only the exact configured verify
    /// token succeeds, and only for `hub.mode=subscribe`.
    pub(super) fn verify_challenge(
        &self,
        mode: &str,
        token: &str,
        challenge: &str,
    ) -> Option<String> {
        let expected = self.verify_token.as_bytes();
        let provided = token.as_bytes();
        // Compare every byte so the answer does not leak the matching prefix.
        let equal = expected.len() == provided.len()
            && expected
                .iter()
                .zip(provided)
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
        (mode == "subscribe" && !expected.is_empty() && equal && !challenge.is_empty())
            .then(|| challenge.to_owned())
    }

    /// Authenticate and ingest one raw delivery.
    pub(super) async fn ingest_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> Result<WebhookDeliveryOutcome, WaFailure> {
        if body.len() > MAX_WEBHOOK_BYTES {
            return Err(WaFailure::rejected(
                "WhatsApp webhook body exceeds its limit",
            ));
        }
        let secret = self
            .app_secret
            .as_deref()
            .filter(|secret| !secret.is_empty())
            .ok_or_else(|| {
                WaFailure::rejected("WhatsApp webhook ingress requires the app secret")
            })?;
        verify_signature(secret, signature, body)?;
        let payload: Value = serde_json::from_slice(body)
            .map_err(|_| WaFailure::rejected("WhatsApp webhook body is not JSON"))?;
        if payload.get("object").and_then(Value::as_str) != Some("whatsapp_business_account") {
            return Err(WaFailure::rejected(
                "WhatsApp webhook object is not supported",
            ));
        }
        let _gate = self.ingest_gate.lock().await;
        let mut outcome = WebhookDeliveryOutcome::default();
        let mut mismatched = 0usize;
        let mut matched = 0usize;
        for entry in payload
            .get("entry")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            for change in entry
                .get("changes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if change
                    .get("field")
                    .and_then(Value::as_str)
                    .is_some_and(|field| field != "messages")
                {
                    continue;
                }
                let Some(value) = change.get("value") else {
                    continue;
                };
                // Bind the delivery to the configured number before routing it.
                let destination = text_of(value, &["metadata", "phone_number_id"]);
                if destination != Some(self.endpoint_id.as_str()) {
                    mismatched += 1;
                    continue;
                }
                matched += 1;
                self.ingest_value(value, &mut outcome).await?;
            }
        }
        if mismatched > 0 && matched == 0 {
            tracing::warn!("[whatsapp] webhook addressed another phone number; refused");
            return Err(WaFailure::rejected(
                "WhatsApp webhook is addressed to a different phone number",
            ));
        }
        tracing::debug!(
            messages = outcome.messages,
            events = outcome.events,
            duplicates = outcome.duplicates,
            "[whatsapp] webhook ingested"
        );
        Ok(outcome)
    }

    async fn ingest_value(
        &self,
        value: &Value,
        outcome: &mut WebhookDeliveryOutcome,
    ) -> Result<(), WaFailure> {
        for message in value
            .get("messages")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(id) = text_of(message, &["id"]) else {
                continue;
            };
            if self.seen.lock().contains(id) {
                outcome.duplicates += 1;
                continue;
            }
            let Some(item) = self.message_item(value, message).await? else {
                // Not for us (disallowed sender, unsupported kind): remember it
                // so a retry of the same delivery stays cheap.
                self.seen.lock().insert(id.to_owned());
                continue;
            };
            self.publish(item).await?;
            self.seen.lock().insert(id.to_owned());
            match message.get("type").and_then(Value::as_str) {
                Some("reaction") => outcome.events += 1,
                _ => outcome.messages += 1,
            }
        }
        for status in value
            .get("statuses")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let (Some(id), Some(kind)) = (text_of(status, &["id"]), text_of(status, &["status"]))
            else {
                continue;
            };
            let key = format!("status:{id}:{kind}");
            if self.seen.lock().contains(&key) {
                outcome.duplicates += 1;
                continue;
            }
            let Some(event) = Self::status_event(status, id, kind) else {
                continue;
            };
            self.publish(Item::Event(Box::new(event))).await?;
            self.seen.lock().insert(key);
            outcome.events += 1;
        }
        Ok(())
    }

    fn status_event(status: &Value, id: &str, kind: &str) -> Option<ChannelEvent> {
        let recipient = text_of(status, &["recipient_id"]).map(plus)?;
        let timestamp = timestamp_ms(status);
        let actor = Some(SenderRef {
            id: recipient.clone(),
            ..Default::default()
        });
        let event = match kind {
            "delivered" => ChannelEventKind::DeliveredReceipt {
                message_id: id.to_owned(),
                actor,
                timestamp,
            },
            "read" => ChannelEventKind::ReadReceipt {
                message_id: id.to_owned(),
                actor,
                timestamp,
            },
            "failed" => ChannelEventKind::Lifecycle {
                state: "message_failed".into(),
                reason: status
                    .get("errors")
                    .and_then(Value::as_array)
                    .and_then(|errors| errors.first())
                    .and_then(|error| error.get("code"))
                    .and_then(Value::as_i64)
                    .map(|code| format!("provider error code {code}")),
            },
            // "sent" only confirms what the send call already returned.
            _ => return None,
        };
        Some(ChannelEvent {
            version: 2,
            event_id: format!("whatsapp:status:{id}:{kind}"),
            source_event_id: Some(id.to_owned()),
            source_timestamp: timestamp,
            channel: ChannelRef {
                id: "whatsapp".into(),
                account_id: None,
            },
            conversation: Some(ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: recipient,
                ..Default::default()
            }),
            event,
        })
    }

    async fn message_item(
        &self,
        value: &Value,
        message: &Value,
    ) -> Result<Option<Item>, WaFailure> {
        let Some(from) = text_of(message, &["from"]) else {
            return Ok(None);
        };
        let sender = plus(from);
        if !self.is_number_allowed(&sender) {
            tracing::warn!("[whatsapp] ignoring a message from a number that is not allowed");
            return Ok(None);
        }
        let Some(id) = text_of(message, &["id"]) else {
            return Ok(None);
        };
        let kind = message
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let conversation = ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: sender.clone(),
            ..Default::default()
        };
        let sender_ref = SenderRef {
            id: sender.clone(),
            name: Self::contact_name(value, from),
            ..Default::default()
        };
        let timestamp = timestamp_ms(message);
        if kind == "reaction" {
            return Ok(self
                .reaction_event(message, id, conversation, sender_ref, timestamp)
                .map(|event| Item::Event(Box::new(event))));
        }
        let mut parts = Vec::new();
        let mut text = String::new();
        let scope = inbound::inbound_scope("whatsapp", &conversation, id);
        match kind {
            "text" => {
                if let Some(body) = text_of(message, &["text", "body"]) {
                    text = body.to_owned();
                }
            }
            "button" => {
                if let Some(label) = text_of(message, &["button", "text"]) {
                    text = label.to_owned();
                }
            }
            "interactive" => {
                let reply = message.get("interactive");
                if let Some(label) = reply
                    .and_then(|reply| text_of(reply, &["button_reply", "title"]))
                    .or_else(|| reply.and_then(|reply| text_of(reply, &["list_reply", "title"])))
                {
                    text = label.to_owned();
                }
            }
            "image" | "video" | "document" | "sticker" | "audio" => {
                let media = message.get(kind).unwrap_or(&Value::Null);
                if let Some(part) = self.attachment_parts(kind, media, id, scope).await? {
                    if let ContentPart::Image {
                        caption: Some(caption),
                        ..
                    }
                    | ContentPart::Video {
                        caption: Some(caption),
                        ..
                    }
                    | ContentPart::File {
                        caption: Some(caption),
                        ..
                    } = &part
                    {
                        text.clone_from(caption);
                    }
                    parts.push(part);
                } else if let Some(caption) = text_of(media, &["caption"]) {
                    text = caption.to_owned();
                }
            }
            "location" => {
                let location = message.get("location").unwrap_or(&Value::Null);
                if let (Some(latitude), Some(longitude)) = (
                    location.get("latitude").and_then(Value::as_f64),
                    location.get("longitude").and_then(Value::as_f64),
                ) && (-90.0..=90.0).contains(&latitude)
                    && (-180.0..=180.0).contains(&longitude)
                {
                    parts.push(ContentPart::Location {
                        latitude,
                        longitude,
                        label: text_of(location, &["name"]).map(str::to_owned),
                        address: text_of(location, &["address"]).map(str::to_owned),
                        accuracy_meters: None,
                        live_until_unix_ms: None,
                    });
                }
            }
            "contacts" => {
                for contact in message
                    .get("contacts")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    parts.push(Self::contact_part(contact));
                }
            }
            other => {
                tracing::debug!(
                    kind = other,
                    "[whatsapp] unsupported inbound message kind skipped"
                );
                return Ok(None);
            }
        }
        if text.trim().is_empty() && parts.is_empty() {
            return Ok(None);
        }
        let mut content = Vec::new();
        if !text.trim().is_empty() {
            content.push(ContentPart::Text {
                text: text.clone(),
                markdown_dialect: None,
            });
        }
        content.extend(parts);
        self.anchors.lock().insert(&sender, id);
        let media = inbound::envelope_media(&content);
        Ok(Some(Item::Message(Box::new(ChannelInboundEnvelope {
            channel: ChannelRef {
                id: "whatsapp".into(),
                account_id: None,
            },
            message_id: id.to_owned(),
            reply_to_id: text_of(message, &["context", "id"]).map(str::to_owned),
            timestamp_unix_ms: timestamp,
            conversation,
            sender: sender_ref,
            text,
            parts: content,
            media,
            raw: Some(serde_json::json!({ "providerMessageId": id })),
            ..Default::default()
        }))))
    }

    fn contact_part(contact: &Value) -> ContentPart {
        let methods = |key: &str, field: &str| -> Vec<ContactMethod> {
            contact
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    Some(ContactMethod {
                        value: text_of(entry, &[field])?.to_owned(),
                        label: text_of(entry, &["type"]).map(str::to_owned),
                    })
                })
                .collect()
        };
        ContentPart::Contact {
            display_name: text_of(contact, &["name", "formatted_name"])
                .unwrap_or_default()
                .to_owned(),
            phone_numbers: methods("phones", "phone"),
            emails: methods("emails", "email"),
            organization: text_of(contact, &["org", "company"]).map(str::to_owned),
        }
    }

    fn reaction_event(
        &self,
        message: &Value,
        id: &str,
        conversation: ConversationRef,
        actor: SenderRef,
        timestamp: Option<u64>,
    ) -> Option<ChannelEvent> {
        let reaction = message.get("reaction")?;
        let target = text_of(reaction, &["message_id"])?.to_owned();
        let emoji = text_of(reaction, &["emoji"]);
        let channel_reaction = ChannelReaction {
            value: emoji.unwrap_or_default().to_owned(),
            custom_emoji_id: None,
        };
        let event = if emoji.is_some() {
            ChannelEventKind::ReactionAdded {
                message_id: target.clone(),
                provider_message_id: Some(target),
                actor,
                reaction: channel_reaction,
            }
        } else {
            ChannelEventKind::ReactionRemoved {
                message_id: target.clone(),
                provider_message_id: Some(target),
                actor,
                reaction: channel_reaction,
            }
        };
        Some(ChannelEvent {
            version: 2,
            event_id: format!("whatsapp:reaction:{id}"),
            source_event_id: Some(id.to_owned()),
            source_timestamp: timestamp,
            channel: ChannelRef {
                id: "whatsapp".into(),
                account_id: None,
            },
            conversation: Some(conversation),
            event,
        })
    }

    /// Download and persist one attachment. `Ok(None)` means the adapter
    /// cannot keep this attachment (no storage wired, or the content is
    /// permanently unusable); an `Err` is a transient failure the delivery
    /// must report so Meta redelivers it.
    async fn attachment_parts(
        &self,
        kind: &str,
        media: &Value,
        message_id: &str,
        scope: tinychannels_bus::HostVoiceScope,
    ) -> Result<Option<ContentPart>, WaFailure> {
        let Some(media_id) = text_of(media, &["id"]) else {
            return Ok(None);
        };
        let is_voice = kind == "audio" && media.get("voice").and_then(Value::as_bool) == Some(true);
        let inbound_kind = match kind {
            "image" => InboundKind::Image,
            "video" => InboundKind::Video,
            "document" => InboundKind::Document,
            "sticker" => InboundKind::Sticker,
            _ => InboundKind::Audio,
        };
        if is_voice && let Some(store) = self.voice_media_store.as_deref() {
            let mut download = match self.download_media(media_id, MAX_INBOUND_VOICE_BYTES).await {
                Ok(download) => download,
                Err(WaFailure::Rejected(reason)) => {
                    tracing::warn!(reason, "[whatsapp] inbound voice note skipped");
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            if download.content_type.is_empty() {
                download.content_type = text_of(media, &["mime_type"])
                    .unwrap_or_default()
                    .to_owned();
            }
            return match inbound::store_voice_note(store, scope, download, None).await {
                Ok(voice_note) => Ok(Some(ContentPart::VoiceNote { voice_note })),
                Err(WaFailure::Rejected(reason)) => {
                    tracing::warn!(reason, "[whatsapp] inbound voice note skipped");
                    Ok(None)
                }
                Err(error) => Err(error),
            };
        }
        let Some(repository) = self.media_repository.as_deref() else {
            return Ok(None);
        };
        let download = match self.download_media(media_id, MAX_INBOUND_MEDIA_BYTES).await {
            Ok(mut download) => {
                if download.content_type.is_empty() {
                    download.content_type = text_of(media, &["mime_type"])
                        .unwrap_or_default()
                        .to_owned();
                }
                download.file_name = text_of(media, &["filename"]).map(str::to_owned);
                download
            }
            Err(WaFailure::Rejected(reason)) => {
                tracing::warn!(reason, "[whatsapp] inbound attachment skipped");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let caption = text_of(media, &["caption"]).map(str::to_owned);
        let dedupe_key = format!("whatsapp:{message_id}:{}", inbound_kind.field());
        match inbound::store_attachment(
            repository,
            scope,
            inbound_kind,
            download,
            caption,
            dedupe_key,
        )
        .await
        {
            Ok(part) => Ok(Some(part)),
            Err(WaFailure::Rejected(reason)) => {
                tracing::warn!(reason, "[whatsapp] inbound attachment skipped");
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Hand one item to the host on the ordered ingress path; fall back to the
    /// legacy listener channel when the host cannot take typed ingress.
    async fn publish(&self, item: Item) -> Result<(), WaFailure> {
        let sink = self.events.as_ref().ok_or_else(|| {
            WaFailure::rejected("WhatsApp webhook ingress has no host event sink")
        })?;
        let (ingress, legacy) = match item {
            Item::Message(envelope) => {
                let legacy = crate::channel::legacy_message_from_inbound_envelope(
                    &envelope,
                    envelope.timestamp_unix_ms.unwrap_or_default() / 1000,
                )
                .ok();
                (ChannelIngress::Inbound(*envelope), legacy)
            }
            Item::Event(event) => (ChannelIngress::Event(*event), None),
        };
        let delivered = sink.publish_ingress(ingress).await.map_err(|_| {
            WaFailure::uncertain("WhatsApp ingress could not be delivered to the host")
        })?;
        if delivered {
            return Ok(());
        }
        let tx = self.listener.lock().clone();
        match (tx, legacy) {
            (Some(tx), Some(message)) => tx
                .send(message)
                .await
                .map_err(|_| WaFailure::uncertain("WhatsApp listener channel is closed")),
            _ => Err(WaFailure::uncertain(
                "WhatsApp ingress had no ordered sink or listener",
            )),
        }
    }
}

#[cfg(test)]
#[path = "ingress_tests.rs"]
mod tests;
