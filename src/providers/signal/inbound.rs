//! Inbound dispatch for Signal: typed messages with attachments and voice
//! notes, reactions, remote deletes and receipts, all through the ordered
//! ingress path, with the legacy channel as the fallback for text.

use super::native::{SIGNAL_MAX_FILE_BYTES, SIGNAL_MAX_VOICE_BYTES};
use super::{DataMessage, Envelope, SignalChannel};
use crate::providers::native::inbound::{
    InboundFile, Ingest, Ingested, assemble, channel_event, media_of, publish, publish_event,
};
use crate::traits::ChannelMessage;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use tinychannels_bus::{
    ChannelEventKind, ChannelInboundEnvelope, ChannelRef, ConversationKind, ConversationRef,
    SenderRef,
};
use tokio::sync::mpsc;

fn signal_ref() -> ChannelRef {
    ChannelRef {
        id: "signal".into(),
        account_id: None,
    }
}

/// An envelope that passed the story, sender and group filters.
struct Accepted<'a> {
    data: &'a DataMessage,
    sender: String,
    target: String,
    timestamp: u64,
}

impl SignalChannel {
    fn conversation_of(target: &str) -> ConversationRef {
        ConversationRef {
            kind: if target.starts_with("group:") {
                ConversationKind::Group
            } else {
                ConversationKind::Dm
            },
            id: target.to_owned(),
            ..Default::default()
        }
    }

    fn sender_of(envelope: &Envelope, sender: &str) -> SenderRef {
        SenderRef {
            id: sender.to_owned(),
            name: envelope
                .source_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
            ..Default::default()
        }
    }

    fn mark_seen(&self, key: String) -> bool {
        self.native.mark_seen(key)
    }

    fn accept<'a>(&self, envelope: &'a Envelope) -> Option<Accepted<'a>> {
        if self.ignore_stories && envelope.story_message.is_some() {
            return None;
        }
        let data = envelope.data_message.as_ref()?;
        let sender = Self::sender(envelope)?;
        if !self.is_sender_allowed(&sender) || !self.matches_group(data) {
            return None;
        }
        let timestamp = data.timestamp.or(envelope.timestamp).unwrap_or_else(now_ms);
        let target = self.reply_target(data, &sender);
        Some(Accepted {
            data,
            sender,
            target,
            timestamp,
        })
    }

    /// Fetch one attachment's bytes through signal-cli.
    async fn fetch_attachment(&self, id: &str, limit: u64) -> Option<Vec<u8>> {
        let result = self
            .rpc_request("getAttachment", json!({"id": id, "account": &self.account}))
            .await
            .map_err(|error| tracing::warn!(%error, "[signal] attachment could not be fetched"))
            .ok()??;
        let data = result.get("data").and_then(Value::as_str)?;
        let bytes = BASE64.decode(data.as_bytes()).ok()?;
        (!bytes.is_empty() && bytes.len() as u64 <= limit).then_some(bytes)
    }

    async fn ingest_attachments(
        &self,
        data: &DataMessage,
        message_id: &str,
        conversation: &ConversationRef,
    ) -> Ingested {
        let mut ingested = Ingested::default();
        let ingest = Ingest {
            provider: "signal",
            channel: signal_ref(),
            repository: self.native.media_repository.as_ref(),
            voice_store: self.native.voice_media_store.as_ref(),
            transcriber: self.native.transcriber.as_ref(),
            file_bytes: SIGNAL_MAX_FILE_BYTES,
            voice_bytes: SIGNAL_MAX_VOICE_BYTES,
        };
        for attachment in data.attachments.iter().flatten() {
            let Some(id) = attachment.get("id").and_then(Value::as_str) else {
                continue;
            };
            let mime = attachment
                .get("contentType")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let voice = attachment
                .get("voiceNote")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let limit = if voice {
                SIGNAL_MAX_VOICE_BYTES
            } else {
                SIGNAL_MAX_FILE_BYTES
            };
            if attachment.get("size").and_then(Value::as_u64) > Some(limit) {
                tracing::warn!("[signal] skipping an oversized attachment");
                continue;
            }
            if self.native.media_repository.is_none() && self.native.voice_media_store.is_none() {
                continue;
            }
            let Some(bytes) = self.fetch_attachment(id, limit).await else {
                continue;
            };
            ingest
                .add(
                    &mut ingested,
                    conversation,
                    message_id,
                    InboundFile {
                        bytes,
                        mime: mime.to_owned(),
                        name: attachment
                            .get("filename")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        duration_ms: None,
                        voice,
                        dedupe_key: format!("signal:{message_id}:{id}"),
                        transcript: None,
                    },
                )
                .await;
        }
        ingested
    }

    async fn dispatch_reaction(
        &self,
        envelope: &Envelope,
        accepted: &Accepted<'_>,
        reaction: &Value,
    ) {
        let emoji = reaction
            .get("emoji")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let target_ts = reaction
            .get("targetSentTimestamp")
            .or_else(|| reaction.get("targetTimestamp"))
            .and_then(Value::as_u64);
        let removed = reaction
            .get("isRemove")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let Some(target_ts) = target_ts.filter(|_| !emoji.is_empty()) else {
            return;
        };
        let direction = if removed { "removed" } else { "added" };
        let event_id = format!(
            "signal:reaction:{direction}:{target_ts}:{}:{}",
            accepted.sender, accepted.timestamp
        );
        if !self.mark_seen(event_id.clone()) {
            return;
        }
        let message_id = format!("sig_{target_ts}");
        let provider_message_id = Some(target_ts.to_string());
        let actor = Self::sender_of(envelope, &accepted.sender);
        let reaction = tinychannels_bus::channel::ChannelReaction {
            value: emoji.to_owned(),
            custom_emoji_id: None,
        };
        let kind = if removed {
            ChannelEventKind::ReactionRemoved {
                message_id,
                provider_message_id,
                actor,
                reaction,
            }
        } else {
            ChannelEventKind::ReactionAdded {
                message_id,
                provider_message_id,
                actor,
                reaction,
            }
        };
        publish_event(
            "signal",
            self.native.events.as_ref(),
            channel_event(
                "signal",
                event_id,
                Some(accepted.timestamp),
                Some(Self::conversation_of(&accepted.target)),
                kind,
            ),
        )
        .await;
    }

    async fn dispatch_receipt(&self, envelope: &Envelope, receipt: &Value) {
        let Some(sender) = Self::sender(envelope).filter(|s| self.is_sender_allowed(s)) else {
            return;
        };
        let read = receipt.get("isRead").and_then(Value::as_bool) == Some(true);
        let delivered = receipt.get("isDelivery").and_then(Value::as_bool) == Some(true);
        if !(read || delivered) {
            return;
        }
        let when = receipt.get("when").and_then(Value::as_u64);
        let actor = Some(Self::sender_of(envelope, &sender));
        for stamp in receipt
            .get("timestamps")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
        {
            let message_id = format!("sig_{stamp}");
            let (label, kind) = if read {
                (
                    "read",
                    ChannelEventKind::ReadReceipt {
                        message_id,
                        actor: actor.clone(),
                        timestamp: when,
                    },
                )
            } else {
                (
                    "delivered",
                    ChannelEventKind::DeliveredReceipt {
                        message_id,
                        actor: actor.clone(),
                        timestamp: when,
                    },
                )
            };
            publish_event(
                "signal",
                self.native.events.as_ref(),
                channel_event(
                    "signal",
                    format!("signal:{label}:{stamp}:{sender}"),
                    when,
                    Some(Self::conversation_of(&sender)),
                    kind,
                ),
            )
            .await;
        }
    }

    async fn dispatch_delete(&self, envelope: &Envelope, accepted: &Accepted<'_>, deleted: &Value) {
        let Some(stamp) = deleted.get("timestamp").and_then(Value::as_u64) else {
            return;
        };
        let event_id = format!("signal:delete:{stamp}:{}", accepted.sender);
        if !self.mark_seen(event_id.clone()) {
            return;
        }
        let _ = envelope;
        publish_event(
            "signal",
            self.native.events.as_ref(),
            channel_event(
                "signal",
                event_id,
                Some(accepted.timestamp),
                Some(Self::conversation_of(&accepted.target)),
                ChannelEventKind::MessageDeleted {
                    message_id: format!("sig_{stamp}"),
                    provider_message_id: Some(stamp.to_string()),
                },
            ),
        )
        .await;
    }

    /// Handle one SSE envelope. False when the legacy receiver is gone and the
    /// listener should stop.
    pub(super) async fn dispatch_envelope(
        &self,
        envelope: &Envelope,
        tx: &mpsc::Sender<ChannelMessage>,
    ) -> bool {
        if let Some(receipt) = &envelope.receipt_message {
            self.dispatch_receipt(envelope, receipt).await;
            return true;
        }
        let Some(accepted) = self.accept(envelope) else {
            return true;
        };
        let data = accepted.data;
        if let Some(reaction) = &data.reaction {
            self.dispatch_reaction(envelope, &accepted, reaction).await;
            return true;
        }
        if let Some(deleted) = &data.remote_delete {
            self.dispatch_delete(envelope, &accepted, deleted).await;
            return true;
        }
        let has_attachments = data.attachments.as_ref().is_some_and(|a| !a.is_empty());
        if has_attachments && self.ignore_attachments && data.message.is_none() {
            return true;
        }
        let text = data.message.as_deref().unwrap_or_default();
        if !self.mark_seen(format!("{}:{}", accepted.sender, accepted.timestamp)) {
            return true;
        }
        let message_id = format!("sig_{}", accepted.timestamp);
        let conversation = Self::conversation_of(&accepted.target);
        let ingested = if has_attachments && !self.ignore_attachments {
            self.ingest_attachments(data, &message_id, &conversation)
                .await
        } else {
            Ingested::default()
        };
        let parts = assemble(text, None, ingested);
        if parts.is_empty() {
            return true;
        }
        let envelope_out = ChannelInboundEnvelope {
            channel: signal_ref(),
            message_id,
            reply_to_id: None,
            timestamp_unix_ms: Some(accepted.timestamp),
            conversation,
            sender: Self::sender_of(envelope, &accepted.sender),
            text: text.to_owned(),
            media: media_of(&parts),
            parts,
            raw: Some(json!({"providerMessageId": accepted.timestamp.to_string()})),
            ..Default::default()
        };
        if publish("signal", self.native.events.as_ref(), envelope_out).await {
            return true;
        }
        match self.process_envelope(envelope) {
            Some(message) => tx.send(message).await.is_ok(),
            None => true,
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
