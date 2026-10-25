//! Inbound dispatch for Signal: typed messages with attachments and voice
//! notes, reactions, remote deletes and receipts, all through the ordered
//! ingress path, with the legacy channel as the fallback for text.

use super::SignalChannel;
use super::listener::{DataMessage, Envelope};
use super::outbound::{GROUP_TARGET_PREFIX, SIGNAL_MAX_FILE_BYTES, SIGNAL_MAX_VOICE_BYTES};
use crate::channel::ChannelInboundSink;
use crate::providers::native::SeenReservationResult;
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
use tokio::sync::{mpsc, watch};

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

    fn reserve_seen(&self, key: String) -> SeenReservationResult {
        self.native.reserve_seen(key)
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
        sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> bool {
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
            return true;
        };
        let direction = if removed { "removed" } else { "added" };
        let event_id = format!(
            "signal:reaction:{direction}:{target_ts}:{}:{}",
            accepted.sender, accepted.timestamp
        );
        let reservation = match self.reserve_seen(event_id.clone()) {
            SeenReservationResult::Accepted => return true,
            SeenReservationResult::Pending => return false,
            SeenReservationResult::Reserved(reservation) => reservation,
        };
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
        let event = channel_event(
            "signal",
            event_id,
            Some(accepted.timestamp),
            Some(Self::conversation_of(&accepted.target)),
            kind,
        );
        if self
            .publish_typed(sink, tinychannels_bus::ChannelIngress::Event(event))
            .await
        {
            reservation.commit();
            true
        } else {
            false
        }
    }

    async fn dispatch_receipt(
        &self,
        envelope: &Envelope,
        receipt: &Value,
        sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> bool {
        let Some(sender) = Self::sender(envelope).filter(|s| self.is_sender_allowed(s)) else {
            return true;
        };
        let read = receipt.get("isRead").and_then(Value::as_bool) == Some(true);
        let delivered = receipt.get("isDelivery").and_then(Value::as_bool) == Some(true);
        if !(read || delivered) {
            return true;
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
            let event_id = format!("signal:{label}:{stamp}:{sender}");
            let event = channel_event(
                "signal",
                event_id.clone(),
                when,
                Some(Self::conversation_of(&sender)),
                kind,
            );
            let reservation = match self.reserve_seen(event_id) {
                SeenReservationResult::Accepted => continue,
                SeenReservationResult::Pending => return false,
                SeenReservationResult::Reserved(reservation) => reservation,
            };
            if !self
                .publish_typed(sink, tinychannels_bus::ChannelIngress::Event(event))
                .await
            {
                return false;
            }
            reservation.commit();
        }
        true
    }

    async fn dispatch_delete(
        &self,
        envelope: &Envelope,
        accepted: &Accepted<'_>,
        deleted: &Value,
        sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
    ) -> bool {
        let Some(stamp) = deleted.get("timestamp").and_then(Value::as_u64) else {
            return true;
        };
        let event_id = format!("signal:delete:{stamp}:{}", accepted.sender);
        let reservation = match self.reserve_seen(event_id.clone()) {
            SeenReservationResult::Accepted => return true,
            SeenReservationResult::Pending => return false,
            SeenReservationResult::Reserved(reservation) => reservation,
        };
        let _ = envelope;
        let event = channel_event(
            "signal",
            event_id,
            Some(accepted.timestamp),
            Some(Self::conversation_of(&accepted.target)),
            ChannelEventKind::MessageDeleted {
                message_id: format!("sig_{stamp}"),
                provider_message_id: Some(stamp.to_string()),
            },
        );
        if self
            .publish_typed(sink, tinychannels_bus::ChannelIngress::Event(event))
            .await
        {
            reservation.commit();
            true
        } else {
            false
        }
    }

    async fn publish_typed(
        &self,
        sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> bool {
        if let Some(sink) = sink {
            return sink.push_ingress(ingress).await.is_ok();
        }
        match ingress {
            tinychannels_bus::ChannelIngress::Inbound(envelope) => {
                publish("signal", self.native.events.as_ref(), envelope).await
            }
            tinychannels_bus::ChannelIngress::Event(event) => {
                publish_event("signal", self.native.events.as_ref(), event).await;
                true
            }
        }
    }

    /// Handle one SSE envelope. False when the legacy receiver is gone and the
    /// listener should stop.
    #[cfg(test)]
    pub(super) async fn dispatch_envelope(
        &self,
        envelope: &Envelope,
        tx: &mpsc::Sender<ChannelMessage>,
    ) -> bool {
        self.dispatch_envelope_to(envelope, None, Some(tx)).await
    }

    pub(super) async fn dispatch_typed_envelope(
        &self,
        envelope: &Envelope,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> bool {
        self.dispatch_envelope_to(envelope, Some(sink), None).await
    }

    pub(super) async fn dispatch_typed_until_cancel(
        &self,
        envelope: &Envelope,
        sink: &(dyn ChannelInboundSink + Send + Sync),
        cancellation: &mut watch::Receiver<bool>,
    ) -> anyhow::Result<Option<bool>> {
        tokio::select! {
            biased;
            _ = cancellation.wait_for(|value| *value) => Ok(None),
            delivered = self.dispatch_typed_envelope(envelope, sink) => {
                if delivered {
                    Ok(Some(true))
                } else {
                    anyhow::bail!("Signal inbound sink rejected an event")
                }
            }
        }
    }

    async fn dispatch_envelope_to(
        &self,
        envelope: &Envelope,
        sink: Option<&(dyn ChannelInboundSink + Send + Sync)>,
        tx: Option<&mpsc::Sender<ChannelMessage>>,
    ) -> bool {
        if let Some(receipt) = &envelope.receipt_message {
            return self.dispatch_receipt(envelope, receipt, sink).await;
        }
        let Some(accepted) = self.accept(envelope) else {
            return true;
        };
        let data = accepted.data;
        if let Some(reaction) = &data.reaction {
            return self
                .dispatch_reaction(envelope, &accepted, reaction, sink)
                .await;
        }
        if let Some(deleted) = &data.remote_delete {
            return self
                .dispatch_delete(envelope, &accepted, deleted, sink)
                .await;
        }
        let has_attachments = data.attachments.as_ref().is_some_and(|a| !a.is_empty());
        if has_attachments && self.ignore_attachments && data.message.is_none() {
            return true;
        }
        let text = data.message.as_deref().unwrap_or_default();
        let reservation =
            match self.reserve_seen(format!("{}:{}", accepted.sender, accepted.timestamp)) {
                SeenReservationResult::Accepted => return true,
                SeenReservationResult::Pending => return false,
                SeenReservationResult::Reserved(reservation) => reservation,
            };
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
            reservation.commit();
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
        if self
            .publish_typed(
                sink,
                tinychannels_bus::ChannelIngress::Inbound(envelope_out),
            )
            .await
        {
            reservation.commit();
            return true;
        }
        let Some(tx) = tx else {
            return false;
        };
        let delivered = match self.process_envelope(envelope) {
            Some(message) => tx.send(message).await.is_ok(),
            None => true,
        };
        if delivered {
            reservation.commit();
        }
        delivered
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

impl SignalChannel {
    /// Effective sender: prefer `sourceNumber` (E.164), fall back to `source`.
    pub(super) fn sender(envelope: &Envelope) -> Option<String> {
        envelope
            .source_number
            .as_deref()
            .or(envelope.source.as_deref())
            .map(String::from)
    }

    pub(super) fn is_sender_allowed(&self, sender: &str) -> bool {
        if self.allowed_from.iter().any(|u| u == "*") {
            return true;
        }
        self.allowed_from.iter().any(|u| u == sender)
    }

    /// Check whether the message targets the configured group.
    /// If no `group_id` is configured (None), all DMs and groups are accepted.
    /// Use "dm" to filter DMs only.
    pub(super) fn matches_group(&self, data_msg: &DataMessage) -> bool {
        let Some(ref expected) = self.group_id else {
            return true;
        };
        match data_msg
            .group_info
            .as_ref()
            .and_then(|g| g.group_id.as_deref())
        {
            Some(gid) => gid == expected.as_str(),
            None => expected.eq_ignore_ascii_case("dm"),
        }
    }

    /// Determine the send target: group id or the sender's number.
    pub(super) fn reply_target(&self, data_msg: &DataMessage, sender: &str) -> String {
        if let Some(group_id) = data_msg
            .group_info
            .as_ref()
            .and_then(|g| g.group_id.as_deref())
        {
            format!("{GROUP_TARGET_PREFIX}{group_id}")
        } else {
            sender.to_string()
        }
    }

    /// Send a JSON-RPC request to signal-cli daemon.
    /// Process a single SSE envelope, returning a ChannelMessage if valid.
    pub(super) fn process_envelope(&self, envelope: &Envelope) -> Option<ChannelMessage> {
        // Skip story messages when configured
        if self.ignore_stories && envelope.story_message.is_some() {
            return None;
        }

        let data_msg = envelope.data_message.as_ref()?;

        // Skip attachment-only messages when configured
        if self.ignore_attachments {
            let has_attachments = data_msg.attachments.as_ref().is_some_and(|a| !a.is_empty());
            if has_attachments && data_msg.message.is_none() {
                return None;
            }
        }

        let text = data_msg.message.as_deref().filter(|t| !t.is_empty())?;
        let sender = Self::sender(envelope)?;

        if !self.is_sender_allowed(&sender) {
            return None;
        }

        if !self.matches_group(data_msg) {
            return None;
        }

        let target = self.reply_target(data_msg, &sender);

        let timestamp = data_msg
            .timestamp
            .or(envelope.timestamp)
            .unwrap_or_else(|| {
                u64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis(),
                )
                .unwrap_or(u64::MAX)
            });

        Some(ChannelMessage {
            id: format!("sig_{timestamp}"),
            sender: sender.clone(),
            reply_target: target,
            content: text.to_string(),
            channel: "signal".to_string(),
            timestamp: timestamp / 1000, // millis to secs.
            topic_id: None,
            reply_to_id: None,
            thread_ts: None,
            sender_name: envelope
                .source_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string),
        })
    }
}
