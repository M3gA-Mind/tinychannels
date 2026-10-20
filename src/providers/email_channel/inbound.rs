//! Inbound email on the typed path: subject and thread headers, attachments
//! and voice attachments, published on the ordered ingress path, with the
//! legacy channel as the fallback for text.

use super::native::{EMAIL_MAX_FILE_BYTES, EMAIL_MAX_VOICE_BYTES, bare_id};
use super::{EmailChannel, ParsedEmail};
use crate::providers::native::inbound::{
    InboundFile, Ingest, Ingested, assemble, media_of, normalize_mime, publish,
};
use crate::traits::ChannelMessage;
use mail_parser::{HeaderValue, MessageParser, MimeHeaders};
use serde_json::json;
use tinychannels_bus::{
    ChannelInboundEnvelope, ChannelRef, ConversationKind, ConversationRef, SenderRef,
};
use tokio::sync::mpsc;

fn email_ref() -> ChannelRef {
    ChannelRef {
        id: "email".into(),
        account_id: None,
    }
}

/// Every message id a header names, bare.
fn ids_of(value: &HeaderValue<'_>) -> Vec<String> {
    match value {
        HeaderValue::Text(text) => vec![bare_id(text).to_owned()],
        HeaderValue::TextList(list) => list.iter().map(|id| bare_id(id).to_owned()).collect(),
        _ => Vec::new(),
    }
}

impl EmailChannel {
    /// Readable body text; empty when the mail only carries attachments.
    fn body_text(parsed: &mail_parser::Message<'_>) -> String {
        if let Some(text) = parsed.body_text(0) {
            return text.trim().to_owned();
        }
        parsed
            .body_html(0)
            .map(|html| Self::strip_html(html.as_ref()))
            .unwrap_or_default()
    }

    /// Hand one fetched, allowed, first-seen email to the host. False when the
    /// legacy receiver is gone and the listener should stop.
    pub(super) async fn dispatch_email(
        &self,
        email: ParsedEmail,
        tx: &mpsc::Sender<ChannelMessage>,
    ) -> bool {
        if self.native.events.is_some() && self.publish_typed(&email).await {
            return true;
        }
        let message = ChannelMessage {
            id: email.msg_id,
            reply_target: email.sender.clone(),
            sender: email.sender,
            content: email.content,
            channel: "email".to_string(),
            timestamp: email.timestamp,
            topic_id: None,
            reply_to_id: None,
            thread_ts: None,
            sender_name: None,
        };
        tx.send(message).await.is_ok()
    }

    /// Publish `email` through the compatibility event sink.
    async fn publish_typed(&self, email: &ParsedEmail) -> bool {
        let Some(envelope) = self.typed_envelope(email).await else {
            return false;
        };
        publish("email", self.native.events.as_ref(), envelope).await
    }

    /// Deliver an email through the ordered native ingress sink. Rejections
    /// are propagated to listener supervision and never fall back to the
    /// legacy channel queue.
    pub(super) async fn dispatch_typed_email(
        &self,
        email: &ParsedEmail,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> anyhow::Result<()> {
        let Some(envelope) = self.typed_envelope(email).await else {
            anyhow::bail!("Email message could not be parsed for typed ingress");
        };
        sink.push(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
            .map_err(|_| anyhow::Error::new(super::InboundSinkRejected))
    }

    /// Construct a typed envelope with host-scoped media and voice parts.
    async fn typed_envelope(&self, email: &ParsedEmail) -> Option<ChannelInboundEnvelope> {
        let Some(parsed) = MessageParser::default().parse(&email.raw) else {
            return None;
        };
        let subject = parsed.subject().unwrap_or("(no subject)").to_owned();
        let in_reply_to = ids_of(parsed.in_reply_to()).into_iter().next();
        let references = ids_of(parsed.references());
        // The thread is rooted at the oldest message it names, else at this one.
        let thread = references
            .first()
            .cloned()
            .or_else(|| in_reply_to.clone())
            .unwrap_or_else(|| email.msg_id.clone());
        self.subjects.remember(&email.msg_id, &subject);
        let conversation = ConversationRef {
            kind: ConversationKind::Dm,
            id: email.sender.clone(),
            thread_id: Some(thread),
            ..Default::default()
        };
        let mut ingested = Ingested::default();
        let ingest = Ingest {
            provider: "email",
            channel: email_ref(),
            repository: self.native.media_repository.as_ref(),
            voice_store: self.native.voice_media_store.as_ref(),
            transcriber: self.native.transcriber.as_ref(),
            file_bytes: EMAIL_MAX_FILE_BYTES,
            voice_bytes: EMAIL_MAX_VOICE_BYTES,
        };
        for (index, part) in parsed.attachments().enumerate() {
            let part: &mail_parser::MessagePart = part;
            let mime = MimeHeaders::content_type(part)
                .map(|ct| match ct.subtype() {
                    Some(subtype) => format!("{}/{subtype}", ct.ctype()),
                    None => ct.ctype().to_owned(),
                })
                .map(|mime| normalize_mime(&mime))
                .unwrap_or_else(|| "application/octet-stream".into());
            // Text parts are the body (or an inline quoted copy), not files.
            if mime.starts_with("text/") || mime.starts_with("multipart/") {
                continue;
            }
            ingest
                .add(
                    &mut ingested,
                    &conversation,
                    &email.msg_id,
                    InboundFile {
                        bytes: part.contents().to_vec(),
                        // Any audio attachment is treated as a voice note.
                        voice: mime.starts_with("audio/"),
                        mime,
                        name: MimeHeaders::attachment_name(part).map(str::to_owned),
                        duration_ms: None,
                        dedupe_key: format!("{}:{index}", email.msg_id),
                        transcript: None,
                    },
                )
                .await;
        }
        let body = Self::body_text(&parsed);
        let text = if body.is_empty() {
            format!("Subject: {subject}")
        } else {
            format!("Subject: {subject}\n\n{body}")
        };
        let parts = assemble(&text, None, ingested);
        let envelope = ChannelInboundEnvelope {
            channel: email_ref(),
            message_id: email.msg_id.clone(),
            reply_to_id: in_reply_to.clone(),
            timestamp_unix_ms: Some(email.timestamp.saturating_mul(1000)),
            conversation,
            sender: SenderRef {
                id: email.sender.clone(),
                ..Default::default()
            },
            text,
            media: media_of(&parts),
            parts,
            raw: Some(json!({
                "subject": subject,
                "messageId": email.msg_id,
                "inReplyTo": in_reply_to,
                "references": references,
            })),
            ..Default::default()
        };
        Some(envelope)
    }
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
