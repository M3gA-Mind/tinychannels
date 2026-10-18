//! Inbound dispatch for iMessage: typed messages with attachments and audio
//! messages, tapbacks as reactions, and read receipts for what we sent.

use super::IMessageChannel;
use super::db::{self, AttachmentRow, InboundRow, ReadRow};
use super::native::{IMESSAGE_MAX_FILE_BYTES, IMESSAGE_MAX_VOICE_BYTES};
use crate::providers::native::inbound::{
    InboundFile, Ingest, Ingested, assemble, channel_event, media_of, publish, publish_event,
};
use crate::traits::ChannelMessage;
use serde_json::json;
use std::path::{Path, PathBuf};
use tinychannels_bus::{
    ChannelEventKind, ChannelInboundEnvelope, ChannelRef, ConversationKind, ConversationRef,
    SenderRef,
};
use tokio::sync::mpsc;

fn imessage_ref() -> ChannelRef {
    ChannelRef {
        id: "imessage".into(),
        account_id: None,
    }
}

/// Messages.app marks an attachment's position in the text with U+FFFC.
fn clean_text(text: Option<&str>) -> String {
    text.unwrap_or_default()
        .replace('\u{FFFC}', "")
        .trim()
        .to_owned()
}

fn mime_from_name(name: &str) -> &'static str {
    match name
        .rsplit('.')
        .next()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("mp4" | "m4v") => "video/mp4",
        Some("mov") => "video/quicktime",
        Some("m4a") => "audio/mp4",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("caf") => "audio/x-caf",
        Some("pdf") => "application/pdf",
        Some("txt") => "text/plain",
        _ => "application/octet-stream",
    }
}

impl IMessageChannel {
    /// Where Messages keeps attachment files; nothing outside is ever read.
    fn attachments_root(&self) -> Option<PathBuf> {
        self.messages_db_path()?
            .parent()
            .map(|dir| dir.join("Attachments"))
    }

    fn resolve_attachment_path(&self, raw: &str) -> Option<PathBuf> {
        let expanded = match raw.strip_prefix("~/") {
            Some(rest) => directories::UserDirs::new()?.home_dir().join(rest),
            None => PathBuf::from(raw),
        };
        let root = self.attachments_root()?.canonicalize().ok()?;
        let resolved = expanded.canonicalize().ok()?;
        resolved.starts_with(&root).then_some(resolved)
    }

    /// Convert an Apple audio message (CAF) to AAC in an `.m4a` container with
    /// the platform converter, because the shared voice pipeline does not take
    /// CAF.
    async fn convert_audio(&self, input: &Path) -> Option<Vec<u8>> {
        let (program, leading) = self.converter_command.split_first()?;
        let out_dir = std::env::temp_dir()
            .join("tinychannels-imessage")
            .join(uuid::Uuid::new_v4().to_string());
        tokio::fs::create_dir_all(&out_dir).await.ok()?;
        let output = out_dir.join("voice.m4a");
        let status = tokio::process::Command::new(program)
            .args(leading)
            .args(["-f", "m4af", "-d", "aac"])
            .arg(input)
            .arg(&output)
            .output()
            .await;
        let bytes = match status {
            Ok(done) if done.status.success() => tokio::fs::read(&output).await.ok(),
            _ => {
                tracing::warn!("[imessage] audio message could not be converted");
                None
            }
        };
        let _ = tokio::fs::remove_dir_all(&out_dir).await;
        bytes
    }

    async fn file_of(&self, row: &InboundRow, attachment: &AttachmentRow) -> Option<InboundFile> {
        let path = self.resolve_attachment_path(attachment.path.as_deref()?)?;
        let name = attachment
            .name
            .clone()
            .or_else(|| path.file_name().map(|n| n.to_string_lossy().into_owned()));
        let mime = attachment
            .mime_type
            .clone()
            .filter(|m| !m.trim().is_empty())
            .unwrap_or_else(|| mime_from_name(name.as_deref().unwrap_or_default()).to_owned());
        let caf = mime == "audio/x-caf" || mime == "audio/caf";
        let limit = if row.audio_message {
            IMESSAGE_MAX_VOICE_BYTES
        } else {
            IMESSAGE_MAX_FILE_BYTES
        };
        // The database size is advisory; the file on disk is what counts.
        let on_disk = tokio::fs::metadata(&path).await.ok()?.len();
        if (attachment.bytes > 0 && attachment.bytes as u64 > limit * 8) || on_disk > limit * 8 {
            tracing::warn!("[imessage] skipping an oversized attachment");
            return None;
        }
        let (bytes, mime) = if caf {
            (self.convert_audio(&path).await?, "audio/mp4".to_owned())
        } else {
            (tokio::fs::read(&path).await.ok()?, mime)
        };
        Some(InboundFile {
            bytes,
            mime,
            name,
            duration_ms: None,
            voice: row.audio_message,
            dedupe_key: format!("imessage:{}:{}", row.guid, row.rowid),
            transcript: None,
        })
    }

    async fn ingest_attachments(
        &self,
        row: &InboundRow,
        message_id: &str,
        conversation: &ConversationRef,
    ) -> Ingested {
        let mut ingested = Ingested::default();
        if self.native.media_repository.is_none() && self.native.voice_media_store.is_none() {
            return ingested;
        }
        let ingest = Ingest {
            provider: "imessage",
            channel: imessage_ref(),
            repository: self.native.media_repository.as_ref(),
            voice_store: self.native.voice_media_store.as_ref(),
            transcriber: self.native.transcriber.as_ref(),
            file_bytes: IMESSAGE_MAX_FILE_BYTES,
            voice_bytes: IMESSAGE_MAX_VOICE_BYTES,
        };
        for attachment in &row.attachments {
            if let Some(file) = self.file_of(row, attachment).await {
                ingest
                    .add(&mut ingested, conversation, message_id, file)
                    .await;
            }
        }
        ingested
    }

    fn conversation_of(row: &InboundRow) -> ConversationRef {
        match &row.group_guid {
            Some(guid) => ConversationRef {
                kind: ConversationKind::Group,
                id: guid.clone(),
                ..Default::default()
            },
            None => ConversationRef {
                kind: ConversationKind::Dm,
                id: row.sender.clone(),
                ..Default::default()
            },
        }
    }

    async fn dispatch_tapback(&self, row: &InboundRow, emoji: &str, removed: bool) {
        let Some(target) = row.associated_guid.as_deref().map(db::target_guid) else {
            return;
        };
        let direction = if removed { "removed" } else { "added" };
        let event_id = format!("imessage:reaction:{direction}:{}:{}", target, row.guid);
        if !self.native.mark_seen(event_id.clone()) {
            return;
        }
        let actor = SenderRef {
            id: row.sender.clone(),
            ..Default::default()
        };
        let reaction = tinychannels_bus::channel::ChannelReaction {
            value: emoji.to_owned(),
            custom_emoji_id: None,
        };
        let kind = if removed {
            ChannelEventKind::ReactionRemoved {
                message_id: target.to_owned(),
                provider_message_id: Some(target.to_owned()),
                actor,
                reaction,
            }
        } else {
            ChannelEventKind::ReactionAdded {
                message_id: target.to_owned(),
                provider_message_id: Some(target.to_owned()),
                actor,
                reaction,
            }
        };
        publish_event(
            "imessage",
            self.native.events.as_ref(),
            channel_event(
                "imessage",
                event_id,
                db::unix_ms(row.date),
                Some(Self::conversation_of(row)),
                kind,
            ),
        )
        .await;
    }

    /// Handle one inbound row. False when the legacy receiver is gone and the
    /// listener should stop.
    pub(super) async fn dispatch_row(
        &self,
        row: InboundRow,
        tx: &mpsc::Sender<ChannelMessage>,
    ) -> bool {
        if !self.is_contact_allowed(&row.sender) {
            return true;
        }
        if let Some((emoji, removed)) = db::tapback(row.associated_type) {
            self.dispatch_tapback(&row, emoji, removed).await;
            return true;
        }
        let dedupe = if row.guid.is_empty() {
            row.rowid.to_string()
        } else {
            row.guid.clone()
        };
        if !self.native.mark_seen(format!("imessage:{dedupe}")) {
            return true;
        }
        let text = clean_text(row.text.as_deref());
        let conversation = Self::conversation_of(&row);
        let ingested = if row.attachments.is_empty() {
            Ingested::default()
        } else {
            self.ingest_attachments(&row, &dedupe, &conversation).await
        };
        let parts = assemble(&text, None, ingested);
        if parts.is_empty() {
            return true;
        }
        let envelope = ChannelInboundEnvelope {
            channel: imessage_ref(),
            message_id: dedupe,
            reply_to_id: None,
            timestamp_unix_ms: db::unix_ms(row.date),
            conversation: conversation.clone(),
            sender: SenderRef {
                id: row.sender.clone(),
                ..Default::default()
            },
            text: text.clone(),
            media: media_of(&parts),
            parts,
            raw: Some(json!({"providerMessageId": row.guid})),
            ..Default::default()
        };
        if publish("imessage", self.native.events.as_ref(), envelope).await || text.is_empty() {
            return true;
        }
        let message = ChannelMessage {
            id: row.rowid.to_string(),
            sender: row.sender.clone(),
            reply_target: conversation.id,
            content: text,
            channel: "imessage".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            topic_id: None,
            reply_to_id: None,
            thread_ts: None,
            sender_name: None,
        };
        tx.send(message).await.is_ok()
    }

    /// Publish a read receipt for one of our messages.
    pub(super) async fn dispatch_read(&self, row: ReadRow) {
        if !self.is_contact_allowed(&row.recipient) {
            return;
        }
        let event_id = format!("imessage:read:{}", row.guid);
        if !self.native.mark_seen(event_id.clone()) {
            return;
        }
        publish_event(
            "imessage",
            self.native.events.as_ref(),
            channel_event(
                "imessage",
                event_id,
                db::unix_ms(row.date_read),
                Some(ConversationRef {
                    kind: ConversationKind::Dm,
                    id: row.recipient.clone(),
                    ..Default::default()
                }),
                ChannelEventKind::ReadReceipt {
                    message_id: row.guid,
                    actor: Some(SenderRef {
                        id: row.recipient,
                        ..Default::default()
                    }),
                    timestamp: db::unix_ms(row.date_read),
                },
            ),
        )
        .await;
    }
}

#[cfg(test)]
#[path = "inbound_tests.rs"]
mod tests;
