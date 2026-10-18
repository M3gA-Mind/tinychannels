//! The live-session operations the WhatsApp Web adapter performs.
//!
//! [`OutboundClient`] is the seam between adapter policy (allow-lists, scope,
//! planning, receipts) and the `whatsapp-rust` session. Implemented by
//! [`whatsapp_rust::Client`]; tests substitute a recording double so the
//! adapter logic runs without a connected device.

use super::media_build::{self, MediaBuild, UploadedMedia};
use anyhow::Result;
use async_trait::async_trait;
use wacore::download::{Downloadable, MediaType};
use whatsapp_rust::Jid;
use whatsapp_rust::waproto::whatsapp as wa;

#[async_trait]
pub(super) trait OutboundClient: Send + Sync {
    /// Send `message` to `to` and return the provider's message id.
    async fn send_message(&self, to: Jid, message: wa::Message) -> Result<String>;
    /// Encrypt and upload `bytes`, returning the ready-to-send media message.
    async fn media_message(
        &self,
        bytes: Vec<u8>,
        build: MediaBuild,
        context: Option<wa::ContextInfo>,
    ) -> Result<wa::Message>;
    /// Download and decrypt one inbound attachment.
    async fn download(&self, media: &dyn Downloadable) -> Result<Vec<u8>>;
    /// React to `key` in `chat`; an empty `emoji` removes the reaction.
    async fn send_reaction(&self, chat: Jid, key: wa::MessageKey, emoji: &str) -> Result<String>;
    /// Replace the text of one of our messages.
    async fn edit_message(
        &self,
        chat: Jid,
        original_id: String,
        content: wa::Message,
    ) -> Result<String>;
    /// Unsend one of our messages for everyone.
    async fn revoke_message(&self, chat: Jid, message_id: String) -> Result<()>;
    /// Send read receipts for `ids` in `chat`.
    async fn mark_read(&self, chat: &Jid, sender: Option<&Jid>, ids: &[String]) -> Result<()>;
    /// Publish the "composing" chat state to `to`.
    async fn send_composing(&self, to: &Jid) -> Result<()>;
    /// Publish the "paused" chat state to `to`.
    async fn send_paused(&self, to: &Jid) -> Result<()>;
    /// The phone-number JID this session is linked to, once it is known.
    fn linked_account(&self) -> Option<Jid>;
}

#[async_trait]
impl OutboundClient for whatsapp_rust::Client {
    async fn send_message(&self, to: Jid, message: wa::Message) -> Result<String> {
        Ok(whatsapp_rust::Client::send_message(self, to, message)
            .await?
            .message_id)
    }

    async fn media_message(
        &self,
        bytes: Vec<u8>,
        build: MediaBuild,
        context: Option<wa::ContextInfo>,
    ) -> Result<wa::Message> {
        let kind = match &build {
            MediaBuild::Image { .. } => MediaType::Image,
            MediaBuild::Video { .. } => MediaType::Video,
            MediaBuild::Audio { .. } | MediaBuild::Voice { .. } => MediaType::Audio,
            MediaBuild::Document { .. } => MediaType::Document,
            MediaBuild::Sticker => MediaType::Sticker,
        };
        let upload = self
            .upload(bytes, kind, whatsapp_rust::UploadOptions::new())
            .await?;
        Ok(media_build::build(
            UploadedMedia {
                url: upload.url,
                direct_path: upload.direct_path,
                media_key: upload.media_key.to_vec(),
                file_sha256: upload.file_sha256.to_vec(),
                file_enc_sha256: upload.file_enc_sha256.to_vec(),
                file_length: upload.file_length,
                media_key_timestamp: upload.media_key_timestamp,
                streaming_sidecar: upload.streaming_sidecar,
            },
            build,
            context,
        ))
    }

    async fn download(&self, media: &dyn Downloadable) -> Result<Vec<u8>> {
        whatsapp_rust::Client::download(self, media).await
    }

    async fn send_reaction(&self, chat: Jid, key: wa::MessageKey, emoji: &str) -> Result<String> {
        Ok(whatsapp_rust::Client::send_reaction(self, chat, key, emoji)
            .await?
            .message_id)
    }

    async fn edit_message(
        &self,
        chat: Jid,
        original_id: String,
        content: wa::Message,
    ) -> Result<String> {
        Ok(whatsapp_rust::Client::edit_message(self, chat, original_id, content).await?)
    }

    async fn revoke_message(&self, chat: Jid, message_id: String) -> Result<()> {
        Ok(whatsapp_rust::Client::revoke_message(
            self,
            chat,
            message_id,
            whatsapp_rust::RevokeType::Sender,
        )
        .await?)
    }

    async fn mark_read(&self, chat: &Jid, sender: Option<&Jid>, ids: &[String]) -> Result<()> {
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        whatsapp_rust::Client::mark_as_read(self, chat, sender, &ids).await
    }

    async fn send_composing(&self, to: &Jid) -> Result<()> {
        Ok(self.chatstate().send_composing(to).await?)
    }

    async fn send_paused(&self, to: &Jid) -> Result<()> {
        Ok(self.chatstate().send_paused(to).await?)
    }

    fn linked_account(&self) -> Option<Jid> {
        self.pn()
    }
}
