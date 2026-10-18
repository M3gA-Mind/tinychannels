//! Native typed media delivery for QQ Official Bot.

use super::QQChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{
    FileStep, Limits, NativeProvider, Sent, Step, Target, pre_dispatch, unsupported,
};
use async_trait::async_trait;
use base64::Engine as _;
use std::sync::Arc;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};
use tinychannels_bus::{ChannelCapabilities, ContentCapability, ConversationKind, MediaKind};

/// QQ's direct binary upload API is limited to 5 MiB. Larger files need the
/// multi-request chunk protocol, which this adapter does not yet implement.
const DIRECT_UPLOAD_LIMIT: u64 = 5 * 1024 * 1024;
const TEXT_LIMIT: usize = 2000;

#[async_trait]
impl NativeProvider for QQChannel {
    fn id(&self) -> &'static str {
        "qq"
    }

    fn display(&self) -> &'static str {
        "QQ"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: TEXT_LIMIT,
            file_bytes: DIRECT_UPLOAD_LIMIT,
            voice_bytes: DIRECT_UPLOAD_LIMIT,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.native.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.native.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        false
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        use crate::providers::long_tail::{LongTail, TextSpec, text_capabilities};
        let mut capabilities = text_capabilities(TextSpec::default());
        let Some(_) = self.native.media_repository else {
            return capabilities;
        };
        let media = |types: &[&str]| ContentCapability {
            outbound: true,
            accepted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
            emitted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
            max_size_bytes: Some(DIRECT_UPLOAD_LIMIT),
            ..Default::default()
        };
        capabilities
            .content
            .parts
            .extend(["image", "video", "file"].into_iter().map(str::to_owned));
        capabilities.content.image = media(&["image/jpeg", "image/png"]);
        capabilities.content.video = media(&["video/mp4"]);
        capabilities.content.file = media(&[
            "application/octet-stream",
            "application/pdf",
            "application/zip",
            "text/plain",
            "text/csv",
            "application/json",
        ]);
        if self.native.voice_media_store.is_some() {
            capabilities.content.parts.push("voice_note".into());
            capabilities.voice_out = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                DIRECT_UPLOAD_LIMIT,
                &["ogg_opus", "mp3", "wav_pcm16", "silk"],
            );
        }
        let _ = LongTail::declared(self);
        capabilities
    }

    fn normalized_target(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<Target, ChannelSendError> {
        let mut target =
            crate::providers::native::outbound::target_of(message, self.id(), self.display())?;
        if let Some((prefix, id)) = target.conversation.id.split_once(':') {
            match prefix {
                "group" => {
                    target.conversation.kind = ConversationKind::Group;
                    target.conversation.id = id.into();
                }
                "user" => {
                    target.conversation.kind = ConversationKind::Dm;
                    target.conversation.id = id.into();
                }
                _ => {}
            }
        }
        Ok(target)
    }

    fn validate_target(&self, target: &Target) -> Result<(), ChannelSendError> {
        if target.thread.is_some() || target.reply_to.is_some() {
            return Err(unsupported("QQ thread and reply addressing"));
        }
        if target.conversation.id.is_empty()
            || target
                .conversation
                .id
                .chars()
                .any(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_')))
        {
            return Err(pre_dispatch("QQ conversation identity is invalid"));
        }
        if !matches!(
            target.conversation.kind,
            ConversationKind::Dm | ConversationKind::Group
        ) {
            return Err(pre_dispatch(
                "QQ send target must identify a direct message or group",
            ));
        }
        Ok(())
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        match step {
            Step::Text(text) => {
                let response = self
                    .send_json(
                        target,
                        "messages",
                        serde_json::json!({
                            "content": text,
                            "msg_type": 0,
                        }),
                        true,
                    )
                    .await?;
                let id = response_id(&response)?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Text)])
            }
            Step::File(file) => {
                let file_type = file_type(file, false)?;
                let file_info = self.upload(target, file, file_type).await?;
                let response = self
                    .send_json(
                        target,
                        "messages",
                        serde_json::json!({
                            "media": {"file_info": file_info},
                            "msg_type": 7,
                        }),
                        true,
                    )
                    .await?;
                let id = response_id(&response)?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Media)])
            }
            Step::Voice(file) => {
                let file_type = file_type(file, true)?;
                let file_info = self.upload(target, file, file_type).await?;
                let response = self
                    .send_json(
                        target,
                        "messages",
                        serde_json::json!({
                            "media": {"file_info": file_info},
                            "msg_type": 7,
                        }),
                        true,
                    )
                    .await?;
                let id = response_id(&response)?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Voice)])
            }
        }
    }
}

impl QQChannel {
    fn route(&self, target: &Target, operation: &str) -> String {
        let (kind, id) = match target.conversation.kind {
            ConversationKind::Group => ("groups", target.conversation.id.as_str()),
            ConversationKind::Dm => ("users", target.conversation.id.as_str()),
            _ => unreachable!("QQ target was validated"),
        };
        format!("{QQ_API_BASE}/v2/{kind}/{id}/{operation}")
    }

    async fn upload(
        &self,
        target: &Target,
        file: &FileStep,
        file_type: u8,
    ) -> Result<String, ChannelSendError> {
        if file.bytes.len() as u64 > DIRECT_UPLOAD_LIMIT {
            return Err(pre_dispatch("QQ direct media upload exceeds 5 MiB"));
        }
        if file
            .caption
            .as_deref()
            .is_some_and(|caption| !caption.trim().is_empty())
        {
            return Err(unsupported("QQ rich media captions"));
        }
        let token = self
            .get_token()
            .await
            .map_err(|_| transient("QQ media authentication failed", false))?;
        let response = self
            .http_client()
            .post(self.route(target, "files"))
            .header("Authorization", format!("QQBot {token}"))
            .json(&serde_json::json!({
                "file_type": file_type,
                "file_name": file.name,
                "file_data": base64::engine::general_purpose::STANDARD.encode(&file.bytes),
                "srv_send_msg": false,
            }))
            .send()
            .await
            .map_err(|_| transient("QQ media upload request failed", false))?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_error(status, "media upload", false));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| transient("QQ media upload response was invalid", false))?;
        body.get("file_info")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| pre_dispatch("QQ media upload returned no file_info"))
    }

    async fn send_json(
        &self,
        target: &Target,
        operation: &str,
        body: serde_json::Value,
        final_send: bool,
    ) -> Result<serde_json::Value, ChannelSendError> {
        let token = self
            .get_token()
            .await
            .map_err(|_| transient("QQ message authentication failed", false))?;
        let response = self
            .http_client()
            .post(self.route(target, operation))
            .header("Authorization", format!("QQBot {token}"))
            .json(&body)
            .send()
            .await
            .map_err(|_| {
                if final_send {
                    unknown("QQ message request failed after submission")
                } else {
                    transient("QQ request failed", false)
                }
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_error(status, "message send", final_send));
        }
        response
            .json()
            .await
            .map_err(|_| unknown("QQ accepted a message without a readable receipt"))
    }
}

fn file_type(file: &FileStep, voice: bool) -> Result<u8, ChannelSendError> {
    let mime = file.mime.to_ascii_lowercase();
    let kind = if voice {
        if matches!(
            mime.as_str(),
            "audio/ogg" | "audio/mpeg" | "audio/wav" | "audio/silk"
        ) {
            3
        } else {
            return Err(unsupported(
                "QQ voice messages require OGG, MP3, WAV or Silk",
            ));
        }
    } else {
        match mime.as_str() {
            "image/png" | "image/jpeg" => 1,
            "video/mp4" => 2,
            _ => 4,
        }
    };
    if !voice && matches!(kind, 1 | 2) && file.bytes.len() as u64 > DIRECT_UPLOAD_LIMIT {
        return Err(pre_dispatch("QQ direct media upload exceeds 5 MiB"));
    }
    Ok(kind)
}

fn response_id(body: &serde_json::Value) -> Result<String, ChannelSendError> {
    body.get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| unknown("QQ accepted a message without an ID"))
}

fn status_error(
    status: reqwest::StatusCode,
    operation: &str,
    final_send: bool,
) -> ChannelSendError {
    if final_send && status.is_server_error() {
        unknown("QQ may have accepted the message but returned a server error")
    } else if status.is_server_error() {
        transient(&format!("QQ {operation} failed"), false)
    } else {
        ChannelSendError {
            kind: SendErrorKind::BadFormat,
            message: format!("QQ {operation} was rejected"),
            ..Default::default()
        }
    }
}

fn transient(message: &str, sent_before_error: bool) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Transient,
        message: message.into(),
        retryable: !sent_before_error,
        sent_before_error,
        ..Default::default()
    }
}

fn unknown(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Unknown,
        message: message.into(),
        sent_before_error: true,
        ..Default::default()
    }
}
