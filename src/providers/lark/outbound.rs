//! Lark outbound delivery and message receipt handling.

use super::*;
use super::{FILE_LIMIT, IMAGE_LIMIT};
use crate::providers::native::{Sent, Step, Target};
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind};

pub(super) async fn deliver(
    channel: &LarkChannel,
    target: &Target,
    step: &Step,
) -> Result<Vec<Sent>, ChannelSendError> {
    match step {
        Step::Text(text) => {
            let id = channel
                .send_content(
                    &target.conversation.id,
                    "text",
                    serde_json::json!({"text": text}),
                )
                .await?;
            Ok(vec![Sent::new(id, MessageReceiptPartKind::Text)])
        }
        Step::File(file) if file.kind == tinychannels_bus::MediaKind::Image => {
            if file.bytes.len() as u64 > IMAGE_LIMIT {
                return Err(known_unsent("Lark image exceeds the 10 MiB limit"));
            }
            let key = channel.upload_image(file).await?;
            let (message_type, content) = if let Some(caption) = file
                .caption
                .as_deref()
                .filter(|caption| !caption.trim().is_empty())
            {
                (
                    "post",
                    serde_json::json!({"zh_cn": {"title": "", "content": [[{"tag": "text", "text": caption}], [{"tag": "img", "image_key": key}]]}}),
                )
            } else {
                ("image", serde_json::json!({"image_key": key}))
            };
            let id = channel
                .send_content(&target.conversation.id, message_type, content)
                .await?;
            Ok(vec![Sent::new(id, MessageReceiptPartKind::Media)])
        }
        Step::File(file) | Step::Voice(file) => {
            if file
                .caption
                .as_deref()
                .is_some_and(|caption| !caption.trim().is_empty())
            {
                return Err(crate::providers::native::unsupported(
                    "Lark file/audio captions",
                ));
            }
            if file.bytes.len() as u64 > FILE_LIMIT {
                return Err(known_unsent("Lark file exceeds the 30 MiB limit"));
            }
            let voice = matches!(step, Step::Voice(_));
            let audio = file.kind == tinychannels_bus::MediaKind::Audio;
            let video = file.kind == tinychannels_bus::MediaKind::Video;
            let (bytes, name, file_type, message_type) = if voice || audio {
                let (bytes, name) = channel.opus_audio(file).await?;
                (bytes, name, "opus", "audio")
            } else if video && file.mime == "video/mp4" {
                (file.bytes.clone(), file.name.clone(), "mp4", "media")
            } else {
                (
                    file.bytes.clone(),
                    file.name.clone(),
                    super::media::file_type_for_mime(&file.mime),
                    "file",
                )
            };
            let key = channel.upload_file(&bytes, &name, file_type, None).await?;
            let id = channel
                .send_content(
                    &target.conversation.id,
                    message_type,
                    serde_json::json!({"file_key": key}),
                )
                .await?;
            Ok(vec![Sent::new(
                id,
                if voice {
                    MessageReceiptPartKind::Voice
                } else {
                    MessageReceiptPartKind::Media
                },
            )])
        }
    }
}

impl LarkChannel {
    pub(super) async fn send_content(
        &self,
        recipient: &str,
        message_type: &str,
        content: serde_json::Value,
    ) -> Result<String, ChannelSendError> {
        let mut token = self
            .get_tenant_access_token()
            .await
            .map_err(|_| known_unsent("Lark authentication failed before message delivery"))?;
        for attempt in 0..=1 {
            let response = self
                .http_client()
                .post(self.send_message_url())
                .bearer_auth(&token)
                .json(&serde_json::json!({
                    "receive_id": recipient,
                    "msg_type": message_type,
                    "content": content.to_string(),
                }))
                .send()
                .await
                .map_err(|_| uncertain_send("Lark message request failed"))?;
            let status = response.status();
            let body = response.json::<serde_json::Value>().await.ok();
            let token_expired = status == reqwest::StatusCode::UNAUTHORIZED
                || body
                    .as_ref()
                    .and_then(|body| body.get("code"))
                    .and_then(serde_json::Value::as_i64)
                    == Some(99991663);
            if token_expired && attempt == 0 {
                self.invalidate_token_if_current(&token).await;
                token = self
                    .get_tenant_access_token()
                    .await
                    .map_err(|_| known_unsent("Lark authentication failed before message retry"))?;
                continue;
            }
            if token_expired {
                return Err(known_unsent("Lark rejected the message authentication"));
            }
            let body = body.ok_or_else(|| uncertain_send("Lark message response was invalid"))?;
            if !status.is_success()
                || body.get("code").and_then(serde_json::Value::as_i64) != Some(0)
            {
                return Err(if status.is_client_error() {
                    known_unsent("Lark rejected the message")
                } else {
                    uncertain_send("Lark message delivery outcome is unknown")
                });
            }
            let message_id = body
                .pointer("/data/message_id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    uncertain_send("Lark accepted a message without returning its id")
                })?;
            return Ok(message_id);
        }
        Err(uncertain_send("Lark message delivery outcome is unknown"))
    }
}

pub(super) fn known_unsent(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Transient,
        message: message.into(),
        retryable: true,
        sent_before_error: false,
        ..Default::default()
    }
}

pub(super) fn uncertain_send(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
        message: message.into(),
        sent_before_error: true,
        ..Default::default()
    }
}
