//! Native typed attachment delivery for Mattermost's v4 files and posts API.

use super::super::long_tail::LongTail;
use super::MattermostChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{Limits, NativeProvider, Sent, Step, Target};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};

const MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;

#[async_trait]
impl NativeProvider for MattermostChannel {
    fn id(&self) -> &'static str {
        "mattermost"
    }

    fn display(&self) -> &'static str {
        "Mattermost"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: 16_383,
            file_bytes: MAX_FILE_BYTES,
            voice_bytes: 0,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        false
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        match step {
            Step::Text(text) => {
                let id = self.create_post(target, text, Vec::new()).await?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Text)])
            }
            Step::File(file) => {
                let channel_id = channel_id(&target.conversation.id);
                let file_id = self.upload_file(channel_id, file).await?;
                let post_id = self
                    .create_post(
                        target,
                        file.caption.as_deref().unwrap_or_default(),
                        vec![file_id],
                    )
                    .await?;
                Ok(vec![Sent::new(post_id, MessageReceiptPartKind::Media)])
            }
            Step::Voice(_) => Err(crate::providers::native::unsupported(
                "Mattermost has file attachments but no verified voice-note send operation",
            )),
        }
    }

    async fn set_typing(&self, conversation: &str, on: bool) -> Result<(), ChannelSendError> {
        let result = if on {
            crate::traits::Channel::start_typing(self, conversation).await
        } else {
            crate::traits::Channel::stop_typing(self, conversation).await
        };
        result.map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "Mattermost typing indicator failed".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;

fn channel_id(conversation: &str) -> &str {
    conversation
        .split_once(':')
        .map_or(conversation, |(id, _)| id)
}

impl MattermostChannel {
    async fn upload_file(
        &self,
        channel_id: &str,
        file: &crate::providers::native::FileStep,
    ) -> Result<String, ChannelSendError> {
        let part = reqwest::multipart::Part::bytes(file.bytes.clone())
            .file_name(file.name.clone())
            .mime_str(&file.mime)
            .map_err(|_| crate::providers::native::pre_dispatch("invalid Mattermost media type"))?;
        let form = reqwest::multipart::Form::new()
            .text("channel_id", channel_id.to_owned())
            .part("files", part);
        let response = self
            .http_client()
            .post(format!("{}/api/v4/files", self.base_url))
            .bearer_auth(&self.bot_token)
            .multipart(form)
            .send()
            .await
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "Mattermost media upload did not complete".into(),
                retryable: true,
                sent_before_error: false,
                ..Default::default()
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(http_error(status, "media upload", false));
        }
        let body: serde_json::Value = response.json().await.map_err(|_| ChannelSendError {
            kind: SendErrorKind::Unknown,
            message: "Mattermost media upload returned an unreadable response".into(),
            sent_before_error: false,
            ..Default::default()
        })?;
        body.pointer("/file_infos/0/id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: "Mattermost media upload returned no file id".into(),
                sent_before_error: false,
                ..Default::default()
            })
    }

    async fn create_post(
        &self,
        target: &Target,
        message: &str,
        file_ids: Vec<String>,
    ) -> Result<String, ChannelSendError> {
        let mut body = serde_json::json!({
            "channel_id": channel_id(&target.conversation.id),
            "message": message,
        });
        if !file_ids.is_empty() {
            body["file_ids"] = serde_json::json!(file_ids);
        }
        if let Some(root_id) = target
            .thread
            .as_deref()
            .or_else(|| target.conversation.id.split_once(':').map(|(_, root)| root))
        {
            body["root_id"] = root_id.into();
        }
        let response = self
            .http_client()
            .post(format!("{}/api/v4/posts", self.base_url))
            .bearer_auth(&self.bot_token)
            .json(&body)
            .send()
            .await
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: "Mattermost post outcome is unknown".into(),
                sent_before_error: true,
                ..Default::default()
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(http_error(status, "post", true));
        }
        response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|post| post.get("id").and_then(|id| id.as_str()).map(str::to_owned))
            .filter(|id| !id.is_empty())
            .ok_or_else(|| ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: "Mattermost accepted the post without returning its id".into(),
                sent_before_error: true,
                ..Default::default()
            })
    }
}

fn http_error(status: reqwest::StatusCode, operation: &str, final_post: bool) -> ChannelSendError {
    let server_error = status.is_server_error();
    ChannelSendError {
        kind: if server_error {
            SendErrorKind::Transient
        } else {
            SendErrorKind::BadFormat
        },
        message: format!("Mattermost {operation} failed with HTTP {status}"),
        retryable: server_error,
        sent_before_error: final_post && server_error,
        ..Default::default()
    }
}
