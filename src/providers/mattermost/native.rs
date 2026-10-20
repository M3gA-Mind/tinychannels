//! Native typed attachment delivery for Mattermost's v4 files and posts API.

use super::super::long_tail::LongTail;
use super::{MAX_FILE_BYTES, MAX_VOICE_BYTES, MattermostChannel};
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{Address, Limits, NativeProvider, Sent, Step, Target};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};

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
            voice_bytes: MAX_VOICE_BYTES,
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

    fn can_edit(&self) -> bool {
        true
    }

    fn can_delete(&self) -> bool {
        true
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    fn normalized_target(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<Target, ChannelSendError> {
        let mut target =
            crate::providers::native::outbound::target_of(message, self.id(), self.display())?;
        if let Some((_, packed_root)) = target.conversation.id.split_once(':') {
            if message
                .conversation_ref
                .as_ref()
                .and_then(|conversation| conversation.thread_id.as_deref())
                .is_some_and(|thread| thread != packed_root)
            {
                return Err(crate::providers::native::pre_dispatch(
                    "Mattermost packed conversation and canonical thread addresses conflict",
                ));
            }
            // Mattermost's deployed legacy address helper preserves an
            // already-packed channel:root recipient instead of appending or
            // replacing its root from the compatibility thread hint.
            target.thread = Some(packed_root.to_owned());
            target.conversation.thread_id = Some(packed_root.to_owned());
        }
        Ok(target)
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        if self.voice_media_store.is_some() {
            vec![("voiceOut", "attachment")]
        } else {
            Vec::new()
        }
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        match step {
            Step::Text(text) => {
                let id = self.create_post(target, text, Vec::new()).await?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Text)])
            }
            Step::File(file) | Step::Voice(file) => {
                let channel_id = channel_id(&target.conversation.id);
                let file_id = self.upload_file(channel_id, file).await?;
                let post_id = self
                    .create_post(
                        target,
                        file.caption.as_deref().unwrap_or_default(),
                        vec![file_id],
                    )
                    .await?;
                let kind = if matches!(step, Step::Voice(_)) {
                    MessageReceiptPartKind::Voice
                } else {
                    MessageReceiptPartKind::Media
                };
                Ok(vec![Sent::new(post_id, kind)])
            }
        }
    }

    async fn set_typing(&self, conversation: &str, on: bool) -> Result<(), ChannelSendError> {
        let result = if on {
            self.start_typing_loop(conversation).await
        } else {
            self.stop_typing_loop()
        };
        result.map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "Mattermost typing indicator failed".into(),
            retryable: true,
            ..Default::default()
        })
    }

    async fn react(
        &self,
        _conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        if provider_message_id.trim().is_empty() {
            return Err(crate::providers::native::pre_dispatch(
                "Mattermost reaction requires a post ID",
            ));
        }
        if reaction.custom_emoji_id.is_some() {
            return Err(crate::providers::native::pre_dispatch(
                "Mattermost reactions require an emoji name",
            ));
        }
        let emoji_name = mattermost_emoji_name(&reaction.value).ok_or_else(|| {
            crate::providers::native::pre_dispatch("invalid Mattermost emoji name")
        })?;
        let user_id = self.bot_user_id().await?;
        let url = if remove {
            format!(
                "{}/api/v4/users/{}/posts/{}/reactions/{}",
                self.base_url,
                urlencoding::encode(&user_id),
                urlencoding::encode(provider_message_id),
                urlencoding::encode(&emoji_name),
            )
        } else {
            format!("{}/api/v4/reactions", self.base_url)
        };
        let request = self.http_client();
        let response = if remove {
            request
                .delete(url)
                .bearer_auth(&self.bot_token)
                .send()
                .await
        } else {
            request
                .post(url)
                .bearer_auth(&self.bot_token)
                .json(&serde_json::json!({
                    "user_id": user_id,
                    "post_id": provider_message_id,
                    "emoji_name": emoji_name,
                }))
                .send()
                .await
        }
        .map_err(|_| mattermost_request_error("reaction", true))?;
        if !response.status().is_success() {
            return Err(mattermost_status_error(&response, "reaction", true));
        }
        Ok(())
    }

    async fn edit_text(&self, address: &Address, text: &str) -> Result<(), ChannelSendError> {
        let response = self
            .http_client()
            .put(format!(
                "{}/api/v4/posts/{}",
                self.base_url,
                urlencoding::encode(&address.message_id)
            ))
            .bearer_auth(&self.bot_token)
            .json(&serde_json::json!({
                "id": address.message_id,
                "message": text,
            }))
            .send()
            .await
            .map_err(|_| mattermost_request_error("post edit", true))?;
        if !response.status().is_success() {
            return Err(mattermost_status_error(&response, "post edit", true));
        }
        Ok(())
    }

    async fn delete_message(&self, address: &Address) -> Result<(), ChannelSendError> {
        let response = self
            .http_client()
            .delete(format!(
                "{}/api/v4/posts/{}",
                self.base_url,
                urlencoding::encode(&address.message_id)
            ))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .map_err(|_| mattermost_request_error("post delete", true))?;
        if !response.status().is_success() {
            return Err(mattermost_status_error(&response, "post delete", true));
        }
        Ok(())
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
    async fn bot_user_id(&self) -> Result<String, ChannelSendError> {
        let response = self
            .http_client()
            .get(format!("{}/api/v4/users/me", self.base_url))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .map_err(|_| mattermost_request_error("identity lookup", false))?;
        if !response.status().is_success() {
            return Err(mattermost_status_error(&response, "identity lookup", false));
        }
        let identity: serde_json::Value = response.json().await.map_err(|_| ChannelSendError {
            kind: SendErrorKind::Unknown,
            message: "Mattermost identity response was unreadable".into(),
            sent_before_error: false,
            ..Default::default()
        })?;
        identity
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: "Mattermost identity response did not include a user ID".into(),
                sent_before_error: false,
                ..Default::default()
            })
    }

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

fn mattermost_emoji_name(value: &str) -> Option<String> {
    let value = value.trim();
    let known = match value {
        "👍" => Some("+1"),
        "👎" => Some("-1"),
        "❤️" | "❤" => Some("heart"),
        "✅" => Some("white_check_mark"),
        "🎉" => Some("tada"),
        "👀" => Some("eyes"),
        "😂" => Some("joy"),
        "🔥" => Some("fire"),
        _ => None,
    };
    let name = known.unwrap_or_else(|| value.trim_matches(':'));
    (!name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '+')
        }))
    .then(|| name.to_owned())
}

fn mattermost_request_error(operation: &str, may_have_been_applied: bool) -> ChannelSendError {
    ChannelSendError {
        kind: if may_have_been_applied {
            SendErrorKind::Unknown
        } else {
            SendErrorKind::Transient
        },
        message: format!("Mattermost {operation} request did not complete"),
        retryable: !may_have_been_applied,
        sent_before_error: may_have_been_applied,
        ..Default::default()
    }
}

fn mattermost_status_error(
    response: &reqwest::Response,
    operation: &str,
    may_have_been_applied: bool,
) -> ChannelSendError {
    let status = response.status();
    let rate_limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS;
    let server_error = status.is_server_error();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<f64>().ok());
    ChannelSendError {
        kind: if status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
        {
            SendErrorKind::Forbidden
        } else if rate_limited {
            SendErrorKind::RateLimited
        } else if server_error {
            if may_have_been_applied {
                SendErrorKind::Unknown
            } else {
                SendErrorKind::Transient
            }
        } else {
            SendErrorKind::BadFormat
        },
        message: format!("Mattermost {operation} failed with HTTP {status}"),
        retryable: rate_limited || (server_error && !may_have_been_applied),
        sent_before_error: server_error && may_have_been_applied,
        retry_after: if rate_limited { retry_after } else { None },
        ..Default::default()
    }
}

fn http_error(status: reqwest::StatusCode, operation: &str, final_post: bool) -> ChannelSendError {
    let server_error = status.is_server_error();
    ChannelSendError {
        kind: if final_post && server_error {
            SendErrorKind::Unknown
        } else if server_error {
            SendErrorKind::Transient
        } else {
            SendErrorKind::BadFormat
        },
        message: if final_post && server_error {
            "Mattermost post failed after submission; delivery state is unknown".into()
        } else {
            format!("Mattermost {operation} failed with HTTP {status}")
        },
        retryable: server_error && !final_post,
        sent_before_error: final_post && server_error,
        ..Default::default()
    }
}
