//! Native DingTalk media ingress and typed App Bot sends.

#[cfg(test)]
use super::DingTalkChannel;
#[cfg(test)]
use crate::providers::native::{FileStep, Step, Target};
use serde::Deserialize;
#[cfg(test)]
use tinychannels_bus::channel::MessageReceiptPartKind;
use tinychannels_bus::channel::{ChannelSendError, ConversationKind, SendErrorKind};

pub(super) const MAX_MEDIA_BYTES: u64 = 10 * 1024 * 1024;
pub(super) const MAX_VOICE_BYTES: u64 = 2 * 1024 * 1024;
pub(super) const DINGTALK_API: &str = "https://api.dingtalk.com";
pub(super) const DINGTALK_UPLOAD: &str = "https://oapi.dingtalk.com/media/upload";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TokenResponse {
    pub(super) access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DownloadResponse {
    pub(super) download_url: String,
}

#[derive(Deserialize)]
pub(super) struct UploadResponse {
    pub(super) media_id: String,
}

pub(super) fn known_unsent(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        ..Default::default()
    }
}

pub(super) fn transient_unsent(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Transient,
        message: message.into(),
        retryable: true,
        sent_before_error: false,
        ..Default::default()
    }
}

pub(super) fn unsent_http_error(status: reqwest::StatusCode, operation: &str) -> ChannelSendError {
    let message = format!("DingTalk {operation} failed with HTTP {status}");
    if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        transient_unsent(message)
    } else {
        known_unsent(message)
    }
}

pub(super) fn unknown_after_send(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Unknown,
        message: message.into(),
        sent_before_error: true,
        ..Default::default()
    }
}

pub(super) fn response_lists_user(
    response: &serde_json::Value,
    field: &str,
    user_id: &str,
) -> bool {
    response
        .get(field)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(user_id)))
}

pub(super) fn conversation_context_key(kind: ConversationKind, id: &str) -> String {
    format!(
        "{}:{id}",
        if kind == ConversationKind::Group {
            "group"
        } else {
            "dm"
        }
    )
}

pub(super) use super::media::{file_type, validate_file};

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
