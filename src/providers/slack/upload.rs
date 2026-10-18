//! Slack's external file upload flow (`files.getUploadURLExternal`, a raw
//! upload, then `files.completeUploadExternal`).

use super::api::send_error;
use super::types::SlackChannel;
use serde_json::{Value, json};
use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

pub(crate) struct Upload<'a> {
    pub(crate) channel: &'a str,
    pub(crate) thread_ts: Option<&'a str>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) file_name: &'a str,
    pub(crate) content_type: &'a str,
    pub(crate) comment: Option<&'a str>,
}

/// The shared file and, when Slack reports it, the message that carries it.
#[derive(Debug)]
pub(crate) struct Uploaded {
    pub(crate) file_id: String,
    pub(crate) message_ts: Option<String>,
}

fn not_sent(mut error: ChannelSendError) -> ChannelSendError {
    error.sent_before_error = false;
    error.retryable = error.retryable || error.kind == SendErrorKind::Transient;
    error
}

impl SlackChannel {
    pub(crate) async fn upload_file(
        &self,
        upload: Upload<'_>,
    ) -> Result<Uploaded, ChannelSendError> {
        let length = upload.bytes.len().to_string();
        let started = self
            .api_form(
                "files.getUploadURLExternal",
                &[("filename", upload.file_name), ("length", &length)],
            )
            .await
            .map_err(|failure| not_sent(send_error(&failure, "file upload start")))?;
        let (Some(url), Some(file_id)) = (
            started.get("upload_url").and_then(Value::as_str),
            started.get("file_id").and_then(Value::as_str),
        ) else {
            return Err(ChannelSendError {
                message: "Slack file upload start returned no upload target".into(),
                ..Default::default()
            });
        };
        let sent = self
            .http_client()
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, upload.content_type)
            .body(upload.bytes)
            .send()
            .await;
        if !sent.is_ok_and(|response| response.status().is_success()) {
            return Err(ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "Slack file bytes were not accepted".into(),
                retryable: true,
                ..Default::default()
            });
        }
        let mut body = json!({
            "files": [{"id": file_id, "title": upload.file_name}],
            "channel_id": upload.channel,
        });
        if let Some(thread) = upload.thread_ts {
            body["thread_ts"] = thread.into();
        }
        if let Some(comment) = upload.comment.filter(|comment| !comment.trim().is_empty()) {
            body["initial_comment"] = comment.into();
        }
        let done = self
            .api_json("files.completeUploadExternal", &body)
            .await
            .map_err(|failure| send_error(&failure, "file upload"))?;
        let file = done.pointer("/files/0");
        let id = file
            .and_then(|file| file.get("id"))
            .and_then(Value::as_str)
            .unwrap_or(file_id)
            .to_owned();
        let message_ts = file
            .and_then(|file| file.get("shares"))
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|shares| shares.values())
            .filter_map(|visibility| visibility.get(upload.channel))
            .filter_map(|shares| shares.pointer("/0/ts"))
            .find_map(Value::as_str)
            .map(str::to_owned);
        Ok(Uploaded {
            file_id: id,
            message_ts,
        })
    }
}

#[cfg(test)]
#[path = "upload_tests.rs"]
mod tests;
