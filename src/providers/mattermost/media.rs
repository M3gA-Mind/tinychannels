//! Mattermost file upload and download operations.

use super::inbound::valid_file_id;
use super::outbound::http_error;
use super::*;
use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

impl MattermostChannel {
    pub(super) async fn upload_file(
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

    pub(super) async fn file_info(
        &self,
        post: &serde_json::Value,
        file_id: &str,
    ) -> Option<(String, Option<String>, Option<u64>)> {
        if let Some(file) = post
            .pointer("/metadata/files")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|file| file.get("id").and_then(serde_json::Value::as_str) == Some(file_id))
        {
            return Some((
                file.get("mime_type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("application/octet-stream")
                    .to_owned(),
                file.get("name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                file.get("size").and_then(serde_json::Value::as_u64),
            ));
        }
        if !valid_file_id(file_id) {
            return None;
        }
        let info = self
            .http_client()
            .get(format!("{}/api/v4/files/{file_id}/info", self.base_url))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?;
        if !info.status().is_success() {
            return None;
        }
        let info: serde_json::Value = info.json().await.ok()?;
        Some((
            info.get("mime_type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("application/octet-stream")
                .to_owned(),
            info.get("name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            info.get("size").and_then(serde_json::Value::as_u64),
        ))
    }

    pub(super) async fn download_file(&self, file_id: &str, limit: u64) -> Option<Vec<u8>> {
        if !valid_file_id(file_id) {
            return None;
        }
        let mut response = self
            .http_client()
            .get(format!("{}/api/v4/files/{file_id}", self.base_url))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?;
        if !response.status().is_success()
            || response.content_length().is_some_and(|size| size > limit)
        {
            return None;
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if bytes.len() as u64 + chunk.len() as u64 > limit {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        (!bytes.is_empty()).then_some(bytes)
    }
}
