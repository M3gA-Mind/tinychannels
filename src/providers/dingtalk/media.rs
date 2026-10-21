//! DingTalk App Bot upload, download, and media validation.

use super::DingTalkChannel;
use super::native::{
    DINGTALK_API, DINGTALK_UPLOAD, DownloadResponse, MAX_MEDIA_BYTES, TokenResponse,
    UploadResponse, known_unsent, transient_unsent, unsent_http_error,
};
use crate::providers::native::{FileStep, pre_dispatch, unsupported};
use tinychannels_bus::channel::ChannelSendError;

pub(super) fn validate_file(file: &FileStep) -> Result<(), ChannelSendError> {
    if file.bytes.is_empty() || file.bytes.len() as u64 > MAX_MEDIA_BYTES {
        return Err(pre_dispatch(
            "DingTalk file exceeds the 10 MiB upload limit",
        ));
    }
    let extension = file_type(&file.name);
    if !matches!(
        extension.as_str(),
        "pdf" | "zip" | "rar" | "doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx"
    ) {
        return Err(unsupported("DingTalk App Bot file type is not supported"));
    }
    Ok(())
}

pub(super) fn file_type(name: &str) -> String {
    name.rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default()
}

impl DingTalkChannel {
    pub(super) async fn access_token(&self) -> Result<String, ChannelSendError> {
        let response = self
            .http_client()
            .post(format!("{DINGTALK_API}/v1.0/oauth2/accessToken"))
            .json(&serde_json::json!({
                "appKey": self.client_id,
                "appSecret": self.client_secret,
            }))
            .send()
            .await
            .map_err(|_| transient_unsent("DingTalk access token request failed"))?;
        if !response.status().is_success() {
            return Err(unsent_http_error(response.status(), "access token request"));
        }
        let token: TokenResponse = response
            .json()
            .await
            .map_err(|_| known_unsent("DingTalk access token response was invalid"))?;
        if token.access_token.is_empty() {
            return Err(known_unsent("DingTalk access token response was empty"));
        }
        Ok(token.access_token)
    }

    pub(super) async fn upload_file(
        &self,
        token: &str,
        file: &FileStep,
    ) -> Result<String, ChannelSendError> {
        self.upload_media(token, file, "file").await
    }

    pub(super) async fn upload_voice(
        &self,
        token: &str,
        voice: &FileStep,
    ) -> Result<String, ChannelSendError> {
        self.upload_media(token, voice, "voice").await
    }

    async fn upload_media(
        &self,
        token: &str,
        file: &FileStep,
        media_type: &str,
    ) -> Result<String, ChannelSendError> {
        let part = reqwest::multipart::Part::bytes(file.bytes.clone())
            .file_name(file.name.clone())
            .mime_str(&file.mime)
            .map_err(|_| pre_dispatch("DingTalk file MIME type is invalid"))?;
        let form = reqwest::multipart::Form::new().part("media", part);
        let response = self
            .http_client()
            .post(format!(
                "{DINGTALK_UPLOAD}?access_token={token}&type={media_type}"
            ))
            .multipart(form)
            .send()
            .await
            .map_err(|_| transient_unsent("DingTalk media upload request failed"))?;
        if !response.status().is_success() {
            return Err(unsent_http_error(response.status(), "media upload"));
        }
        let response: UploadResponse = response
            .json()
            .await
            .map_err(|_| known_unsent("DingTalk media upload response was invalid"))?;
        if response.media_id.is_empty() {
            return Err(known_unsent("DingTalk media upload returned no media_id"));
        }
        Ok(response.media_id)
    }

    pub(super) async fn download_media(
        &self,
        download_code: &str,
        robot_code: &str,
        max_bytes: u64,
    ) -> Option<(Vec<u8>, String)> {
        let token = self.access_token().await.ok()?;
        let response = self
            .http_client()
            .post(format!("{DINGTALK_API}/v1.0/robot/messageFiles/download"))
            .header("x-acs-dingtalk-access-token", token)
            .json(&serde_json::json!({"robotCode": robot_code, "downloadCode": download_code}))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let download: DownloadResponse = response.json().await.ok()?;
        let url = reqwest::Url::parse(&download.download_url).ok()?;
        if url.scheme() != "https"
            || url.host_str().is_none_or(str::is_empty)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some_and(|port| port != 443)
        {
            return None;
        }
        let mut response = self.attachment_http_client.get(url).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        if response
            .content_length()
            .is_some_and(|size| size > max_bytes)
        {
            return None;
        }
        let mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        (!bytes.is_empty()).then_some((bytes, mime))
    }
}
