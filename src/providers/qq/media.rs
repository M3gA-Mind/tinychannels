//! QQ attachment download and media format classification.

use super::QQChannel;
use crate::providers::native::FileStep;
use crate::providers::native::{pre_dispatch, unsupported};
use tinychannels_bus::channel::ChannelSendError;

const DIRECT_UPLOAD_LIMIT: u64 = 5 * 1024 * 1024;

impl QQChannel {
    pub(super) async fn download_attachment(
        &self,
        raw_url: &str,
        max_bytes: u64,
    ) -> Option<(Vec<u8>, String)> {
        let url = reqwest::Url::parse(raw_url).ok()?;
        let host = url.host_str()?.to_ascii_lowercase();
        let tencent_host = ["qq.com", "qq.com.cn"]
            .iter()
            .any(|domain| host == *domain || host.ends_with(&format!(".{domain}")));
        if url.scheme() != "https"
            || !tencent_host
            || url.port().is_some_and(|port| port != 443)
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return None;
        }
        let response = self.attachment_http_client().get(url).send().await.ok()?;
        if !response.status().is_success() || response.url().host_str() != Some(host.as_str()) {
            return None;
        }
        if response
            .content_length()
            .is_some_and(|length| length > max_bytes)
        {
            return None;
        }
        let mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let mut response = response;
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

pub(super) fn file_type(file: &FileStep, voice: bool) -> Result<u8, ChannelSendError> {
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
