//! Graph API transport for the WhatsApp Business Cloud adapter.
//!
//! Every call carries the access token as a bearer header and nothing else:
//! the token is never placed in a URL, a log line or an error message.
//! Failures are classified by whether the account could have been touched.

use super::WhatsAppChannel;
use crate::providers::whatsapp_shared::error::WaFailure;
use crate::providers::whatsapp_shared::inbound::DownloadedAttachment;
use serde_json::Value;

/// Graph API version every request names.
pub(super) const GRAPH_VERSION: &str = "v23.0";
/// Environment override for the Graph API origin (tests and regional proxies).
pub(super) const GRAPH_BASE_ENV: &str = "TINYCHANNELS_WHATSAPP_GRAPH_BASE";
const DEFAULT_GRAPH_BASE: &str = "https://graph.facebook.com";

/// Resolve the Graph origin from an optional override.
pub(super) fn resolve_graph_base(raw: Option<String>) -> String {
    raw.map(|value| value.trim().trim_end_matches('/').to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_GRAPH_BASE.to_owned())
}

/// Whether `url` addresses the local machine. Cleartext is only ever allowed
/// there, so the access token cannot cross a network unencrypted.
fn is_loopback_http(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    parsed.scheme() == "http"
        && match parsed.host() {
            Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        }
}

/// Refuse to send credentials anywhere but HTTPS (or loopback, for tests).
pub(super) fn ensure_secure(url: &str) -> anyhow::Result<()> {
    if is_loopback_http(url) {
        return Ok(());
    }
    super::channel::ensure_https(url)
}

/// Classify a non-success status. A definite client-side refusal left the
/// account untouched; anything else may have been applied.
fn status_failure(status: reqwest::StatusCode, operation: &str) -> WaFailure {
    if status.is_client_error() && status != reqwest::StatusCode::REQUEST_TIMEOUT {
        WaFailure::rejected(format!("WhatsApp {operation} was refused ({status})"))
    } else {
        WaFailure::uncertain(format!(
            "WhatsApp {operation} outcome is uncertain ({status})"
        ))
    }
}

impl WhatsAppChannel {
    pub(super) fn graph_url(&self, path: &str) -> String {
        format!(
            "{}/{GRAPH_VERSION}/{}",
            self.api_base,
            path.trim_start_matches('/')
        )
    }

    fn secure_url(&self, path: &str) -> Result<String, WaFailure> {
        let url = self.graph_url(path);
        ensure_secure(&url).map_err(|error| WaFailure::rejected(error.to_string()))?;
        Ok(url)
    }

    /// POST one `/messages` body and return the provider's JSON answer.
    pub(super) async fn post_messages(&self, body: &Value) -> Result<Value, WaFailure> {
        let url = self.secure_url(&format!("{}/messages", self.endpoint_id))?;
        let response = self
            .http_client()
            .post(&url)
            .bearer_auth(&self.access_token)
            .json(body)
            .send()
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp send outcome is uncertain"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_failure(status, "send"));
        }
        response
            .json::<Value>()
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp send response was unreadable"))
    }

    /// The id of the message a `/messages` answer reports. The id is never
    /// inferred: an answer without one is an uncertain send.
    pub(super) fn answered_message_id(answer: &Value) -> Result<String, WaFailure> {
        answer
            .get("messages")
            .and_then(Value::as_array)
            .and_then(|messages| messages.first())
            .and_then(|message| message.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| WaFailure::uncertain("WhatsApp answered without a message id"))
    }

    /// Upload bytes to `/media` and return the media id.
    pub(super) async fn upload_media(
        &self,
        bytes: Vec<u8>,
        content_type: &str,
        file_name: &str,
    ) -> Result<String, WaFailure> {
        let url = self.secure_url(&format!("{}/media", self.endpoint_id))?;
        let file = reqwest::multipart::Part::bytes(bytes)
            .file_name(file_name.to_owned())
            .mime_str(content_type)
            .map_err(|_| WaFailure::rejected("WhatsApp media content type is invalid"))?;
        let form = reqwest::multipart::Form::new()
            .text("messaging_product", "whatsapp")
            .text("type", content_type.to_owned())
            .part("file", file);
        let response = self
            .http_client()
            .post(&url)
            .bearer_auth(&self.access_token)
            .multipart(form)
            .send()
            .await
            .map_err(|_| WaFailure::rejected("WhatsApp media upload failed before delivery"))?;
        let status = response.status();
        if !status.is_success() {
            // An upload creates no message, so a failed one is always safe to retry.
            return Err(WaFailure::rejected(format!(
                "WhatsApp media upload was refused ({status})"
            )));
        }
        let answer: Value = response
            .json()
            .await
            .map_err(|_| WaFailure::rejected("WhatsApp media upload answer was unreadable"))?;
        answer
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| WaFailure::rejected("WhatsApp media upload returned no id"))
    }

    /// Resolve a media id to its short-lived URL, then download the bytes with
    /// a hard size ceiling.
    pub(super) async fn download_media(
        &self,
        media_id: &str,
        limit: u64,
    ) -> Result<DownloadedAttachment, WaFailure> {
        if media_id.is_empty()
            || !media_id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            return Err(WaFailure::rejected("inbound media id is malformed"));
        }
        let lookup = self.secure_url(media_id)?;
        let response = self
            .http_client()
            .get(&lookup)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp media lookup failed"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_failure(status, "media lookup"));
        }
        let info: Value = response
            .json()
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp media lookup answer was unreadable"))?;
        let url = info
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| WaFailure::rejected("WhatsApp media lookup returned no URL"))?;
        ensure_secure(url).map_err(|_| WaFailure::rejected("WhatsApp media URL is not secure"))?;
        if info
            .get("file_size")
            .and_then(Value::as_u64)
            .is_some_and(|size| size > limit)
        {
            return Err(WaFailure::rejected("inbound media exceeds its size limit"));
        }
        let content_type = info
            .get("mime_type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let mut response = self
            .http_client()
            .get(url)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp media download failed"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_failure(status, "media download"));
        }
        if response
            .content_length()
            .is_some_and(|length| length > limit)
        {
            return Err(WaFailure::rejected("inbound media exceeds its size limit"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| WaFailure::uncertain("WhatsApp media download was interrupted"))?
        {
            if bytes.len() as u64 + chunk.len() as u64 > limit {
                return Err(WaFailure::rejected("inbound media exceeds its size limit"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(DownloadedAttachment {
            bytes,
            content_type,
            file_name: None,
            duration_ms: None,
        })
    }
}

#[cfg(test)]
#[path = "api_tests.rs"]
mod tests;
