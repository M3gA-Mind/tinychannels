//! Discord REST execution and typed error mapping for the v2 adapter.

use super::api::format_discord_http_error;
use super::channel::DiscordChannel;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

/// A file sent in a multipart message body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Upload {
    pub(super) file_name: String,
    pub(super) content_type: String,
    pub(super) bytes: Vec<u8>,
}

/// Request body for one REST call.
pub(super) enum Payload {
    None,
    Json(Value),
    Multipart {
        payload: Value,
        uploads: Vec<Upload>,
    },
}

/// Typed failure that crosses the legacy `anyhow` seam unchanged.
#[derive(Debug)]
#[cfg(test)]
pub(crate) struct DiscordError(pub(crate) ChannelSendError);

#[cfg(test)]
impl std::fmt::Display for DiscordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.message)
    }
}

#[cfg(test)]
impl std::error::Error for DiscordError {}

/// Rejected before any request left the process: nothing was sent.
pub(super) fn pre_dispatch(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        retryable: false,
        sent_before_error: false,
        ..Default::default()
    }
}

/// The adapter does not expose this operation.
pub(super) fn unsupported(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        unsupported: true,
        retryable: false,
        sent_before_error: false,
        ..Default::default()
    }
}

/// Idempotency nonce for one message of a logical send (`<= 25` chars).
pub(super) fn nonce(idempotency_key: &str, index: usize) -> Option<String> {
    if idempotency_key.is_empty() {
        return None;
    }
    let digest = hex::encode(Sha256::digest(format!("{idempotency_key}:{index}")));
    Some(digest[..24].to_owned())
}

/// Map a non-success Discord response to the shared error taxonomy.
pub(super) fn error_for_status(
    endpoint: &str,
    status: reqwest::StatusCode,
    retry_after_header: Option<f64>,
    body: &str,
) -> ChannelSendError {
    let parsed: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let code = parsed.get("code").and_then(Value::as_u64);
    let retry_after = parsed
        .get("retry_after")
        .and_then(Value::as_f64)
        .or(retry_after_header);
    let bounded: String = body.chars().take(300).collect();
    let message = format_discord_http_error(endpoint, status, &bounded);
    let mut error = ChannelSendError {
        message,
        retryable: false,
        sent_before_error: false,
        ..Default::default()
    };
    match status.as_u16() {
        429 => {
            error.kind = SendErrorKind::RateLimited;
            error.retryable = true;
            error.retry_after = retry_after;
        }
        401 | 403 => error.kind = SendErrorKind::Forbidden,
        404 => {
            error.kind = SendErrorKind::NotFound;
            // 10003 Unknown Channel; an unknown message (10008) is not chat level.
            error.chat_level_not_found = code == Some(10003);
        }
        400 => {
            let too_long = parsed.to_string().contains("or fewer in length");
            error.kind = if too_long {
                SendErrorKind::TooLong
            } else {
                SendErrorKind::BadFormat
            };
        }
        500..=599 => {
            error.kind = SendErrorKind::Transient;
            error.retryable = true;
            error.sent_before_error = true;
        }
        _ => error.kind = SendErrorKind::Unknown,
    }
    error
}

fn transport_error(endpoint: &str, error: &reqwest::Error, idempotent: bool) -> ChannelSendError {
    // A connect failure never reached Discord; anything later may have.
    let reached = !error.is_connect() && !error.is_builder();
    ChannelSendError {
        kind: SendErrorKind::Transient,
        message: format!("Discord {endpoint} transport failed"),
        retryable: !reached || idempotent,
        sent_before_error: reached,
        ..Default::default()
    }
}

impl DiscordChannel {
    /// Execute one REST call. `idempotent` marks calls that are safe to
    /// repeat (a nonce-protected create, an edit, a delete, a reaction).
    pub(super) async fn call(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
        payload: Payload,
        endpoint: &'static str,
        idempotent: bool,
    ) -> Result<Value, ChannelSendError> {
        let mut request = self
            .http_client()
            .request(method, url)
            .header("Authorization", format!("Bot {}", self.bot_token));
        request = match payload {
            Payload::None => request,
            Payload::Json(body) => request.json(&body),
            Payload::Multipart { payload, uploads } => {
                let mut form = reqwest::multipart::Form::new().part(
                    "payload_json",
                    reqwest::multipart::Part::text(payload.to_string())
                        .mime_str("application/json")
                        .map_err(|_| pre_dispatch("Discord payload type is invalid"))?,
                );
                for (index, upload) in uploads.into_iter().enumerate() {
                    let part = reqwest::multipart::Part::bytes(upload.bytes)
                        .file_name(upload.file_name)
                        .mime_str(&upload.content_type)
                        .map_err(|_| pre_dispatch("Discord attachment content type is invalid"))?;
                    form = form.part(format!("files[{index}]"), part);
                }
                request.multipart(form)
            }
        };
        let response = request.send().await.map_err(|error| {
            tracing::debug!(endpoint, "[discord] request transport failed");
            transport_error(endpoint, &error, idempotent)
        })?;
        let status = response.status();
        let retry_after_header = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<f64>().ok());
        let body = response.text().await.map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: format!("Discord {endpoint} response was unreadable"),
            retryable: idempotent,
            sent_before_error: true,
            ..Default::default()
        })?;
        if !status.is_success() {
            tracing::debug!(
                endpoint,
                status = status.as_u16(),
                "[discord] request rejected"
            );
            return Err(error_for_status(
                endpoint,
                status,
                retry_after_header,
                &body,
            ));
        }
        if body.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&body).map_err(|_| ChannelSendError {
            kind: SendErrorKind::Unknown,
            message: format!("Discord {endpoint} returned an unreadable body"),
            retryable: false,
            sent_before_error: true,
            ..Default::default()
        })
    }

    /// Build `<api_base>/<segments...>` with each segment percent-encoded.
    pub(super) fn endpoint_url(&self, segments: &[&str]) -> Result<reqwest::Url, ChannelSendError> {
        let mut url = reqwest::Url::parse(&self.api_base)
            .map_err(|_| pre_dispatch("Discord API base is invalid"))?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| pre_dispatch("Discord API base is invalid"))?;
            path.pop_if_empty();
            path.extend(segments);
        }
        Ok(url)
    }
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
