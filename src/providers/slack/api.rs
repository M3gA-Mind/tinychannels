//! Slack Web API calls and their mapping onto typed send errors.

use super::types::SlackChannel;
use serde_json::Value;
use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

/// Why a Web API call did not produce an `ok: true` body.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ApiFailure {
    /// The request may or may not have reached Slack.
    Transport,
    /// Slack answered with a non-success HTTP status.
    Http {
        status: u16,
        retry_after: Option<f64>,
    },
    /// HTTP 200 with a body that is not JSON.
    Malformed,
    /// HTTP 200 with `ok: false`; nothing was applied.
    Slack { code: String },
}

impl SlackChannel {
    pub(crate) fn api_url(&self, method: &str) -> String {
        format!("{}/{method}", self.api_base)
    }

    pub(crate) async fn api_json(&self, method: &str, body: &Value) -> Result<Value, ApiFailure> {
        tracing::debug!(method, "[slack] api call");
        let response = self
            .http_client()
            .post(self.api_url(method))
            .bearer_auth(&self.bot_token)
            .json(body)
            .send()
            .await
            .map_err(|_| ApiFailure::Transport)?;
        finish(response).await
    }

    pub(crate) async fn api_form(
        &self,
        method: &str,
        form: &[(&str, &str)],
    ) -> Result<Value, ApiFailure> {
        tracing::debug!(method, "[slack] api form call");
        let response = self
            .http_client()
            .post(self.api_url(method))
            .bearer_auth(&self.bot_token)
            .form(form)
            .send()
            .await
            .map_err(|_| ApiFailure::Transport)?;
        finish(response).await
    }

    /// The bot's own user ID, cached after the first successful `auth.test`.
    pub(crate) async fn bot_user_id(&self) -> Option<String> {
        if let Some(id) = self.bot_user_id.lock().clone() {
            return Some(id);
        }
        let value = self
            .http_client()
            .get(self.api_url("auth.test"))
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?
            .json::<Value>()
            .await
            .ok()?;
        let id = value.get("user_id")?.as_str()?.to_owned();
        *self.bot_user_id.lock() = Some(id.clone());
        Some(id)
    }
}

async fn finish(response: reqwest::Response) -> Result<Value, ApiFailure> {
    let status = response.status();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<f64>().ok());
        return Err(ApiFailure::Http {
            status: status.as_u16(),
            retry_after,
        });
    }
    let value = response
        .json::<Value>()
        .await
        .map_err(|_| ApiFailure::Malformed)?;
    if value.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(value);
    }
    let code = value
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    Err(ApiFailure::Slack { code })
}

/// Map a failed call onto the typed error. Messages never carry response
/// bodies or tokens.
pub(crate) fn send_error(failure: &ApiFailure, operation: &str) -> ChannelSendError {
    let base = |kind, message: String| ChannelSendError {
        kind,
        message,
        ..Default::default()
    };
    match failure {
        ApiFailure::Transport => ChannelSendError {
            sent_before_error: true,
            ..base(
                SendErrorKind::Transient,
                format!("Slack {operation} outcome is uncertain"),
            )
        },
        ApiFailure::Malformed => ChannelSendError {
            sent_before_error: true,
            ..base(
                SendErrorKind::Unknown,
                format!("Slack {operation} response was unreadable"),
            )
        },
        ApiFailure::Http {
            status,
            retry_after,
        } => {
            let message = format!("Slack {operation} failed with HTTP {status}");
            match *status {
                429 => ChannelSendError {
                    retryable: true,
                    retry_after: *retry_after,
                    ..base(SendErrorKind::RateLimited, message)
                },
                500..=599 => ChannelSendError {
                    sent_before_error: true,
                    ..base(SendErrorKind::Transient, message)
                },
                401 | 403 => base(SendErrorKind::Forbidden, message),
                404 => base(SendErrorKind::NotFound, message),
                _ => base(SendErrorKind::Unknown, message),
            }
        }
        ApiFailure::Slack { code } => {
            let message = format!("Slack {operation} failed: {code}");
            match code.as_str() {
                "ratelimited" => ChannelSendError {
                    retryable: true,
                    ..base(SendErrorKind::RateLimited, message)
                },
                "internal_error" | "service_unavailable" | "fatal_error" | "request_timeout" => {
                    ChannelSendError {
                        retryable: true,
                        ..base(SendErrorKind::Transient, message)
                    }
                }
                "channel_not_found" => ChannelSendError {
                    chat_level_not_found: true,
                    ..base(SendErrorKind::NotFound, message)
                },
                "message_not_found" | "thread_not_found" | "file_not_found" => {
                    base(SendErrorKind::NotFound, message)
                }
                "msg_too_long" => base(SendErrorKind::TooLong, message),
                "no_text"
                | "invalid_arguments"
                | "invalid_blocks"
                | "invalid_blocks_format"
                | "invalid_name"
                | "invalid_array_arg" => base(SendErrorKind::BadFormat, message),
                "not_in_channel"
                | "invalid_auth"
                | "not_authed"
                | "account_inactive"
                | "token_revoked"
                | "token_expired"
                | "missing_scope"
                | "is_archived"
                | "cant_delete_message"
                | "cant_update_message"
                | "edit_window_closed"
                | "restricted_action" => base(SendErrorKind::Forbidden, message),
                _ => base(SendErrorKind::Unknown, message),
            }
        }
    }
}

/// An explicit, non-retryable "this adapter does not do that" result.
pub(crate) fn unsupported(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        unsupported: true,
        ..Default::default()
    }
}

/// Rejected before any request reached Slack.
pub(crate) fn pre_dispatch(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "api_tests.rs"]
mod tests;
