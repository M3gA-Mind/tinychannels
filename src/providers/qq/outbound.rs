//! QQ typed delivery, receipts, and listener adapter.

use super::media::{DIRECT_UPLOAD_LIMIT, file_type};
use super::{QQ_API_BASE, QQChannel};
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{
    Limits, NativeProvider, Sent, Step, Target, pre_dispatch, unsupported,
};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};
use tinychannels_bus::{ChannelCapabilities, ConversationKind};

const TEXT_LIMIT: usize = 2000;

#[async_trait]
impl NativeProvider for QQChannel {
    fn id(&self) -> &'static str {
        "qq"
    }

    fn display(&self) -> &'static str {
        "QQ"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: TEXT_LIMIT,
            file_bytes: DIRECT_UPLOAD_LIMIT,
            voice_bytes: DIRECT_UPLOAD_LIMIT,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.native.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.native.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        false
    }

    async fn health_state(&self) -> &'static str {
        if self.fetch_access_token().await.is_ok() {
            "healthy"
        } else {
            "unhealthy"
        }
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        super::capabilities::declared(self)
    }

    fn normalized_target(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<Target, ChannelSendError> {
        let mut target =
            crate::providers::native::outbound::target_of(message, self.id(), self.display())?;
        if let Some((prefix, id)) = target.conversation.id.split_once(':') {
            match prefix {
                "group" => {
                    target.conversation.kind = ConversationKind::Group;
                    target.conversation.id = id.into();
                }
                "user" => {
                    target.conversation.kind = ConversationKind::Dm;
                    target.conversation.id = id.into();
                }
                _ => {}
            }
        } else if target.conversation.kind == ConversationKind::Unknown {
            // The one-release v1 send shape used an unprefixed recipient for
            // direct messages. Keep that boundary behavior while requiring
            // typed callers to identify groups explicitly.
            target.conversation.kind = ConversationKind::Dm;
        }
        Ok(target)
    }

    fn validate_target(&self, target: &Target) -> Result<(), ChannelSendError> {
        if target.thread.is_some() || target.reply_to.is_some() {
            return Err(unsupported("QQ thread and reply addressing"));
        }
        if target.conversation.id.is_empty()
            || target
                .conversation
                .id
                .chars()
                .any(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_')))
        {
            return Err(pre_dispatch("QQ conversation identity is invalid"));
        }
        if !matches!(
            target.conversation.kind,
            ConversationKind::Dm | ConversationKind::Group
        ) {
            return Err(pre_dispatch(
                "QQ send target must identify a direct message or group",
            ));
        }
        Ok(())
    }

    async fn prepare_steps(
        &self,
        _target: &Target,
        steps: &mut [Step],
    ) -> Result<(), ChannelSendError> {
        for step in steps {
            let file = match step {
                Step::Text(_) => continue,
                Step::File(file) => {
                    if file.mime.to_ascii_lowercase().starts_with("audio/") {
                        return Err(unsupported("QQ standalone audio messages"));
                    }
                    file_type(file, false)?;
                    file
                }
                Step::Voice(file) => {
                    file_type(file, true)?;
                    file
                }
            };
            if file.bytes.len() as u64 > DIRECT_UPLOAD_LIMIT {
                return Err(pre_dispatch("QQ direct media upload exceeds 5 MiB"));
            }
            if file
                .caption
                .as_deref()
                .is_some_and(|caption| !caption.trim().is_empty())
            {
                return Err(unsupported("QQ rich media captions"));
            }
        }
        Ok(())
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        match step {
            Step::Text(text) => {
                let response = self
                    .send_json(
                        target,
                        "messages",
                        serde_json::json!({
                            "content": text,
                            "msg_type": 0,
                        }),
                        true,
                    )
                    .await?;
                let id = response_id(&response)?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Text)])
            }
            Step::File(file) => {
                let file_type = file_type(file, false)?;
                let file_info = self.upload(target, file, file_type).await?;
                let response = self
                    .send_json(
                        target,
                        "messages",
                        serde_json::json!({
                            "media": {"file_info": file_info},
                            "msg_type": 7,
                        }),
                        true,
                    )
                    .await?;
                let id = response_id(&response)?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Media)])
            }
            Step::Voice(file) => {
                let file_type = file_type(file, true)?;
                let file_info = self.upload(target, file, file_type).await?;
                let response = self
                    .send_json(
                        target,
                        "messages",
                        serde_json::json!({
                            "media": {"file_info": file_info},
                            "msg_type": 7,
                        }),
                        true,
                    )
                    .await?;
                let id = response_id(&response)?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Voice)])
            }
        }
    }

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        let (id, cancellation) = self.begin_listener().map_err(|error| ChannelSendError {
            kind: SendErrorKind::Forbidden,
            message: error.to_string(),
            retryable: false,
            ..Default::default()
        })?;
        let _registration = super::ListenerRegistration { channel: self, id };
        self.listen_owned(sink, Some(cancellation))
            .await
            .map_err(|error| {
                tracing::debug!(%error, "QQ typed listener stopped");
                ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "QQ listener stopped".into(),
                    retryable: true,
                    ..Default::default()
                }
            })
    }

    async fn stop_listener(&self) -> Result<(), ChannelSendError> {
        if let Some((_, cancellation)) = self
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            cancellation.send_replace(true);
        }
        Ok(())
    }
}

impl QQChannel {
    pub(super) fn route(&self, target: &Target, operation: &str) -> String {
        let (kind, id) = match target.conversation.kind {
            ConversationKind::Group => ("groups", target.conversation.id.as_str()),
            ConversationKind::Dm => ("users", target.conversation.id.as_str()),
            _ => unreachable!("QQ target was validated"),
        };
        format!("{QQ_API_BASE}/v2/{kind}/{id}/{operation}")
    }

    async fn send_json(
        &self,
        target: &Target,
        operation: &str,
        body: serde_json::Value,
        final_send: bool,
    ) -> Result<serde_json::Value, ChannelSendError> {
        let token = self
            .get_token()
            .await
            .map_err(|_| transient("QQ message authentication failed", false))?;
        let response = self
            .http_client()
            .post(self.route(target, operation))
            .header("Authorization", format!("QQBot {token}"))
            .json(&body)
            .send()
            .await
            .map_err(|_| {
                if final_send {
                    unknown("QQ message request failed after submission")
                } else {
                    transient("QQ request failed", false)
                }
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_error(status, "message send", final_send));
        }
        response
            .json()
            .await
            .map_err(|_| unknown("QQ accepted a message without a readable receipt"))
    }
}

fn response_id(body: &serde_json::Value) -> Result<String, ChannelSendError> {
    body.get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| unknown("QQ accepted a message without an ID"))
}

pub(super) fn status_error(
    status: reqwest::StatusCode,
    operation: &str,
    final_send: bool,
) -> ChannelSendError {
    if final_send && status.is_server_error() {
        unknown("QQ may have accepted the message but returned a server error")
    } else if status.is_server_error() {
        transient(&format!("QQ {operation} failed"), false)
    } else if matches!(
        status,
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
    ) {
        ChannelSendError {
            kind: SendErrorKind::Forbidden,
            message: format!("QQ {operation} was rejected with HTTP {status}"),
            ..Default::default()
        }
    } else {
        ChannelSendError {
            kind: SendErrorKind::BadFormat,
            message: format!("QQ {operation} was rejected"),
            ..Default::default()
        }
    }
}

pub(super) fn transient(message: &str, sent_before_error: bool) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Transient,
        message: message.into(),
        retryable: !sent_before_error,
        sent_before_error,
        ..Default::default()
    }
}

fn unknown(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Unknown,
        message: message.into(),
        sent_before_error: true,
        ..Default::default()
    }
}
