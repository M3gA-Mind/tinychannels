//! Native typed media delivery for QQ Official Bot.

use super::{QQ_API_BASE, QQChannel};
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{
    FileStep, Limits, NativeProvider, Sent, Step, Target, pre_dispatch, unsupported,
};
use async_trait::async_trait;
use base64::Engine as _;
use std::sync::Arc;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};
use tinychannels_bus::{ChannelCapabilities, ContentCapability, ConversationKind};

const INBOUND_ATTACHMENT_LIMIT: u64 = 50 * 1024 * 1024;
const MAX_INBOUND_ATTACHMENTS: usize = 10;

/// QQ's direct binary upload API is limited to 5 MiB. Larger files need the
/// multi-request chunk protocol, which this adapter does not yet implement.
const DIRECT_UPLOAD_LIMIT: u64 = 5 * 1024 * 1024;
const TEXT_LIMIT: usize = 2000;

pub(super) fn timestamp_unix_ms(value: &serde_json::Value) -> Option<u64> {
    if let Some(timestamp) = value.as_u64() {
        return Some(timestamp);
    }
    let timestamp = value.as_str()?;
    if let Ok(timestamp) = timestamp.parse::<u64>() {
        return Some(timestamp);
    }
    let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp).ok()?;
    u64::try_from(timestamp.timestamp_millis()).ok()
}

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
        let mut capabilities = crate::providers::long_tail::text_capabilities(
            crate::providers::long_tail::TextSpec::default(),
        );
        if self.native.media_repository.is_some() {
            let media = |types: &[&str]| ContentCapability {
                inbound: self.native.events.is_some(),
                outbound: true,
                accepted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
                emitted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
                max_size_bytes: Some(DIRECT_UPLOAD_LIMIT),
                ..Default::default()
            };
            capabilities
                .content
                .parts
                .extend(["image", "video", "file"].into_iter().map(str::to_owned));
            capabilities.content.image = media(&["image/jpeg", "image/png"]);
            capabilities.content.video = media(&["video/mp4"]);
            capabilities.content.file = media(&[
                "application/octet-stream",
                "application/pdf",
                "application/zip",
                "text/plain",
                "text/csv",
                "application/json",
            ]);
        }
        if self.native.voice_media_store.is_some() {
            capabilities.content.parts.push("voice_note".into());
            capabilities.voice_out = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                DIRECT_UPLOAD_LIMIT,
                &["ogg_opus", "mp3", "wav_pcm16", "silk"],
            );
        }
        capabilities
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
        self.listen_owned(super::QQIngress::Typed(sink), Some(cancellation))
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
    /// Build a typed QQ inbound envelope from a gateway message. Attachment
    /// downloads are restricted to HTTPS Tencent hosts, do not follow
    /// redirects, and are bounded before they reach host storage.
    pub(crate) async fn inbound_envelope(
        &self,
        event_type: &str,
        message: &serde_json::Value,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        self.inbound_envelope_with_mode(event_type, message, false)
            .await
    }

    pub(crate) async fn inbound_envelope_for_listener(
        &self,
        event_type: &str,
        message: &serde_json::Value,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        self.inbound_envelope_with_mode(event_type, message, true)
            .await
    }

    async fn inbound_envelope_with_mode(
        &self,
        event_type: &str,
        message: &serde_json::Value,
        listener: bool,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        use crate::providers::native::inbound::{
            InboundFile, Ingest, Ingested, assemble, media_of,
        };

        let message_id = message.get("id")?.as_str()?.to_owned();
        if message_id.is_empty() || self.native.has_seen(&format!("qq:{message_id}")) {
            return None;
        }
        let (sender_id, conversation_id, kind) = match event_type {
            "C2C_MESSAGE_CREATE" => {
                let author = message.get("author")?;
                let sender = author
                    .get("user_openid")
                    .or_else(|| author.get("id"))?
                    .as_str()?;
                (
                    sender.to_owned(),
                    format!("user:{sender}"),
                    ConversationKind::Dm,
                )
            }
            "GROUP_AT_MESSAGE_CREATE" => {
                let author = message.get("author")?;
                let sender = author.get("member_openid")?.as_str()?;
                let group = message.get("group_openid")?.as_str()?;
                (
                    sender.to_owned(),
                    format!("group:{group}"),
                    ConversationKind::Group,
                )
            }
            _ => return None,
        };
        // Keep the exact legacy allowlist behavior before any attachment I/O.
        if !self.is_user_allowed(&sender_id) {
            return None;
        }
        let text = message
            .get("content")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_owned();
        let conversation = tinychannels_bus::ConversationRef {
            kind,
            id: conversation_id,
            ..Default::default()
        };
        let mut ingested = Ingested::default();
        let mut downloaded_bytes = 0_u64;
        if (listener || self.native.events.is_some())
            && self.native.media_repository.is_some()
            && let Some(attachments) = message
                .get("attachments")
                .and_then(serde_json::Value::as_array)
        {
            for (index, attachment) in attachments.iter().take(MAX_INBOUND_ATTACHMENTS).enumerate()
            {
                let Some(url) = attachment.get("url").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let remaining = INBOUND_ATTACHMENT_LIMIT.saturating_sub(downloaded_bytes);
                let Some((bytes, response_mime)) = self.download_attachment(url, remaining).await
                else {
                    continue;
                };
                downloaded_bytes += bytes.len() as u64;
                let mime = attachment
                    .get("content_type")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(&response_mime)
                    .to_owned();
                let name = attachment
                    .get("filename")
                    .or_else(|| attachment.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                Ingest {
                    provider: "qq",
                    channel: tinychannels_bus::ChannelRef {
                        id: "qq".into(),
                        account_id: None,
                    },
                    repository: self.native.media_repository.as_ref(),
                    voice_store: self.native.voice_media_store.as_ref(),
                    transcriber: self.native.transcriber.as_ref(),
                    file_bytes: INBOUND_ATTACHMENT_LIMIT,
                    voice_bytes: INBOUND_ATTACHMENT_LIMIT,
                }
                .add(
                    &mut ingested,
                    &conversation,
                    &message_id,
                    InboundFile {
                        bytes,
                        mime,
                        name,
                        duration_ms: None,
                        // QQ attachments do not distinguish voice notes from
                        // ordinary audio files in the documented event shape.
                        voice: false,
                        dedupe_key: format!("{message_id}:{index}"),
                        transcript: None,
                    },
                )
                .await;
            }
        }
        let parts = assemble(&text, None, ingested);
        if parts.is_empty() {
            return None;
        }
        if !listener {
            self.native.mark_seen(format!("qq:{message_id}"));
        }
        Some(tinychannels_bus::ChannelInboundEnvelope {
            channel: tinychannels_bus::ChannelRef {
                id: "qq".into(),
                account_id: None,
            },
            message_id: message_id.clone(),
            timestamp_unix_ms: message.get("timestamp").and_then(timestamp_unix_ms),
            conversation,
            sender: tinychannels_bus::SenderRef {
                id: sender_id,
                ..Default::default()
            },
            text,
            media: media_of(&parts),
            parts,
            raw: Some(serde_json::json!({"providerMessageId": message_id})),
            ..Default::default()
        })
    }

    async fn download_attachment(
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

    fn route(&self, target: &Target, operation: &str) -> String {
        let (kind, id) = match target.conversation.kind {
            ConversationKind::Group => ("groups", target.conversation.id.as_str()),
            ConversationKind::Dm => ("users", target.conversation.id.as_str()),
            _ => unreachable!("QQ target was validated"),
        };
        format!("{QQ_API_BASE}/v2/{kind}/{id}/{operation}")
    }

    async fn upload(
        &self,
        target: &Target,
        file: &FileStep,
        file_type: u8,
    ) -> Result<String, ChannelSendError> {
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
        let token = self
            .get_token()
            .await
            .map_err(|_| transient("QQ media authentication failed", false))?;
        let response = self
            .http_client()
            .post(self.route(target, "files"))
            .header("Authorization", format!("QQBot {token}"))
            .json(&serde_json::json!({
                "file_type": file_type,
                "file_name": file.name,
                "file_data": base64::engine::general_purpose::STANDARD.encode(&file.bytes),
                "srv_send_msg": false,
            }))
            .send()
            .await
            .map_err(|_| transient("QQ media upload request failed", false))?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_error(status, "media upload", false));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| transient("QQ media upload response was invalid", false))?;
        body.get("file_info")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| pre_dispatch("QQ media upload returned no file_info"))
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

fn file_type(file: &FileStep, voice: bool) -> Result<u8, ChannelSendError> {
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

fn response_id(body: &serde_json::Value) -> Result<String, ChannelSendError> {
    body.get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| unknown("QQ accepted a message without an ID"))
}

fn status_error(
    status: reqwest::StatusCode,
    operation: &str,
    final_send: bool,
) -> ChannelSendError {
    if final_send && status.is_server_error() {
        unknown("QQ may have accepted the message but returned a server error")
    } else if status.is_server_error() {
        transient(&format!("QQ {operation} failed"), false)
    } else {
        ChannelSendError {
            kind: SendErrorKind::BadFormat,
            message: format!("QQ {operation} was rejected"),
            ..Default::default()
        }
    }
}

fn transient(message: &str, sent_before_error: bool) -> ChannelSendError {
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
