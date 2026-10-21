//! Native DingTalk media ingress and typed App Bot sends.

use super::DingTalkChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::inbound::{InboundFile, Ingest, Ingested};
use crate::providers::native::{
    FileStep, Limits, NativeProvider, Sent, Step, Target, pre_dispatch, unsupported,
};
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use tinychannels_bus::channel::{
    ChannelInboundEnvelope, ChannelSendError, ConversationKind, ConversationRef,
    MessageReceiptPartKind, SendErrorKind, SenderRef,
};
use tinychannels_bus::{ChannelCapabilities, ContentCapability};
use tinychannels_bus::{VoiceTranscript, VoiceTranscriptSource, VoiceTranscriptState};

const MAX_MEDIA_BYTES: u64 = 10 * 1024 * 1024;
const MAX_VOICE_BYTES: u64 = 2 * 1024 * 1024;
const DINGTALK_API: &str = "https://api.dingtalk.com";
const DINGTALK_UPLOAD: &str = "https://oapi.dingtalk.com/media/upload";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DownloadResponse {
    download_url: String,
}

#[derive(Deserialize)]
struct UploadResponse {
    media_id: String,
}

fn known_unsent(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: message.into(),
        ..Default::default()
    }
}

fn transient_unsent(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Transient,
        message: message.into(),
        retryable: true,
        sent_before_error: false,
        ..Default::default()
    }
}

fn unsent_http_error(status: reqwest::StatusCode, operation: &str) -> ChannelSendError {
    let message = format!("DingTalk {operation} failed with HTTP {status}");
    if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        transient_unsent(message)
    } else {
        known_unsent(message)
    }
}

fn unknown_after_send(message: impl Into<String>) -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::Unknown,
        message: message.into(),
        sent_before_error: true,
        ..Default::default()
    }
}

#[async_trait]
impl NativeProvider for DingTalkChannel {
    fn id(&self) -> &'static str {
        "dingtalk"
    }

    fn display(&self) -> &'static str {
        "DingTalk"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: 20_000,
            file_bytes: MAX_MEDIA_BYTES,
            voice_bytes: MAX_VOICE_BYTES,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.native.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.native.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        self.native.voice_synthesis
    }

    fn voice_caption_supported(&self) -> bool {
        false
    }

    async fn health_state(&self) -> &'static str {
        if self.register_connection().await.is_ok() {
            "healthy"
        } else {
            "unhealthy"
        }
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        let mut caps = crate::providers::long_tail::text_capabilities(
            crate::providers::long_tail::TextSpec::default(),
        );
        if let Some(_repository) = &self.native.media_repository {
            let image = ContentCapability {
                inbound: true,
                accepted_types: vec!["image/jpeg".into(), "image/png".into()],
                max_size_bytes: Some(MAX_MEDIA_BYTES),
                ..Default::default()
            };
            let file = ContentCapability {
                inbound: true,
                outbound: true,
                accepted_types: vec![
                    "application/pdf".into(),
                    "application/zip".into(),
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                        .into(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
                ],
                emitted_types: vec![
                    "application/pdf".into(),
                    "application/zip".into(),
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                        .into(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
                ],
                max_size_bytes: Some(MAX_MEDIA_BYTES),
                ..Default::default()
            };
            caps.content.parts.extend(["image".into(), "file".into()]);
            caps.content.image = image;
            caps.content.file = file;
        }
        if self.native.voice_media_store.is_some() {
            caps.content.audio = ContentCapability {
                inbound: true,
                accepted_types: vec![
                    "audio/ogg".into(),
                    "audio/mpeg".into(),
                    "audio/mp4".into(),
                    "audio/amr".into(),
                ],
                max_size_bytes: Some(MAX_MEDIA_BYTES),
                ..Default::default()
            };
            caps.voice_in.supported = true;
            caps.voice_in.accepted_codecs = vec![
                "ogg_opus".into(),
                "mp3".into(),
                "m4a_aac".into(),
                "amr_nb".into(),
            ];
            caps.voice_in.max_size_bytes = Some(MAX_MEDIA_BYTES);
        }
        if self.native.voice_media_store.is_some() {
            caps.content.parts.push("voice".into());
            caps.voice_out.supported = true;
            caps.voice_out.accepted_codecs = vec!["amr_nb".into()];
            caps.voice_out.max_size_bytes = Some(MAX_VOICE_BYTES);
            caps.voice_out.synthesis = self.native.voice_synthesis;
        }
        caps
    }

    fn proactive_target(&self) -> Option<String> {
        None
    }

    async fn prepare_steps(
        &self,
        _target: &Target,
        steps: &mut [Step],
    ) -> Result<(), ChannelSendError> {
        for step in steps {
            match step {
                Step::File(file) => {
                    if file
                        .caption
                        .as_deref()
                        .is_some_and(|caption| !caption.is_empty())
                    {
                        return Err(unsupported(
                            "DingTalk App Bot file captions are not supported",
                        ));
                    }
                    validate_file(file)?;
                }
                Step::Text(_) => {}
                Step::Voice(voice) => {
                    if voice.bytes.is_empty() || voice.bytes.len() as u64 > MAX_VOICE_BYTES {
                        return Err(pre_dispatch(
                            "DingTalk App Bot voice exceeds the 2 MiB upload limit",
                        ));
                    }
                    if voice.mime != "audio/amr" || !voice.bytes.starts_with(b"#!AMR\n") {
                        return Err(pre_dispatch(
                            "DingTalk App Bot voice must be an AMR-NB audio stream",
                        ));
                    }
                    if voice.duration_ms.is_none() {
                        return Err(pre_dispatch(
                            "DingTalk App Bot voice requires measured duration metadata",
                        ));
                    }
                    if voice
                        .caption
                        .as_deref()
                        .is_some_and(|caption| !caption.is_empty())
                    {
                        return Err(unsupported(
                            "DingTalk App Bot voice captions are not supported",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        let context = self
            .robot_contexts
            .read()
            .await
            .get(&conversation_context_key(
                target.conversation.kind,
                &target.conversation.id,
            ))
            .cloned()
            .ok_or_else(|| {
                pre_dispatch("DingTalk send requires an admitted App Bot conversation")
            })?;
        let robot_code = context
            .get("robotCode")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| pre_dispatch("DingTalk conversation has no robotCode"))?;
        let token = self.access_token().await?;
        let (msg_key, msg_param, part_kind) = match step {
            Step::Text(text) => (
                "sampleText",
                serde_json::json!({"content": text}),
                MessageReceiptPartKind::Text,
            ),
            Step::File(file) => {
                let media_id = self.upload_file(&token, file).await?;
                (
                    "sampleFile",
                    serde_json::json!({
                        "mediaId": media_id,
                        "fileName": file.name,
                        "fileType": file_type(&file.name),
                    }),
                    MessageReceiptPartKind::Media,
                )
            }
            Step::Voice(voice) => {
                let media_id = self.upload_voice(&token, voice).await?;
                (
                    "sampleAudio",
                    serde_json::json!({
                        "mediaId": media_id,
                        "duration": voice.duration_ms.expect("validated voice duration").to_string(),
                    }),
                    MessageReceiptPartKind::Voice,
                )
            }
        };
        let mut request = serde_json::json!({
            "robotCode": robot_code,
            "msgKey": msg_key,
            "msgParam": msg_param.to_string(),
        });
        let direct_user_id = if target.conversation.kind == ConversationKind::Group {
            request["openConversationId"] = serde_json::json!(target.conversation.id);
            None
        } else {
            // The official BatchSendOTO response partitions recipients into
            // `*StaffIdList` outcomes, so the callback's tenant-scoped staff
            // ID is the compatible identifier for this `userIds` entry.
            let user_id = context
                .get("senderStaffId")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| pre_dispatch("DingTalk direct conversation has no senderStaffId"))?
                .to_owned();
            request["userIds"] = serde_json::json!([user_id]);
            Some(user_id)
        };
        let path = if target.conversation.kind == ConversationKind::Group {
            "/v1.0/robot/groupMessages/send"
        } else {
            "/v1.0/robot/oToMessages/batchSend"
        };
        let response = self
            .http_client()
            .post(format!("{DINGTALK_API}{path}"))
            .header("x-acs-dingtalk-access-token", token)
            .json(&request)
            .send()
            .await
            .map_err(|_| unknown_after_send("DingTalk send request outcome is uncertain"))?;
        if !response.status().is_success() {
            if response.status().is_server_error() {
                return Err(unknown_after_send(format!(
                    "DingTalk send outcome is uncertain after HTTP {}",
                    response.status()
                )));
            }
            return Err(known_unsent(format!(
                "DingTalk send failed with HTTP {}",
                response.status()
            )));
        }
        let response: serde_json::Value = response
            .json()
            .await
            .map_err(|_| unknown_after_send("DingTalk send response could not be read"))?;
        if let Some(user_id) = direct_user_id.as_deref() {
            if response_lists_user(&response, "flowControlledStaffIdList", user_id) {
                return Err(transient_unsent(
                    "DingTalk temporarily flow-controlled the direct-message recipient",
                ));
            }
            if response_lists_user(&response, "invalidStaffIdList", user_id) {
                return Err(known_unsent(
                    "DingTalk rejected the direct-message recipient as invalid",
                ));
            }
            if response_lists_user(&response, "filteredStaffIdList", user_id) {
                return Err(known_unsent(
                    "DingTalk filtered the direct-message recipient",
                ));
            }
        }
        let message_id = response
            .get("processQueryKey")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| unknown_after_send("DingTalk accepted the send without a receipt ID"))?;
        Ok(vec![Sent::new(message_id, part_kind)])
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
        self.listen_owned(super::DingTalkIngress::Typed(sink), Some(cancellation))
            .await
            .map_err(|error| {
                tracing::debug!(%error, "DingTalk typed listener stopped");
                ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "DingTalk listener stopped".into(),
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

fn response_lists_user(response: &serde_json::Value, field: &str, user_id: &str) -> bool {
    response
        .get(field)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(user_id)))
}

fn conversation_context_key(kind: ConversationKind, id: &str) -> String {
    format!(
        "{}:{id}",
        if kind == ConversationKind::Group {
            "group"
        } else {
            "dm"
        }
    )
}

fn validate_file(file: &FileStep) -> Result<(), ChannelSendError> {
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

fn file_type(name: &str) -> String {
    name.rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default()
}

impl DingTalkChannel {
    async fn access_token(&self) -> Result<String, ChannelSendError> {
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

    async fn upload_file(&self, token: &str, file: &FileStep) -> Result<String, ChannelSendError> {
        self.upload_media(token, file, "file").await
    }

    async fn upload_voice(
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

    pub(super) async fn inbound_envelope(
        &self,
        data: &serde_json::Value,
    ) -> Option<ChannelInboundEnvelope> {
        self.inbound_envelope_with_mode(data, false).await
    }

    pub(super) async fn inbound_envelope_for_listener(
        &self,
        data: &serde_json::Value,
    ) -> Option<ChannelInboundEnvelope> {
        self.inbound_envelope_with_mode(data, true).await
    }

    async fn inbound_envelope_with_mode(
        &self,
        data: &serde_json::Value,
        listener: bool,
    ) -> Option<ChannelInboundEnvelope> {
        use crate::providers::native::inbound::{assemble, media_of};

        let message_id = data.get("msgId")?.as_str()?.to_owned();
        if message_id.is_empty() || self.native.has_seen(&format!("dingtalk:{message_id}")) {
            return None;
        }
        let sender_id = data.get("senderStaffId")?.as_str()?.to_owned();
        if sender_id.is_empty() || !self.is_user_allowed(&sender_id) {
            return None;
        }
        let is_direct = data
            .get("conversationType")
            .and_then(|value| {
                value
                    .as_str()
                    .map(|value| value == "1")
                    .or_else(|| value.as_i64().map(|value| value == 1))
            })
            .unwrap_or(true);
        let conversation_id = data
            .get("conversationId")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(&sender_id)
            .to_owned();
        let conversation = ConversationRef {
            kind: if is_direct {
                ConversationKind::Dm
            } else {
                ConversationKind::Group
            },
            id: conversation_id.clone(),
            ..Default::default()
        };
        self.robot_contexts.write().await.insert(
            conversation_context_key(conversation.kind, &conversation_id),
            serde_json::json!({
                "robotCode": data.get("robotCode").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "senderId": data.get("senderId").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "senderStaffId": data.get("senderStaffId").and_then(serde_json::Value::as_str).unwrap_or_default(),
            }),
        );
        let message_type = data
            .get("msgtype")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let content = data.get("content").unwrap_or(&serde_json::Value::Null);
        let text = data
            .pointer("/text/content")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_owned();
        let mut ingested = Ingested::default();
        let voice = message_type == "audio";
        let can_ingest_file = (listener || self.native.events.is_some())
            && self.native.media_repository.is_some()
            && matches!(message_type, "picture" | "file");
        let can_ingest_voice = (listener || self.native.events.is_some())
            && self.native.voice_media_store.is_some()
            && voice;
        if can_ingest_file || can_ingest_voice {
            let download_code = if message_type == "picture" {
                content
                    .get("pictureDownloadCode")
                    .or_else(|| content.get("downloadCode"))
            } else {
                content.get("downloadCode")
            }
            .and_then(serde_json::Value::as_str)?;
            let robot_code = data.get("robotCode")?.as_str()?;
            let (bytes, mime) = self
                .download_media(download_code, robot_code, MAX_MEDIA_BYTES)
                .await?;
            let image = message_type == "picture";
            let transcript = content
                .get("recognition")
                .and_then(serde_json::Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(|text| VoiceTranscript {
                    text: text.to_owned(),
                    language: None,
                    state: VoiceTranscriptState::Complete,
                    source: VoiceTranscriptSource::Provider,
                });
            Ingest {
                provider: "dingtalk",
                channel: tinychannels_bus::ChannelRef {
                    id: "dingtalk".into(),
                    account_id: Some(self.client_id.clone()),
                },
                repository: self.native.media_repository.as_ref(),
                voice_store: self.native.voice_media_store.as_ref(),
                transcriber: self.native.transcriber.as_ref(),
                file_bytes: MAX_MEDIA_BYTES,
                voice_bytes: MAX_MEDIA_BYTES,
            }
            .add(
                &mut ingested,
                &conversation,
                &message_id,
                InboundFile {
                    bytes,
                    mime: if mime.is_empty() {
                        if image {
                            "image/jpeg"
                        } else {
                            "application/octet-stream"
                        }
                        .into()
                    } else {
                        mime
                    },
                    name: content
                        .get("fileName")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    duration_ms: None,
                    voice,
                    dedupe_key: format!("{message_id}:0"),
                    transcript,
                },
            )
            .await;
        }
        let parts = assemble(&text, None, ingested);
        if parts.is_empty() {
            return None;
        }
        Some(ChannelInboundEnvelope {
            channel: tinychannels_bus::ChannelRef {
                id: "dingtalk".into(),
                account_id: Some(self.client_id.clone()),
            },
            message_id: message_id.clone(),
            timestamp_unix_ms: data.get("createAt").and_then(serde_json::Value::as_u64),
            conversation,
            sender: SenderRef {
                id: sender_id,
                name: data
                    .get("senderNick")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                ..Default::default()
            },
            text,
            media: media_of(&parts),
            parts,
            raw: Some(serde_json::json!({
                "providerMessageId": message_id,
                "messageType": message_type,
            })),
            ..Default::default()
        })
    }

    async fn download_media(
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

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
