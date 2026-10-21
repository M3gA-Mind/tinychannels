//! Native typed media delivery for Lark and Feishu.

use super::LarkChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{FileStep, Limits, NativeProvider, Sent, Step, Target};
use async_trait::async_trait;
use base64::Engine as _;
use std::sync::Arc;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind};
use tinychannels_bus::{
    ChannelCapabilities, ChannelInboundEnvelope, ChannelRef, ConversationKind, ConversationRef,
    MediaKind, SenderRef,
};

const IMAGE_LIMIT: u64 = 10 * 1024 * 1024;
const FILE_LIMIT: u64 = 30 * 1024 * 1024;

#[async_trait]
impl NativeProvider for LarkChannel {
    fn id(&self) -> &'static str {
        "lark"
    }

    fn display(&self) -> &'static str {
        "Lark"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: 20_000,
            file_bytes: FILE_LIMIT,
            voice_bytes: FILE_LIMIT,
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

    fn declared_capabilities(&self) -> ChannelCapabilities {
        crate::providers::long_tail::LongTail::declared(self)
    }

    fn validate_target(&self, target: &Target) -> Result<(), ChannelSendError> {
        if target.thread.is_some() || target.reply_to.is_some() {
            return Err(crate::providers::native::unsupported(
                "Lark thread and reply addressing",
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
            let is_voice = matches!(step, Step::Voice(_));
            let file = match step {
                Step::Text(_) => continue,
                Step::File(file) if file.kind == MediaKind::Image => {
                    if file.bytes.len() as u64 > IMAGE_LIMIT {
                        return Err(crate::providers::native::pre_dispatch(
                            "Lark image exceeds the 10 MiB limit",
                        ));
                    }
                    if !matches!(
                        file.mime.as_str(),
                        "image/jpeg" | "image/png" | "image/gif" | "image/webp"
                    ) {
                        return Err(crate::providers::native::pre_dispatch(
                            "Lark image type is not supported",
                        ));
                    }
                    continue;
                }
                Step::File(file) | Step::Voice(file) => file,
            };
            if file
                .caption
                .as_deref()
                .is_some_and(|caption| !caption.trim().is_empty())
            {
                return Err(crate::providers::native::unsupported(
                    "Lark file/audio captions",
                ));
            }
            if file.bytes.len() as u64 > FILE_LIMIT {
                return Err(crate::providers::native::pre_dispatch(
                    "Lark file exceeds the 30 MiB limit",
                ));
            }
            if file.kind == MediaKind::Video && file.mime != "video/mp4" {
                return Err(crate::providers::native::unsupported("Lark video type"));
            }
            if is_voice || file.kind == MediaKind::Audio {
                let (bytes, name) = self.opus_audio(file).await?;
                file.bytes = bytes;
                file.name = name;
                file.mime = "audio/ogg".into();
            }
        }
        Ok(())
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        match step {
            Step::Text(text) => {
                let id = self
                    .send_content(
                        &target.conversation.id,
                        "text",
                        serde_json::json!({"text": text}),
                    )
                    .await?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Text)])
            }
            Step::File(file) if file.kind == MediaKind::Image => {
                if file.bytes.len() as u64 > IMAGE_LIMIT {
                    return Err(known_unsent("Lark image exceeds the 10 MiB limit"));
                }
                let key = self.upload_image(file).await?;
                let (message_type, content) = if let Some(caption) = file
                    .caption
                    .as_deref()
                    .filter(|caption| !caption.trim().is_empty())
                {
                    (
                        "post",
                        serde_json::json!({"zh_cn": {"title": "", "content": [
                            [{"tag": "text", "text": caption}],
                            [{"tag": "img", "image_key": key}]
                        ]}}),
                    )
                } else {
                    ("image", serde_json::json!({"image_key": key}))
                };
                let id = self
                    .send_content(&target.conversation.id, message_type, content)
                    .await?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Media)])
            }
            Step::File(file) | Step::Voice(file) => {
                if file
                    .caption
                    .as_deref()
                    .is_some_and(|caption| !caption.trim().is_empty())
                {
                    return Err(crate::providers::native::unsupported(
                        "Lark file/audio captions",
                    ));
                }
                if file.bytes.len() as u64 > FILE_LIMIT {
                    return Err(known_unsent("Lark file exceeds the 30 MiB limit"));
                }
                let voice = matches!(step, Step::Voice(_));
                let audio_part = file.kind == MediaKind::Audio;
                let video_part = file.kind == MediaKind::Video;
                let (bytes, file_name, file_type, message_type) = if voice || audio_part {
                    let (bytes, file_name) = self.opus_audio(file).await?;
                    (bytes, file_name, "opus", "audio")
                } else if video_part && file.mime == "video/mp4" {
                    (file.bytes.clone(), file.name.clone(), "mp4", "media")
                } else {
                    (
                        file.bytes.clone(),
                        file.name.clone(),
                        file_type_for_mime(&file.mime),
                        "file",
                    )
                };
                let key = self
                    .upload_file(&bytes, &file_name, file_type, None)
                    .await?;
                let id = self
                    .send_content(
                        &target.conversation.id,
                        message_type,
                        serde_json::json!({"file_key": key}),
                    )
                    .await?;
                let kind = if voice {
                    MessageReceiptPartKind::Voice
                } else {
                    MessageReceiptPartKind::Media
                };
                Ok(vec![Sent::new(id, kind)])
            }
        }
    }
}

impl LarkChannel {
    pub(crate) async fn inbound_envelope(
        &self,
        event: &serde_json::Value,
    ) -> Option<ChannelInboundEnvelope> {
        self.inbound_envelope_with_listener(event, false).await
    }

    pub(crate) async fn inbound_envelope_for_listener(
        &self,
        event: &serde_json::Value,
        typed_sink_available: bool,
    ) -> Option<ChannelInboundEnvelope> {
        self.inbound_envelope_with_listener(event, typed_sink_available)
            .await
    }

    async fn inbound_envelope_with_listener(
        &self,
        event: &serde_json::Value,
        typed_sink_available: bool,
    ) -> Option<ChannelInboundEnvelope> {
        use crate::providers::native::inbound::{
            InboundFile, Ingest, Ingested, assemble, media_of,
        };

        let message = event.get("message")?;
        let message_id = message.get("message_id")?.as_str()?.to_owned();
        if self.native.has_seen(&format!("lark:{message_id}")) {
            return None;
        }
        let chat_id = message.get("chat_id")?.as_str()?.to_owned();
        let chat_type = message
            .get("chat_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let message_type = message
            .get("message_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let sender_id = event
            .pointer("/sender/sender_id/open_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if sender_id.is_empty()
            || !self.is_user_allowed(sender_id)
            || event
                .pointer("/sender/sender_type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind == "app" || kind == "bot")
            || (chat_type == "group"
                && !super::should_respond_in_group(
                    message
                        .get("mentions")
                        .and_then(serde_json::Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                ))
        {
            return None;
        }
        let content: serde_json::Value = serde_json::from_str(
            message
                .get("content")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("{}"),
        )
        .ok()?;
        let text = match message_type {
            "text" => content
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            "post" => super::parse_post_content(
                message
                    .get("content")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            )
            .unwrap_or_default(),
            _ => String::new(),
        };
        let thread_id = message
            .get("thread_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let conversation = ConversationRef {
            kind: if thread_id.is_some() {
                ConversationKind::Thread
            } else if chat_type == "p2p" {
                ConversationKind::Dm
            } else {
                ConversationKind::Group
            },
            id: chat_id,
            parent_id: message
                .get("root_id")
                .or_else(|| message.get("parent_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            thread_id,
            ..Default::default()
        };
        let sender = SenderRef {
            id: sender_id.to_owned(),
            ..Default::default()
        };
        let mut ingested = Ingested::default();
        let (key, resource_type, voice, default_mime) = match message_type {
            "image" => (
                content.get("image_key").and_then(serde_json::Value::as_str),
                "image",
                false,
                "image/jpeg",
            ),
            "audio" => (
                content.get("file_key").and_then(serde_json::Value::as_str),
                "file",
                true,
                "audio/ogg",
            ),
            "media" => (
                content.get("file_key").and_then(serde_json::Value::as_str),
                "file",
                false,
                "video/mp4",
            ),
            "file" => (
                content.get("file_key").and_then(serde_json::Value::as_str),
                "file",
                false,
                "application/octet-stream",
            ),
            _ => (None, "file", false, "application/octet-stream"),
        };
        if (self.native.events.is_some() || typed_sink_available)
            && (self.native.media_repository.is_some() || self.native.voice_media_store.is_some())
            && let Some(key) = key.filter(|key| !key.is_empty())
            && let Some(file) = self
                .download_resource(&message_id, key, resource_type, default_mime)
                .await
        {
            Ingest {
                provider: "lark",
                channel: ChannelRef {
                    id: "lark".into(),
                    account_id: None,
                },
                repository: self.native.media_repository.as_ref(),
                voice_store: self.native.voice_media_store.as_ref(),
                transcriber: self.native.transcriber.as_ref(),
                file_bytes: if resource_type == "image" {
                    IMAGE_LIMIT
                } else {
                    FILE_LIMIT
                },
                voice_bytes: FILE_LIMIT,
            }
            .add(
                &mut ingested,
                &conversation,
                &message_id,
                InboundFile {
                    bytes: file.0,
                    mime: file.1,
                    name: content
                        .get("file_name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .or(file.2),
                    duration_ms: content.get("duration").and_then(serde_json::Value::as_u64),
                    voice,
                    dedupe_key: format!("{message_id}:{key}"),
                    transcript: None,
                },
            )
            .await;
        }
        let parts = assemble(&text, None, ingested);
        if parts.is_empty() {
            return None;
        }
        let timestamp_unix_ms = message
            .get("create_time")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<u64>().ok());
        Some(ChannelInboundEnvelope {
            channel: ChannelRef {
                id: "lark".into(),
                account_id: None,
            },
            message_id: message_id.clone(),
            timestamp_unix_ms,
            conversation,
            sender,
            text,
            media: media_of(&parts),
            parts,
            raw: Some(serde_json::json!({"providerMessageId": message_id})),
            ..Default::default()
        })
    }

    async fn download_resource(
        &self,
        message_id: &str,
        key: &str,
        resource_type: &str,
        default_mime: &str,
    ) -> Option<(Vec<u8>, String, Option<String>)> {
        let token = self.get_tenant_access_token().await.ok()?;
        let url = format!(
            "{}/im/v1/messages/{}/resources/{}?type={}",
            self.api_base(),
            urlencoding::encode(message_id),
            urlencoding::encode(key),
            resource_type
        );
        let response = self
            .http_client()
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(crate::providers::native::inbound::normalize_mime)
            .filter(|mime| !mime.is_empty() && mime != "application/octet-stream")
            .unwrap_or_else(|| default_mime.into());
        let limit = if resource_type == "image" {
            IMAGE_LIMIT
        } else {
            FILE_LIMIT
        };
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if bytes.len().saturating_add(chunk.len()) as u64 > limit {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return None;
        }
        Some((bytes, mime, None))
    }

    async fn upload_image(&self, file: &FileStep) -> Result<String, ChannelSendError> {
        let image = reqwest::multipart::Part::bytes(file.bytes.clone())
            .file_name(file.name.clone())
            .mime_str(&file.mime)
            .map_err(|_| known_unsent("Lark image MIME type is invalid"))?;
        let form = reqwest::multipart::Form::new()
            .text("image_type", "message")
            .part("image", image);
        let token = self.upload_token().await?;
        let response = self
            .http_client()
            .post(format!("{}/im/v1/images", self.api_base()))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .map_err(|_| known_unsent("Lark image upload failed before message delivery"))?;
        if !response.status().is_success() {
            return Err(known_unsent("Lark image upload was rejected"));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| known_unsent("Lark image upload returned an invalid response"))?;
        if body.get("code").and_then(serde_json::Value::as_i64) != Some(0) {
            return Err(known_unsent("Lark image upload was rejected"));
        }
        body.pointer("/data/image_key")
            .and_then(serde_json::Value::as_str)
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| known_unsent("Lark image upload returned no image key"))
    }

    async fn upload_file(
        &self,
        bytes: &[u8],
        name: &str,
        file_type: &str,
        duration_ms: Option<u64>,
    ) -> Result<String, ChannelSendError> {
        if bytes.is_empty() || bytes.len() as u64 > FILE_LIMIT {
            return Err(known_unsent(
                "Lark file is empty or exceeds the 30 MiB limit",
            ));
        }
        let file = reqwest::multipart::Part::bytes(bytes.to_vec()).file_name(name.to_owned());
        let mut form = reqwest::multipart::Form::new()
            .text("file_type", file_type.to_owned())
            .text("file_name", name.to_owned())
            .part("file", file);
        if let Some(duration) = duration_ms {
            form = form.text("duration", duration.to_string());
        }
        let token = self.upload_token().await?;
        let response = self
            .http_client()
            .post(format!("{}/im/v1/files", self.api_base()))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .map_err(|_| known_unsent("Lark file upload failed before message delivery"))?;
        if !response.status().is_success() {
            return Err(known_unsent("Lark file upload was rejected"));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| known_unsent("Lark file upload returned an invalid response"))?;
        if body.get("code").and_then(serde_json::Value::as_i64) != Some(0) {
            return Err(known_unsent("Lark file upload was rejected"));
        }
        body.pointer("/data/file_key")
            .and_then(serde_json::Value::as_str)
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| known_unsent("Lark file upload returned no file key"))
    }

    async fn upload_token(&self) -> Result<String, ChannelSendError> {
        self.get_tenant_access_token()
            .await
            .map_err(|_| known_unsent("Lark authentication failed before upload"))
    }

    async fn send_content(
        &self,
        recipient: &str,
        message_type: &str,
        content: serde_json::Value,
    ) -> Result<String, ChannelSendError> {
        let mut token = self
            .get_tenant_access_token()
            .await
            .map_err(|_| known_unsent("Lark authentication failed before message delivery"))?;
        for attempt in 0..=1 {
            let response = self
                .http_client()
                .post(self.send_message_url())
                .bearer_auth(&token)
                .json(&serde_json::json!({
                    "receive_id": recipient,
                    "msg_type": message_type,
                    "content": content.to_string(),
                }))
                .send()
                .await
                .map_err(|_| uncertain_send("Lark message request failed"))?;
            let status = response.status();
            let body = response.json::<serde_json::Value>().await.ok();
            let token_expired = status == reqwest::StatusCode::UNAUTHORIZED
                || body
                    .as_ref()
                    .and_then(|body| body.get("code"))
                    .and_then(serde_json::Value::as_i64)
                    == Some(99991663);
            if token_expired && attempt == 0 {
                self.invalidate_token_if_current(&token).await;
                token = self
                    .get_tenant_access_token()
                    .await
                    .map_err(|_| known_unsent("Lark authentication failed before message retry"))?;
                continue;
            }
            if token_expired {
                return Err(known_unsent("Lark rejected the message authentication"));
            }
            let body = body.ok_or_else(|| uncertain_send("Lark message response was invalid"))?;
            if !status.is_success()
                || body.get("code").and_then(serde_json::Value::as_i64) != Some(0)
            {
                return Err(if status.is_client_error() {
                    known_unsent("Lark rejected the message")
                } else {
                    uncertain_send("Lark message delivery outcome is unknown")
                });
            }
            let message_id = body
                .pointer("/data/message_id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    uncertain_send("Lark accepted a message without returning its id")
                })?;
            return Ok(message_id);
        }
        Err(uncertain_send("Lark message delivery outcome is unknown"))
    }

    async fn opus_audio(&self, file: &FileStep) -> Result<(Vec<u8>, String), ChannelSendError> {
        let from = match file.mime.as_str() {
            "audio/ogg" | "audio/opus" => "ogg_opus",
            "audio/mp4" | "audio/m4a" | "audio/x-m4a" => "m4a_aac",
            "audio/mpeg" | "audio/mp3" => "mp3",
            "audio/wav" | "audio/wave" => "wav_pcm16",
            _ => return Err(crate::providers::native::unsupported("Lark audio codec")),
        };
        let bytes = if from == "ogg_opus" {
            if !is_ogg_opus(&file.bytes) {
                return Err(crate::providers::native::pre_dispatch(
                    "Lark audio must contain an Ogg Opus stream",
                ));
            }
            file.bytes.clone()
        } else {
            let codec = self.voice_codec.as_ref().ok_or_else(|| {
                crate::providers::native::pre_dispatch(
                    "Lark requires the host voice codec to convert audio to OPUS",
                )
            })?;
            let encoded = codec
                .transcode(crate::host::VoiceCodecRequest {
                    audio_base64: base64::engine::general_purpose::STANDARD.encode(&file.bytes),
                    from: from.into(),
                    target: "ogg_opus".into(),
                })
                .await
                .map_err(|_| {
                    crate::providers::native::pre_dispatch("Lark audio conversion failed")
                })?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded.audio_base64)
                .ok()
                .filter(|bytes| !bytes.is_empty() && bytes.len() as u64 <= FILE_LIMIT)
                .ok_or_else(|| {
                    crate::providers::native::pre_dispatch(
                        "Lark audio conversion returned invalid or oversized data",
                    )
                })?;
            if !is_ogg_opus(&bytes) {
                return Err(crate::providers::native::pre_dispatch(
                    "Lark audio conversion did not return Ogg Opus",
                ));
            }
            bytes
        };
        if bytes.is_empty() || bytes.len() as u64 > FILE_LIMIT {
            return Err(known_unsent(
                "Lark audio is empty or exceeds the 30 MiB limit",
            ));
        }
        let name = if file.name.to_ascii_lowercase().ends_with(".opus") {
            file.name.clone()
        } else {
            format!(
                "{}.opus",
                file.name
                    .rsplit_once('.')
                    .map_or(file.name.as_str(), |(stem, _)| stem)
            )
        };
        Ok((bytes, name))
    }
}

/// Check the Opus identification header in the first Ogg packet. This proves
/// the declared codec, not that every later audio packet decodes successfully.
pub(super) fn is_ogg_opus(bytes: &[u8]) -> bool {
    if bytes.len() < 27 || &bytes[..4] != b"OggS" || bytes[4] != 0 {
        return false;
    }
    let segment_count = bytes[26] as usize;
    let Some(lacing) = bytes.get(27..27 + segment_count) else {
        return false;
    };
    let mut packet_len = 0usize;
    for segment in lacing {
        packet_len += *segment as usize;
        if *segment < 255 {
            let packet = &bytes[27 + segment_count..];
            let header = &packet[..packet_len.min(packet.len())];
            if packet.len() < packet_len || header.len() < 19 || &header[..8] != b"OpusHead" {
                return false;
            }
            let channels = header[9];
            let mapping_family = header[18];
            return header[8] <= 15 && channels > 0 && (mapping_family != 0 || channels <= 2);
        }
    }
    false
}

fn file_type_for_mime(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "pdf",
        "application/msword"
        | "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "doc",
        "application/vnd.ms-excel"
        | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xls",
        "application/vnd.ms-powerpoint"
        | "application/vnd.openxmlformats-officedocument.presentationml.presentation" => "ppt",
        _ => "stream",
    }
}

fn known_unsent(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Transient,
        message: message.into(),
        retryable: true,
        sent_before_error: false,
        ..Default::default()
    }
}

fn uncertain_send(message: &str) -> ChannelSendError {
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
        message: message.into(),
        sent_before_error: true,
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
