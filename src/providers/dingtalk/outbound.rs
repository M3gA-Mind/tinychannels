//! Typed provider operations and protocol sends for DingTalk.

use super::DingTalkChannel;
use super::native::{
    DINGTALK_API, MAX_MEDIA_BYTES, MAX_VOICE_BYTES, conversation_context_key, file_type,
    known_unsent, response_lists_user, transient_unsent, unknown_after_send, validate_file,
};
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{
    Limits, NativeProvider, Sent, Step, Target, pre_dispatch, unsupported,
};
use async_trait::async_trait;
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{
    ChannelSendError, ConversationKind, MessageReceiptPartKind, SendErrorKind,
};

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
        super::capabilities::declared(self)
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
        if let Step::Text(text) = step
            && let Some(webhook) = self
                .session_webhooks
                .read()
                .await
                .get(&target.conversation.id)
                .cloned()
        {
            let (title, body) = text
                .strip_prefix("Subject: ")
                .and_then(|text| text.split_once('\n'))
                .filter(|(title, _)| !title.trim().is_empty())
                .map_or(("OpenHuman", text.as_str()), |(title, body)| {
                    (title.trim(), body)
                });
            let response = self
                .http_client()
                .post(webhook)
                .json(&serde_json::json!({
                    "msgtype": "markdown",
                    "markdown": { "title": title, "text": body }
                }))
                .send()
                .await
                .map_err(|_| unknown_after_send("DingTalk webhook reply outcome is uncertain"))?;
            if !response.status().is_success() {
                let status = response.status();
                return Err(if status.is_server_error() {
                    unknown_after_send("DingTalk webhook reply outcome is uncertain")
                } else {
                    known_unsent("DingTalk webhook reply was rejected")
                });
            }
            let body: serde_json::Value = response.json().await.map_err(|_| {
                unknown_after_send("DingTalk webhook reply response could not be verified")
            })?;
            match body.get("errcode").and_then(serde_json::Value::as_i64) {
                Some(0) => {}
                Some(_) => return Err(known_unsent("DingTalk webhook reply was rejected")),
                None => {
                    return Err(unknown_after_send(
                        "DingTalk webhook reply response could not be verified",
                    ));
                }
            }
            // Session webhooks do not return provider message IDs.
            return Ok(Vec::new());
        }

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
        self.listen_owned(sink, Some(cancellation))
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
