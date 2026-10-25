//! Signal as a [`NativeProvider`]: typed sends, reactions, typing, edits and
//! remote deletes over signal-cli's JSON-RPC.

use super::SignalChannel;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{
    Address, FileStep, Limits, NativeProvider, Sent, Step, Target, pre_dispatch,
};
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{
    ChannelReaction, ChannelSendError, ConversationRef, MessageReceiptPartKind, SendErrorKind,
};
use uuid::Uuid;

pub(super) const GROUP_TARGET_PREFIX: &str = "group:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RecipientTarget {
    Direct(String),
    Group(String),
}
/// Signal's attachment ceiling is far higher; stay inside the 16 MiB bus frame.
pub(crate) const SIGNAL_MAX_FILE_BYTES: u64 = 12 * 1024 * 1024;
/// Voice bytes cross TinyBus inline, so they stay below the frame cap.
pub(crate) const SIGNAL_MAX_VOICE_BYTES: u64 = 8 * 1024 * 1024;
/// Signal displays longer bodies as a text attachment; chunk below that.
pub(crate) const SIGNAL_TEXT_CHUNK_CHARS: usize = 2000;

/// Map an RPC failure onto the typed error. Messages never carry response
/// bodies, numbers or tokens.
pub(crate) fn rpc_error(error: &anyhow::Error, operation: &str) -> ChannelSendError {
    let text = error.to_string();
    let base = |kind, message: String| ChannelSendError {
        kind,
        message,
        ..Default::default()
    };
    if error.downcast_ref::<reqwest::Error>().is_some() {
        return ChannelSendError {
            sent_before_error: true,
            ..base(
                SendErrorKind::Transient,
                format!("Signal {operation} outcome is uncertain"),
            )
        };
    }
    let lower = text.to_ascii_lowercase();
    if lower.contains("rate limit") || lower.contains("ratelimit") {
        return ChannelSendError {
            retryable: true,
            ..base(
                SendErrorKind::RateLimited,
                format!("Signal {operation} was rate limited"),
            )
        };
    }
    if lower.contains("unregistered") || lower.contains("not registered") {
        return base(
            SendErrorKind::NotFound,
            format!("Signal {operation} recipient is not registered"),
        );
    }
    if text.starts_with("Signal RPC error") {
        return base(
            SendErrorKind::Unknown,
            format!("Signal {operation} was rejected"),
        );
    }
    ChannelSendError {
        sent_before_error: true,
        ..base(
            SendErrorKind::Unknown,
            format!("Signal {operation} response was unreadable"),
        )
    }
}

impl SignalChannel {
    /// RPC params addressing `recipient` (a number, UUID or `group:<id>`).
    pub(crate) fn addressed(&self, recipient: &str) -> Value {
        match Self::parse_recipient_target(recipient) {
            RecipientTarget::Direct(number) => json!({
                "recipient": [number],
                "account": &self.account,
            }),
            RecipientTarget::Group(group_id) => json!({
                "groupId": group_id,
                "account": &self.account,
            }),
        }
    }

    /// The signal-cli RPC as a typed result.
    async fn call(
        &self,
        method: &str,
        params: Value,
        operation: &str,
    ) -> Result<Option<Value>, ChannelSendError> {
        self.rpc_request(method, params)
            .await
            .map_err(|error| rpc_error(&error, operation))
    }

    /// `(author, timestamp)` a reaction or quote targets. A bare timestamp is a
    /// message in a direct chat (its author is the peer) or, in a group, one
    /// the account itself sent; `author:timestamp` names the author.
    pub(crate) fn message_target(
        &self,
        conversation: &str,
        provider_message_id: &str,
    ) -> Result<(String, u64), ChannelSendError> {
        let (author, stamp) = match provider_message_id.rsplit_once(':') {
            Some((author, stamp)) if !author.is_empty() => (Some(author.to_owned()), stamp),
            _ => (None, provider_message_id),
        };
        let timestamp = stamp
            .trim_start_matches("sig_")
            .parse::<u64>()
            .map_err(|_| pre_dispatch("Signal message ids are millisecond timestamps"))?;
        let author = author.unwrap_or_else(|| match Self::parse_recipient_target(conversation) {
            RecipientTarget::Direct(peer) => peer,
            RecipientTarget::Group(_) => self.account.clone(),
        });
        Ok((author, timestamp))
    }

    fn attachment_uri(file: &FileStep) -> String {
        let name = file.name.replace([';', ',', '\n', '\r'], "_");
        format!(
            "data:{};filename={name};base64,{}",
            file.mime,
            BASE64.encode(&file.bytes)
        )
    }

    async fn send_params(
        &self,
        target: &Target,
        mut params: Value,
        operation: &str,
    ) -> Result<String, ChannelSendError> {
        if let Some(reply) = &target.reply_to {
            let (author, timestamp) = self.message_target(&target.conversation.id, reply)?;
            params["quoteTimestamp"] = timestamp.into();
            params["quoteAuthor"] = author.into();
        }
        let result = self.call("send", params, operation).await?;
        result
            .as_ref()
            .and_then(|value| value.get("timestamp"))
            .and_then(Value::as_u64)
            .map(|timestamp| timestamp.to_string())
            .ok_or_else(|| ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: format!("Signal {operation} returned no message timestamp"),
                sent_before_error: true,
                ..Default::default()
            })
    }
}

#[async_trait]
impl NativeProvider for SignalChannel {
    fn id(&self) -> &'static str {
        "signal"
    }

    fn display(&self) -> &'static str {
        "Signal"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: SIGNAL_TEXT_CHUNK_CHARS,
            file_bytes: SIGNAL_MAX_FILE_BYTES,
            voice_bytes: SIGNAL_MAX_VOICE_BYTES,
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

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        vec![("voiceOut", "attachment")]
    }

    async fn health_state(&self) -> &'static str {
        if self.health_probe().await {
            "healthy"
        } else {
            "unhealthy"
        }
    }

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        let (cancel, cancellation) = tokio::sync::watch::channel(false);
        let generation = self
            .listener_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .wrapping_add(1);
        {
            let mut active = self
                .listener_cancel
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if active.is_some() {
                return Err(ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "Signal listener is already running".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            *active = Some((generation, cancel));
        }
        let _registration = super::ListenerRegistration {
            slot: self.listener_cancel.clone(),
            generation,
        };
        let result = self.listen_typed(sink, cancellation).await;
        result.map_err(|error| rpc_error(&error, "listener"))
    }

    async fn stop_listener(&self) -> Result<(), ChannelSendError> {
        if let Some(cancel) = self
            .listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = cancel.1.send(true);
        }
        Ok(())
    }

    fn proactive_target(&self) -> Option<String> {
        self.group_id
            .clone()
            .filter(|group| !group.eq_ignore_ascii_case("dm"))
            .map(|group| format!("group:{group}"))
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        let mut params = self.addressed(&target.conversation.id);
        let (kind, operation) = match step {
            Step::Text(text) => {
                params["message"] = text.clone().into();
                (MessageReceiptPartKind::Text, "send")
            }
            Step::File(file) => {
                params["message"] = file.caption.clone().unwrap_or_default().into();
                params["attachments"] = json!([Self::attachment_uri(file)]);
                (MessageReceiptPartKind::Media, "attachment send")
            }
            Step::Voice(file) => {
                params["message"] = file.caption.clone().unwrap_or_default().into();
                params["attachments"] = json!([Self::attachment_uri(file)]);
                (MessageReceiptPartKind::Voice, "voice send")
            }
        };
        let id = self.send_params(target, params, operation).await?;
        Ok(vec![
            Sent::new(id, kind)
                .editable(matches!(step, Step::Text(_)))
                .deletable(true),
        ])
    }

    async fn set_typing(&self, conversation: &str, on: bool) -> Result<(), ChannelSendError> {
        let mut params = self.addressed(conversation);
        if !on {
            params["stop"] = true.into();
        }
        self.call("sendTyping", params, "typing").await.map(|_| ())
    }

    async fn react(
        &self,
        conversation: &ConversationRef,
        provider_message_id: &str,
        reaction: &ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        let emoji = reaction.value.trim();
        if emoji.is_empty() || conversation.id.trim().is_empty() {
            return Err(pre_dispatch("Signal reaction needs a message and an emoji"));
        }
        let (author, timestamp) = self.message_target(&conversation.id, provider_message_id)?;
        let mut params = self.addressed(&conversation.id);
        params["emoji"] = emoji.into();
        params["targetAuthor"] = author.into();
        params["targetTimestamp"] = timestamp.into();
        params["remove"] = remove.into();
        self.call("sendReaction", params, "reaction")
            .await
            .map(|_| ())
    }

    fn can_edit(&self) -> bool {
        true
    }

    fn can_delete(&self) -> bool {
        true
    }

    async fn edit_text(&self, address: &Address, text: &str) -> Result<(), ChannelSendError> {
        let timestamp = address
            .message_id
            .parse::<u64>()
            .map_err(|_| pre_dispatch("Signal message ids are millisecond timestamps"))?;
        let mut params = self.addressed(&address.conversation.id);
        params["message"] = text.into();
        params["editTimestamp"] = timestamp.into();
        self.call("send", params, "edit").await.map(|_| ())
    }

    async fn delete_message(&self, address: &Address) -> Result<(), ChannelSendError> {
        let timestamp = address
            .message_id
            .parse::<u64>()
            .map_err(|_| pre_dispatch("Signal message ids are millisecond timestamps"))?;
        let mut params = self.addressed(&address.conversation.id);
        params["targetTimestamp"] = timestamp.into();
        self.call("remoteDelete", params, "delete")
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
#[path = "outbound/tests/mod.rs"]
mod tests;

impl SignalChannel {
    pub(super) fn is_e164(recipient: &str) -> bool {
        let Some(number) = recipient.strip_prefix('+') else {
            return false;
        };
        (2..=15).contains(&number.len()) && number.chars().all(|c| c.is_ascii_digit())
    }

    /// Check whether a string is a valid UUID (signal-cli uses these for
    /// privacy-enabled users who have opted out of sharing their phone number).
    pub(super) fn is_uuid(s: &str) -> bool {
        Uuid::parse_str(s).is_ok()
    }

    pub(super) fn parse_recipient_target(recipient: &str) -> RecipientTarget {
        if let Some(group_id) = recipient.strip_prefix(GROUP_TARGET_PREFIX) {
            return RecipientTarget::Group(group_id.to_string());
        }

        if Self::is_e164(recipient) || Self::is_uuid(recipient) {
            RecipientTarget::Direct(recipient.to_string())
        } else {
            RecipientTarget::Group(recipient.to_string())
        }
    }

    pub(super) async fn rpc_request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let url = format!("{}/api/v1/rpc", self.http_url);
        let id = Uuid::new_v4().to_string();

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": id,
        });

        let resp = self
            .http_client()
            .post(&url)
            .timeout(Duration::from_secs(30))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        // 201 = success with no body (e.g. typing indicators)
        if resp.status().as_u16() == 201 {
            return Ok(None);
        }

        let text = resp.text().await?;
        if text.is_empty() {
            return Ok(None);
        }

        let parsed: serde_json::Value = serde_json::from_str(&text)?;
        if let Some(err) = parsed.get("error") {
            let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
            let msg = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Signal RPC error {code}: {msg}");
        }

        Ok(parsed.get("result").cloned())
    }
}
