//! Test-only helpers for exercising providers through their typed ports.

#![allow(deprecated)]

use crate::channel::{
    ChannelAdapter, ChannelInboundSink, ChannelSendOutcome, OutboundPayload, SendMessage,
    outbound_intent_from_send_message,
};
use async_trait::async_trait;
use std::error::Error;
use std::fmt::{Display, Formatter};
use tinychannels_bus::{ChannelIngress, ChannelMessage};

#[derive(Debug)]
pub(crate) struct TypedSendTestError(pub(crate) crate::channel::ChannelSendError);

impl Display for TypedSendTestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

impl Error for TypedSendTestError {}

/// Captures the typed provider stream through its one-release text projection
/// for tests that still assert the historical message fields.
pub(crate) struct LegacyMessageSink(tokio::sync::mpsc::Sender<ChannelMessage>);

#[derive(Default)]
pub(crate) struct RecordingInboundSink(std::sync::Mutex<Vec<ChannelIngress>>);

impl RecordingInboundSink {
    pub(crate) fn events(&self) -> Vec<ChannelIngress> {
        self.0.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChannelInboundSink for RecordingInboundSink {
    async fn push(
        &self,
        envelope: crate::channel::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.0
            .lock()
            .unwrap()
            .push(ChannelIngress::Inbound(envelope));
        Ok(())
    }

    async fn push_event(
        &self,
        event: tinychannels_bus::ChannelEvent,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.0.lock().unwrap().push(ChannelIngress::Event(event));
        Ok(())
    }
}

#[async_trait]
impl ChannelInboundSink for LegacyMessageSink {
    async fn push(
        &self,
        envelope: crate::channel::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        let timestamp = envelope.timestamp_unix_ms.map_or_else(
            || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            },
            |timestamp| timestamp / 1_000,
        );
        let message =
            crate::channel::envelope::typed_message_from_inbound_envelope(&envelope, timestamp);
        self.0
            .send(message)
            .await
            .map_err(|_| crate::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "legacy test receiver closed".into(),
                retryable: true,
                ..Default::default()
            })?;
        Ok(())
    }
}

pub(crate) async fn listen_v1<C: ChannelAdapter + ?Sized>(
    channel: &C,
    tx: tokio::sync::mpsc::Sender<ChannelMessage>,
) -> anyhow::Result<()> {
    channel
        .start(&LegacyMessageSink(tx))
        .await
        .map_err(|error| anyhow::Error::new(TypedSendTestError(error)))
}

/// Exercise a typed adapter with a one-release wire DTO converted at its edge.
pub(crate) async fn send_v1<C: ChannelAdapter>(
    channel: &C,
    message: &SendMessage,
) -> anyhow::Result<ChannelSendOutcome> {
    let mut intent = outbound_intent_from_send_message(&channel.descriptor().id, message);
    // Mirror the module's provider-specific v1-to-v2 conversion: only mail
    // and DingTalk consume the legacy subject field.
    let text = match (channel.descriptor().id.as_str(), message.subject.as_deref()) {
        ("email" | "dingtalk", Some(subject)) => format!(
            "Subject: {}\n{}",
            subject.trim().replace(['\r', '\n'], " "),
            message.content
        ),
        _ => message.content.clone(),
    };
    intent.payload = OutboundPayload::Text { text };
    let outcome = channel
        .send_outcome(intent)
        .await
        .map_err(|error| anyhow::Error::new(TypedSendTestError(error)))?;
    match outcome.error.clone() {
        Some(error) => Err(anyhow::Error::new(TypedSendTestError(error))),
        None => Ok(outcome),
    }
}

pub(crate) async fn is_healthy<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    ChannelAdapter::status(channel)
        .await
        .ok()
        .and_then(|snapshot| snapshot.health_state)
        .is_some_and(|state| state == "healthy")
}

pub(crate) fn supports_action<C: ChannelAdapter + ?Sized>(channel: &C, action: &str) -> bool {
    channel
        .declared_capabilities()
        .actions
        .supported
        .iter()
        .any(|supported| supported == action)
}

pub(crate) fn supports_reactions<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    channel.declared_capabilities().reactions
}

pub(crate) fn supports_edit<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    supports_action(channel, "edit")
}

pub(crate) fn supports_draft_updates<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    channel.declared_capabilities().streaming_reply_pipeline
}

pub(crate) fn supports_reply_to_id<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    channel.declared_capabilities().addressing.reply_to_id
}

pub(crate) fn supports_remote_control<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    channel.declared_capabilities().remote_control
}

pub(crate) fn supports_chat_approvals<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    channel.declared_capabilities().chat_approvals
}

pub(crate) fn history_key_ignores_thread<C: ChannelAdapter + ?Sized>(channel: &C) -> bool {
    channel.declared_capabilities().history_key_ignores_thread
}
