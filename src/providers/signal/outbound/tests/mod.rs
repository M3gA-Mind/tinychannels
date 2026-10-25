use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelInboundSink, ChannelTypedSend,
    ChannelTyping, LegacyChannelReaction as ReactionPort,
};
use crate::providers::native::fixtures::stored_media;
use crate::providers::signal::fixtures::*;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Mutex;
use std::time::Duration;
use tinychannels_bus::channel::{MessageReceipt, MessageReceiptPart};
use tinychannels_bus::{
    ChannelIngress, ChannelOutboundIntent, ChannelOutboundMessage, ChannelRef, ContentPart,
    ConversationKind, MediaKind, MediaReference, OutboundContentPart, OutboundPayload,
    OutboundVoice,
};
use tokio::sync::Notify;

#[derive(Default)]
struct InboundRecorder(Mutex<Vec<ChannelIngress>>);

#[async_trait]
impl ChannelInboundSink for InboundRecorder {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(&self, ingress: ChannelIngress) -> Result<(), ChannelSendError> {
        self.0.lock().unwrap().push(ingress);
        Ok(())
    }
}

struct BlockingInboundSink(Notify);

#[async_trait]
impl ChannelInboundSink for BlockingInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(&self, _ingress: ChannelIngress) -> Result<(), ChannelSendError> {
        self.0.notify_one();
        std::future::pending().await
    }
}

struct RejectingInboundSink;

#[async_trait]
impl ChannelInboundSink for RejectingInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(&self, _ingress: ChannelIngress) -> Result<(), ChannelSendError> {
        Err(ChannelSendError {
            message: "test ingress rejection".into(),
            ..Default::default()
        })
    }
}

async fn send(
    channel: &SignalChannel,
    message: &ChannelOutboundMessage,
) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ChannelSendError> {
    ChannelTypedSend::send_typed(channel, message).await
}

fn conversation(id: &str) -> ConversationRef {
    ConversationRef {
        kind: ConversationKind::Dm,
        id: id.into(),
        ..Default::default()
    }
}

fn message(parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "signal".into(),
        conversation_id: PEER.into(),
        channel_ref: Some(ChannelRef {
            id: "signal".into(),
            account_id: None,
        }),
        conversation_ref: Some(conversation(PEER)),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "k".into(),
        parts,
    }
}

fn text(text: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: text.into(),
            markdown_dialect: None,
        },
    }
}

fn media_ref(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some("media-1".into()),
        kind,
        ..Default::default()
    }
}

fn receipt(id: &str, conv: &str) -> MessageReceipt {
    MessageReceipt {
        channel: Some(ChannelRef {
            id: "signal".into(),
            account_id: None,
        }),
        conversation: Some(conversation(conv)),
        primary_platform_message_id: Some(id.into()),
        platform_message_ids: vec![id.into()],
        parts: vec![MessageReceiptPart {
            platform_message_id: id.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn edit_intent(body: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "e".into(),
        channel_id: "signal".into(),
        conversation_id: PEER.into(),
        payload: OutboundPayload::Text { text: body.into() },
        ..Default::default()
    }
}

#[path = "inbound_tests.rs"]
mod inbound_tests;
#[path = "outbound_tests.rs"]
mod outbound_tests;
