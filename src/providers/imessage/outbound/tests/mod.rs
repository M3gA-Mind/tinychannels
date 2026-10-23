use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelStreamingDraft, ChannelTypedSend,
    ChannelTyping,
};
use crate::providers::imessage::fixtures::*;
use crate::providers::native::fixtures::stored_media;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tinychannels_bus::channel::{ChannelSendOutcome, MessageReceipt, SendErrorKind};
use tinychannels_bus::{
    ChannelEventKind, ChannelIngress, ChannelOutboundIntent, ChannelOutboundMessage, ContentPart,
    ConversationKind, ConversationRef, MediaKind, MediaReference, OutboundContentPart,
    OutboundPayload, OutboundVoice,
};
use tokio::sync::{Notify, oneshot, watch};

const TYPED_PNG: &[u8] = b"png-bytes";

#[derive(Default)]
struct RecordingIngress {
    items: Mutex<Vec<ChannelIngress>>,
    entered: Notify,
}

#[async_trait]
impl crate::channel::ChannelInboundSink for RecordingIngress {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.items.lock().unwrap().push(ingress);
        self.entered.notify_one();
        Ok(())
    }
}

#[derive(Default)]
struct RejectSecondIngress {
    calls: AtomicUsize,
    accepted: Mutex<Vec<ChannelIngress>>,
}

#[async_trait]
impl crate::channel::ChannelInboundSink for RejectSecondIngress {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            return Err(tinychannels_bus::channel::ChannelSendError {
                message: "fixture rejects second delivery".into(),
                retryable: true,
                ..Default::default()
            });
        }
        self.accepted.lock().unwrap().push(ingress);
        Ok(())
    }
}

#[derive(Default)]
struct BlockingIngress {
    entered: Notify,
}

#[async_trait]
impl crate::channel::ChannelInboundSink for BlockingIngress {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        _ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.entered.notify_one();
        std::future::pending().await
    }
}

fn message(to: &str, parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "imessage".into(),
        conversation_id: to.into(),
        channel_ref: None,
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Dm,
            id: to.into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "k".into(),
        parts,
    }
}

#[path = "inbound_tests.rs"]
mod inbound_tests;
#[path = "outbound_tests.rs"]
mod outbound_tests;
