use crate::adapters::{LocalChannelAdapter, LocalOutboundSink};
use crate::channel::{
    ChannelAdapter, ChannelDescriptor, ChannelInboundSink,
    ChannelOutboundIntent, ChannelSendError, DeliveryDurability, MessageReceipt, OutboundPayload,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct RecordingOutboundSink {
    ids: Mutex<Vec<String>>,
}

#[async_trait]
impl LocalOutboundSink for RecordingOutboundSink {
    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.ids
            .lock()
            .expect("ids lock")
            .push(intent.idempotency_key);
        Ok(MessageReceipt {
            primary_platform_message_id: Some("local-1".into()),
            platform_message_ids: vec!["local-1".into()],
            sent_at: 1,
            ..Default::default()
        })
    }
}

struct NoopInboundSink;

#[async_trait]
impl ChannelInboundSink for NoopInboundSink {
    async fn push(&self, _ingress: tinychannels_bus::ChannelIngress) -> Result<(), ChannelSendError> {
        Ok(())
    }
}

fn local_adapter(sink: Arc<RecordingOutboundSink>) -> LocalChannelAdapter {
    LocalChannelAdapter::new(
        ChannelDescriptor {
            id: "local".into(),
            display_name: "Local".into(),
            account_id: Some("host".into()),
            ..Default::default()
        },
        tinychannels_bus::ChannelCapabilities {
            actions: tinychannels_bus::ActionCapabilities {
                supported: vec!["reply".into()],
                ..Default::default()
            },
            ..Default::default()
        },
        sink,
    )
}

#[tokio::test]
async fn local_adapter_delegates_outbound_delivery() {
    let sink = Arc::new(RecordingOutboundSink::default());
    let adapter = local_adapter(sink.clone());

    let outcome = adapter
        .send(ChannelOutboundIntent {
            idempotency_key: "turn-1:text".into(),
            channel_id: "local".into(),
            conversation_id: "chat-1".into(),
            channel_ref: None,
            conversation_ref: None,
            reply_to_id: None,
            thread_id: None,
            turn_id: None,
            durability: DeliveryDurability::BestEffort,
            payload: OutboundPayload::Text { text: "hi".into() },
        })
        .await
        .expect("send");

    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("local-1")
    );
    assert!(outcome.error.is_none());
    assert_eq!(
        sink.ids.lock().expect("ids lock").as_slice(),
        ["turn-1:text"]
    );
}

#[tokio::test]
async fn local_adapter_reports_configured_status() {
    let adapter = local_adapter(Arc::new(RecordingOutboundSink::default()));
    adapter.start(&NoopInboundSink).await.expect("start");

    let status = adapter.status().await.expect("status");
    assert_eq!(status.channel_id, "local");
    assert_eq!(status.account_id.as_deref(), Some("host"));
    assert!(
        status
            .states
            .iter()
            .any(|state| { *state == crate::controllers::ChannelAccountState::Configured })
    );
    assert!(adapter.static_capabilities().reply);
}

#[tokio::test]
async fn local_adapter_preserves_identity_and_ack_policy_across_stop() {
    let sink = Arc::new(RecordingOutboundSink::default());
    let adapter = local_adapter(sink.clone());
    assert_eq!(adapter.descriptor().id, "local");
    assert_eq!(
        adapter.receive_ack_policy(),
        crate::channel::ChannelReceiveAckPolicy::AfterReceiveRecord
    );
    adapter.stop().await.unwrap();
    assert_eq!(adapter.descriptor().account_id.as_deref(), Some("host"));
    assert!(sink.ids.lock().unwrap().is_empty());
}

#[test]
fn local_adapters_keep_per_instance_v2_facts_and_project_legacy_flags() {
    let rich = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        reactions: true,
        text: tinychannels_bus::TextCapabilities {
            text_limit: Some(8192),
            ..Default::default()
        },
        addressing: tinychannels_bus::AddressingCapabilities {
            conversation_types: vec!["group".into()],
            thread_id: true,
            ..Default::default()
        },
        actions: tinychannels_bus::ActionCapabilities {
            supported: vec![
                "send".into(),
                "reply".into(),
                "edit".into(),
                "delete".into(),
                "native-command".into(),
            ],
            ..Default::default()
        },
        content: tinychannels_bus::ContentCapabilities {
            image: tinychannels_bus::ContentCapability {
                outbound: true,
                ..Default::default()
            },
            poll: tinychannels_bus::ContentCapability {
                outbound: true,
                ..Default::default()
            },
            buttons: tinychannels_bus::ContentCapability {
                max_count: Some(12),
                ..Default::default()
            },
            button_presentation: tinychannels_bus::ButtonPresentationCapabilities {
                max_actions_per_row: Some(3),
                max_rows: Some(4),
                supports_styles: true,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };

    let limited = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        text: tinychannels_bus::TextCapabilities {
            text_limit: Some(512),
            ..Default::default()
        },
        actions: tinychannels_bus::ActionCapabilities {
            supported: vec!["send".into()],
            ..Default::default()
        },
        ..Default::default()
    };

    let sink = Arc::new(RecordingOutboundSink::default());
    let first = LocalChannelAdapter::new(
        ChannelDescriptor {
            id: "local-a".into(),
            account_id: Some("account-a".into()),
            ..Default::default()
        },
        rich.clone(),
        sink.clone(),
    );
    let second = LocalChannelAdapter::new(
        ChannelDescriptor {
            id: "local-b".into(),
            account_id: Some("account-b".into()),
            ..Default::default()
        },
        limited.clone(),
        sink,
    );

    assert_eq!(first.capabilities(), &rich);
    assert_eq!(second.capabilities(), &limited);
    assert_eq!(first.descriptor().account_id.as_deref(), Some("account-a"));
    assert_eq!(second.descriptor().account_id.as_deref(), Some("account-b"));

    let first_legacy = first.static_capabilities();
    assert_eq!(first_legacy.chat_types, ["group"]);
    assert!(first_legacy.polls && first_legacy.reactions);
    assert!(first_legacy.edit && first_legacy.unsend && first_legacy.reply);
    assert!(first_legacy.threads && first_legacy.media && first_legacy.native_commands);
    assert!(first_legacy.block_streaming);

    let second_legacy = second.static_capabilities();
    assert!(second_legacy.chat_types.is_empty());
    assert!(!second_legacy.polls && !second_legacy.reactions);
    assert!(!second_legacy.edit && !second_legacy.unsend && !second_legacy.reply);
    assert!(!second_legacy.threads && !second_legacy.media && !second_legacy.native_commands);
    assert!(second_legacy.block_streaming);
}
