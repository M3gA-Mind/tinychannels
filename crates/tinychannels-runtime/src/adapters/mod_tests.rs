use crate::adapters::{LocalChannelAdapter, LocalOutboundSink};
use crate::channel::{
    ChannelAdapter, ChannelDescriptor, ChannelInboundEnvelope, ChannelInboundSink,
    ChannelOutboundIntent, ChannelSendError, DeliveryDurability, MessageReceipt, OutboundPayload,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

#[test]
fn web_adapter_projects_output_with_turn_context_and_sequence() {
    let mut projector = crate::adapters::web::WebTurnProjector::new("client", "thread", "request");
    let events = projector.project(
        tinychannels_bus::module_progressive::ChannelOutputEvent::TextDelta {
            text: "hello".into(),
            iteration: Some(2),
        },
        123,
    );

    assert_eq!(events.len(), 1);
    let value = serde_json::to_value(&events[0]).expect("serialize web event");
    assert_eq!(value["event"], "text_delta");
    assert_eq!(value["client_id"], "client");
    assert_eq!(value["thread_id"], "thread");
    assert_eq!(value["request_id"], "request");
    assert_eq!(value["seq"], 1);
    assert_eq!(value["ts"], 123);
    assert_eq!(value["delta"], "hello");
    assert_eq!(value["round"], 2);
}

#[derive(Default)]
struct RecordingOutboundSink {
    ids: Mutex<Vec<String>>,
    intents: Mutex<Vec<ChannelOutboundIntent>>,
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
            .push(intent.idempotency_key.clone());
        self.intents.lock().expect("intents lock").push(intent);
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
    async fn push(&self, _envelope: ChannelInboundEnvelope) -> Result<(), ChannelSendError> {
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

    let receipt = adapter
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
        receipt.primary_platform_message_id.as_deref(),
        Some("local-1")
    );
    assert_eq!(
        sink.ids.lock().expect("ids lock").as_slice(),
        ["turn-1:text"]
    );
}

#[tokio::test]
async fn local_adapter_forwards_ordered_typed_content_without_loss() {
    let sink = Arc::new(RecordingOutboundSink::default());
    let capabilities = tinychannels_bus::ChannelCapabilities {
        content: tinychannels_bus::ContentCapabilities {
            image: tinychannels_bus::ContentCapability {
                outbound: true,
                ..Default::default()
            },
            ..Default::default()
        },
        voice_out: tinychannels_bus::VoiceCapabilities {
            supported: true,
            synthesis: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let adapter = LocalChannelAdapter::new(
        ChannelDescriptor {
            id: "local".into(),
            account_id: Some("host".into()),
            ..Default::default()
        },
        capabilities.clone(),
        sink.clone(),
    );
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "local".into(),
        conversation_id: "chat-1".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "local".into(),
            account_id: Some("host".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            id: "chat-1".into(),
            thread_id: Some("thread-2".into()),
            ..Default::default()
        }),
        reply_to_id: Some("message-1".into()),
        thread_id: Some("thread-2".into()),
        turn_id: Some("turn-3".into()),
        idempotency_key: "send-4".into(),
        parts: vec![
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Image {
                    media: tinychannels_bus::MediaReference {
                        url: Some("https://example.test/image.png".into()),
                        kind: tinychannels_bus::MediaKind::Image,
                        ..Default::default()
                    },
                    caption: Some("caption".into()),
                    alt_text: Some("alt".into()),
                },
            },
            tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Synthesize {
                    text: "spoken".into(),
                    voice: Some("calm".into()),
                },
            },
        ],
    };

    let outcome = adapter
        .send_typed(&message)
        .await
        .expect("local host accepts typed content");

    assert_eq!(adapter.declared_capabilities(), capabilities);
    assert!(outcome.error.is_none());
    let delivered = sink.intents.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].idempotency_key, "send-4");
    assert_eq!(delivered[0].channel_ref, message.channel_ref);
    assert_eq!(delivered[0].conversation_ref, message.conversation_ref);
    assert_eq!(delivered[0].reply_to_id.as_deref(), Some("message-1"));
    assert_eq!(delivered[0].thread_id.as_deref(), Some("thread-2"));
    assert_eq!(delivered[0].turn_id.as_deref(), Some("turn-3"));
    assert_eq!(
        delivered[0].payload,
        OutboundPayload::Parts {
            parts: message.parts
        }
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
    assert_eq!(
        adapter.declared_capabilities().actions.supported,
        vec!["reply"]
    );
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
