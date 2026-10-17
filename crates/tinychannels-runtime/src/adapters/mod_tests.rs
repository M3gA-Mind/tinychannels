use crate::adapters::{LocalChannelAdapter, LocalOutboundSink};
use crate::channel::{
    ChannelAdapter, ChannelDescriptor, ChannelInboundSink, ChannelOutboundIntent, ChannelSendError,
    DeliveryDurability, MessageReceipt, OutboundPayload,
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

struct FixedOutboundSink(Result<MessageReceipt, ChannelSendError>);

#[async_trait]
impl LocalOutboundSink for FixedOutboundSink {
    async fn send(
        &self,
        _intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.0.clone()
    }
}

#[derive(Default)]
struct IntentRecordingSink(Mutex<Vec<ChannelOutboundIntent>>);

#[async_trait]
impl LocalOutboundSink for IntentRecordingSink {
    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.0.lock().expect("intents lock").push(intent);
        Ok(MessageReceipt {
            primary_platform_message_id: Some("sent".into()),
            ..Default::default()
        })
    }
}

struct NoopInboundSink;

#[async_trait]
impl ChannelInboundSink for NoopInboundSink {
    async fn push(
        &self,
        _ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), ChannelSendError> {
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
            content: tinychannels_bus::ContentCapabilities {
                text: tinychannels_bus::ContentCapability {
                    outbound: true,
                    ..Default::default()
                },
                ..Default::default()
            },
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
    assert_eq!(
        outcome.receipt.channel,
        Some(tinychannels_bus::ChannelRef {
            id: "local".into(),
            account_id: Some("host".into()),
        })
    );
    assert_eq!(
        outcome
            .receipt
            .conversation
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some("chat-1")
    );
}

#[tokio::test]
async fn local_adapter_rejects_foreign_scope_before_dispatch() {
    let sink = Arc::new(RecordingOutboundSink::default());
    let adapter = local_adapter(sink.clone());
    let error = adapter
        .send(ChannelOutboundIntent {
            idempotency_key: "wrong-account".into(),
            channel_id: "local".into(),
            conversation_id: "chat-1".into(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "local".into(),
                account_id: Some("foreign".into()),
            }),
            conversation_ref: None,
            reply_to_id: None,
            thread_id: None,
            turn_id: None,
            durability: DeliveryDurability::BestEffort,
            payload: OutboundPayload::Text {
                text: "nope".into(),
            },
        })
        .await
        .expect_err("foreign account is rejected");
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Forbidden
    );
    assert!(sink.ids.lock().expect("ids lock").is_empty());

    let error = adapter
        .send(ChannelOutboundIntent {
            idempotency_key: "wrong-conversation".into(),
            channel_id: "local".into(),
            conversation_id: "chat-1".into(),
            channel_ref: None,
            conversation_ref: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Group,
                id: "different-chat".into(),
                ..Default::default()
            }),
            reply_to_id: None,
            thread_id: None,
            turn_id: None,
            durability: DeliveryDurability::BestEffort,
            payload: OutboundPayload::Text {
                text: "nope".into(),
            },
        })
        .await
        .expect_err("conflicting conversation address is rejected");
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Forbidden
    );
    assert!(sink.ids.lock().expect("ids lock").is_empty());
}

#[tokio::test]
async fn local_adapter_capability_gate_covers_canonical_payload_kinds() {
    let outbound = tinychannels_bus::ContentCapability {
        outbound: true,
        ..Default::default()
    };
    let capabilities = tinychannels_bus::ChannelCapabilities {
        content: tinychannels_bus::ContentCapabilities {
            text: outbound.clone(),
            image: outbound.clone(),
            video: outbound.clone(),
            audio: outbound.clone(),
            file: outbound.clone(),
            sticker: outbound.clone(),
            location: outbound.clone(),
            contact: outbound.clone(),
            buttons: outbound.clone(),
            poll: outbound.clone(),
            presentation: outbound.clone(),
            native: outbound,
            ..Default::default()
        },
        voice_out: tinychannels_bus::VoiceCapabilities {
            supported: true,
            synthesis: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let sink = Arc::new(RecordingOutboundSink::default());
    let adapter = LocalChannelAdapter::new(
        ChannelDescriptor {
            id: "local".into(),
            account_id: Some("host".into()),
            ..Default::default()
        },
        capabilities,
        sink.clone(),
    );
    let media = tinychannels_bus::MediaReference::default();
    let content = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "text".into(),
                markdown_dialect: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: media.clone(),
                caption: None,
                alt_text: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Video {
                media: media.clone(),
                caption: None,
                duration_ms: None,
                thumbnail: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Audio {
                media: media.clone(),
                codec: None,
                duration_ms: None,
                title: None,
                transcript: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::File {
                media: media.clone(),
                file_name: None,
                caption: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::VoiceNote {
                voice_note: tinychannels_bus::VoiceNote {
                    media: media.clone(),
                    ..Default::default()
                },
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Sticker {
                media: media.clone(),
                pack_id: None,
                sticker_id: None,
                emoji: None,
                alt_text: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Location {
                latitude: 0.0,
                longitude: 0.0,
                label: None,
                address: None,
                accuracy_meters: None,
                live_until_unix_ms: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Contact {
                display_name: "person".into(),
                phone_numbers: vec![],
                emails: vec![],
                organization: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Buttons { rows: vec![] },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Poll {
                question: "Q?".into(),
                options: vec!["A".into()],
                multiple_choice: false,
                closes_at_unix_ms: None,
                provider_poll_id: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Presentation {
                blocks: serde_json::json!([]),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Native {
                provider: "local".into(),
                data: serde_json::json!({}),
            },
        },
        tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: media.clone(),
                audio_base64: None,
                codec: None,
                duration_ms: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Synthesize {
                text: "voice".into(),
                voice: None,
            },
        },
    ];
    let result = adapter
        .send(ChannelOutboundIntent {
            idempotency_key: "all-parts".into(),
            channel_id: "local".into(),
            conversation_id: "chat-1".into(),
            payload: OutboundPayload::Parts { parts: content },
            ..Default::default()
        })
        .await;
    assert!(result.is_ok());
    assert_eq!(sink.ids.lock().unwrap().as_slice(), ["all-parts"]);

    for payload in [
        OutboundPayload::Text { text: "x".into() },
        OutboundPayload::Media {
            text: None,
            media_urls: vec![],
        },
        OutboundPayload::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media,
                audio_base64: None,
                codec: None,
                duration_ms: None,
            },
        },
        OutboundPayload::Files { file_urls: vec![] },
        OutboundPayload::Poll {
            question: "Q?".into(),
            options: vec![],
        },
        OutboundPayload::PresentationBlocks {
            blocks: serde_json::json!([]),
        },
        OutboundPayload::NativeChannelData {
            data: serde_json::json!({}),
        },
    ] {
        assert!(adapter.send(ChannelOutboundIntent {
            idempotency_key: "top-level".into(),
            channel_id: "local".into(),
            conversation_id: "chat-1".into(),
            payload,
            ..Default::default()
        }).await.is_ok());
    }
}

#[tokio::test]
async fn local_adapter_rejects_unadvertised_payloads_without_calling_sink() {
    let sink = Arc::new(RecordingOutboundSink::default());
    let adapter = LocalChannelAdapter::new(
        ChannelDescriptor {
            id: "local".into(),
            account_id: Some("host".into()),
            ..Default::default()
        },
        tinychannels_bus::ChannelCapabilities::default(),
        sink.clone(),
    );
    let media = tinychannels_bus::MediaReference::default();
    let payloads = [
        OutboundPayload::Text { text: "text".into() },
        OutboundPayload::Media { text: None, media_urls: vec![] },
        OutboundPayload::Voice { voice: tinychannels_bus::OutboundVoice::Media {
            media: media.clone(), audio_base64: None, codec: None, duration_ms: None,
        } },
        OutboundPayload::Voice { voice: tinychannels_bus::OutboundVoice::Synthesize {
            text: "speech".into(), voice: None,
        } },
        OutboundPayload::Files { file_urls: vec![] },
        OutboundPayload::Poll { question: "Q?".into(), options: vec![] },
        OutboundPayload::PresentationBlocks { blocks: serde_json::json!([]) },
        OutboundPayload::NativeChannelData { data: serde_json::json!({}) },
    ];

    for payload in payloads {
        let error = adapter.send(ChannelOutboundIntent {
            idempotency_key: "unsupported".into(),
            channel_id: "local".into(),
            conversation_id: "chat".into(),
            payload,
            ..Default::default()
        }).await.unwrap_err();
        assert_eq!(error.kind, tinychannels_bus::channel::SendErrorKind::BadFormat);
        assert!(error.unsupported);
        assert!(!error.retryable && !error.sent_before_error);
    }
    assert!(sink.ids.lock().expect("ids lock").is_empty());
}

#[tokio::test]
async fn local_adapter_forwards_typed_turn_and_delivery_scope_unchanged() {
    let sink = Arc::new(IntentRecordingSink::default());
    let adapter = LocalChannelAdapter::new(
        ChannelDescriptor {
            id: "local".into(),
            account_id: Some("host".into()),
            ..Default::default()
        },
        tinychannels_bus::ChannelCapabilities {
            content: tinychannels_bus::ContentCapabilities {
                text: tinychannels_bus::ContentCapability { outbound: true, ..Default::default() },
                ..Default::default()
            },
            ..Default::default()
        },
        sink.clone(),
    );
    let expected = ChannelOutboundIntent {
        idempotency_key: "operation-9".into(),
        channel_id: "local".into(),
        conversation_id: "room-4".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef { id: "local".into(), account_id: Some("host".into()) }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "room-4".into(),
            thread_id: Some("thread-7".into()),
            ..Default::default()
        }),
        reply_to_id: Some("message-3".into()),
        thread_id: Some("thread-7".into()),
        turn_id: Some("turn-2".into()),
        durability: DeliveryDurability::Durable,
        payload: OutboundPayload::Text { text: "hello".into() },
    };

    let outcome = adapter.send(expected.clone()).await.expect("send");
    assert_eq!(sink.0.lock().expect("intents lock").as_slice(), &[expected]);
    assert_eq!(outcome.receipt.idempotency_key.as_deref(), Some("operation-9"));
    assert!(outcome.error.is_none());
}

#[tokio::test]
async fn local_adapter_normalizes_missing_receipt_scope_and_preserves_foreign_scope() {
    let descriptor = ChannelDescriptor {
        id: "local".into(),
        account_id: Some("host".into()),
        ..Default::default()
    };
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.content.text.outbound = true;
    let intent = ChannelOutboundIntent {
        idempotency_key: "operation".into(),
        channel_id: "local".into(),
        conversation_id: "room".into(),
        thread_id: Some("topic".into()),
        payload: OutboundPayload::Text { text: "hello".into() },
        ..Default::default()
    };
    let adapter = LocalChannelAdapter::new(
        descriptor.clone(),
        capabilities.clone(),
        Arc::new(FixedOutboundSink(Ok(MessageReceipt::default()))),
    );
    let outcome = adapter.send(intent.clone()).await.expect("send");
    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.idempotency_key.as_deref(), Some("operation"));
    assert_eq!(outcome.receipt.channel, Some(tinychannels_bus::ChannelRef {
        id: "local".into(), account_id: Some("host".into()),
    }));
    assert_eq!(outcome.receipt.conversation, Some(tinychannels_bus::ConversationRef {
        id: "room".into(), thread_id: Some("topic".into()), ..Default::default()
    }));

    let adapter = LocalChannelAdapter::new(
        descriptor,
        capabilities,
        Arc::new(FixedOutboundSink(Ok(MessageReceipt {
            channel: Some(tinychannels_bus::ChannelRef {
                id: "local".into(), account_id: Some("other".into()),
            }),
            conversation: Some(tinychannels_bus::ConversationRef {
                id: "other-room".into(), ..Default::default()
            }),
            primary_platform_message_id: Some("provider-sent".into()),
            ..Default::default()
        }))),
    );
    let outcome = adapter.send(intent).await.expect("partial scope is an outcome");
    let error = outcome.error.expect("foreign provider scope is uncertain");
    assert_eq!(error.kind, tinychannels_bus::channel::SendErrorKind::Unknown);
    assert!(error.sent_before_error && !error.retryable && !error.unsupported);
    assert_eq!(outcome.receipt.channel.unwrap().account_id.as_deref(), Some("other"));
    assert_eq!(outcome.receipt.conversation.unwrap().id, "other-room");
}

#[tokio::test]
async fn local_adapter_marks_each_partial_effect_signal_non_retryable() {
    let descriptor = ChannelDescriptor { id: "local".into(), ..Default::default() };
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.content.text.outbound = true;
    let intent = ChannelOutboundIntent {
        idempotency_key: "operation".into(),
        channel_id: "local".into(),
        conversation_id: "room".into(),
        payload: OutboundPayload::Text { text: "hello".into() },
        ..Default::default()
    };
    let partial_errors = [
        ChannelSendError { kind: tinychannels_bus::channel::SendErrorKind::Unknown, retryable: true, ..Default::default() },
        ChannelSendError { kind: tinychannels_bus::channel::SendErrorKind::Transient, sent_before_error: true, retryable: true, unsupported: true, ..Default::default() },
        ChannelSendError { continuation_message_ids: vec!["part-2".into()], retryable: true, ..Default::default() },
        ChannelSendError { partial_overflow: Some(serde_json::json!({"sent": 1})), retryable: true, unsupported: true, ..Default::default() },
    ];
    for partial in partial_errors {
        let adapter = LocalChannelAdapter::new(
            descriptor.clone(),
            capabilities.clone(),
            Arc::new(FixedOutboundSink(Err(partial))),
        );
        let error = adapter.send(intent.clone()).await.unwrap_err();
        assert!(error.sent_before_error);
        assert!(!error.retryable && !error.unsupported);
    }
}

#[tokio::test]
async fn local_adapter_preserves_uncertain_sink_errors_and_rejects_mis_scoped_receipts() {
    let descriptor = ChannelDescriptor {
        id: "local".into(),
        account_id: Some("host".into()),
        ..Default::default()
    };
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.content.text.outbound = true;
    let intent = ChannelOutboundIntent {
        idempotency_key: "uncertain".into(),
        channel_id: "local".into(),
        conversation_id: "chat-1".into(),
        payload: OutboundPayload::Text { text: "text".into() },
        ..Default::default()
    };
    let adapter = LocalChannelAdapter::new(
        descriptor.clone(),
        capabilities.clone(),
        Arc::new(FixedOutboundSink(Err(ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Unknown,
            retryable: true,
            continuation_message_ids: vec!["second-part".into()],
            ..Default::default()
        }))),
    );
    let error = adapter.send(intent.clone()).await.unwrap_err();
    assert!(error.sent_before_error && !error.retryable);
    assert_eq!(error.continuation_message_ids, ["second-part"]);

    let adapter = LocalChannelAdapter::new(
        descriptor,
        capabilities,
        Arc::new(FixedOutboundSink(Ok(MessageReceipt {
            channel: Some(tinychannels_bus::ChannelRef {
                id: "local".into(),
                account_id: Some("foreign".into()),
            }),
            conversation: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "elsewhere".into(),
                ..Default::default()
            }),
            primary_platform_message_id: Some("sent".into()),
            platform_message_ids: vec!["sent".into()],
            parts: vec![tinychannels_bus::channel::MessageReceiptPart {
                platform_message_id: "sent".into(),
                ..Default::default()
            }],
            ..Default::default()
        }))),
    );
    let outcome = adapter.send(intent).await.unwrap();
    assert!(outcome.error.as_ref().is_some_and(|error| {
        error.sent_before_error && !error.retryable && !error.unsupported
    }));
    assert_eq!(
        outcome.receipt.channel.unwrap().account_id.as_deref(),
        Some("foreign")
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
