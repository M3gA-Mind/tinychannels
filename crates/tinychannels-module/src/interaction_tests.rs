use super::*;
use std::sync::Mutex;
use tinychannels::SendMessage;

struct LegacyInteractionChannel;

#[async_trait::async_trait]
impl tinychannels::channel::Channel for LegacyInteractionChannel {
    fn name(&self) -> &str {
        "legacy-fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[allow(deprecated)]
async fn legacy_interaction_boundary_handles_only_v21_operations() {
    let channel = LegacyInteractionChannel;
    for interaction in [
        ChannelInteraction::StartTyping {
            recipient: "room".into(),
        },
        ChannelInteraction::StopTyping {
            recipient: "room".into(),
        },
    ] {
        assert!(matches!(
            dispatch(&channel, interaction).await.unwrap(),
            ChannelInteractionResult::Completed
        ));
    }
    assert!(matches!(
        dispatch(
            &channel,
            ChannelInteraction::SendDraft {
                message: SendMessage::new("partial", "room"),
            },
        )
        .await
        .unwrap(),
        ChannelInteractionResult::Draft(None)
    ));
    assert!(matches!(
        dispatch(&channel, ChannelInteraction::Describe)
            .await
            .unwrap(),
        ChannelInteractionResult::Info(_)
    ));
    assert!(matches!(
        dispatch(&channel, ChannelInteraction::DescribeV2)
            .await
            .unwrap(),
        ChannelInteractionResult::InfoV2(_)
    ));
    assert!(
        dispatch(
            &channel,
            ChannelInteraction::UpdateDraft {
                recipient: "room".into(),
                message_id: "draft-1".into(),
                text: "updated".into(),
            },
        )
        .await
        .is_err()
    );
}

#[test]
#[allow(deprecated)]
fn compatibility_text_and_telegram_address_preserve_legacy_conventions() {
    let email = SendMessage::with_subject("body", "recipient", "  first\nsecond\rline  ");
    assert_eq!(
        compatibility_text("email:account", &email),
        "Subject: first second line\nbody"
    );
    assert_eq!(compatibility_text("slack", &email), "body");

    let message = SendMessage::new("body", "-100:topic-9").in_thread(Some("reply-4".into()));
    let mut intent = tinychannels::outbound_intent_from_send_message("telegram", &message);
    normalize_telegram_v1_address(&mut intent, &message);
    assert_eq!(intent.conversation_id, "-100");
    assert_eq!(
        intent
            .conversation_ref
            .as_ref()
            .unwrap()
            .topic_id
            .as_deref(),
        Some("topic-9")
    );
    assert_eq!(intent.reply_to_id.as_deref(), Some("reply-4"));
    assert_eq!(intent.thread_id, None);
}

struct NativeDescribeAdapter;

#[async_trait::async_trait]
impl tinychannels::channel::ChannelAdapter for NativeDescribeAdapter {
    fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        tinychannels_bus::channel::ChannelDescriptor {
            id: "fixture".into(),
            display_name: "Fixture".into(),
            // Raw provider descriptors may not know the configured account.
            account_id: None,
            ..Default::default()
        }
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        let mut capabilities = tinychannels_bus::ChannelCapabilities::unknown();
        capabilities.reactions = true;
        capabilities.streaming_reply_pipeline = true;
        capabilities
    }

    fn proactive_target(&self) -> Option<String> {
        Some("configured-native-room".into())
    }

    fn supports_webhook_delivery(&self) -> bool {
        true
    }

    async fn deliver_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
        match signature {
            Some("rejected") => Err(anyhow::Error::new(tinychannels::WebhookRejected(
                "bad signature".into(),
            ))),
            Some("failed") => anyhow::bail!("temporary provider failure"),
            _ => Ok(tinychannels_bus::WebhookDeliveryOutcome {
                messages: u32::from(body == b"payload"),
                ..Default::default()
            }),
        }
    }

    fn supports_webhook_verification(&self) -> bool {
        true
    }

    fn verify_webhook(&self, mode: &str, verify_token: &str, challenge: &str) -> Option<String> {
        (mode == "subscribe" && verify_token == "token").then(|| challenge.to_owned())
    }

    async fn start(
        &self,
        _: &(dyn tinychannels::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), tinychannels::channel::ChannelSendError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), tinychannels::channel::ChannelSendError> {
        Ok(())
    }

    async fn send(
        &self,
        _: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<tinychannels::channel::MessageReceipt, tinychannels::channel::ChannelSendError>
    {
        Ok(Default::default())
    }

    async fn status(
        &self,
    ) -> Result<
        tinychannels::controllers::ChannelAccountSnapshot,
        tinychannels::channel::ChannelSendError,
    > {
        Ok(Default::default())
    }
}

#[derive(Default)]
struct NativeDraftLifecyclePort(Mutex<Vec<String>>);

struct UnsupportedDraftPort;

#[async_trait::async_trait]
impl tinychannels::channel::ChannelStreamingDraft for UnsupportedDraftPort {}

struct FailedDraftPort;

#[async_trait::async_trait]
impl tinychannels::channel::ChannelStreamingDraft for FailedDraftPort {
    async fn start_draft(
        &self,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<Option<String>, tinychannels::channel::ChannelSendError> {
        Err(tinychannels::channel::ChannelSendError {
            message: "provider failed".into(),
            ..Default::default()
        })
    }

    async fn update_draft(
        &self,
        _draft_id: &str,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<(), tinychannels::channel::ChannelSendError> {
        Err(tinychannels::channel::ChannelSendError {
            message: "provider failed".into(),
            ..Default::default()
        })
    }

    async fn finalize_draft(
        &self,
        _draft_id: &str,
        _intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<(), tinychannels::channel::ChannelSendError> {
        Err(tinychannels::channel::ChannelSendError {
            message: "provider failed".into(),
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl tinychannels::channel::ChannelStreamingDraft for NativeDraftLifecyclePort {
    async fn start_draft(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<Option<String>, tinychannels::channel::ChannelSendError> {
        let tinychannels_bus::OutboundPayload::Text { text } = intent.payload else {
            panic!("draft start must use a typed text payload")
        };
        let channel = intent.channel_ref.unwrap();
        let conversation = intent.conversation_ref.unwrap();
        self.0.lock().unwrap().push(format!(
            "start:{}:{}:{}:{}:{text}",
            channel.account_id.unwrap(),
            conversation.scope_id.unwrap(),
            conversation.id,
            intent.conversation_id,
        ));
        Ok(Some("draft-id".into()))
    }

    async fn update_draft(
        &self,
        id: &str,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<(), tinychannels::channel::ChannelSendError> {
        let tinychannels_bus::OutboundPayload::Text { text } = intent.payload else {
            panic!("draft update must use a typed text payload")
        };
        self.0.lock().unwrap().push(format!("update:{id}:{text}"));
        Ok(())
    }

    async fn finalize_draft(
        &self,
        id: &str,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<(), tinychannels::channel::ChannelSendError> {
        let tinychannels_bus::OutboundPayload::Text { text } = intent.payload else {
            panic!("draft finalization must use a typed text payload")
        };
        self.0.lock().unwrap().push(format!("finalize:{id}:{text}"));
        Ok(())
    }

    async fn upsert_draft(
        &self,
        _: Option<&tinychannels_bus::channel::MessageReceipt>,
        _: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<tinychannels_bus::channel::MessageReceipt, tinychannels::channel::ChannelSendError>
    {
        unreachable!("legacy upsert is not used by the lifecycle shim")
    }
}

#[tokio::test]
async fn native_describe_uses_configured_adapter_facts_and_keeps_v1_projection() {
    let adapter = NativeDescribeAdapter;
    let v2 = describe_adapter(&adapter, false);
    let ChannelInteractionResult::InfoV2(v2) = v2 else {
        panic!("missing native v2 capabilities")
    };
    assert_eq!(v2.capabilities.account.configured, Some(true));
    assert_eq!(v2.capabilities.account.enabled, Some(true));
    assert_eq!(
        v2.capabilities.account.proactive_target.as_deref(),
        Some("configured-native-room")
    );
    assert!(v2.capabilities.reactions);

    let v1 = describe_adapter(&adapter, true);
    let wire = serde_json::to_value(v1).unwrap();
    assert_eq!(wire["result"], "info");
    assert_eq!(wire["value"]["proactive_target"], "configured-native-room");
    assert_eq!(wire["value"]["supports_reactions"], true);
    assert_eq!(wire["value"]["supports_draft_updates"], true);
}

#[tokio::test]
#[allow(deprecated)]
async fn native_draft_lifecycle_converts_v1_interactions_to_scoped_typed_intents() {
    let adapter = NativeDescribeAdapter;
    let drafts = NativeDraftLifecyclePort::default();
    let started = dispatch_native_draft(
        &adapter,
        &drafts,
        "fixture:configured",
        ChannelInteraction::SendDraft {
            message: SendMessage::new("partial", "room"),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(started, ChannelInteractionResult::Draft(Some(id)) if id == "draft-id"));

    let updated = dispatch_native_draft(
        &adapter,
        &drafts,
        "fixture:configured",
        ChannelInteraction::UpdateDraft {
            recipient: "room".into(),
            message_id: "draft-id".into(),
            text: "more".into(),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(updated, ChannelInteractionResult::Completed));

    let finalized = dispatch_native_draft(
        &adapter,
        &drafts,
        "fixture:configured",
        ChannelInteraction::FinalizeDraft {
            recipient: "room".into(),
            message_id: "draft-id".into(),
            text: "final".into(),
            thread_ts: Some("topic-1".into()),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(finalized, ChannelInteractionResult::Completed));
    assert_eq!(
        *drafts.0.lock().unwrap(),
        [
            "start:fixture:configured:fixture:configured:room:room:partial",
            "update:draft-id:more",
            "finalize:draft-id:final"
        ]
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn native_draft_dispatch_falls_back_only_for_unsupported_provider_operations() {
    let adapter = NativeDescribeAdapter;
    let drafts = UnsupportedDraftPort;
    for interaction in [
        ChannelInteraction::SendDraft {
            message: SendMessage::new("partial", "room"),
        },
        ChannelInteraction::UpdateDraft {
            recipient: "room".into(),
            message_id: "draft-1".into(),
            text: "updated".into(),
        },
        ChannelInteraction::FinalizeDraft {
            recipient: "room".into(),
            message_id: "draft-1".into(),
            text: "final".into(),
            thread_ts: None,
        },
    ] {
        assert!(
            dispatch_native_draft(&adapter, &drafts, "fixture:configured", interaction)
                .await
                .is_none()
        );
    }
}

#[tokio::test]
#[allow(deprecated)]
async fn native_draft_dispatch_keeps_provider_failures_as_errors() {
    let adapter = NativeDescribeAdapter;
    let drafts = FailedDraftPort;
    for interaction in [
        ChannelInteraction::SendDraft {
            message: SendMessage::new("partial", "room"),
        },
        ChannelInteraction::UpdateDraft {
            recipient: "room".into(),
            message_id: "draft-1".into(),
            text: "updated".into(),
        },
        ChannelInteraction::FinalizeDraft {
            recipient: "room".into(),
            message_id: "draft-1".into(),
            text: "final".into(),
            thread_ts: None,
        },
    ] {
        assert!(
            dispatch_native_draft(&adapter, &drafts, "fixture:configured", interaction)
                .await
                .expect("operation is supported")
                .is_err()
        );
    }
}

#[tokio::test]
async fn native_webhook_dispatch_preserves_rejection_and_delivery_error_classes() {
    let adapter = NativeDescribeAdapter;
    let verify = dispatch_native_webhook(
        &adapter,
        ChannelInteraction::VerifyWebhook {
            mode: "subscribe".into(),
            verify_token: "token".into(),
            challenge: "challenge".into(),
        },
    )
    .await
    .expect("verification is supported")
    .expect("valid token is accepted");
    assert!(
        matches!(verify, ChannelInteractionResult::WebhookChallenge(value) if value == "challenge")
    );

    let rejected = dispatch_native_webhook(
        &adapter,
        ChannelInteraction::VerifyWebhook {
            mode: "subscribe".into(),
            verify_token: "wrong".into(),
            challenge: "challenge".into(),
        },
    )
    .await
    .expect("verification is supported")
    .expect_err("invalid verification tokens are refused");
    assert!(format!("{rejected:?}").contains(WEBHOOK_REJECTED_ERROR));

    let delivered = dispatch_native_webhook(
        &adapter,
        ChannelInteraction::DeliverWebhook {
            signature: Some("valid".into()),
            body_base64: "cGF5bG9hZA==".into(),
        },
    )
    .await
    .expect("delivery is supported")
    .expect("valid payload is accepted");
    assert!(
        matches!(delivered, ChannelInteractionResult::WebhookAccepted(outcome) if outcome.messages == 1)
    );

    let invalid_encoding = dispatch_native_webhook(
        &adapter,
        ChannelInteraction::DeliverWebhook {
            signature: None,
            body_base64: "%%%".into(),
        },
    )
    .await
    .expect("delivery is supported")
    .expect_err("invalid base64 is rejected before provider delivery");
    assert!(format!("{invalid_encoding:?}").contains(WEBHOOK_REJECTED_ERROR));

    let provider_rejection = dispatch_native_webhook(
        &adapter,
        ChannelInteraction::DeliverWebhook {
            signature: Some("rejected".into()),
            body_base64: "eA==".into(),
        },
    )
    .await
    .expect("delivery is supported")
    .expect_err("provider-auth rejection remains a client error");
    assert!(format!("{provider_rejection:?}").contains(WEBHOOK_REJECTED_ERROR));

    let provider_failure = dispatch_native_webhook(
        &adapter,
        ChannelInteraction::DeliverWebhook {
            signature: Some("failed".into()),
            body_base64: "eA==".into(),
        },
    )
    .await
    .expect("delivery is supported")
    .expect_err("post-auth provider failures remain retryable");
    assert!(format!("{provider_failure:?}").contains(WEBHOOK_FAILED_ERROR));
}
