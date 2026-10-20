use super::*;
use std::sync::{Arc, Mutex};
use tinychannels::{ChannelMessage, SendMessage};

#[derive(Default)]
struct DraftProvider(Mutex<Vec<String>>);
#[async_trait::async_trait]
impl Channel for DraftProvider {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }
    fn proactive_target(&self) -> Option<String> {
        Some("configured-room".into())
    }
    fn supports_reactions(&self) -> bool {
        true
    }
    fn supports_draft_updates(&self) -> bool {
        true
    }
    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("typing:{recipient}"));
        Ok(())
    }
    async fn stop_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("stop:{recipient}"));
        Ok(())
    }
    async fn send_draft(&self, message: &SendMessage) -> anyhow::Result<Option<String>> {
        self.0
            .lock()
            .unwrap()
            .push(format!("draft:{}:{}", message.recipient, message.content));
        Ok(Some("provider-id".into()))
    }
    async fn update_draft(&self, recipient: &str, id: &str, text: &str) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("update:{recipient}:{id}:{text}"));
        Ok(())
    }
    async fn finalize_draft(
        &self,
        recipient: &str,
        id: &str,
        text: &str,
        thread: Option<&str>,
    ) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!(
            "final:{recipient}:{id}:{text}:{}",
            thread.unwrap_or_default()
        ));
        Ok(())
    }

    async fn send_reaction(
        &self,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!(
            "reaction:{}:{provider_message_id}:{}:{remove}",
            conversation.id, reaction.value
        ));
        anyhow::ensure!(reaction.value != "fail", "fixture reaction failure");
        Ok(())
    }

    async fn edit_message(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        intent: &tinychannels_bus::channel::ChannelOutboundIntent,
    ) -> anyhow::Result<tinychannels_bus::channel::MessageReceipt> {
        self.0
            .lock()
            .unwrap()
            .push(format!("edit:{}", intent.idempotency_key));
        anyhow::ensure!(
            receipt.primary_platform_message_id.as_deref() != Some("fail"),
            "fixture edit failure"
        );
        Ok(receipt.clone())
    }

    async fn delete_message(
        &self,
        receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!(
            "delete:{}",
            receipt
                .primary_platform_message_id
                .as_deref()
                .unwrap_or_default()
        ));
        anyhow::ensure!(
            receipt.primary_platform_message_id.as_deref() != Some("fail"),
            "fixture delete failure"
        );
        Ok(())
    }
}

struct NoDraftProvider;

#[async_trait::async_trait]
impl Channel for NoDraftProvider {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn interaction_dispatch_preserves_provider_draft_and_typing_behavior() {
    let provider = Arc::new(DraftProvider::default());
    let info = dispatch(provider.as_ref(), ChannelInteraction::DescribeV2)
        .await
        .unwrap();
    let ChannelInteractionResult::InfoV2(info) = info else {
        panic!("missing provider facts")
    };
    assert_eq!(
        info.capabilities.account.proactive_target.as_deref(),
        Some("configured-room")
    );
    assert!(info.capabilities.reactions && info.capabilities.progressive_edits);
    dispatch(
        provider.as_ref(),
        ChannelInteraction::StartTyping {
            recipient: "room".into(),
        },
    )
    .await
    .unwrap();
    let draft = dispatch(
        provider.as_ref(),
        ChannelInteraction::SendDraft {
            message: SendMessage::new("partial", "room"),
        },
    )
    .await
    .unwrap();
    assert!(matches!(draft, ChannelInteractionResult::Draft(Some(id)) if id == "provider-id"));
    dispatch(
        provider.as_ref(),
        ChannelInteraction::UpdateDraft {
            recipient: "room".into(),
            message_id: "provider-id".into(),
            text: "more".into(),
        },
    )
    .await
    .unwrap();
    dispatch(
        provider.as_ref(),
        ChannelInteraction::FinalizeDraft {
            recipient: "room".into(),
            message_id: "provider-id".into(),
            text: "complete".into(),
            thread_ts: Some("topic".into()),
        },
    )
    .await
    .unwrap();
    dispatch(
        provider.as_ref(),
        ChannelInteraction::StopTyping {
            recipient: "room".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        *provider.0.lock().unwrap(),
        [
            "typing:room",
            "draft:room:partial",
            "update:room:provider-id:more",
            "final:room:provider-id:complete:topic",
            "stop:room"
        ]
    );
}

#[tokio::test]
async fn describe_preserves_the_pre_v2_wire_projection_for_existing_hosts() {
    let result = dispatch(&DraftProvider::default(), ChannelInteraction::Describe)
        .await
        .unwrap();
    let wire = serde_json::to_value(result).unwrap();
    assert_eq!(wire["result"], "info");
    assert_eq!(wire["value"]["proactive_target"], "configured-room");
    assert_eq!(wire["value"]["supports_reactions"], true);
    assert_eq!(wire["value"]["supports_draft_updates"], true);
    assert_eq!(
        wire["value"]["capabilities"],
        serde_json::json!({
            "remote_control": false,
            "chat_approvals": false,
            "progressive_edits": true,
            "history_key_ignores_thread": false
        })
    );
}

#[tokio::test]
async fn describe_reports_capabilities_for_each_configured_instance() {
    let draft = dispatch(&DraftProvider::default(), ChannelInteraction::DescribeV2)
        .await
        .unwrap();
    let no_draft = dispatch(&NoDraftProvider, ChannelInteraction::DescribeV2)
        .await
        .unwrap();
    let ChannelInteractionResult::InfoV2(draft) = draft else {
        panic!("missing draft facts")
    };
    let ChannelInteractionResult::InfoV2(no_draft) = no_draft else {
        panic!("missing no-draft facts")
    };
    assert!(draft.capabilities.progressive_edits);
    assert!(!no_draft.capabilities.progressive_edits);
    assert!(draft.capabilities.content.text.outbound);
    assert!(no_draft.capabilities.content.text.outbound);
}

#[tokio::test]
async fn typing_refresh_requires_a_running_provider_owner() {
    let provider = DraftProvider::default();
    for operation in [
        ChannelInteraction::StartTypingRefresh {
            recipient: "room".into(),
            interval_secs: 2,
        },
        ChannelInteraction::StopTypingRefresh {
            handle: "handle".into(),
        },
    ] {
        let error = dispatch(&provider, operation).await.unwrap_err();
        assert!(error.to_string().contains("typing refresh must be owned"));
    }
    assert!(provider.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn native_operation_interactions_preserve_results_and_map_failures() {
    let provider = DraftProvider::default();
    let conversation = tinychannels_bus::channel::ConversationRef {
        id: "room".into(),
        ..Default::default()
    };
    let reaction = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        ..Default::default()
    };
    let receipt = tinychannels_bus::channel::MessageReceipt {
        primary_platform_message_id: Some("provider-id".into()),
        ..Default::default()
    };
    let intent = tinychannels_bus::channel::ChannelOutboundIntent {
        idempotency_key: "edit-key".into(),
        ..Default::default()
    };

    assert!(matches!(
        dispatch(
            &provider,
            ChannelInteraction::SendReaction {
                channel: tinychannels_bus::ChannelRef {
                    id: "fixture".into(),
                    account_id: None
                },
                conversation: conversation.clone(),
                provider_message_id: "provider-id".into(),
                reaction: reaction.clone(),
                remove: false,
            }
        )
        .await
        .unwrap(),
        ChannelInteractionResult::Completed
    ));
    assert!(matches!(
        dispatch(&provider, ChannelInteraction::EditMessage { receipt: receipt.clone(), intent }).await.unwrap(),
        ChannelInteractionResult::Sent(outcome) if outcome.receipt == receipt && outcome.error.is_none()
    ));
    assert!(matches!(
        dispatch(
            &provider,
            ChannelInteraction::DeleteMessage {
                receipt: receipt.clone()
            }
        )
        .await
        .unwrap(),
        ChannelInteractionResult::Completed
    ));
    assert!(
        dispatch(
            &provider,
            ChannelInteraction::UpsertDraft {
                previous: None,
                intent: tinychannels_bus::ChannelOutboundIntent::default(),
            }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("configured provider queue")
    );

    let failed_reaction = dispatch(
        &provider,
        ChannelInteraction::SendReaction {
            channel: tinychannels_bus::ChannelRef {
                id: "fixture".into(),
                account_id: None,
            },
            conversation,
            provider_message_id: "provider-id".into(),
            reaction: tinychannels_bus::channel::ChannelReaction {
                value: "fail".into(),
                ..Default::default()
            },
            remove: false,
        },
    )
    .await
    .unwrap_err();
    assert!(
        failed_reaction
            .to_string()
            .contains("provider reaction failed")
    );
    let failed_receipt = tinychannels_bus::channel::MessageReceipt {
        primary_platform_message_id: Some("fail".into()),
        ..Default::default()
    };
    assert!(
        dispatch(
            &provider,
            ChannelInteraction::EditMessage {
                receipt: failed_receipt.clone(),
                intent: tinychannels_bus::ChannelOutboundIntent::default(),
            }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("provider message edit failed")
    );
    assert!(
        dispatch(
            &provider,
            ChannelInteraction::DeleteMessage {
                receipt: failed_receipt
            }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("provider message deletion failed")
    );
}

// ---------------------------------------------------------------------------
// Webhook ingress and read receipts
// ---------------------------------------------------------------------------

#[derive(Default)]
struct WebhookProvider(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl Channel for WebhookProvider {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }
    async fn mark_read(
        &self,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
    ) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("read:{}:{provider_message_id}", conversation.id));
        anyhow::ensure!(provider_message_id != "fail", "fixture read failure");
        Ok(())
    }
    async fn deliver_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
        self.0.lock().unwrap().push(format!(
            "webhook:{}:{}",
            signature.unwrap_or("-"),
            String::from_utf8_lossy(body)
        ));
        match body {
            b"refuse" => Err(anyhow::Error::new(tinychannels::WebhookRejected(
                "bad signature".into(),
            ))),
            b"explode" => anyhow::bail!("host unavailable"),
            _ => Ok(tinychannels_bus::WebhookDeliveryOutcome {
                messages: 2,
                events: 1,
                duplicates: 3,
            }),
        }
    }
    fn verify_webhook(&self, mode: &str, token: &str, challenge: &str) -> Option<String> {
        (mode == "subscribe" && token == "right").then(|| challenge.to_owned())
    }
}

fn webhook_body(body: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(body)
}

fn failure_name(error: tinybus::Error) -> String {
    match error {
        tinybus::Error::MethodFailed { name, .. } => name,
        other => panic!("unexpected error {other:?}"),
    }
}

#[tokio::test]
async fn webhook_deliveries_pass_the_exact_bytes_and_report_the_outcome() {
    let provider = Arc::new(WebhookProvider::default());
    let result = dispatch(
        provider.as_ref(),
        ChannelInteraction::DeliverWebhook {
            signature: Some("sha256=abc".into()),
            body_base64: webhook_body(b"{\"exact\": 1}"),
        },
    )
    .await
    .unwrap();
    let ChannelInteractionResult::WebhookAccepted(outcome) = result else {
        panic!("expected an accepted delivery")
    };
    assert_eq!(
        (outcome.messages, outcome.events, outcome.duplicates),
        (2, 1, 3)
    );
    assert_eq!(
        *provider.0.lock().unwrap(),
        ["webhook:sha256=abc:{\"exact\": 1}"]
    );
    dispatch(
        provider.as_ref(),
        ChannelInteraction::DeliverWebhook {
            signature: None,
            body_base64: webhook_body(b"{}"),
        },
    )
    .await
    .unwrap();
    assert!(provider.0.lock().unwrap()[1].starts_with("webhook:-:"));
}

#[tokio::test]
async fn refused_webhooks_are_client_errors_and_ingest_failures_are_retryable_ones() {
    let provider = Arc::new(WebhookProvider::default());
    let deliver = |body: String| {
        dispatch(
            provider.as_ref(),
            ChannelInteraction::DeliverWebhook {
                signature: None,
                body_base64: body,
            },
        )
    };
    let refused = deliver(webhook_body(b"refuse")).await.unwrap_err();
    assert_eq!(failure_name(refused), WEBHOOK_REJECTED_ERROR);
    let failed = deliver(webhook_body(b"explode")).await.unwrap_err();
    assert_eq!(failure_name(failed), WEBHOOK_FAILED_ERROR);
    // Malformed and oversized envelopes never reach the provider.
    let before = provider.0.lock().unwrap().len();
    let garbage = deliver("!!! not base64 !!!".into()).await.unwrap_err();
    assert_eq!(failure_name(garbage), WEBHOOK_REJECTED_ERROR);
    let oversized = deliver("A".repeat(4 * 1024 * 1024 * 4 / 3 + 8))
        .await
        .unwrap_err();
    assert_eq!(failure_name(oversized), WEBHOOK_REJECTED_ERROR);
    assert_eq!(provider.0.lock().unwrap().len(), before);
}

#[tokio::test]
async fn subscription_handshakes_echo_the_challenge_only_for_the_right_token() {
    let provider = WebhookProvider::default();
    let verify = |token: &str| {
        dispatch(
            &provider,
            ChannelInteraction::VerifyWebhook {
                mode: "subscribe".into(),
                verify_token: token.into(),
                challenge: "12345".into(),
            },
        )
    };
    assert!(matches!(
        verify("right").await.unwrap(),
        ChannelInteractionResult::WebhookChallenge(challenge) if challenge == "12345"
    ));
    assert_eq!(
        failure_name(verify("wrong").await.unwrap_err()),
        WEBHOOK_REJECTED_ERROR
    );
}

#[tokio::test]
async fn read_receipts_run_on_the_provider_and_surface_failure() {
    let provider = WebhookProvider::default();
    let read = |id: &str| {
        dispatch(
            &provider,
            ChannelInteraction::MarkRead {
                channel: tinychannels_bus::channel::ChannelRef {
                    id: "fixture".into(),
                    account_id: None,
                },
                conversation: tinychannels_bus::channel::ConversationRef {
                    id: "room".into(),
                    ..Default::default()
                },
                provider_message_id: id.into(),
            },
        )
    };
    assert!(matches!(
        read("m1").await.unwrap(),
        ChannelInteractionResult::Completed
    ));
    assert!(read("fail").await.is_err());
    assert_eq!(
        *provider.0.lock().unwrap(),
        ["read:room:m1", "read:room:fail"]
    );
}
