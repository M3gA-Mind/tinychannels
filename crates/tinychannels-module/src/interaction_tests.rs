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
