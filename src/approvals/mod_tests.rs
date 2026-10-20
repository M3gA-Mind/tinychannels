//! Approval surface against a recording native adapter.

use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use tinychannels_bus::channel::{
    ChannelDescriptor, ChannelOutboundIntent, ChannelSendError, ChannelSendOutcome, SendErrorKind,
};

use super::*;
use crate::channel::ChannelInboundSink;

struct RecordingAdapter {
    name: String,
    fail: bool,
    sent: StdMutex<Vec<ChannelOutboundIntent>>,
}

impl RecordingAdapter {
    fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            fail: false,
            sent: StdMutex::new(Vec::new()),
        })
    }

    fn sent(&self) -> Vec<ChannelOutboundIntent> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChannelAdapter for RecordingAdapter {
    fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor {
            id: self.name.clone(),
            account_id: Some(format!("{}/account", self.name)),
            ..Default::default()
        }
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        tinychannels_bus::ChannelCapabilities {
            chat_approvals: self.name != "email",
            ..Default::default()
        }
    }

    async fn start(
        &self,
        _sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        if self.fail {
            return Err(ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: "send refused".into(),
                ..Default::default()
            });
        }
        self.sent.lock().unwrap().push(intent);
        Ok(ChannelSendOutcome::default())
    }

    async fn status(
        &self,
    ) -> Result<tinychannels_bus::controllers::ChannelAccountSnapshot, ChannelSendError> {
        Ok(tinychannels_bus::controllers::ChannelAccountSnapshot {
            channel_id: self.name.clone(),
            ..Default::default()
        })
    }
}

fn surface(channels: &[Arc<RecordingAdapter>]) -> ApprovalSurface {
    let map: HashMap<String, Arc<dyn ChannelAdapter>> = channels
        .iter()
        .map(|channel| {
            (
                channel.name.clone(),
                Arc::clone(channel) as Arc<dyn ChannelAdapter>,
            )
        })
        .collect();
    ApprovalSurface::new(Arc::new(map))
}

fn prompt(channel: &str, thread_id: &str) -> ApprovalPrompt {
    ApprovalPrompt {
        request_id: "req-1".into(),
        tool_name: "file_write".into(),
        action_summary: "Write notes/today.md (1.2 KiB)".into(),
        thread_id: thread_id.into(),
        channel: channel.into(),
    }
}

#[test]
fn approval_prompt_includes_action_and_reply_instructions() {
    let body = format_approval_prompt("git_operations", "git commit -m fix");
    assert!(body.contains("git_operations") && body.contains("git commit"));
    assert!(body.contains("yes") && body.contains("no"));
}

#[tokio::test]
async fn every_chat_provider_gets_its_prompt_in_the_originating_chat() {
    for (channel, thread_ts, key) in [
        ("telegram", Some("topic-1"), "telegram_alice_chat-42"),
        ("discord", None, "discord_alice_chat-42"),
        ("slack", Some("1700.1"), "slack_alice_chat-42_thread:1700.1"),
    ] {
        let adapter = RecordingAdapter::new(channel);
        let approval = surface(&[Arc::clone(&adapter)]);
        approval.record_inbound(channel, "alice", "chat-42", thread_ts);
        assert!(
            matches!(
                approval.surface(&prompt(channel, key)).await,
                SurfaceOutcome::Sent
            ),
            "{channel}"
        );
        let sent = adapter.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].conversation_id, "chat-42");
        assert_eq!(sent[0].thread_id.as_deref(), thread_ts);
        assert_eq!(
            sent[0]
                .conversation_ref
                .as_ref()
                .and_then(|conversation| conversation.thread_id.as_deref()),
            thread_ts
        );
        assert_eq!(
            sent[0]
                .channel_ref
                .as_ref()
                .and_then(|channel| channel.account_id.as_deref()),
            Some(format!("{channel}/account").as_str())
        );
        assert!(matches!(
            &sent[0].payload,
            tinychannels_bus::OutboundPayload::Text { text } if text.contains("file_write")
        ));
        assert_eq!(sent[0].idempotency_key, "approval:req-1");
    }
}

#[tokio::test]
async fn telegram_topics_share_one_conversation_key() {
    let adapter = RecordingAdapter::new("telegram");
    let approval = surface(&[Arc::clone(&adapter)]);
    approval.record_inbound("telegram", "alice", "chat-1", Some("topic-9"));
    let ctx = approval
        .reply_context("telegram_alice_chat-1")
        .expect("recorded");
    assert_eq!(ctx.thread_ts.as_deref(), Some("topic-9"));
}

#[tokio::test]
async fn channels_without_chat_approvals_are_ignored() {
    let adapter = RecordingAdapter::new("email");
    let approval = surface(&[Arc::clone(&adapter)]);
    approval.record_inbound("email", "a@b.c", "a@b.c", None);
    assert!(approval.reply_context("email_a@b.c_a@b.c").is_none());
    assert!(matches!(
        approval
            .surface(&prompt("email", "email_a@b.c_a@b.c"))
            .await,
        SurfaceOutcome::NoReplyContext
    ));
    assert!(adapter.sent().is_empty());
}

#[tokio::test]
async fn missing_reply_context_is_reported_not_sent() {
    let adapter = RecordingAdapter::new("discord");
    let approval = surface(&[Arc::clone(&adapter)]);
    assert!(matches!(
        approval.surface(&prompt("discord", "discord_bob_c")).await,
        SurfaceOutcome::NoReplyContext
    ));
    assert!(matches!(
        approval.surface(&prompt("slack", "slack_bob_c")).await,
        SurfaceOutcome::NoReplyContext
    ));
    assert!(adapter.sent().is_empty());
}

#[tokio::test]
async fn send_failure_is_returned() {
    let adapter = Arc::new(RecordingAdapter {
        name: "signal".into(),
        fail: true,
        sent: StdMutex::new(Vec::new()),
    });
    let approval = surface(&[Arc::clone(&adapter)]);
    approval.record_inbound("signal", "bob", "c", None);
    match approval.surface(&prompt("signal", "signal_bob_c")).await {
        SurfaceOutcome::SendFailed(err) => assert_eq!(err.to_string(), "send refused"),
        other => panic!("unexpected {other:?}"),
    }
}
