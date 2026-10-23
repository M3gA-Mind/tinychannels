//! Approval surface against a recording native adapter.

use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use tinychannels_bus::channel::ChannelOutboundIntent;
use tinychannels_bus::{ChannelMessage, SendMessage};
use tinychannels_runtime::Channel;

use super::*;

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
impl Channel for RecordingAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports_chat_approvals(&self) -> bool {
        self.name != "email"
    }

    async fn listen(&self, _tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        if self.fail {
            anyhow::bail!("send refused");
        }
        self.sent.lock().unwrap().push(
            tinychannels_runtime::channel::outbound_intent_from_send_message(&self.name, message),
        );
        Ok(())
    }
}

fn surface(channels: &[Arc<RecordingAdapter>]) -> ApprovalSurface {
    let map: HashMap<String, Arc<dyn Channel>> = channels
        .iter()
        .map(|channel| {
            (
                channel.name.clone(),
                Arc::clone(channel) as Arc<dyn Channel>,
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
        assert_eq!(sent[0].channel_ref, None);
        assert!(matches!(
            &sent[0].payload,
            tinychannels_bus::OutboundPayload::NativeChannelData { data }
                if data["content"].as_str().is_some_and(|text| text.contains("file_write"))
        ));
        assert!(
            sent[0]
                .idempotency_key
                .starts_with(&format!("legacy-send:{channel}:"))
        );
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
