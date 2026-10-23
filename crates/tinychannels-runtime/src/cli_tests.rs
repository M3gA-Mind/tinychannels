use super::*;
use crate::channel::{
    ChannelAdapter, ChannelInboundEnvelope, ChannelInboundSink, ChannelOutboundIntent,
    ChannelSendError, SenderDmDecision,
};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tinychannels_bus::{ChannelOutboundMessage, ContentPart, ConversationKind};
use tokio::io::ReadBuf;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct MpscSink(tokio::sync::mpsc::Sender<ChannelInboundEnvelope>);

#[async_trait]
impl ChannelInboundSink for MpscSink {
    async fn push(&self, envelope: ChannelInboundEnvelope) -> Result<(), ChannelSendError> {
        self.0.send(envelope).await.map_err(|_| ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Transient,
            message: "CLI test receiver closed".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

struct NotifyingSink {
    started: Arc<Notify>,
    sender: tokio::sync::mpsc::Sender<ChannelInboundEnvelope>,
}

#[async_trait]
impl ChannelInboundSink for NotifyingSink {
    async fn push(&self, envelope: ChannelInboundEnvelope) -> Result<(), ChannelSendError> {
        self.started.notify_one();
        self.sender
            .send(envelope)
            .await
            .map_err(|_| ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "CLI test receiver closed".into(),
                retryable: true,
                ..Default::default()
            })
    }
}

#[tokio::test]
async fn cli_adapter_reports_configured_text_capabilities_and_status() {
    let cli = CliChannel::new();
    let descriptor = cli.descriptor();
    assert_eq!(descriptor.id, "cli");
    assert_eq!(descriptor.account_id.as_deref(), Some("cli"));
    let capabilities = cli.declared_capabilities();
    assert!(capabilities.content.text.inbound);
    assert!(capabilities.content.text.outbound);
    assert!(
        capabilities
            .actions
            .supported
            .iter()
            .any(|action| action == "send")
    );
    assert_eq!(cli.media_account_identity().as_deref(), Some("cli"));

    let status = cli.status().await.unwrap();
    assert_eq!(status.channel_id, "cli");
    assert_eq!(status.account_id.as_deref(), Some("cli"));
    assert_eq!(status.health_state.as_deref(), Some("healthy"));
    assert!(cli.stop().await.is_ok());
}

#[tokio::test]
async fn cli_adapter_sends_text_and_rejects_non_text_typed_content() {
    let cli = CliChannel::new();
    let intent = ChannelOutboundIntent {
        channel_id: "cli".into(),
        conversation_id: "user".into(),
        payload: tinychannels_bus::OutboundPayload::Text {
            text: "hello".into(),
        },
        ..Default::default()
    };
    assert!(cli.send(intent).await.is_ok());

    let media = ChannelOutboundMessage {
        channel_id: "cli".into(),
        conversation_id: "user".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Image {
                media: tinychannels_bus::MediaReference::default(),
                caption: None,
                alt_text: None,
            },
        }],
        ..ChannelOutboundMessage::from_intent(&ChannelOutboundIntent::default())
    };
    let error = cli.send_typed(&media).await.unwrap_err();
    assert!(error.unsupported);
}

async fn collect(input: &str) -> Vec<ChannelInboundEnvelope> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let sink = MpscSink(tx);
    forward_lines(input.as_bytes(), &sink, &CancellationToken::new()).await;
    drop(sink);
    let mut out = Vec::new();
    while let Some(envelope) = rx.recv().await {
        out.push(envelope);
    }
    out
}

#[tokio::test]
async fn forward_lines_skips_blank_lines_trims_and_keeps_typed_scope() {
    let messages = collect("  hi  \n\n   \nthere\n").await;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].text, "hi");
    assert_eq!(messages[1].text, "there");
    for message in &messages {
        assert_eq!(message.channel.id, "cli");
        assert_eq!(message.channel.account_id.as_deref(), Some("cli"));
        assert_eq!(message.conversation.kind, ConversationKind::Dm);
        assert_eq!(message.conversation.id, "user");
        assert_eq!(message.conversation.scope_id.as_deref(), Some("cli"));
        assert_eq!(message.sender.id, "user");
        assert!(matches!(
            message.parts.as_slice(),
            [ContentPart::Text { text, markdown_dialect: None }] if text == &message.text
        ));
        assert_eq!(message.access.dm_decision, SenderDmDecision::Allow);
    }
    assert_ne!(messages[0].message_id, messages[1].message_id);
}

#[tokio::test]
async fn forward_lines_stops_at_quit_or_exit() {
    assert_eq!(collect("a\n/quit\nb\n").await.len(), 1);
    assert_eq!(collect("a\n/exit\nb\n").await.len(), 1);
}

#[tokio::test]
async fn forward_lines_stops_when_receiver_closes() {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    forward_lines(
        "a\nb\n".as_bytes(),
        &MpscSink(tx),
        &CancellationToken::new(),
    )
    .await;
}

struct PendingReader {
    started: Arc<Notify>,
    signalled: Arc<AtomicBool>,
}

impl tokio::io::AsyncRead for PendingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.signalled.swap(true, Ordering::SeqCst) {
            self.started.notify_one();
        }
        Poll::Pending
    }
}

#[tokio::test]
async fn stop_cancels_blocked_reader_and_adapter_can_restart() {
    let cli = CliChannel::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let sink = Arc::new(MpscSink(tx));
    let started = Arc::new(Notify::new());
    let signalled = Arc::new(AtomicBool::new(false));
    let reader = BufReader::new(PendingReader {
        started: started.clone(),
        signalled,
    });
    let running_cli = cli.clone();
    let running_sink = sink.clone();
    let task = tokio::spawn(async move {
        running_cli
            .start_with_reader(reader, running_sink.as_ref())
            .await;
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
        .await
        .expect("reader should begin polling");
    cli.stop().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .expect("stop should release a blocked stdin read")
        .unwrap();

    cli.start_with_reader("again\n".as_bytes(), sink.as_ref())
        .await;
    let envelope = rx.recv().await.expect("restarted adapter forwards input");
    assert_eq!(envelope.text, "again");
}

#[tokio::test]
async fn stop_cancels_blocked_inbound_sink_push() {
    let cli = CliChannel::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    tx.try_send(ChannelInboundEnvelope::default())
        .expect("fill the sink before starting the CLI");
    let started = Arc::new(Notify::new());
    let sink = Arc::new(NotifyingSink {
        started: started.clone(),
        sender: tx,
    });
    let running_cli = cli.clone();
    let running_sink = sink.clone();
    let task = tokio::spawn(async move {
        running_cli
            .start_with_reader("blocked\n".as_bytes(), running_sink.as_ref())
            .await;
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
        .await
        .expect("sink push should begin");
    cli.stop().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .expect("stop should release a blocked inbound sink push")
        .unwrap();
}
