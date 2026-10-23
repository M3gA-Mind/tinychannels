//! Console channel: reads user lines from stdin and prints replies to stdout.
//!
//! Useful for local debugging and headless hosts. It has no network
//! dependency and is never part of a desktop UI.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::channel::{
    AccessContext, ChannelAdapter, ChannelDescriptor, ChannelInboundEnvelope, ChannelInboundSink,
    ChannelOutboundIntent, ChannelSendError, MessageReceipt, SenderDmDecision,
};
use crate::controllers::ChannelAccountSnapshot;
use tinychannels_bus::{
    ChannelCapabilities, ChannelRef, ContentPart, ConversationKind, ConversationRef,
    OutboundPayload, SenderRef,
};

/// Console provider adapter. Clones share the active reader's cancellation.
#[derive(Clone, Default)]
pub struct CliChannel {
    active: Arc<Mutex<Option<(Uuid, CancellationToken)>>>,
}

impl std::fmt::Debug for CliChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CliChannel").finish_non_exhaustive()
    }
}

impl CliChannel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read from an injected stream, primarily for embedding and tests.
    pub async fn start_with_reader<R>(
        &self,
        reader: R,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) where
        R: AsyncBufRead + Unpin,
    {
        let generation = Uuid::new_v4();
        let cancellation = CancellationToken::new();
        *self.active.lock().await = Some((generation, cancellation.clone()));
        forward_lines(reader, sink, &cancellation).await;
        let mut active = self.active.lock().await;
        if active
            .as_ref()
            .is_some_and(|(current, _)| *current == generation)
        {
            *active = None;
        }
    }

    /// Stop the active input loop. A later start installs a fresh token.
    pub async fn stop(&self) -> Result<(), ChannelSendError> {
        if let Some((_, cancellation)) = self.active.lock().await.as_ref() {
            cancellation.cancel();
        }
        Ok(())
    }
}

async fn forward_lines<R>(
    reader: R,
    sink: &(dyn ChannelInboundSink + Send + Sync),
    cancellation: &CancellationToken,
) where
    R: AsyncBufRead + Unpin,
{
    let mut lines = reader.lines();
    loop {
        let line = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            line = lines.next_line() => match line {
                Ok(Some(line)) => line.trim().to_owned(),
                _ => break,
            },
        };
        if line.is_empty() {
            continue;
        }
        if line == "/quit" || line == "/exit" {
            break;
        }
        let envelope = ChannelInboundEnvelope {
            channel: ChannelRef {
                id: "cli".into(),
                account_id: Some("cli".into()),
            },
            message_id: Uuid::new_v4().to_string(),
            conversation: ConversationRef {
                kind: ConversationKind::Dm,
                id: "user".into(),
                scope_id: Some("cli".into()),
                ..Default::default()
            },
            sender: SenderRef {
                id: "user".into(),
                ..Default::default()
            },
            text: line.clone(),
            parts: vec![ContentPart::Text {
                text: line,
                markdown_dialect: None,
            }],
            access: AccessContext {
                dm_decision: SenderDmDecision::Allow,
                ..Default::default()
            },
            ..Default::default()
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            result = sink.push(envelope) => if result.is_err() { break; },
        }
    }
}

#[async_trait]
impl ChannelAdapter for CliChannel {
    fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor {
            id: "cli".into(),
            display_name: "Console".into(),
            account_id: Some("cli".into()),
            ..Default::default()
        }
    }

    fn media_account_identity(&self) -> Option<String> {
        Some("cli".into())
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        let mut capabilities = ChannelCapabilities::unknown();
        capabilities.content.text.inbound = true;
        capabilities.content.text.outbound = true;
        capabilities.actions.supported.push("send".into());
        capabilities
    }

    async fn start(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        self.start_with_reader(BufReader::new(io::stdin()), sink)
            .await;
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        CliChannel::stop(self).await
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        match intent.payload {
            OutboundPayload::Text { text } => {
                println!("{text}");
                Ok(MessageReceipt::default())
            }
            _ => Err(ChannelSendError::unsupported(
                "console supports text messages only",
            )),
        }
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        Ok(ChannelAccountSnapshot {
            channel_id: "cli".into(),
            account_id: Some("cli".into()),
            health_state: Some("healthy".into()),
            ..Default::default()
        })
    }
}

#[cfg(test)]
#[path = "cli_tests.rs"]
mod tests;
