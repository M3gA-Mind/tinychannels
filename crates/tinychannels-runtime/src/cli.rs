//! Console channel: reads user lines from stdin and prints replies to stdout.
//!
//! Useful for local debugging and headless hosts. It has no network
//! dependency and is never part of a desktop UI.

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::channel::{
    AccessContext, ChannelAdapter, ChannelDescriptor, ChannelInboundEnvelope, ChannelInboundSink,
    ChannelOutboundIntent, ChannelSendError, ChannelSendOutcome, ChannelTypedSend,
    ConversationKind, ConversationRef, GroupAccessPolicy, MentionGate, SenderDmDecision, SenderRef,
};
use crate::controllers::ChannelAccountSnapshot;
use tinychannels_bus::{ChannelCapabilities, ChannelOutboundMessage, ContentPart};

/// Console channel. `/quit` or `/exit` ends the listen loop.
#[derive(Debug, Clone)]
pub struct CliChannel {
    cancellation: Arc<Mutex<CancellationToken>>,
}

impl Default for CliChannel {
    fn default() -> Self {
        Self {
            cancellation: Arc::new(Mutex::new(CancellationToken::new())),
        }
    }
}

impl CliChannel {
    pub fn new() -> Self {
        Self::default()
    }

    fn start_token(&self) -> CancellationToken {
        let mut guard = self
            .cancellation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if guard.is_cancelled() {
            *guard = CancellationToken::new();
        }
        guard.clone()
    }

    async fn start_with_reader<R: AsyncBufRead + Unpin>(
        &self,
        reader: R,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) {
        let cancellation = self.start_token();
        forward_lines(reader, sink, &cancellation).await;
    }
}

/// Read lines from `reader` and forward each non-empty one as a typed inbound
/// envelope until EOF, `/quit`/`/exit`, or the receiver closes.
async fn forward_lines<R: AsyncBufRead + Unpin>(
    reader: R,
    sink: &(dyn ChannelInboundSink + Send + Sync),
    cancellation: &CancellationToken,
) {
    let mut lines = reader.lines();
    loop {
        let result = tokio::select! {
            _ = cancellation.cancelled() => break,
            result = lines.next_line() => result,
        };
        let Ok(Some(line)) = result else {
            break;
        };
        let line = line.trim().to_owned();
        if line.is_empty() {
            continue;
        }
        if line == "/quit" || line == "/exit" {
            break;
        }
        let timestamp_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let envelope = ChannelInboundEnvelope {
            channel: tinychannels_bus::ChannelRef {
                id: "cli".into(),
                account_id: Some("cli".into()),
            },
            message_id: Uuid::new_v4().to_string(),
            timestamp_unix_ms: Some(timestamp_unix_ms),
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
                group_policy: GroupAccessPolicy::Disabled,
                mention_gate: MentionGate::None,
                command_authorized: false,
                delivered_via_upstream_relay: false,
            },
            ..Default::default()
        };
        let push_result = tokio::select! {
            _ = cancellation.cancelled() => break,
            result = sink.push(tinychannels_bus::ChannelIngress::Inbound(envelope)) => result,
        };
        if push_result.is_err() {
            break;
        }
    }
}

fn text_only_error() -> ChannelSendError {
    ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
        message: "CLI supports text messages only".into(),
        unsupported: true,
        ..Default::default()
    }
}

#[async_trait]
impl ChannelAdapter for CliChannel {
    fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor {
            id: "cli".into(),
            display_name: "CLI".into(),
            account_id: Some("cli".into()),
            ..Default::default()
        }
    }

    fn media_account_identity(&self) -> Option<String> {
        Some("cli".into())
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        let mut capabilities = ChannelCapabilities::unknown();
        capabilities.content.parts = vec!["text".into()];
        capabilities.content.text.inbound = true;
        capabilities.content.text.outbound = true;
        capabilities.actions.supported = vec!["send".into()];
        capabilities.addressing.account_id = true;
        capabilities.addressing.conversation_types = vec!["dm".into()];
        capabilities.account.configured = Some(true);
        capabilities.account.enabled = Some(true);
        capabilities.account.healthy = Some(true);
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
        self.cancellation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancel();
        Ok(())
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        self.send_typed(&ChannelOutboundMessage::from_intent(&intent))
            .await
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        Ok(ChannelAccountSnapshot {
            channel_id: "cli".into(),
            account_id: Some("cli".into()),
            states: vec![tinychannels_bus::controllers::ChannelAccountState::Enabled],
            health_state: Some("healthy".into()),
            ..Default::default()
        })
    }
}

#[async_trait]
impl ChannelTypedSend for CliChannel {
    async fn send_typed(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        let mut lines = Vec::with_capacity(message.parts.len());
        for part in &message.parts {
            match part {
                tinychannels_bus::OutboundContentPart::Content {
                    part: ContentPart::Text { text, .. },
                } => lines.push(text.as_str()),
                _ => return Err(text_only_error()),
            }
        }
        println!("{}", lines.join("\n"));
        Ok(ChannelSendOutcome::default())
    }
}

#[cfg(test)]
#[path = "cli_tests.rs"]
mod tests;
