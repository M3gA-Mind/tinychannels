//! Console channel: reads user lines from stdin and prints replies to stdout.
//!
//! Useful for local debugging and headless hosts. It has no network
//! dependency and is never part of a desktop UI.

use async_trait::async_trait;
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, BufReader};
use uuid::Uuid;

use crate::channel::{
    AccessContext, ChannelAccountSnapshot, ChannelAdapter, ChannelCapabilities, ChannelDescriptor,
    ChannelInboundEnvelope, ChannelInboundSink, ChannelOutboundIntent, ChannelOutboundMessage,
    ChannelSendError, ChannelSendOutcome, ChannelTypedSend, ContentPart, ConversationKind,
    ConversationRef, GroupAccessPolicy, MentionGate, SenderDmDecision, SenderRef,
};

/// Console channel. `/quit` or `/exit` ends the listen loop.
#[derive(Debug, Default, Clone, Copy)]
pub struct CliChannel;

impl CliChannel {
    pub fn new() -> Self {
        Self
    }
}

/// Read lines from `reader` and forward each non-empty one as a typed inbound
/// envelope until EOF, `/quit`/`/exit`, or the receiver closes.
async fn forward_lines<R: AsyncBufRead + Unpin>(
    reader: R,
    sink: &(dyn ChannelInboundSink + Send + Sync),
) {
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
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
        if sink
            .push(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
            .is_err()
        {
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
        forward_lines(BufReader::new(io::stdin()), sink).await;
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
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
