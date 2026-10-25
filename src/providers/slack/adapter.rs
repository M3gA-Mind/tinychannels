//! `Channel` and v2 adapter trait implementations for Slack.

use super::types::SlackChannel;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use tinychannels_bus::channel::{
    ChannelDescriptor, ChannelReaction, ChannelRef, ChannelSendError, ChannelSendOutcome,
    ConversationRef, MessageReceipt,
};
use tinychannels_bus::controllers::{ChannelAccountSnapshot, ChannelAccountState};
use tinychannels_bus::{ChannelCapabilities, ChannelOutboundIntent, ChannelOutboundMessage};

fn check_channel(channel: &ChannelRef) -> Result<(), ChannelSendError> {
    if channel.id == "slack" {
        return Ok(());
    }
    Err(ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Forbidden,
        message: "channel does not match Slack".into(),
        ..Default::default()
    })
}

struct SlackListenerGuard<'a>(&'a SlackChannel);

impl Drop for SlackListenerGuard<'_> {
    fn drop(&mut self) {
        self.0.listener_running.store(false, Ordering::Release);
    }
}

#[async_trait]
impl crate::channel::ChannelAdapter for SlackChannel {
    fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor {
            id: "slack".into(),
            display_name: "Slack".into(),
            metadata: BTreeMap::from([
                ("adapterMode".into(), "native-v2".into()),
                ("voiceOut".into(), "file".into()),
            ]),
            ..Default::default()
        }
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    fn proactive_target(&self) -> Option<String> {
        self.channel_id.clone()
    }

    async fn start(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        if self
            .listener_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Forbidden,
                message: "Slack listener is already running".into(),
                retryable: false,
                ..Default::default()
            });
        }
        let _guard = SlackListenerGuard(self);
        self.listener_cancel.send_replace(false);
        let mut stop = self.listener_cancel.subscribe();
        let listen = async {
            if let Some(app_token) = self.app_token.as_deref() {
                self.listen_socket_typed(sink, app_token).await
            } else if let Some(channel_id) = self.channel_id.as_deref() {
                self.listen_polling_typed(sink, channel_id).await
            } else {
                anyhow::bail!("Slack listening needs a channel_id or Socket Mode app token")
            }
        };
        tokio::select! {
            biased;
            _ = stop.wait_for(|stopped| *stopped) => Ok(()),
            result = listen => result.map_err(|error| ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: error.to_string(),
                retryable: true,
                sent_before_error: false,
                ..Default::default()
            }),
        }
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        self.listener_cancel.send_replace(true);
        Ok(())
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.send_v2_with_receipt(&ChannelOutboundMessage::from_intent(&intent))
            .await
            .map(|outcome| outcome.receipt)
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        let health_state = if self.health_status().await {
            "healthy"
        } else {
            "unhealthy"
        };
        Ok(ChannelAccountSnapshot {
            channel_id: "slack".into(),
            states: vec![
                ChannelAccountState::Configured,
                ChannelAccountState::Enabled,
            ],
            health_state: Some(health_state.into()),
            ..Default::default()
        })
    }
}

#[async_trait]
impl crate::channel::ChannelTypedSend for SlackChannel {
    async fn send_typed(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        self.send_v2_with_receipt(message).await
    }
}

#[async_trait]
impl crate::channel::ChannelTyping for SlackChannel {
    async fn start_typing(&self, conversation_id: &str) -> Result<(), ChannelSendError> {
        self.set_typing_status(conversation_id, "is typing…").await
    }

    async fn stop_typing(&self, conversation_id: &str) -> Result<(), ChannelSendError> {
        self.set_typing_status(conversation_id, "").await
    }
}

#[async_trait]
impl crate::channel::LegacyChannelReaction for SlackChannel {
    async fn send_reaction(
        &self,
        channel: &ChannelRef,
        conversation: &ConversationRef,
        provider_message_id: &str,
        reaction: &ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        check_channel(channel)?;
        self.send_reaction_v2(conversation, provider_message_id, reaction, remove)
            .await
    }
}

#[async_trait]
impl crate::channel::ChannelEdit for SlackChannel {
    async fn edit(
        &self,
        receipt: &MessageReceipt,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.edit_message_v2(receipt, &intent).await
    }
}

#[async_trait]
impl crate::channel::ChannelDelete for SlackChannel {
    async fn delete(&self, receipt: &MessageReceipt) -> Result<(), ChannelSendError> {
        self.delete_message_v2(receipt).await
    }
}

#[async_trait]
impl crate::channel::ChannelStreamingDraft for SlackChannel {
    async fn start_draft(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<Option<String>, ChannelSendError> {
        self.upsert_draft(None, intent)
            .await
            .map(|receipt| receipt.primary_platform_message_id)
    }

    async fn update_draft(
        &self,
        draft_id: &str,
        intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        let previous = legacy_draft_receipt(draft_id, &intent);
        self.upsert_draft(Some(&previous), intent).await.map(|_| ())
    }

    async fn finalize_draft(
        &self,
        draft_id: &str,
        intent: ChannelOutboundIntent,
    ) -> Result<(), ChannelSendError> {
        // Slack's existing finalization path is the same provider edit as an
        // intermediate update; no extra send or overflow messages are needed.
        crate::channel::ChannelStreamingDraft::update_draft(self, draft_id, intent).await
    }

    async fn upsert_draft(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.upsert_draft_v2(previous, intent).await
    }
}

fn legacy_draft_receipt(draft_id: &str, intent: &ChannelOutboundIntent) -> MessageReceipt {
    let channel = intent.channel_ref.clone().unwrap_or_else(|| ChannelRef {
        id: intent.channel_id.clone(),
        ..Default::default()
    });
    let conversation = intent
        .conversation_ref
        .clone()
        .unwrap_or_else(|| ConversationRef {
            id: intent.conversation_id.clone(),
            thread_id: intent.thread_id.clone(),
            ..Default::default()
        });
    MessageReceipt {
        channel: Some(channel),
        conversation: Some(conversation),
        primary_platform_message_id: Some(draft_id.to_owned()),
        platform_message_ids: vec![draft_id.to_owned()],
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
