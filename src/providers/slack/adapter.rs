//! `Channel` and v2 adapter trait implementations for Slack.

use super::interact::edit_text_for_legacy;
use super::outbound::{send_error_to_anyhow, text_message};
use super::types::SlackChannel;
use crate::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use std::collections::BTreeMap;
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

#[async_trait]
impl Channel for SlackChannel {
    fn name(&self) -> &str {
        "slack"
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    fn proactive_target(&self) -> Option<String> {
        self.channel_id.clone()
    }

    fn supports_remote_control(&self) -> bool {
        true
    }

    fn supports_chat_approvals(&self) -> bool {
        true
    }

    fn supports_reactions(&self) -> bool {
        true
    }

    fn supports_edit(&self) -> bool {
        true
    }

    fn supports_draft_updates(&self) -> bool {
        true
    }

    fn supports_reply_to_id(&self) -> bool {
        true
    }

    fn supports_thread_id(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let typed = text_message(
            &message.recipient,
            message.thread_ts.as_deref(),
            &message.content,
        );
        self.send_v2_with_receipt(&typed)
            .await
            .map(|_| ())
            .map_err(send_error_to_anyhow)
    }

    async fn send_typed_with_receipt(
        &self,
        message: &ChannelOutboundMessage,
    ) -> anyhow::Result<ChannelSendOutcome> {
        self.send_v2_with_receipt(message)
            .await
            .map_err(send_error_to_anyhow)
    }

    async fn send_draft(&self, message: &SendMessage) -> anyhow::Result<Option<String>> {
        let typed = text_message(
            &message.recipient,
            message.thread_ts.as_deref(),
            &message.content,
        );
        let outcome = self
            .send_v2_with_receipt(&typed)
            .await
            .map_err(send_error_to_anyhow)?;
        Ok(outcome.receipt.primary_platform_message_id)
    }

    async fn update_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        edit_text_for_legacy(self, recipient, message_id, text)
            .await
            .map_err(send_error_to_anyhow)
    }

    async fn finalize_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
        _thread_ts: Option<&str>,
    ) -> anyhow::Result<()> {
        edit_text_for_legacy(self, recipient, message_id, text)
            .await
            .map_err(send_error_to_anyhow)
    }

    async fn send_reaction(
        &self,
        conversation: &ConversationRef,
        provider_message_id: &str,
        reaction: &ChannelReaction,
        remove: bool,
    ) -> anyhow::Result<()> {
        self.send_reaction_v2(conversation, provider_message_id, reaction, remove)
            .await
            .map_err(send_error_to_anyhow)
    }

    async fn edit_message(
        &self,
        receipt: &MessageReceipt,
        intent: &ChannelOutboundIntent,
    ) -> anyhow::Result<MessageReceipt> {
        self.edit_message_v2(receipt, intent)
            .await
            .map_err(send_error_to_anyhow)
    }

    async fn delete_message(&self, receipt: &MessageReceipt) -> anyhow::Result<()> {
        self.delete_message_v2(receipt)
            .await
            .map_err(send_error_to_anyhow)
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        if let Some(app_token) = self.app_token.clone() {
            return self.listen_socket(&tx, &app_token).await;
        }
        let channel_id = self
            .channel_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Slack channel_id required for listening"))?;
        self.listen_polling(&tx, &channel_id).await
    }

    async fn health_check(&self) -> bool {
        self.health_status().await
    }

    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.set_typing_status(recipient, "is typing…")
            .await
            .map_err(send_error_to_anyhow)
    }

    async fn stop_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.set_typing_status(recipient, "")
            .await
            .map_err(send_error_to_anyhow)
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
        _sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        // The configured module's running task owns the single inbound loop.
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        self.send_v2_with_receipt(&ChannelOutboundMessage::from_intent(&intent))
            .await
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
impl crate::channel::ChannelReaction for SlackChannel {
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
