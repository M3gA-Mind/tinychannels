//! Discord on the v2 adapter contract: capability facts and the optional
//! typing, reaction, edit, delete and streaming-draft extensions.

use super::channel::DiscordChannel;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelInboundSink, ChannelSendError,
    ChannelSendOutcome, ChannelStreamingDraft, ChannelTypedSend, ChannelTyping,
    LegacyChannelReaction, MessageReceipt,
};
use async_trait::async_trait;
use tinychannels_bus::{ChannelCapabilities, ChannelOutboundIntent, ChannelOutboundMessage};

struct GatewayListenerGuard<'a>(&'a DiscordChannel);

impl Drop for GatewayListenerGuard<'_> {
    fn drop(&mut self) {
        self.0.listener_cancel.lock().take();
    }
}

fn gateway_error(error: impl std::fmt::Display) -> ChannelSendError {
    ChannelSendError {
        kind: crate::channel::SendErrorKind::Unknown,
        message: format!("Discord Gateway listener failed: {error}"),
        retryable: true,
        ..Default::default()
    }
}

fn wrong_channel(operation: &str) -> ChannelSendError {
    ChannelSendError {
        kind: crate::channel::SendErrorKind::Forbidden,
        message: format!("{operation} channel does not match Discord"),
        retryable: false,
        ..Default::default()
    }
}

impl DiscordChannel {
    /// Capability facts for this configured account. Media and voice are
    /// declared only when the host supplied the storage they need.
    pub(crate) fn v2_capabilities(&self) -> ChannelCapabilities {
        super::capabilities::declared(self)
    }
}

#[async_trait]
impl ChannelAdapter for DiscordChannel {
    fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        tinychannels_bus::channel::ChannelDescriptor {
            id: "discord".into(),
            display_name: "Discord".into(),
            metadata: std::collections::BTreeMap::from([(
                "adapterMode".into(),
                "native-v2".into(),
            )]),
            ..Default::default()
        }
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.v2_capabilities()
    }

    fn proactive_target(&self) -> Option<String> {
        self.channel_id
            .as_deref()
            .map(str::trim)
            .filter(|channel| !channel.is_empty())
            .map(str::to_owned)
    }

    async fn start(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        let (cancel, mut cancellation) = tokio::sync::watch::channel(false);
        {
            let mut active = self.listener_cancel.lock();
            if active.is_some() {
                return Err(ChannelSendError {
                    kind: crate::channel::SendErrorKind::Unknown,
                    message: "Discord Gateway listener is already running".into(),
                    retryable: false,
                    ..Default::default()
                });
            }
            *active = Some(cancel);
        }
        let _cleanup = GatewayListenerGuard(self);
        let (tx, mut inbound) = tokio::sync::mpsc::channel(32);
        let gateway = self.run_gateway_typed(tx);
        tokio::pin!(gateway);
        let delivery = async {
            while let Some(ingress) = inbound.recv().await {
                sink.push_ingress(ingress).await?;
            }
            Ok::<(), ChannelSendError>(())
        };
        tokio::pin!(delivery);

        tokio::select! {
            biased;
            changed = cancellation.changed() => {
                let _ = changed;
                Ok(())
            }
            result = &mut gateway => {
                let result = result.map_err(gateway_error);
                result?;
                tokio::select! {
                    biased;
                    changed = cancellation.changed() => {
                        let _ = changed;
                        Ok(())
                    }
                    delivery_result = &mut delivery => delivery_result,
                }
            }
            delivery_result = &mut delivery => {
                delivery_result?;
                tokio::select! {
                    biased;
                    changed = cancellation.changed() => {
                        let _ = changed;
                        Ok(())
                    }
                    result = &mut gateway => result.map_err(gateway_error),
                }
            }
        }
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        if let Some(cancel) = self.listener_cancel.lock().as_ref() {
            cancel.send_replace(true);
        }
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

    async fn status(
        &self,
    ) -> Result<tinychannels_runtime::controllers::ChannelAccountSnapshot, ChannelSendError> {
        let health_state = if self.health_status().await {
            "healthy"
        } else {
            "unhealthy"
        };
        Ok(tinychannels_bus::controllers::ChannelAccountSnapshot {
            channel_id: "discord".into(),
            states: vec![
                tinychannels_bus::controllers::ChannelAccountState::Configured,
                tinychannels_bus::controllers::ChannelAccountState::Enabled,
            ],
            health_state: Some(health_state.into()),
            ..Default::default()
        })
    }
}

#[async_trait]
impl ChannelTypedSend for DiscordChannel {
    async fn send_typed(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        self.send_v2_with_receipt(message).await
    }
}

#[async_trait]
impl LegacyChannelReaction for DiscordChannel {
    async fn send_reaction(
        &self,
        channel: &tinychannels_bus::channel::ChannelRef,
        conversation: &tinychannels_bus::channel::ConversationRef,
        provider_message_id: &str,
        reaction: &tinychannels_bus::channel::ChannelReaction,
        remove: bool,
    ) -> Result<(), ChannelSendError> {
        if tinychannels_bus::provider_id(&channel.id) != "discord" {
            return Err(wrong_channel("reaction"));
        }
        self.send_reaction_v2(conversation, provider_message_id, reaction, remove)
            .await
    }
}

#[async_trait]
impl ChannelEdit for DiscordChannel {
    async fn edit(
        &self,
        receipt: &MessageReceipt,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.edit_message_v2(receipt, &intent).await
    }
}

#[async_trait]
impl ChannelDelete for DiscordChannel {
    async fn delete(&self, receipt: &MessageReceipt) -> Result<(), ChannelSendError> {
        self.delete_message_v2(receipt).await
    }
}

#[async_trait]
impl ChannelTyping for DiscordChannel {
    async fn start_typing(&self, conversation_id: &str) -> Result<(), ChannelSendError> {
        self.send_typing_once(conversation_id).await
    }

    async fn stop_typing(&self, _conversation_id: &str) -> Result<(), ChannelSendError> {
        // Discord's indicator expires on its own and has no stop call.
        Ok(())
    }
}

#[async_trait]
impl ChannelStreamingDraft for DiscordChannel {
    async fn upsert_draft(
        &self,
        previous: Option<&MessageReceipt>,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        if intent
            .channel_ref
            .as_ref()
            .is_some_and(|c| tinychannels_bus::provider_id(&c.id) != "discord")
        {
            return Err(wrong_channel("draft"));
        }
        self.upsert_draft_v2(previous, intent).await
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
