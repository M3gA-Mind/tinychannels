//! Module-owned provider interaction execution.
use tinychannels::Channel;

/// Capability facts bound to the exact adapter handle selected at startup.
pub(crate) fn configured_adapter_capabilities(
    provider: &crate::provider::Running,
) -> tinychannels_bus::ChannelCapabilities {
    let adapter = provider.adapter_handle();
    let descriptor = adapter.descriptor();
    let mut capabilities = adapter.declared_capabilities();
    capabilities.account.configured = Some(true);
    capabilities.account.enabled = Some(true);
    capabilities.account.account_id = descriptor.account_id;
    capabilities.account.proactive_target = adapter.proactive_target();
    capabilities
}
use tinychannels_bus::{ChannelInteraction, ChannelInteractionInfo, ChannelInteractionResult};

/// Canonical facts shared by Describe and every configured-instance consumer.
pub(crate) fn configured_capabilities(
    provider: &dyn Channel,
) -> tinychannels_bus::ChannelCapabilities {
    let mut capabilities = provider.capabilities();
    capabilities.account.configured = Some(true);
    capabilities.account.enabled = Some(true);
    capabilities.account.proactive_target = provider.proactive_target();
    capabilities
}

pub(crate) async fn dispatch(
    provider: &dyn Channel,
    interaction: ChannelInteraction,
) -> tinybus::Result<ChannelInteractionResult> {
    use ChannelInteraction as Op;
    let result = match interaction {
        Op::Describe => {
            let capabilities = configured_capabilities(provider);
            return Ok(ChannelInteractionResult::Info(Box::new(
                ChannelInteractionInfo {
                    capabilities: (&capabilities).into(),
                    proactive_target: capabilities.account.proactive_target.clone(),
                    supports_reactions: capabilities.reactions,
                    supports_draft_updates: capabilities.streaming_reply_pipeline,
                },
            )));
        }
        Op::DescribeV2 => {
            return Ok(ChannelInteractionResult::InfoV2(Box::new(
                tinychannels_bus::ChannelInteractionInfoV2 {
                    capabilities: configured_capabilities(provider),
                },
            )));
        }
        Op::StartTyping { recipient } => provider.start_typing(&recipient).await,
        Op::StopTyping { recipient } => provider.stop_typing(&recipient).await,
        Op::StartTypingRefresh { .. } | Op::StopTypingRefresh { .. } => {
            return Err(tinybus::Error::failed(
                "typing refresh must be owned by the running provider",
            ));
        }
        Op::SendDraft { message } => {
            return provider
                .send_draft(&message)
                .await
                .map(ChannelInteractionResult::Draft)
                .map_err(|_| tinybus::Error::failed("provider draft creation failed"));
        }
        Op::UpdateDraft {
            recipient,
            message_id,
            text,
        } => provider.update_draft(&recipient, &message_id, &text).await,
        Op::FinalizeDraft {
            recipient,
            message_id,
            text,
            thread_ts,
        } => {
            provider
                .finalize_draft(&recipient, &message_id, &text, thread_ts.as_deref())
                .await
        }
    };
    result
        .map(|()| ChannelInteractionResult::Completed)
        .map_err(|_| tinybus::Error::failed("provider interaction failed"))
}

#[cfg(test)]
#[path = "interaction_tests.rs"]
#[allow(
    deprecated,
    reason = "interaction test doubles still implement the v21 adapter"
)]
mod tests;
