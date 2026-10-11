//! Module-owned provider interaction execution.
use tinychannels::Channel;
use tinychannels_bus::{ChannelInteraction, ChannelInteractionInfo, ChannelInteractionResult};

pub(crate) async fn dispatch(
    provider: &dyn Channel,
    interaction: ChannelInteraction,
) -> tinybus::Result<ChannelInteractionResult> {
    use ChannelInteraction as Op;
    let result = match interaction {
        Op::Describe => {
            return Ok(ChannelInteractionResult::Info(ChannelInteractionInfo {
                capabilities: provider.capabilities(),
                proactive_target: provider.proactive_target(),
                supports_reactions: provider.supports_reactions(),
                supports_draft_updates: provider.supports_draft_updates(),
            }));
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
mod tests;
