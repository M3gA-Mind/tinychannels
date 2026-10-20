//! Module-owned provider interaction execution.
use tinychannels::Channel;

/// Return Describe information from the exact configured native adapter.
/// V1 keeps its wire projection until the compatibility window ends; all
/// capability facts come from the same adapter selected for provider launch.
pub(crate) fn describe_adapter(
    adapter: &dyn tinychannels::channel::ChannelAdapter,
    legacy: bool,
) -> ChannelInteractionResult {
    let mut capabilities = adapter.declared_capabilities();
    capabilities.account.configured = Some(true);
    capabilities.account.enabled = Some(true);
    capabilities.account.proactive_target = adapter.proactive_target();

    if legacy {
        ChannelInteractionResult::Info(Box::new(ChannelInteractionInfo {
            capabilities: (&capabilities).into(),
            proactive_target: capabilities.account.proactive_target.clone(),
            supports_reactions: capabilities.reactions,
            supports_draft_updates: capabilities.streaming_reply_pipeline,
        }))
    } else {
        ChannelInteractionResult::InfoV2(Box::new(tinychannels_bus::ChannelInteractionInfoV2 {
            capabilities,
        }))
    }
}

/// Convert the frozen interaction DTO to the typed intent sent to a configured
/// provider. The configured adapter supplies account scope; the caller's
/// recipient remains only the conversation address.
fn draft_intent(
    adapter: &dyn tinychannels::channel::ChannelAdapter,
    message: &tinychannels::SendMessage,
) -> tinychannels_bus::ChannelOutboundIntent {
    let descriptor = adapter.descriptor();
    let mut intent = tinychannels::outbound_intent_from_send_message(&descriptor.id, message);
    intent.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: descriptor.id,
        account_id: descriptor.account_id,
    });
    if let Some(conversation) = intent.conversation_ref.as_mut() {
        conversation.scope_id = intent
            .channel_ref
            .as_ref()
            .and_then(|channel| channel.account_id.clone());
    }
    if message.subject.is_none() {
        intent.payload = tinychannels_bus::OutboundPayload::Text {
            text: message.content.clone(),
        };
    }
    intent
}

/// Route draft start/update/finalize through an adapter's typed draft port.
/// `None` means the provider has not migrated that operation yet, so the
/// caller may use the temporary legacy-provider compatibility path.
pub(crate) async fn dispatch_native_draft(
    adapter: &dyn tinychannels::channel::ChannelAdapter,
    drafts: &dyn tinychannels::channel::ChannelStreamingDraft,
    interaction: ChannelInteraction,
) -> Option<tinybus::Result<ChannelInteractionResult>> {
    use ChannelInteraction as Op;
    let capabilities = adapter.declared_capabilities();
    match interaction {
        Op::SendDraft { message } => {
            if !capabilities.streaming_reply_pipeline {
                return Some(Ok(ChannelInteractionResult::Draft(None)));
            }
            let intent = draft_intent(adapter, &message);
            match drafts.start_draft(intent).await {
                Ok(id) => Some(Ok(ChannelInteractionResult::Draft(id))),
                Err(error) if error.unsupported => None,
                Err(_) => Some(Err(tinybus::Error::failed(
                    "provider draft creation failed",
                ))),
            }
        }
        Op::UpdateDraft {
            recipient,
            message_id,
            text,
        } => {
            if !capabilities.streaming_reply_pipeline {
                return None;
            }
            let message = tinychannels::SendMessage::new(text, recipient);
            let intent = draft_intent(adapter, &message);
            match drafts.update_draft(&message_id, intent).await {
                Ok(()) => Some(Ok(ChannelInteractionResult::Completed)),
                Err(error) if error.unsupported => None,
                Err(_) => Some(Err(tinybus::Error::failed("provider draft update failed"))),
            }
        }
        Op::FinalizeDraft {
            recipient,
            message_id,
            text,
            thread_ts,
        } => {
            if !capabilities.streaming_reply_pipeline {
                return None;
            }
            let mut message = tinychannels::SendMessage::new(text, recipient);
            message.thread_ts = thread_ts;
            let intent = draft_intent(adapter, &message);
            match drafts.finalize_draft(&message_id, intent).await {
                Ok(()) => Some(Ok(ChannelInteractionResult::Completed)),
                Err(error) if error.unsupported => None,
                Err(_) => Some(Err(tinybus::Error::failed(
                    "provider draft finalization failed",
                ))),
            }
        }
        _ => None,
    }
}

/// Capability facts bound to the exact adapter handle selected at startup.
pub(crate) fn configured_adapter_capabilities(
    provider: &std::sync::Arc<crate::provider::Running>,
) -> tinychannels_bus::ChannelCapabilities {
    let adapter = provider.adapter_handle();
    let descriptor = adapter.descriptor();
    debug_assert_eq!(
        descriptor.id,
        tinychannels_bus::provider_id(provider.provider_name())
    );
    let mut capabilities = adapter.declared_capabilities();
    capabilities.account.configured = Some(true);
    capabilities.account.enabled = Some(true);
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

/// The error name of a refused webhook; hosts answer it with a client error.
pub(crate) const WEBHOOK_REJECTED_ERROR: &str = "ai.tinyhumans.tinychannels.Error.WebhookRejected";
/// The error name of a delivery that failed after it was authenticated; hosts
/// answer it with a retryable error.
pub(crate) const WEBHOOK_FAILED_ERROR: &str = "ai.tinyhumans.tinychannels.Error.WebhookFailed";

fn webhook_rejected() -> tinybus::Error {
    tinybus::Error::MethodFailed {
        name: WEBHOOK_REJECTED_ERROR.to_owned(),
        message: "webhook delivery was rejected".to_owned(),
    }
}

fn webhook_failed() -> tinybus::Error {
    tinybus::Error::MethodFailed {
        name: WEBHOOK_FAILED_ERROR.to_owned(),
        message: "webhook delivery could not be ingested".to_owned(),
    }
}

/// Decode and ingest one raw webhook POST. Bodies are bounded before the
/// provider sees them; the provider authenticates the exact bytes.
async fn deliver_webhook(
    provider: &dyn Channel,
    signature: Option<&str>,
    body_base64: &str,
) -> tinybus::Result<ChannelInteractionResult> {
    use base64::Engine as _;
    // 4 base64 characters carry 3 bytes; refuse anything over 4 MiB decoded.
    if body_base64.len() > 4 * 1024 * 1024 * 4 / 3 + 4 {
        return Err(webhook_rejected());
    }
    let body = base64::engine::general_purpose::STANDARD
        .decode(body_base64.as_bytes())
        .map_err(|_| webhook_rejected())?;
    match provider.deliver_webhook(signature, &body).await {
        Ok(outcome) => Ok(ChannelInteractionResult::WebhookAccepted(outcome)),
        Err(error)
            if error
                .downcast_ref::<tinychannels::WebhookRejected>()
                .is_some() =>
        {
            Err(webhook_rejected())
        }
        Err(_) => Err(webhook_failed()),
    }
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
        Op::UpsertDraft { .. } => {
            return Err(tinybus::Error::failed(
                "typed draft requires configured provider queue",
            ));
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
        Op::SendReaction {
            conversation,
            provider_message_id,
            reaction,
            remove,
            ..
        } => {
            return provider
                .send_reaction(&conversation, &provider_message_id, &reaction, remove)
                .await
                .map(|()| ChannelInteractionResult::Completed)
                .map_err(|_| tinybus::Error::failed("provider reaction failed"));
        }
        Op::EditMessage { receipt, intent } => {
            return provider
                .edit_message(&receipt, &intent)
                .await
                .map(|receipt| {
                    ChannelInteractionResult::Sent(Box::new(
                        tinychannels_bus::channel::ChannelSendOutcome {
                            receipt,
                            error: None,
                        },
                    ))
                })
                .map_err(|_| tinybus::Error::failed("provider message edit failed"));
        }
        Op::DeleteMessage { receipt } => {
            return provider
                .delete_message(&receipt)
                .await
                .map(|()| ChannelInteractionResult::Completed)
                .map_err(|_| tinybus::Error::failed("provider message deletion failed"));
        }
        Op::MarkRead {
            conversation,
            provider_message_id,
            ..
        } => {
            return provider
                .mark_read(&conversation, &provider_message_id)
                .await
                .map(|()| ChannelInteractionResult::Completed)
                .map_err(|_| tinybus::Error::failed("provider read receipt failed"));
        }
        Op::DeliverWebhook {
            signature,
            body_base64,
        } => return deliver_webhook(provider, signature.as_deref(), &body_base64).await,
        Op::VerifyWebhook {
            mode,
            verify_token,
            challenge,
        } => {
            return provider
                .verify_webhook(&mode, &verify_token, &challenge)
                .map(ChannelInteractionResult::WebhookChallenge)
                .ok_or_else(webhook_rejected);
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
