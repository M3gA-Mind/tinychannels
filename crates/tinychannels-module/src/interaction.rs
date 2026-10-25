// This module retains the one-release v1 wire DTO boundary and converts it
// immediately to typed provider intents.
#![allow(deprecated)]

//! Module-owned provider interaction execution.

/// V21 compatibility interactions for isolated legacy channel doubles.
/// Factory-built production providers use the adapter interaction path.
#[allow(deprecated)]
pub(crate) async fn dispatch(
    channel: &dyn tinychannels::channel::Channel,
    interaction: ChannelInteraction,
) -> tinybus::Result<ChannelInteractionResult> {
    use ChannelInteraction as Op;
    match interaction {
        Op::StartTyping { recipient } => channel
            .start_typing(&recipient)
            .await
            .map(|()| ChannelInteractionResult::Completed)
            .map_err(|_| tinybus::Error::failed("provider typing start failed")),
        Op::StopTyping { recipient } => channel
            .stop_typing(&recipient)
            .await
            .map(|()| ChannelInteractionResult::Completed)
            .map_err(|_| tinybus::Error::failed("provider typing stop failed")),
        Op::SendDraft { message } => channel
            .send_draft(&message)
            .await
            .map(ChannelInteractionResult::Draft)
            .map_err(|_| tinybus::Error::failed("provider draft creation failed")),
        Op::Describe => {
            let capabilities = channel.capabilities();
            Ok(ChannelInteractionResult::Info(Box::new(
                ChannelInteractionInfo {
                    capabilities: (&capabilities).into(),
                    proactive_target: channel.proactive_target(),
                    supports_reactions: capabilities.reactions,
                    supports_draft_updates: capabilities.streaming_reply_pipeline,
                },
            )))
        }
        Op::DescribeV2 => Ok(ChannelInteractionResult::InfoV2(Box::new(
            tinychannels_bus::ChannelInteractionInfoV2 {
                capabilities: channel.capabilities(),
            },
        ))),
        _ => Err(tinybus::Error::failed(
            "interaction is unavailable through the v21 compatibility boundary",
        )),
    }
}

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
#[allow(deprecated)]
#[cfg(test)]
fn draft_intent(
    adapter: &dyn tinychannels::channel::ChannelAdapter,
    configured_instance: &str,
    message: &tinychannels::SendMessage,
) -> tinychannels_bus::ChannelOutboundIntent {
    let descriptor = adapter.descriptor();
    let is_telegram = descriptor.id == "telegram";
    let mut intent = tinychannels::outbound_intent_from_send_message(&descriptor.id, message);
    intent.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: descriptor.id.clone(),
        // Account scope belongs to the configured module instance. Some raw
        // provider adapters (including SlackChannel) intentionally have no
        // account identity in their descriptor.
        account_id: Some(configured_instance.to_owned()),
    });
    if let Some(conversation) = intent.conversation_ref.as_mut() {
        conversation.scope_id = intent
            .channel_ref
            .as_ref()
            .and_then(|channel| channel.account_id.clone());
    }
    intent.payload = tinychannels_bus::OutboundPayload::Text {
        text: compatibility_text(&descriptor.id, message),
    };
    if is_telegram {
        normalize_telegram_v1_address(&mut intent, message);
    }
    intent
}

/// Restore the frozen Telegram v1 address convention before a legacy DTO is
/// handed to typed provider operations. `recipient` may encode
/// `chat_id:topic_id`, while `thread_ts` identifies a reply parent rather than
/// a conversation thread. Canonical typed intents do not pass through here.
#[cfg(test)]
pub(crate) fn normalize_telegram_v1_address(
    intent: &mut tinychannels_bus::ChannelOutboundIntent,
    message: &tinychannels::SendMessage,
) {
    if let Some((chat_id, topic_id)) = message.recipient.split_once(':') {
        intent.conversation_id = chat_id.to_owned();
        let conversation =
            intent
                .conversation_ref
                .get_or_insert_with(|| tinychannels_bus::ConversationRef {
                    id: chat_id.to_owned(),
                    ..Default::default()
                });
        conversation.id = chat_id.to_owned();
        if conversation.topic_id.is_none() {
            conversation.topic_id = Some(topic_id.to_owned());
        }
    }
    if intent.reply_to_id.is_none() {
        intent.reply_to_id = message.thread_ts.clone();
    }
    intent.thread_id = None;
    if let Some(conversation) = intent.conversation_ref.as_mut() {
        conversation.thread_id = None;
    }
}

#[cfg(test)]
pub(crate) fn compatibility_text(provider: &str, message: &tinychannels::SendMessage) -> String {
    // Configured instances are named `<provider>:<account>`. Keep the v1
    // subject convention keyed to the provider family, not the instance id.
    let provider = provider
        .split_once(':')
        .map_or(provider, |(provider, _)| provider);
    match (provider, message.subject.as_deref()) {
        ("email" | "dingtalk", Some(subject)) => format!(
            "Subject: {}\n{}",
            subject.trim().replace(['\r', '\n'], " "),
            message.content
        ),
        _ => message.content.clone(),
    }
}

/// Route draft start/update/finalize through an adapter's typed draft port.
/// `None` means the provider has not migrated that operation yet, so the
/// caller may use the temporary legacy-provider compatibility path.
#[allow(deprecated)]
#[cfg(test)]
pub(crate) async fn dispatch_native_draft(
    adapter: &dyn tinychannels::channel::ChannelAdapter,
    drafts: &dyn tinychannels::channel::ChannelStreamingDraft,
    configured_instance: &str,
    interaction: ChannelInteraction,
) -> Option<tinybus::Result<ChannelInteractionResult>> {
    use ChannelInteraction as Op;
    let capabilities = adapter.declared_capabilities();
    match interaction {
        Op::SendDraft { message } => {
            if !capabilities.streaming_reply_pipeline {
                return Some(Ok(ChannelInteractionResult::Draft(None)));
            }
            let intent = draft_intent(adapter, configured_instance, &message);
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
            let intent = draft_intent(adapter, configured_instance, &message);
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
            let intent = draft_intent(adapter, configured_instance, &message);
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
    let provider_name = provider.provider_name();
    debug_assert_eq!(descriptor.id, tinychannels_bus::provider_id(&provider_name));
    let mut capabilities = adapter.declared_capabilities();
    capabilities.account.configured = Some(true);
    capabilities.account.enabled = Some(true);
    capabilities.account.proactive_target = adapter.proactive_target();
    capabilities
}
use tinychannels_bus::{ChannelInteraction, ChannelInteractionInfo, ChannelInteractionResult};

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

/// Route a migrated provider's webhook through its typed owner port while
/// keeping exact-byte authentication and the existing bounded wire contract.
pub(crate) async fn dispatch_native_webhook(
    provider: &dyn tinychannels::channel::ChannelAdapter,
    interaction: ChannelInteraction,
) -> Option<tinybus::Result<ChannelInteractionResult>> {
    use ChannelInteraction as Op;
    match interaction {
        Op::DeliverWebhook {
            signature,
            body_base64,
        } if provider.supports_webhook_delivery() => {
            Some(deliver_webhook_typed(provider, signature.as_deref(), &body_base64).await)
        }
        Op::VerifyWebhook {
            mode,
            verify_token,
            challenge,
        } if provider.supports_webhook_verification() => Some(
            provider
                .verify_webhook(&mode, &verify_token, &challenge)
                .map(ChannelInteractionResult::WebhookChallenge)
                .ok_or_else(webhook_rejected),
        ),
        _ => None,
    }
}

async fn deliver_webhook_typed(
    provider: &dyn tinychannels::channel::ChannelAdapter,
    signature: Option<&str>,
    body_base64: &str,
) -> tinybus::Result<ChannelInteractionResult> {
    use base64::Engine as _;
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

#[cfg(test)]
#[path = "interaction_tests.rs"]
#[allow(
    deprecated,
    reason = "interaction test doubles still implement the v21 adapter"
)]
mod tests;
