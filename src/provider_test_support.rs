//! Test-only helpers for exercising providers through their typed ports.

use crate::channel::{ChannelAdapter, ChannelSendOutcome, SendMessage, outbound_intent_from_send_message};

/// Exercise a typed adapter with a one-release wire DTO converted at its edge.
pub(crate) async fn send_v1<C: ChannelAdapter>(
    channel: &C,
    message: &SendMessage,
) -> Result<ChannelSendOutcome, crate::channel::ChannelSendError> {
    let intent = outbound_intent_from_send_message(&channel.descriptor().id, message);
    let outcome = channel.send(intent).await?;
    match outcome.error.clone() {
        Some(error) => Err(error),
        None => Ok(outcome),
    }
}

pub(crate) async fn is_healthy<C: ChannelAdapter>(channel: &C) -> bool {
    ChannelAdapter::status(channel)
        .await
        .ok()
        .and_then(|snapshot| snapshot.health_state)
        .is_some_and(|state| state == "healthy")
}
