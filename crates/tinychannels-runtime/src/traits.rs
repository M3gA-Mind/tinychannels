//! In-process provider seams and legacy send helpers.
// V21 DTO use is confined to the compatibility traits in this module.
#![allow(deprecated)]

pub use tinychannels_bus::traits::*;

/// A webhook delivery refused before ingestion because its signature, body,
/// or configured account scope is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookRejected(pub String);

impl std::fmt::Display for WebhookRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WebhookRejected {}

/// Runtime idempotency construction for the legacy send DTO.
pub trait SendMessageExt {
    fn with_deterministic_idempotency_key(self, channel_id: impl AsRef<str>) -> Self;
}
impl SendMessageExt for SendMessage {
    /// Generate a deterministic idempotency key from this legacy send shape.
    fn with_deterministic_idempotency_key(mut self, channel_id: impl AsRef<str>) -> Self {
        let intent = crate::channel::outbound_intent_from_send_message(channel_id.as_ref(), &self);
        self.idempotency_key = Some(intent.idempotency_key);
        self
    }
}

#[cfg(test)]
#[path = "traits_tests.rs"]
mod tests;
