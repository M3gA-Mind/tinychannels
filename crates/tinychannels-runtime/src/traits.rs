//! In-process provider seams and legacy send helpers.
pub use tinychannels_bus::traits::*;

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
