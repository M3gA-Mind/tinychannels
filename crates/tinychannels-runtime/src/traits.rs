// Scoped allowance for the one-release v21 wire DTO compatibility surface.
#![allow(deprecated)]

//! Legacy wire DTOs and webhook rejection type.

pub use tinychannels_bus::{ChannelMessage, SendMessage};

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
