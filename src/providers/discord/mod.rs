mod adapter;
pub mod api;
pub mod bot_api;
mod capabilities;
pub mod channel;
mod gateway;
mod http;
mod inbound;
mod interactions;
mod outbound;
mod voice;

pub use bot_api::{DiscordReactionRequest, ThreadAction};
pub use channel::DiscordChannel;

#[cfg(test)]
#[path = "support_tests.rs"]
mod support;

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod conformance;
