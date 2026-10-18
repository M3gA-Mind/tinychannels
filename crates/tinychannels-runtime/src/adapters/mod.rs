//! Generic channel adapters.

pub mod local;
pub mod web;

pub use local::{LocalChannelAdapter, LocalOutboundSink};
pub use web::{
    WEB_CHANNEL_ID, WebChannelAdapter, WebChatStart, WebEventSink, WebScope, WebTurnProjector,
    normalize_chat_start,
};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;
