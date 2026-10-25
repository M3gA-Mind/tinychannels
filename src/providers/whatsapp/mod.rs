//! WhatsApp Cloud API provider and module exports.

pub(super) mod api;
mod capabilities;
mod channel;
mod ingress;
mod outbound;

pub use channel::WhatsAppChannel;
