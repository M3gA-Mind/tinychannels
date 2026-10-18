//! Behaviour the two WhatsApp adapters share.
//!
//! WhatsApp reaches TinyChannels through two unrelated protocols: the Business
//! Cloud API ([`super::whatsapp`]) and the linked-device Web protocol
//! ([`super::whatsapp_web`]). What they have in common is not wire format but
//! policy: which parts an outbound message may carry, how a stored media
//! reference becomes bytes, what a voice note must look like, how a vCard is
//! written and read, and how an inbound attachment is persisted before it
//! reaches the host. That policy lives here once, so a fix to it cannot reach
//! only one of the two adapters.

pub(crate) mod capabilities;
pub(crate) mod error;
pub(crate) mod inbound;
pub(crate) mod plan;
pub(crate) mod text;
pub(crate) mod vcard;
pub(crate) mod window;

#[cfg(test)]
#[path = "fakes_tests.rs"]
pub(crate) mod fakes;
