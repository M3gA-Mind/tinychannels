//! Loadable TinyBus module adapter for TinyChannels.
//!
//! This private workspace crate keeps the vendored TinyBus dependency out of the
//! independently publishable `tinychannels` crate. Its `cdylib` output is the
//! target-specific binary distributed in GitHub releases.
//!
//! What it buys a host is a dependency boundary that survives compilation: the
//! provider stack — and with it `reqwest`, `rusqlite`, `rustls`,
//! `tokio-tungstenite` and the email/lark cohorts — lives in the loaded library
//! rather than in every binary that wants to send a message.
//!
//! What it costs is process isolation. A loaded module shares the host's address
//! space, privileges and crash domain; TinyBus's deadlines, bounded queues and
//! caught panics contain ordinary misbehaviour, not a segfault. Modules are
//! first-party code that ships separately.

#[allow(
    deprecated,
    reason = "legacy inbound DTO adapter remains during v22 migration"
)]
mod control;
#[allow(
    deprecated,
    reason = "legacy controller requests migrate with the owning host"
)]
mod controller;
mod delivery;
mod email;
mod health;
pub mod host;
mod interaction;
mod media;
mod media_account;
mod prepare_connect;
mod progressive;
#[allow(
    deprecated,
    reason = "legacy provider interface migrates in issue #103"
)]
mod provider;
mod relay;
#[allow(
    deprecated,
    reason = "legacy outbound DTO adapter remains during v22 migration"
)]
mod remote;
mod runtime;
#[allow(
    deprecated,
    reason = "service retains explicit one-release v21 conversions"
)]
mod service;
#[cfg(test)]
mod storage_fixture;
mod voice_reply;
mod web_output;

/// Constructs this module for registration with an in-process TinyBus host.
#[cfg(feature = "static-link")]
pub use service::exports::linked_module;

pub use host::HostChannels;
pub use tinychannels_bus::{
    BUS_NAME, CONTRACT_VERSION, HOST_BUS_NAME, HOST_OBJECT_PATH, METHODS, OBJECT_PATH,
    is_compatible,
};
