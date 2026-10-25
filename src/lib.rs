//! Pluggable channel and messaging primitives for OpenHuman harness
//! communication.
//!
//! This crate is the **implementation** half of TinyChannels: the provider
//! stack (Telegram, Discord, Slack, IMAP, WhatsApp, …), the relay transport
//! loop, delivery, and the host boundary OpenHuman implements for channel side
//! effects.
//!
//! The **contract** half — every type that crosses a bus or an embedding
//! boundary — lives in [`tinychannels_bus`] and is re-exported here under the
//! module paths it has always occupied, so `tinychannels::channel::…`,
//! `tinychannels::config::…` and friends keep resolving unchanged.
//!
//! # Which half to depend on
//!
//! Depend on `tinychannels-bus` alone if you only need to *name* channel types
//! — an envelope in an event enum or the config schema.
//! That costs serde and little else. Depend on this crate when you need to
//! actually open a socket, which pulls `reqwest`, `rustls` and
//! `tokio-tungstenite`. The read-only iMessage system-database provider is
//! optional behind the `imessage` feature and is the only user of `rusqlite`.

// Re-export the WebSocket transport crate so downstream consumers (and their
// tests that exercise provider WS seams) can construct version-matched
// `tungstenite` messages without pinning the same version themselves.
pub use tokio_tungstenite;

// The contract crate, re-exported whole so a downstream can reach it by either
// name. Prefer `tinychannels_bus::…` in new code.
pub use tinychannels_bus;

// Contract declarations and runtime modules preserve their legacy paths. These
// are `pub use` of the *same* items, not copies — there is exactly one
// `ChannelInboundEnvelope` in the build.
pub use tinychannels_bus::{capabilities, error, names, version};

pub use tinychannels_runtime::WebhookRejected;
pub use tinychannels_runtime::{
    adapters, channel, config, context, controllers, security, text, traits,
};

#[allow(
    deprecated,
    reason = "approval sends still use the v21 shim until provider migration"
)]
pub mod approvals;
#[allow(
    deprecated,
    reason = "backend adapter migration is tracked in issue #103"
)]
pub mod backend;
pub mod delivery;
pub mod factory;
pub mod harness;
pub mod host;
#[allow(
    deprecated,
    reason = "provider implementations migrate to typed sends in issue #103"
)]
pub mod providers;
pub mod relay;
pub mod remote;
pub mod routes;
pub mod runtime;
pub mod storage;

pub use backend::{ChannelBackend, ChannelManager};
pub use factory::{
    ConfiguredHttpClients, DefaultHttpClients, HttpClientFactory, build_channels,
    build_long_tail_channel,
};
pub use host::{ChannelHost, ChannelHostBuilder, HostCapabilities, NoopHost, ProviderContext};
#[cfg(feature = "imessage")]
pub use providers::IMessageChannel;
#[cfg(test)]
pub(crate) use providers::long_tail::LongTailNative;
pub use providers::{
    CliChannel, DingTalkChannel, DiscordChannel, IrcChannel, IrcChannelConfig, LinqChannel,
    MattermostChannel, QQChannel, SignalChannel, SlackChannel, TelegramChannel, WhatsAppChannel,
    WhatsAppWebChannel, YuanbaoChannel,
};
#[allow(deprecated, reason = "re-export one-release v21 compatibility DTOs")]
pub use tinychannels_bus::{
    BUS_NAME, CONTRACT_VERSION, ChannelAuthMode, ChannelBackendRequest, ChannelBackendResult,
    ChannelCapabilities, ChannelControllerRequest, ChannelControllerResult, ChannelDefinition,
    ChannelInboundEnvelope, ChannelMessage, ChannelOutboundIntent, ChannelsConfig,
    DeliveryDurability, HOST_BUS_NAME, HOST_OBJECT_PATH, HostEventPublication,
    HostTranscriptionRequest, HostTranscriptionResult, HttpProxyPolicy, METHODS, MODULE_ID,
    OBJECT_PATH, OutboundPayload, ReplyContext, Result, SendMessage, TinyChannelsError,
    is_compatible, methods,
};
pub use tinychannels_runtime::channel::{
    build_session_key_for_inbound_envelope, inbound_envelope_from_legacy_message,
    legacy_message_from_inbound_envelope, legacy_message_value_from_outbound_intent,
    outbound_intent_from_legacy_message, outbound_intent_from_send_message,
    typed_message_from_inbound_envelope,
};
#[allow(
    deprecated,
    reason = "re-export one-release v21 send compatibility adapter"
)]
pub use tinychannels_runtime::{Channel, ChannelSendExt, SendMessageExt};
// Re-exported separately so each can follow its provider's feature gate.
#[cfg(feature = "email-send")]
pub use providers::EmailChannel;
#[cfg(feature = "lark")]
pub use providers::LarkChannel;

#[cfg(all(test, feature = "email-send"))]
#[path = "lib_email_feature_smoke_tests.rs"]
mod email_feature_smoke_tests;

#[cfg(all(test, feature = "lark"))]
#[path = "lib_lark_feature_smoke_tests.rs"]
mod lark_feature_smoke_tests;

#[cfg(test)]
mod provider_test_support;
