//! TinyChannels bus identity and member names.
//!
//! Two objects, in opposite directions. The module serves [`BUS_NAME`] so a host
//! can drive providers; the *host* serves [`HOST_BUS_NAME`] so the module can
//! deliver what arrives from the network. A channel is bidirectional and a
//! served object cannot open a stream back to its caller, so inbound traffic
//! needs its own host-side object rather than a reply on the outbound call.

/// Well-known bus name exported by the TinyChannels module.
pub const BUS_NAME: &str = "ai.tinyhumans.tinychannels.Channels";

/// Object path served by the TinyChannels module.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinychannels/Channels";

/// Well-known bus name the *host* serves for module-to-host callbacks.
pub const HOST_BUS_NAME: &str = "ai.tinyhumans.tinychannels.ChannelsHost";

/// Object path the host serves for module-to-host callbacks.
pub const HOST_OBJECT_PATH: &str = "/ai/tinyhumans/tinychannels/ChannelsHost";

/// One constant per method name on [`BUS_NAME`].
pub mod methods {
    /// Reserve one idle relay handle before native startup.
    pub const RESERVE_RELAY: &str = "ReserveRelay";
    /// Start a reserved relay using confidential hydrated configuration.
    pub const START_RELAY: &str = "StartRelay";
    /// Stop and join one reserved relay, including pending startup.
    pub const STOP_RELAY: &str = "StopRelay";
    /// Return safe lifecycle facts for one relay reservation.
    pub const RELAY_STATUS: &str = "RelayStatus";
    /// Submit an ordered relay outbound intent.
    pub const SEND_RELAY_INTENT: &str = "SendRelayIntent";
    /// Freeze publication and drain every owned provider, relay and callback.
    pub const SHUTDOWN: &str = "Shutdown";
    /// `SendChannelIntent` — deliver one typed intent through a running provider.
    pub const SEND_CHANNEL_INTENT: &str = "SendChannelIntent";
    /// `SendEmail` — send host-configured MIME mail without starting IMAP.
    pub const SEND_EMAIL: &str = "SendEmail";
    /// `StartChannel` — connect and begin receiving on one configured provider.
    pub const START_CHANNEL: &str = "StartChannel";
    /// `StopChannel` — disconnect one running provider.
    pub const STOP_CHANNEL: &str = "StopChannel";
    /// `SendMessage` — deliver one outbound intent through a running provider.
    pub const SEND_MESSAGE: &str = "SendMessage";
    /// `ListChannels` — report every provider this build can serve.
    pub const LIST_CHANNELS: &str = "ListChannels";
    /// `ChannelStatus` — report connection state for one provider.
    pub const CHANNEL_STATUS: &str = "ChannelStatus";
}

/// One constant per method name on [`HOST_BUS_NAME`].
pub mod host_methods {
    /// `DeliverInbound` — hand the host one authenticated inbound envelope.
    pub const DELIVER_INBOUND: &str = "DeliverInbound";
    /// `ReportStatus` — report a provider connection state transition.
    pub const REPORT_STATUS: &str = "ReportStatus";
    /// `PersistAllowedIdentity` — persist a provider-authorized identity host-side.
    pub const PERSIST_ALLOWED_IDENTITY: &str = "PersistAllowedIdentity";
}

/// All module method names in the declaration order used by the interface.
pub const METHODS: [&str; 13] = [
    methods::START_CHANNEL,
    methods::STOP_CHANNEL,
    methods::SEND_MESSAGE,
    methods::LIST_CHANNELS,
    methods::CHANNEL_STATUS,
    methods::RESERVE_RELAY,
    methods::START_RELAY,
    methods::STOP_RELAY,
    methods::RELAY_STATUS,
    methods::SEND_RELAY_INTENT,
    methods::SHUTDOWN,
    methods::SEND_CHANNEL_INTENT,
    methods::SEND_EMAIL,
];

/// All host-callback method names in declaration order.
pub const HOST_METHODS: [&str; 3] = [
    host_methods::DELIVER_INBOUND,
    host_methods::REPORT_STATUS,
    host_methods::PERSIST_ALLOWED_IDENTITY,
];

#[cfg(test)]
#[path = "names_tests.rs"]
mod tests;
