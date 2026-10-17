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
    /// Parse model/provider commands and render help without changing host state.
    pub const PREPARE_RUNTIME_COMMAND: &str = "PrepareRuntimeCommand";
    /// Prepare module-owned channel facts and runtime command responses.
    pub const PREPARE_CHANNEL_CONTROL: &str = "PrepareChannelControl";
    /// Run module-owned remote commands and persisted turn state.
    pub const EXECUTE_REMOTE: &str = "ExecuteRemote";
    /// Drive a module-owned progressive reply through an opaque handle.
    pub const EXECUTE_PROGRESSIVE: &str = "ExecuteProgressive";
    /// Prepare normalized inbound facts and persisted keys inside the module.
    pub const PREPARE_CHANNEL_INBOUND: &str = "PrepareChannelInbound";
    /// Derive hosted-ingress thread and client identities inside the module.
    pub const PREPARE_CHANNEL_IDENTITY: &str = "PrepareChannelIdentity";
    /// Resolve provider facts from confidential hydrated configuration without listening.
    pub const DESCRIBE_CONFIGURED_CHANNELS: &str = "DescribeConfiguredChannels";
    /// Keep an external-webhook provider available for outbound operations.
    pub const START_OUTBOUND_CHANNEL: &str = "StartOutboundChannel";
    /// Normalize legacy messages and produce deterministic intent keys in the module.
    pub const PREPARE_OUTBOUND_INTENT: &str = "PrepareOutboundIntent";
    /// Reserve one inert identity-scoped runtime with immutable callback paths.
    pub const RESERVE_RUNTIME: &str = "ReserveRuntime";
    /// Activate the private provider object for a known runtime handle.
    pub const START_RUNTIME: &str = "StartRuntime";
    /// Activate an identity runtime with bounded `ProcessInbound` turn callbacks.
    pub const START_RUNTIME_WITH_DISPATCH: &str = "StartRuntimeWithDispatch";
    /// Stop and join all resources belonging to one runtime handle.
    pub const STOP_RUNTIME: &str = "StopRuntime";
    /// Start a provider with module-owned reconnect supervision and host-selected timing.
    pub const START_CHANNEL_SUPERVISED: &str = "StartChannelSupervised";
    /// Check configured providers without starting listeners; inputs are confidential.
    pub const CHECK_CHANNEL_HEALTH: &str = "CheckChannelHealth";
    /// Reserve one idle relay handle before native startup.
    pub const RESERVE_RELAY: &str = "ReserveRelay";
    /// Start a reserved relay using confidential hydrated configuration.
    pub const START_RELAY: &str = "StartRelay";
    /// Start a relay using the canonical v2 identity-scoped capability handshake.
    pub const START_RELAY_V2: &str = "StartRelayV2";
    /// Stop and join one reserved relay, including pending startup.
    pub const STOP_RELAY: &str = "StopRelay";
    /// Return safe lifecycle facts for one relay reservation.
    pub const RELAY_STATUS: &str = "RelayStatus";
    /// Query negotiated canonical v2 relay capabilities by reservation.
    pub const RELAY_CAPABILITIES_V2: &str = "RelayCapabilitiesV2";
    /// Submit an ordered relay outbound intent.
    pub const SEND_RELAY_INTENT: &str = "SendRelayIntent";
    /// Freeze publication and drain every owned provider, relay and callback.
    pub const SHUTDOWN: &str = "Shutdown";
    /// `SendChannelIntent` — deliver one typed intent through a running provider.
    pub const SEND_CHANNEL_INTENT: &str = "SendChannelIntent";
    /// `SendEmail` — send host-configured MIME mail without starting IMAP.
    pub const SEND_EMAIL: &str = "SendEmail";
    /// Begin an attachment upload whose chunks remain below TinyBus frame limits.
    pub const BEGIN_EMAIL_UPLOAD: &str = "BeginEmailUpload";
    /// Append one bounded confidential attachment chunk idempotently.
    pub const APPEND_EMAIL_UPLOAD_CHUNK: &str = "AppendEmailUploadChunk";
    /// Send a completed upload once and replay its terminal result by handle.
    pub const SEND_EMAIL_UPLOAD: &str = "SendEmailUpload";
    /// Abort an upload that has not entered SMTP delivery.
    pub const ABORT_EMAIL_UPLOAD: &str = "AbortEmailUpload";
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
    /// Start a provider with host-selected HTTP proxy policy.
    pub const START_CHANNEL_CONFIGURED: &str = "StartChannelConfigured";
    /// Dispatch typed controller work through TinyChannels' owning manager.
    pub const CHANNEL_CONTROLLER: &str = "ChannelController";
    /// Run typing, draft and capability operations on one running provider.
    pub const CHANNEL_INTERACTION: &str = "ChannelInteraction";
}

/// One constant per method name on [`HOST_BUS_NAME`].
pub mod host_methods {
    /// `DeliverEvent` — deliver one typed provider event to the host.
    pub const DELIVER_EVENT: &str = "DeliverEvent";
    /// `DeliverInbound` — hand the host one authenticated v22 inbound envelope.
    pub const DELIVER_INBOUND: &str = "DeliverInbound";
    /// Complete one admitted v22 inbound envelope turn. Opt-in runtimes invoke
    /// bounded concurrent callbacks and retain each call until flush/stop has drained it.
    pub const PROCESS_INBOUND: &str = "ProcessInbound";
    /// `ReportStatus` — report a provider connection state transition.
    pub const REPORT_STATUS: &str = "ReportStatus";
    /// `PersistAllowedIdentity` — persist a provider-authorized identity host-side.
    pub const PERSIST_ALLOWED_IDENTITY: &str = "PersistAllowedIdentity";
    /// `Transcribe` — use the host's configured speech recognition provider.
    pub const TRANSCRIBE: &str = "Transcribe";
    /// Scoped STT for the shared voice pipeline.
    pub const TRANSCRIBE_VOICE: &str = "TranscribeVoice";
    /// `SynthesizeSpeech` — use the host's configured speech synthesis provider.
    pub const SYNTHESIZE_SPEECH: &str = "SynthesizeSpeech";
    /// `StoreVoiceMedia` — persist voice media and return a durable host reference.
    pub const STORE_VOICE_MEDIA: &str = "StoreVoiceMedia";
    pub const READ_VOICE_MEDIA: &str = "ReadVoiceMedia";
    /// Host-selected voice reply mode and authoritative turn facts.
    pub const GET_VOICE_REPLY_POLICY: &str = "GetVoiceReplyPolicy";
    /// `PublishEvent` — publish one provider event through the host event sink.
    pub const PUBLISH_EVENT: &str = "PublishEvent";
    /// Call a narrow host credential or managed API capability. Durable state is not
    /// one of them: modules persist through the storage bus instead.
    pub const DISPATCH_CONTROLLER_BACKEND: &str = "DispatchControllerBackend";
}

/// The source-of-truth module method registry, shared with the module export generator.
#[doc(hidden)]
#[macro_export]
macro_rules! channel_method_registry {
    ($consumer:ident) => {
        $consumer!([
            "StartChannel",
            "StopChannel",
            "SendMessage",
            "ListChannels",
            "ChannelStatus",
            "ReserveRelay",
            "StartRelay",
            "StartRelayV2",
            "StopRelay",
            "RelayStatus",
            "RelayCapabilitiesV2",
            "SendRelayIntent",
            "Shutdown",
            "SendChannelIntent",
            "SendEmail",
            "BeginEmailUpload",
            "AppendEmailUploadChunk",
            "SendEmailUpload",
            "AbortEmailUpload",
            "StartChannelConfigured",
            "ChannelController",
            "ChannelInteraction",
            "CheckChannelHealth",
            "StartChannelSupervised",
            "ReserveRuntime",
            "StartRuntime",
            "StopRuntime",
            "DescribeConfiguredChannels",
            "StartOutboundChannel",
            "PrepareOutboundIntent",
            "PrepareChannelInbound",
            "PrepareChannelIdentity",
            "ExecuteProgressive",
            "ExecuteRemote",
            "PrepareChannelControl",
            "StartRuntimeWithDispatch",
            "PrepareRuntimeCommand"
        ])
    };
}

macro_rules! define_method_table {
    ([$($method:literal),* $(,)?]) => {
        /// All module method names in the declaration order used by the interface.
        pub const METHODS: [&str; 37] = [$($method),*];
    };
}
crate::channel_method_registry!(define_method_table);

/// All host-callback method names in declaration order.
pub const HOST_METHODS: [&str; 13] = [
    host_methods::DELIVER_INBOUND,
    host_methods::REPORT_STATUS,
    host_methods::PERSIST_ALLOWED_IDENTITY,
    host_methods::TRANSCRIBE,
    host_methods::PUBLISH_EVENT,
    host_methods::DISPATCH_CONTROLLER_BACKEND,
    host_methods::PROCESS_INBOUND,
    host_methods::DELIVER_EVENT,
    host_methods::TRANSCRIBE_VOICE,
    host_methods::SYNTHESIZE_SPEECH,
    host_methods::STORE_VOICE_MEDIA,
    host_methods::READ_VOICE_MEDIA,
    host_methods::GET_VOICE_REPLY_POLICY,
];

#[cfg(test)]
#[path = "names_tests.rs"]
mod tests;
