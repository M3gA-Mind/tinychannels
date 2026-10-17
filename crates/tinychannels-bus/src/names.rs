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
            START_CHANNEL => "StartChannel",
            STOP_CHANNEL => "StopChannel",
            SEND_MESSAGE => "SendMessage",
            LIST_CHANNELS => "ListChannels",
            CHANNEL_STATUS => "ChannelStatus",
            RESERVE_RELAY => "ReserveRelay",
            START_RELAY => "StartRelay",
            START_RELAY_V2 => "StartRelayV2",
            STOP_RELAY => "StopRelay",
            RELAY_STATUS => "RelayStatus",
            RELAY_CAPABILITIES_V2 => "RelayCapabilitiesV2",
            SEND_RELAY_INTENT => "SendRelayIntent",
            SHUTDOWN => "Shutdown",
            SEND_CHANNEL_INTENT => "SendChannelIntent",
            SEND_EMAIL => "SendEmail",
            BEGIN_EMAIL_UPLOAD => "BeginEmailUpload",
            APPEND_EMAIL_UPLOAD_CHUNK => "AppendEmailUploadChunk",
            SEND_EMAIL_UPLOAD => "SendEmailUpload",
            ABORT_EMAIL_UPLOAD => "AbortEmailUpload",
            START_CHANNEL_CONFIGURED => "StartChannelConfigured",
            CHANNEL_CONTROLLER => "ChannelController",
            CHANNEL_INTERACTION => "ChannelInteraction",
            CHECK_CHANNEL_HEALTH => "CheckChannelHealth",
            START_CHANNEL_SUPERVISED => "StartChannelSupervised",
            RESERVE_RUNTIME => "ReserveRuntime",
            START_RUNTIME => "StartRuntime",
            STOP_RUNTIME => "StopRuntime",
            DESCRIBE_CONFIGURED_CHANNELS => "DescribeConfiguredChannels",
            START_OUTBOUND_CHANNEL => "StartOutboundChannel",
            PREPARE_OUTBOUND_INTENT => "PrepareOutboundIntent",
            PREPARE_CHANNEL_INBOUND => "PrepareChannelInbound",
            PREPARE_CHANNEL_IDENTITY => "PrepareChannelIdentity",
            EXECUTE_PROGRESSIVE => "ExecuteProgressive",
            EXECUTE_REMOTE => "ExecuteRemote",
            PREPARE_CHANNEL_CONTROL => "PrepareChannelControl",
            START_RUNTIME_WITH_DISPATCH => "StartRuntimeWithDispatch",
            PREPARE_RUNTIME_COMMAND => "PrepareRuntimeCommand"
        ]);
    };
}

macro_rules! define_method_table {
    ([$($name:ident => $method:literal),* $(,)?]) => {
        #[allow(missing_docs, reason = "generated from the canonical module method registry")]
        pub mod methods { $(pub const $name: &str = $method;)* }
        /// All module method names in the declaration order used by the interface.
        pub const METHODS: [&str; 37] = [$(methods::$name),*];
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
