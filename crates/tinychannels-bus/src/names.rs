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
            #[doc = "`StartChannel` — connect and begin receiving on one configured provider."] START_CHANNEL => "StartChannel",
            #[doc = "`StopChannel` — disconnect one running provider."] STOP_CHANNEL => "StopChannel",
            #[doc = "`SendMessage` — deliver one outbound intent through a running provider."] SEND_MESSAGE => "SendMessage",
            #[doc = "`ListChannels` — report every provider this build can serve."] LIST_CHANNELS => "ListChannels",
            #[doc = "`ChannelStatus` — report connection state for one provider."] CHANNEL_STATUS => "ChannelStatus",
            #[doc = "Reserve one idle relay handle before native startup."] RESERVE_RELAY => "ReserveRelay",
            #[doc = "Start a reserved relay using confidential hydrated configuration."] START_RELAY => "StartRelay",
            #[doc = "Start a relay using the canonical v2 identity-scoped capability handshake."] START_RELAY_V2 => "StartRelayV2",
            #[doc = "Stop and join one reserved relay, including pending startup."] STOP_RELAY => "StopRelay",
            #[doc = "Return safe lifecycle facts for one relay reservation."] RELAY_STATUS => "RelayStatus",
            #[doc = "Query negotiated canonical v2 relay capabilities by reservation."] RELAY_CAPABILITIES_V2 => "RelayCapabilitiesV2",
            #[doc = "Submit an ordered relay outbound intent."] SEND_RELAY_INTENT => "SendRelayIntent",
            #[doc = "Freeze publication and drain every owned provider, relay and callback."] SHUTDOWN => "Shutdown",
            #[doc = "`SendChannelIntent` — deliver one typed intent through a running provider."] SEND_CHANNEL_INTENT => "SendChannelIntent",
            #[doc = "`SendEmail` — send host-configured MIME mail without starting IMAP."] SEND_EMAIL => "SendEmail",
            #[doc = "Begin an attachment upload whose chunks remain below TinyBus frame limits."] BEGIN_EMAIL_UPLOAD => "BeginEmailUpload",
            #[doc = "Append one bounded confidential attachment chunk idempotently."] APPEND_EMAIL_UPLOAD_CHUNK => "AppendEmailUploadChunk",
            #[doc = "Send a completed upload once and replay its terminal result by handle."] SEND_EMAIL_UPLOAD => "SendEmailUpload",
            #[doc = "Abort an upload that has not entered SMTP delivery."] ABORT_EMAIL_UPLOAD => "AbortEmailUpload",
            #[doc = "Start a provider with host-selected HTTP proxy policy."] START_CHANNEL_CONFIGURED => "StartChannelConfigured",
            #[doc = "Dispatch typed controller work through TinyChannels' owning manager."] CHANNEL_CONTROLLER => "ChannelController",
            #[doc = "Run typing, draft and capability operations on one running provider."] CHANNEL_INTERACTION => "ChannelInteraction",
            #[doc = "Check configured providers without starting listeners; inputs are confidential."] CHECK_CHANNEL_HEALTH => "CheckChannelHealth",
            #[doc = "Start a provider with module-owned reconnect supervision and host-selected timing."] START_CHANNEL_SUPERVISED => "StartChannelSupervised",
            #[doc = "Reserve one inert identity-scoped runtime with immutable callback paths."] RESERVE_RUNTIME => "ReserveRuntime",
            #[doc = "Activate the private provider object for a known runtime handle."] START_RUNTIME => "StartRuntime",
            #[doc = "Stop and join all resources belonging to one runtime handle."] STOP_RUNTIME => "StopRuntime",
            #[doc = "Resolve provider facts from confidential hydrated configuration without listening."] DESCRIBE_CONFIGURED_CHANNELS => "DescribeConfiguredChannels",
            #[doc = "Keep an external-webhook provider available for outbound operations."] START_OUTBOUND_CHANNEL => "StartOutboundChannel",
            #[doc = "Normalize legacy messages and produce deterministic intent keys in the module."] PREPARE_OUTBOUND_INTENT => "PrepareOutboundIntent",
            #[doc = "Prepare normalized inbound facts and persisted keys inside the module."] PREPARE_CHANNEL_INBOUND => "PrepareChannelInbound",
            #[doc = "Derive hosted-ingress thread and client identities inside the module."] PREPARE_CHANNEL_IDENTITY => "PrepareChannelIdentity",
            #[doc = "Drive a module-owned progressive reply through an opaque handle."] EXECUTE_PROGRESSIVE => "ExecuteProgressive",
            #[doc = "Run module-owned remote commands and persisted turn state."] EXECUTE_REMOTE => "ExecuteRemote",
            #[doc = "Prepare module-owned channel facts and runtime command responses."] PREPARE_CHANNEL_CONTROL => "PrepareChannelControl",
            #[doc = "Activate an identity runtime with bounded `ProcessInbound` turn callbacks."] START_RUNTIME_WITH_DISPATCH => "StartRuntimeWithDispatch",
            #[doc = "Parse model/provider commands and render help without changing host state."] PREPARE_RUNTIME_COMMAND => "PrepareRuntimeCommand",
            #[doc = "Begin bounded media ingestion for an active configured provider."] BEGIN_MEDIA_UPLOAD => "BeginMediaUpload",
            #[doc = "Append one idempotent confidential media chunk."] APPEND_MEDIA_UPLOAD_CHUNK => "AppendMediaUploadChunk",
            #[doc = "Persist a complete upload as a scoped opaque media reference."] FINISH_MEDIA_UPLOAD => "FinishMediaUpload",
            #[doc = "Abort a buffered media upload; retries are idempotent."] ABORT_MEDIA_UPLOAD => "AbortMediaUpload",
        ]);
    };
}

macro_rules! define_method_table {
    ([$($(#[$doc:meta])* $name:ident => $method:literal),* $(,)?]) => {
        pub mod methods { $($(#[$doc])* pub const $name: &str = $method;)* }
        /// All module method names in the declaration order used by the interface.
        pub const METHODS: [&str; 41] = [$(methods::$name),*];
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
