//! Auxiliary host capabilities beyond the core turn dispatcher.
//!
//! Each capability is an independent, object-safe trait. A host implements
//! only the ones it can back; a provider queries [`super::ChannelHost`] for
//! the ones it needs. Every DTO here is portable — no dependency on the
//! host's config or RPC envelope types.

use async_trait::async_trait;

use super::{MediaScope, MediaStoreRequest, MediaStreamRequest, StoredMedia};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceCodecRequest {
    pub audio_base64: String,
    pub from: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VoiceCodecResult {
    pub audio_base64: String,
    pub duration_ms: Option<u64>,
    pub waveform: Option<Vec<f32>>,
}

#[async_trait]
pub trait VoiceCodec: Send + Sync {
    async fn transcode(&self, request: VoiceCodecRequest) -> anyhow::Result<VoiceCodecResult>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceMediaRequest {
    pub audio_base64: String,
    pub mime_type: String,
    pub codec: Option<String>,
    pub duration_ms: Option<u64>,
    pub file_name: Option<String>,
    pub direction: tinychannels_bus::HostVoiceMediaDirection,
    pub scope: tinychannels_bus::HostVoiceScope,
}

#[async_trait]
pub trait VoiceMediaStore: Send + Sync {
    async fn store_voice(
        &self,
        request: VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult>;

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        anyhow::bail!("host voice media resolution is unavailable")
    }
}

/// Durable channel media persistence with account, conversation, and turn
/// checks enforced by the store on every operation.
#[async_trait]
pub trait MediaRepository: Send + Sync {
    /// Store a fully buffered object and return a host-owned opaque reference.
    async fn store_media(
        &self,
        request: MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference>;

    /// Stream an untrusted source under its declared byte limit before storing.
    async fn store_media_stream(
        &self,
        _request: MediaStreamRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        anyhow::bail!("media repository does not support streamed storage")
    }

    /// Resolve an opaque reference only for its exact storage scope.
    async fn read_media(
        &self,
        scope: MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<StoredMedia>;

    /// Delete one object only after validating its stored scope.
    async fn delete_media(&self, _scope: MediaScope, _opaque_id: String) -> anyhow::Result<bool> {
        anyhow::bail!("media repository does not support deletion")
    }
}

// ---------------------------------------------------------------------------
// Voice: speech-to-text
// ---------------------------------------------------------------------------

/// A voice note / audio blob to transcribe.
#[derive(Debug, Clone, PartialEq)]
pub struct TranscriptionRequest {
    /// Base64-encoded audio bytes.
    pub audio_base64: String,
    /// MIME hint (`"audio/ogg"`, `"audio/mp4"`); providers may ignore it.
    pub mime_type: Option<String>,
    /// Original file name hint.
    pub file_name: Option<String>,
    /// BCP-47 language hint; `None` = auto-detect.
    pub language: Option<String>,
}

/// The transcribed text plus optional metadata.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TranscriptionResult {
    pub text: String,
    pub language: Option<String>,
    pub duration_secs: Option<f64>,
}

/// Speech-to-text. Needed by: **telegram** (voice notes), **web** (PTT).
#[async_trait]
pub trait Transcriber: Send + Sync {
    /// Stable id used in logs/config (`"cloud"`, `"whisper"`).
    fn name(&self) -> &str;
    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> anyhow::Result<TranscriptionResult>;

    async fn transcribe_scoped(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        request: TranscriptionRequest,
    ) -> anyhow::Result<TranscriptionResult> {
        self.transcribe(request).await
    }
}

// ---------------------------------------------------------------------------
// Voice: text-to-speech
// ---------------------------------------------------------------------------

/// Text to synthesize into spoken audio.
#[derive(Debug, Clone, PartialEq)]
pub struct SpeechRequest {
    pub text: String,
    /// Named voice; `None` uses the host default.
    pub voice: Option<String>,
    /// Desired container (`"mp3"`, `"wav"`); `None` = host default.
    pub format: Option<String>,
    /// Conversation/thread id, so the host can scope voice settings.
    pub thread_id: Option<String>,
}

/// Synthesized audio plus optional viseme alignment for lip-sync.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SpeechResult {
    pub audio_base64: String,
    pub mime_type: String,
    pub duration_ms: Option<u64>,
    /// Provider-specific viseme timeline (mascot lip-sync); opaque JSON.
    pub visemes: Option<serde_json::Value>,
}

/// Text-to-speech. Needed by: **web** (spoken replies), voice surfaces.
#[async_trait]
pub trait SpeechSynthesizer: Send + Sync {
    async fn synthesize(&self, request: SpeechRequest) -> anyhow::Result<SpeechResult>;

    async fn synthesize_scoped(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        request: SpeechRequest,
    ) -> anyhow::Result<SpeechResult> {
        self.synthesize(request).await
    }
}

// ---------------------------------------------------------------------------
// Approvals
// ---------------------------------------------------------------------------

/// A human-in-the-loop approval prompt raised mid-turn.
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalAsk {
    pub id: String,
    pub prompt: String,
    /// Allowed choices; an empty list implies a binary approve/deny.
    pub choices: Vec<String>,
    /// Session this approval belongs to (routes the reply back).
    pub session_key: String,
    /// Auto-deny after this many seconds; `None` = host default TTL.
    pub timeout_secs: Option<u64>,
}

/// The resolution of an [`ApprovalAsk`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalDecision {
    Approve,
    Deny,
    /// A specific choice was selected (multi-option approvals).
    Choice(String),
    /// No response within the TTL.
    Timeout,
}

/// Approval gate. Needed by: **telegram, web** (and any channel that parks
/// interactive turns for confirmation).
#[async_trait]
pub trait ApprovalGate: Send + Sync {
    /// Raise an approval and await its resolution (blocks the turn).
    ///
    /// Defaults to an "unsupported" error: in OpenHuman today approvals are
    /// *raised* by the tool gate (host-internal) and channels only observe the
    /// surface + [`ApprovalGate::parse_reply`] inbound replies. Hosts that can
    /// mediate interactive requests override this.
    async fn request(&self, _ask: ApprovalAsk) -> anyhow::Result<ApprovalDecision> {
        anyhow::bail!("interactive approval requests are not supported by this host")
    }

    /// Interpret a free-text inbound reply as an approval decision
    /// (`"yes"`/`"no"`/`"1"`), or `None` if it isn't one. Default: no parse.
    fn parse_reply(&self, _message: &str) -> Option<ApprovalDecision> {
        None
    }
}

// ---------------------------------------------------------------------------
// Reaction gate (should the assistant respond at all?)
// ---------------------------------------------------------------------------

/// A candidate inbound message the host may choose to ignore.
#[derive(Debug, Clone, PartialEq)]
pub struct ReactionQuery {
    pub message: String,
    /// Channel kind (`"telegram"`, `"discord"`) for policy tuning.
    pub channel_type: String,
}

/// Whether the assistant should react, plus the emoji to use for an emoji
/// reaction (`presentation`/`telegram` ACK reactions) and an optional rationale.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReactionDecision {
    pub should_react: bool,
    /// Emoji to react with when `should_react` is true (`None` = no emoji,
    /// e.g. a pure suppress/allow gate). Mirrors OpenHuman's `ReactionDecision`.
    pub emoji: Option<String>,
    /// Optional human-readable rationale for logs/telemetry.
    pub reason: Option<String>,
}

/// Inference-driven reaction gate: whether to respond/react and with what
/// emoji. Backs both group-chat suppression and emoji-ACK reactions.
/// Needed by: **web, telegram, presentation**.
#[async_trait]
pub trait ReactionGate: Send + Sync {
    async fn should_react(&self, query: ReactionQuery) -> anyhow::Result<ReactionDecision>;
}

// ---------------------------------------------------------------------------
// Conversation history store
// ---------------------------------------------------------------------------

/// One stored conversation turn.
#[derive(Debug, Clone, PartialEq)]
pub struct ConversationMessage {
    /// `"user"` / `"assistant"` / `"system"`.
    pub role: String,
    pub content: String,
    /// Epoch millis; `None` if the store assigns its own.
    pub timestamp: Option<u64>,
}

/// Durable per-session conversation history. Complements the semantic
/// [`crate::context::Memory`] recall trait (which stays the vector/recall
/// side). Needed by: **web, telegram** (multi-turn context).
#[async_trait]
pub trait ConversationStore: Send + Sync {
    /// Most-recent-last history for a session, capped at `limit`.
    async fn history(
        &self,
        session_key: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<ConversationMessage>>;

    /// Append a message to a session's history.
    async fn append(&self, session_key: &str, message: ConversationMessage) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// Event sink (domain event bus)
// ---------------------------------------------------------------------------

/// Fire-and-forget publish onto the host's domain event bus. Providers use
/// it to announce inbound receipt, delivery, remote-control actions, etc.
/// Needed by: **web, telegram** (event-bus fan-out, remote control).
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Publish a JSON payload under a `domain` (`"channel"`, `"agent"`) and
    /// `kind` (`"inbound"`, `"delivered"`). Non-fatal; hosts log failures.
    async fn publish(
        &self,
        domain: &str,
        kind: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<()>;

    /// Publish one normalized channel event. Legacy hosts keep working through
    /// the default projection while module hosts override this with DeliverEvent.
    async fn publish_channel_event(
        &self,
        event: tinychannels_bus::ChannelEvent,
    ) -> anyhow::Result<()> {
        let kind = match &event.event {
            tinychannels_bus::ChannelEventKind::Typing { .. } => "typing",
            tinychannels_bus::ChannelEventKind::ReadReceipt { .. } => "read_receipt",
            tinychannels_bus::ChannelEventKind::DeliveredReceipt { .. } => "delivered_receipt",
            tinychannels_bus::ChannelEventKind::ReactionAdded { .. } => "reaction_added",
            tinychannels_bus::ChannelEventKind::ReactionRemoved { .. } => "reaction_removed",
            tinychannels_bus::ChannelEventKind::OutboundReaction { .. } => "outbound_reaction",
            tinychannels_bus::ChannelEventKind::MessageUpdated { .. } => "message_updated",
            tinychannels_bus::ChannelEventKind::MessageDeleted { .. } => "message_deleted",
            tinychannels_bus::ChannelEventKind::Presence { .. } => "presence",
            tinychannels_bus::ChannelEventKind::Lifecycle { .. } => "lifecycle",
            tinychannels_bus::ChannelEventKind::ButtonCallback { .. } => "button_callback",
        };
        self.publish("channel", kind, serde_json::to_value(event)?)
            .await
    }

    /// Submit an inbound message or event on one ordered provider path.
    /// `false` asks the legacy listener adapter to use its message channel.
    async fn publish_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> anyhow::Result<bool> {
        match ingress {
            tinychannels_bus::ChannelIngress::Event(event) => {
                self.publish_channel_event(event).await?;
                Ok(true)
            }
            tinychannels_bus::ChannelIngress::Inbound(_) => Ok(false),
        }
    }

    /// Deliver one provider message on the ordered v22 ingress path.
    /// Legacy hosts return `false` and continue receiving the v21 message.
    #[allow(
        deprecated,
        reason = "this default bridge preserves the one-release ChannelMessage adapter"
    )]
    async fn publish_inbound(
        &self,
        _message: tinychannels_bus::ChannelMessage,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Lifecycle registry (graceful shutdown)
// ---------------------------------------------------------------------------

/// The future a [`ShutdownHook`] returns when invoked.
pub type ShutdownFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

/// A one-shot **async** cleanup callback run at process shutdown. Async so a
/// hook can await real teardown (close a WebSocket, flush a session) — mirrors
/// the host's own async shutdown-hook contract.
pub type ShutdownHook = Box<dyn FnOnce() -> ShutdownFuture + Send + 'static>;

/// Register cleanup that must run when the host shuts down (close sockets,
/// flush sessions). Needed by: **whatsapp_web** (WA session teardown), any
/// provider holding a long-lived native connection.
pub trait LifecycleRegistry: Send + Sync {
    /// Register a named shutdown hook. Names aid logging/dedupe.
    fn register_shutdown(&self, name: &str, hook: ShutdownHook);
}

// ---------------------------------------------------------------------------
// Allowlist store (persisted access control)
// ---------------------------------------------------------------------------

/// Persist a newly-authorized identity into a channel's configured allowlist so
/// it survives restarts. Needed by: **telegram** (first-run bind-code pairing
/// promotes the paired account into `allowed_users`). The host owns the config
/// file; the provider only names the channel + identity.
#[async_trait]
pub trait AllowlistStore: Send + Sync {
    /// Add `identity` to `channel`'s persisted allowlist (idempotent). Errors
    /// if the channel has no config section to persist into.
    async fn persist_allowed_identity(&self, channel: &str, identity: &str) -> anyhow::Result<()>;
}

/// A handle to the host's durable storage, on the tinystoragedrivers document
/// port. Needed by: **whatsapp_web** (the linked-device session survives a
/// restart instead of asking for a new QR scan). The handle is already scoped
/// by the host; a provider never names a scope, database or backend.
///
/// Resolution is async because a loaded module reaches storage over the bus
/// and connects on first use.
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// The scoped document store, or why it is unavailable.
    async fn documents(
        &self,
    ) -> anyhow::Result<std::sync::Arc<dyn tinystoragedrivers_api::DocumentStore>>;
}

// ---------------------------------------------------------------------------
// Run ledger (telemetry / observability)
// ---------------------------------------------------------------------------

/// Create/update a run row in the host's ledger.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RunUpsert {
    pub run_id: String,
    pub session_key: String,
    pub channel: String,
    /// `"running"` / `"succeeded"` / `"failed"` / `"cancelled"`.
    pub status: String,
    /// Optional human-facing title/summary.
    pub title: Option<String>,
}

/// Append a discrete event to a run's timeline.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RunEventAppend {
    pub run_id: String,
    pub kind: String,
    pub payload: serde_json::Value,
}

/// Token/cost telemetry for a run.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RunTelemetry {
    pub run_id: String,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub cost_usd: Option<f64>,
}

/// Observability sink for agent runs. Needed by: **web** (run ledger,
/// progress tracing, telemetry). Optional everywhere else.
#[async_trait]
pub trait RunLedger: Send + Sync {
    async fn upsert_run(&self, run: RunUpsert) -> anyhow::Result<()>;
    async fn append_event(&self, event: RunEventAppend) -> anyhow::Result<()>;
    async fn record_telemetry(&self, telemetry: RunTelemetry) -> anyhow::Result<()>;
}

#[cfg(test)]
#[path = "services_tests.rs"]
mod tests;
