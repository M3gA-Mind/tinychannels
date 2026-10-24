//! The in-app chat as a TinyChannels channel.

use super::inbound::{WEB_CHANNEL_ID, normalize_chat_start};
use super::project::WebTurnProjector;
use super::sink::{WebEventSink, WebScope};
use crate::channel::{
    ChannelAdapter, ChannelDescriptor, ChannelInboundSink, ChannelOutboundIntent, ChannelSendError,
    ChannelSendOutcome, MessageReceipt,
};
use crate::controllers::{ChannelAccountSnapshot, ChannelAccountState};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tinychannels_bus::channel::{
    ChannelRef, ContentPart, ConversationRef, OutboundContentPart, OutboundPayload, SendErrorKind,
};
use tinychannels_bus::module_progressive::ChannelOutputEvent;
use tinychannels_bus::web::WebChatStart;
use tinychannels_bus::web::{WebChannelEvent, WebEventContext};
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ChannelCapabilities, ChannelIngress,
    ContentCapabilities, ContentCapability, EventCapabilities, VoiceCapabilities,
    VoiceTranscriptSource,
};

/// `client_id` stamped on events that no browser client originated.
pub const SYSTEM_CLIENT_ID: &str = "system";

type TurnKey = (String, String, String);

/// The web surface as a v2 [`ChannelAdapter`].
///
/// Inbound, a host forwards each `chat:start` to [`submit`](Self::submit),
/// which normalizes it (typed attachments and voice notes included) and pushes
/// it through the runtime's [`ChannelInboundSink`]. Outbound, the turn runner
/// feeds canonical [`ChannelOutputEvent`]s to
/// [`publish_output`](Self::publish_output); each is projected to a
/// [`WebChannelEvent`] with a per-turn `seq` and published through the host's
/// [`WebEventSink`], scoped by [`WebScope`]. Transport (Socket.IO, SSE, relay)
/// stays behind that sink.
pub struct WebChannelAdapter {
    descriptor: ChannelDescriptor,
    scope: WebScope,
    sink: Arc<dyn WebEventSink>,
    running: AtomicBool,
    clock: fn() -> u64,
    turns: Mutex<HashMap<TurnKey, WebTurnProjector>>,
    publish_lock: tokio::sync::Mutex<()>,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn err(kind: SendErrorKind, message: &str, retryable: bool) -> ChannelSendError {
    ChannelSendError {
        kind,
        message: message.to_owned(),
        retryable,
        ..Default::default()
    }
}

impl WebChannelAdapter {
    pub fn new(account_id: Option<String>, scope: WebScope, sink: Arc<dyn WebEventSink>) -> Self {
        Self {
            descriptor: ChannelDescriptor {
                id: WEB_CHANNEL_ID.to_owned(),
                display_name: "Web".to_owned(),
                account_id,
                ..Default::default()
            },
            scope,
            sink,
            running: AtomicBool::new(false),
            clock: unix_ms,
            turns: Mutex::new(HashMap::new()),
            publish_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Replace the wall clock (Unix milliseconds); tests use a fixed value.
    pub fn with_clock(mut self, clock: fn() -> u64) -> Self {
        self.clock = clock;
        self
    }

    /// Capability facts of the in-app chat.
    pub fn capabilities() -> ChannelCapabilities {
        let media = |kinds: &[&str]| ContentCapability {
            inbound: true,
            outbound: true,
            accepted_types: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
            emitted_types: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
            ..Default::default()
        };
        let text = ContentCapability {
            inbound: true,
            outbound: true,
            ..Default::default()
        };
        let voice = VoiceCapabilities {
            supported: true,
            accepted_codecs: ["webm_opus", "ogg_opus", "wav", "mp3"]
                .map(String::from)
                .into(),
            transcript_sources: vec![VoiceTranscriptSource::HostStt],
            synthesis: true,
            ..Default::default()
        };
        ChannelCapabilities {
            version: ChannelCapabilities::VERSION,
            chat_approvals: true,
            progressive_edits: true,
            streaming_reply_pipeline: true,
            content: ContentCapabilities {
                parts: ["text", "image", "video", "audio", "file", "voice_note"]
                    .map(String::from)
                    .into(),
                text,
                image: media(&["image/*"]),
                video: media(&["video/*"]),
                audio: media(&["audio/*"]),
                file: media(&["*/*"]),
                ..Default::default()
            },
            voice_in: voice.clone(),
            voice_out: voice,
            addressing: AddressingCapabilities {
                conversation_types: vec!["dm".into()],
                thread_id: true,
                ..Default::default()
            },
            events: EventCapabilities {
                supported: ["message", "cancel"].map(String::from).into(),
            },
            actions: ActionCapabilities {
                supported: ["send", "draft-update", "approve"].map(String::from).into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Normalize a web submission and push it to the runtime.
    pub async fn submit(
        &self,
        start: &WebChatStart,
        inbound: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(err(
                SendErrorKind::Transient,
                "web adapter is not running",
                true,
            ));
        }
        let envelope = normalize_chat_start(start, self.descriptor.account_id.as_deref())?;
        tracing::debug!(
            thread_id = %start.thread_id,
            request_id = %start.request_id,
            parts = envelope.parts.len(),
            "web adapter: inbound submission"
        );
        inbound
            .push_ingress(ChannelIngress::Inbound(envelope))
            .await
    }

    /// Project one canonical output event and publish the result.
    ///
    /// Returns how many web events were published (zero when the web wire has
    /// no representation for the event). A terminal event (`chat_done`,
    /// `chat_error`, `chat_cancelled`) ends the turn's `seq` counter, so a
    /// later request with the same id starts again at 1.
    pub async fn publish_output(
        &self,
        client_id: &str,
        thread_id: &str,
        request_id: &str,
        event: ChannelOutputEvent,
    ) -> Result<usize, ChannelSendError> {
        // Keep sequence assignment and sink delivery in one order. The sink is
        // scoped to this adapter instance, so serializing its publications also
        // preserves each turn's increasing sequence numbers on the wire.
        let _publish = self.publish_lock.lock().await;
        let key = (
            client_id.to_owned(),
            thread_id.to_owned(),
            request_id.to_owned(),
        );
        let (projected, terminal) = {
            let mut turns = self.turns.lock();
            let projector = turns
                .entry(key.clone())
                .or_insert_with(|| WebTurnProjector::new(client_id, thread_id, request_id));
            let projected = projector.project(event, (self.clock)());
            let terminal = projected.iter().any(is_terminal);
            (projected, terminal)
        };
        let count = projected.len();
        for event in projected {
            self.sink.publish(&self.scope, event).await?;
        }
        if terminal {
            self.turns.lock().remove(&key);
        }
        Ok(count)
    }

    /// Publish a host-authored event unchanged (queue, goal, todo, error and
    /// other events with no `ChannelOutputEvent` form).
    pub async fn publish_event(&self, mut event: WebChannelEvent) -> Result<(), ChannelSendError> {
        let _publish = self.publish_lock.lock().await;
        let terminal_key = if is_terminal(&event) {
            let context = event.context_mut().clone();
            Some((context.client_id, context.thread_id, context.request_id))
        } else {
            None
        };
        self.sink.publish(&self.scope, event).await?;
        if let Some(key) = terminal_key {
            self.turns.lock().remove(&key);
        }
        Ok(())
    }

    fn proactive_event(
        &self,
        intent: &ChannelOutboundIntent,
    ) -> Result<WebChannelEvent, ChannelSendError> {
        let mut text = String::new();
        let parts: Vec<OutboundContentPart> = match &intent.payload {
            OutboundPayload::Text { text: body } => {
                text.clone_from(body);
                Vec::new()
            }
            OutboundPayload::Voice { voice } => {
                vec![OutboundContentPart::Voice {
                    voice: voice.clone(),
                }]
            }
            OutboundPayload::Parts { parts } => {
                for part in parts {
                    match part {
                        OutboundContentPart::Content { part } => {
                            if !web_content_supported(part) {
                                return Err(ChannelSendError::unsupported(
                                    "web adapter does not declare this content part",
                                ));
                            }
                            if let ContentPart::Text { text: body, .. } = part {
                                if !text.is_empty() {
                                    text.push('\n');
                                }
                                text.push_str(body);
                            }
                        }
                        OutboundContentPart::Voice { .. } => {}
                    }
                }
                parts.clone()
            }
            _ => {
                return Err(ChannelSendError::unsupported(
                    "web adapter does not declare this outbound payload",
                ));
            }
        };
        let thread_id = intent
            .thread_id
            .clone()
            .filter(|thread_id| !thread_id.trim().is_empty())
            .or_else(|| {
                intent
                    .conversation_ref
                    .as_ref()
                    .and_then(|conversation| conversation.thread_id.clone())
                    .filter(|thread_id| !thread_id.trim().is_empty())
            })
            .unwrap_or_else(|| intent.conversation_id.clone());
        Ok(WebChannelEvent::ProactiveMessage {
            context: WebEventContext {
                client_id: SYSTEM_CLIENT_ID.to_owned(),
                thread_id,
                request_id: intent
                    .turn_id
                    .clone()
                    .unwrap_or_else(|| intent.idempotency_key.clone()),
                turn_request_id: None,
                seq: None,
                ts: Some((self.clock)()),
            },
            full_response: (!text.is_empty()).then_some(text),
            message: None,
            success: Some(true),
            persisted_message_id: None,
            parts: (!parts.is_empty()).then_some(parts),
        })
    }
}

fn web_content_supported(part: &ContentPart) -> bool {
    matches!(
        part,
        ContentPart::Text { .. }
            | ContentPart::Image { .. }
            | ContentPart::Video { .. }
            | ContentPart::Audio { .. }
            | ContentPart::File { .. }
            | ContentPart::VoiceNote { .. }
    )
}

fn is_terminal(event: &WebChannelEvent) -> bool {
    matches!(
        event,
        WebChannelEvent::ChatDone { .. }
            | WebChannelEvent::ChatError { .. }
            | WebChannelEvent::ChatCancelled { .. }
    )
}

#[async_trait]
impl ChannelAdapter for WebChannelAdapter {
    fn descriptor(&self) -> ChannelDescriptor {
        self.descriptor.clone()
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        Self::capabilities()
    }

    async fn start(
        &self,
        _sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        let _publish = self.publish_lock.lock().await;
        self.running.store(false, Ordering::SeqCst);
        self.turns.lock().clear();
        Ok(())
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<MessageReceipt, ChannelSendError> {
        self.send_outcome(intent)
            .await
            .map(|outcome| outcome.receipt)
    }

    async fn send_outcome(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        if tinychannels_bus::provider_id(&intent.channel_id) != WEB_CHANNEL_ID
            || intent.conversation_id.trim().is_empty()
        {
            return Err(err(
                SendErrorKind::Forbidden,
                "channel or conversation address does not match the web adapter",
                false,
            ));
        }
        let event = self.proactive_event(&intent)?;
        self.sink.publish(&self.scope, event).await?;
        let channel = ChannelRef {
            id: WEB_CHANNEL_ID.to_owned(),
            account_id: self.descriptor.account_id.clone(),
        };
        let conversation = intent.conversation_ref.clone().unwrap_or(ConversationRef {
            id: intent.conversation_id.clone(),
            thread_id: intent.thread_id.clone(),
            ..Default::default()
        });
        Ok(ChannelSendOutcome {
            receipt: MessageReceipt {
                idempotency_key: Some(intent.idempotency_key.clone()),
                channel: Some(channel),
                conversation: Some(conversation),
                thread_id: intent.thread_id,
                reply_to_id: intent.reply_to_id,
                sent_at: (self.clock)(),
                ..Default::default()
            },
            error: None,
        })
    }

    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        if message.parts.is_empty() {
            return Err(err(
                SendErrorKind::BadFormat,
                "typed outbound message must contain at least one content part",
                false,
            ));
        }
        self.send_outcome(ChannelOutboundIntent {
            idempotency_key: message.idempotency_key.clone(),
            channel_id: message.channel_id.clone(),
            conversation_id: message.conversation_id.clone(),
            channel_ref: message.channel_ref.clone(),
            conversation_ref: message.conversation_ref.clone(),
            reply_to_id: message.reply_to_id.clone(),
            thread_id: message.thread_id.clone(),
            turn_id: message.turn_id.clone(),
            payload: OutboundPayload::Parts {
                parts: message.parts.clone(),
            },
            ..Default::default()
        })
        .await
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        Ok(ChannelAccountSnapshot {
            channel_id: WEB_CHANNEL_ID.to_owned(),
            account_id: self.descriptor.account_id.clone(),
            states: vec![
                ChannelAccountState::Configured,
                if self.running.load(Ordering::SeqCst) {
                    ChannelAccountState::Enabled
                } else {
                    ChannelAccountState::Disabled
                },
            ],
            ..Default::default()
        })
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
