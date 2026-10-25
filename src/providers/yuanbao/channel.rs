//! Channel facade for the Yuanbao provider.
//!
//! This module owns the provider's typed [`ChannelAdapter`](crate::channel::ChannelAdapter) implementation and keeps
//! provider wiring out of `mod.rs`. Protocol decoding, transport, inbound
//! filtering, and outbound sending remain delegated to sibling modules.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tinychannels_runtime::config::YuanbaoConfigExt as _;

use tokio::sync::{Mutex as TokioMutex, mpsc, watch};
use tokio::task::JoinHandle;
#[cfg(test)]
use tracing::info;
use tracing::warn;

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
#[cfg(test)]
use crate::traits::ChannelMessage;

use super::config::YuanbaoConfig;
use super::connection::{InboundEvent, YuanbaoConnection};
use super::ids::{shorten_account_id, shorten_reply_target};
use super::inbound::{InboundPipeline, PipelineOutcome, PipelineState};
use super::outbound::OutboundSender;
use super::proto::decode_push_msg;
use super::sign::SignManager;
use super::{splitter, types};

/// Reply Heartbeat keepalive interval. The yuanbao gateway expects the
/// bot to ping (`SendPrivateHeartbeat RUNNING`) at this cadence so the
/// "正在输入" indicator stays alive for long-running responses.
const REPLY_HEARTBEAT_INTERVAL_SECS: u64 = 2;

/// Hard ceiling on the in-memory shortened-recipient → original-recipient
/// map. Each entry is two short strings (~80 B), so 4096 distinct senders
/// give ~320 KB — plenty for any realistic chat load and small enough
/// that we can blow the whole map away when we hit the cap instead of
/// dragging in an LRU dependency. See `register_recipient_alias`.
const RECIPIENT_ALIAS_CAP: usize = 4096;

type ListenerSlot = Arc<std::sync::Mutex<Option<YuanbaoListenerState>>>;

struct YuanbaoListenerState {
    id: u64,
    cancel: watch::Sender<bool>,
    connection_started: bool,
    connection_done: bool,
    abandoned: bool,
}

/// The yuanbao channel — owns one WebSocket and one inbound pipeline.
pub struct YuanbaoChannel {
    config: YuanbaoConfig,
    connection: Arc<YuanbaoConnection>,
    outbound: Arc<OutboundSender>,
    pipeline: Arc<InboundPipeline>,
    shutdown_tx: watch::Sender<bool>,
    /// Serializes ownership of the current connection's inbound receiver.
    inbound_rx: TokioMutex<Option<mpsc::UnboundedReceiver<InboundEvent>>>,
    /// Per-recipient Reply Heartbeat keepalive tasks (started on `start_typing`).
    heartbeat_tasks: TokioMutex<HashMap<String, JoinHandle<()>>>,
    /// Reverse lookup table from shortened recipient ids (the ones we
    /// emit on `ChannelMessage.sender` / `reply_target`) back to the
    /// original server-recognized ids that outbound `send_c2c_message`
    /// / `send_group_message` must use as `to_account` / `group_code`.
    ///
    /// Why this exists: yuanbao uids are ~64-char hashes, and
    /// `super::ids::shorten_account_id` rewrites them as
    /// `<prefix>_<sha256-16hex>` so the conversation store's per-thread
    /// JSONL filenames stay under filesystem `NAME_MAX`. Without this
    /// table the agent loop sends replies addressed to the shortened
    /// hash, which the yuanbao gateway silently drops because no such
    /// user exists. See `register_recipient_alias` / `resolve_recipient`.
    recipient_aliases: TokioMutex<HashMap<String, String>>,
    /// Host-scoped services used by the native typed-send adapter.
    pub(super) native: NativeState,
    voice_http: reqwest::Client,
    /// Single-owner cancellation for the native typed listener.
    listener: ListenerSlot,
    listener_generation: std::sync::atomic::AtomicU64,
}

impl YuanbaoChannel {
    async fn start_typed_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

        let id = self
            .listener_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let (cancel, mut cancellation) = watch::channel(false);
        {
            let mut active = self
                .listener
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if active.is_some() {
                return Err(ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "Yuanbao listener is already running".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            *active = Some(YuanbaoListenerState {
                id,
                cancel: cancel.clone(),
                connection_started: false,
                connection_done: false,
                abandoned: false,
            });
        }
        let _registration = YuanbaoListenerRegistration {
            listener: Arc::clone(&self.listener),
            id,
            cancel: cancel.clone(),
        };
        self.shutdown_tx.send_replace(false);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        self.connection.set_inbound_sender(inbound_tx);
        *self.inbound_rx.lock().await = Some(inbound_rx);
        let mut inbound_rx = self
            .inbound_rx
            .lock()
            .await
            .take()
            .expect("new listener receiver");
        let connection = Arc::clone(&self.connection);
        let connection_cancel = cancellation.clone();
        let completion = YuanbaoConnectionRegistration {
            listener: Arc::clone(&self.listener),
            id,
        };
        let mut connection_task = tokio::spawn(async move {
            let _completion = completion;
            connection.run(connection_cancel).await;
        });
        if let Some(active) = self
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
            .filter(|active| active.id == id)
        {
            active.connection_started = true;
        }

        loop {
            tokio::select! {
                biased;
                _ = cancellation.changed() => break,
                event = inbound_rx.recv() => match event {
                    Some(InboundEvent::Push(frame)) => {
                        tokio::select! {
                            biased;
                            _ = cancellation.changed() => break,
                            result = self.dispatch_push_typed(frame, sink) => {
                                if let Err(error) = result {
                                    cancel.send_replace(true);
                                    if tokio::time::timeout(Duration::from_secs(2), &mut connection_task).await.is_err() {
                                        connection_task.abort();
                                        let _ = connection_task.await;
                                    }
                                    return Err(error);
                                }
                            }
                        }
                    }
                    Some(InboundEvent::Kickout(reason)) => {
                        warn!("[yuanbao] kickout: {} — stopping typed listener", reason);
                        break;
                    }
                    None => break,
                }
            }
        }
        cancel.send_replace(true);
        if tokio::time::timeout(Duration::from_secs(2), &mut connection_task)
            .await
            .is_err()
        {
            connection_task.abort();
            let _ = connection_task.await;
        }
        Ok(())
    }

    fn stop_typed_listener(&self) {
        if let Some(active) = self
            .listener
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            active.cancel.send_replace(true);
        }
    }

    /// Build a channel from a validated config. Returns an error if the
    /// config is missing required fields (so misconfiguration surfaces
    /// at startup, not on the first inbound message).
    pub fn new(mut config: YuanbaoConfig) -> anyhow::Result<Self> {
        config.apply_env_defaults();
        config.validate().map_err(anyhow::Error::msg)?;
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel::<InboundEvent>();

        // SignManager is only useful when we have an app_secret — without
        // it we'd never call the sign endpoint anyway.
        let sign_manager: Option<Arc<SignManager>> = if !config.app_secret.is_empty() {
            Some(SignManager::new(reqwest::Client::new()))
        } else {
            None
        };

        let connection = YuanbaoConnection::new(config.clone(), inbound_tx, sign_manager.clone());
        let outbound = Arc::new(OutboundSender::new(
            Arc::clone(&connection),
            sign_manager.clone(),
            config.app_key.clone(),
            config.bot_id.clone(),
        ));
        // PipelineState's `from_account` is used by the echo-guard stage to
        // drop self-sent messages. We feed it the static config value here
        // (which may be empty); the canonical server-issued bot_id only
        // becomes known after sign-token, so this is a known minor gap —
        // echo guard will simply not fire when bot_id isn't statically set.
        let pipeline_state = PipelineState::new(&config, config.bot_id.clone());
        let pipeline = Arc::new(InboundPipeline::new(pipeline_state));

        Ok(Self {
            config,
            connection,
            outbound,
            pipeline,
            shutdown_tx,
            inbound_rx: TokioMutex::new(Some(inbound_rx)),
            heartbeat_tasks: TokioMutex::new(HashMap::new()),
            recipient_aliases: TokioMutex::new(HashMap::new()),
            native: NativeState::default(),
            voice_http: no_redirect_client(),
            listener: Arc::new(std::sync::Mutex::new(None)),
            listener_generation: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Enable typed image and file sends through host-scoped media storage.
    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Enable URL-backed inbound TIMSound storage through the host voice port.
    /// Yuanbao still has no verified outbound audio element.
    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Publish native inbound events through the host's ordered ingress port.
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// Attach host speech-to-text for voice notes without a transcript.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_voice_http_client(mut self, client: reqwest::Client) -> Self {
        self.voice_http = client;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_outbound(mut self, outbound: Arc<OutboundSender>) -> Self {
        self.outbound = outbound;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_cos_endpoint(mut self, endpoint: String) -> Self {
        Arc::get_mut(&mut self.outbound)
            .expect("factory-built Yuanbao outbound has a single owner in tests")
            .set_test_cos_endpoint(endpoint);
        self
    }

    /// Record a `shortened → original` recipient mapping so the outbound
    /// side can recover the server-recognized id when the agent loop
    /// addresses a reply with the shortened sender / reply_target it
    /// received on `ChannelMessage`.
    ///
    /// No-op when the two are equal (uid is short enough to skip
    /// shortening, or this is the `g:` group-target case where the
    /// inner code is short). When the map crosses `RECIPIENT_ALIAS_CAP`
    /// we clear it — the next inbound message from each active sender
    /// re-populates the entry it needs, and stale entries from idle
    /// conversations are fine to lose.
    async fn register_recipient_alias(&self, shortened: &str, original: &str) {
        if shortened == original {
            return;
        }
        let mut m = self.recipient_aliases.lock().await;
        if m.len() >= RECIPIENT_ALIAS_CAP {
            warn!(
                "[yuanbao] recipient alias map hit cap ({}), clearing",
                RECIPIENT_ALIAS_CAP
            );
            m.clear();
        }
        m.insert(shortened.to_string(), original.to_string());
    }

    /// Look up the server-recognized recipient for a (possibly
    /// shortened) inbound id. Falls back to the input unchanged when
    /// nothing is registered — which keeps the previous behavior for
    /// recipients that don't go through `shorten_account_id` (short
    /// uids, group codes, `imessage`-style ids).
    async fn resolve_recipient(&self, recipient: &str) -> String {
        let m = self.recipient_aliases.lock().await;
        m.get(recipient)
            .cloned()
            .unwrap_or_else(|| recipient.to_string())
    }

    fn split_message(&self, text: &str) -> Vec<String> {
        splitter::split_markdown(text, self.config.max_message_length)
    }

    async fn start_heartbeat_task(&self, recipient: &str) {
        let mut tasks = self.heartbeat_tasks.lock().await;
        if tasks.contains_key(recipient) {
            return;
        }
        let outbound = Arc::clone(&self.outbound);
        let target = recipient.to_string();
        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(REPLY_HEARTBEAT_INTERVAL_SECS));
            interval.tick().await; // skip first tick (start_typing already sent RUNNING)
            loop {
                interval.tick().await;
                if let Err(e) = outbound.start_heartbeat(&target).await {
                    // Connection bouncing — bail out of this loop; the
                    // next start_typing call will spawn a new one.
                    warn!(
                        "[yuanbao] reply heartbeat send failed: {} — stopping loop",
                        e
                    );
                    return;
                }
            }
        });
        tasks.insert(recipient.to_string(), handle);
    }

    async fn stop_heartbeat_task(&self, recipient: &str) {
        let mut tasks = self.heartbeat_tasks.lock().await;
        if let Some(handle) = tasks.remove(recipient) {
            handle.abort();
        }
    }
}

impl YuanbaoChannel {
    pub(crate) async fn set_typing_native(
        &self,
        recipient: &str,
        start: bool,
    ) -> anyhow::Result<()> {
        let recipient = self.resolve_recipient(recipient).await;
        if start {
            // Send RUNNING immediately, then spawn a keepalive so the
            // indicator does not expire while a response is generated.
            self.outbound.start_heartbeat(&recipient).await?;
            self.start_heartbeat_task(&recipient).await;
        } else {
            self.stop_heartbeat_task(&recipient).await;
            self.outbound.stop_heartbeat(&recipient).await?;
        }
        Ok(())
    }
    #[cfg(test)]
    async fn dispatch_push(&self, frame: types::ConnFrame, tx: &mpsc::Sender<ChannelMessage>) {
        // The Yuanbao gateway pushes inbound messages with `cmd_type=Push`;
        // the actual `cmd` word is decided server-side and varies (mirrors
        // hermes-agent `yuanbao.py::_handle_received_frame` which routes
        // purely on cmd_type). The connection layer has already filtered
        // out non-push frames before we get here, so every frame we see
        // should be a candidate for the inbound pipeline.
        if frame.data.is_empty() {
            tracing::trace!("[yuanbao] empty push body cmd={} — skipping", frame.cmd);
            return;
        }
        // Some push frames wrap the biz body in an extra
        // `PushMsg { cmd, module, msg_id, data }` envelope; others (e.g.
        // cmd="inbound_message", module="yuanbao_openclaw_proxy") put the
        // InboundMessagePush bytes directly in `ConnMsg.data` with the
        // ConnMsg.head already carrying cmd/module. Mirrors plugin
        // client.ts::onPush (l. 813): try PushMsg first, but only accept
        // it when it has a non-empty cmd or module; otherwise treat the
        // raw frame.data as the biz body.
        let unwrapped: Option<Vec<u8>> = match decode_push_msg(&frame.data) {
            Ok(p) if (!p.cmd.is_empty() || !p.module.is_empty()) && !p.data.is_empty() => {
                info!(
                    "[yuanbao] push envelope decoded: cmd={} module={} msg_id={} biz_len={}",
                    p.cmd,
                    p.module,
                    p.msg_id,
                    p.data.len()
                );
                Some(p.data)
            }
            _ => {
                info!(
                    "[yuanbao] push has no PushMsg envelope — treating ConnMsg.data as biz body (conn_cmd={} module={} len={})",
                    frame.cmd,
                    frame.module,
                    frame.data.len()
                );
                None
            }
        };
        let biz_body: &[u8] = unwrapped.as_deref().unwrap_or(&frame.data);
        let outcome = self.pipeline.process(biz_body).await;
        match outcome {
            PipelineOutcome::Dispatch(ctx) => {
                if self.publish_voice(&ctx).await {
                    return;
                }
                if ctx.kind == super::types::MessageKind::Voice {
                    self.pipeline.release_message(&ctx.msg.msg_id).await;
                    warn!(
                        message_id = %ctx.msg.msg_id,
                        "[yuanbao] voice event could not be stored and published through the typed host path"
                    );
                    return;
                }
                // Shorten ids at the channel boundary so the composite thread_id
                // derived downstream (channel:yuanbao_<sender>_<reply_target>)
                // stays under filesystem NAME_MAX once hex-encoded for the
                // per-thread JSONL filename. Yuanbao internals (echo guard,
                // access control, owner-command check) keep the original
                // `from_account` — see `super::ids` for the format and rationale.
                let original_from = ctx.msg.from_account.clone();
                let original_reply_target = ctx.source.reply_target();
                let short_sender = shorten_account_id(&original_from);
                let short_reply_target = shorten_reply_target(&original_reply_target);
                // Remember the original ids so the outbound side can
                // recover them when the agent loop addresses a reply
                // with the shortened values it sees here.
                self.register_recipient_alias(&short_sender, &original_from)
                    .await;
                self.register_recipient_alias(&short_reply_target, &original_reply_target)
                    .await;
                let msg = ChannelMessage {
                    id: ctx.msg.msg_id.clone(),
                    sender: short_sender,
                    reply_target: short_reply_target,
                    content: if ctx.text.is_empty() && !ctx.image_urls.is_empty() {
                        // Surface image URLs as content so downstream tools have something to work with.
                        ctx.image_urls.join("\n")
                    } else {
                        ctx.text.clone()
                    },
                    channel: "yuanbao".into(),
                    timestamp: ctx.msg.msg_time as u64,
                    topic_id: None,
                    reply_to_id: None,
                    thread_ts: None,
                    sender_name: None,
                };
                if tx.send(msg).await.is_err() {
                    warn!("[yuanbao] dispatch receiver gone — dropping message");
                }
            }
            PipelineOutcome::Filtered(reason) => {
                tracing::trace!("[yuanbao] filtered at {reason}");
            }
            PipelineOutcome::Failed(err) => {
                // Intentionally omit the raw biz payload — it can carry
                // user content / PII. The decoder error already encodes
                // the structural reason; only the length is safe to log.
                warn!(
                    "[yuanbao] pipeline error: {err} | biz_len={}",
                    biz_body.len()
                );
            }
        }
    }

    async fn dispatch_push_typed(
        &self,
        frame: types::ConnFrame,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        use tinychannels_bus::channel::{ChannelSendError, ConversationKind, SendErrorKind};
        use tinychannels_bus::{
            ChannelInboundEnvelope, ChannelIngress, ChannelRef, ConversationRef, SenderRef,
        };

        if frame.data.is_empty() {
            return Ok(());
        }
        let unwrapped = match decode_push_msg(&frame.data) {
            Ok(push)
                if (!push.cmd.is_empty() || !push.module.is_empty()) && !push.data.is_empty() =>
            {
                Some(push.data)
            }
            _ => None,
        };
        let body = unwrapped.as_deref().unwrap_or(&frame.data);
        let PipelineOutcome::Dispatch(ctx) = self.pipeline.process_uncommitted(body).await else {
            return Ok(());
        };
        if self.pipeline.has_seen_message(&ctx.msg.msg_id).await {
            return Ok(());
        }

        let envelope = if ctx.kind == super::types::MessageKind::Voice {
            match self.voice_envelope(&ctx).await {
                Some(envelope) => envelope,
                None => {
                    warn!(message_id = %ctx.msg.msg_id, "[yuanbao] typed voice ingress could not be fetched or stored");
                    return Ok(());
                }
            }
        } else {
            let sender_id = ctx.msg.from_account.clone();
            let reply_target = ctx.source.reply_target();
            let short_sender = shorten_account_id(&sender_id);
            let short_reply_target = shorten_reply_target(&reply_target);
            self.register_recipient_alias(&short_sender, &sender_id)
                .await;
            self.register_recipient_alias(&short_reply_target, &reply_target)
                .await;
            ChannelInboundEnvelope {
                channel: ChannelRef {
                    id: "yuanbao".into(),
                    account_id: Some(self.config.bot_id.clone()),
                },
                message_id: ctx.msg.msg_id.clone(),
                timestamp_unix_ms: Some(u64::from(ctx.msg.msg_time) * 1000),
                conversation: ConversationRef {
                    kind: if ctx.source.is_group {
                        ConversationKind::Group
                    } else {
                        ConversationKind::Dm
                    },
                    id: short_reply_target,
                    ..Default::default()
                },
                sender: SenderRef {
                    id: sender_id,
                    name: Some(ctx.msg.sender_nickname.clone()),
                    ..Default::default()
                },
                text: ctx.text.clone(),
                raw: Some(serde_json::json!({"providerMessageId": ctx.msg.msg_id})),
                ..Default::default()
            }
        };

        if let Err(error) = sink.push_ingress(ChannelIngress::Inbound(envelope)).await {
            return Err(ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "Yuanbao typed ingress sink rejected a message".into(),
                retryable: true,
                ..error
            });
        }
        self.pipeline.mark_message_seen(&ctx.msg.msg_id).await;
        Ok(())
    }

    #[cfg(test)]
    async fn publish_voice(&self, ctx: &super::inbound::PipelineCtx) -> bool {
        use crate::providers::native::inbound::publish;
        let Some(events) = self.native.events.as_ref() else {
            return false;
        };
        let Some(envelope) = self.voice_envelope(ctx).await else {
            return false;
        };
        publish("yuanbao", Some(events), envelope).await
    }

    async fn voice_envelope(
        &self,
        ctx: &super::inbound::PipelineCtx,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        use crate::providers::native::inbound::{
            InboundFile, Ingest, Ingested, assemble, media_of,
        };
        use tinychannels_bus::{ChannelInboundEnvelope, ChannelRef, ConversationRef, SenderRef};

        let store = self.native.voice_media_store.as_ref()?;
        let sound = ctx
            .msg
            .msg_body
            .iter()
            .find(|element| element.msg_type == super::proto_constants::tim::SOUND)?;
        let url = sound
            .msg_content
            .url
            .as_deref()
            .filter(|url| !url.trim().is_empty())?;
        let Ok(url) = reqwest::Url::parse(url) else {
            return None;
        };
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some_and(|port| port != 443)
        {
            tracing::warn!("[yuanbao] rejected unsafe voice media URL");
            return None;
        }
        let max_bytes = u64::from(self.config.max_media_mb).saturating_mul(1024 * 1024);
        let Ok(mut response) = self.voice_http.get(url).send().await else {
            return None;
        };
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > max_bytes)
        {
            return None;
        }
        let mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !mime.starts_with("audio/") {
            return None;
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if bytes.len() as u64 + chunk.len() as u64 <= max_bytes => {
                    bytes.extend_from_slice(&chunk);
                }
                Ok(Some(_)) | Err(_) => return None,
                Ok(None) => break,
            }
        }
        if bytes.is_empty() {
            return None;
        }
        let conversation = ConversationRef {
            kind: if ctx.source.is_group {
                tinychannels_bus::channel::ConversationKind::Group
            } else {
                tinychannels_bus::channel::ConversationKind::Dm
            },
            id: ctx.source.reply_target(),
            ..Default::default()
        };
        let channel = ChannelRef {
            id: "yuanbao".into(),
            account_id: Some(self.config.bot_id.clone()),
        };
        let mut ingested = Ingested::default();
        Ingest {
            provider: "yuanbao",
            channel: channel.clone(),
            repository: self.native.media_repository.as_ref(),
            voice_store: Some(store),
            transcriber: self.native.transcriber.as_ref(),
            file_bytes: max_bytes,
            voice_bytes: max_bytes,
        }
        .add(
            &mut ingested,
            &conversation,
            &ctx.msg.msg_id,
            InboundFile {
                bytes,
                mime,
                name: sound.msg_content.file_name.clone(),
                duration_ms: None,
                voice: true,
                dedupe_key: format!("{}:voice", ctx.msg.msg_id),
                transcript: None,
            },
        )
        .await;
        let parts = assemble(&ctx.text, None, ingested);
        if !parts
            .iter()
            .any(|part| matches!(part, tinychannels_bus::ContentPart::VoiceNote { .. }))
        {
            return None;
        }
        Some(ChannelInboundEnvelope {
            channel,
            message_id: ctx.msg.msg_id.clone(),
            timestamp_unix_ms: Some(u64::from(ctx.msg.msg_time) * 1000),
            conversation,
            sender: SenderRef {
                id: ctx.msg.from_account.clone(),
                name: Some(ctx.msg.sender_nickname.clone()),
                ..Default::default()
            },
            text: ctx.text.clone(),
            media: media_of(&parts),
            parts,
            raw: Some(
                serde_json::json!({"providerMessageId": ctx.msg.msg_id, "messageType": "TIMSoundElem"}),
            ),
            ..Default::default()
        })
    }
}

struct YuanbaoListenerRegistration {
    listener: ListenerSlot,
    id: u64,
    cancel: watch::Sender<bool>,
}

impl Drop for YuanbaoListenerRegistration {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        let mut slot = self
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(active) = slot.as_mut() else {
            return;
        };
        if active.id != self.id {
            return;
        }
        if active.connection_started && !active.connection_done {
            active.abandoned = true;
            return;
        }
        slot.take();
    }
}

impl Drop for YuanbaoConnectionRegistration {
    fn drop(&mut self) {
        let mut slot = self
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(active) = slot.as_mut().filter(|active| active.id == self.id) {
            active.connection_done = true;
            if active.abandoned {
                slot.take();
            }
        }
    }
}

struct YuanbaoConnectionRegistration {
    listener: ListenerSlot,
    id: u64,
}

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .expect("building the Yuanbao voice HTTP client should succeed")
}

impl Drop for YuanbaoChannel {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(active) = self
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            active.cancel.send_replace(true);
        }
    }
}

#[path = "native.rs"]
mod native;
crate::providers::native::native_v2_adapter!(YuanbaoChannel);

#[cfg(test)]
#[path = "channel_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod conformance_tests;
