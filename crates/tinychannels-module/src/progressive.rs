//! Bounded module-owned progressive reply workers and retained terminal cleanup.
use crate::host::HostChannels;
use crate::provider::Running;
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tinybus::{Error, Result};
use tinychannels::delivery::progressive::{
    EDIT_FLUSH_INTERVAL, FILLER_INTERVAL, ProgressiveOutputContext, ProgressiveReply,
    ProgressiveSendError, ProgressiveSender, TYPING_REFRESH_INTERVAL,
};
use tinychannels::factory::BuiltChannel;
use tinychannels_bus::module_progressive::{
    ProgressiveCommand, ProgressiveEvent, ProgressiveResult, ProgressiveTransportRequest,
    ProgressiveTransportResult,
};
use tinychannels_bus::{ChannelBackendRequest, ChannelBackendResult};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

const MAX_ACTIVE: usize = 16;
const MAX_RETAINED: usize = 128;
const MAX_CONTENT: usize = 384 * 1024;
// Both text and thinking can fill their content budget in one feed. JSON
// escaping expands a byte to at most six bytes; retain bounded envelope space.
const MAX_REQUEST: usize = MAX_CONTENT * 12 + 64 * 1024;
const TURN_LIFETIME: Duration = Duration::from_secs(180);

struct Sender(HostChannels);

impl Sender {
    async fn invoke(
        &self,
        request: ProgressiveTransportRequest,
    ) -> std::result::Result<Value, ProgressiveSendError> {
        let result = self
            .0
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(Box::new(
                request,
            )))
            .await
            .map_err(|_| ProgressiveSendError::Unavailable)?;
        match result {
            ChannelBackendResult::ProgressiveTransport(result) => match *result {
                ProgressiveTransportResult::Success(value) => Ok(value),
                ProgressiveTransportResult::Unavailable => Err(ProgressiveSendError::Unavailable),
                ProgressiveTransportResult::EditUnsupported => {
                    Err(ProgressiveSendError::EditUnsupported)
                }
                ProgressiveTransportResult::MessageGone => Err(ProgressiveSendError::MessageGone),
                ProgressiveTransportResult::Outcome(outcome) => {
                    serde_json::to_value(outcome.as_ref())
                        .map_err(|error| ProgressiveSendError::Other(anyhow::anyhow!(error)))
                }
                ProgressiveTransportResult::Acknowledged | ProgressiveTransportResult::Accepted => {
                    Ok(Value::Null)
                }
                ProgressiveTransportResult::UnsupportedContent
                | ProgressiveTransportResult::Failed => Err(ProgressiveSendError::Other(
                    anyhow::anyhow!("progressive transport failed"),
                )),
            },
            _ => Err(ProgressiveSendError::Other(anyhow::anyhow!(
                "progressive transport failed"
            ))),
        }
    }
}

#[async_trait]
impl ProgressiveSender for Sender {
    async fn send(
        &self,
        channel: &str,
        text: &str,
    ) -> std::result::Result<Value, ProgressiveSendError> {
        let intent = tinychannels::outbound_intent_from_legacy_message(
            channel,
            serde_json::json!({"text": text}),
        );
        let message = tinychannels::legacy_message_value_from_outbound_intent(&intent);
        self.invoke(ProgressiveTransportRequest::Send {
            channel: channel.into(),
            turn_id: None,
            channel_ref: None,
            conversation: None,
            sequence: None,
            idempotency_key: None,
            message,
        })
        .await
    }
    async fn edit(
        &self,
        channel: &str,
        message_id: &str,
        text: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Edit {
            channel: channel.into(),
            turn_id: None,
            channel_ref: None,
            conversation: None,
            message_id: message_id.into(),
            receipt: None,
            text: text.into(),
        })
        .await
        .map(drop)
    }
    async fn delete(
        &self,
        channel: &str,
        message_id: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Delete {
            channel: channel.into(),
            turn_id: None,
            channel_ref: None,
            conversation: None,
            message_id: message_id.into(),
            receipt: None,
        })
        .await
        .map(drop)
    }
    async fn typing(&self, channel: &str) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Typing {
            channel: channel.into(),
            turn_id: None,
            channel_ref: None,
            conversation: None,
        })
        .await
        .map(drop)
    }
}

enum Command {
    Feed(Vec<ProgressiveEvent>, oneshot::Sender<Result<()>>),
    FeedOutput(
        Vec<tinychannels_bus::module_progressive::ChannelOutputEvent>,
        oneshot::Sender<Result<ProgressiveResult>>,
    ),
    Finish(String, oneshot::Sender<()>),
}

struct Closing {
    fingerprint: [u8; 32],
    task: Option<JoinHandle<std::result::Result<(), String>>>,
    outcome: Option<std::result::Result<(), String>>,
}
struct Entry {
    commands: mpsc::Sender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    closing: Mutex<Option<Closing>>,
}

enum OutputTarget {
    Legacy,
    Hosted(OutputScope),
    Adapter(OutputScope, BuiltChannel),
}

#[derive(Clone)]
struct OutputScope {
    channel: tinychannels_bus::ChannelRef,
    conversation: tinychannels_bus::ConversationRef,
    turn_id: String,
    lifetime: Duration,
}
struct State {
    terminal: bool,
    entries: HashMap<String, Arc<Entry>>,
}

/// One private runtime's reply handles; all callbacks retain that runtime's host.
pub(crate) struct Replies {
    host: HostChannels,
    running: Arc<Mutex<HashMap<String, Arc<Running>>>>,
    state: Mutex<State>,
}

impl Replies {
    pub(crate) fn new(
        host: HostChannels,
        running: Arc<Mutex<HashMap<String, Arc<Running>>>>,
    ) -> Self {
        Self {
            host,
            running,
            state: Mutex::new(State {
                terminal: false,
                entries: HashMap::new(),
            }),
        }
    }

    pub(crate) async fn execute(&self, request: ProgressiveCommand) -> Result<ProgressiveResult> {
        if serde_json::to_vec(&request).map_or(true, |bytes| bytes.len() > MAX_REQUEST) {
            return Err(Error::failed("progressive request exceeds its limit"));
        }
        match request {
            ProgressiveCommand::Open { channel } => self.open(channel).await,
            ProgressiveCommand::OpenOutput {
                channel,
                conversation,
                turn_id,
                lifetime_ms,
            } => {
                let scope = output_scope(channel, conversation, turn_id, lifetime_ms)?;
                let provider_id = tinychannels_bus::provider_id(&scope.channel.id);
                let built = {
                    let running = self.running.lock().await;
                    scope
                        .channel
                        .account_id
                        .as_deref()
                        .and_then(|id| running.get(id))
                        .or_else(|| running.get(provider_id))
                        .map(|provider| provider.channel.clone())
                }
                .ok_or_else(|| Error::failed("configured channel adapter is unavailable"))?;
                self.open_target(
                    scope.channel.id.clone(),
                    OutputTarget::Adapter(scope, built),
                )
                .await
            }
            ProgressiveCommand::OpenHostedOutput {
                channel,
                conversation,
                turn_id,
                lifetime_ms,
            } => {
                let scope = output_scope(channel, conversation, turn_id, lifetime_ms)?;
                self.open_target(scope.channel.id.clone(), OutputTarget::Hosted(scope))
                    .await
            }
            ProgressiveCommand::Feed { handle, events } => {
                if events.len() > 32 {
                    return Err(Error::failed("too many progressive events"));
                }
                let entry = self.entry(&handle).await?;
                let closing = entry.closing.lock().await;
                if closing.is_some() {
                    return Err(Error::failed("progressive reply is closed"));
                }
                let (reply, wait) = oneshot::channel();
                entry
                    .commands
                    .send(Command::Feed(events, reply))
                    .await
                    .map_err(|_| Error::failed("progressive reply is closed"))?;
                drop(closing);
                wait.await
                    .map_err(|_| Error::failed("progressive worker stopped"))??;
                Ok(ProgressiveResult::Updated)
            }
            ProgressiveCommand::FeedOutput { handle, events } => {
                if events.len() > 32 {
                    return Err(Error::failed("too many progressive output events"));
                }
                let entry = self.entry(&handle).await?;
                let closing = entry.closing.lock().await;
                if closing.is_some() {
                    return Err(Error::failed("progressive reply is closed"));
                }
                let (reply, wait) = oneshot::channel();
                entry
                    .commands
                    .send(Command::FeedOutput(events, reply))
                    .await
                    .map_err(|_| Error::failed("progressive reply is closed"))?;
                drop(closing);
                wait.await
                    .map_err(|_| Error::failed("progressive worker stopped"))?
            }
            ProgressiveCommand::Finish { handle, text } => {
                if text.len() > MAX_CONTENT {
                    return Err(Error::failed("progressive content exceeds its limit"));
                }
                let entry = self.entry(&handle).await?;
                close_entry(entry, text, false).await?;
                Ok(ProgressiveResult::Closed)
            }
            ProgressiveCommand::Close { handle } => {
                let entry = self.state.lock().await.entries.get(&handle).cloned();
                if let Some(entry) = entry {
                    close_entry(entry, "Cancelled.".into(), true).await?;
                }
                Ok(ProgressiveResult::Closed)
            }
        }
    }

    async fn entry(&self, handle: &str) -> Result<Arc<Entry>> {
        let state = self.state.lock().await;
        if state.terminal {
            return Err(Error::failed("progressive replies are shutting down"));
        }
        state
            .entries
            .get(handle)
            .cloned()
            .ok_or_else(|| Error::failed("unknown progressive handle"))
    }

    async fn open(&self, channel: String) -> Result<ProgressiveResult> {
        self.open_target(channel, OutputTarget::Legacy).await
    }

    async fn open_target(
        &self,
        channel: String,
        target: OutputTarget,
    ) -> Result<ProgressiveResult> {
        if channel.is_empty() || channel.len() > 4096 {
            return Err(Error::failed("invalid progressive target"));
        }
        let provider_id = tinychannels_bus::provider_id(&channel);
        let capabilities = match &target {
            OutputTarget::Adapter(_, built) => built.capabilities(),
            _ => self
                .running
                .lock()
                .await
                .get(provider_id)
                .map(|provider| provider.channel.capabilities())
                .unwrap_or_else(tinychannels_bus::ChannelCapabilities::unknown),
        };
        let mut state = self.state.lock().await;
        if state.terminal {
            return Err(Error::failed("progressive replies are shutting down"));
        }
        let mut completed = Vec::new();
        let mut active = 0;
        for (handle, entry) in &state.entries {
            let finished = match entry.worker.try_lock() {
                Ok(worker) => match worker.as_ref() {
                    Some(worker) => worker.is_finished(),
                    None => match entry.closing.try_lock() {
                        Ok(closing) => closing.as_ref().is_some_and(|closing| {
                            closing.outcome.is_some()
                                || closing.task.as_ref().is_some_and(|task| task.is_finished())
                        }),
                        Err(_) => false,
                    },
                },
                Err(_) => false,
            };
            if finished {
                completed.push(handle.clone());
            } else {
                active += 1;
            }
        }
        if active >= MAX_ACTIVE {
            return Err(Error::failed("progressive reply capacity reached"));
        }
        if state.entries.len() >= MAX_RETAINED {
            if let Some(handle) = completed.into_iter().next() {
                state.entries.remove(&handle);
            } else {
                return Err(Error::failed("progressive reply capacity reached"));
            }
        }
        let handle = uuid::Uuid::new_v4().simple().to_string();
        let (commands, inbox) = mpsc::channel(16);
        let host = self.host.clone();
        let lifetime = match &target {
            OutputTarget::Legacy => TURN_LIFETIME,
            OutputTarget::Hosted(scope) | OutputTarget::Adapter(scope, _) => scope.lifetime,
        };
        let worker_target = match &target {
            OutputTarget::Legacy => WorkerTarget::Legacy,
            OutputTarget::Hosted(scope) => WorkerTarget::Hosted(scope.clone()),
            OutputTarget::Adapter(scope, built) => {
                WorkerTarget::Adapter(scope.clone(), built.clone())
            }
        };
        let worker = tokio::spawn(run(
            host,
            channel,
            capabilities,
            worker_target,
            lifetime,
            inbox,
        ));
        state.entries.insert(
            handle.clone(),
            Arc::new(Entry {
                commands,
                worker: Mutex::new(Some(worker)),
                closing: Mutex::new(None),
            }),
        );
        Ok(ProgressiveResult::Opened { handle })
    }

    /// Freeze admission and await terminal delivery before callback shutdown.
    pub(crate) async fn shutdown(&self) -> Result<()> {
        let entries = {
            let mut state = self.state.lock().await;
            state.terminal = true;
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        // Start every terminal task before awaiting any one provider. A blocked
        // delivery must not prevent unrelated reply workers from draining.
        let mut closes = tokio::task::JoinSet::new();
        for entry in entries {
            closes.spawn(close_entry(entry, "Cancelled.".into(), true));
        }
        let mut failure = None;
        while let Some(result) = closes.join_next().await {
            let result =
                result.unwrap_or_else(|_| Err(Error::failed("progressive shutdown worker failed")));
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            // Retained per-entry terminal tasks can finish after the timeout;
            // the next Shutdown awaits them without sending a second reply.
            return Err(error);
        }
        self.state.lock().await.entries.clear();
        Ok(())
    }
}

async fn close_entry(entry: Arc<Entry>, text: String, retain_existing: bool) -> Result<()> {
    let fingerprint: [u8; 32] = Sha256::digest(text.as_bytes()).into();
    let mut closing = entry.closing.lock().await;
    if let Some(existing) = closing.as_ref() {
        if !retain_existing && existing.fingerprint != fingerprint {
            return Err(Error::failed("progressive terminal content differs"));
        }
    } else {
        let retained = entry.clone();
        let task = tokio::spawn(async move {
            let (reply, wait) = oneshot::channel();
            if retained
                .commands
                .send(Command::Finish(text, reply))
                .await
                .is_ok()
            {
                let _ = wait.await;
            }
            if let Some(worker) = retained.worker.lock().await.take() {
                worker
                    .await
                    .map_err(|_| "progressive worker failed".to_string())?;
            }
            Ok(())
        });
        *closing = Some(Closing {
            fingerprint,
            task: Some(task),
            outcome: None,
        });
    }
    let closing = closing.as_mut().expect("terminal work was installed");
    if closing.outcome.is_none() {
        let task = closing
            .task
            .as_mut()
            .expect("unfinished terminal task is retained");
        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .map_err(|_| Error::failed("progressive cleanup is pending"))?
            .map_err(|_| Error::failed("progressive cleanup worker failed"))?;
        closing.outcome = Some(result);
        closing.task = None;
    }
    closing
        .outcome
        .clone()
        .expect("terminal result was retained")
        .map_err(|_| Error::failed("progressive cleanup failed"))
}

enum WorkerTarget {
    Legacy,
    Hosted(OutputScope),
    Adapter(OutputScope, BuiltChannel),
}

async fn run(
    host: HostChannels,
    channel: String,
    capabilities: tinychannels_bus::ChannelCapabilities,
    target: WorkerTarget,
    lifetime: Duration,
    mut inbox: mpsc::Receiver<Command>,
) {
    let (sender, context, hosted) = match &target {
        WorkerTarget::Legacy => (
            Arc::new(Sender(host.clone())) as Arc<dyn ProgressiveSender>,
            None,
            false,
        ),
        WorkerTarget::Hosted(scope) => (
            Arc::new(Sender(host.clone())) as Arc<dyn ProgressiveSender>,
            Some(output_context(scope)),
            true,
        ),
        WorkerTarget::Adapter(scope, built) => (
            Arc::new(AdapterSender {
                built: built.clone(),
                scope: scope.clone(),
            }) as Arc<dyn ProgressiveSender>,
            Some(output_context(scope)),
            false,
        ),
    };
    let mut reply = match context {
        Some(context) if hosted => {
            ProgressiveReply::new_hosted_scoped(sender, channel.clone(), capabilities, context)
        }
        Some(context) => {
            ProgressiveReply::new_scoped(sender, channel.clone(), capabilities, context)
        }
        None => ProgressiveReply::new(sender, channel.clone(), capabilities),
    };
    let next_sequence = AtomicU64::new(0);
    let mut edits = tokio::time::interval(EDIT_FLUSH_INTERVAL);
    let mut typing = tokio::time::interval(TYPING_REFRESH_INTERVAL);
    let mut fillers = tokio::time::interval(FILLER_INTERVAL);
    for timer in [&mut edits, &mut typing, &mut fillers] {
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer.tick().await;
    }
    reply.typing_tick().await;
    let expiry = tokio::time::sleep(lifetime);
    tokio::pin!(expiry);
    loop {
        tokio::select! {
            command = inbox.recv() => match command {
                Some(Command::Feed(events, response)) => {
                    let result = apply(&mut reply, events);
                    let _ = response.send(result);
                }
                Some(Command::FeedOutput(events, response)) => {
                    let result = match &target {
                        WorkerTarget::Hosted(scope) => feed_output(&host, &channel, Some(scope), &next_sequence, events).await,
                        WorkerTarget::Adapter(_, _) => feed_adapter_output(&mut reply, events).await,
                        WorkerTarget::Legacy => Err(Error::failed("canonical output requires an opened output scope")),
                    };
                    let _ = response.send(result);
                }
                Some(Command::Finish(text, response)) => {
                    reply.finalize(&text).await;
                    let _ = response.send(());
                    break;
                }
                None => { reply.finalize("Cancelled.").await; break; }
            },
            _ = edits.tick() => reply.edit_tick().await,
            _ = typing.tick() => reply.typing_tick().await,
            _ = fillers.tick() => reply.filler_tick().await,
            _ = &mut expiry => { reply.finalize("Sorry, the request timed out.").await; break; }
        }
    }
}

fn output_context(scope: &OutputScope) -> ProgressiveOutputContext {
    ProgressiveOutputContext {
        turn_id: scope.turn_id.clone(),
        channel: scope.channel.clone(),
        conversation: scope.conversation.clone(),
    }
}

async fn feed_adapter_output(
    reply: &mut ProgressiveReply,
    events: Vec<tinychannels_bus::module_progressive::ChannelOutputEvent>,
) -> Result<ProgressiveResult> {
    let mut dispatch = tinychannels::delivery::progressive::ProgressiveOutputDispatch::default();
    for event in events {
        let current = if matches!(
            event,
            tinychannels_bus::module_progressive::ChannelOutputEvent::FinalMessage { .. }
        ) {
            reply.finalize_output_event(&event).await
        } else {
            reply.apply_output_event(&event, true).await
        }
        .map_err(|error| Error::failed(error.to_string()))?;
        dispatch.outcomes.extend(current.outcomes);
        dispatch.unsupported.extend(current.unsupported);
        dispatch.failure = dispatch.failure.or(current.failure);
    }
    Ok(dispatch.into_result())
}

struct AdapterSender {
    built: BuiltChannel,
    scope: OutputScope,
}

impl AdapterSender {
    fn intent(&self, text: &str, key: String) -> tinychannels_bus::ChannelOutboundIntent {
        tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: key,
            channel_id: self.scope.channel.id.clone(),
            conversation_id: self.scope.conversation.id.clone(),
            channel_ref: Some(self.scope.channel.clone()),
            conversation_ref: Some(self.scope.conversation.clone()),
            turn_id: Some(self.scope.turn_id.clone()),
            payload: tinychannels_bus::OutboundPayload::Text {
                text: text.to_owned(),
            },
            ..Default::default()
        }
    }

    async fn send_parts_inner(
        &self,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        sequence: u64,
        segment: u32,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError>
    {
        let mut intent = self.intent(
            text,
            format!("{}:{}:{}", self.scope.turn_id, sequence, segment),
        );
        intent.payload = tinychannels_bus::OutboundPayload::NativeChannelData {
            data: serde_json::json!({"parts": parts}),
        };
        let mut message = tinychannels_bus::ChannelOutboundMessage::from_intent(&intent);
        message.parts = parts.to_vec();
        self.built
            .adapter()
            .send_typed(&message)
            .await
            .map_err(map_send_error)
    }
}

fn map_send_error(error: tinychannels::channel::ChannelSendError) -> ProgressiveSendError {
    if error.is_unsupported() {
        ProgressiveSendError::UnsupportedContent
    } else if error.retryable {
        ProgressiveSendError::Unavailable
    } else {
        ProgressiveSendError::Other(anyhow::anyhow!(error.message))
    }
}

#[async_trait]
impl ProgressiveSender for AdapterSender {
    async fn send(
        &self,
        _channel: &str,
        text: &str,
    ) -> std::result::Result<Value, ProgressiveSendError> {
        let outcome = self
            .send_parts_inner(
                &[tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: text.to_owned(),
                        markdown_dialect: None,
                    },
                }],
                text,
                0,
                0,
            )
            .await?;
        if outcome.error.is_some() {
            return Err(ProgressiveSendError::PartialOutcome(Box::new(outcome)));
        }
        serde_json::to_value(outcome)
            .map_err(|error| ProgressiveSendError::Other(anyhow::anyhow!(error)))
    }

    async fn send_parts(
        &self,
        _channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        _context: Option<&ProgressiveOutputContext>,
        sequence: u64,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError>
    {
        self.send_parts_inner(parts, text, sequence, 0).await
    }

    async fn send_parts_with_segment(
        &self,
        _channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        _context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        segment_index: u32,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError>
    {
        self.send_parts_inner(parts, text, sequence, segment_index)
            .await
    }

    async fn edit(
        &self,
        _channel: &str,
        message_id: &str,
        text: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        let receipt = tinychannels_bus::channel::MessageReceipt {
            primary_platform_message_id: Some(message_id.into()),
            ..Default::default()
        };
        self.edit_with_receipt("", &receipt, text).await
    }

    async fn edit_with_receipt(
        &self,
        _channel: &str,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        text: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        let edit = self
            .built
            .edit()
            .ok_or(ProgressiveSendError::EditUnsupported)?;
        let intent = self.intent(text, format!("{}:edit", self.scope.turn_id));
        edit.edit(receipt, intent)
            .await
            .map(|_| ())
            .map_err(map_send_error)
    }

    async fn delete(
        &self,
        _channel: &str,
        _message_id: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        Err(ProgressiveSendError::EditUnsupported)
    }

    async fn typing(&self, _channel: &str) -> std::result::Result<(), ProgressiveSendError> {
        Ok(())
    }
}

fn output_scope(
    channel: tinychannels_bus::ChannelRef,
    conversation: tinychannels_bus::ConversationRef,
    turn_id: String,
    lifetime_ms: u64,
) -> Result<OutputScope> {
    if channel.id.trim().is_empty()
        || conversation.id.trim().is_empty()
        || turn_id.trim().is_empty()
        || lifetime_ms == 0
        || lifetime_ms > TURN_LIFETIME.as_millis() as u64
    {
        return Err(Error::failed("invalid progressive output scope"));
    }
    Ok(OutputScope {
        channel,
        conversation,
        turn_id,
        lifetime: Duration::from_millis(lifetime_ms),
    })
}

async fn feed_output(
    host: &HostChannels,
    channel: &str,
    scope: Option<&OutputScope>,
    next_sequence: &AtomicU64,
    events: Vec<tinychannels_bus::module_progressive::ChannelOutputEvent>,
) -> Result<ProgressiveResult> {
    let scope =
        scope.ok_or_else(|| Error::failed("canonical output requires an opened output scope"))?;
    let mut outcomes = Vec::new();
    let mut unsupported = Vec::new();
    let mut failure = None;
    let mut hosted_accepted = false;
    for event in events {
        let sequence = next_sequence.fetch_add(1, Ordering::SeqCst);
        let request = ProgressiveTransportRequest::OutputEvent {
            channel: channel.to_owned(),
            turn_id: Some(scope.turn_id.clone()),
            channel_ref: Some(scope.channel.clone()),
            conversation: Some(scope.conversation.clone()),
            sequence,
            event: Box::new(event.clone()),
        };
        let callback = host
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(Box::new(
                request,
            )))
            .await;
        let result = match callback {
            Ok(ChannelBackendResult::ProgressiveTransport(result)) => *result,
            _ => {
                failure = Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ObservationCallback);
                break;
            }
        };
        match result {
            ProgressiveTransportResult::Acknowledged => {}
            ProgressiveTransportResult::Accepted => hosted_accepted = true,
            ProgressiveTransportResult::Outcome(outcome) => outcomes.push(*outcome),
            ProgressiveTransportResult::Success(value) => {
                if let Ok(outcome) =
                    serde_json::from_value::<tinychannels_bus::channel::ChannelSendOutcome>(value)
                {
                    outcomes.push(outcome);
                }
            }
            ProgressiveTransportResult::UnsupportedContent => {
                if let tinychannels_bus::module_progressive::ChannelOutputEvent::FinalMessage {
                    parts,
                    ..
                } = event
                {
                    unsupported.extend(parts);
                } else {
                    failure = Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport);
                    break;
                }
            }
            ProgressiveTransportResult::Unavailable
            | ProgressiveTransportResult::EditUnsupported
            | ProgressiveTransportResult::MessageGone
            | ProgressiveTransportResult::Failed => {
                failure = Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport);
                break;
            }
        }
    }
    Ok(
        match (
            outcomes.len(),
            unsupported.is_empty(),
            failure,
            hosted_accepted,
        ) {
            (0, true, None, false) => ProgressiveResult::Updated,
            (1, true, None, false) => ProgressiveResult::DeliveryOutcome {
                outcome: Box::new(outcomes.into_iter().next().expect("one outcome")),
            },
            (0, false, None, false) => {
                ProgressiveResult::DeliveryUnsupported { parts: unsupported }
            }
            (count, true, None, false) if count > 1 => {
                ProgressiveResult::DeliveryOutcomes { outcomes }
            }
            (_, _, failure, hosted_accepted) => ProgressiveResult::DeliveryBatch {
                outcomes,
                unsupported,
                failure,
                hosted_accepted,
            },
        },
    )
}

fn apply(reply: &mut ProgressiveReply, events: Vec<ProgressiveEvent>) -> Result<()> {
    let text = events
        .iter()
        .filter_map(|event| match event {
            ProgressiveEvent::TextDelta(value) => Some(value.len()),
            _ => None,
        })
        .sum::<usize>();
    let thinking = events
        .iter()
        .filter_map(|event| match event {
            ProgressiveEvent::ThinkingDelta(value) => Some(value.len()),
            _ => None,
        })
        .sum::<usize>();
    if reply.state().content.len().saturating_add(text) > MAX_CONTENT
        || reply
            .state()
            .thinking_accumulator
            .len()
            .saturating_add(thinking)
            > MAX_CONTENT
    {
        return Err(Error::failed("progressive content exceeds its limit"));
    }
    for event in events {
        match event {
            ProgressiveEvent::TextDelta(value) => reply.on_text_delta(&value),
            ProgressiveEvent::ThinkingDelta(value) => reply.on_thinking_delta(&value),
            ProgressiveEvent::ToolCall(name) => reply.on_tool_call(&name),
            ProgressiveEvent::ToolResult { tool_name, success } => {
                reply.on_tool_result(&tool_name, success)
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "progressive_tests.rs"]
mod tests;
