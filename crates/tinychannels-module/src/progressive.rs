//! Bounded module-owned progressive reply workers and retained terminal cleanup.
use crate::host::HostChannels;
use crate::provider::Running;
use crate::voice_reply::SpokenReply;
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
    ChannelOutputEvent, ProgressiveCommand, ProgressiveEvent, ProgressiveResult,
    ProgressiveTransportRequest, ProgressiveTransportResult,
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
/// Longest lifetime a host-scoped output may be opened with: a hosted turn
/// lease (an hour) plus its grace, which native turns request.
const MAX_SCOPED_OUTPUT_LIFETIME: Duration = Duration::from_secs(3_700);

#[derive(Clone)]
struct Sender {
    host: HostChannels,
    scope: Arc<Mutex<Option<OutputScope>>>,
    output_scope: Arc<Mutex<Option<OutputScope>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OutputScope {
    handle: String,
    channel: String,
    turn_id: Option<String>,
    channel_ref: Option<tinychannels_bus::ChannelRef>,
    conversation: Option<tinychannels_bus::ConversationRef>,
}

struct FeedOutput {
    handle: String,
    turn_id: Option<String>,
    channel_ref: Option<tinychannels_bus::ChannelRef>,
    conversation: Option<tinychannels_bus::ConversationRef>,
    sequence: u64,
    events: Vec<ChannelOutputEvent>,
}

#[derive(Clone)]
struct AppliedOutput {
    deliveries: Vec<ProgressiveTransportResult>,
    output_event_failures: Vec<u64>,
    final_message_delivered: bool,
}

#[derive(Clone)]
struct OutputBatchReplay {
    scope: OutputScope,
    sequence: u64,
    events: Vec<ChannelOutputEvent>,
    result: AppliedOutput,
}

impl Sender {
    fn new(host: HostChannels) -> Self {
        Self {
            host,
            scope: Arc::new(Mutex::new(None)),
            output_scope: Arc::new(Mutex::new(None)),
        }
    }

    async fn set_scope(&self, scope: Option<OutputScope>) {
        *self.scope.lock().await = scope;
    }

    async fn has_scope(&self) -> bool {
        self.scope.lock().await.is_some()
    }

    async fn check_output_scope(&self, scope: &OutputScope) -> Result<()> {
        if self
            .output_scope
            .lock()
            .await
            .as_ref()
            .is_some_and(|bound| bound != scope)
        {
            return Err(Error::failed("progressive reply scope cannot change"));
        }
        Ok(())
    }

    async fn bind_output_scope(&self, scope: OutputScope) {
        let mut bound = self.output_scope.lock().await;
        if bound.is_none() {
            *bound = Some(scope);
        }
    }

    async fn apply_scope(
        &self,
        request: ProgressiveTransportRequest,
    ) -> ProgressiveTransportRequest {
        let Some(scope) = self.scope.lock().await.clone() else {
            return request;
        };
        match request {
            ProgressiveTransportRequest::Send {
                channel,
                sequence,
                idempotency_key,
                message,
                ..
            } => ProgressiveTransportRequest::Send {
                channel,
                turn_id: scope.turn_id,
                channel_ref: scope.channel_ref,
                conversation: scope.conversation,
                sequence,
                idempotency_key,
                message,
            },
            ProgressiveTransportRequest::Edit {
                channel,
                message_id,
                receipt,
                text,
                ..
            } => ProgressiveTransportRequest::Edit {
                channel,
                turn_id: scope.turn_id,
                channel_ref: scope.channel_ref,
                conversation: scope.conversation,
                message_id,
                receipt,
                text,
            },
            ProgressiveTransportRequest::Delete {
                channel,
                message_id,
                receipt,
                ..
            } => ProgressiveTransportRequest::Delete {
                channel,
                turn_id: scope.turn_id,
                channel_ref: scope.channel_ref,
                conversation: scope.conversation,
                message_id,
                receipt,
            },
            ProgressiveTransportRequest::Typing { channel, .. } => {
                ProgressiveTransportRequest::Typing {
                    channel,
                    turn_id: scope.turn_id,
                    channel_ref: scope.channel_ref,
                    conversation: scope.conversation,
                }
            }
            request => request,
        }
    }
}

impl Sender {
    async fn output_event(
        &self,
        scope: &OutputScope,
        sequence: u64,
        event: ChannelOutputEvent,
    ) -> Result<()> {
        let result = self
            .host
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(Box::new(
                ProgressiveTransportRequest::OutputEvent {
                    channel: scope.channel.clone(),
                    turn_id: scope.turn_id.clone(),
                    channel_ref: scope.channel_ref.clone(),
                    conversation: scope.conversation.clone(),
                    sequence,
                    event: Box::new(event),
                },
            )))
            .await
            .map_err(|_| Error::failed("progressive output callback failed"))?;
        match result {
            ChannelBackendResult::ProgressiveTransport(result)
                if matches!(
                    *result,
                    ProgressiveTransportResult::Acknowledged | ProgressiveTransportResult::Accepted
                ) =>
            {
                Ok(())
            }
            _ => Err(Error::failed("progressive output callback failed")),
        }
    }

    async fn send_typed(
        &self,
        scope: &OutputScope,
        sequence: u64,
        parts: Vec<tinychannels_bus::OutboundContentPart>,
        text: String,
    ) -> Result<ProgressiveTransportResult> {
        let result = self
            .host
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(Box::new(
                ProgressiveTransportRequest::SendTyped {
                    channel: scope.channel.clone(),
                    turn_id: scope.turn_id.clone(),
                    channel_ref: scope.channel_ref.clone(),
                    conversation: scope.conversation.clone(),
                    sequence: Some(sequence),
                    segment_index: None,
                    idempotency_key: Some(format!("progressive:{}:{sequence}", scope.handle)),
                    parts,
                    text,
                },
            )))
            .await
            .map_err(|_| Error::failed("progressive typed delivery failed"))?;
        match result {
            ChannelBackendResult::ProgressiveTransport(result) => Ok(*result),
            _ => Err(Error::failed("progressive typed delivery failed")),
        }
    }

    async fn invoke(
        &self,
        request: ProgressiveTransportRequest,
    ) -> std::result::Result<Value, ProgressiveSendError> {
        let result = self
            .host
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(Box::new(
                self.apply_scope(request).await,
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
    FeedSequencedOutput(Box<FeedOutput>, oneshot::Sender<Result<AppliedOutput>>),
    Finish(String, oneshot::Sender<()>),
}

struct Closing {
    fingerprint: [u8; 32],
    task: Option<JoinHandle<std::result::Result<(), String>>>,
    outcome: Option<std::result::Result<(), String>>,
}
struct Entry {
    channel: String,
    bound_scope: Option<OutputScope>,
    /// Scope fixed at open for module-sequenced handles.
    module_scope: Option<OpenScope>,
    commands: mpsc::Sender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    closing: Mutex<Option<Closing>>,
    output_replay: Arc<Mutex<Option<OutputBatchReplay>>>,
}

enum OutputTarget {
    Legacy,
    Hosted(OpenScope),
    Adapter(OpenScope, BuiltChannel, Option<Arc<SpokenReply>>),
}

/// Full turn scope fixed by `OpenOutput`/`OpenHostedOutput`.
#[derive(Clone)]
struct OpenScope {
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
                        .map(|provider| (provider.channel.clone(), provider.spoken.clone()))
                }
                .ok_or_else(|| Error::failed("configured channel adapter is unavailable"))?;
                let (built, spoken) = built;
                self.open_target(
                    scope.channel.id.clone(),
                    OutputTarget::Adapter(scope, built, spoken),
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
            ProgressiveCommand::OpenScoped {
                channel,
                turn_id,
                channel_ref,
                conversation,
            } => {
                self.open_bound(
                    channel,
                    OutputTarget::Legacy,
                    Some((
                        turn_id,
                        channel_ref.map(|value| *value),
                        conversation.map(|value| *value),
                    )),
                )
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
            ProgressiveCommand::FeedOutput {
                handle,
                turn_id,
                channel_ref,
                conversation,
                sequence,
                events,
            } => {
                if events.len() > 32 {
                    return Err(Error::failed("too many progressive events"));
                }
                let entry = self.entry(&handle).await?;
                let closing = entry.closing.lock().await;
                if closing.is_some() {
                    return Err(Error::failed("progressive reply is closed"));
                }
                if let Some(opened) = entry.module_scope.as_ref() {
                    // Module-sequenced handle (`OpenOutput`/`OpenHostedOutput`):
                    // the scope was fixed at open, so any asserted field must
                    // match it and the module assigns event sequences itself.
                    if sequence.is_some() {
                        return Err(Error::failed(
                            "module-sequenced progressive output does not accept a host sequence",
                        ));
                    }
                    if turn_id.as_ref().is_some_and(|turn| turn != &opened.turn_id)
                        || channel_ref
                            .as_deref()
                            .is_some_and(|channel| channel != &opened.channel)
                        || conversation
                            .as_deref()
                            .is_some_and(|conversation| conversation != &opened.conversation)
                    {
                        return Err(Error::failed("progressive output scope changed"));
                    }
                    let (reply, wait) = oneshot::channel();
                    entry
                        .commands
                        .send(Command::FeedOutput(events, reply))
                        .await
                        .map_err(|_| Error::failed("progressive reply is closed"))?;
                    drop(closing);
                    return wait
                        .await
                        .map_err(|_| Error::failed("progressive worker stopped"))?;
                }
                let sequence = sequence
                    .ok_or_else(|| Error::failed("progressive output sequence is required"))?;
                let scope = OutputScope {
                    handle: handle.clone(),
                    channel: entry.channel.clone(),
                    turn_id,
                    channel_ref: channel_ref.map(|value| *value),
                    conversation: conversation.map(|value| *value),
                };
                if entry
                    .bound_scope
                    .as_ref()
                    .is_some_and(|bound| bound != &scope)
                {
                    return Err(Error::failed("progressive output scope changed"));
                }
                let previous = entry.output_replay.lock().await.clone();
                if let Some(applied) = replay_output(previous.as_ref(), &scope, sequence, &events) {
                    let mut applied = applied?;
                    retry_failed_output_callbacks(
                        &Sender::new(self.host.clone()),
                        &scope,
                        sequence,
                        &events,
                        &mut applied,
                    )
                    .await;
                    if let Some(previous) = entry.output_replay.lock().await.as_mut()
                        && previous.scope == scope
                        && previous.sequence == sequence
                        && previous.events == events
                    {
                        previous.result = applied.clone();
                    }
                    return output_result(applied);
                }
                let (response, wait) = oneshot::channel();
                entry
                    .commands
                    .send(Command::FeedSequencedOutput(
                        Box::new(FeedOutput {
                            handle: handle.clone(),
                            turn_id: scope.turn_id.clone(),
                            channel_ref: scope.channel_ref.clone(),
                            conversation: scope.conversation.clone(),
                            sequence,
                            events,
                        }),
                        response,
                    ))
                    .await
                    .map_err(|_| Error::failed("progressive reply is closed"))?;
                drop(closing);
                let applied = wait
                    .await
                    .map_err(|_| Error::failed("progressive worker stopped"))??;
                output_result(applied)
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
        self.open_bound(channel, OutputTarget::Legacy, None).await
    }

    async fn open_target(
        &self,
        channel: String,
        target: OutputTarget,
    ) -> Result<ProgressiveResult> {
        self.open_bound(channel, target, None).await
    }

    async fn open_bound(
        &self,
        channel: String,
        target: OutputTarget,
        scope: Option<(
            Option<String>,
            Option<tinychannels_bus::ChannelRef>,
            Option<tinychannels_bus::ConversationRef>,
        )>,
    ) -> Result<ProgressiveResult> {
        if channel.is_empty() || channel.len() > 4096 {
            return Err(Error::failed("invalid progressive target"));
        }
        let provider_id = tinychannels_bus::provider_id(&channel);
        let capabilities = match &target {
            OutputTarget::Adapter(_, built, _) => built.capabilities(),
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
        let bound_scope = scope.map(|(turn_id, channel_ref, conversation)| OutputScope {
            handle: handle.clone(),
            channel: channel.clone(),
            turn_id,
            channel_ref,
            conversation,
        });
        let (commands, inbox) = mpsc::channel(16);
        let host = self.host.clone();
        let output_replay = Arc::new(Mutex::new(None));
        let module_scope = match &target {
            OutputTarget::Legacy => None,
            OutputTarget::Hosted(scope) | OutputTarget::Adapter(scope, _, _) => Some(scope.clone()),
        };
        let lifetime = match &target {
            OutputTarget::Legacy => TURN_LIFETIME,
            OutputTarget::Hosted(scope) | OutputTarget::Adapter(scope, _, _) => scope.lifetime,
        };
        let worker_target = match &target {
            OutputTarget::Legacy => WorkerTarget::Legacy,
            OutputTarget::Hosted(scope) => WorkerTarget::Hosted(scope.clone()),
            OutputTarget::Adapter(scope, built, spoken) => {
                WorkerTarget::Adapter(scope.clone(), built.clone(), spoken.clone())
            }
        };
        let worker = tokio::spawn(run(
            host,
            channel.clone(),
            capabilities,
            worker_target,
            lifetime,
            inbox,
            output_replay.clone(),
        ));
        state.entries.insert(
            handle.clone(),
            Arc::new(Entry {
                channel,
                bound_scope,
                module_scope,
                commands,
                worker: Mutex::new(Some(worker)),
                closing: Mutex::new(None),
                output_replay,
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
    Hosted(OpenScope),
    Adapter(OpenScope, BuiltChannel, Option<Arc<SpokenReply>>),
}

async fn run(
    host: HostChannels,
    channel: String,
    capabilities: tinychannels_bus::ChannelCapabilities,
    target: WorkerTarget,
    lifetime: Duration,
    mut inbox: mpsc::Receiver<Command>,
    output_replay: Arc<Mutex<Option<OutputBatchReplay>>>,
) {
    let module_sequenced = !matches!(target, WorkerTarget::Legacy);
    let sender = Sender::new(host.clone());
    let (transport, context, hosted) = match &target {
        WorkerTarget::Legacy => (
            Arc::new(sender.clone()) as Arc<dyn ProgressiveSender>,
            None,
            false,
        ),
        WorkerTarget::Hosted(scope) => (
            Arc::new(sender.clone()) as Arc<dyn ProgressiveSender>,
            Some(output_context(scope)),
            true,
        ),
        WorkerTarget::Adapter(scope, built, _) => (
            Arc::new(AdapterSender {
                built: built.clone(),
                scope: scope.clone(),
            }) as Arc<dyn ProgressiveSender>,
            Some(output_context(scope)),
            false,
        ),
    };
    let reply_capabilities = capabilities.clone();
    let mut reply = match context {
        Some(context) if hosted => {
            ProgressiveReply::new_hosted_scoped(transport, channel.clone(), capabilities, context)
        }
        Some(context) => {
            ProgressiveReply::new_scoped(transport, channel.clone(), capabilities, context)
        }
        None => ProgressiveReply::new(transport, channel.clone(), capabilities),
    };
    let next_sequence = AtomicU64::new(0);
    let adapter_capabilities = reply_capabilities;
    let mut edits = tokio::time::interval(EDIT_FLUSH_INTERVAL);
    let mut typing = tokio::time::interval(TYPING_REFRESH_INTERVAL);
    let mut fillers = tokio::time::interval(FILLER_INTERVAL);
    for timer in [&mut edits, &mut typing, &mut fillers] {
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer.tick().await;
    }
    // A module-sequenced handle is scoped from its open; a legacy handle only
    // types once a scoped output batch has bound its transport scope.
    if module_sequenced {
        reply.typing_tick().await;
    }
    let expiry = tokio::time::sleep(lifetime);
    tokio::pin!(expiry);
    loop {
        tokio::select! {
            command = inbox.recv() => match command {
                Some(Command::Feed(events, response)) => {
                    let result = apply_legacy_feed(&mut reply, &sender, events).await;
                    let _ = response.send(result);
                }
                Some(Command::FeedOutput(events, response)) => {
                    // A delivered `FinalMessage` ends the module-sequenced
                    // handle, as it does for host-sequenced output: a later
                    // `Close` must not announce a cancellation.
                    let is_final = events.iter().any(|event| {
                        matches!(event, ChannelOutputEvent::FinalMessage { .. })
                    });
                    let result = match &target {
                        WorkerTarget::Hosted(scope) => feed_output(&host, &channel, Some(scope), &next_sequence, events).await,
                        WorkerTarget::Adapter(scope, _, spoken) => {
                            let voice = SpokenContext {
                                spoken: spoken.as_deref(),
                                scope,
                                capabilities: &adapter_capabilities,
                            };
                            feed_adapter_output(&mut reply, &voice, events).await
                        }
                        WorkerTarget::Legacy => Err(Error::failed("canonical output requires an opened output scope")),
                    };
                    let terminal = is_final && result.is_ok();
                    let _ = response.send(result);
                    if terminal {
                        reply.cleanup_after_external_final(true).await;
                        break;
                    }
                }
                Some(Command::FeedSequencedOutput(batch, response)) => {
                    let scope = OutputScope {
                        handle: batch.handle,
                        channel: channel.clone(),
                        turn_id: batch.turn_id,
                        channel_ref: batch.channel_ref,
                        conversation: batch.conversation,
                    };
                    let is_final = batch.events.iter().any(|event| {
                        matches!(event, ChannelOutputEvent::FinalMessage { .. })
                    });
                    let result = apply_scoped_output(
                        &mut reply,
                        &sender,
                        &scope,
                        batch.sequence,
                        batch.events,
                        &output_replay,
                    ).await;
                    let terminal = is_final && result.is_ok();
                    let final_message_delivered = result
                        .as_ref()
                        .is_ok_and(|applied| applied.final_message_delivered);
                    let successful = result.is_ok();
                    let _ = response.send(result);
                    if successful && !terminal && !final_message_delivered {
                        reply.typing_tick().await;
                    }
                    if terminal {
                        reply
                            .cleanup_after_external_final(final_message_delivered)
                            .await;
                        break;
                    }
                }
                Some(Command::Finish(text, response)) => {
                    reply.finalize(&text).await;
                    let _ = response.send(());
                    break;
                }
                None => { reply.finalize("Cancelled.").await; break; }
            },
            _ = edits.tick() => reply.edit_tick().await,
            _ = typing.tick() => {
                if sender.has_scope().await {
                    reply.typing_tick().await;
                }
            },
            _ = fillers.tick() => reply.filler_tick().await,
            _ = &mut expiry => { reply.finalize("Sorry, the request timed out.").await; break; }
        }
    }
}

fn output_context(scope: &OpenScope) -> ProgressiveOutputContext {
    ProgressiveOutputContext {
        turn_id: scope.turn_id.clone(),
        channel: scope.channel.clone(),
        conversation: scope.conversation.clone(),
    }
}

/// What the adapter worker needs to add a spoken reply to a final message.
struct SpokenContext<'a> {
    spoken: Option<&'a SpokenReply>,
    scope: &'a OpenScope,
    capabilities: &'a tinychannels_bus::ChannelCapabilities,
}

impl SpokenContext<'_> {
    /// Add the synthesized voice part beside a final text reply. Text-only
    /// delivery is the fallback for every unavailable stage.
    async fn with_voice(
        &self,
        event: tinychannels_bus::module_progressive::ChannelOutputEvent,
    ) -> tinychannels_bus::module_progressive::ChannelOutputEvent {
        use tinychannels_bus::module_progressive::ChannelOutputEvent as Event;
        let Some(spoken) = self.spoken else {
            return event;
        };
        let Event::FinalMessage {
            text,
            mut parts,
            receipt,
        } = event
        else {
            return event;
        };
        let has_voice = parts
            .iter()
            .any(|part| matches!(part, tinychannels_bus::OutboundContentPart::Voice { .. }));
        if !has_voice
            && let Some(voice) = spoken
                .voice_part(
                    &self.scope.conversation,
                    &self.scope.turn_id,
                    &text,
                    self.capabilities,
                )
                .await
        {
            if parts.is_empty() {
                parts.push(tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: text.clone(),
                        markdown_dialect: None,
                    },
                });
            }
            parts.push(voice);
        }
        Event::FinalMessage {
            text,
            parts,
            receipt,
        }
    }
}

async fn feed_adapter_output(
    reply: &mut ProgressiveReply,
    voice: &SpokenContext<'_>,
    events: Vec<tinychannels_bus::module_progressive::ChannelOutputEvent>,
) -> Result<ProgressiveResult> {
    let mut dispatch = tinychannels::delivery::progressive::ProgressiveOutputDispatch::default();
    for event in events {
        let event = voice.with_voice(event).await;
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
    scope: OpenScope,
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
        // Adapters without a typing port have no indicator to refresh.
        match self.built.typing() {
            Some(port) => port
                .start_typing(&self.scope.conversation.id)
                .await
                .map_err(map_send_error),
            None => Ok(()),
        }
    }
}

fn output_scope(
    channel: tinychannels_bus::ChannelRef,
    conversation: tinychannels_bus::ConversationRef,
    turn_id: String,
    lifetime_ms: u64,
) -> Result<OpenScope> {
    if channel.id.trim().is_empty()
        || conversation.id.trim().is_empty()
        || turn_id.trim().is_empty()
        || lifetime_ms == 0
        || lifetime_ms > MAX_SCOPED_OUTPUT_LIFETIME.as_millis() as u64
    {
        return Err(Error::failed("invalid progressive output scope"));
    }
    Ok(OpenScope {
        channel,
        conversation,
        turn_id,
        lifetime: Duration::from_millis(lifetime_ms),
    })
}

async fn feed_output(
    host: &HostChannels,
    channel: &str,
    scope: Option<&OpenScope>,
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

async fn apply_scoped_output(
    reply: &mut ProgressiveReply,
    sender: &Sender,
    scope: &OutputScope,
    sequence: u64,
    events: Vec<ChannelOutputEvent>,
    output_replay: &Mutex<Option<OutputBatchReplay>>,
) -> Result<AppliedOutput> {
    sender.check_output_scope(scope).await?;
    let applied = apply_output(reply, sender, scope, sequence, events, output_replay).await?;
    sender.bind_output_scope(scope.clone()).await;
    sender.set_scope(Some(scope.clone())).await;
    Ok(applied)
}

async fn apply_legacy_feed(
    reply: &mut ProgressiveReply,
    sender: &Sender,
    events: Vec<ProgressiveEvent>,
) -> Result<()> {
    apply(reply, events)?;
    sender.set_scope(None).await;
    Ok(())
}

async fn apply_output(
    reply: &mut ProgressiveReply,
    sender: &Sender,
    scope: &OutputScope,
    sequence: u64,
    events: Vec<ChannelOutputEvent>,
    output_replay: &Mutex<Option<OutputBatchReplay>>,
) -> Result<AppliedOutput> {
    let event_count =
        u64::try_from(events.len()).map_err(|_| Error::failed("progressive sequence overflow"))?;
    if event_count > 0 {
        sequence
            .checked_add(event_count - 1)
            .ok_or_else(|| Error::failed("progressive sequence overflow"))?;
    }
    if events.iter().enumerate().any(|(index, event)| {
        matches!(event, ChannelOutputEvent::FinalMessage { .. }) && index + 1 != events.len()
    }) {
        return Err(Error::failed(
            "progressive final message must be the last event in its batch",
        ));
    }
    let previous = output_replay.lock().await.clone();
    if let Some(applied) = replay_output(previous.as_ref(), scope, sequence, &events) {
        let mut applied = applied?;
        retry_failed_output_callbacks(sender, scope, sequence, &events, &mut applied).await;
        if let Some(previous) = output_replay.lock().await.as_mut()
            && previous.scope == *scope
            && previous.sequence == sequence
            && previous.events == events
        {
            previous.result = applied.clone();
        }
        return Ok(applied);
    }
    let text = events
        .iter()
        .filter_map(|event| match event {
            ChannelOutputEvent::TextDelta { text, .. } => Some(text.len()),
            _ => None,
        })
        .fold(0usize, usize::saturating_add);
    let oversized_final = events.iter().any(|event| {
        matches!(event, ChannelOutputEvent::FinalMessage { text, .. } if text.len() > MAX_CONTENT)
    });
    let thinking = events
        .iter()
        .filter_map(|event| match event {
            ChannelOutputEvent::ThinkingDelta { text, .. } => Some(text.len()),
            _ => None,
        })
        .sum::<usize>();
    if oversized_final
        || reply.state().content.len().saturating_add(text) > MAX_CONTENT
        || reply
            .state()
            .thinking_accumulator
            .len()
            .saturating_add(thinking)
            > MAX_CONTENT
    {
        return Err(Error::failed("progressive content exceeds its limit"));
    }
    let replay_events = events.clone();
    let mut deliveries = Vec::new();
    let mut output_event_failures = Vec::new();
    let mut final_message_delivered = false;
    for (offset, event) in events.into_iter().enumerate() {
        let sequence = sequence
            .checked_add(
                u64::try_from(offset)
                    .map_err(|_| Error::failed("progressive sequence overflow"))?,
            )
            .ok_or_else(|| Error::failed("progressive sequence overflow"))?;
        let event = match event {
            ChannelOutputEvent::FinalMessage {
                text,
                parts,
                receipt: original_receipt,
            } => {
                let delivery = sender
                    .send_typed(scope, sequence, parts.clone(), text.clone())
                    .await
                    .unwrap_or(ProgressiveTransportResult::Failed);
                final_message_delivered =
                    matches!(&delivery, ProgressiveTransportResult::Success(_))
                        || matches!(
                            &delivery,
                            ProgressiveTransportResult::Outcome(outcome)
                                if outcome.error.is_none()
                                    || outcome.receipt.primary_platform_message_id.is_some()
                                    || !outcome.receipt.platform_message_ids.is_empty()
                                    || !outcome.receipt.parts.is_empty()
                        );
                let receipt = match &delivery {
                    ProgressiveTransportResult::Outcome(outcome) => Some(outcome.receipt.clone()),
                    _ => original_receipt,
                };
                deliveries.push(delivery);
                ChannelOutputEvent::FinalMessage {
                    text,
                    parts,
                    receipt,
                }
            }
            event => event,
        };
        if sender
            .output_event(scope, sequence, event.clone())
            .await
            .is_err()
        {
            output_event_failures.push(sequence);
        }
        match event {
            ChannelOutputEvent::TextDelta { text, .. } => reply.on_text_delta(&text),
            ChannelOutputEvent::ThinkingDelta { text, .. } => reply.on_thinking_delta(&text),
            ChannelOutputEvent::ToolCallStarted { tool_name, .. } => reply.on_tool_call(&tool_name),
            ChannelOutputEvent::ToolCallCompleted {
                tool_name, success, ..
            } => reply.on_tool_result(&tool_name, success),
            _ => {}
        }
    }
    let applied = AppliedOutput {
        deliveries,
        output_event_failures,
        final_message_delivered,
    };
    if !replay_events.is_empty() {
        *output_replay.lock().await = Some(OutputBatchReplay {
            scope: scope.clone(),
            sequence,
            events: replay_events,
            result: applied.clone(),
        });
    }
    Ok(applied)
}

async fn retry_failed_output_callbacks(
    sender: &Sender,
    scope: &OutputScope,
    sequence: u64,
    events: &[ChannelOutputEvent],
    applied: &mut AppliedOutput,
) {
    let failures = std::mem::take(&mut applied.output_event_failures);
    for failed_sequence in failures {
        let Some(offset) = failed_sequence
            .checked_sub(sequence)
            .and_then(|offset| usize::try_from(offset).ok())
        else {
            applied.output_event_failures.push(failed_sequence);
            continue;
        };
        let Some(event) = events.get(offset) else {
            applied.output_event_failures.push(failed_sequence);
            continue;
        };
        if sender
            .output_event(scope, failed_sequence, event.clone())
            .await
            .is_err()
        {
            applied.output_event_failures.push(failed_sequence);
        }
    }
}

fn replay_output(
    previous: Option<&OutputBatchReplay>,
    scope: &OutputScope,
    sequence: u64,
    events: &[ChannelOutputEvent],
) -> Option<Result<AppliedOutput>> {
    let previous = previous?;
    let previous_end = previous
        .sequence
        .saturating_add(previous.events.len().saturating_sub(1) as u64);
    if sequence == previous.sequence {
        return Some(if previous.scope == *scope && previous.events == events {
            Ok(previous.result.clone())
        } else {
            Err(Error::failed(
                "progressive output sequence was reused with different content",
            ))
        });
    }
    (sequence <= previous_end).then(|| Err(Error::failed("progressive output sequence is stale")))
}

fn output_result(applied: AppliedOutput) -> Result<ProgressiveResult> {
    if applied.deliveries.is_empty() && applied.output_event_failures.is_empty() {
        Ok(ProgressiveResult::Updated)
    } else {
        Ok(ProgressiveResult::OutputUpdated {
            deliveries: applied.deliveries,
            output_event_failures: applied.output_event_failures,
        })
    }
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
