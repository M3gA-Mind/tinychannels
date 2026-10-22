//! Bounded module-owned progressive reply workers and retained terminal cleanup.
use crate::host::HostChannels;
use crate::provider::Running;
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tinybus::{Error, Result};
use tinychannels::delivery::progressive::{
    EDIT_FLUSH_INTERVAL, FILLER_INTERVAL, ProgressiveReply, ProgressiveSendError,
    ProgressiveSender, TYPING_REFRESH_INTERVAL,
};
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

struct Sender(HostChannels);

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

struct AppliedOutput {
    deliveries: Vec<ProgressiveTransportResult>,
    output_event_failures: Vec<u64>,
}

impl Sender {
    async fn output_event(
        &self,
        scope: &OutputScope,
        sequence: u64,
        event: ChannelOutputEvent,
    ) -> Result<()> {
        let result = self
            .0
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
            ChannelBackendResult::ProgressiveTransport(
                ProgressiveTransportResult::Acknowledged | ProgressiveTransportResult::Accepted,
            ) => Ok(()),
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
            .0
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
            ChannelBackendResult::ProgressiveTransport(result) => Ok(result),
            _ => Err(Error::failed("progressive typed delivery failed")),
        }
    }

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
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::Success(
                value,
            )) => Ok(value),
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::Unavailable) => {
                Err(ProgressiveSendError::Unavailable)
            }
            ChannelBackendResult::ProgressiveTransport(
                ProgressiveTransportResult::EditUnsupported,
            ) => Err(ProgressiveSendError::EditUnsupported),
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::MessageGone) => {
                Err(ProgressiveSendError::MessageGone)
            }
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
    FeedOutput(Box<FeedOutput>, oneshot::Sender<Result<AppliedOutput>>),
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
                let (response, wait) = oneshot::channel();
                entry
                    .commands
                    .send(Command::FeedOutput(
                        Box::new(FeedOutput {
                            handle: handle.clone(),
                            turn_id,
                            channel_ref: channel_ref.map(|value| *value),
                            conversation: conversation.map(|value| *value),
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
                if applied.deliveries.is_empty() && applied.output_event_failures.is_empty() {
                    Ok(ProgressiveResult::Updated)
                } else {
                    Ok(ProgressiveResult::OutputUpdated {
                        deliveries: applied.deliveries,
                        output_event_failures: applied.output_event_failures,
                    })
                }
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
        if channel.is_empty() || channel.len() > 4096 {
            return Err(Error::failed("invalid progressive target"));
        }
        let provider_id = tinychannels_bus::provider_id(&channel);
        let capabilities = self
            .running
            .lock()
            .await
            .get(provider_id)
            .map(|provider| provider.channel.capabilities())
            .unwrap_or_else(tinychannels_bus::ChannelCapabilities::unknown);
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
        let worker = tokio::spawn(run(host, channel, capabilities, inbox));
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

async fn run(
    host: HostChannels,
    channel: String,
    capabilities: tinychannels_bus::ChannelCapabilities,
    mut inbox: mpsc::Receiver<Command>,
) {
    let mut reply = ProgressiveReply::new(
        Arc::new(Sender(host.clone())),
        channel.clone(),
        capabilities,
    );
    let sender = Sender(host);
    let mut edits = tokio::time::interval(EDIT_FLUSH_INTERVAL);
    let mut typing = tokio::time::interval(TYPING_REFRESH_INTERVAL);
    let mut fillers = tokio::time::interval(FILLER_INTERVAL);
    for timer in [&mut edits, &mut typing, &mut fillers] {
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer.tick().await;
    }
    reply.typing_tick().await;
    let expiry = tokio::time::sleep(TURN_LIFETIME);
    tokio::pin!(expiry);
    loop {
        tokio::select! {
            command = inbox.recv() => match command {
                Some(Command::Feed(events, response)) => {
                    let result = apply(&mut reply, events);
                    let _ = response.send(result);
                }
                Some(Command::FeedOutput(batch, response)) => {
                    let scope = OutputScope {
                        handle: batch.handle,
                        channel: channel.clone(),
                        turn_id: batch.turn_id,
                        channel_ref: batch.channel_ref,
                        conversation: batch.conversation,
                    };
                    let result = apply_output(
                        &mut reply,
                        &sender,
                        &scope,
                        batch.sequence,
                        batch.events,
                    ).await;
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

async fn apply_output(
    reply: &mut ProgressiveReply,
    sender: &Sender,
    scope: &OutputScope,
    sequence: u64,
    events: Vec<ChannelOutputEvent>,
) -> Result<AppliedOutput> {
    let event_count =
        u64::try_from(events.len()).map_err(|_| Error::failed("progressive sequence overflow"))?;
    if event_count > 0 {
        sequence
            .checked_add(event_count - 1)
            .ok_or_else(|| Error::failed("progressive sequence overflow"))?;
    }
    let mut deliveries = Vec::new();
    let mut output_event_failures = Vec::new();
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
    Ok(AppliedOutput {
        deliveries,
        output_event_failures,
    })
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
