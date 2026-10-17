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
    EDIT_FLUSH_INTERVAL, FILLER_INTERVAL, ProgressiveOutputContext, ProgressiveReply,
    ProgressiveSendError, ProgressiveSender, TYPING_REFRESH_INTERVAL,
};
use tinychannels_bus::module_progressive::{
    ChannelOutputEvent, ProgressiveCommand, ProgressiveResult, ProgressiveTransportRequest,
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

struct Sender {
    host: HostChannels,
    context: Option<ProgressiveOutputContext>,
}

impl Sender {
    async fn invoke(
        &self,
        request: ProgressiveTransportRequest,
    ) -> std::result::Result<Value, ProgressiveSendError> {
        let result = self
            .host
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(request))
            .await
            .map_err(|_| ProgressiveSendError::Unavailable)?;
        match result {
            ChannelBackendResult::ProgressiveTransport(
                ProgressiveTransportResult::Acknowledged,
            ) => Ok(Value::Null),
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::Outcome(
                outcome,
            )) => match outcome.error {
                None => serde_json::to_value(outcome.receipt)
                    .map_err(|error| ProgressiveSendError::Other(error.into())),
                Some(_) => Err(ProgressiveSendError::PartialOutcome(outcome)),
            },
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::Success(
                value,
            )) => {
                if value.get("error").is_some_and(|error| !error.is_null())
                    || value.get("success").and_then(Value::as_bool) == Some(false)
                    || value.get("status").and_then(Value::as_str) == Some("unknown_after_send")
                {
                    Err(ProgressiveSendError::Other(anyhow::anyhow!(
                        "progressive delivery failed"
                    )))
                } else {
                    Ok(value)
                }
            }
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
    async fn output_event(
        &self,
        channel: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        event: &ChannelOutputEvent,
    ) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::OutputEvent {
            channel: channel.to_owned(),
            turn_id: context.map(|context| context.turn_id.clone()),
            channel_ref: context.map(|context| context.channel.clone()),
            conversation: context.map(|context| context.conversation.clone()),
            sequence,
            event: event.clone(),
        })
        .await
        .map(drop)
    }
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
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            message,
        })
        .await
    }
    async fn send_parts(
        &self,
        channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        context: Option<&ProgressiveOutputContext>,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError>
    {
        match self
            .host
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(
                ProgressiveTransportRequest::SendTyped {
                    channel: channel.into(),
                    turn_id: context.map(|context| context.turn_id.clone()),
                    channel_ref: context.map(|context| context.channel.clone()),
                    conversation: context.map(|context| context.conversation.clone()),
                    parts: parts.to_vec(),
                    text: text.into(),
                },
            ))
            .await
            .map_err(|_| ProgressiveSendError::Unavailable)?
        {
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::Outcome(
                outcome,
            )) => Ok(outcome),
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::Unavailable) => {
                Err(ProgressiveSendError::Unavailable)
            }
            ChannelBackendResult::ProgressiveTransport(ProgressiveTransportResult::Failed) => {
                Err(ProgressiveSendError::Other(anyhow::anyhow!(
                    "typed provider send failed with unknown outcome"
                )))
            }
            ChannelBackendResult::ProgressiveTransport(
                ProgressiveTransportResult::UnsupportedContent,
            ) => Err(ProgressiveSendError::UnsupportedContent),
            _ => Err(ProgressiveSendError::UnsupportedContent),
        }
    }
    async fn edit(
        &self,
        channel: &str,
        message_id: &str,
        text: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Edit {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            message_id: message_id.into(),
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
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            message_id: message_id.into(),
        })
        .await
        .map(drop)
    }
    async fn typing(&self, channel: &str) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Typing {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
        })
        .await
        .map(drop)
    }
}

enum Command {
    Feed(
        Vec<ChannelOutputEvent>,
        bool,
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
            } => self.open_output(channel, conversation, turn_id).await,
            ProgressiveCommand::Feed { handle, events } => {
                let events = events.into_iter().map(compat_event).collect();
                self.feed(handle, events, false).await
            }
            ProgressiveCommand::FeedOutput { handle, events } => {
                self.feed(handle, events, true).await
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

    async fn feed(
        &self,
        handle: String,
        events: Vec<ChannelOutputEvent>,
        forward: bool,
    ) -> Result<ProgressiveResult> {
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
            .send(Command::Feed(events, forward, reply))
            .await
            .map_err(|_| Error::failed("progressive reply is closed"))?;
        drop(closing);
        Ok(wait
            .await
            .map_err(|_| Error::failed("progressive worker stopped"))??)
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
        self.open_inner(channel, None).await
    }

    async fn open_output(
        &self,
        channel: tinychannels_bus::ChannelRef,
        conversation: tinychannels_bus::ConversationRef,
        turn_id: String,
    ) -> Result<ProgressiveResult> {
        if channel.id.is_empty()
            || conversation.id.is_empty()
            || turn_id.is_empty()
            || channel.id.len() > 4096
            || conversation.id.len() > 4096
            || turn_id.len() > 256
        {
            return Err(Error::failed("invalid scoped progressive target"));
        }
        let route = format!("{}:{}", channel.id, conversation.id);
        self.open_inner(
            route,
            Some(ProgressiveOutputContext {
                turn_id,
                channel,
                conversation,
            }),
        )
        .await
    }

    async fn open_inner(
        &self,
        channel: String,
        context: Option<ProgressiveOutputContext>,
    ) -> Result<ProgressiveResult> {
        if channel.is_empty() || channel.len() > 4096 {
            return Err(Error::failed("invalid progressive target"));
        }
        let provider_id = tinychannels_bus::provider_id(&channel);
        let running = self.running.lock().await;
        let capabilities = if let Some(context) = context.as_ref() {
            let configured_instance = context
                .channel
                .account_id
                .as_deref()
                .unwrap_or(&context.channel.id);
            if tinychannels_bus::provider_id(configured_instance) != context.channel.id {
                return Err(Error::failed(
                    "scoped channel identity does not match its configured account",
                ));
            }
            running
                .get(configured_instance)
                .ok_or_else(|| Error::failed("scoped channel instance is not configured"))?
                .channel
                .capabilities()
        } else {
            running
                .get(provider_id)
                .map(|provider| provider.channel.capabilities())
                .unwrap_or_else(tinychannels_bus::ChannelCapabilities::unknown)
        };
        drop(running);
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
        let worker = tokio::spawn(run(host, channel, capabilities, context, inbox));
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

fn compat_event(
    event: tinychannels_bus::module_progressive::ProgressiveEvent,
) -> ChannelOutputEvent {
    use tinychannels_bus::module_progressive::ProgressiveEvent as Legacy;
    match event {
        Legacy::TextDelta(text) => ChannelOutputEvent::TextDelta {
            text,
            iteration: None,
        },
        Legacy::ThinkingDelta(text) => ChannelOutputEvent::ThinkingDelta {
            text,
            iteration: None,
        },
        Legacy::ToolCall(tool_name) => ChannelOutputEvent::ToolProgress {
            label: tool_name,
            message: None,
        },
        Legacy::ToolResult { tool_name, success } => ChannelOutputEvent::ToolCallCompleted {
            call_id: String::new(),
            tool_name,
            success,
            output_chars: 0,
            output: String::new(),
            arguments: None,
            elapsed_ms: 0,
            iteration: 0,
            failure: None,
            display_label: None,
            display_detail: None,
            structured: None,
        },
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
    context: Option<ProgressiveOutputContext>,
    mut inbox: mpsc::Receiver<Command>,
) {
    let mut reply = if let Some(context) = context {
        ProgressiveReply::new_scoped(
            Arc::new(Sender {
                host,
                context: Some(context.clone()),
            }),
            channel,
            capabilities,
            context,
        )
    } else {
        ProgressiveReply::new(
            Arc::new(Sender {
                host,
                context: None,
            }),
            channel,
            capabilities,
        )
    };
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
                Some(Command::Feed(events, forward, response)) => {
                    let cancellation = events
                        .iter()
                        .any(|event| matches!(event, ChannelOutputEvent::Cancellation { .. }));
                    let final_message = events.iter().find_map(|event| match event {
                        ChannelOutputEvent::FinalMessage { text, parts, .. } => {
                            Some((text.clone(), parts.clone(), !parts.is_empty()))
                        }
                        _ => None,
                    });
                    let mut result = apply(&mut reply, events, forward).await;
                    let failed = result.is_err();
                    if failed {
                        reply.abort().await;
                        let _ = response.send(result);
                        break;
                    }
                    if cancellation {
                        reply.abort().await;
                        let _ = response.send(Ok(ProgressiveResult::Closed));
                        break;
                    }
                    if let Some((text, event_parts, has_typed_parts)) = final_message {
                        if forward {
                            let parts = if has_typed_parts {
                                event_parts
                            } else {
                                vec![tinychannels_bus::OutboundContentPart::Content {
                                    part: tinychannels_bus::ContentPart::Text { text: text.clone(), markdown_dialect: None },
                                }]
                            };
                            result = match reply.send_parts(&parts, &text).await {
                                Ok(outcome) => Ok(ProgressiveResult::DeliveryOutcome { outcome }),
                                Err(ProgressiveSendError::UnsupportedContent) => {
                                    reply.abort().await;
                                    Ok(ProgressiveResult::DeliveryUnsupported { parts })
                                }
                                Err(error) => {
                                    reply.abort().await;
                                    Err(Error::failed(error.to_string()))
                                }
                            };
                        } else {
                            reply.finalize(&text).await;
                            result = Ok(ProgressiveResult::Closed);
                        }
                        let _ = response.send(result);
                        break;
                    }
                    let _ = response.send(result);
                }
                Some(Command::Finish(text, response)) => {
                    reply.finalize(&text).await;
                    let _ = response.send(());
                    break;
                }
                None => { reply.abort().await; break; }
            },
            _ = edits.tick() => reply.edit_tick().await,
            _ = typing.tick() => reply.typing_tick().await,
            _ = fillers.tick() => reply.filler_tick().await,
            _ = &mut expiry => { reply.abort().await; break; }
        }
    }
}

async fn apply(
    reply: &mut ProgressiveReply,
    events: Vec<ChannelOutputEvent>,
    forward: bool,
) -> Result<ProgressiveResult> {
    let text = events
        .iter()
        .filter_map(|event| match event {
            ChannelOutputEvent::TextDelta { text, .. } => Some(text.len()),
            _ => None,
        })
        .sum::<usize>();
    let thinking = events
        .iter()
        .filter_map(|event| match event {
            ChannelOutputEvent::ThinkingDelta { text, .. } => Some(text.len()),
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
    let mut outcomes = Vec::new();
    let mut unsupported = Vec::new();
    for event in events {
        if forward {
            reply
                .on_output_event(&event)
                .await
                .map_err(|_| Error::failed("host output event callback failed"))?;
        }
        match event {
            ChannelOutputEvent::TextDelta { text, .. } => reply.on_text_delta(&text),
            ChannelOutputEvent::ThinkingDelta { text, .. } => reply.on_thinking_delta(&text),
            ChannelOutputEvent::ToolCallStarted {
                tool_name,
                display_label,
                ..
            } => reply.on_tool_call(display_label.as_deref().unwrap_or(&tool_name)),
            ChannelOutputEvent::ToolProgress { label, message } => {
                reply.on_tool_call(message.as_deref().unwrap_or(&label))
            }
            ChannelOutputEvent::ToolCallProgress { message, .. } => reply.on_tool_call(&message),
            ChannelOutputEvent::ToolCallCompleted {
                tool_name,
                display_label,
                success,
                ..
            } => reply.on_tool_result(display_label.as_deref().unwrap_or(&tool_name), success),
            ChannelOutputEvent::FinalMessage { .. }
            | ChannelOutputEvent::ToolCallArgsDelta { .. }
            | ChannelOutputEvent::ApprovalRequest { .. }
            | ChannelOutputEvent::Clarification { .. } => {}
            ChannelOutputEvent::Media { text, parts } if forward => {
                let mut outbound = Vec::new();
                if let Some(text) = text {
                    outbound.push(tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text,
                            markdown_dialect: None,
                        },
                    });
                }
                outbound.extend(
                    parts
                        .into_iter()
                        .map(|part| tinychannels_bus::OutboundContentPart::Content { part }),
                );
                match reply.send_content_parts(&outbound, "").await {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(ProgressiveSendError::UnsupportedContent) => unsupported.extend(outbound),
                    Err(error) => return Err(Error::failed(error.to_string())),
                }
            }
            ChannelOutputEvent::VoiceNote { text, voice } if forward => {
                let mut outbound = Vec::new();
                if let Some(text) = text.or_else(|| {
                    voice
                        .transcript
                        .as_ref()
                        .map(|transcript| transcript.text.clone())
                }) {
                    outbound.push(tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text,
                            markdown_dialect: None,
                        },
                    });
                }
                outbound.push(tinychannels_bus::OutboundContentPart::Voice {
                    voice: tinychannels_bus::OutboundVoice::Media {
                        media: voice.media,
                        codec: voice.codec,
                        duration_ms: voice.duration_ms,
                    },
                });
                match reply.send_content_parts(&outbound, "").await {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(ProgressiveSendError::UnsupportedContent) => unsupported.extend(outbound),
                    Err(error) => return Err(Error::failed(error.to_string())),
                }
            }
            ChannelOutputEvent::Media { .. }
            | ChannelOutputEvent::VoiceNote { .. }
            | ChannelOutputEvent::Subagent { .. }
            | ChannelOutputEvent::Usage { .. }
            | ChannelOutputEvent::ModelUsage { .. }
            | ChannelOutputEvent::Artifact { .. }
            | ChannelOutputEvent::Cancellation { .. }
            | ChannelOutputEvent::Lifecycle { .. }
            | ChannelOutputEvent::Native { .. } => {}
        }
    }
    if !unsupported.is_empty() {
        Ok(ProgressiveResult::DeliveryUnsupported { parts: unsupported })
    } else if !outcomes.is_empty() {
        Ok(ProgressiveResult::DeliveryOutcomes { outcomes })
    } else {
        Ok(ProgressiveResult::Updated)
    }
}

#[cfg(test)]
#[path = "progressive_tests.rs"]
mod tests;
