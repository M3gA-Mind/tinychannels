//! Owned provider listener, lifecycle hooks and ordered inbound forwarding.
use crate::host::HostChannels;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tinybus::{Error, Result};
use tinychannels::host::{LifecycleRegistry, ShutdownHook};
use tinychannels::{Channel, ChannelHostBuilder, SendMessage};
use tinychannels_bus::{ChannelSupervisionPolicy, HostEventPublication, HostTranscriptionRequest};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum ProviderOperation {
    Send(SendMessage),
    Interaction(tinychannels_bus::ChannelInteraction),
}

struct TypingRefreshTask {
    recipient: String,
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

type TypingRefreshes = HashMap<String, TypingRefreshTask>;

const MAX_TYPING_REFRESH_RECIPIENTS: usize = 64;
const MAX_TYPING_REFRESH_INTERVAL_SECS: u64 = 60;

async fn dispatch_interaction(
    channel: Arc<dyn Channel>,
    interaction: tinychannels_bus::ChannelInteraction,
    typing: &mut TypingRefreshes,
) -> Result<tinychannels_bus::ChannelInteractionResult> {
    use tinychannels_bus::ChannelInteraction as Op;
    match interaction {
        Op::StartTypingRefresh {
            recipient,
            interval_secs,
        } => {
            if interval_secs == 0 || interval_secs > MAX_TYPING_REFRESH_INTERVAL_SECS {
                return Err(Error::failed(
                    "typing refresh interval is outside its limit",
                ));
            }
            if typing.len() >= MAX_TYPING_REFRESH_RECIPIENTS {
                return Err(Error::failed("typing refresh capacity is full"));
            }
            let (cancel, mut canceled) = watch::channel(false);
            let refresh_channel = Arc::clone(&channel);
            let refresh_recipient = recipient.clone();
            let interval = Duration::from_secs(interval_secs);
            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = async { let _ = canceled.wait_for(|value| *value).await; } => break,
                        _ = tokio::time::sleep(interval) => {
                            let refresh = refresh_channel.start_typing(&refresh_recipient);
                            tokio::select! {
                                biased;
                                _ = async { let _ = canceled.wait_for(|value| *value).await; } => break,
                                result = refresh => if let Err(error) = result {
                                tracing::debug!(channel = refresh_channel.name(), "typing refresh failed: {error}");
                                }
                            }
                        }
                    }
                }
            });
            let handle = loop {
                let candidate = uuid::Uuid::new_v4().simple().to_string();
                if !typing.contains_key(&candidate) {
                    break candidate;
                }
            };
            typing.insert(
                handle.clone(),
                TypingRefreshTask {
                    recipient,
                    cancel,
                    task,
                },
            );
            Ok(tinychannels_bus::ChannelInteractionResult::TypingRefresh { handle })
        }
        Op::StopTypingRefresh { handle } => {
            stop_typing_refresh(&handle, typing).await;
            Ok(tinychannels_bus::ChannelInteractionResult::Completed)
        }
        interaction => crate::interaction::dispatch(channel.as_ref(), interaction).await,
    }
}

async fn stop_typing_refresh(handle: &str, typing: &mut TypingRefreshes) {
    if let Some(refresh) = typing.remove(handle) {
        refresh.cancel.send_replace(true);
        if let Err(error) = refresh.task.await {
            tracing::debug!(%handle, recipient = %refresh.recipient, "typing refresh task join failed: {error}");
        }
    }
}

async fn stop_all_typing_refreshes(typing: &mut TypingRefreshes) {
    let handles = typing.keys().cloned().collect::<Vec<_>>();
    for handle in handles {
        stop_typing_refresh(&handle, typing).await;
    }
}

struct SendCommand {
    operation: ProviderOperation,
    reply: oneshot::Sender<Result<tinychannels_bus::ChannelInteractionResult>>,
    started: oneshot::Sender<()>,
}

#[derive(Debug)]
pub(crate) enum RunningSendError {
    NotDispatched(String),
    AfterDispatch(String),
}

impl std::fmt::Display for RunningSendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotDispatched(message) | Self::AfterDispatch(message) => {
                formatter.write_str(message)
            }
        }
    }
}

/// Native hooks registered while the selected provider starts.
#[derive(Default)]
pub(crate) struct Hooks(StdMutex<Vec<ShutdownHook>>);
impl LifecycleRegistry for Hooks {
    fn register_shutdown(&self, _name: &str, hook: ShutdownHook) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).push(hook);
    }
}
impl Hooks {
    pub(crate) fn host(
        self: &Arc<Self>,
        callbacks: HostChannels,
    ) -> Arc<dyn tinychannels::host::ChannelHost> {
        ChannelHostBuilder::new()
            .lifecycle(self.clone())
            .transcriber(Arc::new(ModuleTranscriber(callbacks.clone())))
            .events(Arc::new(ModuleEvents(callbacks.clone())))
            .storage(Arc::new(ModuleStorage(callbacks.clone())))
            .allowlist(Arc::new(ModuleAllowlist(callbacks)))
            .build()
    }
    async fn drain(&self) {
        let hooks = std::mem::take(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()));
        for hook in hooks {
            hook().await;
        }
    }
}

struct ModuleStorage(HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::StorageProvider for ModuleStorage {
    async fn documents(&self) -> anyhow::Result<Arc<dyn tinystoragedrivers_api::DocumentStore>> {
        self.0
            .documents()
            .await
            .map_err(|error| anyhow::anyhow!("host storage: {:?}", error.kind()))
    }
}

struct ModuleAllowlist(HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::AllowlistStore for ModuleAllowlist {
    async fn persist_allowed_identity(&self, channel: &str, identity: &str) -> anyhow::Result<()> {
        self.0
            .persist_allowed_identity(channel, identity)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

struct ModuleTranscriber(HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::Transcriber for ModuleTranscriber {
    fn name(&self) -> &str {
        "module-host"
    }

    async fn transcribe(
        &self,
        request: tinychannels::host::TranscriptionRequest,
    ) -> anyhow::Result<tinychannels::host::TranscriptionResult> {
        let result = self
            .0
            .transcribe(HostTranscriptionRequest {
                audio_base64: request.audio_base64,
                mime_type: request.mime_type,
                file_name: request.file_name,
                language: request.language,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(tinychannels::host::TranscriptionResult {
            text: result.text,
            language: result.language,
            duration_secs: result.duration_secs,
        })
    }
}

struct ModuleEvents(HostChannels);

#[async_trait::async_trait]
impl tinychannels::host::EventSink for ModuleEvents {
    async fn publish(
        &self,
        domain: &str,
        kind: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.0
            .publish_event(HostEventPublication {
                domain: domain.to_owned(),
                kind: kind.to_owned(),
                payload,
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

/// Reconnect execution stays in the module; raw provider errors never cross callbacks.
async fn listen_with_supervision(
    channel: Arc<dyn Channel>,
    tx: mpsc::Sender<tinychannels::ChannelMessage>,
    host: HostChannels,
    policy: Option<ChannelSupervisionPolicy>,
) -> anyhow::Result<()> {
    let Some(policy) = policy else {
        return channel.listen(tx).await;
    };
    let name = channel.name().to_owned();
    let mut backoff = policy.initial_backoff_secs.max(1);
    let maximum = policy.max_backoff_secs.max(backoff);
    loop {
        let result = channel.listen(tx.clone()).await;
        if tx.is_closed() {
            return result;
        }
        host.report_status(&name, "restarting", Some("provider disconnected".into()))
            .await;
        tokio::time::sleep(
            Duration::from_secs(backoff)
                + Duration::from_millis(tinychannels::runtime::jitter_millis(backoff)),
        )
        .await;
        backoff = backoff.saturating_mul(2).min(maximum);
        host.report_status(&name, "connecting", None).await;
    }
}

/// A resource is retained until its owning task acknowledges cleanup.
pub(crate) struct Running {
    pub(crate) channel: Arc<dyn Channel>,
    send: mpsc::Sender<SendCommand>,
    cancel: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    finished: Arc<AtomicBool>,
}
impl Running {
    pub(crate) fn start(
        channel: Arc<dyn Channel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
    ) -> Arc<Self> {
        Self::start_inner(channel, hooks, host, None, false)
    }
    pub(crate) fn start_supervised(
        channel: Arc<dyn Channel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
        policy: ChannelSupervisionPolicy,
    ) -> Arc<Self> {
        Self::start_inner(channel, hooks, host, Some(policy), false)
    }
    pub(crate) fn start_outbound(
        channel: Arc<dyn Channel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
    ) -> Arc<Self> {
        Self::start_inner(channel, hooks, host, None, true)
    }
    fn start_inner(
        channel: Arc<dyn Channel>,
        hooks: Arc<Hooks>,
        host: HostChannels,
        supervision: Option<ChannelSupervisionPolicy>,
        outbound_only: bool,
    ) -> Arc<Self> {
        let (cancel, mut canceled) = watch::channel(false);
        let send_cancel = cancel.subscribe();
        let (send, mut sends) = mpsc::channel::<SendCommand>(256);
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = finished.clone();
        let listen_channel = channel.clone();
        let send_channel = channel.clone();
        let worker_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            let name = listen_channel.name().to_owned();
            host.report_status(&name, "connecting", None).await;
            let (tx, mut rx) = tokio::sync::mpsc::channel(256);
            let forward_host = host.clone();
            let forward_name = name.clone();
            let mut forwarder = AbortOnDrop(tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    if let Ok(value) = serde_json::to_value(message) {
                        forward_host.deliver_inbound(&forward_name, value).await;
                    }
                }
            }));
            let listener_host = host.clone();
            let mut listener = AbortOnDrop(tokio::spawn(async move {
                if outbound_only {
                    // Keep the inbound sender alive until cancellation so the forwarder
                    // has the same explicit cleanup path as native listeners.
                    let _inbound = tx;
                    std::future::pending::<anyhow::Result<()>>().await
                } else {
                    listen_with_supervision(listen_channel, tx, listener_host, supervision).await
                }
            }));
            let mut send_cancel = send_cancel;
            let mut sender = AbortOnDrop(tokio::spawn(async move {
                let mut typing = TypingRefreshes::new();
                loop {
                    let command = tokio::select! {biased;
                        _=async {let _=send_cancel.wait_for(|value|*value).await;}=>break,
                        command=sends.recv()=>match command {Some(command)=>command,None=>break},
                    };
                    let _ = command.started.send(());
                    let result = match command.operation {
                        ProviderOperation::Send(message) => send_channel
                            .send(&message)
                            .await
                            .map(|()| tinychannels_bus::ChannelInteractionResult::Completed)
                            .map_err(|error| Error::failed(error.to_string())),
                        ProviderOperation::Interaction(interaction) => {
                            dispatch_interaction(send_channel.clone(), interaction, &mut typing)
                                .await
                        }
                    };
                    let _ = command.reply.send(result);
                }
                stop_all_typing_refreshes(&mut typing).await;
                while let Ok(command) = sends.try_recv() {
                    let _ = command
                        .reply
                        .send(Err(Error::failed("provider is stopping")));
                }
            }));
            let outcome = tokio::select! {biased;
                _=async {let _=canceled.wait_for(|value|*value).await;}=>{listener.0.abort(); let _=(&mut listener.0).await;None},
                outcome=&mut listener.0=>Some(outcome),
            };
            worker_cancel.send_replace(true);
            // Join listener first: it cannot register another native hook later.
            hooks.drain().await;
            let _ = (&mut sender.0).await;
            let _ = (&mut forwarder.0).await;
            host.report_status(
                &name,
                if outcome.is_some_and(|outcome| !matches!(outcome, Ok(Ok(())))) {
                    "failed"
                } else {
                    "stopped"
                },
                None,
            )
            .await;
            task_finished.store(true, Ordering::SeqCst);
        });
        Arc::new(Self {
            channel,
            send,
            cancel,
            worker: Mutex::new(Some(worker)),
            finished,
        })
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
    pub(crate) async fn send(
        &self,
        message: SendMessage,
    ) -> std::result::Result<(), RunningSendError> {
        self.submit(ProviderOperation::Send(message))
            .await
            .map(|_| ())
    }

    pub(crate) async fn interact(
        &self,
        interaction: tinychannels_bus::ChannelInteraction,
    ) -> std::result::Result<tinychannels_bus::ChannelInteractionResult, RunningSendError> {
        self.submit(ProviderOperation::Interaction(interaction))
            .await
    }

    async fn submit(
        &self,
        operation: ProviderOperation,
    ) -> std::result::Result<tinychannels_bus::ChannelInteractionResult, RunningSendError> {
        let (reply, wait) = oneshot::channel();
        let (started, dispatch) = oneshot::channel();
        self.send
            .try_send(SendCommand {
                operation,
                reply,
                started,
            })
            .map_err(|_| {
                RunningSendError::NotDispatched("provider send queue is full or closed".into())
            })?;
        match wait.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => match dispatch.await {
                Ok(()) => Err(RunningSendError::AfterDispatch(error.to_string())),
                Err(_) => Err(RunningSendError::NotDispatched(error.to_string())),
            },
            Err(_) => match dispatch.await {
                Ok(()) => Err(RunningSendError::AfterDispatch(
                    "provider send worker stopped after dispatch".into(),
                )),
                Err(_) => Err(RunningSendError::NotDispatched(
                    "provider send worker stopped before dispatch".into(),
                )),
            },
        }
    }
    pub(crate) async fn stop(&self) -> Result<()> {
        self.cancel.send_replace(true);
        let mut worker = self.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .map_err(|_| Error::failed("provider cleanup is pending; retry StopChannel"))?
                .map_err(|_| Error::failed("provider cleanup worker failed"))?;
        }
        worker.take();
        Ok(())
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().take() {
            worker.abort();
        }
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
