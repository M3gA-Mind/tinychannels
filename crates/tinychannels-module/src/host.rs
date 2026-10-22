//! The host-side object this module calls, and the client that reaches it.
//!
//! # Why inbound needs its own object
//!
//! `Interface::call` receives a member name and a JSON body — no caller
//! identity, no connection — so a served object cannot open a stream or send a
//! reply back to whoever called it. That is fine for a codec, where every
//! answer belongs to the call that asked for it. A channel is not a codec: the
//! interesting traffic is unsolicited, arriving from Telegram or Slack at a
//! moment no host call is outstanding.
//!
//! So the direction is inverted for that half. The **host** serves
//! [`HOST_BUS_NAME`], and this module calls it. That is the same shape
//! `tinymemory`'s `RuntimeHost` uses, and it is why `tinychannels-bus` declares
//! two objects rather than one.
//!
//! # One ordered consumer, not a task per message
//!
//! Notifications go through a bounded queue drained by a **single** task that
//! awaits each call before starting the next. Two properties fall out, and both
//! are load-bearing:
//!
//! - **Order is preserved.** Spawning a task per notification would let two host
//!   calls race, and the host would observe conversation turns in an order the
//!   provider never produced them in. For a chat surface that is corruption, not
//!   a performance detail.
//! - **Memory is bounded.** A task per notification also means unbounded
//!   outstanding calls and cloned payloads whenever the host is slower than the
//!   traffic, which is exactly when you can least afford to grow.
//!
//! Backpressure therefore propagates the way it already did: a full outbox
//! blocks the forwarder, which stops draining the provider's bounded inbound
//! queue, which blocks the provider — the same path a slow consumer always took.
//! Nothing is silently dropped.
//!
//! Identity runtimes may explicitly opt into a separate bounded turn dispatcher.
//! It invokes `ProcessInbound` concurrently while the notification consumer
//! still owns status/persistence/event ordering. Its barriers drain the accepted
//! turn frontier and shutdown joins the dispatcher. Legacy callers retain the
//! serial v22 `DeliverInbound` envelope contract described above.
//!
//! # Failures are logged, not retried
//!
//! A failed delivery is logged and the queue moves on. Retrying here would be
//! wrong: this module has no durable spool, so a retry loop would either stall
//! the ordered queue behind one unreachable host or grow in memory. Durable
//! redelivery belongs to whoever owns storage, which is the host.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tinybus::{Connection, Error, Result};
use tinychannels_bus::ChannelEvent;
use tinychannels_bus::names::{HOST_BUS_NAME, HOST_OBJECT_PATH, host_methods};
use tinychannels_bus::{ChannelBackendRequest, ChannelBackendResult};
use tinychannels_bus::{HostEventPublication, HostTranscriptionRequest, HostTranscriptionResult};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

const OUTBOX_CAPACITY: usize = 256;
const MAX_CALLBACK_BYTES: usize = 128 * 1024;
const MAX_TRANSCRIPTION_REQUEST_BYTES: usize = 12 * 1024 * 1024;
const MAX_ACTIVE_HOST_CALLBACKS: usize = 16;
const MAX_CONTROLLER_PAYLOAD_BYTES: usize = 1024 * 1024;

#[path = "host_dispatch.rs"]
mod dispatch;

enum Notification {
    DeliverInbound {
        channel: String,
        message: serde_json::Value,
    },
    ReportStatus {
        channel: String,
        state: String,
        detail: Option<String>,
    },
    PersistAllowedIdentity {
        channel: String,
        identity: String,
        reply: oneshot::Sender<Result<()>>,
    },
    PublishEvent {
        publication: HostEventPublication,
        reply: oneshot::Sender<Result<()>>,
    },
    DeliverEvent {
        event: Box<ChannelEvent>,
        reply: oneshot::Sender<Result<()>>,
    },
    Barrier {
        stop: bool,
        reply: oneshot::Sender<()>,
    },
}

struct Owner {
    worker: Mutex<Option<JoinHandle<()>>>,
    terminal: AtomicBool,
    publication: Mutex<()>,
    active_callbacks: AtomicUsize,
    callbacks_drained: Notify,
    failure: Arc<StdMutex<Option<String>>>,
}

struct ActiveHostCallback(Arc<Owner>);

impl Drop for ActiveHostCallback {
    fn drop(&mut self) {
        self.0.active_callbacks.fetch_sub(1, Ordering::SeqCst);
        self.0.callbacks_drained.notify_waiters();
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().take() {
            worker.abort();
        }
    }
}

/// An ordered, bounded client for the optional host callback object.
#[derive(Clone)]
pub struct HostChannels {
    outbox: mpsc::Sender<Notification>,
    owner: Arc<Owner>,
    connection: Connection,
    object_path: Arc<str>,
    /// The host's storage objects, connected on first use and then shared by
    /// every clone.
    storage: Arc<Mutex<Option<tinystoragedrivers_api::Storage>>>,
}
impl std::fmt::Debug for HostChannels {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostChannels")
            .finish_non_exhaustive()
    }
}
impl HostChannels {
    pub(crate) fn new_at_with_dispatch(
        connection: Connection,
        object_path: String,
        max_in_flight: usize,
    ) -> Self {
        Self::new_at_inner(connection, object_path, Some(max_in_flight))
    }

    /// Start one owned callback consumer. Explicit shutdown drains and joins it.
    #[must_use]
    pub fn new(connection: Connection) -> Self {
        Self::new_at(connection, HOST_OBJECT_PATH.to_owned())
    }

    /// Bind all callbacks to an immutable runtime-specific host object.
    pub(crate) fn new_at(connection: Connection, object_path: String) -> Self {
        Self::new_at_inner(connection, object_path, None)
    }

    fn new_at_inner(
        connection: Connection,
        object_path: String,
        dispatch_limit: Option<usize>,
    ) -> Self {
        let object_path: Arc<str> = object_path.into();
        let callback_path = object_path.clone();
        let (outbox, mut inbox) = mpsc::channel(OUTBOX_CAPACITY);
        let failure = Arc::new(StdMutex::new(None));
        let task_failure = failure.clone();
        let callback_connection = connection.clone();
        let worker = tokio::spawn(async move {
            let mut dispatcher = dispatch_limit.map(|limit| {
                dispatch::InboundDispatcher::new(
                    callback_connection.clone(),
                    callback_path.clone(),
                    task_failure.clone(),
                    limit,
                )
            });
            while let Some(notification) = inbox.recv().await {
                if let Notification::Barrier { stop, reply } = notification {
                    if let Some(dispatcher) = dispatcher.as_ref() {
                        dispatcher.flush().await;
                    }
                    let _ = reply.send(());
                    if stop {
                        break;
                    }
                    continue;
                }
                if let (Some(dispatcher), Notification::DeliverInbound { channel, message }) =
                    (dispatcher.as_mut(), &notification)
                {
                    dispatcher.enqueue(channel.clone(), message.clone()).await;
                    continue;
                }
                if let Notification::PersistAllowedIdentity {
                    channel,
                    identity,
                    reply,
                } = notification
                {
                    let result = async {
                        let proxy = callback_connection.proxy(
                            HOST_BUS_NAME,
                            callback_path.as_ref(),
                            HOST_BUS_NAME,
                        )?;
                        proxy
                            .call::<()>(host_methods::PERSIST_ALLOWED_IDENTITY, (channel, identity))
                            .await
                    }
                    .await;
                    let _ = reply.send(result);
                    continue;
                }
                if let Notification::PublishEvent { publication, reply } = notification {
                    let result = async {
                        let proxy = callback_connection.proxy(
                            HOST_BUS_NAME,
                            callback_path.as_ref(),
                            HOST_BUS_NAME,
                        )?;
                        proxy
                            .call::<()>(host_methods::PUBLISH_EVENT, publication)
                            .await
                    }
                    .await;
                    let result = match result {
                        Err(error)
                            if error.wire_name()
                                == "ai.tinyhumans.tinybus.Error.NameHasNoOwner"
                                || error.wire_name()
                                    == "ai.tinyhumans.tinybus.Error.MemberNotFound" =>
                        {
                            // Event sinks are optional capabilities. A host on
                            // an older callback surface must not make provider
                            // startup or shutdown fail because an event is
                            // unavailable.
                            Ok(())
                        }
                        result => result,
                    };
                    let _ = reply.send(result);
                    continue;
                }
                if let Notification::DeliverEvent { event, reply } = notification {
                    let result = async {
                        let proxy = callback_connection.proxy(
                            HOST_BUS_NAME,
                            callback_path.as_ref(),
                            HOST_BUS_NAME,
                        )?;
                        proxy.call::<()>(host_methods::DELIVER_EVENT, *event).await
                    }
                    .await;
                    let _ = reply.send(result);
                    continue;
                }
                let inbound = matches!(notification, Notification::DeliverInbound { .. });
                let result = async {
                    let proxy = callback_connection.proxy(
                        HOST_BUS_NAME,
                        callback_path.as_ref(),
                        HOST_BUS_NAME,
                    )?;
                    match notification {
                        Notification::DeliverInbound { channel, message } => {
                            proxy
                                .call::<()>(host_methods::DELIVER_INBOUND, (channel, message))
                                .await
                        }
                        Notification::ReportStatus {
                            channel,
                            state,
                            detail,
                        } => {
                            proxy
                                .call::<()>(host_methods::REPORT_STATUS, (channel, state, detail))
                                .await
                        }
                        Notification::PersistAllowedIdentity { .. } => unreachable!(),
                        Notification::PublishEvent { .. } => unreachable!(),
                        Notification::DeliverEvent { .. } => unreachable!(),
                        Notification::Barrier { .. } => Ok(()),
                    }
                }
                .await;
                if let Err(error) = result {
                    // No host object is a supported outbound-only configuration.
                    if inbound || error.wire_name() != "ai.tinyhumans.tinybus.Error.NameHasNoOwner"
                    {
                        record_failure(&task_failure, error.to_string());
                    }
                }
            }
            if let Some(dispatcher) = dispatcher.as_mut() {
                dispatcher.shutdown().await;
            }
        });
        Self {
            outbox,
            connection,
            object_path,
            storage: Arc::new(Mutex::new(None)),
            owner: Arc::new(Owner {
                worker: Mutex::new(Some(worker)),
                terminal: AtomicBool::new(false),
                publication: Mutex::new(()),
                active_callbacks: AtomicUsize::new(0),
                callbacks_drained: Notify::new(),
                failure,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn connection(&self) -> Connection {
        self.connection.clone()
    }

    /// The module's scoped handle on the host's storage, over the storage bus.
    ///
    /// The host binds the scope from the authenticated caller; nothing here
    /// names one. A connect failure is not cached, so a host that starts
    /// serving storage later is picked up by the next call.
    pub(crate) async fn storage(
        &self,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_api::Storage> {
        let mut slot = self.storage.lock().await;
        if let Some(scoped) = slot.as_ref() {
            return Ok(scoped.clone());
        }
        let storage = tinystoragedrivers_busproxy::connect_storage(self.connection.clone()).await?;
        *slot = Some(storage.clone());
        Ok(storage)
    }

    /// The scoped document store, for the delivery, remote-session and
    /// WhatsApp session records.
    pub(crate) async fn documents(
        &self,
    ) -> tinystoragedrivers_types::Result<Arc<dyn tinystoragedrivers_api::DocumentStore>> {
        Ok(Arc::clone(self.storage().await?.documents()))
    }

    /// Request transcription through the host's configured speech provider.
    pub async fn transcribe(
        &self,
        request: HostTranscriptionRequest,
    ) -> Result<HostTranscriptionResult> {
        if serde_json::to_vec(&request)
            .map_or(true, |bytes| bytes.len() > MAX_TRANSCRIPTION_REQUEST_BYTES)
        {
            return Err(Error::failed("transcription request exceeds byte limit"));
        }
        let publication = self.owner.publication.lock().await;
        if self.owner.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("callback consumer is closed"));
        }
        // Admission is serialized with shutdown by `publication`; callbacks
        // can only decrement this count while an admission is in progress.
        // Reserve first, then roll back if the existing count was already full.
        if self.owner.active_callbacks.fetch_add(1, Ordering::SeqCst) >= MAX_ACTIVE_HOST_CALLBACKS {
            self.owner.active_callbacks.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::failed("too many active transcription callbacks"));
        }
        let owner = self.owner.clone();
        let connection = self.connection.clone();
        let object_path = self.object_path.clone();
        let (reply, wait) = oneshot::channel();
        drop(publication);
        tokio::spawn(async move {
            let _active = ActiveHostCallback(owner);
            let result = async {
                let proxy = connection.proxy(HOST_BUS_NAME, object_path.as_ref(), HOST_BUS_NAME)?;
                proxy.call(host_methods::TRANSCRIBE, request).await
            }
            .await;
            let _ = reply.send(result);
        });
        wait.await
            .map_err(|_| Error::failed("transcription callback was interrupted"))?
    }

    /// Invoke one bounded host-owned credential, persistence, or managed API
    /// capability required by the module's controller adapter.
    pub async fn dispatch_controller_backend(
        &self,
        request: ChannelBackendRequest,
    ) -> Result<ChannelBackendResult> {
        if serde_json::to_vec(&request)
            .map_or(true, |bytes| bytes.len() > MAX_CONTROLLER_PAYLOAD_BYTES)
        {
            return Err(Error::failed("controller request exceeds byte limit"));
        }
        let publication = self.owner.publication.lock().await;
        if self.owner.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("callback consumer is closed"));
        }
        if self.owner.active_callbacks.fetch_add(1, Ordering::SeqCst) >= MAX_ACTIVE_HOST_CALLBACKS {
            self.owner.active_callbacks.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::failed("too many active host callbacks"));
        }
        let owner = self.owner.clone();
        let connection = self.connection.clone();
        let object_path = self.object_path.clone();
        let (reply, wait) = oneshot::channel();
        drop(publication);
        tokio::spawn(async move {
            let _active = ActiveHostCallback(owner);
            let result = async {
                let proxy = connection.proxy(HOST_BUS_NAME, object_path.as_ref(), HOST_BUS_NAME)?;
                let result: ChannelBackendResult = proxy
                    .call(host_methods::DISPATCH_CONTROLLER_BACKEND, request)
                    .await?;
                if serde_json::to_vec(&result)
                    .map_or(true, |bytes| bytes.len() > MAX_CONTROLLER_PAYLOAD_BYTES)
                {
                    return Err(Error::failed("controller result exceeds byte limit"));
                }
                Ok(result)
            }
            .await;
            let _ = reply.send(result);
        });
        wait.await
            .map_err(|_| Error::failed("host controller callback was interrupted"))?
    }

    /// Publish one bounded provider event through the host event sink.
    pub async fn publish_event(&self, publication: HostEventPublication) -> Result<()> {
        if serde_json::to_vec(&publication).map_or(true, |bytes| bytes.len() > MAX_CALLBACK_BYTES) {
            return Err(Error::failed("event callback byte limit exceeded"));
        }
        let (reply, wait) = oneshot::channel();
        let _publication = self.owner.publication.lock().await;
        if self.owner.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("callback consumer is closed"));
        }
        self.outbox
            .send(Notification::PublishEvent { publication, reply })
            .await
            .map_err(|_| Error::failed("callback consumer is closed"))?;
        drop(_publication);
        wait.await
            .map_err(|_| Error::failed("event publication was interrupted"))?
    }

    /// Deliver one typed provider event over the scoped host callback.
    pub async fn deliver_event(&self, event: ChannelEvent) -> Result<()> {
        if serde_json::to_vec(&event).map_or(true, |bytes| bytes.len() > MAX_CALLBACK_BYTES) {
            return Err(Error::failed("channel event callback byte limit exceeded"));
        }
        let (reply, wait) = oneshot::channel();
        let _publication = self.owner.publication.lock().await;
        if self.owner.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("callback consumer is closed"));
        }
        self.outbox
            .send(Notification::DeliverEvent {
                event: Box::new(event),
                reply,
            })
            .await
            .map_err(|_| Error::failed("callback consumer is closed"))?;
        drop(_publication);
        match wait
            .await
            .map_err(|_| Error::failed("event delivery was interrupted"))?
        {
            Err(error)
                if error.wire_name() == "ai.tinyhumans.tinybus.Error.NameHasNoOwner"
                    || error.wire_name() == "ai.tinyhumans.tinybus.Error.MemberNotFound" =>
            {
                Ok(())
            }
            result => result,
        }
    }

    /// Enqueue one inbound message; a flush observes delivery failures.
    pub async fn deliver_inbound(
        &self,
        channel: &str,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) {
        let Ok(message) = serde_json::to_value(envelope) else {
            record_failure(
                &self.owner.failure,
                "inbound envelope could not be encoded".into(),
            );
            return;
        };
        self.deliver_inbound_value(channel, message).await;
    }

    /// Enqueue authenticated relay provenance with its normalized envelope.
    pub async fn deliver_relay_inbound(
        &self,
        channel: &str,
        delivery: tinychannels_bus::module_relay::RelayInboundDelivery,
    ) {
        let Ok(message) = serde_json::to_value(delivery) else {
            record_failure(
                &self.owner.failure,
                "relay envelope could not be encoded".into(),
            );
            return;
        };
        self.deliver_inbound_value(channel, message).await;
    }

    async fn deliver_inbound_value(&self, channel: &str, message: serde_json::Value) {
        if serde_json::to_vec(&message).map_or(true, |bytes| bytes.len() > MAX_CALLBACK_BYTES) {
            record_failure(
                &self.owner.failure,
                "inbound callback byte limit exceeded".into(),
            );
            return;
        }
        self.enqueue(Notification::DeliverInbound {
            channel: channel.to_owned(),
            message,
        })
        .await;
    }

    /// Enqueue a status fact after preceding inbound messages.
    pub async fn report_status(&self, channel: &str, state: &str, detail: Option<String>) {
        if channel.len() > 1024
            || state.len() > 128
            || detail.as_ref().is_some_and(|detail| detail.len() > 4096)
        {
            record_failure(
                &self.owner.failure,
                "status callback byte limit exceeded".into(),
            );
            return;
        }
        self.enqueue(Notification::ReportStatus {
            channel: channel.to_owned(),
            state: state.to_owned(),
            detail,
        })
        .await;
    }

    /// Persist a provider-authorized identity through the host-owned policy store.
    pub async fn persist_allowed_identity(&self, channel: &str, identity: &str) -> Result<()> {
        let (reply, wait) = oneshot::channel();
        let _publication = self.owner.publication.lock().await;
        if self.owner.terminal.load(Ordering::SeqCst) {
            return Err(Error::failed("callback consumer is closed"));
        }
        self.outbox
            .send(Notification::PersistAllowedIdentity {
                channel: channel.to_owned(),
                identity: identity.to_owned(),
                reply,
            })
            .await
            .map_err(|_| Error::failed("callback consumer is closed"))?;
        drop(_publication);
        wait.await
            .map_err(|_| Error::failed("allowlist persistence was interrupted"))?
    }

    async fn enqueue(&self, notification: Notification) {
        let _publication = self.owner.publication.lock().await;
        if self.owner.terminal.load(Ordering::SeqCst)
            || self.outbox.send(notification).await.is_err()
        {
            record_failure(&self.owner.failure, "callback consumer is closed".into());
        }
    }

    /// Await all preceding callbacks, returning an observed terminal delivery error.
    ///
    /// # Errors
    /// Returns a callback failure once, or a closed-consumer error. Dropping this
    /// waiter does not cancel the owned consumer or discard its failure facts.
    pub async fn flush(&self) -> Result<()> {
        let (reply, wait) = oneshot::channel();
        self.outbox
            .send(Notification::Barrier { stop: false, reply })
            .await
            .map_err(|_| Error::failed("callback consumer is closed"))?;
        wait.await
            .map_err(|_| Error::failed("callback barrier was interrupted"))?;
        self.take_failure()
    }

    /// Freeze publication, drain all callbacks and join the consumer.
    ///
    /// # Errors
    /// Reports a terminal delivery error once. A canceled waiter leaves the
    /// join handle retained for a later shutdown retry.
    pub async fn shutdown(&self) -> Result<()> {
        let publication = self.owner.publication.lock().await;
        self.owner.terminal.store(true, Ordering::SeqCst);
        drop(publication);
        loop {
            let notified = self.owner.callbacks_drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.owner.active_callbacks.load(Ordering::SeqCst) == 0 {
                break;
            }
            notified.await;
        }
        let (reply, wait) = oneshot::channel();
        if self
            .outbox
            .send(Notification::Barrier { stop: true, reply })
            .await
            .is_ok()
        {
            let _ = wait.await;
        }
        let mut worker = self.owner.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            let _ = task.await;
        }
        worker.take();
        self.take_failure()
    }

    fn take_failure(&self) -> Result<()> {
        match self
            .owner
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            Some(error) => Err(Error::failed(error)),
            None => Ok(()),
        }
    }
}
fn record_failure(failure: &StdMutex<Option<String>>, error: String) {
    let mut failure = failure.lock().unwrap_or_else(|error| error.into_inner());
    if failure.is_none() {
        *failure = Some(error);
    }
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "host_dispatch_tests.rs"]
mod dispatch_tests;
