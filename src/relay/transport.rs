//! Relay frame transport loop.
//! Runtime-owned I/O seams are re-exported here to preserve legacy paths.

use tinychannels_runtime::relay::ConnectorToGatewayFrameExt as _;

pub use tinychannels_runtime::relay::transport::{
    RelayFrameDialer, RelayFrameIo, RelayIdentity, RelayInboundHandler,
    RelayInterruptInboundHandler, RelayPassthroughHandler, RelayReconnectPolicy,
    RelayTransportError, RelayTransportTimeouts,
};

use crate::relay::{CapabilityDescriptor, ConnectorToGatewayFrame, GatewayToConnectorFrame};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify, RwLock, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};

/// Hermes-compatible relay transport loop over a frame I/O implementation.
pub struct RelayTransport {
    identities: Vec<RelayIdentity>,
    io: RwLock<Arc<dyn RelayFrameIo>>,
    timeouts: RelayTransportTimeouts,
    state: Arc<RelayTransportState>,
    reader: Mutex<Option<JoinHandle<()>>>,
    terminal: AtomicBool,
    v2_mode: AtomicBool,
}

impl RelayTransport {
    pub fn new(
        identities: Vec<RelayIdentity>,
        io: Arc<dyn RelayFrameIo>,
        timeouts: RelayTransportTimeouts,
    ) -> Self {
        let configured_identities = identities.iter().cloned().collect();
        Self {
            identities,
            io: RwLock::new(io),
            timeouts,
            state: Arc::new(RelayTransportState {
                configured_identities,
                ..RelayTransportState::default()
            }),
            reader: Mutex::new(None),
            terminal: AtomicBool::new(false),
            v2_mode: AtomicBool::new(false),
        }
    }

    pub async fn connect(&self) -> Result<(), RelayTransportError> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(RelayTransportError::Closed);
        }
        self.prepare_connect().await;
        self.v2_mode.store(false, Ordering::SeqCst);
        self.start_reader().await;
        self.send_hellos().await
    }

    /// Start the reader and send only v2 hellos, without requiring a v1 descriptor.
    pub async fn connect_v2(&self) -> Result<(), RelayTransportError> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(RelayTransportError::Closed);
        }
        self.prepare_connect().await;
        self.v2_mode.store(true, Ordering::SeqCst);
        self.start_reader().await;
        let io = self.current_io().await;
        for identity in &self.identities {
            io.send(GatewayToConnectorFrame::HelloV2 {
                platform: identity.platform.clone(),
                bot_id: identity.bot_id.clone(),
                contract_version: tinychannels_bus::ChannelCapabilities::VERSION,
            })
            .await?;
        }
        Ok(())
    }

    pub async fn reconnect_with_io(
        &self,
        io: Arc<dyn RelayFrameIo>,
    ) -> Result<(), RelayTransportError> {
        *self.io.write().await = io;
        if self.v2_mode.load(Ordering::SeqCst) {
            self.connect_v2().await
        } else {
            self.connect().await
        }
    }

    pub fn spawn_reconnect_supervisor(
        self: &Arc<Self>,
        dialer: Arc<dyn RelayFrameDialer>,
        policy: RelayReconnectPolicy,
    ) -> RelayReconnectHandle {
        let shutdown = Arc::new(AtomicBool::new(false));
        let task_shutdown = shutdown.clone();
        let transport = self.clone();
        let task = tokio::spawn(async move {
            transport
                .reconnect_loop(dialer, policy, task_shutdown)
                .await;
        });
        RelayReconnectHandle { shutdown, task }
    }

    /// Permanently stop and join the owned reader, failing all pending replies.
    /// A shut down transport cannot reconnect; create a new owner instead.
    pub async fn shutdown(&self) {
        self.terminal.store(true, Ordering::SeqCst);
        let mut reader = self.reader.lock().await;
        if let Some(task) = reader.take() {
            task.abort();
            let _ = task.await;
        }
        self.state.fail_waiters(RelayTransportError::Closed).await;
    }

    /// Safe connection facts without credentials or message contents.
    pub async fn descriptor(&self) -> Option<CapabilityDescriptor> {
        self.state.descriptor.lock().await.clone()
    }

    /// Wait for the negotiated v2 handshake descriptor.
    pub async fn capabilities_v2(
        &self,
    ) -> HashMap<RelayIdentity, tinychannels_bus::ChannelCapabilities> {
        self.state.capabilities_v2.lock().await.clone()
    }

    /// Return the v2 descriptor negotiated for one configured relay identity.
    pub async fn capabilities_v2_for(
        &self,
        identity: &RelayIdentity,
    ) -> Option<tinychannels_bus::ChannelCapabilities> {
        self.state
            .capabilities_v2
            .lock()
            .await
            .get(identity)
            .cloned()
    }

    /// Whether the reader has ended or terminal shutdown was requested.
    pub fn is_closed(&self) -> bool {
        self.terminal.load(Ordering::SeqCst) || self.state.closed.load(Ordering::SeqCst)
    }

    async fn prepare_connect(&self) {
        *self.state.descriptor.lock().await = None;
        self.state.capabilities_v2.lock().await.clear();
        self.state.capability_errors.lock().await.clear();
        self.state.closed.store(false, Ordering::SeqCst);
    }

    async fn send_hellos(&self) -> Result<(), RelayTransportError> {
        let io = self.current_io().await;
        for identity in &self.identities {
            io.send(GatewayToConnectorFrame::Hello {
                platform: identity.platform.clone(),
                bot_id: identity.bot_id.clone(),
            })
            .await?;
        }
        Ok(())
    }

    async fn current_io(&self) -> Arc<dyn RelayFrameIo> {
        self.io.read().await.clone()
    }

    pub async fn handshake(&self) -> Result<CapabilityDescriptor, RelayTransportError> {
        loop {
            let notified = self.state.descriptor_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(descriptor) = self.state.descriptor.lock().await.clone() {
                return Ok(descriptor);
            }
            if self.state.closed.load(Ordering::SeqCst) {
                return Err(RelayTransportError::Closed);
            }
            timeout(Duration::from_millis(self.timeouts.handshake_ms), notified)
                .await
                .map_err(|_| RelayTransportError::Timeout {
                    operation: "handshake",
                })?;
        }
    }

    /// Wait until the relay peer supplies its canonical v2 capabilities.
    pub async fn handshake_v2(
        &self,
        identity: &RelayIdentity,
    ) -> Result<tinychannels_bus::ChannelCapabilities, RelayTransportError> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.timeouts.handshake_ms);
        self.handshake_v2_until(identity, deadline).await
    }

    /// Negotiate canonical v2 capabilities with every configured relay identity.
    /// The v1 hello and descriptor path remains available to older peers.
    pub async fn negotiate_v2(
        &self,
    ) -> Result<HashMap<RelayIdentity, tinychannels_bus::ChannelCapabilities>, RelayTransportError>
    {
        if self.terminal.load(Ordering::SeqCst)
            || self.state.closed.load(Ordering::SeqCst)
            || !self.state.reader_started.load(Ordering::SeqCst)
        {
            return Err(RelayTransportError::Closed);
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.timeouts.handshake_ms);
        let mut negotiated = HashMap::new();
        for identity in &self.identities {
            let capabilities = self.handshake_v2_until(identity, deadline).await?;
            negotiated.insert(identity.clone(), capabilities);
        }
        Ok(negotiated)
    }

    async fn handshake_v2_until(
        &self,
        identity: &RelayIdentity,
        deadline: tokio::time::Instant,
    ) -> Result<tinychannels_bus::ChannelCapabilities, RelayTransportError> {
        loop {
            let notified = self.state.descriptor_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.terminal.load(Ordering::SeqCst) || self.state.closed.load(Ordering::SeqCst) {
                return Err(RelayTransportError::Closed);
            }
            if let Some(capabilities) = self
                .state
                .capabilities_v2
                .lock()
                .await
                .get(identity)
                .cloned()
            {
                return Ok(capabilities);
            }
            if let Some(error) = self
                .state
                .capability_errors
                .lock()
                .await
                .get(identity)
                .cloned()
            {
                return Err(error);
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|_| RelayTransportError::Timeout {
                    operation: "handshake_v2",
                })?;
        }
    }

    pub async fn send_outbound(
        &self,
        action: Value,
        platform: Option<&str>,
    ) -> Result<Value, RelayTransportError> {
        self.request_response(action, platform).await
    }

    pub async fn send_follow_up(
        &self,
        action: Value,
        platform: Option<&str>,
    ) -> Result<Value, RelayTransportError> {
        self.request_response(action, platform).await
    }

    pub async fn get_chat_info(&self, chat_id: &str) -> Result<Value, RelayTransportError> {
        let result = self
            .request_response(json!({"op": "get_chat_info", "chat_id": chat_id}), None)
            .await?;
        let info = result.get("chat_info").unwrap_or(&result);
        Ok(json!({
            "name": info.get("name").and_then(Value::as_str).unwrap_or(chat_id),
            "type": info.get("type").and_then(Value::as_str).unwrap_or("dm"),
        }))
    }

    pub async fn send_interrupt(
        &self,
        session_key: impl Into<String>,
        reason: Option<String>,
    ) -> Result<(), RelayTransportError> {
        self.current_io()
            .await
            .send(GatewayToConnectorFrame::Interrupt {
                session_key: session_key.into(),
                reason,
            })
            .await
    }

    pub async fn go_idle(&self) -> Result<bool, RelayTransportError> {
        let (tx, rx) = oneshot::channel();
        {
            let mut waiter = self.state.going_idle.lock().await;
            *waiter = Some(tx);
        }
        if let Err(error) = self
            .current_io()
            .await
            .send(GatewayToConnectorFrame::GoingIdle)
            .await
        {
            self.state.going_idle.lock().await.take();
            return Err(error);
        }
        match timeout(Duration::from_millis(self.timeouts.idle_ms), rx).await {
            Ok(Ok(())) => Ok(true),
            Ok(Err(_)) => Err(RelayTransportError::Closed),
            Err(_) => {
                self.state.going_idle.lock().await.take();
                Ok(false)
            }
        }
    }

    pub async fn set_inbound_handler(&self, handler: Arc<dyn RelayInboundHandler>) {
        *self.state.inbound_handler.write().await = Some(handler);
    }

    pub async fn set_passthrough_handler(&self, handler: Arc<dyn RelayPassthroughHandler>) {
        *self.state.passthrough_handler.write().await = Some(handler);
    }

    pub async fn set_interrupt_inbound_handler(
        &self,
        handler: Arc<dyn RelayInterruptInboundHandler>,
    ) {
        *self.state.interrupt_handler.write().await = Some(handler);
    }

    async fn start_reader(&self) {
        let mut reader = self.reader.lock().await;
        if self.terminal.load(Ordering::SeqCst) {
            return;
        }
        if self
            .state
            .reader_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        if let Some(previous) = reader.take() {
            previous.abort();
            let _ = previous.await;
        }
        let io = self.current_io().await;
        let state = self.state.clone();
        *reader = Some(tokio::spawn(async move {
            let result = read_loop(io.clone(), state.clone()).await;
            if let Err(error) = result {
                state.fail_waiters(error).await;
            }
        }));
    }

    async fn reconnect_loop(
        &self,
        dialer: Arc<dyn RelayFrameDialer>,
        policy: RelayReconnectPolicy,
        shutdown: Arc<AtomicBool>,
    ) {
        let mut backoff_ms = policy.backoff_ms;
        loop {
            self.wait_closed().await;
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            sleep(Duration::from_millis(backoff_ms)).await;
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            match dialer.dial().await {
                Ok(io) => match self.reconnect_with_io(io).await {
                    Ok(()) => {
                        backoff_ms = policy.backoff_ms;
                    }
                    Err(_) => {
                        backoff_ms = next_backoff(backoff_ms, policy.max_backoff_ms);
                    }
                },
                Err(_) => {
                    backoff_ms = next_backoff(backoff_ms, policy.max_backoff_ms);
                }
            }
        }
    }

    async fn wait_closed(&self) {
        loop {
            let notified = self.state.closed_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.closed.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    async fn request_response(
        &self,
        action: Value,
        platform: Option<&str>,
    ) -> Result<Value, RelayTransportError> {
        let request_id = self.state.next_request_id();
        let (tx, rx) = oneshot::channel();
        self.state
            .pending
            .lock()
            .await
            .insert(request_id.clone(), tx);

        let (platform, bot_id) = platform
            .map(|platform| {
                (
                    Some(platform.to_string()),
                    self.bot_id_for(platform).map(str::to_string),
                )
            })
            .unwrap_or((None, None));

        let send_result = self
            .current_io()
            .await
            .send(GatewayToConnectorFrame::Outbound {
                request_id: request_id.clone(),
                action,
                platform,
                bot_id,
            })
            .await;
        if let Err(error) = send_result {
            self.state.pending.lock().await.remove(&request_id);
            return Err(error);
        }

        match timeout(Duration::from_millis(self.timeouts.outbound_ms), rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(RelayTransportError::Closed),
            Err(_) => {
                self.state.pending.lock().await.remove(&request_id);
                Err(RelayTransportError::Timeout {
                    operation: "outbound_result",
                })
            }
        }
    }

    fn bot_id_for(&self, platform: &str) -> Option<&str> {
        self.identities
            .iter()
            .find(|identity| identity.platform == platform)
            .map(|identity| identity.bot_id.as_str())
    }
}

impl Drop for RelayTransport {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.get_mut().take() {
            reader.abort();
        }
    }
}

/// Handle for a spawned reconnect supervisor.
pub struct RelayReconnectHandle {
    shutdown: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl RelayReconnectHandle {
    pub fn abort(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.abort();
    }
    /// Stop and join this reconnect supervisor before stopping its transport.
    pub async fn shutdown(mut self) {
        self.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for RelayReconnectHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Default)]
struct RelayTransportState {
    reader_started: AtomicBool,
    closed: AtomicBool,
    request_counter: AtomicU64,
    descriptor: Mutex<Option<CapabilityDescriptor>>,
    capabilities_v2: Mutex<HashMap<RelayIdentity, tinychannels_bus::ChannelCapabilities>>,
    capability_errors: Mutex<HashMap<RelayIdentity, RelayTransportError>>,
    configured_identities: std::collections::HashSet<RelayIdentity>,
    descriptor_ready: Notify,
    closed_ready: Notify,
    pending: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    going_idle: Mutex<Option<oneshot::Sender<()>>>,
    inbound_handler: RwLock<Option<Arc<dyn RelayInboundHandler>>>,
    passthrough_handler: RwLock<Option<Arc<dyn RelayPassthroughHandler>>>,
    interrupt_handler: RwLock<Option<Arc<dyn RelayInterruptInboundHandler>>>,
}

impl RelayTransportState {
    fn next_request_id(&self) -> String {
        let id = self.request_counter.fetch_add(1, Ordering::SeqCst) + 1;
        format!("req-{id}")
    }

    async fn fail_waiters(&self, _error: RelayTransportError) {
        self.closed.store(true, Ordering::SeqCst);
        self.reader_started.store(false, Ordering::SeqCst);
        self.pending.lock().await.clear();
        self.going_idle.lock().await.take();
        self.descriptor_ready.notify_waiters();
        self.closed_ready.notify_waiters();
    }
}

fn next_backoff(current_ms: u64, max_ms: u64) -> u64 {
    current_ms.saturating_mul(2).min(max_ms)
}

async fn read_loop(
    io: Arc<dyn RelayFrameIo>,
    state: Arc<RelayTransportState>,
) -> Result<(), RelayTransportError> {
    loop {
        let Some(frame) = io.recv().await? else {
            return Err(RelayTransportError::Closed);
        };
        handle_frame(&io, &state, frame).await?;
    }
}

async fn handle_frame(
    io: &Arc<dyn RelayFrameIo>,
    state: &Arc<RelayTransportState>,
    frame: ConnectorToGatewayFrame,
) -> Result<(), RelayTransportError> {
    match frame {
        ConnectorToGatewayFrame::Descriptor { descriptor } => {
            *state.descriptor.lock().await = Some(descriptor);
            state.descriptor_ready.notify_waiters();
        }
        ConnectorToGatewayFrame::DescriptorV2 {
            platform,
            bot_id,
            capabilities,
        } => {
            let identity = RelayIdentity { platform, bot_id };
            if !state.configured_identities.contains(&identity) {
                return Ok(());
            }
            if capabilities.version != tinychannels_bus::ChannelCapabilities::VERSION {
                state.capability_errors.lock().await.insert(
                    identity,
                    RelayTransportError::UnsupportedCapabilitiesVersion {
                        actual: capabilities.version,
                        expected: tinychannels_bus::ChannelCapabilities::VERSION,
                    },
                );
                state.descriptor_ready.notify_waiters();
                return Ok(());
            }
            state
                .capabilities_v2
                .lock()
                .await
                .insert(identity, *capabilities);
            state.descriptor_ready.notify_waiters();
        }
        ConnectorToGatewayFrame::Inbound { event, buffer_id } => {
            let frame = ConnectorToGatewayFrame::Inbound { event, buffer_id };
            let Some(inbound) = frame.authenticated_inbound_event() else {
                return Ok(());
            };
            let ack = frame.inbound_ack();
            let handler = { state.inbound_handler.read().await.clone() };
            if let Some(handler) = handler {
                handler.handle(inbound).await?;
                if let Some(ack) = ack {
                    io.send(ack).await?;
                }
            }
        }
        ConnectorToGatewayFrame::OutboundResult { request_id, result } => {
            if let Some(sender) = state.pending.lock().await.remove(&request_id) {
                let _ = sender.send(result);
            }
        }
        ConnectorToGatewayFrame::InterruptInbound {
            session_key,
            chat_id,
        } => {
            let handler = { state.interrupt_handler.read().await.clone() };
            if let Some(handler) = handler {
                handler.handle(session_key, chat_id).await?;
            }
        }
        ConnectorToGatewayFrame::GoingIdleAck => {
            if let Some(sender) = state.going_idle.lock().await.take() {
                let _ = sender.send(());
            }
        }
        ConnectorToGatewayFrame::PassthroughForward { forward, buffer_id } => {
            let ack = buffer_id
                .as_ref()
                .map(|buffer_id| GatewayToConnectorFrame::InboundAck {
                    buffer_id: buffer_id.clone(),
                });
            let handler = { state.passthrough_handler.read().await.clone() };
            if let Some(handler) = handler {
                handler.handle(forward, buffer_id).await?;
                if let Some(ack) = ack {
                    io.send(ack).await?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
