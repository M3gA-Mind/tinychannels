//! Module-owned relay resources: reserve before startup, stop by known handle.
use crate::host::HostChannels;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tinybus::{Error, Result};
use tinychannels::config::RelayRuntimeConfigExt as _;
use tinychannels::relay::{
    CapabilityDescriptor, RelayFrameDialer, RelayIdentity, RelayInboundHandler, RelayTransport,
    RelayTransportError, WebSocketRelayConfig, WebSocketRelayDialer,
};
use tinychannels_bus::config::RelayRuntimeConfig;
use tinychannels_bus::controllers::ChannelSendMessageResult;
use tinychannels_bus::module_relay::*;
use tinychannels_bus::{ChannelInboundEnvelope, ChannelOutboundIntent};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

const STOP_WAIT: Duration = Duration::from_secs(10);
#[derive(Clone)]
enum State {
    Reserved,
    Starting,
    Connected(CapabilityDescriptor),
    ConnectedV2(RelayCapabilitiesV2),
    Reconnecting,
    Stopping,
    Stopped,
    Failed,
}
impl State {
    fn phase(&self) -> RelayPhase {
        match self {
            Self::Reserved => RelayPhase::Reserved,
            Self::Starting => RelayPhase::Starting,
            Self::Connected(_) | Self::ConnectedV2(_) => RelayPhase::Connected,
            Self::Reconnecting => RelayPhase::Reconnecting,
            Self::Stopping => RelayPhase::Stopping,
            Self::Stopped => RelayPhase::Stopped,
            Self::Failed => RelayPhase::Failed,
        }
    }
}
struct Command {
    intent: ChannelOutboundIntent,
    reply: oneshot::Sender<Result<ChannelSendMessageResult>>,
}
struct Slot {
    id: RelayReservation,
    expires: Instant,
    config_hash: StdMutex<Option<String>>,
    state: watch::Sender<State>,
    cancel: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    commands: mpsc::Sender<Command>,
    inbox: Mutex<Option<mpsc::Receiver<Command>>>,
    transport: Mutex<Option<Arc<RelayTransport>>>,
}
/// One bounded active/pending relay, with no lifetime tombstone history.
pub(crate) struct Relays {
    slot: Mutex<Option<Arc<Slot>>>,
    terminal: AtomicBool,
    host: HostChannels,
}
impl Relays {
    pub(crate) fn new(host: HostChannels) -> Self {
        Self {
            slot: Mutex::new(None),
            terminal: AtomicBool::new(false),
            host,
        }
    }
    pub(crate) fn freeze(&self) {
        self.terminal.store(true, Ordering::SeqCst);
    }
    pub(crate) async fn is_empty(&self) -> bool {
        self.slot.lock().await.is_none()
    }
    pub(crate) async fn reserve(&self) -> Result<RelayReservation> {
        let mut current = self.slot.lock().await;
        if self.terminal.load(Ordering::SeqCst) {
            return Err(closed());
        }
        let previous = current.as_ref().cloned();
        // Hold this guard until the slot is replaced. Otherwise StartRelay can
        // mark an expired reservation started after this idle check and then
        // have ReserveRelay evict its live owner based on a stale observation.
        let previous_config = previous
            .as_ref()
            .map(|slot| slot.config_hash.lock().unwrap_or_else(|e| e.into_inner()));
        if let Some(slot) = previous.as_ref()
            && (previous_config
                .as_ref()
                .is_some_and(|config| config.is_some())
                || slot.expires > Instant::now())
        {
            return Err(Error::failed("relay capacity is occupied"));
        }
        let (state, _) = watch::channel(State::Reserved);
        let (cancel, _) = watch::channel(false);
        let (commands, inbox) = mpsc::channel(RELAY_OUTBOUND_QUEUE);
        let slot = Arc::new(Slot {
            id: RelayReservation(uuid::Uuid::new_v4().to_string()),
            expires: Instant::now() + Duration::from_secs(RELAY_RESERVATION_SECONDS),
            config_hash: StdMutex::new(None),
            state,
            cancel,
            worker: Mutex::new(None),
            commands,
            inbox: Mutex::new(Some(inbox)),
            transport: Mutex::new(None),
        });
        let id = slot.id.clone();
        *current = Some(slot);
        drop(previous_config);
        Ok(id)
    }
    async fn get(&self, id: &RelayReservation) -> Result<Arc<Slot>> {
        self.slot
            .lock()
            .await
            .as_ref()
            .filter(|slot| slot.id == *id)
            .cloned()
            .ok_or_else(|| Error::failed("unknown or closed relay reservation"))
    }
    pub(crate) async fn start(
        &self,
        id: RelayReservation,
        config: RelayRuntimeConfig,
    ) -> Result<CapabilityDescriptor> {
        match self.start_mode(id, config, false).await? {
            State::Connected(descriptor) => Ok(descriptor),
            _ => Err(Error::failed(
                "relay v1 handshake returned the wrong result",
            )),
        }
    }

    pub(crate) async fn start_v2(
        &self,
        id: RelayReservation,
        config: RelayRuntimeConfig,
    ) -> Result<RelayCapabilitiesV2> {
        match self.start_mode(id, config, true).await? {
            State::ConnectedV2(capabilities) => Ok(capabilities),
            _ => Err(Error::failed(
                "relay v2 handshake returned the wrong result",
            )),
        }
    }

    async fn start_mode(
        &self,
        id: RelayReservation,
        config: RelayRuntimeConfig,
        v2: bool,
    ) -> Result<State> {
        validate_config(&config)?;
        if self.terminal.load(Ordering::SeqCst) {
            return Err(closed());
        }
        let slot = self.get(&id).await?;
        let bytes = serde_json::to_string(&config)
            .map_err(|_| Error::failed("invalid relay configuration"))?;
        let hash =
            tinychannels::security::hash_token(&format!("{bytes}:v{}", if v2 { 2 } else { 1 }));
        let mut worker = slot.worker.lock().await;
        if self.terminal.load(Ordering::SeqCst) || *slot.cancel.borrow() {
            return Err(closed());
        }
        {
            let mut previous = slot.config_hash.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(previous) = previous.as_ref() {
                if previous != &hash {
                    return Err(Error::failed("relay reservation configuration conflict"));
                }
            } else {
                if slot.expires <= Instant::now() {
                    return Err(Error::failed("relay reservation expired"));
                }
                *previous = Some(hash);
                slot.state.send_replace(State::Starting);
                let owner = slot.clone();
                let host = self.host.clone();
                *worker = Some(tokio::spawn(async move {
                    run(owner, config, host, v2).await;
                }));
            }
        }
        drop(worker);
        let mut state = slot.state.subscribe();
        loop {
            match state.borrow().clone() {
                State::Connected(descriptor) if !v2 => return Ok(State::Connected(descriptor)),
                State::ConnectedV2(capabilities) if v2 => {
                    return Ok(State::ConnectedV2(capabilities));
                }
                State::Connected(_) | State::ConnectedV2(_) => {
                    return Err(Error::failed("relay handshake mode mismatch"));
                }
                State::Stopped | State::Stopping => return Err(closed()),
                State::Failed => return Err(Error::failed("relay startup failed")),
                State::Reserved | State::Starting | State::Reconnecting => {}
            }
            state.changed().await.map_err(|_| closed())?;
        }
    }
    pub(crate) async fn status(&self, id: RelayReservation) -> Result<RelayStatus> {
        let slot = self.get(&id).await?;
        let state = slot.state.borrow().clone();
        let state = match &state {
            State::ConnectedV2(capabilities) => {
                let expected = relay_v2_identities(capabilities);
                if self.live_v2_descriptors(&slot, &expected).await.is_some() {
                    state
                } else {
                    State::Reconnecting
                }
            }
            State::Connected(_) => {
                if slot
                    .transport
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|transport| transport.is_closed())
                {
                    State::Reconnecting
                } else {
                    state
                }
            }
            _ => state,
        };
        Ok(RelayStatus {
            contract_version: MODULE_OPERATION_VERSION,
            state: state.phase(),
        })
    }
    pub(crate) async fn capabilities_v2(
        &self,
        id: RelayReservation,
    ) -> Result<Option<RelayCapabilitiesV2>> {
        let slot = self.get(&id).await?;
        let expected = match slot.state.borrow().clone() {
            State::ConnectedV2(capabilities) => relay_v2_identities(&capabilities),
            _ => return Ok(None),
        };
        let Some(descriptors) = self.live_v2_descriptors(&slot, &expected).await else {
            return Ok(None);
        };
        let instances = descriptors
            .into_iter()
            .map(|(identity, capabilities)| RelayInstanceCapabilitiesV2 {
                platform: identity.platform,
                bot_id: identity.bot_id,
                capabilities,
            })
            .collect();
        Ok(Some(RelayCapabilitiesV2 {
            contract_version: tinychannels_bus::ChannelCapabilities::VERSION,
            instances,
        }))
    }

    async fn live_v2_descriptors(
        &self,
        slot: &Slot,
        expected: &[RelayIdentity],
    ) -> Option<std::collections::HashMap<RelayIdentity, tinychannels_bus::ChannelCapabilities>>
    {
        if self.terminal.load(Ordering::SeqCst) || *slot.cancel.borrow() {
            return None;
        }
        let transport = slot.transport.lock().await.as_ref().cloned()?;
        if transport.is_closed() {
            return None;
        }
        let descriptors = transport.capabilities_v2().await;
        if transport.is_closed() || !v2_capabilities_complete(expected, &descriptors) {
            return None;
        }
        Some(descriptors)
    }
    pub(crate) async fn send(
        &self,
        id: RelayReservation,
        intent: ChannelOutboundIntent,
    ) -> Result<ChannelSendMessageResult> {
        if intent.durability == tinychannels::DeliveryDurability::Required {
            return Err(Error::failed(
                "required durability needs the host delivery queue",
            ));
        }
        let slot = self.get(&id).await?;
        if self.terminal.load(Ordering::SeqCst) || *slot.cancel.borrow() {
            return Err(closed());
        }
        if !matches!(
            *slot.state.borrow(),
            State::Connected(_) | State::ConnectedV2(_)
        ) {
            return Err(Error::failed("relay is not connected"));
        }
        let bytes =
            serde_json::to_vec(&intent).map_err(|_| Error::failed("invalid relay intent"))?;
        if bytes.len() > RELAY_FRAME_BYTES {
            return Err(Error::failed("relay intent byte limit exceeded"));
        }
        let (reply, wait) = oneshot::channel();
        slot.commands
            .try_send(Command { intent, reply })
            .map_err(|_| Error::failed("relay outbound queue is full or closed"))?;
        wait.await.map_err(|_| closed())?
    }
    pub(crate) async fn stop(&self, id: RelayReservation) -> Result<()> {
        let slot = match self.get(&id).await {
            Ok(slot) => slot,
            Err(_) => return Ok(()),
        };
        slot.cancel.send_replace(true);
        slot.state.send_replace(State::Stopping);
        let mut worker = slot.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            match tokio::time::timeout(STOP_WAIT, task).await {
                Err(_) => {
                    return Err(Error::failed(
                        "relay cleanup is still pending; retry StopRelay",
                    ));
                }
                Ok(Err(_)) => {
                    tracing::warn!("relay worker ended unexpectedly during cleanup");
                }
                Ok(Ok(())) => {}
            }
        }
        worker.take();
        drop(worker);
        let delivery = tokio::time::timeout(STOP_WAIT, self.host.flush())
            .await
            .map_err(|_| Error::failed("relay callbacks are pending; retry StopRelay"))?;
        let mut current = self.slot.lock().await;
        if current.as_ref().is_some_and(|current| current.id == id) {
            current.take();
        }
        drop(current);
        delivery
    }
    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.terminal.store(true, Ordering::SeqCst);
        let id = self.slot.lock().await.as_ref().map(|slot| slot.id.clone());
        if let Some(id) = id {
            self.stop(id).await?;
        }
        Ok(())
    }
}
fn closed() -> Error {
    Error::failed("relay is stopped or module is shutting down")
}
fn validate_config(config: &RelayRuntimeConfig) -> Result<()> {
    let unique_identities: std::collections::BTreeSet<_> = config
        .identities
        .iter()
        .map(|identity| (identity.platform.as_str(), identity.bot_id.as_str()))
        .collect();
    if !config.is_listener_configured()
        || config.identities.len() > 32
        || unique_identities.len() != config.identities.len()
        || config.url.len() > 4096
        || config
            .upgrade_secret
            .as_ref()
            .is_some_and(|secret| secret.len() > 4096)
        || config.gateway_id.as_ref().is_some_and(|id| id.len() > 1024)
        || config
            .identities
            .iter()
            .any(|id| id.platform.len() > 128 || id.bot_id.len() > 1024)
        || config.timeouts.handshake_ms == 0
        || config.timeouts.handshake_ms > 30_000
        || config.timeouts.outbound_ms == 0
        || config.timeouts.outbound_ms > 30_000
        || config.reconnect.backoff_ms == 0
        || config.reconnect.max_backoff_ms > 30_000
    {
        return Err(Error::failed("invalid or oversized relay configuration"));
    }
    Ok(())
}
struct Inbound {
    host: HostChannels,
}
#[async_trait::async_trait]
impl RelayInboundHandler for Inbound {
    async fn handle(
        &self,
        event: tinychannels::relay::AuthenticatedRelayInboundEvent,
    ) -> std::result::Result<(), RelayTransportError> {
        let envelope: ChannelInboundEnvelope = serde_json::from_value(event.event)
            .map_err(|_| RelayTransportError::Handler("invalid inbound envelope".into()))?;
        let channel = envelope.channel.id.clone();
        let delivery = RelayInboundDelivery {
            envelope,
            delivered_via_authenticated_relay: event.delivered_via_authenticated_relay,
        };
        self.host.deliver_relay_inbound(&channel, delivery).await;
        Ok(())
    }
}
async fn canceled(cancel: &mut watch::Receiver<bool>) {
    if *cancel.borrow() {
        return;
    }
    let _ = cancel.wait_for(|value| *value).await;
}
async fn run(slot: Arc<Slot>, config: RelayRuntimeConfig, host: HostChannels, v2: bool) {
    let mut cancel = slot.cancel.subscribe();
    let dialer = Arc::new(WebSocketRelayDialer::new_bounded(
        WebSocketRelayConfig::from(&config),
        RELAY_FRAME_BYTES,
    ));
    let mut transport = None;
    let expected_identities = config.relay_identities();
    let start = async {
        let io = dialer.dial().await?;
        let native = Arc::new(RelayTransport::new(
            config.relay_identities(),
            io,
            config.timeouts,
        ));
        *slot.transport.lock().await = Some(native.clone());
        transport = Some(native.clone());
        native
            .set_inbound_handler(Arc::new(Inbound { host: host.clone() }))
            .await;
        if v2 {
            native.connect_v2().await?;
            let instances = native
                .negotiate_v2()
                .await?
                .into_iter()
                .map(|(identity, capabilities)| RelayInstanceCapabilitiesV2 {
                    platform: identity.platform,
                    bot_id: identity.bot_id,
                    capabilities,
                })
                .collect();
            Ok::<_, RelayTransportError>(State::ConnectedV2(RelayCapabilitiesV2 {
                contract_version: tinychannels_bus::ChannelCapabilities::VERSION,
                instances,
            }))
        } else {
            native.connect().await?;
            native.handshake().await.map(State::Connected)
        }
    };
    let outcome = tokio::select! { biased;
        ()=canceled(&mut cancel)=>Err(RelayTransportError::Closed),
        result=tokio::time::timeout(Duration::from_secs(30),start)=>result.unwrap_or(Err(RelayTransportError::Timeout {operation:"startup"})),
    };
    let mut reconnect = None;
    let mut observer = None;
    if let Ok(connected) = outcome
        && let Some(native) = transport.as_ref()
    {
        slot.state.send_replace(connected);
        host.report_status("relay", "connected", None).await;
        reconnect = Some(native.spawn_reconnect_supervisor(dialer, config.reconnect));
        let observed_slot = slot.clone();
        let observed_native = native.clone();
        let observed_host = host.clone();
        let mut observed_cancel = slot.cancel.subscribe();
        observer = Some(tokio::spawn(async move {
            let mut previous = RelayPhase::Connected;
            loop {
                tokio::select! {biased;
                    ()=canceled(&mut observed_cancel)=>break,
                    _=tokio::time::sleep(Duration::from_millis(100))=>{},
                }
                let (next, state) = if observed_native.is_closed() {
                    (RelayPhase::Reconnecting, State::Reconnecting)
                } else if v2 {
                    let descriptors = observed_native.capabilities_v2().await;
                    if !observed_native.is_closed()
                        && v2_capabilities_complete(&expected_identities, &descriptors)
                    {
                        let instances = descriptors
                            .into_iter()
                            .map(|(identity, capabilities)| RelayInstanceCapabilitiesV2 {
                                platform: identity.platform,
                                bot_id: identity.bot_id,
                                capabilities,
                            })
                            .collect();
                        (
                            RelayPhase::Connected,
                            State::ConnectedV2(RelayCapabilitiesV2 {
                                contract_version: tinychannels_bus::ChannelCapabilities::VERSION,
                                instances,
                            }),
                        )
                    } else {
                        (RelayPhase::Reconnecting, State::Reconnecting)
                    }
                } else if !v2 && let Some(descriptor) = observed_native.descriptor().await {
                    (RelayPhase::Connected, State::Connected(descriptor))
                } else {
                    (RelayPhase::Reconnecting, State::Reconnecting)
                };
                observed_slot.state.send_replace(state);
                if next != previous {
                    previous = next;
                    let label = match next {
                        RelayPhase::Connected => "connected",
                        RelayPhase::Reconnecting => "reconnecting",
                        _ => continue,
                    };
                    observed_host.report_status("relay", label, None).await;
                }
            }
        }));
        if let Some(mut inbox) = slot.inbox.lock().await.take() {
            loop {
                let command = tokio::select! { biased;
                    ()=canceled(&mut cancel)=>break,
                    command=inbox.recv()=>match command {Some(command)=>command,None=>break},
                };
                let action =
                    tinychannels::relay::relay_send_action_from_outbound_intent(&command.intent);
                let result = tokio::select! { biased;
                    ()=canceled(&mut cancel)=>{let _=command.reply.send(Err(closed()));break},
                    result=native.send_outbound(action,Some(&command.intent.channel_id))=>result,
                };
                let result = result
                    .map_err(|_| Error::failed("relay delivery failed"))
                    .and_then(|raw| {
                        if raw.get("success").and_then(serde_json::Value::as_bool) == Some(false) {
                            return Err(Error::failed("relay delivery refused"));
                        }
                        Ok(ChannelSendMessageResult {
                            message_id: raw
                                .get("message_id")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned),
                            raw: Some(raw),
                            ..Default::default()
                        })
                    });
                let _ = command.reply.send(result);
            }
        }
    }
    if let Some(supervisor) = reconnect {
        supervisor.shutdown().await;
    }
    if let Some(observer) = observer {
        let _ = observer.await;
    }
    if let Some(native) = transport {
        native.shutdown().await;
        *slot.transport.lock().await = None;
    }
    let phase = if *slot.cancel.borrow() {
        State::Stopped
    } else {
        State::Failed
    };
    host.report_status(
        "relay",
        if matches!(phase, State::Stopped) {
            "stopped"
        } else {
            "failed"
        },
        None,
    )
    .await;
    slot.state.send_replace(phase);
}

fn relay_v2_identities(capabilities: &RelayCapabilitiesV2) -> Vec<RelayIdentity> {
    capabilities
        .instances
        .iter()
        .map(|instance| RelayIdentity {
            platform: instance.platform.clone(),
            bot_id: instance.bot_id.clone(),
        })
        .collect()
}

fn v2_capabilities_complete(
    expected: &[RelayIdentity],
    actual: &std::collections::HashMap<RelayIdentity, tinychannels_bus::ChannelCapabilities>,
) -> bool {
    let expected: std::collections::BTreeSet<_> = expected
        .iter()
        .map(|identity| (identity.platform.as_str(), identity.bot_id.as_str()))
        .collect();
    let actual: std::collections::BTreeSet<_> = actual
        .keys()
        .map(|identity| (identity.platform.as_str(), identity.bot_id.as_str()))
        .collect();
    !expected.is_empty() && expected == actual
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
