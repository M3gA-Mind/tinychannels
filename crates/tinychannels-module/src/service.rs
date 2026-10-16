//! The TinyBus service boundary for the channel surface.
//!
//! One object, `/ai/tinyhumans/tinychannels/Channels`, preserving its original
//! five methods and adding typed intent, relay lifecycle, and shutdown members:
//!
//! ```text
//! StartChannel(name, ChannelsConfig) -> ()
//! StopChannel(name)                  -> ()
//! SendMessage(name, SendMessage)     -> ()
//! ListChannels()                     -> Vec<String>
//! ChannelStatus(name)                -> ChannelState
//! StartChannelConfigured(name, config, proxy) -> ()
//! StartChannelSupervised(name, config, proxy, policy) -> ()
//! ChannelController(request) -> ChannelControllerResult
//! SendChannelIntent(name, intent)     -> ChannelSendMessageResult
//! ```
//!
//! # This module is stateful, unlike the document one
//!
//! `tinydocs` holds nothing between calls: a document is generated and handed
//! back. A channel provider is the opposite — it owns a live connection and a
//! listen loop that outlives every call. So this object keeps a registry of
//! running providers, and `StopChannel` is not optional politeness: without it
//! a provider's task and socket leak until the process exits.
//!
//! # Config arrives per call, not at load
//!
//! `StartChannel` takes the whole [`ChannelsConfig`] rather than reading one
//! handed over at load time, because a desktop host changes it at runtime — a
//! user pastes a bot token and expects the channel to come up. Reading a
//! load-time config would mean a module restart per edit.
//!
//! Credentials in it are expected to be **already hydrated** by the host; see
//! `tinychannels::factory`. This module has no keyring and cannot resolve a
//! placeholder.

use std::collections::HashMap;
use std::sync::Arc;
use tinychannels::config::WhatsAppConfigExt as _;

use crate::delivery::HostDeliveryStore;
use crate::provider::{Hooks, Running, RunningSendError};
use crate::relay::Relays;
use std::sync::atomic::{AtomicBool, Ordering};
use tinybus::{Connection, Error as BusError, Result as BusResult};
use tinychannels::delivery::{
    DeliveryAttemptFailure, DeliveryAttemptResult, DeliveryAttemptSuccess, DeliveryQueueError,
    DeliveryQueueHandler, DeliveryQueueStore, DeliveryResultStore, PendingDeliveryDrainDecision,
    QueuedDelivery, UnknownSendReconciliation, ack_delivery, drain_selected_pending_deliveries,
    enqueue_delivery, recover_pending_deliveries,
};
use tinychannels::factory::{
    ConfiguredHttpClients, DefaultHttpClients, HttpClientFactory, build_channels,
};
use tinychannels::host::ChannelHost;
use tinychannels_bus::ChannelControllerRequest;
use tinychannels_bus::ChannelControllerResult;
use tinychannels_bus::ChannelOutboundIntent;
use tinychannels_bus::HttpProxyPolicy;
use tinychannels_bus::config::RelayRuntimeConfig;
use tinychannels_bus::controllers::ChannelSendMessageResult;
use tinychannels_bus::module_relay::{RelayReservation, RelayStatus};
use tinychannels_bus::{
    BUS_NAME, ChannelsConfig, EmailSendRequest, EmailUploadBegin, EmailUploadChunk,
    EmailUploadHandle, OBJECT_PATH, SendMessage,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::email::EmailSender;
use crate::host::HostChannels;

const UNKNOWN_CHANNEL_ERROR: &str = "ai.tinyhumans.tinychannels.Error.UnknownChannel";
const ALREADY_RUNNING_ERROR: &str = "ai.tinyhumans.tinychannels.Error.AlreadyRunning";
const NOT_CONFIGURED_ERROR: &str = "ai.tinyhumans.tinychannels.Error.NotConfigured";
const SEND_FAILED_ERROR: &str = "ai.tinyhumans.tinychannels.Error.SendFailed";
const NO_INGRESS_ERROR: &str = "ai.tinyhumans.tinychannels.Error.NoWebhookIngress";

/// Whether `name`, as configured, receives inbound traffic through a webhook
/// this module cannot serve. See the refusal in `start_channel` for why.
fn is_webhook_backed(name: &str, config: &ChannelsConfig) -> bool {
    name == "linq"
        || (name == "whatsapp"
            && config
                .whatsapp
                .as_ref()
                .is_some_and(|wa| wa.backend_type() == "cloud"))
}

/// The served object.
pub(crate) struct Channels {
    pub(crate) host: HostChannels,
    email: EmailSender,
    progressive: Arc<crate::progressive::Replies>,
    /// Shared with each listener task so it can retire its own entry.
    pub(crate) running: Arc<Mutex<HashMap<String, Arc<Running>>>>,
    relays: Arc<Relays>,
    pub(crate) terminal: AtomicBool,
    runtimes: Option<Arc<crate::runtime::Runtimes>>,
    shutdown_task: Mutex<Option<JoinHandle<BusResult<()>>>>,
    delivery_admission: Arc<Mutex<()>>,
}

struct ModuleDeliveryHandler {
    running: Arc<Mutex<HashMap<String, Arc<Running>>>>,
    store: HostDeliveryStore,
}

impl Channels {
    pub(crate) fn new(host: HostChannels, runtimes: Option<Arc<crate::runtime::Runtimes>>) -> Self {
        Self {
            progressive: Arc::new(crate::progressive::Replies::new(host.clone())),
            relays: Arc::new(Relays::new(host.clone())),
            host,
            email: EmailSender::new(),
            running: Arc::new(Mutex::new(HashMap::new())),
            terminal: AtomicBool::new(false),
            runtimes,
            shutdown_task: Mutex::new(None),
            delivery_admission: Arc::new(Mutex::new(())),
        }
    }

    async fn start_channel_inner(
        &self,
        name: String,
        config: ChannelsConfig,
        http: &dyn HttpClientFactory,
        supervision: Option<tinychannels_bus::ChannelSupervisionPolicy>,
        outbound_only: bool,
    ) -> BusResult<()> {
        let mut running = self.running.lock().await;
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        if running
            .get(&name)
            .is_some_and(|running| running.is_finished())
        {
            running.remove(&name);
        }
        if running.contains_key(&name) {
            return Err(BusError::MethodFailed {
                name: ALREADY_RUNNING_ERROR.to_owned(),
                message: format!("channel {name} is already running"),
            });
        }

        let hooks = Arc::new(Hooks::default());
        let owner_host: Arc<dyn ChannelHost> = hooks.host(self.host.clone());
        let built = build_channels(&config, &owner_host, http);
        let channel = built
            .into_iter()
            .find(|candidate| candidate.name() == name)
            .ok_or_else(|| BusError::MethodFailed {
                name: NOT_CONFIGURED_ERROR.to_owned(),
                message: format!("channel {name} is not present in the supplied config"),
            })?;

        if is_webhook_backed(&name, &config) && !outbound_only {
            return Err(BusError::MethodFailed {
                name: NO_INGRESS_ERROR.to_owned(),
                message: format!(
                    "channel {name} receives inbound traffic through an external webhook, \
                     and this module has no ingress for it; run this provider in-process \
                     or serve its webhook host-side"
                ),
            });
        }

        if outbound_only && !is_webhook_backed(&name, &config) {
            return Err(BusError::failed(
                "outbound-only startup requires an external-webhook provider",
            ));
        }
        let provider = match supervision {
            Some(policy) => Running::start_supervised(channel, hooks, self.host.clone(), policy),
            None if outbound_only => Running::start_outbound(channel, hooks, self.host.clone()),
            None => Running::start(channel, hooks, self.host.clone()),
        };
        running.insert(name.clone(), provider);
        drop(running);
        finish_start_admission(
            &self.host,
            &self.running,
            &self.delivery_admission,
            &self.terminal,
            &name,
        )
        .await
    }
}

#[async_trait::async_trait]
impl DeliveryQueueHandler for ModuleDeliveryHandler {
    async fn deliver(&self, entry: &QueuedDelivery) -> DeliveryAttemptResult {
        let Ok(intent_fingerprint) = intent_fingerprint(&entry.intent) else {
            return Err(DeliveryAttemptFailure {
                error: "delivery payload could not be encoded".into(),
                sent_before_error: false,
                permanent: Some(true),
            });
        };
        let running = self.running.lock().await;
        let Some(provider) = running
            .get(&entry.intent.channel_id)
            .filter(|provider| !provider.is_finished())
            .cloned()
        else {
            return Err(DeliveryAttemptFailure {
                error: "delivery target is unavailable".into(),
                sent_before_error: false,
                permanent: None,
            });
        };
        drop(running);

        let message = tinychannels_bus::ChannelOutboundMessage::from_intent(&entry.intent);
        if let Err(error) = provider.send_typed(message).await {
            return Err(DeliveryAttemptFailure {
                error: "provider send failed".into(),
                sent_before_error: matches!(error, RunningSendError::AfterDispatch(_)),
                permanent: None,
            });
        }
        let result = tinychannels_bus::DeliveryTerminalResult {
            id: entry.id.clone(),
            intent_fingerprint,
            completed_at_unix_ms: now_unix_ms(),
        };
        if self.store.save_result(result).await.is_err() {
            return Err(DeliveryAttemptFailure {
                error: "terminal delivery result could not be persisted".into(),
                sent_before_error: true,
                permanent: None,
            });
        }
        Ok(DeliveryAttemptSuccess::default())
    }

    async fn reconcile_unknown_send(
        &self,
        entry: &QueuedDelivery,
    ) -> Result<Option<UnknownSendReconciliation>, DeliveryQueueError> {
        let Some(result) = self.store.load_result(&entry.id).await? else {
            return Ok(None);
        };
        let fingerprint = intent_fingerprint(&entry.intent).map_err(|_| {
            DeliveryQueueError::Store("delivery intent could not be encoded".into())
        })?;
        if result.intent_fingerprint != fingerprint {
            return Err(DeliveryQueueError::Store(
                "terminal delivery result conflicts with pending intent".into(),
            ));
        }
        Ok(Some(UnknownSendReconciliation::Sent {
            receipt: tinychannels::channel::MessageReceipt::default(),
            message_id: None,
        }))
    }
}

#[allow(
    clippy::unused_async,
    reason = "tinybus::interface requires every method to be `async fn`"
)]
#[tinybus::interface(name = "ai.tinyhumans.tinychannels.Channels")]
impl Channels {
    /// Reserve an inert identity-scoped runtime and immutable callback paths.
    #[tinybus(confidential)]
    async fn reserve_runtime(&self) -> BusResult<tinychannels_bus::ChannelRuntimeReservation> {
        self.runtimes
            .as_ref()
            .ok_or_else(|| BusError::failed("nested channel runtimes are unavailable"))?
            .reserve()
            .await
    }

    /// Activate a reserved runtime after its callback object is registered.
    /// The returned object's provider methods retain their existing arities.
    #[tinybus(confidential)]
    async fn start_runtime(&self, handle: String) -> BusResult<()> {
        self.runtimes
            .as_ref()
            .ok_or_else(|| BusError::failed("nested channel runtimes are unavailable"))?
            .start(handle)
            .await
    }

    /// Activate an immutable identity runtime with module-owned turn admission.
    #[tinybus(confidential)]
    async fn start_runtime_with_dispatch(
        &self,
        handle: String,
        policy: tinychannels_bus::ChannelRuntimeDispatchPolicy,
    ) -> BusResult<()> {
        self.runtimes
            .as_ref()
            .ok_or_else(|| BusError::failed("nested channel runtimes are unavailable"))?
            .start_with_dispatch(handle, policy)
            .await
    }

    /// Stop this identity's providers, relay and callbacks, joining pending startup.
    #[tinybus(confidential)]
    async fn stop_runtime(&self, handle: String) -> BusResult<()> {
        self.runtimes
            .as_ref()
            .ok_or_else(|| BusError::failed("nested channel runtimes are unavailable"))?
            .stop(handle)
            .await
    }

    /// Connect `name` from `config` and start receiving.
    async fn start_channel(&self, name: String, config: ChannelsConfig) -> BusResult<()> {
        self.start_channel_inner(name, config, &DefaultHttpClients, None, false)
            .await
    }

    /// Connect one provider with host-selected HTTP proxy configuration.
    ///
    /// This additive operation is confidential because proxy URLs may include
    /// credentials. `StartChannel` retains its original two-argument wire.
    #[tinybus(confidential)]
    async fn start_channel_configured(
        &self,
        name: String,
        config: ChannelsConfig,
        proxy: HttpProxyPolicy,
    ) -> BusResult<()> {
        let http = ConfiguredHttpClients::new(proxy)
            .map_err(|_| BusError::failed("invalid host HTTP proxy policy"))?;
        self.start_channel_inner(name, config, &http, None, false)
            .await
    }

    /// Start a provider with confidential configuration and module-owned reconnects.
    /// Legacy start operations keep their original arguments and single-listener behavior.
    #[tinybus(confidential)]
    async fn start_channel_supervised(
        &self,
        name: String,
        config: ChannelsConfig,
        proxy: HttpProxyPolicy,
        policy: tinychannels_bus::ChannelSupervisionPolicy,
    ) -> BusResult<()> {
        if policy.initial_backoff_secs > 86_400 || policy.max_backoff_secs > 86_400 {
            return Err(BusError::failed("provider retry delay exceeds one day"));
        }
        let http = ConfiguredHttpClients::new(proxy)
            .map_err(|_| BusError::failed("invalid host HTTP proxy policy"))?;
        self.start_channel_inner(name, config, &http, Some(policy), false)
            .await
    }

    /// Resolve instance-specific facts without starting native listeners or webhook ingress.
    #[tinybus(confidential)]
    async fn describe_configured_channels(
        &self,
        config: ChannelsConfig,
        proxy: HttpProxyPolicy,
    ) -> BusResult<Vec<tinychannels_bus::module_runtime::ConfiguredChannelInfo>> {
        use tinychannels_bus::module_runtime::{ChannelIngressKind, ConfiguredChannelInfo};
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        let http = ConfiguredHttpClients::new(proxy)
            .map_err(|_| BusError::failed("invalid host HTTP proxy policy"))?;
        let host: Arc<dyn ChannelHost> = Arc::new(tinychannels::NoopHost);
        Ok(build_channels(&config, &host, &http)
            .into_iter()
            .map(|channel| ConfiguredChannelInfo {
                name: channel.name().to_owned(),
                ingress: if is_webhook_backed(channel.name(), &config) {
                    ChannelIngressKind::ExternalWebhook
                } else {
                    ChannelIngressKind::Listener
                },
                interaction: tinychannels_bus::ChannelInteractionInfo {
                    capabilities: {
                        let mut capabilities = channel.capabilities();
                        capabilities.account.proactive_target = channel.proactive_target();
                        capabilities
                    },
                },
            })
            .collect())
    }

    /// Keep webhook-backed providers sendable without claiming to serve their ingress.
    #[tinybus(confidential)]
    async fn start_outbound_channel(
        &self,
        name: String,
        config: ChannelsConfig,
        proxy: HttpProxyPolicy,
    ) -> BusResult<()> {
        let http = ConfiguredHttpClients::new(proxy)
            .map_err(|_| BusError::failed("invalid host HTTP proxy policy"))?;
        self.start_channel_inner(name, config, &http, None, true)
            .await
    }

    /// Prepare channel facts without executing host-owned policy.
    #[tinybus(confidential)]
    async fn prepare_channel_control(
        &self,
        request: tinychannels_bus::module_control::ChannelControlRequest,
    ) -> BusResult<tinychannels_bus::module_control::ChannelControlResult> {
        let _admission = self.delivery_admission.lock().await;
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        crate::control::prepare(&self.host, request).await
    }

    /// Parse and execute remote chat commands inside the channel module.
    #[tinybus(confidential)]
    async fn execute_remote(
        &self,
        request: tinychannels_bus::module_remote::RemoteOperation,
    ) -> BusResult<tinychannels_bus::module_remote::RemoteOperationResult> {
        let _admission = self.delivery_admission.lock().await;
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        crate::remote::execute(&self.host, request).await
    }

    /// Drive progressive delivery without exposing its state to the host.
    #[tinybus(confidential)]
    async fn execute_progressive(
        &self,
        request: tinychannels_bus::module_progressive::ProgressiveCommand,
    ) -> BusResult<tinychannels_bus::module_progressive::ProgressiveResult> {
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        self.progressive.execute(request).await
    }

    /// Parse runtime switching and render help from host-resolved catalog facts.
    #[tinybus(confidential)]
    async fn prepare_runtime_command(
        &self,
        request: tinychannels_bus::module_routes::PrepareRuntimeCommandRequest,
    ) -> BusResult<Option<tinychannels_bus::module_routes::PreparedRuntimeCommand>> {
        use tinychannels::routes::{self, ChannelRuntimeCommand};
        use tinychannels_bus::module_routes::PreparedRuntimeCommand;
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        if serde_json::to_vec(&request)
            .map_err(|_| BusError::failed("invalid runtime command preparation"))?
            .len()
            > tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES
        {
            return Err(BusError::failed(
                "runtime command preparation exceeds its limit",
            ));
        }
        Ok(
            routes::parse_runtime_command(&request.channel, &request.content).map(|command| {
                match command {
                    ChannelRuntimeCommand::SetProvider(requested) => {
                        PreparedRuntimeCommand::SetProvider {
                            provider: routes::resolve_provider_alias(
                                &requested,
                                &request.providers,
                            ),
                            requested,
                        }
                    }
                    ChannelRuntimeCommand::SetModel(model) => {
                        PreparedRuntimeCommand::SetModel(model)
                    }
                    ChannelRuntimeCommand::ShowProviders => PreparedRuntimeCommand::ShowProviders(
                        routes::build_providers_help_response(&request.current, &request.providers),
                    ),
                    ChannelRuntimeCommand::ShowModel => PreparedRuntimeCommand::ShowModel(
                        routes::build_models_help_response_from_catalog(
                            &request.current,
                            &request.model_catalog,
                        ),
                    ),
                }
            }),
        )
    }

    /// Preserve provider data and compute persisted identities inside the module.
    #[tinybus(confidential)]
    async fn prepare_channel_inbound(
        &self,
        request: tinychannels_bus::module_inbound::PrepareChannelInboundRequest,
    ) -> BusResult<tinychannels_bus::module_inbound::PreparedChannelInbound> {
        use tinychannels_bus::module_inbound::{ChannelInboundSource, PreparedChannelInbound};
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        if request.namespace.len() > 256
            || serde_json::to_vec(&request)
                .map_err(|_| BusError::failed("invalid inbound preparation"))?
                .len()
                > tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES
        {
            return Err(BusError::failed("inbound preparation exceeds its limit"));
        }
        let (message, envelope) = match request.source {
            ChannelInboundSource::Legacy { message, envelope } => {
                let envelope = envelope.unwrap_or_else(|| {
                    tinychannels::inbound_envelope_from_legacy_message(&message)
                });
                if envelope.channel.id != message.channel
                    || envelope.message_id != message.id
                    || envelope.sender.id != message.sender
                    || envelope.conversation.id != message.reply_target
                {
                    return Err(BusError::failed(
                        "inbound envelope identity does not match message",
                    ));
                }
                (message, envelope)
            }
            ChannelInboundSource::Envelope {
                envelope,
                timestamp,
            } => (
                tinychannels::legacy_message_from_inbound_envelope(&envelope, timestamp)
                    .map_err(|error| BusError::failed(error.to_string()))?,
                envelope,
            ),
        };
        let prepared = PreparedChannelInbound {
            history_key: tinychannels::context::conversation_history_key(&message),
            thread_id: tinychannels::channel::derive_inbound_thread_id(
                &message.channel,
                Some(&message.sender),
                Some(&message.reply_target),
                message.thread_ts.as_deref(),
            ),
            client_id: tinychannels::channel::derive_inbound_client_id(
                &message.channel,
                Some(&message.sender),
            ),
            session_key: tinychannels::channel::build_session_key_for_inbound_envelope(
                &request.namespace,
                &envelope,
                request.session_policy,
            ),
            message,
            envelope,
        };
        if serde_json::to_vec(&prepared)
            .map_err(|_| BusError::failed("invalid inbound preparation"))?
            .len()
            > 2 * tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES
        {
            return Err(BusError::failed(
                "prepared inbound result exceeds its limit",
            ));
        }
        Ok(prepared)
    }

    /// Hosted ingress retains its optional-sender legacy identity behavior.
    #[tinybus(confidential)]
    async fn prepare_channel_identity(
        &self,
        request: tinychannels_bus::module_inbound::PrepareChannelIdentityRequest,
    ) -> BusResult<tinychannels_bus::module_inbound::PreparedChannelIdentity> {
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        if serde_json::to_vec(&request)
            .map_err(|_| BusError::failed("invalid inbound identity"))?
            .len()
            > 4096
        {
            return Err(BusError::failed("inbound identity exceeds its limit"));
        }
        Ok(tinychannels_bus::module_inbound::PreparedChannelIdentity {
            thread_id: tinychannels::channel::derive_inbound_thread_id(
                &request.channel,
                request.sender.as_deref(),
                request.reply_target.as_deref(),
                request.thread_ts.as_deref(),
            ),
            client_id: tinychannels::channel::derive_inbound_client_id(
                &request.channel,
                request.sender.as_deref(),
            ),
        })
    }

    /// Preserve the existing legacy and typed message conversion and hashing.
    #[tinybus(confidential)]
    async fn prepare_outbound_intent(
        &self,
        channel: String,
        request: tinychannels_bus::module_runtime::ChannelMessagePreparation,
    ) -> BusResult<tinychannels_bus::module_runtime::PreparedChannelIntent> {
        use tinychannels_bus::module_runtime::{ChannelMessagePreparation, PreparedChannelIntent};
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        if channel.is_empty()
            || channel.len() > 256
            || serde_json::to_vec(&request)
                .map_err(|_| BusError::failed("invalid channel message"))?
                .len()
                > tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES
        {
            return Err(BusError::failed("channel message exceeds its limit"));
        }
        let intent = match request {
            ChannelMessagePreparation::Legacy(message) => {
                tinychannels::outbound_intent_from_legacy_message(channel, message)
            }
            ChannelMessagePreparation::Typed(message) => {
                tinychannels::outbound_intent_from_send_message(channel, &message)
            }
            ChannelMessagePreparation::Intent(intent) => {
                if intent.channel_id != channel {
                    return Err(BusError::failed(
                        "intent destination does not match channel",
                    ));
                }
                intent
            }
        };
        Ok(PreparedChannelIntent {
            legacy_message: tinychannels::legacy_message_value_from_outbound_intent(&intent),
            intent,
        })
    }

    /// Check configured providers using their native health methods without
    /// starting listen loops. Credentials and proxy policy require attestation.
    #[tinybus(confidential)]
    async fn check_channel_health(
        &self,
        config: ChannelsConfig,
        proxy: HttpProxyPolicy,
        timeout_ms: u64,
    ) -> BusResult<Vec<tinychannels_bus::ChannelHealthReport>> {
        if self.terminal.load(Ordering::Acquire) {
            return Err(BusError::failed("channel module is shutting down"));
        }
        let budget = crate::health::health_budget(timeout_ms)?;
        let http = ConfiguredHttpClients::new(proxy)
            .map_err(|_| BusError::failed("invalid host HTTP proxy policy"))?;
        let host: Arc<dyn ChannelHost> = Arc::new(tinychannels::NoopHost);
        let providers = build_channels(&config, &host, &http);
        Ok(crate::health::check_providers(&providers, budget).await)
    }

    /// Dispatch one typed operation through TinyChannels' ChannelManager.
    ///
    /// The request can contain channel credentials and therefore requires a
    /// confidential host-to-module call. TinyChannels owns validation and
    /// provider-native operations; its callback port is limited to host-owned
    /// configuration persistence and authenticated managed API calls.
    #[tinybus(confidential)]
    async fn channel_controller(
        &self,
        request: ChannelControllerRequest,
    ) -> BusResult<ChannelControllerResult> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        crate::controller::dispatch(&self.host, request).await
    }

    /// Disconnect `name` and drop its tasks.
    ///
    /// Stopping a channel that is not running is **not** an error: a host
    /// reconciling desired against actual state should be able to call this
    /// unconditionally.
    async fn stop_channel(&self, name: String) -> BusResult<()> {
        let entry = self.running.lock().await.get(&name).cloned();
        if let Some(entry) = entry {
            let _admission = self.delivery_admission.lock().await;
            if let Err(error) = recover_delivery_channel(&self.host, &self.running, &name).await
                && !matches!(error, DeliveryQueueError::Unsupported)
            {
                return Err(delivery_store_error(error));
            }
            entry.stop().await?;
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(10), self.host.flush())
                    .await
                    .map_err(|_| BusError::failed("callbacks are pending; retry StopChannel"))?;
            let mut running = self.running.lock().await;
            if running
                .get(&name)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
            {
                running.remove(&name);
            }
            result?;
        }
        Ok(())
    }

    /// Send one outbound message through a running provider.
    async fn send_message(&self, name: String, message: SendMessage) -> BusResult<()> {
        // Cloned out of the map so the send does not hold the registry lock for
        // the duration of a network round trip.
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        let entry = {
            let running = self.running.lock().await;
            running
                .get(&name)
                .filter(|entry| !entry.is_finished())
                .cloned()
                .ok_or_else(|| BusError::MethodFailed {
                    name: UNKNOWN_CHANNEL_ERROR.to_owned(),
                    message: format!("channel {name} is not running"),
                })?
        };

        entry
            .send(message)
            .await
            .map_err(|error| BusError::MethodFailed {
                name: SEND_FAILED_ERROR.to_owned(),
                message: error.to_string(),
            })
    }

    /// Run provider interactions in the same bounded actor queue as sends.
    /// Recipients and draft contents cross the bus confidentially.
    #[tinybus(confidential)]
    async fn channel_interaction(
        &self,
        request: tinychannels_bus::ChannelInteractionRequest,
    ) -> BusResult<tinychannels_bus::ChannelInteractionResult> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        let entry = self
            .running
            .lock()
            .await
            .get(&request.channel)
            .filter(|entry| !entry.is_finished())
            .cloned()
            .ok_or_else(|| BusError::MethodFailed {
                name: UNKNOWN_CHANNEL_ERROR.to_owned(),
                message: "requested channel is not running".into(),
            })?;
        entry
            .interact(request.interaction)
            .await
            .map_err(|_| BusError::MethodFailed {
                name: SEND_FAILED_ERROR.to_owned(),
                message: "provider interaction failed".into(),
            })
    }

    /// Send one intent through the provider actor or host-backed durable queue.
    /// Required durability fails closed when queue storage is unavailable.
    async fn send_channel_intent(
        &self,
        name: String,
        intent: ChannelOutboundIntent,
    ) -> BusResult<ChannelSendMessageResult> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        if intent.channel_id != name {
            return Err(BusError::failed("intent channel does not match provider"));
        }
        if tinychannels_bus::ChannelOutboundMessage::from_intent(&intent)
            .parts
            .is_empty()
        {
            return Err(BusError::failed(
                "outbound intent must contain at least one content part",
            ));
        }
        if intent.durability == tinychannels::DeliveryDurability::Required {
            return self.send_required_intent(intent).await;
        }
        if serde_json::to_vec(&intent).map_or(true, |bytes| {
            bytes.len() > tinychannels_bus::module_relay::RELAY_FRAME_BYTES
        }) {
            return Err(BusError::failed("channel intent byte limit exceeded"));
        }
        let entry = {
            let running = self.running.lock().await;
            running
                .get(&name)
                .filter(|entry| !entry.is_finished())
                .cloned()
                .ok_or_else(|| BusError::MethodFailed {
                    name: UNKNOWN_CHANNEL_ERROR.to_owned(),
                    message: format!("channel {name} is not running"),
                })?
        };
        entry
            .send_typed(tinychannels_bus::ChannelOutboundMessage::from_intent(
                &intent,
            ))
            .await
            .map_err(|error| BusError::MethodFailed {
                name: SEND_FAILED_ERROR.to_owned(),
                message: error.to_string(),
            })?;
        Ok(ChannelSendMessageResult::default())
    }

    async fn send_required_intent(
        &self,
        intent: ChannelOutboundIntent,
    ) -> BusResult<ChannelSendMessageResult> {
        if intent.idempotency_key.is_empty() || intent.idempotency_key.len() > 256 {
            return Err(BusError::failed(
                "required delivery idempotency key is invalid",
            ));
        }
        if intent.channel_id.is_empty() || intent.channel_id != intent.channel_id.trim() {
            return Err(BusError::failed("required delivery channel is invalid"));
        }
        if serde_json::to_vec(&intent).map_or(true, |bytes| {
            bytes.len() > tinychannels_bus::module_delivery::DELIVERY_RECORD_BYTES
        }) {
            return Err(BusError::failed(
                "required delivery exceeds the record limit",
            ));
        }
        let fingerprint = intent_fingerprint(&intent)
            .map_err(|_| BusError::failed("required delivery could not be encoded"))?;
        let _admission = self
            .delivery_admission
            .try_lock()
            .map_err(|_| BusError::failed("required delivery queue is busy"))?;
        let store = HostDeliveryStore::from_host(&self.host);
        let id = intent.idempotency_key.clone();

        if let Some(result) = store.load_result(&id).await.map_err(delivery_store_error)? {
            if result.intent_fingerprint != fingerprint {
                return Err(BusError::failed("required delivery key conflicts"));
            }
            if store
                .load_pending(&id)
                .await
                .map_err(delivery_store_error)?
                .is_some()
            {
                ack_delivery(&store, &id)
                    .await
                    .map_err(delivery_store_error)?;
            }
            return Ok(ChannelSendMessageResult::default());
        }
        if let Some(failed) = store.load_failed(&id).await.map_err(delivery_store_error)? {
            return if failed.intent == intent {
                Err(BusError::failed("required delivery is in failed storage"))
            } else {
                Err(BusError::failed("required delivery key conflicts"))
            };
        }

        let pending = store
            .load_pending(&id)
            .await
            .map_err(delivery_store_error)?;
        if let Some(existing) = &pending {
            if existing.intent != intent {
                return Err(BusError::failed("required delivery key conflicts"));
            }
        } else {
            enqueue_delivery(
                &store,
                QueuedDelivery::new(id.clone(), intent.clone(), now_unix_ms()),
            )
            .await
            .map_err(delivery_store_error)?;
        }

        let handler = ModuleDeliveryHandler {
            running: self.running.clone(),
            store: store.clone(),
        };
        let recovery =
            drain_selected_pending_deliveries(&store, &handler, now_unix_ms(), |entry| {
                PendingDeliveryDrainDecision {
                    matches: entry.intent.channel_id == intent.channel_id,
                    bypass_backoff: false,
                }
            })
            .await
            .map_err(delivery_store_error)?;
        if let Some(result) = store.load_result(&id).await.map_err(delivery_store_error)? {
            if result.intent_fingerprint != fingerprint {
                return Err(BusError::failed("required delivery key conflicts"));
            }
            if store
                .load_pending(&id)
                .await
                .map_err(delivery_store_error)?
                .is_some()
            {
                ack_delivery(&store, &id)
                    .await
                    .map_err(delivery_store_error)?;
            }
            return Ok(ChannelSendMessageResult::default());
        }
        let still_pending = store
            .load_pending(&id)
            .await
            .map_err(delivery_store_error)?
            .is_some();
        let moved_to_failed = store
            .load_failed(&id)
            .await
            .map_err(delivery_store_error)?
            .is_some();
        if moved_to_failed || still_pending || recovery.failed > 0 || recovery.deferred_backoff > 0
        {
            return Err(BusError::failed(
                "required delivery remains pending or failed",
            ));
        }
        Ok(ChannelSendMessageResult::default())
    }

    /// Send SMTP mail without requiring an active IMAP listener.
    /// The host hydrates credentials and sends this payload confidentially.
    #[tinybus(confidential)]
    async fn send_email(&self, request: EmailSendRequest) -> BusResult<ChannelSendMessageResult> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        self.email.send(request).await
    }

    /// Start a bounded confidential upload for an SMTP message.
    #[tinybus(confidential)]
    async fn begin_email_upload(&self, request: EmailUploadBegin) -> BusResult<EmailUploadHandle> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        self.email.begin_upload(request).await
    }

    /// Append an ordered, retry-safe confidential attachment chunk.
    #[tinybus(confidential)]
    async fn append_email_upload_chunk(&self, chunk: EmailUploadChunk) -> BusResult<u32> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        self.email.append_upload_chunk(chunk).await
    }

    /// Send a completed upload once; retries replay the handle's terminal result.
    #[tinybus(confidential)]
    async fn send_email_upload(
        &self,
        handle: EmailUploadHandle,
    ) -> BusResult<ChannelSendMessageResult> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        self.email.send_upload(handle).await
    }

    /// Discard an upload that has not entered SMTP delivery.
    #[tinybus(confidential)]
    async fn abort_email_upload(&self, handle: EmailUploadHandle) -> BusResult<()> {
        self.email.abort_upload(handle).await
    }

    /// The providers currently running, in no particular order.
    async fn list_channels(&self) -> BusResult<Vec<String>> {
        Ok(self
            .running
            .lock()
            .await
            .iter()
            .filter(|(_, entry)| !entry.is_finished())
            .map(|(name, _)| name.clone())
            .collect())
    }

    /// Whether `name` is running, and whether the provider reports itself healthy.
    async fn channel_status(&self, name: String) -> BusResult<String> {
        let channel = {
            let running = self.running.lock().await;
            running
                .get(&name)
                .filter(|entry| !entry.is_finished())
                .map(|entry| Arc::clone(&entry.channel))
        };
        let Some(channel) = channel else {
            return Ok("stopped".to_owned());
        };
        Ok(if channel.health_check().await {
            "connected".to_owned()
        } else {
            "unhealthy".to_owned()
        })
    }
    /// Mint a bounded, single-use relay reservation before async native startup.
    async fn reserve_relay(&self) -> BusResult<RelayReservation> {
        self.relays.reserve().await
    }
    /// Start this known handle with host-hydrated secrets over confidential delivery.
    #[tinybus(confidential)]
    async fn start_relay(
        &self,
        id: RelayReservation,
        config: RelayRuntimeConfig,
    ) -> BusResult<tinychannels::relay::CapabilityDescriptor> {
        self.relays.start(id, config).await
    }
    /// Cancel pending startup and join relay/reconnect/native callback work.
    async fn stop_relay(&self, id: RelayReservation) -> BusResult<()> {
        self.relays.stop(id).await
    }
    /// Report safe lifecycle facts without URL, identities or credentials.
    async fn relay_status(&self, id: RelayReservation) -> BusResult<RelayStatus> {
        self.relays.status(id).await
    }
    /// Enqueue one bounded, ordered relay intent.
    async fn send_relay_intent(
        &self,
        id: RelayReservation,
        intent: ChannelOutboundIntent,
    ) -> BusResult<ChannelSendMessageResult> {
        self.relays.send(id, intent).await
    }
    /// Freeze operations and join all native providers, relay and callbacks.
    pub(crate) async fn shutdown(&self) -> BusResult<()> {
        self.terminal.store(true, Ordering::SeqCst);
        self.relays.freeze();
        // Fence and stop identity-scoped children before draining the legacy
        // service resources, but do not let one failed child skip cleanup of
        // root providers, relay, email, delivery state, or host callbacks.
        // Runtimes retains failed handles, so a later Shutdown retries them.
        let runtime_failure = if let Some(runtimes) = &self.runtimes {
            runtimes.shutdown().await.err()
        } else {
            None
        };
        let mut task = self.shutdown_task.lock().await;
        if task.is_none() {
            let progressive = self.progressive.clone();
            let providers = self.running.clone();
            let relay = self.relays.clone();
            let email = self.email.clone();
            let host = self.host.clone();
            let delivery_admission = self.delivery_admission.clone();
            *task = Some(tokio::spawn(async move {
                let progressive_result = progressive.shutdown().await;
                let progressive_pending = progressive_result.is_err();
                let mut failure = runtime_failure.or_else(|| progressive_result.err());
                let _delivery = delivery_admission.lock().await;
                let delivery_store = HostDeliveryStore::from_host(&host);
                let delivery_handler = ModuleDeliveryHandler {
                    running: providers.clone(),
                    store: delivery_store.clone(),
                };
                match recover_pending_deliveries(&delivery_store, &delivery_handler, now_unix_ms())
                    .await
                {
                    Err(DeliveryQueueError::Unsupported) => {}
                    Err(error) => failure = Some(delivery_store_error(error)),
                    Ok(_) => {}
                }
                drop(_delivery);
                let entries: Vec<_> = providers
                    .lock()
                    .await
                    .iter()
                    .map(|(name, entry)| (name.clone(), entry.clone()))
                    .collect();
                let mut pending = false;
                for (name, entry) in entries {
                    match entry.stop().await {
                        Ok(()) => {
                            providers.lock().await.remove(&name);
                        }
                        Err(error) => {
                            pending = true;
                            if failure.is_none() {
                                failure = Some(error);
                            }
                        }
                    }
                }
                if let Err(error) = relay.shutdown().await
                    && failure.is_none()
                {
                    failure = Some(error);
                }
                if let Err(error) = email.shutdown().await
                    && failure.is_none()
                {
                    failure = Some(error);
                }
                if !pending && !progressive_pending && relay.is_empty().await {
                    match host.shutdown().await {
                        Err(error) if failure.is_none() => failure = Some(error),
                        _ => {}
                    }
                }
                match failure {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }));
        }
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            task.as_mut()
                .ok_or_else(|| BusError::failed("missing shutdown owner"))?,
        )
        .await
        .map_err(|_| BusError::failed("module cleanup is pending; retry Shutdown"))?
        .map_err(|_| BusError::failed("module cleanup worker failed"))?;
        task.take();
        result
    }
}

async fn recover_delivery_channel(
    host: &HostChannels,
    running: &Arc<Mutex<HashMap<String, Arc<Running>>>>,
    channel_id: &str,
) -> Result<(), DeliveryQueueError> {
    let store = HostDeliveryStore::from_host(host);
    let handler = ModuleDeliveryHandler {
        running: running.clone(),
        store: store.clone(),
    };
    drain_selected_pending_deliveries(&store, &handler, now_unix_ms(), |entry| {
        PendingDeliveryDrainDecision {
            matches: entry.intent.channel_id == channel_id,
            bypass_backoff: false,
        }
    })
    .await?;
    Ok(())
}

async fn finish_start_admission(
    host: &HostChannels,
    running: &Arc<Mutex<HashMap<String, Arc<Running>>>>,
    delivery_admission: &Mutex<()>,
    terminal: &AtomicBool,
    name: &str,
) -> BusResult<()> {
    let _admission = delivery_admission.lock().await;
    if terminal.load(Ordering::SeqCst) {
        let entry = running.lock().await.get(name).cloned();
        if let Some(entry) = entry {
            entry.stop().await?;
            let mut running = running.lock().await;
            if running
                .get(name)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
            {
                running.remove(name);
            }
        }
        return Err(BusError::failed("module is shutting down"));
    }
    if let Err(error) = recover_delivery_channel(host, running, name).await
        && !matches!(error, DeliveryQueueError::Unsupported)
    {
        let entry = running.lock().await.get(name).cloned();
        if let Some(entry) = entry
            && entry.stop().await.is_ok()
        {
            let mut running = running.lock().await;
            if running
                .get(name)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
            {
                running.remove(name);
            }
        }
        return Err(delivery_store_error(error));
    }
    Ok(())
}

async fn setup(connection: Connection) -> BusResult<()> {
    let host = HostChannels::new(connection.clone());
    let channels = Channels::new(
        host,
        Some(Arc::new(crate::runtime::Runtimes::new(connection.clone()))),
    );
    connection
        .serve_at(OBJECT_PATH.try_into()?, channels)
        .await?;
    connection.request_name(BUS_NAME).await?;
    Ok(())
}

fn intent_fingerprint(intent: &ChannelOutboundIntent) -> Result<String, serde_json::Error> {
    use sha2::Digest as _;
    let encoded = serde_json::to_vec(intent)?;
    Ok(hex::encode(sha2::Sha256::digest(encoded)))
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn delivery_store_error(error: DeliveryQueueError) -> BusError {
    match error {
        DeliveryQueueError::Unsupported => {
            BusError::failed("required delivery host queue is unavailable")
        }
        DeliveryQueueError::Store(_) => {
            BusError::failed("required delivery persistence or recovery failed")
        }
    }
}

// Isolate the generated public C symbols so the lint exception cannot hide
// undocumented Rust API. Their contract is TinyBus ABI v1.
#[allow(
    missing_docs,
    unreachable_pub,
    reason = "generated C ABI symbols are documented by the TinyBus module SDK"
)]
pub(crate) mod exports {
    tinybus_module::module_export_optional_static! {
        setup = super::setup,
        worker_threads = 4,
        provides = ["ai.tinyhumans.tinychannels.Channels"],
        methods = [
            "StartChannel",
            "StopChannel",
            "SendMessage",
            "ListChannels",
            "ChannelStatus",
            "ReserveRelay", "StartRelay", "StopRelay", "RelayStatus", "SendRelayIntent", "Shutdown", "SendChannelIntent", "SendEmail",
            "BeginEmailUpload", "AppendEmailUploadChunk", "SendEmailUpload", "AbortEmailUpload",
            "StartChannelConfigured",
            "ChannelController",
            "ChannelInteraction",
            "CheckChannelHealth",
            "StartChannelSupervised",
            "ReserveRuntime", "StartRuntime", "StopRuntime",
            "DescribeConfiguredChannels", "StartOutboundChannel", "PrepareOutboundIntent",
            "PrepareChannelInbound", "PrepareChannelIdentity", "ExecuteProgressive", "ExecuteRemote", "PrepareChannelControl", "StartRuntimeWithDispatch", "PrepareRuntimeCommand",
        ],
        signals = [],
        // The host callback object is resolved per call, not required at load:
        // an outbound-only host that serves nothing is supported.
        requires = [],
        optional = ["ai.tinyhumans.tinychannels.ChannelsHost"],
        lazy = false,
    }
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
