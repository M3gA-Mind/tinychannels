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

use crate::provider::{Hooks, Running};
use crate::relay::Relays;
use std::sync::atomic::{AtomicBool, Ordering};
use tinybus::{Connection, Error as BusError, Result as BusResult};
use tinychannels::factory::{DefaultHttpClients, build_channels};
use tinychannels::host::ChannelHost;
use tinychannels_bus::ChannelOutboundIntent;
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
struct Channels {
    host: HostChannels,
    email: EmailSender,
    /// Shared with each listener task so it can retire its own entry.
    running: Arc<Mutex<HashMap<String, Arc<Running>>>>,
    relays: Arc<Relays>,
    terminal: AtomicBool,
    shutdown_task: Mutex<Option<JoinHandle<BusResult<()>>>>,
}

#[allow(
    clippy::unused_async,
    reason = "tinybus::interface requires every method to be `async fn`"
)]
#[tinybus::interface(name = "ai.tinyhumans.tinychannels.Channels")]
impl Channels {
    /// Connect `name` from `config` and start receiving.
    async fn start_channel(&self, name: String, config: ChannelsConfig) -> BusResult<()> {
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

        // The factory builds every *configured* provider; we take the one asked
        // for. Building the set is cheap (no I/O) and keeps a single
        // config-to-provider mapping rather than a second one here that could
        // disagree about, say, which WhatsApp backend a stanza describes.
        let hooks = Arc::new(Hooks::default());
        let owner_host: Arc<dyn ChannelHost> = hooks.host(self.host.clone());
        let built = build_channels(&config, &owner_host, &DefaultHttpClients);
        let channel = built
            .into_iter()
            .find(|candidate| candidate.name() == name)
            .ok_or_else(|| BusError::MethodFailed {
                name: NOT_CONFIGURED_ERROR.to_owned(),
                message: format!("channel {name} is not present in the supplied config"),
            })?;

        // Refuse the providers whose inbound path this module cannot serve.
        //
        // Linq and the WhatsApp *Cloud API* variant are push-based: their
        // `listen` implementations sleep forever, because messages arrive at an
        // HTTPS webhook the host operates. The module exports no route or bus
        // member that can feed such a payload into the channel, so starting one
        // here would report success and then deliver nothing, for ever — the
        // worst failure shape available.
        //
        // Failing loudly is deliberately preferred to a silent no-op. When
        // webhook ingress is added to the contract this check is what gets
        // relaxed. (WhatsApp *Web* is unaffected: it has a real listen loop.
        // Both variants report `name() == "whatsapp"`, so the config shape is
        // what distinguishes them, not the name.)
        if is_webhook_backed(&name, &config) {
            return Err(BusError::MethodFailed {
                name: NO_INGRESS_ERROR.to_owned(),
                message: format!(
                    "channel {name} receives inbound traffic through an external webhook, \
                     and this module has no ingress for it; run this provider in-process \
                     or serve its webhook host-side"
                ),
            });
        }

        running.insert(name, Running::start(channel, hooks, self.host.clone()));
        Ok(())
    }

    /// Disconnect `name` and drop its tasks.
    ///
    /// Stopping a channel that is not running is **not** an error: a host
    /// reconciling desired against actual state should be able to call this
    /// unconditionally.
    async fn stop_channel(&self, name: String) -> BusResult<()> {
        let entry = self.running.lock().await.get(&name).cloned();
        if let Some(entry) = entry {
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

    /// Send one best-effort or disabled intent through the provider actor.
    /// Required durability needs a host queue and is refused when that port
    /// has not been installed; this method never silently downgrades it.
    async fn send_channel_intent(
        &self,
        name: String,
        intent: ChannelOutboundIntent,
    ) -> BusResult<ChannelSendMessageResult> {
        if self.terminal.load(Ordering::SeqCst) {
            return Err(BusError::failed("module is shutting down"));
        }
        if intent.durability == tinychannels::DeliveryDurability::Required {
            return Err(BusError::failed(
                "required durability needs the host delivery queue",
            ));
        }
        if intent.channel_id != name {
            return Err(BusError::failed("intent channel does not match provider"));
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
        let value = tinychannels::legacy_message_value_from_outbound_intent(&intent);
        let content = match &intent.payload {
            tinychannels::OutboundPayload::Text { text } => text.clone(),
            _ => serde_json::to_string(&value)
                .map_err(|_| BusError::failed("could not encode channel intent"))?,
        };
        entry
            .send(SendMessage {
                content,
                recipient: intent.conversation_id,
                subject: None,
                thread_ts: intent.thread_id,
                idempotency_key: Some(intent.idempotency_key),
            })
            .await
            .map_err(|error| BusError::MethodFailed {
                name: SEND_FAILED_ERROR.to_owned(),
                message: error.to_string(),
            })?;
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
    async fn shutdown(&self) -> BusResult<()> {
        self.terminal.store(true, Ordering::SeqCst);
        self.relays.freeze();
        let mut task = self.shutdown_task.lock().await;
        if task.is_none() {
            let providers = self.running.clone();
            let relay = self.relays.clone();
            let email = self.email.clone();
            let host = self.host.clone();
            *task = Some(tokio::spawn(async move {
                let entries: Vec<_> = providers
                    .lock()
                    .await
                    .iter()
                    .map(|(name, entry)| (name.clone(), entry.clone()))
                    .collect();
                let mut failure = None;
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
                if !pending && relay.is_empty().await {
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

async fn setup(connection: Connection) -> BusResult<()> {
    let host = HostChannels::new(connection.clone());
    let channels = Channels {
        relays: Arc::new(Relays::new(host.clone())),
        host,
        email: EmailSender::new(),
        running: Arc::new(Mutex::new(HashMap::new())),
        terminal: AtomicBool::new(false),
        shutdown_task: Mutex::new(None),
    };
    connection
        .serve_at(OBJECT_PATH.try_into()?, channels)
        .await?;
    connection.request_name(BUS_NAME).await?;
    Ok(())
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
