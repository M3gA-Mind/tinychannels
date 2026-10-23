//! WhatsApp Web channel backed by upstream [`whatsapp-rust`] 0.7.
//!
//! # Why the upgrade
//!
//! The previous implementation used `wa-rs` 0.2 (a fork that pinned to stable
//! Rust). That fork silently dropped `Event::Message` for LID-addressed
//! contacts and group sender-key (`skmsg`) messages: the protocol layer
//! decrypted the payload but never dispatched it to user code, breaking
//! agent dispatch for the bulk of modern WhatsApp traffic (LID is the
//! current default). Upstream `whatsapp-rust` fixed this in PRs #170
//! (SKDM tracking) + #181 (LID/PN mapping) + sender-key dispatch.
//!
//! # Feature Flag
//!
//! ```sh
//! cargo build --features whatsapp-web
//! ```
//!
//! # Configuration
//!
//! ```toml
//! [channels.whatsapp]
//! session_path = "~/.openhuman/whatsapp-session.db"  # Account label for the durable session
//! pair_phone = "15551234567"                         # Optional: pair-code linking
//! allowed_numbers = ["+1234567890", "*"]             # Same shape as Cloud API
//! ```
//!
//! # Runtime negotiation
//!
//! Selected automatically by the channels runtime startup when `session_path`
//! is set. The Cloud API channel ([`crate::providers::whatsapp`]) is used when
//! `phone_number_id` is set instead.
//!
//! # Durable session
//!
//! With a [`crate::host::StorageProvider`] the pairing state is written through
//! [`crate::storage::whatsapp::DocumentWhatsAppBackend`] to the host's document
//! store, so a restart resumes the linked device. Without one the channel falls
//! back to wacore's in-memory backend and needs re-linking. `session_path` no
//! longer names a file: it only labels the account inside the scoped store.
//!
//! [`whatsapp-rust`]: https://docs.rs/whatsapp-rust/0.7

#[cfg(feature = "whatsapp-web")]
use crate::traits::ChannelMessage;
#[cfg(feature = "whatsapp-web")]
use anyhow::Result;
#[cfg(feature = "whatsapp-web")]
use async_trait::async_trait;
use std::sync::Arc;

#[cfg(feature = "whatsapp-web")]
#[path = "capabilities.rs"]
mod capabilities;
#[cfg(feature = "whatsapp-web")]
#[path = "client.rs"]
pub(crate) mod client;
#[cfg(feature = "whatsapp-web")]
#[path = "identity.rs"]
mod identity;
#[cfg(feature = "whatsapp-web")]
#[path = "inbound.rs"]
mod inbound;
#[cfg(feature = "whatsapp-web")]
#[path = "media_build.rs"]
pub(crate) mod media_build;
#[cfg(feature = "whatsapp-web")]
#[path = "outbound.rs"]
mod outbound;

#[cfg(feature = "whatsapp-web")]
#[cfg(feature = "whatsapp-web")]
use crate::providers::whatsapp_shared::error::WaFailure;
#[cfg(feature = "whatsapp-web")]
use crate::providers::whatsapp_shared::window::{BoundedMap, SeenSet};
#[cfg(feature = "whatsapp-web")]
use client::OutboundClient;

// Imports used only by the real (feature-on) implementation; gated so the
// feature-off stub build doesn't flag them as unused.
#[cfg(feature = "whatsapp-web")]
use anyhow::anyhow;
#[cfg(feature = "whatsapp-web")]
use parking_lot::Mutex;
#[cfg(feature = "whatsapp-web")]
use std::collections::HashSet;
#[cfg(feature = "whatsapp-web")]
use std::sync::atomic::{AtomicBool, Ordering};

/// State the bot's event handler shares with the channel: the inbound sink,
/// the allow-list, the liveness flag and the group-provenance set.
///
/// Holding it apart from the bot lets inbound events be translated and gated
/// without a WhatsApp connection.
#[cfg(feature = "whatsapp-web")]
#[derive(Clone)]
struct InboundState {
    tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    typed_tx: Option<tokio::sync::mpsc::Sender<TypedIngressDelivery>>,
    allowed_numbers: Vec<String>,
    connected: Arc<AtomicBool>,
    allowed_groups: Arc<Mutex<HashSet<String>>>,
    /// Typed ingress and the ports it persists attachments through. Without an
    /// event sink the state keeps the v21 text path.
    events: Option<Arc<dyn crate::host::EventSink>>,
    media_repository: Option<Arc<dyn crate::host::MediaRepository>>,
    voice_media_store: Option<Arc<dyn crate::host::VoiceMediaStore>>,
    /// The live session, for attachment downloads and the linked account.
    client: Arc<Mutex<Option<Arc<dyn OutboundClient>>>>,
    /// Provider message ids already published (the SDK may redeliver).
    seen: Arc<Mutex<SeenSet>>,
    typed_delivery: Arc<tokio::sync::Mutex<()>>,
    /// Author of each recent inbound message, for group reactions and quotes.
    authors: Arc<Mutex<BoundedMap>>,
    identity: Arc<identity::LinkedIdentity>,
}

#[cfg(feature = "whatsapp-web")]
struct TypedIngressDelivery {
    ingress: tinychannels_bus::ChannelIngress,
    accepted:
        tokio::sync::oneshot::Sender<std::result::Result<(), crate::channel::ChannelSendError>>,
}

#[cfg(feature = "whatsapp-web")]
struct TypedListenerOwnership {
    active: Arc<AtomicBool>,
    bot_handle: Arc<Mutex<Option<whatsapp_rust::bot::BotHandle>>>,
    client: Arc<Mutex<Option<Arc<dyn OutboundClient>>>>,
    connected: Arc<AtomicBool>,
    tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<ChannelMessage>>>>,
}

#[cfg(feature = "whatsapp-web")]
#[derive(Debug)]
enum TypedDeliveryFailure {
    Cancelled,
    Rejected(crate::channel::ChannelSendError),
}

#[cfg(feature = "whatsapp-web")]
async fn deliver_typed_ingress(
    sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ingress: &tinychannels_bus::ChannelIngress,
    shutdown: &tokio::sync::Notify,
    cancellation: &mut tokio::sync::watch::Receiver<bool>,
) -> std::result::Result<(), TypedDeliveryFailure> {
    loop {
        let result = tokio::select! {
            biased;
            _ = shutdown.notified() => return Err(TypedDeliveryFailure::Cancelled),
            _ = cancellation.wait_for(|value| *value) => return Err(TypedDeliveryFailure::Cancelled),
            result = sink.push_ingress(ingress.clone()) => result,
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error) if error.retryable => {
                tokio::select! {
                    biased;
                    _ = shutdown.notified() => return Err(TypedDeliveryFailure::Cancelled),
                    _ = cancellation.wait_for(|value| *value) => return Err(TypedDeliveryFailure::Cancelled),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
            }
            Err(error) => return Err(TypedDeliveryFailure::Rejected(error)),
        }
    }
}

#[cfg(feature = "whatsapp-web")]
impl Drop for TypedListenerOwnership {
    fn drop(&mut self) {
        self.connected.store(false, Ordering::Release);
        *self.client.lock() = None;
        *self.tx.lock() = None;
        if let Some(handle) = self.bot_handle.lock().take() {
            handle.abort();
            let active = Arc::clone(&self.active);
            tokio::spawn(async move {
                let _ = handle.await;
                active.store(false, Ordering::Release);
            });
        } else {
            self.active.store(false, Ordering::Release);
        }
    }
}

/// WhatsApp Web channel.
///
/// Wraps a `whatsapp-rust` Bot with our `Channel` trait. The bot owns an
/// `Arc<Client>` for outbound operations (`send`, typing) and a `BotHandle`
/// for shutdown. Inbound messages are pushed onto an [`tokio::sync::mpsc::Sender`] so
/// the existing channel inbound subscriber pipeline can process them.
#[cfg(feature = "whatsapp-web")]
pub struct WhatsAppWebChannel {
    /// Path to the SQLite session database.
    session_path: String,
    /// Optional phone number for pair-code linking (E.164 digits, no leading `+`).
    pair_phone: Option<String>,
    /// Optional pre-allocated pair code paired with `pair_phone`.
    pair_code: Option<String>,
    /// E.164 numbers (with leading `+`) allowed to interact, or `["*"]` for any.
    /// Empty also means "allow all" — same convention as the Cloud API channel.
    allowed_numbers: Vec<String>,
    /// Bot run handle, retained for graceful shutdown.
    bot_handle: Arc<Mutex<Option<whatsapp_rust::bot::BotHandle>>>,
    /// Guards the typed listener's single-owner lifecycle.
    typed_listener_active: Arc<AtomicBool>,
    /// Live client used for outbound calls; populated after `Bot::build` returns.
    client: Arc<Mutex<Option<Arc<dyn OutboundClient>>>>,
    /// Liveness signal driven by upstream `Event::Connected` / `LoggedOut` /
    /// `StreamError`. Used by `health_check` so a dropped session no longer
    /// reports healthy until process shutdown.
    connected: Arc<AtomicBool>,
    /// Group JIDs (`...@g.us`) we've already accepted an allowed inbound
    /// from. Acts as outbound provenance: replies into a group are only
    /// permitted after a participant on the per-number allowlist messaged
    /// in. Without this, any caller able to pass a `recipient` could post
    /// into arbitrary joined groups via the @g.us suffix.
    allowed_groups: Arc<Mutex<HashSet<String>>>,
    /// Sink for inbound typed ingress. Populated when [`ChannelAdapter::start`](crate::channel::ChannelAdapter::start)
    /// is called and shared with the event-handler closure.
    tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<ChannelMessage>>>>,
    /// Cancellation for the configured typed listener. The provider callback
    /// owns only a bounded queue sender; the listener future retains the
    /// caller's borrowed sink and acknowledges delivery after it accepts.
    listener_cancel: tokio::sync::watch::Sender<bool>,
    /// Host lifecycle capability used to register graceful-shutdown teardown.
    /// When absent, teardown is not wired into the host and `listen` still
    /// parks on the internal notify gate.
    lifecycle: Option<Arc<dyn crate::host::LifecycleRegistry>>,
    /// Host storage for the durable session. When absent the session lives in
    /// memory and the device must be re-linked after a restart.
    storage: Option<Arc<dyn crate::host::StorageProvider>>,
    media_repository: Option<Arc<dyn crate::host::MediaRepository>>,
    voice_media_store: Option<Arc<dyn crate::host::VoiceMediaStore>>,
    voice_synthesis: bool,
    /// Where typed inbound messages and events are published.
    events: Option<Arc<dyn crate::host::EventSink>>,
    seen: Arc<Mutex<SeenSet>>,
    typed_delivery: Arc<tokio::sync::Mutex<()>>,
    authors: Arc<Mutex<BoundedMap>>,
    /// The account the linked session is authenticated as.
    identity: Arc<identity::LinkedIdentity>,
}

#[cfg(feature = "whatsapp-web")]
impl WhatsAppWebChannel {
    fn non_self_echoes<T>(
        messages: &[T],
        is_from_me: impl Fn(&T) -> bool,
    ) -> impl Iterator<Item = &T> {
        messages.iter().filter(move |message| !is_from_me(message))
    }

    /// Construct a channel. The bot does not connect until [`ChannelAdapter::start`](crate::channel::ChannelAdapter::start)
    /// is invoked.
    pub fn new(
        session_path: String,
        pair_phone: Option<String>,
        pair_code: Option<String>,
        allowed_numbers: Vec<String>,
    ) -> Self {
        Self {
            session_path,
            pair_phone,
            pair_code,
            allowed_numbers,
            bot_handle: Arc::new(Mutex::new(None)),
            typed_listener_active: Arc::new(AtomicBool::new(false)),
            client: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            allowed_groups: Arc::new(Mutex::new(HashSet::new())),
            tx: Arc::new(Mutex::new(None)),
            listener_cancel: tokio::sync::watch::channel(false).0,
            lifecycle: None,
            storage: None,
            media_repository: None,
            voice_media_store: None,
            voice_synthesis: false,
            events: None,
            seen: Arc::new(Mutex::new(SeenSet::new(4096))),
            typed_delivery: Arc::new(tokio::sync::Mutex::new(())),
            authors: Arc::new(Mutex::new(BoundedMap::new(4096))),
            identity: Arc::new(identity::LinkedIdentity::default()),
        }
    }

    /// Construct from the provider-owned configuration used by the factory.
    pub fn from_config(config: super::WhatsAppWebConfig) -> Self {
        Self::new(
            config.session_path,
            config.pair_phone,
            config.pair_code,
            config.allowed_numbers,
        )
    }

    /// Scoped host media storage, which unlocks typed media in both directions.
    #[must_use]
    pub fn with_media_repository(mut self, store: Arc<dyn crate::host::MediaRepository>) -> Self {
        self.media_repository = Some(store);
        self
    }

    /// Host voice storage, which unlocks voice notes in both directions.
    #[must_use]
    pub fn with_voice_media_store(mut self, store: Arc<dyn crate::host::VoiceMediaStore>) -> Self {
        self.voice_media_store = Some(store);
        self
    }

    /// Whether the module can prepare synthesized voice replies for this account.
    #[must_use]
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.voice_synthesis = enabled;
        self
    }

    /// Where typed inbound messages and events are published.
    #[must_use]
    pub fn with_events(mut self, events: Arc<dyn crate::host::EventSink>) -> Self {
        self.events = Some(events);
        self
    }

    /// Persist the linked-device session through the host's storage so a
    /// restart resumes it instead of asking for a new QR scan.
    pub fn with_storage(mut self, storage: Arc<dyn crate::host::StorageProvider>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// The session backend: durable when the host provides storage, in-memory
    /// (with a warning) otherwise or when storage cannot be opened.
    async fn session_backend(&self) -> Arc<dyn whatsapp_rust::store::Backend> {
        if let Some(storage) = &self.storage {
            let opened = async {
                let docs = storage.documents().await?;
                crate::storage::whatsapp::DocumentWhatsAppBackend::open(docs, &self.session_path)
                    .await
                    .map_err(anyhow::Error::from)
            }
            .await;
            match opened {
                Ok(backend) => {
                    tracing::info!("[whatsapp_web] using the durable session backend");
                    return Arc::new(backend);
                }
                Err(error) => tracing::warn!(
                    "[whatsapp_web] durable session storage unavailable ({error}); \
                     falling back to an in-memory session"
                ),
            }
        } else {
            tracing::warn!(
                "[whatsapp_web] the host provides no storage; the session is in memory and \
                 the device must be re-linked after a restart"
            );
        }
        Arc::new(wacore::store::InMemoryBackend::new())
    }

    /// Wire in the host [`crate::host::LifecycleRegistry`] so `listen` can
    /// register its graceful-shutdown teardown with the host runtime.
    pub fn with_lifecycle(
        mut self,
        lifecycle: std::sync::Arc<dyn crate::host::LifecycleRegistry>,
    ) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }

    /// Allowlist check. Empty list ⇒ allow-all (matches Cloud API behaviour).
    fn is_number_allowed(&self, phone: &str) -> bool {
        self.allowed_numbers.is_empty()
            || self.allowed_numbers.iter().any(|n| n == "*" || n == phone)
    }

    /// Recognise WhatsApp group JIDs (`...@g.us`). Group recipients bypass
    /// the per-number outbound allowlist because group membership is
    /// governed by WhatsApp itself; the inbound side already gated on the
    /// participant's allowlist status before we ever decided to reply.
    fn is_group_jid(recipient: &str) -> bool {
        recipient.trim().ends_with("@g.us")
    }

    /// Outbound gate combining group-provenance with the per-number allowlist.
    /// Group JIDs are only permitted when an allowed inbound has already
    /// been received from that exact group — populated in the inbound
    /// handler when an allow-listed participant posts. This narrows the
    /// previous "all `@g.us` is fine" path so an attacker that can supply
    /// a `recipient` cannot post into arbitrary groups the bot has joined.
    fn should_allow_outbound(&self, recipient: &str) -> bool {
        if Self::is_group_jid(recipient) {
            return self.allowed_groups.lock().contains(recipient.trim());
        }
        let normalized = self.normalize_phone(recipient);
        self.is_number_allowed(&normalized)
    }

    /// Mask a recipient identifier for log emission. Handles bare phone
    /// numbers, `<digits>@s.whatsapp.net`/`@lid` DM JIDs, and `@g.us`
    /// group JIDs uniformly so warning paths never carry a full ID.
    fn redact_recipient(recipient: &str) -> String {
        let trimmed = recipient.trim();
        if let Some((user, server)) = trimmed.split_once('@') {
            format!("{}@{}", Self::redact_phone(user), server)
        } else {
            Self::redact_phone(trimmed)
        }
    }

    /// Pick the address downstream replies should be sent back to.
    ///
    /// Group chats are addressed by the group JID (`...@g.us`); a reply that
    /// targeted the participant's phone instead would leak the conversation
    /// into a private DM.
    fn compute_reply_target(chat_jid: &str, sender_normalized: &str) -> String {
        if chat_jid.ends_with("@g.us") {
            chat_jid.to_string()
        } else {
            sender_normalized.to_string()
        }
    }

    /// Mask the middle digits of an E.164 number so logs only carry a coarse
    /// fingerprint instead of the full identifier.
    fn redact_phone(phone: &str) -> String {
        let prefix = if phone.starts_with('+') { "+" } else { "" };
        if phone.len() <= prefix.len() + 4 {
            return format!("{prefix}****");
        }
        let tail = &phone[phone.len() - 4..];
        format!("{prefix}***{tail}")
    }

    /// Pull the displayable text out of an inbound WhatsApp Message proto.
    /// Falls back from `conversation` to `extended_text_message.text`, then
    /// to an empty string for non-text payloads.
    fn extract_message_text(conversation: Option<&str>, extended_text: Option<&str>) -> String {
        conversation
            .or(extended_text)
            .map(|s| s.to_string())
            .unwrap_or_default()
    }

    /// Render an arbitrary recipient string as E.164 with a leading `+`,
    /// stripping any `@server` JID suffix the caller passed in.
    fn normalize_phone(&self, phone: &str) -> String {
        let trimmed = phone.trim();
        let user_part = trimmed
            .split_once('@')
            .map(|(user, _)| user)
            .unwrap_or(trimmed);
        let normalized_user = user_part.trim_start_matches('+');
        format!("+{normalized_user}")
    }

    /// Who sent an inbound message, as the allow-list, the reply target and
    /// the host see it.
    ///
    /// A phone-addressed sender is its E.164 number. A LID-addressed sender
    /// (WhatsApp's privacy identifier, `@lid`) is the phone that came with
    /// the message (`sender_alt`) when there is one, else the LID JID itself
    /// (`<lid>@lid`): never the LID dressed as a phone, which a reply would
    /// send to a phone JID that is not this person.
    ///
    /// The second value is every form `allowed_numbers` may name for this
    /// sender, including the `+<lid>` form earlier builds compared, so an
    /// existing allow-list keeps matching.
    fn sender_identity(
        sender: &whatsapp_rust::Jid,
        sender_alt: Option<&whatsapp_rust::Jid>,
    ) -> (String, Vec<String>) {
        let as_phone = |user: &str| {
            if user.starts_with('+') {
                user.to_string()
            } else {
                format!("+{user}")
            }
        };
        if !sender.server.is_lid_family() {
            let phone = as_phone(sender.user.as_str());
            return (phone.clone(), vec![phone]);
        }
        let legacy = as_phone(sender.user.as_str());
        match sender_alt.filter(|alt| alt.server.is_pn_family()) {
            Some(alt) => {
                let phone = as_phone(alt.user.as_str());
                (phone.clone(), vec![phone, legacy])
            }
            None => (
                format!("{}@{}", sender.user, sender.server.as_str()),
                vec![legacy],
            ),
        }
    }

    /// Convert a recipient (full JID like `12345@s.whatsapp.net` or an E.164
    /// number like `+1234567890`) into a `whatsapp-rust` JID.
    fn recipient_to_jid(&self, recipient: &str) -> Result<whatsapp_rust::Jid> {
        let trimmed = recipient.trim();
        if trimmed.is_empty() {
            anyhow::bail!("Recipient cannot be empty");
        }

        if trimmed.contains('@') {
            return trimmed
                .parse::<whatsapp_rust::Jid>()
                .map_err(|e| anyhow!("Invalid WhatsApp JID `{trimmed}`: {e}"));
        }

        let digits: String = trimmed.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            anyhow::bail!("Recipient `{trimmed}` does not contain a valid phone number");
        }

        Ok(whatsapp_rust::Jid::pn(digits))
    }
}

#[cfg(feature = "whatsapp-web")]
impl WhatsAppWebChannel {
    /// The production WebSocket transport, aimed at `ws_url` when one is
    /// configured (`WHATSAPP_WS_URL`) and at WhatsApp's default endpoint
    /// otherwise. Building it opens no connection.
    fn websocket_transport(
        ws_url: Option<String>,
    ) -> whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory {
        let mut transport_factory =
            whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory::new();
        if let Some(ws_url) = ws_url {
            transport_factory = transport_factory.with_url(ws_url);
        }
        transport_factory
    }

    /// The live outbound client, or the "not connected" error every outbound
    /// path reports before the bot is built.
    fn connected_client(&self) -> Result<Arc<dyn OutboundClient>> {
        let client = self.client.lock().clone();
        let Some(client) = client else {
            anyhow::bail!("WhatsApp Web client not connected. Initialize the bot first.");
        };
        Ok(client)
    }

    /// The state the bot's event handler shares with this channel.
    fn inbound_state(
        &self,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
        typed_tx: Option<tokio::sync::mpsc::Sender<TypedIngressDelivery>>,
    ) -> InboundState {
        InboundState {
            tx,
            typed_tx,
            allowed_numbers: self.allowed_numbers.clone(),
            connected: Arc::clone(&self.connected),
            allowed_groups: Arc::clone(&self.allowed_groups),
            events: self.events.clone(),
            media_repository: self.media_repository.clone(),
            voice_media_store: self.voice_media_store.clone(),
            client: Arc::clone(&self.client),
            seen: Arc::clone(&self.seen),
            typed_delivery: Arc::clone(&self.typed_delivery),
            authors: Arc::clone(&self.authors),
            identity: Arc::clone(&self.identity),
        }
    }

    /// Body of [`ChannelAdapter::start`](crate::channel::ChannelAdapter::start) over an injected network stack: the
    /// transport the bot connects through and the HTTP client it fetches the
    /// app version with. Production passes the WebSocket transport and the
    /// ureq client; tests pass offline doubles.
    #[cfg(test)]
    async fn listen_with<T, H>(
        &self,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
        transport_factory: T,
        http_client: H,
    ) -> Result<()>
    where
        T: whatsapp_rust::transport::TransportFactory + 'static,
        H: whatsapp_rust::http::HttpClient + 'static,
    {
        self.listen_with_ingress(tx, transport_factory, http_client, None, None)
            .await
    }

    async fn listen_with_ingress<T, H>(
        &self,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
        transport_factory: T,
        http_client: H,
        typed_sink: Option<&(dyn crate::channel::ChannelInboundSink + Send + Sync)>,
        mut cancellation: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<()>
    where
        T: whatsapp_rust::transport::TransportFactory + 'static,
        H: whatsapp_rust::http::HttpClient + 'static,
    {
        *self.tx.lock() = Some(tx.clone());
        // Release the sender when the listener ends or is aborted, so the
        // module's inbound forwarder can finish during shutdown.
        let _release = crate::providers::whatsapp_shared::window::ClearOnDrop(&self.tx);

        use whatsapp_rust::TokioRuntime;
        use whatsapp_rust::bot::Bot;
        use whatsapp_rust::pair_code::PairCodeOptions;

        tracing::info!(
            "WhatsApp Web channel starting (session: {})",
            self.session_path
        );

        let backend = self.session_backend().await;

        let (typed_tx, mut typed_rx) = if typed_sink.is_some() {
            let (sender, receiver) = tokio::sync::mpsc::channel(256);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let state = self.inbound_state(tx, typed_tx);

        let mut builder = Bot::builder()
            .with_backend_arc(backend)
            .with_transport_factory(transport_factory)
            .with_http_client(http_client)
            .with_runtime(TokioRuntime)
            .on_event(move |event, _client| state.clone().handle_owned(event));

        if let Some(ref phone) = self.pair_phone {
            tracing::info!("WhatsApp Web: pair-code flow enabled for configured phone number");
            builder = builder.with_pair_code(PairCodeOptions {
                phone_number: phone.clone(),
                custom_code: self.pair_code.clone(),
                ..Default::default()
            });
        } else if self.pair_code.is_some() {
            tracing::warn!(
                "WhatsApp Web: pair_code is set but pair_phone is missing; pair code config is ignored"
            );
        }

        let bot = builder.build().await?;
        *self.client.lock() = Some(bot.client());

        let bot_handle = bot.spawn();
        *self.bot_handle.lock() = Some(bot_handle);

        // Wire into the host lifecycle registry so SIGTERM and SIGINT both
        // trigger a coordinated tear-down through the host runtime. The
        // previous `tokio::signal::ctrl_c()` path silently ignored SIGTERM
        // and bypassed the registered cleanup hooks the rest of the process
        // uses.
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        if let Some(reg) = &self.lifecycle {
            let bot_handle_for_hook = Arc::clone(&self.bot_handle);
            let connected_for_hook = Arc::clone(&self.connected);
            let client_for_hook = Arc::clone(&self.client);
            let notify_for_hook = Arc::clone(&shutdown_notify);
            reg.register_shutdown(
                "whatsapp_web",
                Box::new(move || {
                    let bot_handle = Arc::clone(&bot_handle_for_hook);
                    let connected = Arc::clone(&connected_for_hook);
                    let client = Arc::clone(&client_for_hook);
                    let notify = Arc::clone(&notify_for_hook);
                    Box::pin(async move {
                        tracing::info!(
                            "[whatsapp_web] graceful shutdown hook firing — aborting bot"
                        );
                        connected.store(false, Ordering::Release);
                        *client.lock() = None;
                        let handle = bot_handle.lock().take();
                        if let Some(mut handle) = handle {
                            handle.abort();
                            let _ = (&mut handle).await;
                        }
                        notify.notify_waiters();
                    })
                }),
            );
        } else {
            tracing::warn!(
                "[whatsapp_web] no host LifecycleRegistry provided; graceful shutdown \
                 teardown will not be registered. The channel will still park until \
                 process exit."
            );
        }

        'listener: loop {
            tokio::select! {
                biased;
                _ = shutdown_notify.notified() => break,
                _ = async {
                    if let Some(cancellation) = cancellation.as_mut() {
                        let _ = cancellation.wait_for(|value| *value).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => break,
                delivery = async {
                    match typed_rx.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let Some(delivery) = delivery else { break; };
                    let Some(sink) = typed_sink else {
                        let _ = delivery.accepted.send(Err(crate::channel::ChannelSendError {
                            kind: crate::channel::SendErrorKind::Forbidden,
                            message: "WhatsApp Web typed ingress has no host sink".into(),
                            ..Default::default()
                        }));
                        continue;
                    };
                    let Some(cancellation) = cancellation.as_mut() else {
                        let _ = delivery.accepted.send(Err(crate::channel::ChannelSendError {
                            kind: crate::channel::SendErrorKind::Forbidden,
                            message: "WhatsApp Web typed listener has no cancellation owner".into(),
                            ..Default::default()
                        }));
                        continue;
                    };
                    match deliver_typed_ingress(
                        sink,
                        &delivery.ingress,
                        shutdown_notify.as_ref(),
                        cancellation,
                    )
                    .await
                    {
                        Ok(()) => {
                            let _ = delivery.accepted.send(Ok(()));
                        }
                        Err(TypedDeliveryFailure::Cancelled) => {
                            let _ = delivery.accepted.send(Err(crate::channel::ChannelSendError {
                                kind: crate::channel::SendErrorKind::Transient,
                                message: "WhatsApp Web listener stopped before ingress acceptance".into(),
                                retryable: true,
                                ..Default::default()
                            }));
                            break 'listener;
                        }
                        Err(TypedDeliveryFailure::Rejected(error)) => {
                            let _ = delivery.accepted.send(Err(error.clone()));
                            self.stop_bot().await;
                            return Err(anyhow!("WhatsApp Web typed ingress was rejected: {}", error.message));
                        }
                    }
                }
            }
        }
        if cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            self.stop_bot().await;
        }
        tracing::info!("WhatsApp Web channel exited via shared shutdown");

        Ok(())
    }

    async fn stop_bot(&self) {
        self.connected.store(false, Ordering::Release);
        *self.client.lock() = None;
        let handle = self.bot_handle.lock().take();
        if let Some(mut handle) = handle {
            handle.abort();
            let _ = (&mut handle).await;
        }
    }

    async fn start_typed_with<T, H>(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        transport_factory: T,
        http_client: H,
    ) -> Result<()>
    where
        T: whatsapp_rust::transport::TransportFactory + 'static,
        H: whatsapp_rust::http::HttpClient + 'static,
    {
        self.typed_listener_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| anyhow!("WhatsApp Web listener is already running"))?;
        let _ownership = TypedListenerOwnership {
            active: Arc::clone(&self.typed_listener_active),
            bot_handle: Arc::clone(&self.bot_handle),
            client: Arc::clone(&self.client),
            connected: Arc::clone(&self.connected),
            tx: Arc::clone(&self.tx),
        };
        self.listener_cancel.send_replace(false);
        self.listen_typed_with(
            sink,
            self.listener_cancel.subscribe(),
            transport_factory,
            http_client,
        )
        .await
    }

    async fn listen_typed_with<T, H>(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        cancellation: tokio::sync::watch::Receiver<bool>,
        transport_factory: T,
        http_client: H,
    ) -> Result<()>
    where
        T: whatsapp_rust::transport::TransportFactory + 'static,
        H: whatsapp_rust::http::HttpClient + 'static,
    {
        let (tx, _legacy_rx) = tokio::sync::mpsc::channel(1);
        let mut stopped = cancellation.clone();
        let mut listener = Box::pin(self.listen_with_ingress(
            tx,
            transport_factory,
            http_client,
            Some(sink),
            Some(cancellation),
        ));
        tokio::select! {
            biased;
            result = &mut listener => result,
            _ = async { let _ = stopped.wait_for(|value| *value).await; } => {
                self.stop_bot().await;
                Ok(())
            }
        }
    }
}

#[cfg(feature = "whatsapp-web")]
impl WhatsAppWebChannel {
    fn provider_media_account_identity(&self) -> Option<String> {
        self.identity.current()
    }

    async fn mark_read_provider(
        &self,
        conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
    ) -> Result<()> {
        self.read_v2(conversation, provider_message_id)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn start_typing_provider(&self, recipient: &str) -> Result<()> {
        if !self.should_allow_outbound(recipient) {
            tracing::warn!(
                "WhatsApp Web: typing target {} not in allowed list",
                Self::redact_recipient(recipient)
            );
            return Ok(());
        }

        let to = self.recipient_to_jid(recipient).map_err(|_| {
            anyhow::Error::new(
                crate::providers::whatsapp_shared::error::WaFailure::bad_format(
                    "WhatsApp recipient address is invalid",
                ),
            )
        })?;
        let client = self.connected_client().map_err(|_| {
            anyhow::Error::new(
                crate::providers::whatsapp_shared::error::WaFailure::rejected(
                    "WhatsApp Web client is not connected",
                ),
            )
        })?;
        client
            .send_composing(&to)
            .await
            .map_err(|e| anyhow!("Failed to send typing state (composing): {e}"))?;

        tracing::debug!("WhatsApp Web: start typing for {}", recipient);
        Ok(())
    }

    async fn stop_typing_provider(&self, recipient: &str) -> Result<()> {
        if !self.should_allow_outbound(recipient) {
            tracing::warn!(
                "WhatsApp Web: typing target {} not in allowed list",
                Self::redact_recipient(recipient)
            );
            return Ok(());
        }

        let to = self.recipient_to_jid(recipient).map_err(|_| {
            anyhow::Error::new(
                crate::providers::whatsapp_shared::error::WaFailure::bad_format(
                    "WhatsApp recipient address is invalid",
                ),
            )
        })?;
        let client = self.connected_client().map_err(|_| {
            anyhow::Error::new(
                crate::providers::whatsapp_shared::error::WaFailure::rejected(
                    "WhatsApp Web client is not connected",
                ),
            )
        })?;
        client
            .send_paused(&to)
            .await
            .map_err(|e| anyhow!("Failed to send typing state (paused): {e}"))?;

        tracing::debug!("WhatsApp Web: stop typing for {}", recipient);
        Ok(())
    }
}

/// The failure class of an error produced by the Web adapter.
#[cfg(feature = "whatsapp-web")]
fn failure_of(error: &anyhow::Error) -> WaFailure {
    error
        .downcast_ref::<WaFailure>()
        .cloned()
        .unwrap_or_else(|| WaFailure::uncertain("WhatsApp Web operation outcome is uncertain"))
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::ChannelAdapter for WhatsAppWebChannel {
    fn descriptor(&self) -> tinychannels_bus::channel::ChannelDescriptor {
        tinychannels_bus::channel::ChannelDescriptor {
            id: "whatsapp".into(),
            display_name: "WhatsApp".into(),
            metadata: std::collections::BTreeMap::from([
                ("adapterMode".into(), "native-v2".into()),
                ("backend".into(), "web".into()),
            ]),
            ..Default::default()
        }
    }

    fn media_account_identity(&self) -> Option<String> {
        self.provider_media_account_identity()
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        capabilities::declared(self)
    }

    async fn start(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> std::result::Result<(), crate::channel::ChannelSendError> {
        self.start_typed_with(
            sink,
            Self::websocket_transport(std::env::var("WHATSAPP_WS_URL").ok()),
            whatsapp_rust_ureq_http_client::UreqHttpClient::new(),
        )
        .await
        .map_err(|error| crate::channel::ChannelSendError {
            kind: crate::channel::SendErrorKind::Unknown,
            message: error.to_string(),
            retryable: true,
            ..Default::default()
        })
    }

    async fn stop(&self) -> std::result::Result<(), crate::channel::ChannelSendError> {
        self.listener_cancel.send_replace(true);
        self.stop_bot().await;
        Ok(())
    }

    async fn send(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> std::result::Result<crate::channel::MessageReceipt, crate::channel::ChannelSendError> {
        let message = tinychannels_bus::ChannelOutboundMessage::from_intent(&intent);
        self.send_v2(&message)
            .await
            .map(|outcome| outcome.receipt)
            .map_err(WaFailure::into_send_error)
    }

    async fn mark_read(
        &self,
        conversation: &tinychannels_bus::ConversationRef,
        provider_message_id: &str,
    ) -> std::result::Result<(), crate::channel::ChannelSendError> {
        self.mark_read_provider(conversation, provider_message_id)
            .await
            .map_err(|error| failure_of(&error).into_send_error())
    }

    async fn status(
        &self,
    ) -> std::result::Result<
        tinychannels_runtime::controllers::ChannelAccountSnapshot,
        crate::channel::ChannelSendError,
    > {
        let connected = self.connected.load(Ordering::Acquire);
        Ok(tinychannels_bus::controllers::ChannelAccountSnapshot {
            channel_id: "whatsapp".into(),
            states: vec![
                tinychannels_bus::controllers::ChannelAccountState::Configured,
                tinychannels_bus::controllers::ChannelAccountState::Enabled,
            ],
            health_state: Some(if connected { "healthy" } else { "unhealthy" }.into()),
            ..Default::default()
        })
    }
}

// Every test is `#[cfg(feature = "whatsapp-web")]`, so gate the whole module
// on the feature to avoid an unused `use super::*` in the feature-off build.
#[cfg(all(test, feature = "whatsapp-web"))]
#[path = "conformance_tests.rs"]
mod conformance;

#[cfg(all(test, feature = "whatsapp-web"))]
#[path = "fakes_tests.rs"]
pub(crate) mod fakes;

#[cfg(all(test, feature = "whatsapp-web"))]
#[path = "mod_tests.rs"]
mod tests;
