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
//! is set. The Cloud API channel ([`super::whatsapp`]) is used when
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

use crate::traits::{Channel, ChannelMessage, SendMessage};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

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

/// Outbound operations the channel performs on a live WhatsApp session.
///
/// Implemented by [`whatsapp_rust::Client`]; the seam lets the outbound gate,
/// recipient parsing and error mapping run against a test double instead of a
/// connected session.
#[cfg(feature = "whatsapp-web")]
#[async_trait]
trait OutboundClient: Send + Sync {
    /// Send a plain-text message to `to`.
    async fn send_text(&self, to: whatsapp_rust::Jid, text: String) -> Result<()>;
    /// Publish the "composing" chat state to `to`.
    async fn send_composing(&self, to: &whatsapp_rust::Jid) -> Result<()>;
    /// Publish the "paused" chat state to `to`.
    async fn send_paused(&self, to: &whatsapp_rust::Jid) -> Result<()>;
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl OutboundClient for whatsapp_rust::Client {
    async fn send_text(&self, to: whatsapp_rust::Jid, text: String) -> Result<()> {
        let outgoing = whatsapp_rust::waproto::whatsapp::Message {
            conversation: Some(text),
            ..Default::default()
        };
        self.send_message(to, outgoing).await?;
        Ok(())
    }

    async fn send_composing(&self, to: &whatsapp_rust::Jid) -> Result<()> {
        Ok(self.chatstate().send_composing(to).await?)
    }

    async fn send_paused(&self, to: &whatsapp_rust::Jid) -> Result<()> {
        Ok(self.chatstate().send_paused(to).await?)
    }
}

/// State the bot's event handler shares with the channel: the inbound sink,
/// the allow-list, the liveness flag and the group-provenance set.
///
/// Holding it apart from the bot lets inbound events be translated and gated
/// without a WhatsApp connection.
#[cfg(feature = "whatsapp-web")]
#[derive(Clone)]
struct InboundState {
    tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    allowed_numbers: Vec<String>,
    connected: Arc<AtomicBool>,
    allowed_groups: Arc<Mutex<HashSet<String>>>,
}

#[cfg(feature = "whatsapp-web")]
impl InboundState {
    /// Owned entry point for the bot's `on_event` closure.
    async fn handle_owned(self, event: Arc<wacore::types::events::Event>) {
        self.handle_event(&event).await;
    }

    /// Whether any form of the sender's identity is on the allow-list.
    /// Empty list ⇒ allow-all (matches Cloud API behaviour).
    fn is_sender_allowed(&self, allow_forms: &[String]) -> bool {
        self.allowed_numbers.is_empty()
            || self
                .allowed_numbers
                .iter()
                .any(|n| n == "*" || allow_forms.contains(n))
    }

    /// React to one upstream bot event.
    async fn handle_event(&self, event: &wacore::types::events::Event) {
        use wacore::types::events::Event;
        match event {
            Event::Messages(batch) => {
                for inbound in WhatsAppWebChannel::non_self_echoes(
                    &batch.messages,
                    |inbound: &wacore::types::events::InboundMessage| {
                        inbound.info.source.is_from_me
                    },
                ) {
                    self.handle_inbound(inbound).await;
                }
            }
            Event::Connected(_) => {
                self.connected.store(true, Ordering::Release);
                tracing::info!("✅ WhatsApp Web connected successfully!");
            }
            Event::LoggedOut(_) => {
                self.connected.store(false, Ordering::Release);
                tracing::warn!("❌ WhatsApp Web was logged out!");
            }
            Event::StreamError(stream_error) => {
                self.connected.store(false, Ordering::Release);
                tracing::error!("❌ WhatsApp Web stream error: {:?}", stream_error);
            }
            // The pair code and QR payload are short-lived link
            // credentials — anyone reading the logs while they
            // are valid can hijack the session. Surface only a
            // non-sensitive notice; the raw payload is never
            // logged at any level. Surfacing the code to the
            // user is the responsibility of an upstream UX
            // path (e.g. a JSON-RPC event the frontend renders).
            Event::PairingCode { .. } => {
                tracing::info!(
                    "🔑 WhatsApp Web pair code received. Enter the code shown on \
                     your linking surface into WhatsApp > Linked Devices."
                );
            }
            Event::PairingQrCode { .. } => {
                tracing::info!(
                    "📱 WhatsApp Web QR code received. Render via QR generator and \
                     scan with WhatsApp > Linked Devices."
                );
            }
            _ => {}
        }
    }

    /// Translate one non-self-echo inbound message and forward it when the
    /// sender is allowed.
    async fn handle_inbound(&self, inbound: &wacore::types::events::InboundMessage) {
        let msg = &inbound.message;
        let info = &inbound.info;
        let text = WhatsAppWebChannel::extract_message_text(
            msg.conversation.as_deref(),
            msg.extended_text_message.text.as_deref(),
        );

        // Sender JID can use either the legacy `s.whatsapp.net`
        // server (phone-number addressing) or the newer `lid`
        // server (privacy-preserving identifier): a phone in
        // E.164, or a LID kept as a JID (`sender_identity`).
        let (normalized, allow_forms) = WhatsAppWebChannel::sender_identity(
            &info.source.sender,
            info.source.sender_alt.as_ref(),
        );
        // The name the sender set for themselves, if any.
        let sender_name = Some(info.push_name.trim())
            .filter(|name| !name.is_empty())
            .map(str::to_string);
        let chat = info.source.chat.to_string();
        let reply_target = WhatsAppWebChannel::compute_reply_target(&chat, &normalized);

        // Routine logs only carry coarse metadata — no raw
        // sender identifier, no message body — so PII does
        // not leak into application logs at any level.
        // For DM chats `chat` is `<phone>@s.whatsapp.net`,
        // which still carries the participant's phone
        // number. Redact the user part so the routine
        // log keeps only the server suffix (DM vs group)
        // and a coarse identifier tail.
        tracing::info!(
            "📨 WhatsApp inbound: chat={} sender={} text_len={}",
            WhatsAppWebChannel::redact_recipient(&chat),
            WhatsAppWebChannel::redact_recipient(&normalized),
            text.len()
        );

        if !self.is_sender_allowed(&allow_forms) {
            tracing::warn!(
                "WhatsApp Web: message from {} not in allowed list",
                WhatsAppWebChannel::redact_recipient(&normalized)
            );
            return;
        }

        // Record group provenance: this group has had at
        // least one allow-listed participant message in,
        // so subsequent outbound replies into the same
        // group are legitimate. Outbound to groups
        // without provenance is rejected by
        // `should_allow_outbound`.
        if WhatsAppWebChannel::is_group_jid(&chat) {
            self.allowed_groups.lock().insert(chat.clone());
        }
        let message = ChannelMessage {
            id: uuid::Uuid::new_v4().to_string(),
            channel: "whatsapp".to_string(),
            sender: normalized,
            reply_target,
            content: text,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            topic_id: None,
            reply_to_id: None,
            thread_ts: None,
            sender_name,
        };
        if let Err(e) = self.tx.send(message).await {
            tracing::error!("Failed to forward WhatsApp message to channel: {}", e);
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
    /// Sink for inbound `ChannelMessage`s. Populated when [`Channel::listen`]
    /// is called and shared with the event-handler closure.
    tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<ChannelMessage>>>>,
    /// Host lifecycle capability used to register graceful-shutdown teardown.
    /// When absent, teardown is not wired into the host and `listen` still
    /// parks on the internal notify gate.
    lifecycle: Option<Arc<dyn crate::host::LifecycleRegistry>>,
    /// Host storage for the durable session. When absent the session lives in
    /// memory and the device must be re-linked after a restart.
    storage: Option<Arc<dyn crate::host::StorageProvider>>,
}

#[cfg(feature = "whatsapp-web")]
impl WhatsAppWebChannel {
    fn non_self_echoes<T>(
        messages: &[T],
        is_from_me: impl Fn(&T) -> bool,
    ) -> impl Iterator<Item = &T> {
        messages.iter().filter(move |message| !is_from_me(message))
    }

    /// Construct a channel. The bot does not connect until [`Channel::listen`]
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
            client: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            allowed_groups: Arc::new(Mutex::new(HashSet::new())),
            tx: Arc::new(Mutex::new(None)),
            lifecycle: None,
            storage: None,
        }
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

    /// Body of [`Channel::listen`] over an injected network stack: the
    /// transport the bot connects through and the HTTP client it fetches the
    /// app version with. Production passes the WebSocket transport and the
    /// ureq client; tests pass offline doubles.
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
        *self.tx.lock() = Some(tx.clone());

        use whatsapp_rust::TokioRuntime;
        use whatsapp_rust::bot::Bot;
        use whatsapp_rust::pair_code::PairCodeOptions;

        tracing::info!(
            "WhatsApp Web channel starting (session: {})",
            self.session_path
        );

        let backend = self.session_backend().await;

        let state = InboundState {
            tx,
            allowed_numbers: self.allowed_numbers.clone(),
            connected: Arc::clone(&self.connected),
            allowed_groups: Arc::clone(&self.allowed_groups),
        };

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

        shutdown_notify.notified().await;
        tracing::info!("WhatsApp Web channel exited via shared shutdown");

        Ok(())
    }
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl Channel for WhatsAppWebChannel {
    fn name(&self) -> &str {
        "whatsapp"
    }

    fn supports_remote_control(&self) -> bool {
        true
    }

    fn supports_chat_approvals(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> Result<()> {
        let client = self.connected_client()?;

        if !self.should_allow_outbound(&message.recipient) {
            tracing::warn!(
                "WhatsApp Web: recipient {} not in allowed list",
                Self::redact_recipient(&message.recipient)
            );
            return Ok(());
        }

        let to = self.recipient_to_jid(&message.recipient)?;
        client.send_text(to, message.content.clone()).await?;
        tracing::debug!("WhatsApp Web: sent message to {}", message.recipient);
        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> Result<()> {
        let transport = Self::websocket_transport(std::env::var("WHATSAPP_WS_URL").ok());
        let http_client = whatsapp_rust_ureq_http_client::UreqHttpClient::new();
        self.listen_with(tx, transport, http_client).await
    }

    async fn health_check(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    async fn start_typing(&self, recipient: &str) -> Result<()> {
        let client = self.connected_client()?;

        if !self.should_allow_outbound(recipient) {
            tracing::warn!(
                "WhatsApp Web: typing target {} not in allowed list",
                Self::redact_recipient(recipient)
            );
            return Ok(());
        }

        let to = self.recipient_to_jid(recipient)?;
        client
            .send_composing(&to)
            .await
            .map_err(|e| anyhow!("Failed to send typing state (composing): {e}"))?;

        tracing::debug!("WhatsApp Web: start typing for {}", recipient);
        Ok(())
    }

    async fn stop_typing(&self, recipient: &str) -> Result<()> {
        let client = self.connected_client()?;

        if !self.should_allow_outbound(recipient) {
            tracing::warn!(
                "WhatsApp Web: typing target {} not in allowed list",
                Self::redact_recipient(recipient)
            );
            return Ok(());
        }

        let to = self.recipient_to_jid(recipient)?;
        client
            .send_paused(&to)
            .await
            .map_err(|e| anyhow!("Failed to send typing state (paused): {e}"))?;

        tracing::debug!("WhatsApp Web: stop typing for {}", recipient);
        Ok(())
    }
}

// Stub implementation when the feature is not enabled. Keeps the public ctor
// signature compatible so channel startup compiles unchanged.
#[cfg(not(feature = "whatsapp-web"))]
pub struct WhatsAppWebChannel {
    _private: (),
    /// Retained so `startup` can call `.with_lifecycle(...)` unconditionally
    /// regardless of feature flags.
    lifecycle: Option<Arc<dyn crate::host::LifecycleRegistry>>,
}

#[cfg(not(feature = "whatsapp-web"))]
impl WhatsAppWebChannel {
    pub fn new(
        _session_path: String,
        _pair_phone: Option<String>,
        _pair_code: Option<String>,
        _allowed_numbers: Vec<String>,
    ) -> Self {
        Self {
            _private: (),
            lifecycle: None,
        }
    }

    /// No-op host lifecycle wiring for the disabled-feature stub. Kept so
    /// callers can invoke `.with_lifecycle(...)` unconditionally.
    pub fn with_lifecycle(
        mut self,
        lifecycle: std::sync::Arc<dyn crate::host::LifecycleRegistry>,
    ) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }
}

#[cfg(not(feature = "whatsapp-web"))]
#[async_trait]
impl Channel for WhatsAppWebChannel {
    fn name(&self) -> &str {
        "whatsapp"
    }

    async fn send(&self, _message: &SendMessage) -> Result<()> {
        anyhow::bail!(
            "WhatsApp Web channel requires the 'whatsapp-web' feature. \
            Enable with: cargo build --features whatsapp-web"
        );
    }

    async fn listen(&self, _tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> Result<()> {
        anyhow::bail!(
            "WhatsApp Web channel requires the 'whatsapp-web' feature. \
            Enable with: cargo build --features whatsapp-web"
        );
    }

    async fn health_check(&self) -> bool {
        false
    }

    async fn start_typing(&self, _recipient: &str) -> Result<()> {
        anyhow::bail!(
            "WhatsApp Web channel requires the 'whatsapp-web' feature. \
            Enable with: cargo build --features whatsapp-web"
        );
    }

    async fn stop_typing(&self, _recipient: &str) -> Result<()> {
        anyhow::bail!(
            "WhatsApp Web channel requires the 'whatsapp-web' feature. \
            Enable with: cargo build --features whatsapp-web"
        );
    }
}

// Every test is `#[cfg(feature = "whatsapp-web")]`, so gate the whole module
// on the feature to avoid an unused `use super::*` in the feature-off build.
#[cfg(all(test, feature = "whatsapp-web"))]
#[path = "whatsapp_web_tests.rs"]
mod tests;
