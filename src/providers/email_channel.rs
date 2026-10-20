#![allow(clippy::uninlined_format_args)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::trim_split_whitespace)]
#![allow(clippy::doc_link_with_quotes)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unnecessary_map_or)]

use anyhow::Result;
#[cfg(feature = "email")]
use anyhow::anyhow;
#[cfg(feature = "email")]
use async_imap::Session;
#[cfg(feature = "email")]
use async_imap::extensions::idle::IdleResponse;
#[cfg(feature = "email")]
use async_imap::types::Fetch;
#[cfg(feature = "email")]
use async_trait::async_trait;
#[cfg(feature = "email")]
use futures::TryStreamExt;
use lettre::message::{Attachment, MultiPart, SinglePart, header::ContentType};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
#[cfg(feature = "email")]
use mail_parser::{MessageParser, MimeHeaders};
#[cfg(feature = "email")]
use rustls::{ClientConfig, RootCertStore};
#[cfg(feature = "email")]
use rustls_pki_types::DnsName;
#[cfg(feature = "email")]
use std::collections::HashSet;
#[cfg(feature = "email")]
use std::sync::Arc;
#[cfg(feature = "email")]
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicU64, Ordering},
};
#[cfg(feature = "email")]
use std::time::Duration;
#[cfg(feature = "email")]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "email")]
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "email")]
use tokio::net::TcpStream;
#[cfg(feature = "email")]
use tokio::sync::Mutex;
#[cfg(feature = "email")]
use tokio::sync::mpsc;
#[cfg(feature = "email")]
use tokio::sync::watch;
#[cfg(feature = "email")]
use tokio::time::{sleep, timeout};
#[cfg(feature = "email")]
use tokio_rustls::TlsConnector;
#[cfg(feature = "email")]
use tokio_rustls::client::TlsStream;
use tracing::info;
#[cfg(feature = "email")]
use tracing::{debug, error, warn};
#[cfg(feature = "email")]
use uuid::Uuid;

#[cfg(feature = "email")]
mod capabilities;
#[cfg(feature = "email")]
mod inbound;
#[cfg(feature = "email")]
mod native;

#[cfg(feature = "email")]
use crate::channel::{ChannelInboundSink, ChannelSendError};
pub use crate::config::EmailConfig;
#[cfg(feature = "email")]
use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::mail::{ImapCredentials, MailSecret, SmtpCredentials, SmtpSecurity};
#[cfg(feature = "email")]
use crate::providers::mail::{LettreMailSender, MailCredentials, MailSender, OutboundEmail};
#[cfg(feature = "email")]
use crate::providers::native::HostPorts as NativeState;
#[cfg(feature = "email")]
use crate::traits::{Channel, ChannelMessage, SendMessage};

#[cfg(feature = "email")]
type ImapSession = Session<TlsStream<TcpStream>>;

/// The bounds `async-imap` (runtime-tokio) needs of a connected stream, so the
/// IMAP exchange after the TLS handshake is testable over plain TCP.
#[cfg(feature = "email")]
trait ImapStream: AsyncRead + AsyncWrite + Unpin + std::fmt::Debug + Send {}
#[cfg(feature = "email")]
impl<S: AsyncRead + AsyncWrite + Unpin + std::fmt::Debug + Send> ImapStream for S {}

/// Budget for one [`Channel::health_check`] IMAP login probe.
#[cfg(feature = "email")]
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// Email channel — IMAP IDLE for instant push notifications, SMTP for outbound
pub struct EmailChannel {
    pub config: EmailConfig,
    /// Dedupe set for IMAP IDLE, which can re-report a message across
    /// re-establishes. A send-only build never opens a mailbox, so the field
    /// would be dead weight and a `never read` denial there.
    #[cfg(feature = "email")]
    seen_messages: Arc<Mutex<HashSet<String>>>,
    /// First delay before reconnecting a failed IMAP session; doubles up to
    /// [`Self::max_reconnect_backoff`].
    #[cfg(feature = "email")]
    reconnect_backoff: Duration,
    #[cfg(feature = "email")]
    max_reconnect_backoff: Duration,
    /// Host ports for the v2 adapter surface.
    #[cfg(feature = "email")]
    native: NativeState,
    /// Subjects of inbound mail by message id, so a reply keeps its thread subject.
    #[cfg(feature = "email")]
    subjects: native::Subjects,
    #[cfg(feature = "email")]
    listener_cancel: ListenerCancellationSlot,
    #[cfg(feature = "email")]
    listener_generation: Arc<AtomicU64>,
}

#[cfg(feature = "email")]
type ListenerCancellationSlot = Arc<StdMutex<Option<(u64, watch::Sender<bool>)>>>;

#[cfg(feature = "email")]
struct ListenerRegistration {
    slot: ListenerCancellationSlot,
    generation: u64,
}

#[cfg(feature = "email")]
impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        let mut active = self.slot.lock().unwrap_or_else(|error| error.into_inner());
        if active
            .as_ref()
            .is_some_and(|(generation, _)| *generation == self.generation)
        {
            *active = None;
        }
    }
}

#[cfg(feature = "email")]
enum EmailDelivery<'a> {
    Legacy(&'a mpsc::Sender<ChannelMessage>),
    Typed(&'a (dyn ChannelInboundSink + Send + Sync)),
}

#[cfg(feature = "email")]
#[derive(Debug, thiserror::Error)]
#[error("Email inbound sink rejected an event")]
struct InboundSinkRejected;

impl EmailChannel {
    pub fn new(config: EmailConfig) -> Self {
        Self {
            config,
            #[cfg(feature = "email")]
            seen_messages: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(feature = "email")]
            reconnect_backoff: Duration::from_secs(1),
            #[cfg(feature = "email")]
            max_reconnect_backoff: Duration::from_secs(60),
            #[cfg(feature = "email")]
            native: NativeState::default(),
            #[cfg(feature = "email")]
            subjects: native::Subjects::default(),
            #[cfg(feature = "email")]
            listener_cancel: Arc::new(StdMutex::new(None)),
            #[cfg(feature = "email")]
            listener_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Scoped media storage; enables typed attachments in both directions.
    #[cfg(feature = "email")]
    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Durable voice storage; enables voice attachments in both directions.
    #[cfg(feature = "email")]
    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Host speech-to-text for inbound voice attachments.
    #[cfg(feature = "email")]
    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    /// Enable shared final-turn synthesis when the host provides TTS, a codec
    /// and durable voice storage.
    #[cfg(feature = "email")]
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.native.voice_synthesis = enabled;
        self
    }

    /// Ordered ingress for typed inbound messages.
    #[cfg(feature = "email")]
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// Check if a sender email is in the allowlist
    pub fn is_sender_allowed(&self, email: &str) -> bool {
        if self.config.allowed_senders.is_empty() {
            return false; // Empty = deny all
        }
        if self.config.allowed_senders.iter().any(|a| a == "*") {
            return true; // Wildcard = allow all
        }
        let email_lower = email.to_lowercase();
        self.config.allowed_senders.iter().any(|allowed| {
            if allowed.starts_with('@') {
                // Domain match with @ prefix: "@example.com"
                email_lower.ends_with(&allowed.to_lowercase())
            } else if allowed.contains('@') {
                // Full email address match
                allowed.eq_ignore_ascii_case(email)
            } else {
                // Domain match without @ prefix: "example.com"
                email_lower.ends_with(&format!("@{}", allowed.to_lowercase()))
            }
        })
    }

    /// Strip HTML tags from content (basic)
    pub fn strip_html(html: &str) -> String {
        let mut result = String::new();
        let mut in_tag = false;
        for ch in html.chars() {
            match ch {
                '<' => in_tag = true,
                '>' => in_tag = false,
                _ if !in_tag => result.push(ch),
                _ => {}
            }
        }
        let mut normalized = String::with_capacity(result.len());
        for word in result.split_whitespace() {
            if !normalized.is_empty() {
                normalized.push(' ');
            }
            normalized.push_str(word);
        }
        normalized
    }

    /// Extract the sender address from a parsed email
    #[cfg(feature = "email")]
    fn extract_sender(parsed: &mail_parser::Message) -> String {
        parsed
            .from()
            .and_then(|addr| addr.first())
            .and_then(|a| a.address())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown".into())
    }

    /// Extract readable text from a parsed email
    #[cfg(feature = "email")]
    fn extract_text(parsed: &mail_parser::Message) -> String {
        if let Some(text) = parsed.body_text(0) {
            return text.to_string();
        }
        if let Some(html) = parsed.body_html(0) {
            return Self::strip_html(html.as_ref());
        }
        for part in parsed.attachments() {
            let part: &mail_parser::MessagePart = part;
            if let Some(ct) = MimeHeaders::content_type(part)
                && ct.ctype() == "text"
                && let Ok(text) = std::str::from_utf8(part.contents())
            {
                return text.to_string();
            }
        }
        "(no readable content)".to_string()
    }

    /// Connect to IMAP server with TLS and authenticate
    #[cfg(feature = "email")]
    async fn connect_imap(&self) -> Result<ImapSession> {
        let addr = format!("{}:{}", self.config.imap_host, self.config.imap_port);
        debug!("Connecting to IMAP server at {}", addr);

        // Connect TCP
        let tcp = TcpStream::connect(&addr).await?;

        // Establish TLS using rustls
        let certs = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.into(),
        };
        let config = ClientConfig::builder()
            .with_root_certificates(certs)
            .with_no_client_auth();
        let tls_stream: TlsConnector = Arc::new(config).into();
        let sni: DnsName = self.config.imap_host.clone().try_into()?;
        let stream = tls_stream.connect(sni.into(), tcp).await?;
        self.login_imap(stream).await
    }

    /// Log in over an already-connected stream.
    #[cfg(feature = "email")]
    async fn login_imap<S: ImapStream>(&self, stream: S) -> Result<Session<S>> {
        let client = async_imap::Client::new(stream);

        // Login
        let session = client
            .login(&self.config.username, &self.config.password)
            .await
            .map_err(|(e, _)| anyhow!("IMAP login failed: {}", e))?;

        debug!("IMAP login successful");
        Ok(session)
    }

    /// Fetch and process unseen messages from the selected mailbox
    #[cfg(feature = "email")]
    async fn fetch_unseen<S: ImapStream>(
        &self,
        session: &mut Session<S>,
        uid_validity: u32,
    ) -> Result<Vec<ParsedEmail>> {
        // Search for unseen messages
        let uids = session.uid_search("UNSEEN").await?;
        if uids.is_empty() {
            return Ok(Vec::new());
        }

        debug!("Found {} unseen messages", uids.len());

        let mut results = Vec::new();
        let uid_set: String = uids
            .iter()
            .map(|u| u.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Fetch message bodies
        let messages = session.uid_fetch(&uid_set, "UID BODY.PEEK[]").await?;
        let messages: Vec<Fetch> = messages.try_collect().await?;

        for msg in messages {
            let uid = msg.uid.unwrap_or(0);
            if let Some(body) = msg.body()
                && let Some(parsed) = MessageParser::default().parse(body)
            {
                let sender = Self::extract_sender(&parsed);
                let subject = parsed.subject().unwrap_or("(no subject)").to_string();
                let body_text = Self::extract_text(&parsed);
                let content = format!("Subject: {}\n\n{}", subject, body_text);
                let msg_id = parsed
                    .message_id()
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        let identity = format!(
                            "{}:{}:{}:{}:{}",
                            self.config.imap_host.to_ascii_lowercase(),
                            self.config.username.to_ascii_lowercase(),
                            self.config.imap_folder,
                            uid_validity,
                            uid
                        );
                        format!("email:{}", hex::encode(sha2::Sha256::digest(identity)))
                    });

                #[allow(clippy::cast_sign_loss)]
                let ts = parsed
                    .date()
                    .map(|d| {
                        let naive = chrono::NaiveDate::from_ymd_opt(
                            d.year as i32,
                            u32::from(d.month),
                            u32::from(d.day),
                        )
                        .and_then(|date| {
                            date.and_hms_opt(
                                u32::from(d.hour),
                                u32::from(d.minute),
                                u32::from(d.second),
                            )
                        });
                        naive.map_or(0, |n| n.and_utc().timestamp() as u64)
                    })
                    .unwrap_or_else(|| {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0)
                    });

                results.push(ParsedEmail {
                    _uid: uid,
                    msg_id,
                    sender,
                    content,
                    timestamp: ts,
                    raw: body.to_vec(),
                });
            }
        }

        Ok(results)
    }

    /// Run the IDLE loop, returning when a new message arrives or timeout
    /// Note: IDLE consumes the session and returns it via done()
    #[cfg(feature = "email")]
    async fn wait_for_changes<S: ImapStream>(
        &self,
        session: Session<S>,
    ) -> Result<(IdleWaitResult, Session<S>)> {
        let idle_timeout = Duration::from_secs(self.config.idle_timeout_secs);

        // Start IDLE mode - this consumes the session
        let mut idle = session.idle();
        idle.init().await?;

        debug!("Entering IMAP IDLE mode");

        // wait() returns (future, stop_source) - we only need the future
        let (wait_future, _stop_source) = idle.wait();

        // Wait for server notification or timeout
        let result = timeout(idle_timeout, wait_future).await;

        match result {
            Ok(Ok(response)) => {
                debug!("IDLE response: {:?}", response);
                // Done with IDLE, return session to normal mode
                let session = idle.done().await?;
                Ok((IdleWaitResult::from_response(&response), session))
            }
            Ok(Err(e)) => {
                // Try to clean up IDLE state
                let _ = idle.done().await;
                Err(anyhow!("IDLE error: {}", e))
            }
            Err(_) => {
                // Timeout - RFC 2177 recommends restarting IDLE every 29 minutes
                debug!("IDLE timeout reached, will re-establish");
                let session = idle.done().await?;
                Ok((IdleWaitResult::Timeout, session))
            }
        }
    }

    /// Main IDLE-based listen loop with automatic reconnection
    #[cfg(feature = "email")]
    async fn listen_with_idle(&self, tx: mpsc::Sender<ChannelMessage>) -> Result<()> {
        self.listen_with_reconnect(&tx, || self.connect_imap())
            .await
    }

    #[cfg(feature = "email")]
    async fn listen_typed_with<S, F, Fut>(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
        connect: F,
    ) -> std::result::Result<(), ChannelSendError>
    where
        S: ImapStream,
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Session<S>>>,
    {
        let (cancel, mut cancellation) = watch::channel(false);
        let generation = self
            .listener_generation
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        {
            let mut active = self
                .listener_cancel
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if active.is_some() {
                return Err(ChannelSendError {
                    message: "Email listener is already running".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            *active = Some((generation, cancel));
        }
        let _registration = ListenerRegistration {
            slot: self.listener_cancel.clone(),
            generation,
        };

        let result = tokio::select! {
            biased;
            _ = cancellation.wait_for(|stopped| *stopped) => Ok(()),
            result = self.listen_with_reconnect_to(EmailDelivery::Typed(sink), connect) => result,
        };
        result.map_err(|_| ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Transient,
            message: "Email listener stopped after an inbound delivery error".into(),
            retryable: true,
            ..Default::default()
        })
    }

    /// The reconnect loop over any session source, so it is testable without
    /// a TLS server.
    #[cfg(feature = "email")]
    async fn listen_with_reconnect<S, F, Fut>(
        &self,
        tx: &mpsc::Sender<ChannelMessage>,
        connect: F,
    ) -> Result<()>
    where
        S: ImapStream,
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Session<S>>>,
    {
        self.listen_with_reconnect_to(EmailDelivery::Legacy(tx), connect)
            .await
    }

    #[cfg(feature = "email")]
    async fn listen_with_reconnect_to<S, F, Fut>(
        &self,
        delivery: EmailDelivery<'_>,
        connect: F,
    ) -> Result<()>
    where
        S: ImapStream,
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Session<S>>>,
    {
        let mut backoff = self.reconnect_backoff;
        let max_backoff = self.max_reconnect_backoff;

        loop {
            match self.run_idle_session(&delivery, connect()).await {
                Ok(()) => {
                    // Clean exit (channel closed)
                    return Ok(());
                }
                Err(error) if error.downcast_ref::<InboundSinkRejected>().is_some() => {
                    return Err(error);
                }
                Err(e) => {
                    error!(
                        "IMAP session error: {}. Reconnecting in {:?}...",
                        e, backoff
                    );
                    sleep(backoff).await;
                    // Exponential backoff with cap
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }
    }

    /// Run a single IDLE session until error or clean shutdown
    #[cfg(feature = "email")]
    async fn run_idle_session<S: ImapStream>(
        &self,
        delivery: &EmailDelivery<'_>,
        connect: impl std::future::Future<Output = Result<Session<S>>>,
    ) -> Result<()> {
        // Connect and authenticate
        let mut session = connect.await?;

        // Select the mailbox
        session.select(&self.config.imap_folder).await?;
        info!(
            "Email IDLE listening on {} (instant push enabled)",
            self.config.imap_folder
        );

        // Check for existing unseen messages first
        self.process_unseen_to(&mut session, delivery).await?;

        loop {
            // Enter IDLE and wait for changes (consumes session, returns it via result)
            match self.wait_for_changes(session).await {
                Ok((IdleWaitResult::NewMail, returned_session)) => {
                    debug!("New mail notification received");
                    session = returned_session;
                    self.process_unseen_to(&mut session, delivery).await?;
                }
                Ok((IdleWaitResult::Timeout, returned_session)) => {
                    // Re-check for mail after IDLE timeout (defensive)
                    session = returned_session;
                    self.process_unseen_to(&mut session, delivery).await?;
                }
                Ok((IdleWaitResult::Interrupted, _)) => {
                    info!("IDLE interrupted, exiting");
                    return Ok(());
                }
                Err(e) => {
                    // Connection likely broken, need to reconnect
                    return Err(e);
                }
            }
        }
    }

    /// Fetch unseen messages and send to channel
    #[cfg(feature = "email")]
    #[cfg(test)]
    async fn process_unseen<S: ImapStream>(
        &self,
        session: &mut Session<S>,
        tx: &mpsc::Sender<ChannelMessage>,
    ) -> Result<()> {
        self.process_unseen_to(session, &EmailDelivery::Legacy(tx))
            .await
    }

    #[cfg(feature = "email")]
    async fn process_unseen_to<S: ImapStream>(
        &self,
        session: &mut Session<S>,
        delivery: &EmailDelivery<'_>,
    ) -> Result<()> {
        let messages = self.fetch_unseen(session).await?;

        for email in messages {
            let uid = email._uid;
            let message_id = email.msg_id.clone();
            // Check allowlist
            if !self.is_sender_allowed(&email.sender) {
                warn!("Blocked email from {}", email.sender);
                self.mark_uid_seen(session, uid).await?;
                continue;
            }

            if self.seen_messages.lock().await.contains(&email.msg_id) {
                self.mark_uid_seen(session, uid).await?;
                continue;
            }

            let accepted = match delivery {
                EmailDelivery::Legacy(tx) => self.dispatch_email(email, tx).await,
                EmailDelivery::Typed(sink) => {
                    self.dispatch_typed_email(&email, *sink).await?;
                    true
                }
            };
            if !accepted {
                // Channel closed, exit cleanly
                return Ok(());
            }

            self.seen_messages.lock().await.insert(message_id);
            self.mark_uid_seen(session, uid).await?;
        }

        Ok(())
    }

    #[cfg(feature = "email")]
    async fn mark_uid_seen<S: ImapStream>(&self, session: &mut Session<S>, uid: u32) -> Result<()> {
        if uid == 0 {
            return Ok(());
        }
        session
            .run_command_and_check_ok(format!("UID STORE {uid} +FLAGS (\\Seen)"))
            .await?;
        Ok(())
    }

    fn create_smtp_transport(&self) -> Result<SmtpTransport> {
        let creds = Credentials::new(self.config.username.clone(), self.config.password.clone());
        let transport = if self.config.smtp_tls {
            SmtpTransport::relay(&self.config.smtp_host)?
                .port(self.config.smtp_port)
                .credentials(creds)
                .build()
        } else {
            SmtpTransport::builder_dangerous(&self.config.smtp_host)
                .port(self.config.smtp_port)
                .credentials(creds)
                .build()
        };
        Ok(transport)
    }

    /// This channel's SMTP account as per-call [`SmtpCredentials`], for the
    /// async [`MailSender`] seam.
    ///
    /// `smtp_tls` maps to implicit TLS ([`SmtpSecurity::Ssl`]) and its absence to
    /// a plaintext connection ([`SmtpSecurity::None`]) — the same two transports
    /// the blocking [`Self::send_message`] path builds. `from_address` is used
    /// as given, so a `Name <addr>` form keeps its display name.
    pub fn smtp_credentials(&self) -> SmtpCredentials {
        SmtpCredentials {
            host: self.config.smtp_host.clone(),
            port: self.config.smtp_port,
            security: if self.config.smtp_tls {
                SmtpSecurity::Ssl
            } else {
                SmtpSecurity::None
            },
            username: self.config.username.clone(),
            password: MailSecret::new(self.config.password.clone()),
            from_name: String::new(),
            from_email: self.config.from_address.clone(),
        }
    }

    /// This channel's IMAP login as per-call [`ImapCredentials`], for the
    /// [`MailReceiver`](crate::providers::mail::MailReceiver) seam.
    pub fn imap_credentials(&self) -> ImapCredentials {
        ImapCredentials {
            host: self.config.imap_host.clone(),
            port: self.config.imap_port,
            username: self.config.username.clone(),
            password: MailSecret::new(self.config.password.clone()),
        }
    }

    pub fn send_message(&self, email: Message) -> Result<()> {
        let transport = self.create_smtp_transport()?;
        transport.send(&email)?;
        info!("Email sent");
        Ok(())
    }

    pub fn build_plain_message(
        &self,
        recipient: &str,
        subject: &str,
        body: &str,
    ) -> Result<Message> {
        Message::builder()
            .from(self.config.from_address.parse()?)
            .to(recipient.parse()?)
            .subject(subject)
            .singlepart(SinglePart::plain(body.to_string()))
            .map_err(Into::into)
    }

    pub fn build_message_with_attachment(
        &self,
        recipient: &str,
        subject: &str,
        body: &str,
        attachment_name: &str,
        content_type: ContentType,
        attachment_bytes: Vec<u8>,
    ) -> Result<Message> {
        let attachment =
            Attachment::new(attachment_name.to_string()).body(attachment_bytes, content_type);
        Message::builder()
            .from(self.config.from_address.parse()?)
            .to(recipient.parse()?)
            .subject(subject)
            .multipart(
                MultiPart::mixed()
                    .singlepart(SinglePart::plain(body.to_string()))
                    .singlepart(attachment),
            )
            .map_err(Into::into)
    }
}

/// Internal struct for parsed email data
#[cfg(feature = "email")]
struct ParsedEmail {
    _uid: u32,
    msg_id: String,
    sender: String,
    content: String,
    timestamp: u64,
    /// The RFC 822 bytes, for the typed inbound path.
    raw: Vec<u8>,
}

/// Result from waiting on IDLE
#[cfg(feature = "email")]
#[derive(Debug, PartialEq, Eq)]
enum IdleWaitResult {
    NewMail,
    Timeout,
    Interrupted,
}

#[cfg(feature = "email")]
impl IdleWaitResult {
    fn from_response(response: &IdleResponse) -> Self {
        match response {
            IdleResponse::NewData(_) => Self::NewMail,
            IdleResponse::Timeout => Self::Timeout,
            IdleResponse::ManualInterrupt => Self::Interrupted,
        }
    }
}

#[cfg(feature = "email")]
#[async_trait]
impl Channel for EmailChannel {
    fn name(&self) -> &str {
        "email"
    }

    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.declared()
    }

    async fn send_typed_with_receipt(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<crate::channel::ChannelSendOutcome> {
        crate::providers::native::outbound::send_typed(self, message)
            .await
            .map_err(crate::providers::native::to_anyhow)
    }

    async fn send(&self, message: &SendMessage) -> Result<()> {
        // Use explicit subject if provided, otherwise fall back to legacy parsing or default
        let (subject, body) = if let Some(ref subj) = message.subject {
            (subj.as_str(), message.content.as_str())
        } else if message.content.starts_with("Subject: ") {
            if let Some(pos) = message.content.find('\n') {
                (&message.content[9..pos], message.content[pos + 1..].trim())
            } else {
                ("OpenHuman Message", message.content.as_str())
            }
        } else {
            ("OpenHuman Message", message.content.as_str())
        };

        // The async sender, not `send_message`: this runs on a runtime
        // worker, and the blocking transport would stall it for the whole
        // SMTP exchange.
        let email = OutboundEmail {
            to: message.recipient.clone(),
            subject: subject.to_string(),
            body: body.to_string(),
            attachments: Vec::new(),
            headers: Default::default(),
        };
        LettreMailSender::new()
            .send(&MailCredentials::Smtp(self.smtp_credentials()), &email)
            .await?;
        info!("Email sent to {}", message.recipient);
        Ok(())
    }

    async fn listen(&self, tx: mpsc::Sender<ChannelMessage>) -> Result<()> {
        info!(
            "Starting email channel with IDLE support on {}",
            self.config.imap_folder
        );
        self.listen_with_idle(tx).await
    }

    async fn health_check(&self) -> bool {
        self.health_check_with(self.connect_imap(), HEALTH_CHECK_TIMEOUT)
            .await
    }
}

#[cfg(feature = "email")]
impl EmailChannel {
    /// Bounded IMAP login probe over any session source.
    async fn health_check_with<S: ImapStream>(
        &self,
        connect: impl std::future::Future<Output = Result<Session<S>>>,
        limit: Duration,
    ) -> bool {
        // Fully async health check - attempt IMAP connection
        match timeout(limit, connect).await {
            Ok(Ok(mut session)) => {
                // Try to logout cleanly
                let _ = session.logout().await;
                true
            }
            Ok(Err(e)) => {
                debug!("Health check failed: {}", e);
                false
            }
            Err(_) => {
                debug!("Health check timed out");
                false
            }
        }
    }
}

#[cfg(feature = "email")]
crate::providers::native::native_v2_adapter!(EmailChannel);

#[cfg(all(test, feature = "email"))]
#[path = "email_channel/fixtures_tests.rs"]
mod fixtures;

#[cfg(all(test, feature = "email"))]
#[path = "email_channel/conformance_tests.rs"]
mod conformance;

#[cfg(all(test, feature = "email"))]
#[path = "email_channel_tests.rs"]
mod tests;

#[cfg(all(feature = "email", any(test, debug_assertions)))]
#[path = "email_channel_test_support_tests.rs"]
pub mod test_support;

/// The send-only surface, exercised in a build that has no IMAP stack.
///
/// This is the half of the split a compile check cannot state on its own: with
/// `email-send` on and `email` off the crate builds either way, so nothing
/// would notice if the send path quietly grew a dependency on the receive half
/// and had to be gated along with it. `voice` in OpenHuman reaches for exactly
/// these three items and nothing else, so this is the contract to keep.
#[cfg(all(test, feature = "email-send", not(feature = "email")))]
#[path = "email_channel_send_only_tests.rs"]
mod send_only_tests;
