use super::outbound::SENDER_PREFIX_RESERVE;
use super::{IrcChannel, WriteHalf, split_message};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_rustls::rustls;

const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Monotonic counter to ensure unique message IDs under burst traffic.
static MSG_SEQ: AtomicU64 = AtomicU64::new(0);

/// Style instruction prepended to every IRC message before it reaches the LLM.
/// IRC clients render plain text only — no markdown, no HTML, no XML.
pub(super) const IRC_STYLE_PREFIX: &str = "\
[context: you are responding over IRC. \
Plain text only. No markdown, no tables, no XML/HTML tags. \
Never use triple backtick code fences. Use a single blank line to separate blocks instead. \
Be terse and concise. \
Use short lines. Avoid walls of text.]\n";

pub(super) struct ListenerRegistration<'a>(pub(super) &'a IrcChannel);

impl Drop for ListenerRegistration<'_> {
    fn drop(&mut self) {
        self.0.end_listener();
    }
}

/// A parsed IRC message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IrcMessage {
    pub(super) prefix: Option<String>,
    pub(super) command: String,
    pub(super) params: Vec<String>,
}

impl IrcMessage {
    /// Parse a raw IRC line into an `IrcMessage`.
    ///
    /// IRC format: `[:<prefix>] <command> [<params>] [:<trailing>]`
    pub(super) fn parse(line: &str) -> Option<Self> {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            return None;
        }

        let (prefix, rest) = if let Some(stripped) = line.strip_prefix(':') {
            let space = stripped.find(' ')?;
            (Some(stripped[..space].to_string()), &stripped[space + 1..])
        } else {
            (None, line)
        };

        // Split at trailing (first `:` after command/params)
        let (params_part, trailing) = if let Some(colon_pos) = rest.find(" :") {
            (&rest[..colon_pos], Some(&rest[colon_pos + 2..]))
        } else {
            (rest, None)
        };

        let mut parts: Vec<&str> = params_part.split_whitespace().collect();
        if parts.is_empty() {
            return None;
        }

        let command = parts.remove(0).to_uppercase();
        let mut params: Vec<String> = parts.iter().map(std::string::ToString::to_string).collect();
        if let Some(t) = trailing {
            params.push(t.to_string());
        }

        Some(IrcMessage {
            prefix,
            command,
            params,
        })
    }

    /// Extract the nickname from the prefix (nick!user@host → nick).
    pub(super) fn nick(&self) -> Option<&str> {
        self.prefix.as_ref().and_then(|p| {
            let end = p.find('!').unwrap_or(p.len());
            let nick = &p[..end];
            if nick.is_empty() { None } else { Some(nick) }
        })
    }
}

/// Encode SASL PLAIN credentials: base64(\0nick\0password).
pub(super) fn encode_sasl_plain(nick: &str, password: &str) -> String {
    // Simple base64 encoder — avoids adding a base64 crate dependency.
    // The project's Discord channel uses a similar inline approach.
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let input = format!("\0{nick}\0{password}");
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(chunk.get(1).copied().unwrap_or(0));
        let b2 = u32::from(chunk.get(2).copied().unwrap_or(0));
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(CHARS[(triple >> 18 & 0x3F) as usize] as char);
        out.push(CHARS[(triple >> 12 & 0x3F) as usize] as char);

        if chunk.len() > 1 {
            out.push(CHARS[(triple >> 6 & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }

        if chunk.len() > 2 {
            out.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }

    out
}

impl IrcChannel {
    pub(super) fn is_user_allowed(&self, nick: &str) -> bool {
        if self.allowed_users.iter().any(|u| u == "*") {
            return true;
        }
        self.allowed_users
            .iter()
            .any(|u| u.eq_ignore_ascii_case(nick))
    }

    /// Create a TLS connection to the IRC server.
    async fn connect(
        &self,
    ) -> anyhow::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let addr = format!("{}:{}", self.server, self.port);
        let tcp = tokio::net::TcpStream::connect(&addr).await?;

        let tls_config = if self.verify_tls {
            let root_store: rustls::RootCertStore =
                webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect();
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        } else {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerify))
                .with_no_client_auth()
        };

        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
        let domain = rustls::pki_types::ServerName::try_from(self.server.clone())?;
        let tls = connector.connect(domain, tcp).await?;

        Ok(tls)
    }

    /// Send a raw IRC line (appends \r\n).
    pub(super) async fn send_raw(writer: &mut WriteHalf, line: &str) -> anyhow::Result<()> {
        let data = format!("{line}\r\n");
        writer.write_all(data.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }

    pub(super) async fn send_text_connected(
        writer: &mut WriteHalf,
        recipient: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        // Calculate safe payload size:
        // 512 - sender prefix (~64 bytes for :nick!user@host) - "PRIVMSG " - target - " :" - "\r\n"
        let overhead = SENDER_PREFIX_RESERVE + 10 + recipient.len() + 2;
        let max_payload = 512_usize.saturating_sub(overhead);
        for chunk in split_message(text, max_payload) {
            Self::send_raw(writer, &format!("PRIVMSG {recipient} :{chunk}")).await?;
        }
        Ok(())
    }

    pub(super) fn typed_send_failure(
        error: anyhow::Error,
    ) -> tinychannels_bus::channel::ChannelSendError {
        use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

        tracing::debug!(%error, "[irc] typed send failed");
        ChannelSendError {
            kind: SendErrorKind::Unknown,
            message: "IRC send failed".into(),
            sent_before_error: true,
            ..Default::default()
        }
    }

    pub(super) async fn listen_owned(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        mut cancellation: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> anyhow::Result<()> {
        let result = if let Some(cancel) = &mut cancellation {
            tokio::select! {
                biased;
                _ = cancel.wait_for(|value| *value) => Ok(()),
                result = self.listen_loop(sink) => result,
            }
        } else {
            self.listen_loop(sink).await
        };
        self.clear_writer().await;
        result
    }

    pub(super) fn begin_listener(&self) -> anyhow::Result<tokio::sync::watch::Receiver<bool>> {
        let mut active = self
            .listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if active.is_some() {
            anyhow::bail!("IRC listener is already running");
        }
        let (cancel, receiver) = tokio::sync::watch::channel(false);
        *active = Some(cancel);
        Ok(receiver)
    }

    pub(super) fn end_listener(&self) {
        self.listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    async fn clear_writer(&self) {
        self.writer.lock().await.take();
    }

    pub(super) async fn stop_writer(&self) -> anyhow::Result<()> {
        let Some(mut writer) = self.writer.lock().await.take() else {
            return Ok(());
        };
        match tokio::time::timeout(std::time::Duration::from_secs(1), async {
            Self::send_raw(&mut writer, "QUIT :OpenHuman stopping").await?;
            writer.shutdown().await?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        {
            Ok(result) => result,
            // The write half is already detached from shared state. Dropping it
            // closes the socket even when a peer has stopped reading.
            Err(_) => Ok(()),
        }
    }

    #[cfg(test)]
    pub(super) async fn health_check(&self) -> bool {
        // Lightweight connectivity check: TLS connect + QUIT.
        match self.connect().await {
            Ok(tls) => {
                let (_, mut writer) = tokio::io::split(tls);
                let _ = Self::send_raw(&mut writer, "QUIT :health check").await;
                true
            }
            Err(_) => false,
        }
    }

    async fn listen_loop(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> anyhow::Result<()> {
        let mut current_nick = self.nickname.clone();
        tracing::info!(
            "IRC channel connecting to {}:{} as {}...",
            self.server,
            self.port,
            current_nick
        );

        let tls = self.connect().await?;
        let (reader, mut writer) = tokio::io::split(tls);

        // --- SASL negotiation ---
        if self.sasl_password.is_some() {
            Self::send_raw(&mut writer, "CAP REQ :sasl").await?;
        }

        // --- Server password ---
        if let Some(ref pass) = self.server_password {
            Self::send_raw(&mut writer, &format!("PASS {pass}")).await?;
        }

        // --- Nick/User registration ---
        Self::send_raw(&mut writer, &format!("NICK {current_nick}")).await?;
        Self::send_raw(
            &mut writer,
            &format!("USER {} 0 * :OpenHuman", self.username),
        )
        .await?;

        // Store writer for send()
        {
            let mut guard = self.writer.lock().await;
            *guard = Some(writer);
        }

        let mut buf_reader = BufReader::new(reader);
        let mut line = String::new();
        let mut registered = false;
        let mut sasl_pending = self.sasl_password.is_some();

        loop {
            line.clear();
            let n = tokio::time::timeout(READ_TIMEOUT, buf_reader.read_line(&mut line))
                .await
                .map_err(|_| {
                    anyhow::anyhow!("IRC read timed out (no data for {READ_TIMEOUT:?})")
                })??;
            if n == 0 {
                if self.writer.lock().await.is_none() {
                    return Ok(());
                }
                anyhow::bail!("IRC connection closed by server");
            }

            let Some(msg) = IrcMessage::parse(&line) else {
                continue;
            };

            match msg.command.as_str() {
                "PING" => {
                    let token = msg.params.first().map_or("", String::as_str);
                    let mut guard = self.writer.lock().await;
                    if let Some(ref mut w) = *guard {
                        Self::send_raw(w, &format!("PONG :{token}")).await?;
                    }
                }

                // CAP responses for SASL
                "CAP" => {
                    if sasl_pending && msg.params.iter().any(|p| p.contains("sasl")) {
                        if msg.params.iter().any(|p| p.contains("ACK")) {
                            // CAP * ACK :sasl — server accepted, start SASL auth
                            let mut guard = self.writer.lock().await;
                            if let Some(ref mut w) = *guard {
                                Self::send_raw(w, "AUTHENTICATE PLAIN").await?;
                            }
                        } else if msg.params.iter().any(|p| p.contains("NAK")) {
                            // CAP * NAK :sasl — server rejected SASL, proceed without it
                            tracing::warn!(
                                "IRC server does not support SASL, continuing without it"
                            );
                            sasl_pending = false;
                            let mut guard = self.writer.lock().await;
                            if let Some(ref mut w) = *guard {
                                Self::send_raw(w, "CAP END").await?;
                            }
                        }
                    }
                }

                "AUTHENTICATE" => {
                    // Server sends "AUTHENTICATE +" to request credentials
                    if sasl_pending && msg.params.first().is_some_and(|p| p == "+") {
                        // sasl_password is loaded from runtime config, not hard-coded
                        if let Some(password) = self.sasl_password.as_deref() {
                            let encoded = encode_sasl_plain(&current_nick, password);
                            let mut guard = self.writer.lock().await;
                            if let Some(ref mut w) = *guard {
                                Self::send_raw(w, &format!("AUTHENTICATE {encoded}")).await?;
                            }
                        } else {
                            // SASL was requested but no password is configured; abort SASL
                            tracing::warn!(
                                "SASL authentication requested but no SASL password is configured; aborting SASL"
                            );
                            sasl_pending = false;
                            let mut guard = self.writer.lock().await;
                            if let Some(ref mut w) = *guard {
                                Self::send_raw(w, "CAP END").await?;
                            }
                        }
                    }
                }

                // RPL_SASLSUCCESS (903) — SASL done, end CAP
                "903" => {
                    sasl_pending = false;
                    let mut guard = self.writer.lock().await;
                    if let Some(ref mut w) = *guard {
                        Self::send_raw(w, "CAP END").await?;
                    }
                }

                // SASL failure (904, 905, 906, 907)
                "904" | "905" | "906" | "907" => {
                    tracing::warn!("IRC SASL authentication failed ({})", msg.command);
                    sasl_pending = false;
                    let mut guard = self.writer.lock().await;
                    if let Some(ref mut w) = *guard {
                        Self::send_raw(w, "CAP END").await?;
                    }
                }

                // RPL_WELCOME — registration complete
                "001" => {
                    registered = true;
                    tracing::info!("IRC registered as {}", current_nick);

                    // NickServ authentication
                    if let Some(ref pass) = self.nickserv_password {
                        let mut guard = self.writer.lock().await;
                        if let Some(ref mut w) = *guard {
                            Self::send_raw(w, &format!("PRIVMSG NickServ :IDENTIFY {pass}"))
                                .await?;
                        }
                    }

                    // Join channels
                    for chan in &self.channels {
                        let mut guard = self.writer.lock().await;
                        if let Some(ref mut w) = *guard {
                            Self::send_raw(w, &format!("JOIN {chan}")).await?;
                        }
                    }
                }

                // ERR_NICKNAMEINUSE (433)
                "433" => {
                    let alt = format!("{current_nick}_");
                    tracing::warn!("IRC nickname {current_nick} is in use, trying {alt}");
                    let mut guard = self.writer.lock().await;
                    if let Some(ref mut w) = *guard {
                        Self::send_raw(w, &format!("NICK {alt}")).await?;
                    }
                    current_nick = alt;
                }

                "PRIVMSG" => {
                    if !registered {
                        continue;
                    }

                    let target = msg.params.first().map_or("", String::as_str);
                    let text = msg.params.get(1).map_or("", String::as_str);
                    let sender_nick = msg.nick().unwrap_or("unknown");

                    // Skip messages from NickServ/ChanServ
                    if sender_nick.eq_ignore_ascii_case("NickServ")
                        || sender_nick.eq_ignore_ascii_case("ChanServ")
                    {
                        continue;
                    }

                    if !self.is_user_allowed(sender_nick) {
                        continue;
                    }

                    // Determine reply target: if sent to a channel, reply to channel;
                    // if DM (target == our nick), reply to sender
                    let is_channel = target.starts_with('#') || target.starts_with('&');
                    let content = if is_channel {
                        format!("{IRC_STYLE_PREFIX}<{sender_nick}> {text}")
                    } else {
                        format!("{IRC_STYLE_PREFIX}{text}")
                    };

                    let seq = MSG_SEQ.fetch_add(1, Ordering::Relaxed);
                    let timestamp_unix_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    let envelope = tinychannels_bus::channel::ChannelInboundEnvelope {
                        channel: tinychannels_bus::ChannelRef {
                            id: "irc".into(),
                            account_id: None,
                        },
                        message_id: format!("irc_{timestamp_unix_ms}_{seq}"),
                        timestamp_unix_ms: Some(timestamp_unix_ms),
                        conversation: tinychannels_bus::ConversationRef {
                            kind: if is_channel {
                                tinychannels_bus::ConversationKind::Channel
                            } else {
                                tinychannels_bus::ConversationKind::Dm
                            },
                            id: if is_channel {
                                target.to_owned()
                            } else {
                                sender_nick.to_owned()
                            },
                            ..Default::default()
                        },
                        sender: tinychannels_bus::SenderRef {
                            id: sender_nick.to_owned(),
                            ..Default::default()
                        },
                        text: content.clone(),
                        parts: vec![tinychannels_bus::channel::ContentPart::Text {
                            text: content,
                            markdown_dialect: None,
                        }],
                        ..Default::default()
                    };

                    sink.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                        .await
                        .map_err(|error| anyhow::anyhow!(error.message))?;
                }

                // ERR_PASSWDMISMATCH (464) or other fatal errors
                "464" => {
                    anyhow::bail!("IRC password mismatch");
                }

                _ => {}
            }
        }
    }
}

/// Certificate verifier that accepts any certificate (for `verify_tls=false`).
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
