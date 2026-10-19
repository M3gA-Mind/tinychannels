use crate::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const QQ_API_BASE: &str = "https://api.sgroup.qq.com";
const QQ_AUTH_URL: &str = "https://bots.qq.com/app/getAppAccessToken";

fn ensure_https(url: &str) -> anyhow::Result<()> {
    if !url.starts_with("https://") {
        anyhow::bail!(
            "Refusing to transmit sensitive data over non-HTTPS URL: URL scheme must be https"
        );
    }
    Ok(())
}

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("building the default QQ attachment HTTP client should succeed")
}

/// Deduplication set capacity — evict half of entries when full.
const DEDUP_CAPACITY: usize = 10_000;

/// QQ Official Bot channel — uses Tencent's official QQ Bot API with
/// OAuth2 authentication and a Discord-like WebSocket gateway protocol.
pub struct QQChannel {
    app_id: String,
    app_secret: String,
    allowed_users: Vec<String>,
    /// Cached access token + expiry timestamp.
    token_cache: Arc<RwLock<Option<(String, u64)>>>,
    /// Message deduplication set.
    dedup: Arc<RwLock<HashSet<String>>>,
    http_client: reqwest::Client,
    /// Attachment downloads never follow redirects; QQ attachment URLs are
    /// signed platform URLs and must not be usable as arbitrary fetch proxies.
    attachment_http_client: reqwest::Client,
    /// Host-scoped media and voice stores used by native typed sends.
    native: crate::providers::native::HostPorts,
    /// Single-owner cancellation for the public typed gateway listener.
    listener: std::sync::Mutex<Option<(Uuid, tokio::sync::watch::Sender<bool>)>>,
}

struct ListenerRegistration<'a> {
    channel: &'a QQChannel,
    id: Uuid,
}

impl Drop for ListenerRegistration<'_> {
    fn drop(&mut self) {
        let mut listener = self
            .channel
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if listener.as_ref().is_some_and(|(id, _)| *id == self.id) {
            *listener = None;
        }
    }
}

enum QQIngress<'a> {
    Legacy(tokio::sync::mpsc::Sender<ChannelMessage>),
    Typed(&'a (dyn crate::channel::ChannelInboundSink + Send + Sync)),
}

impl QQChannel {
    pub fn new(app_id: String, app_secret: String, allowed_users: Vec<String>) -> Self {
        Self {
            app_id,
            app_secret,
            allowed_users,
            token_cache: Arc::new(RwLock::new(None)),
            dedup: Arc::new(RwLock::new(HashSet::new())),
            http_client: reqwest::Client::new(),
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            listener: std::sync::Mutex::new(None),
        }
    }

    pub fn with_http_client(
        app_id: String,
        app_secret: String,
        allowed_users: Vec<String>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            app_id,
            app_secret,
            allowed_users,
            token_cache: Arc::new(RwLock::new(None)),
            dedup: Arc::new(RwLock::new(HashSet::new())),
            http_client,
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            listener: std::sync::Mutex::new(None),
        }
    }

    /// Enable scoped media uploads through the host repository.
    pub fn with_media_repository(
        mut self,
        repository: Arc<dyn crate::host::MediaRepository>,
    ) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Enable native QQ voice messages through the host voice store.
    pub fn with_voice_media_store(mut self, store: Arc<dyn crate::host::VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Attach the host's transcriber for platform-marked inbound voice clips.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn crate::host::Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    /// Attach the host's ordered typed-ingress path.
    pub fn with_events(mut self, events: Arc<dyn crate::host::EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// Replace the no-redirect attachment client, primarily for host-managed
    /// trust roots and deterministic local tests.
    #[cfg(test)]
    pub(crate) fn with_attachment_http_client(mut self, client: reqwest::Client) -> Self {
        self.attachment_http_client = client;
        self
    }

    fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    fn attachment_http_client(&self) -> reqwest::Client {
        self.attachment_http_client.clone()
    }

    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Fetch an access token from QQ's OAuth2 endpoint.
    async fn fetch_access_token(&self) -> anyhow::Result<(String, u64)> {
        let body = json!({
            "appId": self.app_id,
            "clientSecret": self.app_secret,
        });

        let resp = self
            .http_client()
            .post(QQ_AUTH_URL)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("QQ token request failed ({status}): {err}");
        }

        let data: serde_json::Value = resp.json().await?;
        let token = data
            .get("access_token")
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing access_token in QQ response"))?
            .to_string();

        let expires_in = data
            .get("expires_in")
            .and_then(|e| e.as_str())
            .and_then(|e| e.parse::<u64>().ok())
            .unwrap_or(7200);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Expire 60 seconds early to avoid edge cases
        let expiry = now + expires_in.saturating_sub(60);

        Ok((token, expiry))
    }

    /// Get a valid access token, refreshing if expired.
    async fn get_token(&self) -> anyhow::Result<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        {
            let cache = self.token_cache.read().await;
            if let Some((ref token, expiry)) = *cache
                && now < expiry
            {
                return Ok(token.clone());
            }
        }

        let (token, expiry) = self.fetch_access_token().await?;
        {
            let mut cache = self.token_cache.write().await;
            *cache = Some((token.clone(), expiry));
        }
        Ok(token)
    }

    /// Get the WebSocket gateway URL.
    async fn get_gateway_url(&self, token: &str) -> anyhow::Result<String> {
        let resp = self
            .http_client()
            .get(format!("{QQ_API_BASE}/gateway"))
            .header("Authorization", format!("QQBot {token}"))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("QQ gateway request failed ({status}): {err}");
        }

        let data: serde_json::Value = resp.json().await?;
        let url = data
            .get("url")
            .and_then(|u| u.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing gateway URL in QQ response"))?
            .to_string();

        Ok(url)
    }

    /// Check and insert message ID for deduplication.
    async fn is_duplicate(&self, msg_id: &str) -> bool {
        if msg_id.is_empty() {
            return false;
        }

        let mut dedup = self.dedup.write().await;

        if dedup.contains(msg_id) {
            return true;
        }

        // Evict oldest half when at capacity
        if dedup.len() >= DEDUP_CAPACITY {
            let to_remove: Vec<String> = dedup.iter().take(DEDUP_CAPACITY / 2).cloned().collect();
            for key in to_remove {
                dedup.remove(&key);
            }
        }

        dedup.insert(msg_id.to_string());
        false
    }

    async fn has_duplicate(&self, msg_id: &str) -> bool {
        !msg_id.is_empty() && self.dedup.read().await.contains(msg_id)
    }

    async fn mark_duplicate(&self, msg_id: &str) {
        if msg_id.is_empty() {
            return;
        }
        let mut dedup = self.dedup.write().await;
        if dedup.len() >= DEDUP_CAPACITY {
            let remove: Vec<String> = dedup.iter().take(DEDUP_CAPACITY / 2).cloned().collect();
            for key in remove {
                dedup.remove(&key);
            }
        }
        dedup.insert(msg_id.to_owned());
    }

    fn begin_listener(&self) -> anyhow::Result<(Uuid, tokio::sync::watch::Receiver<bool>)> {
        let mut listener = self
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if listener.is_some() {
            anyhow::bail!("QQ listener is already running");
        }
        let id = Uuid::new_v4();
        let (cancel, receiver) = tokio::sync::watch::channel(false);
        *listener = Some((id, cancel));
        Ok((id, receiver))
    }
}

impl QQChannel {
    #[allow(clippy::too_many_lines)]
    async fn listen_owned(&self, ingress: QQIngress<'_>) -> anyhow::Result<()> {
        let legacy_tx = match &ingress {
            QQIngress::Legacy(tx) => Some(tx),
            QQIngress::Typed(_) => None,
        };
        tracing::info!("QQ: authenticating...");
        let token = self.get_token().await?;

        tracing::info!("QQ: fetching gateway URL...");
        let gw_url = self.get_gateway_url(&token).await?;

        tracing::info!("QQ: connecting to gateway WebSocket...");
        let (ws_stream, _) = tokio_tungstenite::connect_async(&gw_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Read Hello (opcode 10)
        let hello = read
            .next()
            .await
            .ok_or(anyhow::anyhow!("QQ: no hello frame"))??;
        let hello_data: serde_json::Value = serde_json::from_str(&hello.to_string())?;
        let heartbeat_interval = hello_data
            .get("d")
            .and_then(|d| d.get("heartbeat_interval"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(41250);

        // Send Identify (opcode 2)
        // Intents: PUBLIC_GUILD_MESSAGES (1<<30) | C2C_MESSAGE_CREATE & GROUP_AT_MESSAGE_CREATE (1<<25)
        let intents: u64 = (1 << 25) | (1 << 30);
        let identify = json!({
            "op": 2,
            "d": {
                "token": format!("QQBot {token}"),
                "intents": intents,
                "properties": {
                    "os": "linux",
                    "browser": "openhuman",
                    "device": "openhuman",
                }
            }
        });
        write
            .send(Message::Text(identify.to_string().into()))
            .await?;

        tracing::info!("QQ: connected and identified");

        let mut sequence: i64 = -1;

        // Spawn heartbeat timer
        let (hb_tx, mut hb_rx) = tokio::sync::mpsc::channel::<()>(1);
        let hb_interval = heartbeat_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(hb_interval));
            loop {
                interval.tick().await;
                if hb_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                _ = hb_rx.recv() => {
                    let d = if sequence >= 0 { json!(sequence) } else { json!(null) };
                    let hb = json!({"op": 1, "d": d});
                    if write
                        .send(Message::Text(hb.to_string().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                msg = read.next() => {
                    let msg = match msg {
                        Some(Ok(Message::Text(t))) => t,
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => continue,
                    };

                    let event: serde_json::Value = match serde_json::from_str(msg.as_ref()) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    // Track sequence number
                    if let Some(s) = event.get("s").and_then(serde_json::Value::as_i64) {
                        sequence = s;
                    }

                    let op = event.get("op").and_then(serde_json::Value::as_u64).unwrap_or(0);

                    match op {
                        // Server requests immediate heartbeat
                        1 => {
                            let d = if sequence >= 0 { json!(sequence) } else { json!(null) };
                            let hb = json!({"op": 1, "d": d});
                            if write
                                .send(Message::Text(hb.to_string().into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            continue;
                        }
                        // Reconnect
                        7 => {
                            tracing::warn!("QQ: received Reconnect (op 7)");
                            break;
                        }
                        // Invalid Session
                        9 => {
                            tracing::warn!("QQ: received Invalid Session (op 9)");
                            break;
                        }
                        _ => {}
                    }

                    // Only process dispatch events (op 0)
                    if op != 0 {
                        continue;
                    }

                    let event_type = event.get("t").and_then(|t| t.as_str()).unwrap_or("");
                    let d = match event.get("d") {
                        Some(d) => d,
                        None => continue,
                    };

                    match event_type {
                        "C2C_MESSAGE_CREATE" => {
                            let msg_id = d.get("id").and_then(|i| i.as_str()).unwrap_or("");
                            if let QQIngress::Typed(sink) = &ingress {
                                if self.has_duplicate(msg_id).await {
                                    continue;
                                }
                                let Some(envelope) = self
                                    .inbound_envelope_for_listener("C2C_MESSAGE_CREATE", d)
                                    .await
                                else {
                                    continue;
                                };
                                sink.push(tinychannels_bus::ChannelIngress::Inbound(envelope))
                                    .await
                                    .map_err(|error| anyhow::anyhow!(error.message))?;
                                self.mark_duplicate(msg_id).await;
                                continue;
                            }
                            if self.is_duplicate(msg_id).await {
                                continue;
                            }

                            let content = d.get("content").and_then(|c| c.as_str()).unwrap_or("").trim();

                            let author_id = d.get("author").and_then(|a| a.get("id")).and_then(|i| i.as_str()).unwrap_or("unknown");
                            // For QQ, user_openid is the identifier
                            let user_openid = d.get("author").and_then(|a| a.get("user_openid")).and_then(|u| u.as_str()).unwrap_or(author_id);

                            if !self.is_user_allowed(user_openid) {
                                tracing::warn!("QQ: ignoring C2C message from unauthorized user: {user_openid}");
                                continue;
                            }

                            let chat_id = format!("user:{user_openid}");

                            if let Some(envelope) = self
                                .inbound_envelope("C2C_MESSAGE_CREATE", d)
                                .await
                                && crate::providers::native::inbound::publish(
                                    "qq",
                                    self.native.events.as_ref(),
                                    envelope,
                                )
                                .await
                            {
                                continue;
                            }
                            if content.is_empty() {
                                continue;
                            }

                            let channel_msg = ChannelMessage {
                                id: Uuid::new_v4().to_string(),
                                sender: user_openid.to_string(),
                                reply_target: chat_id,
                                content: content.to_string(),
                                channel: "qq".to_string(),
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                topic_id: None,
                                reply_to_id: None,
                                thread_ts: None,
                                sender_name: None,
                            };

                            if let Some(tx) = legacy_tx {
                                if tx.send(channel_msg).await.is_err() {
                                    tracing::warn!("QQ: message channel closed");
                                    break;
                                }
                            }
                        }
                        "GROUP_AT_MESSAGE_CREATE" => {
                            let msg_id = d.get("id").and_then(|i| i.as_str()).unwrap_or("");
                            if let QQIngress::Typed(sink) = &ingress {
                                if self.has_duplicate(msg_id).await {
                                    continue;
                                }
                                let Some(envelope) = self
                                    .inbound_envelope_for_listener("GROUP_AT_MESSAGE_CREATE", d)
                                    .await
                                else {
                                    continue;
                                };
                                sink.push(tinychannels_bus::ChannelIngress::Inbound(envelope))
                                    .await
                                    .map_err(|error| anyhow::anyhow!(error.message))?;
                                self.mark_duplicate(msg_id).await;
                                continue;
                            }
                            if self.is_duplicate(msg_id).await {
                                continue;
                            }

                            let content = d.get("content").and_then(|c| c.as_str()).unwrap_or("").trim();

                            let author_id = d.get("author").and_then(|a| a.get("member_openid")).and_then(|m| m.as_str()).unwrap_or("unknown");

                            if !self.is_user_allowed(author_id) {
                                tracing::warn!("QQ: ignoring group message from unauthorized user: {author_id}");
                                continue;
                            }

                            let group_openid = d.get("group_openid").and_then(|g| g.as_str()).unwrap_or("unknown");
                            let chat_id = format!("group:{group_openid}");

                            if let Some(envelope) = self
                                .inbound_envelope("GROUP_AT_MESSAGE_CREATE", d)
                                .await
                                && crate::providers::native::inbound::publish(
                                    "qq",
                                    self.native.events.as_ref(),
                                    envelope,
                                )
                                .await
                            {
                                continue;
                            }
                            if content.is_empty() {
                                continue;
                            }

                            let channel_msg = ChannelMessage {
                                id: Uuid::new_v4().to_string(),
                                sender: author_id.to_string(),
                                reply_target: chat_id,
                                content: content.to_string(),
                                channel: "qq".to_string(),
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                topic_id: None,
                                reply_to_id: None,
                                thread_ts: None,
                                sender_name: None,
                            };

                            if let Some(tx) = legacy_tx {
                                if tx.send(channel_msg).await.is_err() {
                                    tracing::warn!("QQ: message channel closed");
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        anyhow::bail!("QQ WebSocket connection closed")
    }
}

#[async_trait]
impl Channel for QQChannel {
    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        crate::providers::long_tail::LongTail::declared(self)
    }

    fn name(&self) -> &str {
        "qq"
    }

    fn supports_remote_control(&self) -> bool {
        true
    }
    fn supports_chat_approvals(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let token = self.get_token().await?;

        // Determine if this is a group or private message based on recipient format
        // Format: "user:{openid}" or "group:{group_openid}"
        let (url, body) = if let Some(group_id) = message.recipient.strip_prefix("group:") {
            (
                format!("{QQ_API_BASE}/v2/groups/{group_id}/messages"),
                json!({
                    "content": &message.content,
                    "msg_type": 0,
                }),
            )
        } else {
            let user_id = message
                .recipient
                .strip_prefix("user:")
                .unwrap_or(&message.recipient);
            (
                format!("{QQ_API_BASE}/v2/users/{user_id}/messages"),
                json!({
                    "content": &message.content,
                    "msg_type": 0,
                }),
            )
        };

        ensure_https(&url)?;

        let resp = self
            .http_client()
            .post(&url)
            .header("Authorization", format!("QQBot {token}"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("QQ send message failed ({status}): {err}");
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        self.listen_owned(QQIngress::Legacy(tx)).await
    }

    async fn health_check(&self) -> bool {
        self.fetch_access_token().await.is_ok()
    }
}

impl crate::providers::long_tail::LongTail for QQChannel {
    const ID: &'static str = "qq";
    const DISPLAY: &'static str = "QQ";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        crate::providers::native::NativeProvider::declared_capabilities(self)
    }
}

#[path = "qq/native.rs"]
mod native;

crate::providers::native::native_v2_adapter!(QQChannel);

#[cfg(test)]
#[path = "qq_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "qq/native_tests.rs"]
mod native_tests;
