use crate::host::{EventSink, MediaRepository, Transcriber, VoiceCodec, VoiceMediaStore};
use crate::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, watch};
use tokio_tungstenite::tungstenite::Message as WsMsg;
use uuid::Uuid;

const FEISHU_BASE_URL: &str = "https://open.feishu.cn/open-apis";
const FEISHU_WS_BASE_URL: &str = "https://open.feishu.cn";
const LARK_BASE_URL: &str = "https://open.larksuite.com/open-apis";
const LARK_WS_BASE_URL: &str = "https://open.larksuite.com";

// ─────────────────────────────────────────────────────────────────────────────
// Feishu WebSocket long-connection: pbbp2.proto frame codec
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, prost::Message)]
struct PbHeader {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// Feishu WS frame (pbbp2.proto).
/// method=0 → CONTROL (ping/pong)  method=1 → DATA (events)
#[derive(Clone, PartialEq, prost::Message)]
struct PbFrame {
    #[prost(uint64, tag = "1")]
    pub seq_id: u64,
    #[prost(uint64, tag = "2")]
    pub log_id: u64,
    #[prost(int32, tag = "3")]
    pub service: i32,
    #[prost(int32, tag = "4")]
    pub method: i32,
    #[prost(message, repeated, tag = "5")]
    pub headers: Vec<PbHeader>,
    #[prost(bytes = "vec", optional, tag = "8")]
    pub payload: Option<Vec<u8>>,
}

impl PbFrame {
    fn header_value<'a>(&'a self, key: &str) -> &'a str {
        self.headers
            .iter()
            .find(|h| h.key == key)
            .map(|h| h.value.as_str())
            .unwrap_or("")
    }
}

/// Server-sent client config (parsed from pong payload)
#[derive(Debug, serde::Deserialize, Default, Clone)]
struct WsClientConfig {
    #[serde(rename = "PingInterval")]
    ping_interval: Option<u64>,
}

/// POST /callback/ws/endpoint response
#[derive(Debug, serde::Deserialize)]
struct WsEndpointResp {
    code: i32,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    data: Option<WsEndpoint>,
}

#[derive(Debug, serde::Deserialize)]
struct WsEndpoint {
    #[serde(rename = "URL")]
    url: String,
    #[serde(rename = "ClientConfig")]
    client_config: Option<WsClientConfig>,
}

/// LarkEvent envelope (method=1 / type=event payload)
#[derive(Debug, serde::Deserialize)]
struct LarkEvent {
    header: LarkEventHeader,
    event: serde_json::Value,
}

#[derive(Debug, serde::Deserialize)]
struct LarkEventHeader {
    event_type: String,
    #[allow(dead_code)]
    event_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct MsgReceivePayload {
    sender: LarkSender,
    message: LarkMessage,
}

#[derive(Debug, serde::Deserialize)]
struct LarkSender {
    sender_id: LarkSenderId,
    #[serde(default)]
    sender_type: String,
}

#[derive(Debug, serde::Deserialize, Default)]
struct LarkSenderId {
    open_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct LarkMessage {
    message_id: String,
    chat_id: String,
    chat_type: String,
    message_type: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    mentions: Vec<serde_json::Value>,
}

struct LarkListenerRegistration<'a>(&'a LarkChannel);

impl Drop for LarkListenerRegistration<'_> {
    fn drop(&mut self) {
        self.0.end_listener();
    }
}

enum LarkIngress<'a> {
    Legacy(tokio::sync::mpsc::Sender<ChannelMessage>),
    Typed(&'a (dyn crate::channel::ChannelInboundSink + Send + Sync)),
}

struct TypedWebhookEvent {
    event: serde_json::Value,
    reply: tokio::sync::oneshot::Sender<TypedWebhookResult>,
}

#[derive(Clone, Copy)]
enum TypedWebhookResult {
    Acknowledged,
    Retry,
}

/// Heartbeat timeout for WS connection — must be larger than ping_interval (default 120 s).
/// If no binary frame (pong or event) is received within this window, reconnect.
const WS_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(300);

/// Returns true when the WebSocket frame indicates live traffic that should
/// refresh the heartbeat watchdog.
fn should_refresh_last_recv(msg: &WsMsg) -> bool {
    matches!(msg, WsMsg::Binary(_) | WsMsg::Ping(_) | WsMsg::Pong(_))
}

/// Lark/Feishu channel.
///
/// Supports two receive modes (configured via `receive_mode` in config):
/// - **`websocket`** (default): persistent WSS long-connection; no public URL needed.
/// - **`webhook`**: HTTP callback server; requires a public HTTPS endpoint.
#[derive(Clone)]
pub struct LarkChannel {
    app_id: String,
    app_secret: String,
    verification_token: String,
    port: Option<u16>,
    allowed_users: Vec<String>,
    /// When true, use Feishu (CN) endpoints; when false, use Lark (international).
    use_feishu: bool,
    /// How to receive events: WebSocket long-connection or HTTP webhook.
    receive_mode: crate::config::LarkReceiveMode,
    /// HTTP client used for all REST calls (injected so callers can thread a
    /// runtime proxy client through). Cheaply cloneable.
    http_client: reqwest::Client,
    /// Scoped host services for native typed media and inbound ingress.
    native: crate::providers::native::HostPorts,
    /// Converts supported source audio into the OPUS format accepted by Lark.
    voice_codec: Option<Arc<dyn VoiceCodec>>,
    /// Cached tenant access token
    tenant_token: Arc<RwLock<Option<String>>>,
    /// Dedup set: WS message_ids seen in last ~30 min to prevent double-dispatch
    ws_seen_ids: Arc<RwLock<HashMap<String, Instant>>>,
    /// The typed adapter owns one listener at a time across all provider clones.
    listener_cancel: Arc<StdMutex<Option<watch::Sender<bool>>>>,
}

impl LarkChannel {
    pub fn new(
        app_id: String,
        app_secret: String,
        verification_token: String,
        port: Option<u16>,
        allowed_users: Vec<String>,
    ) -> Self {
        Self {
            app_id,
            app_secret,
            verification_token,
            port,
            allowed_users,
            use_feishu: true,
            receive_mode: crate::config::LarkReceiveMode::default(),
            http_client: reqwest::Client::new(),
            native: crate::providers::native::HostPorts::default(),
            voice_codec: None,
            tenant_token: Arc::new(RwLock::new(None)),
            ws_seen_ids: Arc::new(RwLock::new(HashMap::new())),
            listener_cancel: Arc::new(StdMutex::new(None)),
        }
    }

    /// Build from `LarkConfig` (preserves `use_feishu` and `receive_mode`).
    pub fn from_config(config: &crate::config::LarkConfig) -> Self {
        let mut ch = Self::new(
            config.app_id.clone(),
            config.app_secret.clone(),
            config.verification_token.clone().unwrap_or_default(),
            config.port,
            config.allowed_users.clone(),
        );
        ch.use_feishu = config.use_feishu;
        ch.receive_mode = config.receive_mode.clone();
        ch
    }

    /// Build from config using the host-selected HTTP client.
    pub fn from_config_with_http_client(
        config: &crate::config::LarkConfig,
        http_client: reqwest::Client,
    ) -> Self {
        let mut channel = Self::from_config(config);
        channel.http_client = http_client;
        channel
    }

    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    pub fn with_voice_codec(mut self, codec: Arc<dyn VoiceCodec>) -> Self {
        self.voice_codec = Some(codec);
        self
    }

    fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    fn begin_listener(
        &self,
    ) -> Result<watch::Receiver<bool>, tinychannels_bus::channel::ChannelSendError> {
        let mut active = self
            .listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if active.is_some() {
            return Err(tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Forbidden,
                message: "Lark listener is already running".into(),
                retryable: false,
                ..Default::default()
            });
        }
        let (cancel, receiver) = watch::channel(false);
        *active = Some(cancel);
        Ok(receiver)
    }

    fn end_listener(&self) {
        if let Some(cancel) = self
            .listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            cancel.send_replace(true);
        }
    }

    async fn mark_ws_message_seen(&self, message_id: &str) -> bool {
        let now = Instant::now();
        let mut seen = self.ws_seen_ids.write().await;
        seen.retain(|_, received| now.duration_since(*received) < Duration::from_secs(30 * 60));
        if seen.contains_key(message_id) {
            return false;
        }
        seen.insert(message_id.to_owned(), now);
        true
    }

    async fn ws_message_was_seen(&self, message_id: &str) -> bool {
        let now = Instant::now();
        let mut seen = self.ws_seen_ids.write().await;
        seen.retain(|_, received| now.duration_since(*received) < Duration::from_secs(30 * 60));
        seen.contains_key(message_id)
    }

    fn api_base(&self) -> &'static str {
        if self.use_feishu {
            FEISHU_BASE_URL
        } else {
            LARK_BASE_URL
        }
    }

    fn ws_base(&self) -> &'static str {
        if self.use_feishu {
            FEISHU_WS_BASE_URL
        } else {
            LARK_WS_BASE_URL
        }
    }

    fn tenant_access_token_url(&self) -> String {
        format!("{}/auth/v3/tenant_access_token/internal", self.api_base())
    }

    fn send_message_url(&self) -> String {
        format!("{}/im/v1/messages?receive_id_type=chat_id", self.api_base())
    }

    /// POST /callback/ws/endpoint → (wss_url, client_config)
    async fn get_ws_endpoint(&self) -> anyhow::Result<(String, WsClientConfig)> {
        let resp = self
            .http_client()
            .post(format!("{}/callback/ws/endpoint", self.ws_base()))
            .header("locale", if self.use_feishu { "zh" } else { "en" })
            .json(&serde_json::json!({
                "AppID": self.app_id,
                "AppSecret": self.app_secret,
            }))
            .send()
            .await?
            .json::<WsEndpointResp>()
            .await?;
        if resp.code != 0 {
            anyhow::bail!(
                "Lark WS endpoint failed: code={} msg={}",
                resp.code,
                resp.msg.as_deref().unwrap_or("(none)")
            );
        }
        let ep = resp
            .data
            .ok_or_else(|| anyhow::anyhow!("Lark WS endpoint: empty data"))?;
        Ok((ep.url, ep.client_config.unwrap_or_default()))
    }

    /// WS long-connection event loop.  Returns Ok(()) when the connection closes
    /// (the caller reconnects).
    #[allow(clippy::too_many_lines)]
    async fn listen_ws(&self, ingress: LarkIngress<'_>) -> anyhow::Result<()> {
        let (wss_url, client_config) = self.get_ws_endpoint().await?;
        let service_id = wss_url
            .split('?')
            .nth(1)
            .and_then(|qs| {
                qs.split('&')
                    .find(|kv| kv.starts_with("service_id="))
                    .and_then(|kv| kv.split('=').nth(1))
                    .and_then(|v| v.parse::<i32>().ok())
            })
            .unwrap_or(0);
        tracing::info!("Lark: connecting to {wss_url}");

        let (ws_stream, _) = tokio_tungstenite::connect_async(&wss_url).await?;
        let (mut write, mut read) = ws_stream.split();
        tracing::info!("Lark: WS connected (service_id={service_id})");

        let mut ping_secs = client_config.ping_interval.unwrap_or(120).max(10);
        let mut hb_interval = tokio::time::interval(Duration::from_secs(ping_secs));
        let mut timeout_check = tokio::time::interval(Duration::from_secs(10));
        hb_interval.tick().await; // consume immediate tick

        let mut seq: u64 = 0;
        let mut last_recv = Instant::now();

        // Send initial ping immediately (like the official SDK) so the server
        // starts responding with pongs and we can calibrate the ping_interval.
        seq = seq.wrapping_add(1);
        let initial_ping = PbFrame {
            seq_id: seq,
            log_id: 0,
            service: service_id,
            method: 0,
            headers: vec![PbHeader {
                key: "type".into(),
                value: "ping".into(),
            }],
            payload: None,
        };
        if write
            .send(WsMsg::Binary(initial_ping.encode_to_vec().into()))
            .await
            .is_err()
        {
            anyhow::bail!("Lark: initial ping failed");
        }
        // message_id → (fragment_slots, created_at) for multi-part reassembly
        type FragEntry = (Vec<Option<Vec<u8>>>, Instant);
        let mut frag_cache: HashMap<String, FragEntry> = HashMap::new();

        loop {
            tokio::select! {
                biased;

                _ = hb_interval.tick() => {
                    seq = seq.wrapping_add(1);
                    let ping = PbFrame {
                        seq_id: seq, log_id: 0, service: service_id, method: 0,
                        headers: vec![PbHeader { key: "type".into(), value: "ping".into() }],
                        payload: None,
                    };
                    if write.send(WsMsg::Binary(ping.encode_to_vec().into())).await.is_err() {
                        tracing::warn!("Lark: ping failed, reconnecting");
                        break;
                    }
                    // GC stale fragments > 5 min
                    let cutoff = Instant::now().checked_sub(Duration::from_secs(300)).unwrap_or(Instant::now());
                    frag_cache.retain(|_, (_, ts)| *ts > cutoff);
                }

                _ = timeout_check.tick() => {
                    if last_recv.elapsed() > WS_HEARTBEAT_TIMEOUT {
                        tracing::warn!("Lark: heartbeat timeout, reconnecting");
                        break;
                    }
                }

                msg = read.next() => {
                    let raw = match msg {
                        Some(Ok(ws_msg)) => {
                            if should_refresh_last_recv(&ws_msg) {
                                last_recv = Instant::now();
                            }
                            match ws_msg {
                                WsMsg::Binary(b) => b,
                                WsMsg::Ping(d) => { let _ = write.send(WsMsg::Pong(d)).await; continue; }
                                WsMsg::Pong(_) => continue,
                                WsMsg::Close(_) => { tracing::info!("Lark: WS closed — reconnecting"); break; }
                                _ => continue,
                            }
                        }
                        None => { tracing::info!("Lark: WS closed — reconnecting"); break; }
                        Some(Err(e)) => { tracing::error!("Lark: WS read error: {e}"); break; }
                    };

                    let frame = match PbFrame::decode(&raw[..]) {
                        Ok(f) => f,
                        Err(e) => { tracing::error!("Lark: proto decode: {e}"); continue; }
                    };

                    // CONTROL frame
                    if frame.method == 0 {
                        if frame.header_value("type") == "pong"
                            && let Some(p) = &frame.payload
                                && let Ok(cfg) = serde_json::from_slice::<WsClientConfig>(p)
                                    && let Some(secs) = cfg.ping_interval {
                                        let secs = secs.max(10);
                                        if secs != ping_secs {
                                            ping_secs = secs;
                                            hb_interval = tokio::time::interval(Duration::from_secs(ping_secs));
                                            tracing::info!("Lark: ping_interval → {ping_secs}s");
                                        }
                                    }
                        continue;
                    }

                    // DATA frame
                    let msg_type = frame.header_value("type").to_string();
                    let msg_id   = frame.header_value("message_id").to_string();
                    let sum      = frame.header_value("sum").parse::<usize>().unwrap_or(1);
                    let seq_num  = frame.header_value("seq").parse::<usize>().unwrap_or(0);

                    // ACK immediately (Feishu requires within 3 s)
                    {
                        let mut ack = frame.clone();
                        ack.payload = Some(br#"{"code":200,"headers":{},"data":[]}"#.to_vec());
                        ack.headers.push(PbHeader { key: "biz_rt".into(), value: "0".into() });
                        let _ = write.send(WsMsg::Binary(ack.encode_to_vec().into())).await;
                    }

                    // Fragment reassembly
                    let sum = if sum == 0 { 1 } else { sum };
                    let payload: Vec<u8> = if sum == 1 || msg_id.is_empty() || seq_num >= sum {
                        frame.payload.clone().unwrap_or_default()
                    } else {
                        let entry = frag_cache.entry(msg_id.clone())
                            .or_insert_with(|| (vec![None; sum], Instant::now()));
                        if entry.0.len() != sum { *entry = (vec![None; sum], Instant::now()); }
                        entry.0[seq_num] = frame.payload.clone();
                        if entry.0.iter().all(|s| s.is_some()) {
                            let full: Vec<u8> = entry.0.iter()
                                .flat_map(|s| s.as_deref().unwrap_or(&[]))
                                .copied().collect();
                            frag_cache.remove(&msg_id);
                            full
                        } else { continue; }
                    };

                    if msg_type != "event" { continue; }

                    let event: LarkEvent = match serde_json::from_slice(&payload) {
                        Ok(e) => e,
                        Err(e) => { tracing::error!("Lark: event JSON: {e}"); continue; }
                    };
                    if event.header.event_type != "im.message.receive_v1" { continue; }

                    let event_payload = event.event.clone();
                    let recv: MsgReceivePayload = match serde_json::from_value(event.event) {
                        Ok(r) => r,
                        Err(e) => { tracing::error!("Lark: payload parse: {e}"); continue; }
                    };

                    if matches!(&ingress, LarkIngress::Typed(_)) {
                        if !self.is_event_admitted(&event_payload) {
                            continue;
                        }
                    } else {
                        if recv.sender.sender_type == "app" || recv.sender.sender_type == "bot" {
                            continue;
                        }
                        let sender_open_id = recv.sender.sender_id.open_id.as_deref().unwrap_or("");
                        if !self.is_user_allowed(sender_open_id) {
                            tracing::warn!("Lark WS: ignoring {sender_open_id} (not in allowed_users)");
                            continue;
                        }
                    }

                    let lark_msg = &recv.message;

                    // Dedup before decoding or downloading content, including when
                    // the same provider message is delivered through both modes.
                    if !self.mark_ws_message_seen(&lark_msg.message_id).await {
                        tracing::debug!("Lark WS: dup {}", lark_msg.message_id);
                        continue;
                    }

                    if let LarkIngress::Typed(sink) = &ingress {
                        if let Some(envelope) = self
                            .inbound_envelope_for_listener(&event_payload, true)
                            .await
                            && sink
                                .push(tinychannels_bus::ChannelIngress::Inbound(envelope))
                                .await
                                .is_err()
                        {
                            tracing::warn!("Lark WS: typed ingress sink rejected an event");
                            break;
                        }
                        continue;
                    }

                    if let Some(envelope) = self.inbound_envelope(&event_payload).await
                        && crate::providers::native::inbound::publish(
                            "lark",
                            self.native.events.as_ref(),
                            envelope,
                        )
                        .await
                    {
                        continue;
                    }

                    // Decode content by type (mirrors clawdbot-feishu parsing)
                    let text = match lark_msg.message_type.as_str() {
                        "text" => {
                            let v: serde_json::Value = match serde_json::from_str(&lark_msg.content) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            match v.get("text").and_then(|t| t.as_str()).filter(|s| !s.is_empty()) {
                                Some(t) => t.to_string(),
                                None => continue,
                            }
                        }
                        "post" => match parse_post_content(&lark_msg.content) {
                            Some(t) => t,
                            None => continue,
                        },
                        _ => { tracing::debug!("Lark WS: skipping unsupported type '{}'", lark_msg.message_type); continue; }
                    };

                    // Strip @_user_N placeholders
                    let text = strip_at_placeholders(&text);
                    let text = text.trim().to_string();
                    if text.is_empty() { continue; }

                    // Group-chat: only respond when explicitly @-mentioned
                    if lark_msg.chat_type == "group" && !should_respond_in_group(&lark_msg.mentions) {
                        continue;
                    }

                    let channel_msg = ChannelMessage {
                        id: Uuid::new_v4().to_string(),
                        sender: lark_msg.chat_id.clone(),
                        reply_target: lark_msg.chat_id.clone(),
                        content: text,
                        channel: "lark".to_string(),
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        topic_id: None,
                        reply_to_id: None,
                        thread_ts: None,
                        sender_name: None,
                    };

                    tracing::debug!("Lark WS: message in {}", lark_msg.chat_id);
                    let LarkIngress::Legacy(tx) = &ingress else {
                        unreachable!("typed Lark ingress returned above")
                    };
                    if tx.send(channel_msg).await.is_err() { break; }
                }
            }
        }
        Ok(())
    }

    /// Check if a user open_id is allowed
    fn is_user_allowed(&self, open_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == open_id)
    }

    fn is_event_admitted(&self, event: &serde_json::Value) -> bool {
        let sender_id = event
            .pointer("/sender/sender_id/open_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let sender_type = event
            .pointer("/sender/sender_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let chat_type = event
            .pointer("/message/chat_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        !sender_id.is_empty()
            && self.is_user_allowed(sender_id)
            && !matches!(sender_type, "app" | "bot")
            && (chat_type != "group"
                || should_respond_in_group(
                    event
                        .pointer("/message/mentions")
                        .and_then(serde_json::Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                ))
    }

    /// Get or refresh tenant access token
    async fn get_tenant_access_token(&self) -> anyhow::Result<String> {
        // Check cache first
        {
            let cached = self.tenant_token.read().await;
            if let Some(ref token) = *cached {
                return Ok(token.clone());
            }
        }

        let url = self.tenant_access_token_url();
        let body = serde_json::json!({
            "app_id": self.app_id,
            "app_secret": self.app_secret,
        });

        let resp = self.http_client().post(&url).json(&body).send().await?;
        let data: serde_json::Value = resp.json().await?;

        let code = data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
        if code != 0 {
            let msg = data
                .get("msg")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            anyhow::bail!("Lark tenant_access_token failed: {msg}");
        }

        let token = data
            .get("tenant_access_token")
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing tenant_access_token in response"))?
            .to_string();

        // Cache it
        {
            let mut cached = self.tenant_token.write().await;
            *cached = Some(token.clone());
        }

        Ok(token)
    }

    /// Invalidate cached token (called on 401)
    async fn invalidate_token(&self) {
        let mut cached = self.tenant_token.write().await;
        *cached = None;
    }

    /// Parse an event callback payload and extract text messages
    pub fn parse_event_payload(&self, payload: &serde_json::Value) -> Vec<ChannelMessage> {
        let mut messages = Vec::new();

        // Lark event v2 structure:
        // { "header": { "event_type": "im.message.receive_v1" }, "event": { "message": { ... }, "sender": { ... } } }
        let event_type = payload
            .pointer("/header/event_type")
            .and_then(|e| e.as_str())
            .unwrap_or("");

        if event_type != "im.message.receive_v1" {
            return messages;
        }

        let event = match payload.get("event") {
            Some(e) => e,
            None => return messages,
        };
        if event
            .pointer("/sender/sender_type")
            .and_then(|kind| kind.as_str())
            .is_some_and(|kind| kind == "app" || kind == "bot")
        {
            return messages;
        }

        // Extract sender open_id
        let open_id = event
            .pointer("/sender/sender_id/open_id")
            .and_then(|s| s.as_str())
            .unwrap_or("");

        if open_id.is_empty() {
            return messages;
        }

        // Check allowlist
        if !self.is_user_allowed(open_id) {
            tracing::warn!("Lark: ignoring message from unauthorized user: {open_id}");
            return messages;
        }
        if event
            .pointer("/message/chat_type")
            .and_then(|kind| kind.as_str())
            == Some("group")
            && !should_respond_in_group(
                event
                    .pointer("/message/mentions")
                    .and_then(serde_json::Value::as_array)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            )
        {
            return messages;
        }

        // Extract message content (text and post supported)
        let msg_type = event
            .pointer("/message/message_type")
            .and_then(|t| t.as_str())
            .unwrap_or("");

        let content_str = event
            .pointer("/message/content")
            .and_then(|c| c.as_str())
            .unwrap_or("");

        let text: String = match msg_type {
            "text" => {
                let extracted = serde_json::from_str::<serde_json::Value>(content_str)
                    .ok()
                    .and_then(|v| {
                        v.get("text")
                            .and_then(|t| t.as_str())
                            .filter(|s| !s.is_empty())
                            .map(String::from)
                    });
                match extracted {
                    Some(t) => t,
                    None => return messages,
                }
            }
            "post" => match parse_post_content(content_str) {
                Some(t) => t,
                None => return messages,
            },
            _ => {
                tracing::debug!("Lark: skipping unsupported message type: {msg_type}");
                return messages;
            }
        };

        let timestamp = event
            .pointer("/message/create_time")
            .and_then(|t| t.as_str())
            .and_then(|t| t.parse::<u64>().ok())
            // Lark timestamps are in milliseconds
            .map(|ms| ms / 1000)
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            });

        let chat_id = event
            .pointer("/message/chat_id")
            .and_then(|c| c.as_str())
            .unwrap_or(open_id);

        messages.push(ChannelMessage {
            id: Uuid::new_v4().to_string(),
            sender: chat_id.to_string(),
            reply_target: chat_id.to_string(),
            content: text,
            channel: "lark".to_string(),
            timestamp,
            topic_id: None,
            reply_to_id: None,
            thread_ts: None,
            sender_name: None,
        });

        messages
    }
}

#[async_trait]
impl Channel for LarkChannel {
    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        crate::providers::long_tail::LongTail::declared(self)
    }

    fn name(&self) -> &str {
        "lark"
    }

    fn supports_remote_control(&self) -> bool {
        true
    }
    fn supports_chat_approvals(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let token = self.get_tenant_access_token().await?;
        let url = self.send_message_url();

        let content = serde_json::json!({ "text": message.content }).to_string();
        let body = serde_json::json!({
            "receive_id": message.recipient,
            "msg_type": "text",
            "content": content,
        });

        let resp = self
            .http_client()
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await?;

        if resp.status().as_u16() == 401 {
            // Token expired, invalidate and retry once
            self.invalidate_token().await;
            let new_token = self.get_tenant_access_token().await?;
            let retry_resp = self
                .http_client()
                .post(&url)
                .header("Authorization", format!("Bearer {new_token}"))
                .header("Content-Type", "application/json; charset=utf-8")
                .json(&body)
                .send()
                .await?;

            if !retry_resp.status().is_success() {
                let err = retry_resp.text().await.unwrap_or_default();
                anyhow::bail!("Lark send failed after token refresh: {err}");
            }
            return Ok(());
        }

        if !resp.status().is_success() {
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("Lark send failed: {err}");
        }

        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        use crate::config::LarkReceiveMode;
        match self.receive_mode {
            LarkReceiveMode::Websocket => self.listen_ws(LarkIngress::Legacy(tx)).await,
            LarkReceiveMode::Webhook => self.listen_http(tx).await,
        }
    }

    async fn health_check(&self) -> bool {
        self.get_tenant_access_token().await.is_ok()
    }
}

#[derive(Clone)]
struct LarkWebhookState {
    verification_token: String,
    channel: Arc<LarkChannel>,
    tx: Option<tokio::sync::mpsc::Sender<ChannelMessage>>,
    typed_tx: Option<tokio::sync::mpsc::Sender<TypedWebhookEvent>>,
}

async fn handle_lark_webhook(
    axum::extract::State(state): axum::extract::State<LarkWebhookState>,
    axum::Json(payload): axum::Json<serde_json::Value>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    // Both challenge and event callbacks must prove knowledge of the
    // verification token. A missing token is never authentication.
    let token_ok = !state.verification_token.is_empty()
        && payload
            .get("token")
            .or_else(|| payload.get("header").and_then(|header| header.get("token")))
            .and_then(|token| token.as_str())
            .is_some_and(|token| token == state.verification_token);
    if !token_ok {
        return (StatusCode::FORBIDDEN, "invalid token").into_response();
    }

    if let Some(challenge) = payload.get("challenge").and_then(|c| c.as_str()) {
        let resp = serde_json::json!({ "challenge": challenge });
        return (StatusCode::OK, axum::Json(resp)).into_response();
    }

    if let Some(typed_tx) = &state.typed_tx {
        if payload
            .pointer("/header/event_type")
            .and_then(serde_json::Value::as_str)
            != Some("im.message.receive_v1")
        {
            return (StatusCode::OK, "ok").into_response();
        }
        let Some(event) = payload.get("event") else {
            return (StatusCode::OK, "ok").into_response();
        };
        let (reply, response) = tokio::sync::oneshot::channel();
        if typed_tx
            .send(TypedWebhookEvent {
                event: event.clone(),
                reply,
            })
            .await
            .is_err()
        {
            return (StatusCode::SERVICE_UNAVAILABLE, "listener stopped").into_response();
        }
        return match response.await {
            Ok(TypedWebhookResult::Acknowledged) => (StatusCode::OK, "ok").into_response(),
            Ok(TypedWebhookResult::Retry) | Err(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, "listener unavailable").into_response()
            }
        };
    }

    let messages = state.channel.parse_event_payload(&payload);
    if payload
        .pointer("/header/event_type")
        .and_then(serde_json::Value::as_str)
        == Some("im.message.receive_v1")
        && let Some(event) = payload.get("event")
        && let Some(envelope) = state.channel.inbound_envelope(event).await
        && crate::providers::native::inbound::publish(
            "lark",
            state.channel.native.events.as_ref(),
            envelope,
        )
        .await
    {
        return (StatusCode::OK, "ok").into_response();
    }
    for msg in messages {
        let Some(tx) = &state.tx else {
            break;
        };
        if tx.send(msg).await.is_err() {
            tracing::warn!("Lark: message channel closed");
            break;
        }
    }

    (StatusCode::OK, "ok").into_response()
}

impl LarkChannel {
    /// HTTP callback server (legacy — requires a public endpoint).
    /// Use `listen()` (WS long-connection) for new deployments.
    pub async fn listen_http(
        &self,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    ) -> anyhow::Result<()> {
        use axum::{Router, routing::post};

        let port = self.port.ok_or_else(|| {
            anyhow::anyhow!("Lark webhook mode requires `port` to be set in [channels_config.lark]")
        })?;
        if self.verification_token.is_empty() {
            anyhow::bail!("Lark webhook mode requires `verification_token`");
        }

        let state = LarkWebhookState {
            verification_token: self.verification_token.clone(),
            channel: Arc::new(self.clone()),
            tx: Some(tx),
            typed_tx: None,
        };

        let app = Router::new()
            .route("/lark", post(handle_lark_webhook))
            .with_state(state);

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        tracing::info!("Lark event callback server listening on {addr}");

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    async fn listen_http_typed(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        cancellation: &mut watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        use axum::{Router, routing::post};

        let port = self.port.ok_or_else(|| {
            anyhow::anyhow!("Lark webhook mode requires `port` to be set in [channels_config.lark]")
        })?;
        if self.verification_token.is_empty() {
            anyhow::bail!("Lark webhook mode requires `verification_token`");
        }

        let (typed_tx, mut events) = tokio::sync::mpsc::channel(128);
        let state = LarkWebhookState {
            verification_token: self.verification_token.clone(),
            channel: Arc::new(self.clone()),
            tx: None,
            typed_tx: Some(typed_tx),
        };
        let app = Router::new()
            .route("/lark", post(handle_lark_webhook))
            .with_state(state);
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let server = axum::serve(listener, app).into_future();
        tokio::pin!(server);

        loop {
            tokio::select! {
                biased;
                changed = cancellation.changed() => {
                    if changed.is_err() || *cancellation.borrow() {
                        return Ok(());
                    }
                }
                result = &mut server => return result.map_err(Into::into),
                Some(request) = events.recv() => {
                    let message_id = request
                        .event
                        .pointer("/message/message_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let result = if !self.is_event_admitted(&request.event) || message_id.is_empty() {
                        TypedWebhookResult::Acknowledged
                    } else if self.ws_message_was_seen(&message_id).await {
                        TypedWebhookResult::Acknowledged
                    } else if let Some(envelope) = self
                        .inbound_envelope_for_listener(&request.event, true)
                        .await
                    {
                        match sink
                            .push(tinychannels_bus::ChannelIngress::Inbound(envelope))
                            .await
                        {
                            Ok(()) => {
                                self.mark_ws_message_seen(&message_id).await;
                                TypedWebhookResult::Acknowledged
                            }
                            Err(error) => {
                                tracing::debug!(error = %error.message, "Lark typed webhook sink rejected an event");
                                TypedWebhookResult::Retry
                            }
                        }
                    } else {
                        TypedWebhookResult::Acknowledged
                    };
                    let _ = request.reply.send(result);
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WS helper functions
// ─────────────────────────────────────────────────────────────────────────────

/// Flatten a Feishu `post` rich-text message to plain text.
///
/// Returns `None` when the content cannot be parsed or yields no usable text,
/// so callers can simply `continue` rather than forwarding a meaningless
/// placeholder string to the agent.
fn parse_post_content(content: &str) -> Option<String> {
    let parsed = serde_json::from_str::<serde_json::Value>(content).ok()?;
    let locale = parsed
        .get("zh_cn")
        .or_else(|| parsed.get("en_us"))
        .or_else(|| {
            parsed
                .as_object()
                .and_then(|m| m.values().find(|v| v.is_object()))
        })?;

    let mut text = String::new();

    if let Some(title) = locale
        .get("title")
        .and_then(|t| t.as_str())
        .filter(|s| !s.is_empty())
    {
        text.push_str(title);
        text.push_str("\n\n");
    }

    if let Some(paragraphs) = locale.get("content").and_then(|c| c.as_array()) {
        for para in paragraphs {
            if let Some(elements) = para.as_array() {
                for el in elements {
                    match el.get("tag").and_then(|t| t.as_str()).unwrap_or("") {
                        "text" => {
                            if let Some(t) = el.get("text").and_then(|t| t.as_str()) {
                                text.push_str(t);
                            }
                        }
                        "a" => {
                            text.push_str(
                                el.get("text")
                                    .and_then(|t| t.as_str())
                                    .filter(|s| !s.is_empty())
                                    .or_else(|| el.get("href").and_then(|h| h.as_str()))
                                    .unwrap_or(""),
                            );
                        }
                        "at" => {
                            let n = el
                                .get("user_name")
                                .and_then(|n| n.as_str())
                                .or_else(|| el.get("user_id").and_then(|i| i.as_str()))
                                .unwrap_or("user");
                            text.push('@');
                            text.push_str(n);
                        }
                        _ => {}
                    }
                }
                text.push('\n');
            }
        }
    }

    let result = text.trim().to_string();
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Remove `@_user_N` placeholder tokens injected by Feishu in group chats.
fn strip_at_placeholders(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        if ch == '@' {
            let rest: String = chars.clone().map(|(_, c)| c).collect();
            if let Some(after) = rest.strip_prefix("_user_") {
                let digit_count = after.chars().take_while(|c| c.is_ascii_digit()).count();
                if digit_count == 0 {
                    result.push(ch);
                    continue;
                }
                let skip = "_user_".len() + digit_count;
                for _ in 0..skip {
                    chars.next();
                }
                if chars.peek().map(|(_, c)| *c == ' ').unwrap_or(false) {
                    chars.next();
                }
                continue;
            }
        }
        result.push(ch);
    }
    result
}

/// In group chats, only respond when the bot is explicitly @-mentioned.
fn should_respond_in_group(mentions: &[serde_json::Value]) -> bool {
    !mentions.is_empty()
}

#[cfg(test)]
#[path = "lark_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lark_api_tests.rs"]
mod api_tests;

#[cfg(test)]
#[path = "lark_conformance_tests.rs"]
mod conformance_tests;

#[async_trait]
impl crate::providers::long_tail::LongTail for LarkChannel {
    const ID: &'static str = "lark";
    const DISPLAY: &'static str = "Lark";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        self.native_declared_capabilities(text_capabilities(TextSpec::default()))
    }

    async fn send_typed_message(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<
        tinychannels_bus::channel::ChannelSendOutcome,
        tinychannels_bus::channel::ChannelSendError,
    > {
        crate::providers::native::outbound::send_typed(self, message).await
    }

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        let mut cancellation = self.begin_listener()?;
        let _registration = LarkListenerRegistration(self);
        let result = match self.receive_mode {
            crate::config::LarkReceiveMode::Websocket => {
                tokio::select! {
                    biased;
                    _ = cancellation.changed() => Ok(()),
                    result = self.listen_ws(LarkIngress::Typed(sink)) => result,
                }
            }
            crate::config::LarkReceiveMode::Webhook => {
                self.listen_http_typed(sink, &mut cancellation).await
            }
        };
        result.map_err(|error| {
            tracing::debug!(%error, "Lark typed listener stopped");
            tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "Lark listener stopped".into(),
                retryable: true,
                ..Default::default()
            }
        })
    }

    async fn stop_listener(&self) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.end_listener();
        Ok(())
    }
}

impl LarkChannel {
    fn native_declared_capabilities(
        &self,
        mut capabilities: tinychannels_bus::ChannelCapabilities,
    ) -> tinychannels_bus::ChannelCapabilities {
        use tinychannels_bus::ContentCapability;
        const IMAGE_LIMIT: u64 = 10 * 1024 * 1024;
        const FILE_LIMIT: u64 = 30 * 1024 * 1024;
        if self.native.media_repository.is_some() {
            capabilities
                .content
                .parts
                .extend(["image", "video", "audio", "file"].map(str::to_owned));
            let media = |types: &[&str], max_size_bytes| ContentCapability {
                inbound: self.native.events.is_some(),
                outbound: true,
                accepted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
                emitted_types: types.iter().map(|mime| (*mime).to_owned()).collect(),
                max_size_bytes: Some(max_size_bytes),
                ..Default::default()
            };
            capabilities.content.image = media(
                &["image/jpeg", "image/png", "image/gif", "image/webp"],
                IMAGE_LIMIT,
            );
            capabilities.content.video = media(&["video/mp4"], FILE_LIMIT);
            let accepted_audio = if self.voice_codec.is_some() {
                vec![
                    "audio/ogg",
                    "audio/opus",
                    "audio/mp4",
                    "audio/mpeg",
                    "audio/wav",
                ]
            } else {
                vec!["audio/ogg", "audio/opus"]
            };
            capabilities.content.audio = ContentCapability {
                inbound: self.native.events.is_some(),
                outbound: true,
                accepted_types: accepted_audio
                    .iter()
                    .map(|mime| (*mime).to_owned())
                    .collect(),
                emitted_types: vec!["audio/opus".into()],
                max_size_bytes: Some(FILE_LIMIT),
                ..Default::default()
            };
            capabilities.content.file = media(
                &[
                    "application/pdf",
                    "text/plain",
                    "text/csv",
                    "application/json",
                    "application/zip",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ],
                FILE_LIMIT,
            );
        }
        if self.native.voice_media_store.is_some() {
            capabilities.content.parts.push("voice_note".into());
            let accepted_codecs = if self.voice_codec.is_some() {
                &["ogg_opus", "m4a_aac", "mp3", "wav_pcm16"][..]
            } else {
                &["ogg_opus"][..]
            };
            capabilities.voice_out = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                FILE_LIMIT,
                accepted_codecs,
            );
        }
        if self.native.voice_media_store.is_some() && self.native.events.is_some() {
            capabilities.voice_in = crate::providers::native::capabilities::voice_capabilities(
                true,
                false,
                FILE_LIMIT,
                &["ogg_opus", "m4a_aac", "mp3"],
            );
        }
        capabilities
    }
}

#[path = "lark/native.rs"]
mod native;

crate::providers::long_tail::impl_long_tail_adapter!(LarkChannel);

#[cfg(test)]
#[path = "lark_ws_tests.rs"]
mod ws_tests;

#[cfg(any(test, debug_assertions))]
#[path = "lark_test_support_tests.rs"]
pub mod test_support;
