mod inbound;
mod config;
use crate::host::{EventSink, MediaRepository, Transcriber, VoiceCodec, VoiceMediaStore};
use crate::traits::ChannelMessage;
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

struct LarkListenerRegistration<'a> {
    channel: &'a LarkChannel,
    id: Uuid,
}

type LarkListenerCancel = Arc<StdMutex<Option<(Uuid, watch::Sender<bool>)>>>;

impl Drop for LarkListenerRegistration<'_> {
    fn drop(&mut self) {
        self.channel.end_listener(self.id);
    }
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
    listener_cancel: LarkListenerCancel,
}


#[cfg(test)]
#[path = "lark_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "api_tests.rs"]
mod api_tests;

#[cfg(test)]
#[path = "legacy_conformance_tests.rs"]
mod conformance_tests;

#[async_trait]
impl crate::providers::long_tail::LongTail for LarkChannel {
    const ID: &'static str = "lark";
    const DISPLAY: &'static str = "Lark";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        self.native_declared_capabilities(text_capabilities(TextSpec::default()))
    }

    async fn health_state(&self) -> &'static str {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.get_tenant_access_token(),
        )
        .await
        {
            Ok(Ok(_)) => "healthy",
            Ok(Err(_)) | Err(_) => "unhealthy",
        }
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
        let (mut cancellation, id) = self.begin_listener()?;
        let _registration = LarkListenerRegistration { channel: self, id };
        let result = match self.receive_mode {
            crate::config::LarkReceiveMode::Websocket => {
                tokio::select! {
                    biased;
                    _ = cancellation.changed() => Ok(()),
                    result = self.listen_ws(sink) => result,
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
        self.cancel_listener();
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

#[path = "native.rs"]
mod native;

crate::providers::long_tail::impl_long_tail_adapter!(LarkChannel);

#[cfg(test)]
#[path = "ws_tests.rs"]
mod ws_tests;

#[cfg(any(test, debug_assertions))]
#[path = "test_support_tests.rs"]
pub mod test_support;
