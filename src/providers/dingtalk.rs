use crate::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const DINGTALK_BOT_CALLBACK_TOPIC: &str = "/v1.0/im/bot/messages/get";

/// DingTalk channel — connects via Stream Mode WebSocket for real-time messages.
/// Replies are sent through per-message session webhook URLs.
pub struct DingTalkChannel {
    client_id: String,
    client_secret: String,
    allowed_users: Vec<String>,
    /// Per-chat session webhooks for sending replies (chatID -> webhook URL).
    /// DingTalk provides a unique webhook URL with each incoming message.
    session_webhooks: Arc<RwLock<HashMap<String, String>>>,
    http_client: reqwest::Client,
    attachment_http_client: reqwest::Client,
    native: crate::providers::native::HostPorts,
    robot_contexts: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// Single-owner cancellation for the public Stream listener.
    listener: std::sync::Mutex<Option<(Uuid, tokio::sync::watch::Sender<bool>)>>,
}

struct ListenerRegistration<'a> {
    channel: &'a DingTalkChannel,
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

enum DingTalkIngress<'a> {
    Legacy(tokio::sync::mpsc::Sender<ChannelMessage>),
    Typed(&'a (dyn crate::channel::ChannelInboundSink + Send + Sync)),
}

/// Response from DingTalk gateway connection registration.
#[derive(serde::Deserialize)]
struct GatewayResponse {
    endpoint: String,
    ticket: String,
}

impl DingTalkChannel {
    pub fn new(client_id: String, client_secret: String, allowed_users: Vec<String>) -> Self {
        Self {
            client_id,
            client_secret,
            allowed_users,
            session_webhooks: Arc::new(RwLock::new(HashMap::new())),
            http_client: reqwest::Client::new(),
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            robot_contexts: Arc::new(RwLock::new(HashMap::new())),
            listener: std::sync::Mutex::new(None),
        }
    }

    pub fn with_http_client(
        client_id: String,
        client_secret: String,
        allowed_users: Vec<String>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            allowed_users,
            session_webhooks: Arc::new(RwLock::new(HashMap::new())),
            http_client,
            attachment_http_client: no_redirect_client(),
            native: crate::providers::native::HostPorts::default(),
            robot_contexts: Arc::new(RwLock::new(HashMap::new())),
            listener: std::sync::Mutex::new(None),
        }
    }

    /// Enable native typed media ingress and upload through host-scoped storage.
    pub fn with_media_repository(
        mut self,
        repository: Arc<dyn crate::host::MediaRepository>,
    ) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Attach the host's ordered typed-ingress path.
    pub fn with_events(mut self, events: Arc<dyn crate::host::EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// Enable native voice ingress through the host's scoped voice store.
    pub fn with_voice_media_store(mut self, store: Arc<dyn crate::host::VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Enable shared final-turn synthesis when the host has TTS, a codec,
    /// and durable scoped voice storage.
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.native.voice_synthesis = enabled;
        self
    }

    /// Attach the host's speech-to-text service for media that carries no transcript.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn crate::host::Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_attachment_http_client(mut self, client: reqwest::Client) -> Self {
        self.attachment_http_client = client;
        self
    }

    fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    fn parse_stream_data(frame: &serde_json::Value) -> Option<serde_json::Value> {
        match frame.get("data") {
            Some(serde_json::Value::String(raw)) => serde_json::from_str(raw).ok(),
            Some(serde_json::Value::Object(_)) => frame.get("data").cloned(),
            _ => None,
        }
    }

    fn resolve_chat_id(data: &serde_json::Value, sender_id: &str) -> String {
        let is_private_chat = data
            .get("conversationType")
            .and_then(|value| {
                value
                    .as_str()
                    .map(|v| v == "1")
                    .or_else(|| value.as_i64().map(|v| v == 1))
            })
            .unwrap_or(true);

        if is_private_chat {
            sender_id.to_string()
        } else {
            data.get("conversationId")
                .and_then(|c| c.as_str())
                .unwrap_or(sender_id)
                .to_string()
        }
    }

    /// Register a connection with DingTalk's gateway to get a WebSocket endpoint.
    async fn register_connection(&self) -> anyhow::Result<GatewayResponse> {
        let body = serde_json::json!({
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "subscriptions": [
                {
                    "type": "CALLBACK",
                    "topic": DINGTALK_BOT_CALLBACK_TOPIC,
                }
            ],
        });

        let resp = self
            .http_client()
            .post("https://api.dingtalk.com/v1.0/gateway/connections/open")
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("DingTalk gateway registration failed ({status}): {err}");
        }

        let gw: GatewayResponse = resp.json().await?;
        Ok(gw)
    }

    fn begin_listener(&self) -> anyhow::Result<(Uuid, tokio::sync::watch::Receiver<bool>)> {
        let mut listener = self
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if listener.is_some() {
            anyhow::bail!("DingTalk listener is already running");
        }
        let id = Uuid::new_v4();
        let (cancel, receiver) = tokio::sync::watch::channel(false);
        *listener = Some((id, cancel));
        Ok((id, receiver))
    }
}

impl DingTalkChannel {
    async fn listen_owned(
        &self,
        ingress: DingTalkIngress<'_>,
        mut cancellation: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> anyhow::Result<()> {
        let legacy_tx = match &ingress {
            DingTalkIngress::Legacy(tx) => Some(tx),
            DingTalkIngress::Typed(_) => None,
        };
        tracing::info!("DingTalk: registering gateway connection...");

        let Some(gw) =
            until_listener_cancelled(&mut cancellation, self.register_connection()).await
        else {
            return Ok(());
        };
        let gw = gw?;
        let ws_url = format!("{}?ticket={}", gw.endpoint, gw.ticket);

        tracing::info!("DingTalk: connecting to stream WebSocket...");
        let Some(connection) =
            until_listener_cancelled(&mut cancellation, tokio_tungstenite::connect_async(&ws_url))
                .await
        else {
            return Ok(());
        };
        let (ws_stream, _) = connection?;
        let (mut write, mut read) = ws_stream.split();

        tracing::info!("DingTalk: connected and listening for messages...");

        let mut stopped = false;
        'stream: loop {
            let next = tokio::select! {
                biased;
                () = listener_cancelled(&mut cancellation) => {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        write.send(Message::Close(None)),
                    ).await;
                    stopped = true;
                    break 'stream;
                }
                message = read.next() => message,
            };
            let Some(msg) = next else {
                break;
            };
            let msg = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    tracing::warn!("DingTalk WebSocket error: {e}");
                    break;
                }
                _ => continue,
            };

            let frame: serde_json::Value = match serde_json::from_str(msg.as_ref()) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let frame_type = frame.get("type").and_then(|t| t.as_str()).unwrap_or("");

            match frame_type {
                "SYSTEM" => {
                    // Respond to system pings to keep the connection alive
                    let message_id = frame
                        .get("headers")
                        .and_then(|h| h.get("messageId"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("");

                    let pong = serde_json::json!({
                        "code": 200,
                        "headers": {
                            "contentType": "application/json",
                            "messageId": message_id,
                        },
                        "message": "OK",
                        "data": "",
                    });

                    let pong_result = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        write.send(Message::Text(pong.to_string().into())),
                    )
                    .await;
                    if !matches!(pong_result, Ok(Ok(()))) {
                        tracing::warn!("DingTalk: failed to send pong or timed out");
                        break;
                    }
                }
                "EVENT" | "CALLBACK" => {
                    // Parse the chatbot callback data from the frame.
                    let data = match Self::parse_stream_data(&frame) {
                        Some(v) => v,
                        None => {
                            tracing::debug!("DingTalk: frame has no parseable data payload");
                            continue;
                        }
                    };

                    // Extract message content
                    let content = data
                        .get("text")
                        .and_then(|t| t.get("content"))
                        .and_then(|c| c.as_str())
                        .unwrap_or("")
                        .trim();

                    let sender_id = data
                        .get("senderStaffId")
                        .and_then(|s| s.as_str())
                        .unwrap_or("unknown");

                    if !self.is_user_allowed(sender_id) {
                        tracing::warn!(
                            "DingTalk: ignoring message from unauthorized user: {sender_id}"
                        );
                        continue;
                    }

                    let provider_message_id = data
                        .get("msgId")
                        .and_then(serde_json::Value::as_str)
                        .filter(|id| !id.is_empty());
                    if provider_message_id
                        .is_some_and(|id| self.native.has_seen(&format!("dingtalk:{id}")))
                    {
                        if !send_callback_ack(&mut write, &frame).await {
                            break 'stream;
                        }
                        continue;
                    }

                    // Private chat uses sender ID, group chat uses conversation ID.
                    let chat_id = Self::resolve_chat_id(&data, sender_id);

                    // Store session webhook for later replies
                    if let Some(webhook) = data.get("sessionWebhook").and_then(|w| w.as_str()) {
                        let webhook = webhook.to_string();
                        let mut webhooks = self.session_webhooks.write().await;
                        // Use both keys so reply routing works for both group and private flows.
                        webhooks.insert(chat_id.clone(), webhook.clone());
                        webhooks.insert(sender_id.to_string(), webhook);
                    }

                    if let DingTalkIngress::Typed(sink) = &ingress {
                        let envelope = tokio::select! {
                            biased;
                            () = listener_cancelled(&mut cancellation) => {
                                let _ = tokio::time::timeout(
                                    std::time::Duration::from_secs(1),
                                    write.send(Message::Close(None)),
                                ).await;
                                stopped = true;
                                break 'stream;
                            }
                            envelope = self.inbound_envelope_for_listener(&data) => envelope,
                        };
                        let Some(envelope) = envelope else {
                            continue;
                        };
                        let provider_message_id = envelope.message_id.clone();
                        let published = tokio::select! {
                            biased;
                            () = listener_cancelled(&mut cancellation) => {
                                let _ = tokio::time::timeout(
                                    std::time::Duration::from_secs(1),
                                    write.send(Message::Close(None)),
                                ).await;
                                stopped = true;
                                break 'stream;
                            }
                            result = sink.push(tinychannels_bus::ChannelIngress::Inbound(envelope)) => result,
                        };
                        published.map_err(|error| {
                            anyhow::anyhow!("DingTalk typed ingress rejected: {}", error.message)
                        })?;
                        self.native
                            .mark_seen(format!("dingtalk:{provider_message_id}"));
                        if !send_callback_ack(&mut write, &frame).await {
                            break 'stream;
                        }
                        continue;
                    }

                    if let Some(envelope) = self.inbound_envelope(&data).await {
                        let provider_message_id = envelope.message_id.clone();
                        if crate::providers::native::inbound::publish(
                            "dingtalk",
                            self.native.events.as_ref(),
                            envelope,
                        )
                        .await
                        {
                            self.native
                                .mark_seen(format!("dingtalk:{provider_message_id}"));
                            if !send_callback_ack(&mut write, &frame).await {
                                break 'stream;
                            }
                            continue;
                        }
                    }

                    if content.is_empty() {
                        continue;
                    }

                    // Acknowledge the event
                    if !send_callback_ack(&mut write, &frame).await {
                        break 'stream;
                    }

                    let channel_msg = ChannelMessage {
                        id: Uuid::new_v4().to_string(),
                        sender: sender_id.to_string(),
                        reply_target: chat_id,
                        content: content.to_string(),
                        channel: "dingtalk".to_string(),
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
                            tracing::warn!("DingTalk: message channel closed");
                            break;
                        }
                    }
                }
                _ => {}
            }
        }

        if stopped {
            Ok(())
        } else {
            anyhow::bail!("DingTalk WebSocket stream ended")
        }
    }
}

async fn send_callback_ack<S>(write: &mut S, frame: &serde_json::Value) -> bool
where
    S: futures_util::Sink<Message> + Unpin,
{
    let message_id = frame
        .get("headers")
        .and_then(|headers| headers.get("messageId"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let ack = serde_json::json!({
        "code": 200,
        "headers": {
            "contentType": "application/json",
            "messageId": message_id,
        },
        "message": "OK",
        "data": "",
    });
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            write.send(Message::Text(ack.to_string().into())),
        )
        .await,
        Ok(Ok(()))
    )
}

async fn listener_cancelled(cancellation: &mut Option<tokio::sync::watch::Receiver<bool>>) {
    match cancellation {
        Some(receiver) => {
            let _ = receiver.changed().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn until_listener_cancelled<F: std::future::Future>(
    cancellation: &mut Option<tokio::sync::watch::Receiver<bool>>,
    future: F,
) -> Option<F::Output> {
    tokio::select! {
        biased;
        () = listener_cancelled(cancellation) => None,
        output = future => Some(output),
    }
}

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("building the DingTalk attachment HTTP client should succeed")
}

#[async_trait]
impl Channel for DingTalkChannel {
    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        crate::providers::native::NativeProvider::declared_capabilities(self)
    }

    fn name(&self) -> &str {
        "dingtalk"
    }

    fn supports_remote_control(&self) -> bool {
        true
    }

    fn supports_chat_approvals(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let webhooks = self.session_webhooks.read().await;
        let webhook_url = webhooks.get(&message.recipient).ok_or_else(|| {
            anyhow::anyhow!(
                "No session webhook found for chat {}. \
                 The user must send a message first to establish a session.",
                message.recipient
            )
        })?;

        let title = message.subject.as_deref().unwrap_or("OpenHuman");
        let body = serde_json::json!({
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": message.content,
            }
        });

        let resp = self
            .http_client()
            .post(webhook_url)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("DingTalk webhook reply failed ({status}): {err}");
        }

        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        let (id, cancellation) = self.begin_listener()?;
        let _registration = ListenerRegistration { channel: self, id };
        self.listen_owned(DingTalkIngress::Legacy(tx), Some(cancellation))
            .await
    }

    async fn health_check(&self) -> bool {
        self.register_connection().await.is_ok()
    }
}

#[path = "dingtalk/native.rs"]
mod native;

crate::providers::native::native_v2_adapter!(DingTalkChannel);

#[cfg(test)]
#[path = "dingtalk_tests.rs"]
mod tests;
