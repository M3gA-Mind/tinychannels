mod inbound;
mod config;
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

/// Response from DingTalk gateway connection registration.
#[derive(serde::Deserialize)]
struct GatewayResponse {
    endpoint: String,
    ticket: String,
}

impl DingTalkChannel {
   pub(crate) fn with_attachment_http_client(mut self, client: reqwest::Client) -> Self {
        self.attachment_http_client = client;
        self
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

#[path = "native.rs"]
mod native;

crate::providers::native::native_v2_adapter!(DingTalkChannel);

#[cfg(test)]
#[path = "dingtalk_tests.rs"]
mod tests;
