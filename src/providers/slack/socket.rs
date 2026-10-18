//! Inbound transports: Socket Mode (preferred) and `conversations.history`
//! polling (when no app-level token is configured).

use super::events::Flow;
use super::types::SlackChannel;
use crate::traits::ChannelMessage;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;

impl SlackChannel {
    /// `apps.connections.open` with the app-level token; returns the `wss` URL.
    async fn open_socket_url(&self, app_token: &str) -> Option<String> {
        let response = self
            .http_client()
            .post(self.api_url("apps.connections.open"))
            .bearer_auth(app_token)
            .send()
            .await
            .ok()?;
        let value = response.json::<Value>().await.ok()?;
        if value.get("ok").and_then(Value::as_bool) != Some(true) {
            tracing::warn!("[slack] apps.connections.open was rejected");
            return None;
        }
        value.get("url")?.as_str().map(str::to_owned)
    }

    /// Run Socket Mode, reconnecting after every disconnect.
    pub(crate) async fn listen_socket(
        &self,
        tx: &Sender<ChannelMessage>,
        app_token: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("[slack] socket mode listening");
        loop {
            if let Some(url) = self.open_socket_url(app_token).await
                && self.run_connection(&url, tx).await == Flow::Stop
            {
                return Ok(());
            }
            tokio::time::sleep(self.reconnect_backoff).await;
        }
    }

    /// Serve one websocket connection until it closes or Slack asks us to
    /// reconnect.
    async fn run_connection(&self, url: &str, tx: &Sender<ChannelMessage>) -> Flow {
        let Ok((stream, _)) = tokio_tungstenite::connect_async(url).await else {
            tracing::warn!("[slack] socket connection failed");
            return Flow::Continue;
        };
        let (mut write, mut read) = stream.split();
        while let Some(Ok(frame)) = read.next().await {
            let Message::Text(text) = frame else {
                continue;
            };
            let Ok(envelope) = serde_json::from_str::<Value>(text.as_str()) else {
                continue;
            };
            let kind = envelope.get("type").and_then(Value::as_str).unwrap_or_default();
            if kind == "disconnect" {
                tracing::info!("[slack] server requested a reconnect");
                break;
            }
            // Acknowledge first: Slack redelivers anything not acked within 3s.
            if let Some(id) = envelope.get("envelope_id").and_then(Value::as_str) {
                let ack = serde_json::json!({ "envelope_id": id }).to_string();
                if write.send(Message::Text(ack.into())).await.is_err() {
                    break;
                }
            }
            let payload = envelope.get("payload").unwrap_or(&Value::Null);
            match kind {
                "events_api" => {
                    let event = payload.get("event").unwrap_or(&Value::Null);
                    if self.handle_event(event, Some(tx)).await == Flow::Stop {
                        return Flow::Stop;
                    }
                }
                "interactive" => self.handle_interactive(payload).await,
                _ => {}
            }
        }
        Flow::Continue
    }

    /// Poll `conversations.history` of the configured channel.
    pub(crate) async fn listen_polling(
        &self,
        tx: &Sender<ChannelMessage>,
        channel_id: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("[slack] polling history of the configured channel");
        let mut oldest = String::new();
        loop {
            tokio::time::sleep(self.poll_interval).await;
            let mut query = vec![("channel", channel_id.to_owned()), ("limit", "10".into())];
            if !oldest.is_empty() {
                query.push(("oldest", oldest.clone()));
            }
            let Ok(response) = self
                .http_client()
                .get(self.api_url("conversations.history"))
                .bearer_auth(&self.bot_token)
                .query(&query)
                .send()
                .await
            else {
                tracing::warn!("[slack] history poll failed");
                continue;
            };
            let Ok(data) = response.json::<Value>().await else {
                tracing::warn!("[slack] history response was unreadable");
                continue;
            };
            let messages = data.get("messages").and_then(Value::as_array);
            // History is newest-first; deliver oldest-first.
            for message in messages.into_iter().flatten().rev() {
                let ts = message.get("ts").and_then(Value::as_str).unwrap_or_default();
                if ts <= oldest.as_str() {
                    continue;
                }
                let mut event = message.clone();
                event["type"] = "message".into();
                event["channel"] = channel_id.into();
                if self.handle_event(&event, Some(tx)).await == Flow::Stop {
                    return Ok(());
                }
                oldest = ts.to_owned();
            }
        }
    }
}

#[cfg(test)]
#[path = "socket_tests.rs"]
mod tests;
