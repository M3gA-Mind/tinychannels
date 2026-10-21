//! Inbound transports: Socket Mode (preferred) and `conversations.history`
//! polling (when no app-level token is configured).

use super::events::Flow;
use super::types::SlackChannel;
#[cfg(test)]
use crate::traits::ChannelMessage;
use futures_util::stream::FuturesUnordered;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
#[cfg(test)]
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;

struct SocketDelivery {
    kind: String,
    payload: Value,
    envelope_id: Option<String>,
    accepted: Option<tokio::sync::oneshot::Sender<()>>,
}

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
    #[cfg(test)]
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

    /// Run Socket Mode with a bounded queue between the protocol reader and
    /// typed ingress delivery. Envelopes are acknowledged only after the sink
    /// accepts their typed record. The reader continues servicing websocket
    /// control frames while delivery is pending; a full queue reconnects
    /// without acknowledging the envelope Slack must redeliver.
    pub(crate) async fn listen_socket_typed(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        app_token: &str,
    ) -> anyhow::Result<()> {
        let (delivery_tx, delivery_rx) = tokio::sync::mpsc::channel(32);
        let transport = self.listen_socket_typed_transport(delivery_tx, app_token);
        let delivery = self.deliver_socket_queue(delivery_rx, sink);
        tokio::select! {
            result = transport => result,
            result = delivery => result,
        }
    }

    async fn listen_socket_typed_transport(
        &self,
        delivery_tx: tokio::sync::mpsc::Sender<SocketDelivery>,
        app_token: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("[slack] typed socket mode listening");
        loop {
            if let Some(url) = self.open_socket_url(app_token).await
                && self.run_connection_typed(&url, &delivery_tx).await == Flow::Stop
            {
                return Ok(());
            }
            tokio::time::sleep(self.reconnect_backoff).await;
        }
    }

    async fn deliver_socket_queue(
        &self,
        mut deliveries: tokio::sync::mpsc::Receiver<SocketDelivery>,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> anyhow::Result<()> {
        while let Some(delivery) = deliveries.recv().await {
            loop {
                match self
                    .handle_socket_delivery(
                        &delivery.kind,
                        &delivery.payload,
                        delivery.envelope_id.as_deref(),
                        sink,
                    )
                    .await
                {
                    Ok(()) => {
                        if let Some(accepted) = delivery.accepted {
                            let _ = accepted.send(());
                        }
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(message = %error.message, "[slack] typed ingress is waiting for sink acceptance");
                        tokio::time::sleep(self.reconnect_backoff).await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_connection_typed(
        &self,
        url: &str,
        delivery_tx: &tokio::sync::mpsc::Sender<SocketDelivery>,
    ) -> Flow {
        let Ok((stream, _)) = tokio_tungstenite::connect_async(url).await else {
            tracing::warn!("[slack] typed socket connection failed");
            return Flow::Continue;
        };
        let (mut write, mut read) = stream.split();
        let mut pending_acks = FuturesUnordered::new();
        loop {
            tokio::select! {
                Some(ack_id) = pending_acks.next(), if !pending_acks.is_empty() => {
                    if let Some(id) = ack_id {
                        let ack = serde_json::json!({ "envelope_id": id }).to_string();
                        if write.send(Message::Text(ack.into())).await.is_err() {
                            break;
                        }
                    }
                }
                frame = read.next() => {
                    let Some(Ok(frame)) = frame else { break; };
                    match frame {
                        Message::Ping(payload) => {
                            if write.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Message::Pong(_) => {}
                        Message::Close(_) => break,
                        Message::Text(text) => {
                            let Ok(envelope) = serde_json::from_str::<Value>(text.as_str()) else {
                                continue;
                            };
                            let kind = envelope
                                .get("type")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            if kind == "disconnect" {
                                tracing::info!("[slack] server requested a typed reconnect");
                                break;
                            }
                            let payload = envelope.get("payload").unwrap_or(&Value::Null);
                            if matches!(kind, "events_api" | "interactive") {
                                let envelope_id = envelope
                                    .get("envelope_id")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned);
                                let (accepted, wait_for_acceptance) = if let Some(id) = &envelope_id {
                                    let (accepted, wait) = tokio::sync::oneshot::channel();
                                    (Some(accepted), Some((id.clone(), wait)))
                                } else {
                                    (None, None)
                                };
                                let delivery = SocketDelivery {
                                    kind: kind.to_owned(),
                                    payload: payload.clone(),
                                    envelope_id,
                                    accepted,
                                };
                                match delivery_tx.try_send(delivery) {
                                    Ok(()) => {
                                        if let Some((id, wait)) = wait_for_acceptance {
                                            pending_acks.push(async move {
                                                wait.await.ok().map(|()| id)
                                            });
                                        }
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        // Do not acknowledge an envelope we could not
                                        // retain; Slack will redeliver after reconnect.
                                        tracing::warn!(
                                            "[slack] typed ingress queue is full; reconnecting without ack"
                                        );
                                        break;
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                        return Flow::Stop;
                                    }
                                }
                            } else if let Some(id) = envelope.get("envelope_id").and_then(Value::as_str) {
                                // Unsupported envelope types have no typed record to
                                // await, so acknowledge them immediately as before.
                                let ack = serde_json::json!({ "envelope_id": id }).to_string();
                                if write.send(Message::Text(ack.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Flow::Continue
    }

    /// Serve one websocket connection until it closes or Slack asks us to
    /// reconnect.
    #[cfg(test)]
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
            let kind = envelope
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
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
    #[cfg(test)]
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
                let ts = message
                    .get("ts")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
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

    pub(crate) async fn listen_polling_typed(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        channel_id: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("[slack] typed polling history of the configured channel");
        let mut oldest = String::new();
        loop {
            tokio::time::sleep(self.poll_interval).await;
            let mut query = vec![("channel", channel_id.to_owned()), ("limit", "10".into())];
            if !oldest.is_empty() {
                query.push(("oldest", oldest.clone()));
            }
            let response = match self
                .http_client()
                .get(self.api_url("conversations.history"))
                .bearer_auth(&self.bot_token)
                .query(&query)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(message = %error, "[slack] typed history poll failed");
                    continue;
                }
            };
            let data = match response.json::<Value>().await {
                Ok(data) => data,
                Err(error) => {
                    tracing::warn!(message = %error, "[slack] typed history response was unreadable");
                    continue;
                }
            };
            let messages = data.get("messages").and_then(Value::as_array);
            for message in messages.into_iter().flatten().rev() {
                let ts = message
                    .get("ts")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if ts <= oldest.as_str() {
                    continue;
                }
                let mut event = message.clone();
                event["type"] = "message".into();
                event["channel"] = channel_id.into();
                match self.handle_event_typed(&event, sink).await {
                    Ok(()) => oldest = ts.to_owned(),
                    Err(error) => {
                        tracing::warn!(message = %error.message, "[slack] typed history delivery was not accepted; retrying");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "socket_tests.rs"]
mod tests;
