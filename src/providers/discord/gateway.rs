//! Discord Gateway session: connect, identify, heartbeat and dispatch routing.

use super::channel::DiscordChannel;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

pub(super) enum GatewaySink {
    Typed(tokio::sync::mpsc::Sender<tinychannels_bus::ChannelIngress>),
}

/// GUILDS | GUILD_MESSAGES | GUILD_MESSAGE_REACTIONS | DIRECT_MESSAGES |
/// DIRECT_MESSAGE_REACTIONS | MESSAGE_CONTENT.
pub(super) const GATEWAY_INTENTS: u64 =
    1 | (1 << 9) | (1 << 10) | (1 << 12) | (1 << 13) | (1 << 15);

impl DiscordChannel {
    pub(super) async fn run_gateway_typed(
        &self,
        tx: tokio::sync::mpsc::Sender<tinychannels_bus::ChannelIngress>,
    ) -> anyhow::Result<()> {
        self.run_gateway_to(GatewaySink::Typed(tx)).await
    }

    async fn run_gateway_to(&self, sink: GatewaySink) -> anyhow::Result<()> {
        let bot_user_id = Self::bot_user_id_from_token(&self.bot_token).unwrap_or_default();

        // Get Gateway URL
        let gw_resp: Value = self
            .http_client()
            .get(format!("{}/gateway/bot", self.api_base))
            .header("Authorization", format!("Bot {}", self.bot_token))
            .send()
            .await?
            .json()
            .await?;

        let gw_url = gw_resp
            .get("url")
            .and_then(|u| u.as_str())
            .unwrap_or("wss://gateway.discord.gg");

        let ws_url = format!("{gw_url}/?v=10&encoding=json");
        tracing::info!("Discord: connecting to gateway...");

        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Read Hello (opcode 10)
        let hello = read.next().await.ok_or(anyhow::anyhow!("No hello"))??;
        let hello_data: Value = serde_json::from_str(&hello.to_string())?;
        let heartbeat_interval = hello_data
            .get("d")
            .and_then(|d| d.get("heartbeat_interval"))
            .and_then(Value::as_u64)
            .unwrap_or(41250);

        // Send Identify (opcode 2)
        let identify = json!({
            "op": 2,
            "d": {
                "token": self.bot_token,
                "intents": GATEWAY_INTENTS,
                "properties": {
                    "os": "linux",
                    "browser": "openhuman",
                    "device": "openhuman"
                }
            }
        });
        write
            .send(Message::Text(identify.to_string().into()))
            .await?;

        tracing::info!("Discord: connected and identified");

        // Track the last sequence number for heartbeats and resume.
        // Only accessed in the select! loop below, so a plain i64 suffices.
        let mut sequence: i64 = -1;

        // Spawn heartbeat timer — sends a tick signal, actual heartbeat
        // is assembled in the select! loop where `sequence` lives.
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
                    if write.send(Message::Text(hb.to_string().into())).await.is_err() {
                        break;
                    }
                }
                msg = read.next() => {
                    let msg = match msg {
                        Some(Ok(Message::Text(t))) => t,
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => continue,
                    };

                    let event: Value = match serde_json::from_str(msg.as_ref()) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    // Track sequence number from all dispatch events
                    if let Some(s) = event.get("s").and_then(Value::as_i64) {
                        sequence = s;
                    }

                    let op = event.get("op").and_then(Value::as_u64).unwrap_or(0);

                    match op {
                        // Op 1: Server requests an immediate heartbeat
                        1 => {
                            let d = if sequence >= 0 { json!(sequence) } else { json!(null) };
                            let hb = json!({"op": 1, "d": d});
                            if write.send(Message::Text(hb.to_string().into())).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        // Op 7: Reconnect
                        7 => {
                            tracing::warn!("Discord: received Reconnect (op 7), closing for restart");
                            break;
                        }
                        // Op 9: Invalid Session
                        9 => {
                            tracing::warn!("Discord: received Invalid Session (op 9), closing for restart");
                            break;
                        }
                        _ => {}
                    }

                    let event_type = event.get("t").and_then(|t| t.as_str()).unwrap_or("");
                    let Some(d) = event.get("d") else {
                        continue;
                    };
                    if !self.handle_dispatch_to(event_type, d, &bot_user_id, &sink).await {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_dispatch_to(
        &self,
        event_type: &str,
        d: &Value,
        bot_user_id: &str,
        sink: &GatewaySink,
    ) -> bool {
        match event_type {
            "MESSAGE_CREATE" => match sink {
                GatewaySink::Typed(tx) => {
                    self.handle_message_create_typed(d, bot_user_id, tx).await
                }
            },
            "MESSAGE_UPDATE" => {
                self.handle_message_update_to(
                    d,
                    bot_user_id,
                    match sink {
                        GatewaySink::Typed(tx) => Some(tx),
                    },
                )
                .await;
                true
            }
            "MESSAGE_DELETE" => {
                self.handle_message_delete_to(
                    d,
                    match sink {
                        GatewaySink::Typed(tx) => Some(tx),
                    },
                )
                .await;
                true
            }
            "MESSAGE_REACTION_ADD" => {
                self.handle_reaction_to(
                    d,
                    bot_user_id,
                    true,
                    match sink {
                        GatewaySink::Typed(tx) => Some(tx),
                    },
                )
                .await;
                true
            }
            "MESSAGE_REACTION_REMOVE" => {
                self.handle_reaction_to(
                    d,
                    bot_user_id,
                    false,
                    match sink {
                        GatewaySink::Typed(tx) => Some(tx),
                    },
                )
                .await;
                true
            }
            "THREAD_CREATE" | "THREAD_UPDATE" => {
                self.remember_thread(d);
                true
            }
            "GUILD_CREATE" => {
                for thread in d
                    .get("threads")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    self.remember_thread(thread);
                }
                true
            }
            "INTERACTION_CREATE" => {
                self.handle_interaction_to(
                    d,
                    match sink {
                        GatewaySink::Typed(tx) => Some(tx),
                    },
                )
                .await;
                true
            }
            _ => true,
        }
    }
}

#[cfg(test)]
#[path = "gateway_tests.rs"]
mod tests;
