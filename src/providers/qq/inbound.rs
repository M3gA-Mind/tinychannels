//! QQ gateway authentication, listener lifecycle, and inbound events.

use super::*;

const INBOUND_ATTACHMENT_LIMIT: u64 = 50 * 1024 * 1024;
const MAX_INBOUND_ATTACHMENTS: usize = 10;

pub(super) fn timestamp_unix_ms(value: &serde_json::Value) -> Option<u64> {
    if let Some(timestamp) = value.as_u64() {
        return Some(timestamp);
    }
    let timestamp = value.as_str()?;
    if let Ok(timestamp) = timestamp.parse::<u64>() {
        return Some(timestamp);
    }
    let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp).ok()?;
    u64::try_from(timestamp.timestamp_millis()).ok()
}

impl QQChannel {
    pub(super) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    pub(super) fn attachment_http_client(&self) -> reqwest::Client {
        self.attachment_http_client.clone()
    }

    pub(super) fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Fetch an access token from QQ's OAuth2 endpoint.
    pub(super) async fn fetch_access_token(&self) -> anyhow::Result<(String, u64)> {
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
    pub(super) async fn get_token(&self) -> anyhow::Result<String> {
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
    pub(super) async fn get_gateway_url(&self, token: &str) -> anyhow::Result<String> {
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
    #[cfg(test)]
    pub(super) async fn is_duplicate(&self, msg_id: &str) -> bool {
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

    pub(super) async fn has_duplicate(&self, msg_id: &str) -> bool {
        !msg_id.is_empty() && self.dedup.read().await.contains(msg_id)
    }

    pub(super) async fn mark_duplicate(&self, msg_id: &str) {
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

    pub(super) fn begin_listener(
        &self,
    ) -> anyhow::Result<(Uuid, tokio::sync::watch::Receiver<bool>)> {
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
    pub(super) async fn listen_owned(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        mut cancellation: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> anyhow::Result<()> {
        tracing::info!("QQ: authenticating...");
        let Some(token) = until_listener_cancelled(&mut cancellation, self.get_token()).await
        else {
            return Ok(());
        };
        let token = token?;

        tracing::info!("QQ: fetching gateway URL...");
        let Some(gw_url) =
            until_listener_cancelled(&mut cancellation, self.get_gateway_url(&token)).await
        else {
            return Ok(());
        };
        let gw_url = gw_url?;

        tracing::info!("QQ: connecting to gateway WebSocket...");
        let Some(connection) =
            until_listener_cancelled(&mut cancellation, tokio_tungstenite::connect_async(&gw_url))
                .await
        else {
            return Ok(());
        };
        let (ws_stream, _) = connection?;
        let (mut write, mut read) = ws_stream.split();

        // Read Hello (opcode 10)
        let Some(hello) = until_listener_cancelled(&mut cancellation, read.next()).await else {
            return Ok(());
        };
        let hello = hello.ok_or(anyhow::anyhow!("QQ: no hello frame"))??;
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

        // Keep the heartbeat timer in this task so cancellation cannot leave
        // a detached timer sleeping after the gateway connection closes.
        let mut heartbeat =
            tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval));
        let mut stopped = false;

        'gateway: loop {
            tokio::select! {
                biased;
                () = listener_cancelled(&mut cancellation) => {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        write.send(Message::Close(None)),
                    ).await;
                    stopped = true;
                    break;
                }
                _ = heartbeat.tick() => {
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
                            if self.has_duplicate(msg_id).await {
                                continue;
                            }
                            let envelope = tokio::select! {
                                biased;
                                () = listener_cancelled(&mut cancellation) => {
                                    let _ = tokio::time::timeout(
                                        std::time::Duration::from_secs(1),
                                        write.send(Message::Close(None)),
                                    ).await;
                                    stopped = true;
                                    break 'gateway;
                                }
                                envelope = self.inbound_envelope_for_listener("C2C_MESSAGE_CREATE", d) => envelope,
                            };
                            let Some(envelope) = envelope else {
                                continue;
                            };
                            let published = tokio::select! {
                                biased;
                                () = listener_cancelled(&mut cancellation) => {
                                    let _ = tokio::time::timeout(
                                        std::time::Duration::from_secs(1),
                                        write.send(Message::Close(None)),
                                    ).await;
                                    stopped = true;
                                    break 'gateway;
                                }
                                result = sink.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope)) => result,
                            };
                            published.map_err(|error| anyhow::anyhow!(error.message))?;
                            self.mark_duplicate(msg_id).await;
                        }
                        "GROUP_AT_MESSAGE_CREATE" => {
                            let msg_id = d.get("id").and_then(|i| i.as_str()).unwrap_or("");
                            if self.has_duplicate(msg_id).await {
                                continue;
                            }
                            let envelope = tokio::select! {
                                biased;
                                () = listener_cancelled(&mut cancellation) => {
                                    let _ = tokio::time::timeout(
                                        std::time::Duration::from_secs(1),
                                        write.send(Message::Close(None)),
                                    ).await;
                                    stopped = true;
                                    break 'gateway;
                                }
                                envelope = self.inbound_envelope_for_listener("GROUP_AT_MESSAGE_CREATE", d) => envelope,
                            };
                            let Some(envelope) = envelope else {
                                continue;
                            };
                            let published = tokio::select! {
                                biased;
                                () = listener_cancelled(&mut cancellation) => {
                                    let _ = tokio::time::timeout(
                                        std::time::Duration::from_secs(1),
                                        write.send(Message::Close(None)),
                                    ).await;
                                    stopped = true;
                                    break 'gateway;
                                }
                                result = sink.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope)) => result,
                            };
                            published.map_err(|error| anyhow::anyhow!(error.message))?;
                            self.mark_duplicate(msg_id).await;
                        }
                        _ => {}
                    }
                }
            }
        }

        if stopped {
            Ok(())
        } else {
            anyhow::bail!("QQ WebSocket connection closed")
        }
    }
}

// Typed gateway message decoding and configured host-store ingestion.
use tinychannels_bus::channel::ConversationKind;

impl QQChannel {
    /// Build a typed QQ inbound envelope from a gateway message. Attachment
    /// downloads are restricted to HTTPS Tencent hosts, do not follow
    /// redirects, and are bounded before they reach host storage.
    #[cfg(test)]
    pub(crate) async fn inbound_envelope(
        &self,
        event_type: &str,
        message: &serde_json::Value,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        self.inbound_envelope_with_mode(event_type, message, false)
            .await
    }

    pub(crate) async fn inbound_envelope_for_listener(
        &self,
        event_type: &str,
        message: &serde_json::Value,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        self.inbound_envelope_with_mode(event_type, message, true)
            .await
    }

    async fn inbound_envelope_with_mode(
        &self,
        event_type: &str,
        message: &serde_json::Value,
        listener: bool,
    ) -> Option<tinychannels_bus::ChannelInboundEnvelope> {
        use crate::providers::native::inbound::{
            InboundFile, Ingest, Ingested, assemble, media_of,
        };

        let message_id = message.get("id")?.as_str()?.to_owned();
        if message_id.is_empty() || self.native.has_seen(&format!("qq:{message_id}")) {
            return None;
        }
        let (sender_id, conversation_id, kind) = match event_type {
            "C2C_MESSAGE_CREATE" => {
                let author = message.get("author")?;
                let sender = author
                    .get("user_openid")
                    .or_else(|| author.get("id"))?
                    .as_str()?;
                (
                    sender.to_owned(),
                    format!("user:{sender}"),
                    ConversationKind::Dm,
                )
            }
            "GROUP_AT_MESSAGE_CREATE" => {
                let author = message.get("author")?;
                let sender = author.get("member_openid")?.as_str()?;
                let group = message.get("group_openid")?.as_str()?;
                (
                    sender.to_owned(),
                    format!("group:{group}"),
                    ConversationKind::Group,
                )
            }
            _ => return None,
        };
        // Keep the exact legacy allowlist behavior before any attachment I/O.
        if !self.is_user_allowed(&sender_id) {
            return None;
        }
        let text = message
            .get("content")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_owned();
        let conversation = tinychannels_bus::ConversationRef {
            kind,
            id: conversation_id,
            ..Default::default()
        };
        let mut ingested = Ingested::default();
        let mut downloaded_bytes = 0_u64;
        if (listener || self.native.events.is_some())
            && self.native.media_repository.is_some()
            && let Some(attachments) = message
                .get("attachments")
                .and_then(serde_json::Value::as_array)
        {
            for (index, attachment) in attachments.iter().take(MAX_INBOUND_ATTACHMENTS).enumerate()
            {
                let Some(url) = attachment.get("url").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let remaining = INBOUND_ATTACHMENT_LIMIT.saturating_sub(downloaded_bytes);
                let Some((bytes, response_mime)) = self.download_attachment(url, remaining).await
                else {
                    continue;
                };
                downloaded_bytes += bytes.len() as u64;
                let mime = attachment
                    .get("content_type")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(&response_mime)
                    .to_owned();
                let name = attachment
                    .get("filename")
                    .or_else(|| attachment.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                Ingest {
                    provider: "qq",
                    channel: tinychannels_bus::ChannelRef {
                        id: "qq".into(),
                        account_id: None,
                    },
                    repository: self.native.media_repository.as_ref(),
                    voice_store: self.native.voice_media_store.as_ref(),
                    transcriber: self.native.transcriber.as_ref(),
                    file_bytes: INBOUND_ATTACHMENT_LIMIT,
                    voice_bytes: INBOUND_ATTACHMENT_LIMIT,
                }
                .add(
                    &mut ingested,
                    &conversation,
                    &message_id,
                    InboundFile {
                        bytes,
                        mime,
                        name,
                        duration_ms: None,
                        // QQ attachments do not distinguish voice notes from
                        // ordinary audio files in the documented event shape.
                        voice: false,
                        dedupe_key: format!("{message_id}:{index}"),
                        transcript: None,
                    },
                )
                .await;
            }
        }
        let parts = assemble(&text, None, ingested);
        if parts.is_empty() {
            return None;
        }
        if !listener {
            self.native.mark_seen(format!("qq:{message_id}"));
        }
        Some(tinychannels_bus::ChannelInboundEnvelope {
            channel: tinychannels_bus::ChannelRef {
                id: "qq".into(),
                account_id: None,
            },
            message_id: message_id.clone(),
            timestamp_unix_ms: message.get("timestamp").and_then(timestamp_unix_ms),
            conversation,
            sender: tinychannels_bus::SenderRef {
                id: sender_id,
                ..Default::default()
            },
            text,
            media: media_of(&parts),
            parts,
            raw: Some(serde_json::json!({"providerMessageId": message_id})),
            ..Default::default()
        })
    }
}
