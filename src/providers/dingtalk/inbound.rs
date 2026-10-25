//! DingTalk Stream Mode registration, acknowledgements, and inbound events.

use super::*;

impl DingTalkChannel {
    pub(super) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    pub(super) fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    pub(super) fn parse_stream_data(frame: &serde_json::Value) -> Option<serde_json::Value> {
        match frame.get("data") {
            Some(serde_json::Value::String(raw)) => serde_json::from_str(raw).ok(),
            Some(serde_json::Value::Object(_)) => frame.get("data").cloned(),
            _ => None,
        }
    }

    pub(super) fn resolve_chat_id(data: &serde_json::Value, sender_id: &str) -> String {
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
    pub(super) async fn register_connection(&self) -> anyhow::Result<GatewayResponse> {
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

    pub(super) fn begin_listener(
        &self,
    ) -> anyhow::Result<(Uuid, tokio::sync::watch::Receiver<bool>)> {
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
    pub(super) async fn listen_owned(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
        mut cancellation: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> anyhow::Result<()> {
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
                        result = sink.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope)) => result,
                    };
                    published.map_err(|error| {
                        anyhow::anyhow!("DingTalk typed ingress rejected: {}", error.message)
                    })?;
                    self.native
                        .mark_seen(format!("dingtalk:{provider_message_id}"));
                    if !send_callback_ack(&mut write, &frame).await {
                        break 'stream;
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

pub(super) fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("building the DingTalk attachment HTTP client should succeed")
}

// Typed callback decoding and host-store ingestion.
use super::native::{MAX_MEDIA_BYTES, conversation_context_key};
use crate::providers::native::inbound::{InboundFile, Ingest, Ingested};
use tinychannels_bus::channel::{
    ChannelInboundEnvelope, ConversationKind, ConversationRef, SenderRef,
};
use tinychannels_bus::{VoiceTranscript, VoiceTranscriptSource, VoiceTranscriptState};

impl DingTalkChannel {
    pub(super) async fn inbound_envelope_for_listener(
        &self,
        data: &serde_json::Value,
    ) -> Option<ChannelInboundEnvelope> {
        self.inbound_envelope_with_mode(data).await
    }

    async fn inbound_envelope_with_mode(
        &self,
        data: &serde_json::Value,
    ) -> Option<ChannelInboundEnvelope> {
        use crate::providers::native::inbound::{assemble, media_of};

        let message_id = data.get("msgId")?.as_str()?.to_owned();
        if message_id.is_empty() || self.native.has_seen(&format!("dingtalk:{message_id}")) {
            return None;
        }
        let sender_id = data.get("senderStaffId")?.as_str()?.to_owned();
        if sender_id.is_empty() || !self.is_user_allowed(&sender_id) {
            return None;
        }
        let is_direct = data
            .get("conversationType")
            .and_then(|value| {
                value
                    .as_str()
                    .map(|value| value == "1")
                    .or_else(|| value.as_i64().map(|value| value == 1))
            })
            .unwrap_or(true);
        let conversation_id = data
            .get("conversationId")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(&sender_id)
            .to_owned();
        let conversation = ConversationRef {
            kind: if is_direct {
                ConversationKind::Dm
            } else {
                ConversationKind::Group
            },
            id: conversation_id.clone(),
            ..Default::default()
        };
        self.robot_contexts.write().await.insert(
            conversation_context_key(conversation.kind, &conversation_id),
            serde_json::json!({
                "robotCode": data.get("robotCode").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "senderId": data.get("senderId").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "senderStaffId": data.get("senderStaffId").and_then(serde_json::Value::as_str).unwrap_or_default(),
            }),
        );
        let message_type = data
            .get("msgtype")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let content = data.get("content").unwrap_or(&serde_json::Value::Null);
        let text = data
            .pointer("/text/content")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_owned();
        let mut ingested = Ingested::default();
        let voice = message_type == "audio";
        let can_ingest_file =
            self.native.media_repository.is_some() && matches!(message_type, "picture" | "file");
        let can_ingest_voice = self.native.voice_media_store.is_some() && voice;
        if can_ingest_file || can_ingest_voice {
            let download_code = if message_type == "picture" {
                content
                    .get("pictureDownloadCode")
                    .or_else(|| content.get("downloadCode"))
            } else {
                content.get("downloadCode")
            }
            .and_then(serde_json::Value::as_str)?;
            let robot_code = data.get("robotCode")?.as_str()?;
            let (bytes, mime) = self
                .download_media(download_code, robot_code, MAX_MEDIA_BYTES)
                .await?;
            let image = message_type == "picture";
            let transcript = content
                .get("recognition")
                .and_then(serde_json::Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(|text| VoiceTranscript {
                    text: text.to_owned(),
                    language: None,
                    state: VoiceTranscriptState::Complete,
                    source: VoiceTranscriptSource::Provider,
                });
            Ingest {
                provider: "dingtalk",
                channel: tinychannels_bus::ChannelRef {
                    id: "dingtalk".into(),
                    account_id: Some(self.client_id.clone()),
                },
                repository: self.native.media_repository.as_ref(),
                voice_store: self.native.voice_media_store.as_ref(),
                transcriber: self.native.transcriber.as_ref(),
                file_bytes: MAX_MEDIA_BYTES,
                voice_bytes: MAX_MEDIA_BYTES,
            }
            .add(
                &mut ingested,
                &conversation,
                &message_id,
                InboundFile {
                    bytes,
                    mime: if mime.is_empty() {
                        if image {
                            "image/jpeg"
                        } else {
                            "application/octet-stream"
                        }
                        .into()
                    } else {
                        mime
                    },
                    name: content
                        .get("fileName")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    duration_ms: None,
                    voice,
                    dedupe_key: format!("{message_id}:0"),
                    transcript,
                },
            )
            .await;
        }
        let parts = assemble(&text, None, ingested);
        if parts.is_empty() {
            return None;
        }
        Some(ChannelInboundEnvelope {
            channel: tinychannels_bus::ChannelRef {
                id: "dingtalk".into(),
                account_id: Some(self.client_id.clone()),
            },
            message_id: message_id.clone(),
            timestamp_unix_ms: data.get("createAt").and_then(serde_json::Value::as_u64),
            conversation,
            sender: SenderRef {
                id: sender_id,
                name: data
                    .get("senderNick")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                ..Default::default()
            },
            text,
            media: media_of(&parts),
            parts,
            raw: Some(serde_json::json!({
                "providerMessageId": message_id,
                "messageType": message_type,
            })),
            ..Default::default()
        })
    }
}
