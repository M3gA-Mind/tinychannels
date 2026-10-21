use super::SignalChannel;
use crate::channel::ChannelInboundSink;
use futures_util::StreamExt;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::watch;

// signal-cli SSE event JSON shapes.

#[derive(Debug, Deserialize)]
pub(super) struct SseEnvelope {
    #[serde(default)]
    pub(super) envelope: Option<Envelope>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct Envelope {
    #[serde(default)]
    pub(super) source: Option<String>,
    #[serde(rename = "sourceNumber", default)]
    pub(super) source_number: Option<String>,
    /// The sender's profile name, as signal-cli reports it.
    #[serde(rename = "sourceName", default)]
    pub(super) source_name: Option<String>,
    #[serde(rename = "dataMessage", default)]
    pub(super) data_message: Option<DataMessage>,
    #[serde(rename = "storyMessage", default)]
    pub(super) story_message: Option<serde_json::Value>,
    #[serde(default)]
    pub(super) timestamp: Option<u64>,
    #[serde(rename = "receiptMessage", default)]
    pub(super) receipt_message: Option<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct DataMessage {
    #[serde(default)]
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) timestamp: Option<u64>,
    #[serde(rename = "groupInfo", default)]
    pub(super) group_info: Option<GroupInfo>,
    #[serde(default)]
    pub(super) attachments: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub(super) reaction: Option<serde_json::Value>,
    #[serde(rename = "remoteDelete", default)]
    pub(super) remote_delete: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GroupInfo {
    #[serde(rename = "groupId", default)]
    pub(super) group_id: Option<String>,
}

async fn sleep_or_cancel(cancellation: &mut watch::Receiver<bool>, seconds: u64) -> bool {
    tokio::select! {
        biased;
        _ = cancellation.wait_for(|value| *value) => false,
        _ = tokio::time::sleep(Duration::from_secs(seconds)) => true,
    }
}

impl SignalChannel {
    async fn dispatch_sse_envelope(
        &self,
        envelope: &Envelope,
        sink: &(dyn ChannelInboundSink + Send + Sync),
        cancellation: &mut watch::Receiver<bool>,
    ) -> anyhow::Result<Option<bool>> {
        self.dispatch_typed_until_cancel(envelope, sink, cancellation)
            .await
    }

    pub(super) async fn listen_typed(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
        cancellation: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.listen_loop(sink, cancellation).await
    }

    async fn listen_loop(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
        mut cancellation: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let mut url = reqwest::Url::parse(&format!("{}/api/v1/events", self.http_url))?;
        url.query_pairs_mut().append_pair("account", &self.account);

        tracing::info!("Signal channel listening via SSE on {}...", self.http_url);

        let mut retry_delay_secs = 2u64;
        let max_delay_secs = 60u64;

        loop {
            if *cancellation.borrow() {
                return Ok(());
            }
            let request = self
                .http_client()
                .get(url.clone())
                .header("Accept", "text/event-stream")
                .send();
            let resp = tokio::select! {
                biased;
                _ = cancellation.wait_for(|value| *value) => return Ok(()),
                response = request => response,
            };

            let resp = match resp {
                Ok(r) if r.status().is_success() => r,
                Ok(r) => {
                    let status = r.status();
                    let body = tokio::select! {
                        biased;
                        _ = cancellation.wait_for(|value| *value) => return Ok(()),
                        body = r.text() => body.unwrap_or_default(),
                    };
                    tracing::warn!("Signal SSE returned {status}: {body}");
                    if !sleep_or_cancel(&mut cancellation, retry_delay_secs).await {
                        return Ok(());
                    }
                    retry_delay_secs = (retry_delay_secs * 2).min(max_delay_secs);
                    continue;
                }
                Err(e) => {
                    tracing::warn!("Signal SSE connect error: {e}, retrying...");
                    if !sleep_or_cancel(&mut cancellation, retry_delay_secs).await {
                        return Ok(());
                    }
                    retry_delay_secs = (retry_delay_secs * 2).min(max_delay_secs);
                    continue;
                }
            };

            retry_delay_secs = 2;

            let mut bytes_stream = resp.bytes_stream();
            let mut buffer = String::new();
            let mut current_data = String::new();

            loop {
                let next_chunk = tokio::select! {
                    biased;
                    _ = cancellation.wait_for(|value| *value) => return Ok(()),
                    chunk = bytes_stream.next() => chunk,
                };
                let Some(chunk) = next_chunk else {
                    break;
                };
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::debug!("Signal SSE chunk error, reconnecting: {e}");
                        break;
                    }
                };

                let text = match String::from_utf8(chunk.to_vec()) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::debug!("Signal SSE invalid UTF-8, skipping chunk: {}", e);
                        continue;
                    }
                };

                buffer.push_str(&text);

                while let Some(newline_pos) = buffer.find('\n') {
                    let line = buffer[..newline_pos].trim_end_matches('\r').to_string();
                    buffer = buffer[newline_pos + 1..].to_string();

                    // Skip SSE comments (keepalive)
                    if line.starts_with(':') {
                        continue;
                    }

                    if line.is_empty() {
                        // Empty line = event boundary, dispatch accumulated data
                        if !current_data.is_empty() {
                            match serde_json::from_str::<SseEnvelope>(&current_data) {
                                Ok(sse) => {
                                    if let Some(ref envelope) = sse.envelope {
                                        match self
                                            .dispatch_sse_envelope(
                                                envelope,
                                                sink,
                                                &mut cancellation,
                                            )
                                            .await?
                                        {
                                            Some(true) => {}
                                            Some(false) | None => return Ok(()),
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("Signal SSE parse skip: {e}");
                                }
                            }
                            current_data.clear();
                        }
                    } else if let Some(data) = line.strip_prefix("data:") {
                        if !current_data.is_empty() {
                            current_data.push('\n');
                        }
                        current_data.push_str(data.trim_start());
                    }
                    // Ignore "event:", "id:", "retry:" lines
                }
            }

            if !current_data.is_empty() {
                match serde_json::from_str::<SseEnvelope>(&current_data) {
                    Ok(sse) => {
                        if let Some(ref envelope) = sse.envelope {
                            match self
                                .dispatch_sse_envelope(envelope, sink, &mut cancellation)
                                .await?
                            {
                                Some(true) => {}
                                Some(false) | None => return Ok(()),
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Signal SSE trailing parse skip: {e}");
                    }
                }
            }

            tracing::debug!("Signal SSE stream ended, reconnecting...");
            if !sleep_or_cancel(&mut cancellation, 2).await {
                return Ok(());
            }
        }
    }
}
