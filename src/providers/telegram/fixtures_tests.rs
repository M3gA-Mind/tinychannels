//! Shared test fixtures for the Telegram channel: a local mock Bot API
//! server, recording host capabilities and request inspection helpers.

#[allow(unused_imports)]
use super::*;
use crate::host::{AllowlistStore, EventSink, Transcriber, TranscriptionRequest};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// In-process loopback Bot API for provider tests that need to inject polls
/// and inspect the messages the channel sends back.
#[derive(Clone)]
pub(super) struct BotApiFake {
    base: String,
    state: Arc<BotApiFakeState>,
}

#[derive(Default)]
struct BotApiFakeState {
    updates: Mutex<VecDeque<serde_json::Value>>,
    poll_failures: Mutex<VecDeque<(u16, serde_json::Value)>>,
    sent: Mutex<Vec<(String, serde_json::Value)>>,
}

impl BotApiFake {
    pub(super) async fn start() -> Self {
        use axum::{Json, Router, body::Bytes, extract::State, http::StatusCode, routing::post};

        async fn handle(
            State(state): State<Arc<BotApiFakeState>>,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
            body: Bytes,
        ) -> (StatusCode, Json<serde_json::Value>) {
            let method = uri
                .path()
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_string();
            if method == "getUpdates" {
                if let Some((status, body)) = state.poll_failures.lock().unwrap().pop_front() {
                    return (StatusCode::from_u16(status).unwrap(), Json(body));
                }
                let updates = state.updates.lock().unwrap().drain(..).collect::<Vec<_>>();
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({ "ok": true, "result": updates })),
                );
            }

            let request = serde_json::from_slice(&body).unwrap_or_else(|_| {
                serde_json::json!({
                    "content_type": headers
                        .get(axum::http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok()),
                    "raw_body": String::from_utf8_lossy(&body).into_owned()
                })
            });
            state.sent.lock().unwrap().push((method, request));
            (
                StatusCode::OK,
                Json(serde_json::json!({ "ok": true, "result": { "message_id": 900 } })),
            )
        }

        let state = Arc::new(BotApiFakeState::default());
        let app = Router::new()
            .route("/{*path}", post(handle))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Bot API fake");
        let address = listener.local_addr().expect("Bot API fake address");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve Bot API fake");
        });
        Self {
            base: format!("http://{address}"),
            state,
        }
    }

    pub(super) fn channel(&self, allowed: &[&str]) -> TelegramChannel {
        let mut channel = TelegramChannel::new(
            TOKEN.into(),
            allowed.iter().map(|value| (*value).to_string()).collect(),
            false,
        );
        channel.api_base = self.base.clone();
        channel
    }

    pub(super) fn inject(&self, update: serde_json::Value) {
        self.state.updates.lock().unwrap().push_back(update);
    }

    pub(super) fn fail_next_poll(&self, status: u16, body: serde_json::Value) {
        self.state
            .poll_failures
            .lock()
            .unwrap()
            .push_back((status, body));
    }

    pub(super) fn sent(&self) -> Vec<(String, serde_json::Value)> {
        self.state.sent.lock().unwrap().clone()
    }
}

pub(super) const TOKEN: &str = "test-token";

/// Start a local mock Telegram Bot API server.
pub(super) async fn mock_api() -> MockServer {
    MockServer::start().await
}

/// Build a channel aimed at `server` with the given allowlist.
pub(super) fn channel_for(server: &MockServer, allowed: &[&str]) -> TelegramChannel {
    let mut ch = TelegramChannel::new(
        TOKEN.into(),
        allowed.iter().map(|s| s.to_string()).collect(),
        false,
    );
    ch.api_base = server.uri();
    ch
}

/// The Bot API path for `method_name` under the fixture token.
pub(super) fn api_path(method_name: &str) -> String {
    format!("/bot{TOKEN}/{method_name}")
}

/// A Bot API `ok: true` response carrying `result`.
pub(super) fn ok_with(result: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": true, "result": result }))
}

/// A Bot API `ok: true` response.
pub(super) fn ok() -> ResponseTemplate {
    ok_with(serde_json::json!({ "message_id": 1 }))
}

/// Mount `response` for `POST /bot<token>/<method_name>`.
pub(super) async fn mount_post(server: &MockServer, method_name: &str, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path(api_path(method_name)))
        .respond_with(response)
        .mount(server)
        .await;
}

/// JSON bodies of every request the server received for `method_name`.
pub(super) async fn json_bodies(server: &MockServer, method_name: &str) -> Vec<serde_json::Value> {
    let wanted = api_path(method_name);
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|req| req.url.path() == wanted)
        .map(|req| serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null))
        .collect()
}

/// Raw (multipart) bodies of every request the server received for `method_name`.
pub(super) async fn raw_bodies(server: &MockServer, method_name: &str) -> Vec<String> {
    let wanted = api_path(method_name);
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|req| req.url.path() == wanted)
        .map(|req| String::from_utf8_lossy(&req.body).into_owned())
        .collect()
}

/// `text` fields of every `sendMessage` the server received.
pub(super) async fn sent_texts(server: &MockServer) -> Vec<String> {
    json_bodies(server, "sendMessage")
        .await
        .into_iter()
        .filter_map(|body| {
            body.get("text")
                .and_then(|t| t.as_str())
                .map(str::to_string)
        })
        .collect()
}

/// A base URL on a loopback port nothing listens on, so requests fail fast
/// with a connection error.
pub(super) fn dead_base() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe port");
    let addr = listener.local_addr().expect("probe addr");
    drop(listener);
    format!("http://{addr}")
}

/// Event sink that records every published event.
#[derive(Default)]
pub(super) struct RecordingEvents {
    pub(super) events: Mutex<Vec<(String, String, serde_json::Value)>>,
}

#[async_trait]
impl EventSink for RecordingEvents {
    async fn publish(
        &self,
        domain: &str,
        event: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push((domain.to_string(), event.to_string(), payload));
        Ok(())
    }
}

/// Transcriber returning a fixed transcript (or error) and recording requests.
pub(super) struct FixedTranscriber {
    pub(super) result: Result<String, String>,
    pub(super) requests: Mutex<Vec<TranscriptionRequest>>,
}

impl FixedTranscriber {
    pub(super) fn ok(text: &str) -> Arc<Self> {
        Arc::new(Self {
            result: Ok(text.to_string()),
            requests: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn failing(error: &str) -> Arc<Self> {
        Arc::new(Self {
            result: Err(error.to_string()),
            requests: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl Transcriber for FixedTranscriber {
    fn name(&self) -> &str {
        "fixed"
    }

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> anyhow::Result<crate::host::TranscriptionResult> {
        self.requests.lock().unwrap().push(request);
        match &self.result {
            Ok(text) => Ok(crate::host::TranscriptionResult {
                text: text.clone(),
                ..Default::default()
            }),
            Err(error) => Err(anyhow::anyhow!(error.clone())),
        }
    }
}

/// Allowlist store that records persisted identities, optionally failing.
#[derive(Default)]
pub(super) struct RecordingAllowlist {
    pub(super) fail: bool,
    pub(super) persisted: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl AllowlistStore for RecordingAllowlist {
    async fn persist_allowed_identity(&self, channel: &str, identity: &str) -> anyhow::Result<()> {
        self.persisted
            .lock()
            .unwrap()
            .push((channel.to_string(), identity.to_string()));
        if self.fail {
            anyhow::bail!("read-only config");
        }
        Ok(())
    }
}
