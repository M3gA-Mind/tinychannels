//! Shared fixtures for the Slack tests: a mock Web API server and recording
//! host ports.

use super::*;
use crate::host::{
    EventSink, MediaMetadata, MediaRepository, MediaScope, MediaStoreRequest, StoredMedia,
    VoiceMediaRequest, VoiceMediaStore,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Mutex;
use tinychannels_bus::{ChannelEvent, ChannelIngress, MediaKind, MediaReference};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub(super) const TOKEN: &str = "bot-test-token";

pub(super) async fn mock_api() -> MockServer {
    MockServer::start().await
}

/// A channel aimed at `server`, allowing `allowed` senders.
pub(super) fn channel_for(server: &MockServer, allowed: &[&str]) -> SlackChannel {
    SlackChannel::new(
        TOKEN.into(),
        Some("C1".into()),
        allowed.iter().map(|user| (*user).to_owned()).collect(),
    )
    .with_api_base(format!("{}/api", server.uri()))
    .with_timing(
        std::time::Duration::from_millis(10),
        std::time::Duration::from_millis(10),
    )
}

/// Answer `POST /api/<method>` with HTTP 200 and `body`.
pub(super) async fn mount(server: &MockServer, method_name: &str, body: Value) {
    mount_status(server, method_name, 200, body).await;
}

pub(super) async fn mount_status(server: &MockServer, method_name: &str, status: u16, body: Value) {
    Mock::given(method("POST"))
        .and(path(format!("/api/{method_name}")))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(server)
        .await;
}

/// Request bodies sent to `POST /api/<method>`, parsed as JSON or as a form.
pub(super) async fn bodies(server: &MockServer, method_name: &str) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|request| request.url.path() == format!("/api/{method_name}"))
        .map(|request| {
            serde_json::from_slice(&request.body).unwrap_or_else(|_| {
                let pairs: serde_json::Map<String, Value> =
                    url::form_urlencoded::parse(&request.body)
                        .map(|(k, v)| (k.into_owned(), Value::String(v.into_owned())))
                        .collect();
                Value::Object(pairs)
            })
        })
        .collect()
}

/// Answer `GET /api/auth.test` with the bot's user ID.
pub(super) async fn mount_auth(server: &MockServer, user_id: &str) {
    Mock::given(method("GET"))
        .and(path("/api/auth.test"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"ok": true, "user_id": user_id})),
        )
        .mount(server)
        .await;
}

/// Serve `bytes` at `GET <path>` and return the absolute URL.
pub(super) async fn mount_file(
    server: &MockServer,
    file_path: &str,
    content_type: &str,
    bytes: &[u8],
) -> String {
    Mock::given(method("GET"))
        .and(path(file_path))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", content_type)
                .set_body_bytes(bytes.to_vec()),
        )
        .mount(server)
        .await;
    format!("{}{file_path}", server.uri())
}

pub(super) fn ok(extra: Value) -> Value {
    let mut body = json!({"ok": true});
    if let (Some(base), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        base.extend(extra.clone());
    }
    body
}

/// Host media repository with one canned stored file.
#[derive(Default)]
pub(super) struct StubMedia {
    pub(super) stored: Mutex<Vec<MediaStoreRequest>>,
    pub(super) fail_store: bool,
    pub(super) read: Mutex<Option<StoredMedia>>,
    pub(super) scopes: Mutex<Vec<MediaScope>>,
}

#[async_trait]
impl MediaRepository for StubMedia {
    async fn store_media(&self, request: MediaStoreRequest) -> anyhow::Result<MediaReference> {
        if self.fail_store {
            anyhow::bail!("storage unavailable");
        }
        let MediaMetadata {
            kind,
            content_type,
            file_name,
            ..
        } = request.metadata.clone();
        self.stored.lock().unwrap().push(request);
        Ok(MediaReference {
            opaque_id: Some("media-1".into()),
            kind,
            content_type: Some(content_type),
            file_name,
            ..Default::default()
        })
    }

    async fn read_media(
        &self,
        scope: MediaScope,
        _id: String,
        _max: u64,
    ) -> anyhow::Result<StoredMedia> {
        self.scopes.lock().unwrap().push(scope);
        self.read
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("not found"))
    }
}

pub(super) fn stored_media(kind: MediaKind, content_type: &str, bytes: &[u8]) -> StoredMedia {
    StoredMedia {
        media: MediaReference {
            opaque_id: Some("media-1".into()),
            kind,
            ..Default::default()
        },
        content_type: content_type.into(),
        file_name: Some("report.pdf".into()),
        bytes: bytes.to_vec(),
    }
}

/// Host voice store that records stored clips and serves one canned upload.
#[derive(Default)]
pub(super) struct StubVoice {
    pub(super) stored: Mutex<Vec<VoiceMediaRequest>>,
    pub(super) empty_reference: bool,
    pub(super) fail_store: bool,
    pub(super) audio_base64: Mutex<Option<String>>,
}

#[async_trait]
impl VoiceMediaStore for StubVoice {
    async fn store_voice(
        &self,
        request: VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        if self.fail_store {
            anyhow::bail!("storage unavailable");
        }
        self.stored.lock().unwrap().push(request);
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media: MediaReference {
                opaque_id: (!self.empty_reference).then(|| "voice-1".into()),
                url: Some("https://private.invalid/x".into()),
                ..Default::default()
            },
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        let audio_base64 = self
            .audio_base64
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64,
            mime_type: "audio/mp4".into(),
            file_name: Some("reply.m4a".into()),
        })
    }
}

/// Event sink that records everything published; `deliver` is the answer
/// returned for inbound messages.
#[derive(Default)]
pub(super) struct RecordingEvents {
    pub(super) ingress: Mutex<Vec<ChannelIngress>>,
    pub(super) deliver: bool,
    pub(super) fail: bool,
}

impl RecordingEvents {
    pub(super) fn events(&self) -> Vec<ChannelEvent> {
        self.ingress
            .lock()
            .unwrap()
            .iter()
            .filter_map(|item| match item {
                ChannelIngress::Event(event) => Some(event.clone()),
                ChannelIngress::Inbound(_) => None,
            })
            .collect()
    }

    pub(super) fn inbound(&self) -> Vec<tinychannels_bus::ChannelInboundEnvelope> {
        self.ingress
            .lock()
            .unwrap()
            .iter()
            .filter_map(|item| match item {
                ChannelIngress::Inbound(envelope) => Some(envelope.clone()),
                ChannelIngress::Event(_) => None,
            })
            .collect()
    }
}

#[async_trait]
impl EventSink for RecordingEvents {
    async fn publish(&self, _domain: &str, _kind: &str, _payload: Value) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish_ingress(&self, ingress: ChannelIngress) -> anyhow::Result<bool> {
        if self.fail {
            anyhow::bail!("sink unavailable");
        }
        self.ingress.lock().unwrap().push(ingress);
        Ok(self.deliver)
    }
}
