//! A mock Linq API and recording host ports for the Linq tests.

use super::*;
use crate::providers::native::fixtures::*;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use std::sync::Arc;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub(super) const FROM: &str = "+15551234567";
pub(super) const PEER: &str = "+15557654321";
pub(super) const CHAT: &str = "chat-789";
pub(super) const SECRET: &str = "whsec";

/// The API plus a media host: messages answer `MSG-n`, attachments upload to
/// `/upload/1`, and `/media/clip` serves `clip`.
pub(super) async fn mock_api(clip: &[u8]) -> MockServer {
    let server = MockServer::start().await;
    let upload = format!("{}/upload/1", server.uri());
    Mock::given(method("POST"))
        .and(path("/attachments"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "attachment_id": "att-1",
            "upload_url": upload,
            "required_headers": {"x-amz-meta": "1"},
        })))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/upload/1"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/chats/[^/]+/messages$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "chat_id": CHAT,
            "message": {"id": "MSG-1"},
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/chats/[^/]+/typing$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/chats/[^/]+/typing$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/media/clip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(clip.to_vec()))
        .mount(&server)
        .await;
    server
}

pub(super) fn channel_for(server: &MockServer) -> LinqChannel {
    LinqChannel::with_http_client(
        "token".into(),
        FROM.into(),
        vec!["*".into()],
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_api_base(server.uri())
    .with_webhook_secret(Some(SECRET.into()))
}

pub(super) struct Rig {
    pub(super) channel: LinqChannel,
    pub(super) media: Arc<StubMedia>,
    pub(super) voice: Arc<StubVoice>,
    pub(super) events: Arc<RecordingEvents>,
}

pub(super) fn rig(server: &MockServer) -> Rig {
    let media = Arc::new(StubMedia::default());
    let voice = Arc::new(StubVoice::default());
    *voice.audio_base64.lock().unwrap() = Some(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"voice",
    ));
    let events = Arc::new(RecordingEvents::delivering());
    let channel = channel_for(server)
        .with_media_repository(media.clone())
        .with_voice_media_store(voice.clone())
        .with_transcriber(Arc::new(StubTranscriber::default()))
        .with_events(events.clone());
    Rig {
        channel,
        media,
        voice,
        events,
    }
}

/// The signature header the host forwards for `body`, signed now.
pub(super) fn signed(body: &str) -> String {
    let timestamp = chrono::Utc::now().timestamp().to_string();
    signed_at(body, &timestamp)
}

pub(super) fn signed_at(body: &str, timestamp: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(format!("{timestamp}.{body}").as_bytes());
    format!(
        "t={timestamp},v1={}",
        hex::encode(mac.finalize().into_bytes())
    )
}

/// A `message.received` webhook body with `parts`.
pub(super) fn webhook(message_id: &str, parts: Value) -> String {
    json!({
        "api_version": "v3",
        "event_type": "message.received",
        "event_id": format!("evt-{message_id}"),
        "created_at": "2025-01-15T12:00:00Z",
        "data": {
            "chat_id": CHAT,
            "from": PEER,
            "recipient_phone": FROM,
            "is_from_me": false,
            "service": "iMessage",
            "message": {"id": message_id, "parts": parts},
        },
    })
    .to_string()
}

/// Requests the API received for `path_suffix`, as JSON bodies.
pub(super) async fn bodies(
    server: &MockServer,
    http_method: &str,
    path_suffix: &str,
) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| {
            request.method.as_str() == http_method && request.url.path().ends_with(path_suffix)
        })
        .map(|request| serde_json::from_slice(&request.body).unwrap_or_default())
        .collect()
}
