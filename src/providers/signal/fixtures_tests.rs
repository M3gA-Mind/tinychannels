//! A mock signal-cli daemon and recording host ports for the Signal tests.

use super::*;
use crate::providers::native::fixtures::*;
use base64::Engine as _;
use serde_json::{Value, json};
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

pub(super) const ACCOUNT: &str = "+15550000";
pub(super) const PEER: &str = "+15551111";
pub(super) const SENT_TS: u64 = 1_700_000_000_123;

/// A daemon answering `send`, `sendTyping`, `sendReaction`, `remoteDelete` and
/// `getAttachment`; `getAttachment` serves `attachment`.
pub(super) async fn mock_daemon(attachment: &[u8]) -> MockServer {
    let server = MockServer::start().await;
    let data = base64::engine::general_purpose::STANDARD.encode(attachment);
    Mock::given(method("POST"))
        .and(path("/api/v1/rpc"))
        .respond_with(move |request: &Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let id = body["id"].clone();
            match body["method"].as_str().unwrap_or_default() {
                "send" => ResponseTemplate::new(200).set_body_json(
                    json!({"jsonrpc": "2.0", "result": {"timestamp": SENT_TS}, "id": id}),
                ),
                "getAttachment" => ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc": "2.0", "result": {"data": data}, "id": id})),
                "fail" => ResponseTemplate::new(200).set_body_json(
                    json!({"jsonrpc": "2.0", "error": {"code": -1, "message": "boom"}, "id": id}),
                ),
                _ => ResponseTemplate::new(201),
            }
        })
        .mount(&server)
        .await;
    server
}

pub(super) fn channel_for(server: &MockServer) -> SignalChannel {
    SignalChannel::with_http_client(
        server.uri(),
        ACCOUNT.into(),
        None,
        vec!["*".into()],
        false,
        false,
        Client::builder().no_proxy().build().unwrap(),
    )
}

/// A channel with media, voice, transcription and recorded ingress attached.
pub(super) struct Rig {
    pub(super) channel: SignalChannel,
    pub(super) media: Arc<StubMedia>,
    pub(super) voice: Arc<StubVoice>,
    pub(super) events: Arc<RecordingEvents>,
}

pub(super) fn rig(server: &MockServer) -> Rig {
    let media = Arc::new(StubMedia::default());
    let voice = Arc::new(StubVoice::default());
    *voice.audio_base64.lock().unwrap() =
        Some(base64::engine::general_purpose::STANDARD.encode(b"voice"));
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

/// Every JSON-RPC call of `method_name` the daemon received, as its params.
pub(super) async fn calls(server: &MockServer, method_name: &str) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|request| {
            let body: Value = serde_json::from_slice(&request.body).ok()?;
            (body["method"] == method_name).then(|| body["params"].clone())
        })
        .collect()
}

/// An SSE envelope for a data message from `PEER`.
pub(super) fn data_envelope(data: Value) -> Envelope {
    serde_json::from_value(json!({
        "sourceNumber": PEER,
        "sourceName": "Ada",
        "timestamp": 1_700_000_000_500u64,
        "dataMessage": data,
    }))
    .unwrap()
}
