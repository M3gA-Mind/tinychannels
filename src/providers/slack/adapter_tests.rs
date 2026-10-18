use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction as _, ChannelStreamingDraft,
    ChannelTyping, ChannelTypedSend,
};
use crate::providers::slack::fixtures::*;
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::channel::SendErrorKind;
use tinychannels_bus::{ContentPart, ConversationKind, MediaKind, MediaReference, OutboundPayload};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature, ExpectedScope, WireObservation, check_all_features,
};
use wiremock::MockServer;

const MOCK_TS: &str = "5.5";
const SHARE_TS: &str = "7.7";

fn channel_ref() -> tinychannels_bus::ChannelRef {
    tinychannels_bus::ChannelRef { id: "slack".into(), account_id: None }
}

fn conversation(thread: Option<&str>) -> ConversationRef {
    ConversationRef { kind: ConversationKind::Channel, id: "C1".into(), thread_id: thread.map(str::to_owned), ..Default::default() }
}

fn intent(payload: OutboundPayload) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "kit-op".into(),
        channel_id: "slack".into(),
        conversation_id: "C1".into(),
        channel_ref: Some(channel_ref()),
        conversation_ref: Some(conversation(None)),
        turn_id: Some("turn-1".into()),
        payload,
        ..Default::default()
    }
}

fn part(part: ContentPart) -> OutboundPayload {
    OutboundPayload::Parts { parts: vec![tinychannels_bus::OutboundContentPart::Content { part }] }
}

fn media_ref(kind: MediaKind) -> MediaReference {
    MediaReference { opaque_id: Some("media-1".into()), kind, ..Default::default() }
}

/// A Slack mock platform plus the real adapter, observed through wire traffic.
struct SlackWireFixture {
    slack: Arc<SlackChannel>,
    server: MockServer,
    events: Arc<RecordingEvents>,
    media: Arc<StubMedia>,
    observations: std::sync::Mutex<Vec<WireObservation>>,
    cursor: std::sync::Mutex<usize>,
}

impl SlackWireFixture {
    async fn start() -> Self {
        use base64::Engine as _;
        let server = mock_api().await;
        mount_auth(&server, "UBOT").await;
        mount(&server, "chat.postMessage", ok(json!({"ts": MOCK_TS}))).await;
        for method in ["chat.update", "chat.delete", "reactions.add", "assistant.threads.setStatus"] {
            mount(&server, method, ok(json!({}))).await;
        }
        mount(
            &server,
            "files.getUploadURLExternal",
            ok(json!({"upload_url": format!("{}/upload/F1", server.uri()), "file_id": "F1"})),
        )
        .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/upload/F1"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;
        mount(
            &server,
            "files.completeUploadExternal",
            ok(json!({"files": [{"id": "F1", "shares": {"public": {"C1": [{"ts": SHARE_TS}]}}}]})),
        )
        .await;
        let clip = mount_file(&server, "/f/clip.m4a", "audio/mp4", b"audio").await;
        let media = Arc::new(StubMedia::default());
        let voice = Arc::new(StubVoice::default());
        *voice.audio_base64.lock().unwrap() = Some(base64::engine::general_purpose::STANDARD.encode(b"voice"));
        let events = Arc::new(RecordingEvents { deliver: true, ..Default::default() });
        let slack = Arc::new(
            channel_for(&server, &["*"])
                .with_media_repository(media.clone())
                .with_voice_media_store(voice)
                .with_events(events.clone()),
        );
        *media.read.lock().unwrap() = None;
        let fixture = Self {
            slack,
            server,
            events,
            media,
            observations: Default::default(),
            cursor: Default::default(),
        };
        *fixture.clip_url.lock().unwrap() = clip;
        fixture
    }
}
