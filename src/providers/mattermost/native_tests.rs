use super::*;
use crate::channel::{ChannelAdapter, ChannelTypedSend};
use crate::providers::native::NativeProvider;
use crate::providers::native::fixtures::RecordingEvents;
use crate::providers::native::fixtures::{StubMedia, StubVoice, stored_media};
use serde_json::json;
use tinychannels_bus::channel::{ConversationRef, SendErrorKind};
use tinychannels_bus::{
    ChannelOutboundMessage, ChannelRef, ContentPart, ConversationKind, MediaKind, MediaReference,
    OutboundContentPart, OutboundVoice,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn image_message(conversation_id: &str) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "mattermost".into(),
        conversation_id: conversation_id.into(),
        channel_ref: Some(ChannelRef {
            id: "mattermost".into(),
            account_id: Some("account-1".into()),
        }),
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Channel,
            id: conversation_id.into(),
            ..Default::default()
        }),
        turn_id: Some("turn-1".into()),
        reply_to_id: None,
        thread_id: Some("root-1".into()),
        idempotency_key: "send-1".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("opaque-image".into()),
                    kind: MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("diagram".into()),
                alt_text: None,
            },
        }],
    }
}

fn channel(server: &MockServer, media: Arc<StubMedia>) -> MattermostChannel {
    MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_media_repository(media)
}

#[tokio::test]
async fn typed_image_upload_uses_scoped_bytes_and_posts_caption_with_real_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/files"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "file_infos": [{"id": "file-1"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "post-1"})))
        .expect(1)
        .mount(&server)
        .await;

    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Image,
        "image/png",
        b"opaque-host-image-bytes",
    ));
    let channel = channel(&server, media.clone());
    let result = ChannelTypedSend::send_typed(&channel, &image_message("channel-1"))
        .await
        .unwrap();

    assert_eq!(
        result.receipt.primary_platform_message_id.as_deref(),
        Some("post-1")
    );
    assert_eq!(result.receipt.parts[0].kind, MessageReceiptPartKind::Media);
    assert_eq!(media.scopes.lock().unwrap()[0].turn_id, "turn-1");
    let requests = server.received_requests().await.unwrap();
    let upload = requests
        .iter()
        .find(|r| r.url.path() == "/api/v4/files")
        .unwrap();
    let upload_body = String::from_utf8_lossy(&upload.body);
    assert!(upload_body.contains("channel_id"));
    assert!(upload_body.contains("channel-1"));
    assert!(upload_body.contains("opaque-host-image-bytes"));
    let post = requests
        .iter()
        .find(|r| r.url.path() == "/api/v4/posts")
        .unwrap();
    let post_body: serde_json::Value = serde_json::from_slice(&post.body).unwrap();
    assert_eq!(post_body["channel_id"], "channel-1");
    assert_eq!(post_body["root_id"], "root-1");
    assert_eq!(post_body["message"], "diagram");
    assert_eq!(post_body["file_ids"], json!(["file-1"]));
}

#[tokio::test]
async fn typed_voice_upload_uses_host_audio_and_returns_voice_receipt() {
    use base64::Engine as _;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/files"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "file_infos": [{"id": "voice-file-1"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "voice-post-1"})))
        .expect(1)
        .mount(&server)
        .await;

    let voice = Arc::new(StubVoice::default());
    *voice.audio_base64.lock().unwrap() =
        Some(base64::engine::general_purpose::STANDARD.encode(b"host-audio-bytes"));
    let channel = channel(&server, Arc::new(StubMedia::default())).with_voice_media_store(voice);
    assert!(
        NativeProvider::declared_capabilities(&channel)
            .voice_out
            .supported
    );
    let mut message = image_message("channel-1");
    message.parts = vec![OutboundContentPart::Voice {
        voice: OutboundVoice::Media {
            media: MediaReference {
                opaque_id: Some("opaque-voice".into()),
                kind: MediaKind::Audio,
                ..Default::default()
            },
            audio_base64: None,
            codec: Some("m4a_aac".into()),
            duration_ms: Some(1500),
            waveform: None,
        },
    }];

    let result = ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap();
    assert_eq!(result.receipt.parts[0].kind, MessageReceiptPartKind::Voice);
    assert_eq!(
        result.receipt.primary_platform_message_id.as_deref(),
        Some("voice-post-1")
    );
    let requests = server.received_requests().await.unwrap();
    let upload = requests
        .iter()
        .find(|r| r.url.path() == "/api/v4/files")
        .unwrap();
    let upload_body = String::from_utf8_lossy(&upload.body);
    assert!(upload_body.contains("host-audio-bytes"));
    assert!(upload_body.contains("reply.m4a"));
    let post = requests
        .iter()
        .find(|r| r.url.path() == "/api/v4/posts")
        .unwrap();
    let post_body: serde_json::Value = serde_json::from_slice(&post.body).unwrap();
    assert_eq!(post_body["file_ids"], json!(["voice-file-1"]));
}

#[tokio::test]
async fn upload_rejection_is_known_unsent_and_does_not_create_a_post() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/files"))
        .respond_with(ResponseTemplate::new(413))
        .expect(1)
        .mount(&server)
        .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"image"));
    let error = ChannelTypedSend::send_typed(&channel(&server, media), &image_message("chan"))
        .await
        .unwrap_err();
    assert!(!error.sent_before_error);
    assert!(!error.retryable);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/api/v4/files");
}

#[tokio::test]
async fn final_post_server_error_preserves_unknown_delivery_state() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/files"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "file_infos": [{"id": "file-1"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"image"));
    let error = ChannelTypedSend::send_typed(&channel(&server, media), &image_message("chan"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Unknown);
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].url.path(), "/api/v4/posts");
}

#[tokio::test]
async fn upload_without_a_file_id_does_not_attempt_to_create_a_post() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/files"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"file_infos": [{}]})))
        .expect(1)
        .mount(&server)
        .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"image"));

    let error = ChannelTypedSend::send_typed(&channel(&server, media), &image_message("chan"))
        .await
        .unwrap_err();

    assert_eq!(error.kind, SendErrorKind::Unknown);
    assert!(!error.sent_before_error);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn accepted_post_without_an_id_is_reported_as_unknown_delivery() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let mut message = image_message("chan");
    message.parts = vec![OutboundContentPart::Content {
        part: ContentPart::Text {
            text: "hello".into(),
            markdown_dialect: None,
        },
    }];

    let error =
        ChannelTypedSend::send_typed(&channel(&server, Arc::new(StubMedia::default())), &message)
            .await
            .unwrap_err();

    assert_eq!(error.kind, SendErrorKind::Unknown);
    assert!(error.sent_before_error);
    assert!(!error.retryable);
}

#[tokio::test]
async fn packed_thread_address_cannot_conflict_with_the_canonical_thread() {
    let server = MockServer::start().await;
    let channel = channel(&server, Arc::new(StubMedia::default()));
    let mut message = image_message("channel-1:root-1");
    message.conversation_ref.as_mut().unwrap().thread_id = Some("root-2".into());

    let error = NativeProvider::normalized_target(&channel, &message)
        .err()
        .expect("conflicting thread roots must be rejected before upload");

    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(!error.sent_before_error);
}

#[tokio::test]
async fn descriptor_advertises_voice_attachment_only_when_host_voice_storage_exists() {
    let server = MockServer::start().await;
    let without_voice = channel(&server, Arc::new(StubMedia::default()));
    assert!(
        !ChannelAdapter::descriptor(&without_voice)
            .metadata
            .contains_key("voiceOut")
    );

    let with_voice = channel(&server, Arc::new(StubMedia::default()))
        .with_voice_media_store(Arc::new(StubVoice::default()));
    assert_eq!(
        ChannelAdapter::descriptor(&with_voice)
            .metadata
            .get("voiceOut")
            .map(String::as_str),
        Some("attachment")
    );
}

#[test]
fn media_capabilities_require_host_scoped_storage() {
    let bare = MattermostChannel::new(
        "https://mattermost.invalid".into(),
        "token".into(),
        None,
        vec![],
        true,
        false,
    );
    let capabilities = NativeProvider::declared_capabilities(&bare);
    assert!(!capabilities.content.image.outbound);
    assert!(!capabilities.content.video.outbound);
    assert!(!capabilities.content.audio.outbound);
    assert!(!capabilities.content.file.outbound);

    let host_media = MattermostChannel::new(
        "https://mattermost.invalid".into(),
        "token".into(),
        None,
        vec![],
        true,
        false,
    )
    .with_media_repository(Arc::new(StubMedia::default()));
    assert!(
        !NativeProvider::declared_capabilities(&host_media)
            .content
            .image
            .inbound
    );
    let host_media_and_events = host_media.with_events(Arc::new(RecordingEvents::delivering()));
    assert!(
        NativeProvider::declared_capabilities(&host_media_and_events)
            .content
            .image
            .inbound
    );
}

#[tokio::test]
async fn inbound_file_is_downloaded_bounded_and_published_as_scoped_typed_media() {
    use tinychannels_bus::{ChannelIngress, ContentPart};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/files/file-1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"incoming-image-bytes"))
        .expect(1)
        .mount(&server)
        .await;
    let media = Arc::new(StubMedia::default());
    let events = Arc::new(RecordingEvents::delivering());
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_media_repository(media.clone())
    .with_events(events.clone());
    let post = json!({
        "id": "post-7",
        "user_id": "user-2",
        "message": "look",
        "create_at": 1_700_000_000_123_i64,
        "root_id": "root-3",
        "file_ids": ["file-1"],
        "metadata": {"files": [{
            "id": "file-1", "mime_type": "image/png", "name": "chart.png", "size": 20
        }]}
    });
    let message = channel
        .parse_mattermost_post(&post, "bot-1", "bot", 1_600_000_000_000_i64, "channel-9")
        .unwrap();
    let envelope = channel
        .inbound_envelope(&post, "channel-9", &message)
        .await
        .unwrap();
    assert!(crate::providers::native::inbound::publish(
        "mattermost",
        channel.events.as_ref(),
        envelope,
    )
    .await);

    let inbound = events.inbound();
    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].message_id, "post-7");
    assert_eq!(inbound[0].timestamp_unix_ms, Some(1_700_000_000_123));
    assert_eq!(inbound[0].conversation.id, "channel-9");
    assert_eq!(inbound[0].conversation.thread_id.as_deref(), Some("root-3"));
    assert_eq!(inbound[0].reply_to_id.as_deref(), Some("root-3"));
    assert!(matches!(inbound[0].parts[1], ContentPart::Image { .. }));
    assert_eq!(inbound[0].media[0].opaque_id.as_deref(), Some("media-1"));
    let stored = media.stored.lock().unwrap();
    assert_eq!(stored[0].bytes, b"incoming-image-bytes");
    assert_eq!(stored[0].metadata.scope.turn_id, "post-7");
    assert_eq!(
        stored[0].metadata.dedupe_key.as_deref(),
        Some("mattermost:post-7:file-1")
    );
    assert!(matches!(
        events.ingress.lock().unwrap().first(),
        Some(ChannelIngress::Inbound(_))
    ));
}

#[tokio::test]
async fn inbound_download_rejects_untrusted_file_ids_before_network_io() {
    let server = MockServer::start().await;
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );
    assert!(
        channel
            .download_file("../private", MAX_FILE_BYTES)
            .await
            .is_none()
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn generic_audio_attachment_is_typed_audio_not_a_voice_note() {
    use tinychannels_bus::ContentPart;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/files/audio-1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"audio-bytes"))
        .expect(1)
        .mount(&server)
        .await;
    let media = Arc::new(StubMedia::default());
    let voice = Arc::new(StubVoice::default());
    let events = Arc::new(RecordingEvents::delivering());
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_media_repository(media)
    .with_voice_media_store(voice.clone())
    .with_events(events.clone());
    let post = json!({
        "id": "audio-post",
        "user_id": "user-2",
        "message": "",
        "create_at": 1_700_000_000_123_i64,
        "root_id": "",
        "file_ids": ["audio-1"],
        "metadata": {"files": [{
            "id": "audio-1", "mime_type": "audio/mp4", "name": "clip.m4a", "size": 11
        }]}
    });
    let message = channel
        .parse_mattermost_post(&post, "bot-1", "bot", 1_600_000_000_000_i64, "channel-9")
        .unwrap();
    let envelope = channel
        .inbound_envelope(&post, "channel-9", &message)
        .await
        .unwrap();
    assert!(crate::providers::native::inbound::publish(
        "mattermost",
        channel.events.as_ref(),
        envelope,
    )
    .await);
    let inbound = events.inbound();
    assert!(matches!(inbound[0].parts[0], ContentPart::Audio { .. }));
    assert!(voice.stored.lock().unwrap().is_empty());
}

#[tokio::test]
async fn inbound_file_download_enforces_the_limit_before_returning_bytes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/files/file-1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"12345678901"))
        .expect(1)
        .mount(&server)
        .await;
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );
    assert!(channel.download_file("file-1", 10).await.is_none());
}

#[tokio::test]
async fn empty_file_only_posts_are_not_published_when_ingress_cannot_store_media() {
    let server = MockServer::start().await;
    let events = Arc::new(RecordingEvents::delivering());
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_events(events.clone());
    let post = json!({
        "id": "post-no-store",
        "user_id": "user-2",
        "message": "",
        "create_at": 1_700_000_000_123_i64,
        "root_id": "",
        "file_ids": ["file-1"],
        "metadata": {"files": [{
            "id": "file-1", "mime_type": "image/png", "name": "chart.png", "size": 20
        }]}
    });
    let message = channel
        .parse_mattermost_post(&post, "bot-1", "bot", 1_600_000_000_000_i64, "channel-9")
        .unwrap();
    let envelope = channel.inbound_envelope(&post, "channel-9", &message).await;
    if let Some(envelope) = envelope {
        crate::providers::native::inbound::publish("mattermost", channel.events.as_ref(), envelope)
            .await;
    }
    assert!(events.inbound().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn file_only_posts_with_unusable_ids_are_not_published() {
    let server = MockServer::start().await;
    let media = Arc::new(StubMedia::default());
    let events = Arc::new(RecordingEvents::delivering());
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_media_repository(media)
    .with_events(events.clone());
    let post = json!({
        "id": "post-bad-id",
        "user_id": "user-2",
        "message": "",
        "create_at": 1_700_000_000_123_i64,
        "root_id": "",
        "file_ids": ["../private"],
        "metadata": {"files": [{
            "id": "../private", "mime_type": "image/png", "name": "chart.png", "size": 20
        }]}
    });
    let message = channel
        .parse_mattermost_post(&post, "bot-1", "bot", 1_600_000_000_000_i64, "channel-9")
        .unwrap();
    let envelope = channel.inbound_envelope(&post, "channel-9", &message).await;
    if let Some(envelope) = envelope {
        crate::providers::native::inbound::publish("mattermost", channel.events.as_ref(), envelope)
            .await;
    }
    assert!(events.inbound().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn file_only_posts_with_oversized_metadata_are_not_downloaded_or_published() {
    let server = MockServer::start().await;
    let media = Arc::new(StubMedia::default());
    let events = Arc::new(RecordingEvents::delivering());
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_media_repository(media)
    .with_events(events.clone());
    let post = json!({
        "id": "post-oversized",
        "user_id": "user-2",
        "message": "",
        "create_at": 1_700_000_000_123_i64,
        "root_id": "",
        "file_ids": ["file-1"],
        "metadata": {"files": [{
            "id": "file-1", "mime_type": "image/png", "name": "chart.png",
            "size": MAX_FILE_BYTES + 1
        }]}
    });
    let message = channel
        .parse_mattermost_post(&post, "bot-1", "bot", 1_600_000_000_000_i64, "channel-9")
        .unwrap();
    let envelope = channel.inbound_envelope(&post, "channel-9", &message).await;
    if let Some(envelope) = envelope {
        crate::providers::native::inbound::publish("mattermost", channel.events.as_ref(), envelope)
            .await;
    }
    assert!(events.inbound().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn file_only_posts_are_not_published_when_media_storage_fails() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/files/file-1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"image-bytes"))
        .expect(1)
        .mount(&server)
        .await;
    let media = Arc::new(StubMedia {
        fail_store: true,
        ..Default::default()
    });
    let events = Arc::new(RecordingEvents::delivering());
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_media_repository(media)
    .with_events(events.clone());
    let post = json!({
        "id": "post-store-error",
        "user_id": "user-2",
        "message": "",
        "create_at": 1_700_000_000_123_i64,
        "root_id": "",
        "file_ids": ["file-1"],
        "metadata": {"files": [{
            "id": "file-1", "mime_type": "image/png", "name": "chart.png", "size": 11
        }]}
    });
    let message = channel
        .parse_mattermost_post(&post, "bot-1", "bot", 1_600_000_000_000_i64, "channel-9")
        .unwrap();
    let envelope = channel.inbound_envelope(&post, "channel-9", &message).await;
    if let Some(envelope) = envelope {
        crate::providers::native::inbound::publish("mattermost", channel.events.as_ref(), envelope)
            .await;
    }
    assert!(events.inbound().is_empty());
}

#[tokio::test]
async fn text_post_survives_when_its_attachment_cannot_be_ingested() {
    use tinychannels_bus::ContentPart;

    let server = MockServer::start().await;
    let media = Arc::new(StubMedia {
        fail_store: true,
        ..Default::default()
    });
    let events = Arc::new(RecordingEvents::delivering());
    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_media_repository(media)
    .with_events(events.clone());
    let post = json!({
        "id": "post-text-media",
        "user_id": "user-2",
        "message": "caption survives",
        "create_at": 1_700_000_000_123_i64,
        "root_id": "",
        "file_ids": ["../bad-id"],
        "metadata": {"files": [{
            "id": "../bad-id", "mime_type": "image/png", "name": "chart.png", "size": 20
        }]}
    });
    let message = channel
        .parse_mattermost_post(&post, "bot-1", "bot", 1_600_000_000_000_i64, "channel-9")
        .unwrap();
    let envelope = channel
        .inbound_envelope(&post, "channel-9", &message)
        .await
        .expect("text should remain deliverable");
    assert_eq!(envelope.text, "caption survives");
    assert!(matches!(
        envelope.parts.first(),
        Some(ContentPart::Text { .. })
    ));
}
