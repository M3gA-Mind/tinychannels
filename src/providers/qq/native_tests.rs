use super::super::http_test_support::HttpsServer;
use super::native::timestamp_unix_ms;
use crate::channel::{ChannelAdapter, ChannelInboundSink};
use crate::factory::{HttpClientFactory, build_long_tail_channel_with_host};
use crate::host::{ChannelHostBuilder, VoiceMediaRequest, VoiceMediaStore};
use crate::providers::native::fixtures::{StubMedia, stored_media};
use crate::providers::native::{FileStep, NativeProvider, Step, Target};
use base64::Engine as _;
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::{
    ChannelIngress, ChannelOutboundMessage, ChannelRef, ContentPart, ConversationKind,
    ConversationRef, HostReadVoiceMediaResult, HostStoreVoiceMediaResult, MediaKind,
    MediaReference, OutboundContentPart, OutboundVoice,
};

#[derive(Clone)]
struct RecordingInboundSink(tokio::sync::mpsc::UnboundedSender<ChannelIngress>);

#[async_trait::async_trait]
impl ChannelInboundSink for RecordingInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.0
            .send(ingress)
            .map_err(|_| tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "test sink closed".into(),
                retryable: true,
                ..Default::default()
            })
    }
}

fn typed_inbound(ingress: ChannelIngress) -> tinychannels_bus::ChannelInboundEnvelope {
    match ingress {
        ChannelIngress::Inbound(envelope) => envelope,
        other => panic!("expected inbound message, received {other:?}"),
    }
}

#[tokio::test]
async fn qq_inbound_image_downloads_tencent_attachment_and_publishes_typed_ingress() {
    use crate::providers::native::fixtures::RecordingEvents;

    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/attachments/image-1" => (200, "exact-qq-image-bytes".into()),
        other => panic!("unexpected QQ attachment request: {other}"),
    })
    .await;
    let media = Arc::new(StubMedia::default());
    let events = Arc::new(RecordingEvents::delivering());
    let channel = super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_media_repository(media.clone())
    .with_events(events.clone())
    .with_attachment_http_client(server.client.clone());

    let envelope = channel
        .inbound_envelope(
            "C2C_MESSAGE_CREATE",
            &json!({
                "id": "qq-inbound-1",
                "timestamp": "2024-07-03T09:46:40+00:00",
                "content": "look",
                "author": {"user_openid": "alice"},
                "attachments": [{
                    "content_type": "image/png",
                    "filename": "diagram.png",
                    "url": "https://api.sgroup.qq.com/attachments/image-1"
                }]
            }),
        )
        .await
        .expect("allowed QQ attachment becomes typed ingress");

    assert_eq!(envelope.message_id, "qq-inbound-1");
    assert_eq!(envelope.timestamp_unix_ms, Some(1_720_000_000_000));
    assert_eq!(envelope.conversation.id, "user:alice");
    assert_eq!(envelope.sender.id, "alice");
    assert_eq!(envelope.text, "look");
    assert_eq!(envelope.parts.len(), 2);
    assert!(matches!(
        &envelope.parts[1],
        ContentPart::Image { media, .. }
            if media.opaque_id.as_deref() == Some("media-1")
                && media.file_name.as_deref() == Some("diagram.png")
    ));
    assert!(
        crate::providers::native::inbound::publish("qq", channel.native.events.as_ref(), envelope,)
            .await
    );
    let stored = media.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].bytes, b"exact-qq-image-bytes");
    assert_eq!(stored[0].metadata.content_type, "image/png");
    assert_eq!(stored[0].metadata.scope.conversation.id, "user:alice");
    assert_eq!(stored[0].metadata.scope.turn_id, "qq-inbound-1");
    assert_eq!(events.inbound().len(), 1);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[test]
fn qq_declares_inbound_file_capabilities_when_media_storage_is_installed() {
    let with_ingress = super::super::QQChannel::new("app-id".into(), "app-secret".into(), vec![])
        .with_media_repository(Arc::new(StubMedia::default()))
        .with_events(Arc::new(
            crate::providers::native::fixtures::RecordingEvents::delivering(),
        ));
    let capabilities =
        crate::providers::native::NativeProvider::declared_capabilities(&with_ingress);
    assert!(capabilities.content.image.inbound);
    assert!(capabilities.content.file.inbound);

    let outbound_only = super::super::QQChannel::new("app-id".into(), "app-secret".into(), vec![])
        .with_media_repository(Arc::new(StubMedia::default()));
    let capabilities =
        crate::providers::native::NativeProvider::declared_capabilities(&outbound_only);
    assert!(capabilities.content.image.inbound);
    assert!(capabilities.content.image.outbound);
    assert!(!crate::providers::native::NativeProvider::voice_synthesis(
        &outbound_only
    ));

    let no_media = super::super::QQChannel::new("app-id".into(), "app-secret".into(), vec![]);
    let capabilities = crate::providers::native::NativeProvider::declared_capabilities(&no_media);
    assert!(!capabilities.content.image.inbound);
    assert!(!capabilities.content.image.outbound);
}

#[test]
fn qq_timestamps_accept_numeric_values_and_numeric_strings() {
    assert_eq!(
        timestamp_unix_ms(&json!(1_720_000_000_000_u64)),
        Some(1_720_000_000_000)
    );
    assert_eq!(
        timestamp_unix_ms(&json!("1700000000000")),
        Some(1_700_000_000_000)
    );
    assert_eq!(timestamp_unix_ms(&json!("not-a-timestamp")), None);
    assert_eq!(timestamp_unix_ms(&json!(-1)), None);
}

#[test]
fn qq_target_validation_rejects_threads_invalid_ids_and_non_chat_targets() {
    let channel = super::super::QQChannel::new("app-id".into(), "app-secret".into(), vec![]);
    let target = |id: &str, kind: ConversationKind| Target {
        channel: ChannelRef {
            id: "qq".into(),
            account_id: None,
        },
        conversation: ConversationRef {
            kind,
            id: id.into(),
            ..Default::default()
        },
        reply_to: None,
        thread: None,
    };

    let mut threaded = target("user-1", ConversationKind::Dm);
    threaded.thread = Some("thread-1".into());
    assert!(
        NativeProvider::validate_target(&channel, &threaded)
            .unwrap_err()
            .unsupported
    );

    let invalid_id = target("user/1", ConversationKind::Dm);
    assert_eq!(
        NativeProvider::validate_target(&channel, &invalid_id)
            .unwrap_err()
            .kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );

    let empty_id = target("", ConversationKind::Dm);
    assert_eq!(
        NativeProvider::validate_target(&channel, &empty_id)
            .unwrap_err()
            .kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );

    let non_chat = target("channel-1", ConversationKind::Channel);
    assert_eq!(
        NativeProvider::validate_target(&channel, &non_chat)
            .unwrap_err()
            .kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );
}

#[tokio::test]
async fn qq_preflights_unsupported_voice_mime_before_delivery() {
    let channel = super::super::QQChannel::new("app-id".into(), "app-secret".into(), vec![]);
    let target = Target {
        channel: ChannelRef {
            id: "qq".into(),
            account_id: None,
        },
        conversation: ConversationRef {
            kind: ConversationKind::Dm,
            id: "user-1".into(),
            ..Default::default()
        },
        reply_to: None,
        thread: None,
    };
    let mut steps = [Step::Voice(FileStep {
        #[cfg(feature = "lark")]
        kind: MediaKind::Audio,
        bytes: b"not a supported voice codec".to_vec(),
        mime: "audio/flac".into(),
        name: "voice.flac".into(),
        caption: None,
        duration_ms: None,
    })];

    let error = NativeProvider::prepare_steps(&channel, &target, &mut steps)
        .await
        .unwrap_err();

    assert!(error.unsupported);
    assert!(!error.sent_before_error);
}

#[tokio::test]
async fn qq_inbound_attachment_rejects_non_tencent_urls_before_network_io() {
    let server = HttpsServer::start(|request| {
        panic!("unexpected QQ attachment request: {}", request.target)
    })
    .await;
    let channel = super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_media_repository(Arc::new(StubMedia::default()))
    .with_events(Arc::new(
        crate::providers::native::fixtures::RecordingEvents::delivering(),
    ))
    .with_attachment_http_client(server.client.clone());
    let envelope = channel
        .inbound_envelope(
            "C2C_MESSAGE_CREATE",
            &json!({
                "id": "qq-inbound-untrusted-url",
                "author": {"user_openid": "alice"},
                "attachments": [{
                    "content_type": "image/png",
                    "url": "https://169.254.169.254/latest/meta-data"
                }]
            }),
        )
        .await;
    assert!(envelope.is_none());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn qq_inbound_attachment_ignores_non_success_download_responses() {
    let server = HttpsServer::start(|request| {
        assert_eq!(request.target, "/attachments/not-found");
        (404, "{}".into())
    })
    .await;
    let channel = super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_attachment_http_client(server.client.clone());

    let downloaded = channel
        .download_attachment("https://api.sgroup.qq.com/attachments/not-found", 1024)
        .await;

    assert!(downloaded.is_none());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn qq_inbound_attachment_checks_sender_allowlist_before_network_io() {
    let server = HttpsServer::start(|request| {
        panic!("unexpected QQ attachment request: {}", request.target)
    })
    .await;
    let channel = super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        server.client.clone(),
    )
    .with_media_repository(Arc::new(StubMedia::default()))
    .with_events(Arc::new(
        crate::providers::native::fixtures::RecordingEvents::delivering(),
    ))
    .with_attachment_http_client(server.client.clone());
    let envelope = channel
        .inbound_envelope(
            "C2C_MESSAGE_CREATE",
            &json!({
                "id": "qq-inbound-forbidden-sender",
                "author": {"user_openid": "mallory"},
                "attachments": [{
                    "content_type": "image/png",
                    "url": "https://api.sgroup.qq.com/attachments/image-1"
                }]
            }),
        )
        .await;
    assert!(envelope.is_none());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn qq_gateway_listener_publishes_attachment_only_message_without_legacy_fallback() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gateway = HttpsServer::start(move |request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/gateway" => (200, json!({"url": format!("ws://{address}/")}).to_string()),
        "/attachments/image-2" => (200, "listener-image-bytes".into()),
        "/attachments/group-image" => (200, "group-image-bytes".into()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let websocket = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.send(Message::Text(
            json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let identify: serde_json::Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
        assert_eq!(identify["op"], 2);
        // The gateway's heartbeat interval fires immediately.
        let heartbeat: serde_json::Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
        assert_eq!(heartbeat["op"], 1);
        ws.send(Message::Text(
            json!({
                "op": 0,
                "t": "C2C_MESSAGE_CREATE",
                "d": {
                    "id": "qq-listener-image",
                    "author": {"user_openid": "alice"},
                    "attachments": [{
                        "content_type": "image/png",
                        "filename": "listener.png",
                        "url": "https://api.sgroup.qq.com/attachments/image-2"
                    }]
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({
                "op": 0,
                "t": "GROUP_AT_MESSAGE_CREATE",
                "d": {
                    "id": "qq-listener-group-image",
                    "author": {"member_openid": "alice"},
                    "group_openid": "group-1",
                    "attachments": [{
                        "content_type": "image/png",
                        "filename": "group-listener.png",
                        "url": "https://api.sgroup.qq.com/attachments/group-image"
                    }]
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        release_rx.await.unwrap();
        ws.close(None).await.unwrap();
    });

    let media = Arc::new(StubMedia::default());
    let channel = super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    )
    .with_media_repository(media.clone())
    .with_attachment_http_client(gateway.client.clone());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(RecordingInboundSink(tx));
    let listener_sink = sink.clone();
    let listening = tokio::spawn(async move { channel.start(listener_sink.as_ref()).await });
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        (
            typed_inbound(rx.recv().await.unwrap()),
            typed_inbound(rx.recv().await.unwrap()),
        )
    })
    .await
    .expect("QQ listener should publish typed attachment ingress");
    assert!(matches!(
        &first.parts[0],
        ContentPart::Image { media, .. }
            if media.file_name.as_deref() == Some("listener.png")
    ));
    assert_eq!(first.message_id, "qq-listener-image");
    assert_eq!(first.conversation.id, "user:alice");
    assert_eq!(second.conversation.id, "group:group-1");
    assert!(matches!(
        &second.parts[0],
        ContentPart::Image { media, .. }
            if media.file_name.as_deref() == Some("group-listener.png")
    ));
    assert!(
        rx.try_recv().is_err(),
        "one provider event emits one ingress"
    );
    assert_eq!(
        media
            .stored
            .lock()
            .unwrap()
            .iter()
            .map(|stored| stored.bytes.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"listener-image-bytes".as_slice(),
            b"group-image-bytes".as_slice()
        ]
    );
    release_tx.send(()).unwrap();
    drop(rx);
    assert!(listening.await.unwrap().is_err());
    websocket.await.unwrap();
}

#[tokio::test]
async fn qq_gateway_listener_publishes_group_attachment_only_message_without_legacy_fallback() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gateway = HttpsServer::start(move |request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/gateway" => (200, json!({"url": format!("ws://{address}/")}).to_string()),
        "/attachments/group-image" => (200, "group-image-bytes".into()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let websocket = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.send(Message::Text(
            json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let _: serde_json::Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
        let _: serde_json::Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().to_string()).unwrap();
        ws.send(Message::Text(
            json!({
                "op": 0,
                "t": "GROUP_AT_MESSAGE_CREATE",
                "d": {
                    "id": "qq-listener-group-only-image",
                    "author": {"member_openid": "alice"},
                    "group_openid": "group-1",
                    "attachments": [{
                        "content_type": "image/png",
                        "filename": "group-listener.png",
                        "url": "https://api.sgroup.qq.com/attachments/group-image"
                    }]
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        release_rx.await.unwrap();
        ws.close(None).await.unwrap();
    });

    let media = Arc::new(StubMedia::default());
    let channel = super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    )
    .with_media_repository(media.clone())
    .with_attachment_http_client(gateway.client.clone());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(RecordingInboundSink(tx));
    let listener_sink = sink.clone();
    let listening = tokio::spawn(async move { channel.start(listener_sink.as_ref()).await });
    let inbound = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        typed_inbound(rx.recv().await.unwrap())
    })
    .await
    .expect("QQ group listener should publish attachment-only typed ingress");
    assert_eq!(inbound.conversation.id, "group:group-1");
    assert!(matches!(
        &inbound.parts[0],
        ContentPart::Image { media, .. }
            if media.file_name.as_deref() == Some("group-listener.png")
    ));
    assert!(
        rx.try_recv().is_err(),
        "one provider event emits one ingress"
    );
    assert_eq!(media.stored.lock().unwrap()[0].bytes, b"group-image-bytes");
    release_tx.send(()).unwrap();
    drop(rx);
    assert!(listening.await.unwrap().is_err());
    websocket.await.unwrap();
}

struct QQVoiceStore;

#[async_trait::async_trait]
impl VoiceMediaStore for QQVoiceStore {
    async fn store_voice(
        &self,
        _request: VoiceMediaRequest,
    ) -> anyhow::Result<HostStoreVoiceMediaResult> {
        anyhow::bail!("inbound storage is not used by this test")
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<HostReadVoiceMediaResult> {
        anyhow::ensure!(opaque_id == "opaque-voice-1", "unexpected voice reference");
        Ok(HostReadVoiceMediaResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(b"ogg-voice-bytes"),
            mime_type: "audio/ogg".into(),
            file_name: Some("reply.ogg".into()),
        })
    }
}

struct FixtureHttpClients(reqwest::Client);

impl HttpClientFactory for FixtureHttpClients {
    fn client_for(&self, _channel: &str) -> reqwest::Client {
        self.0.clone()
    }
}

#[tokio::test]
async fn host_scoped_image_media_uploads_then_sends_the_server_file_info() {
    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/v2/users/user-1/files" => (
            200,
            json!({"file_info": "opaque-qq-file-info", "ttl": 300}).to_string(),
        ),
        "/v2/users/user-1/messages" => (200, json!({"id": "qq-message-1"}).to_string()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    assert!(server.address.ip().is_loopback());
    assert_ne!(server.address.port(), 0);
    let media = Arc::new(StubMedia::default());
    let mut stored = stored_media(MediaKind::Image, "image/png", b"exact-scoped-png-bytes");
    stored.file_name = Some("diagram.png".into());
    *media.read.lock().unwrap() = Some(stored);
    let host = ChannelHostBuilder::new()
        .media_repository(media.clone())
        .build();
    let config = tinychannels_bus::ChannelsConfig {
        qq: Some(tinychannels_bus::config::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let channel = build_long_tail_channel_with_host(
        &config,
        &host,
        &FixtureHttpClients(server.client.clone()),
        "qq",
    )
    .expect("configured QQ adapter");
    assert!(
        ChannelAdapter::declared_capabilities(channel.as_ref())
            .content
            .image
            .outbound
    );

    let message = ChannelOutboundMessage {
        channel_id: "qq".into(),
        conversation_id: "user-1".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "qq".into(),
            account_id: Some("qq-account".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "user-1".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "image-1".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("opaque-image-1".into()),
                    kind: MediaKind::Image,
                    content_type: Some("image/png".into()),
                    file_name: Some("diagram.png".into()),
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        }],
    };
    let result =
        tinychannels_runtime::channel::ChannelTypedSend::send_typed(channel.as_ref(), &message)
            .await
            .expect("typed QQ image send");

    assert!(result.error.is_none());
    assert_eq!(
        result.receipt.primary_platform_message_id.as_deref(),
        Some("qq-message-1")
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3, "token, upload, then message");
    assert_eq!(requests[1].target, "/v2/users/user-1/files");
    assert_eq!(
        requests[1].body,
        json!({
            "file_type": 1,
            "file_name": "diagram.png",
            "file_data": base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b"exact-scoped-png-bytes"
            ),
            "srv_send_msg": false
        })
    );
    assert_eq!(requests[2].target, "/v2/users/user-1/messages");
    assert_eq!(requests[2].body["msg_type"], 7);
    assert_eq!(
        requests[2].body["media"]["file_info"],
        "opaque-qq-file-info"
    );
    assert_eq!(media.scopes.lock().unwrap()[0].conversation.id, "user-1");
    assert_eq!(media.scopes.lock().unwrap()[0].turn_id, "turn-1");
}

#[tokio::test]
async fn qq_rejects_text_with_captioned_media_before_sending_any_step() {
    use crate::providers::native::fixtures::stored_media;

    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/v2/users/user-1/messages" => (200, json!({"id": "qq-text-1"}).to_string()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Image,
        "image/png",
        b"exact-image-bytes",
    ));
    let host = ChannelHostBuilder::new().media_repository(media).build();
    let config = tinychannels_bus::ChannelsConfig {
        qq: Some(tinychannels_bus::config::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let channel = build_long_tail_channel_with_host(
        &config,
        &host,
        &FixtureHttpClients(server.client.clone()),
        "qq",
    )
    .expect("configured QQ adapter");
    let message = ChannelOutboundMessage {
        channel_id: "qq".into(),
        conversation_id: "user:user-1".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-captioned-image".into()),
        idempotency_key: "captioned-image".into(),
        parts: vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "must not be sent before validation".into(),
                    markdown_dialect: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::Image {
                    media: MediaReference {
                        opaque_id: Some("opaque-image-1".into()),
                        kind: MediaKind::Image,
                        content_type: Some("image/png".into()),
                        ..Default::default()
                    },
                    caption: Some("QQ cannot preserve this caption".into()),
                    alt_text: None,
                },
            },
        ],
    };

    let result =
        tinychannels_runtime::channel::ChannelTypedSend::send_typed(channel.as_ref(), &message)
            .await;

    assert!(
        result.is_err() || result.as_ref().is_ok_and(|outcome| outcome.error.is_some()),
        "QQ must reject an unrepresentable caption"
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn host_voice_store_uploads_native_voice_and_sends_its_opaque_file_info() {
    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/v2/groups/group-1/files" => (
            200,
            json!({"file_info": "opaque-qq-voice-info", "ttl": 300}).to_string(),
        ),
        "/v2/groups/group-1/messages" => (200, json!({"id": "qq-voice-message-1"}).to_string()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    let host = ChannelHostBuilder::new()
        .voice_media_store(Arc::new(QQVoiceStore))
        .build();
    let config = tinychannels_bus::ChannelsConfig {
        qq: Some(tinychannels_bus::config::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let channel = build_long_tail_channel_with_host(
        &config,
        &host,
        &FixtureHttpClients(server.client.clone()),
        "qq",
    )
    .expect("configured QQ adapter");
    assert!(
        ChannelAdapter::declared_capabilities(channel.as_ref())
            .voice_out
            .supported
    );

    let message = ChannelOutboundMessage {
        channel_id: "qq".into(),
        conversation_id: "group:group-1".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "qq".into(),
            account_id: Some("qq-account".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "group:group-1".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-voice".into()),
        idempotency_key: "voice-1".into(),
        parts: vec![OutboundContentPart::Voice {
            voice: OutboundVoice::Media {
                media: MediaReference {
                    opaque_id: Some("opaque-voice-1".into()),
                    kind: MediaKind::Audio,
                    content_type: Some("audio/ogg".into()),
                    ..Default::default()
                },
                codec: None,
                duration_ms: None,
            },
        }],
    };
    let result =
        tinychannels_runtime::channel::ChannelTypedSend::send_typed(channel.as_ref(), &message)
            .await
            .expect("typed QQ voice send");

    assert!(result.error.is_none());
    assert_eq!(
        result.receipt.primary_platform_message_id.as_deref(),
        Some("qq-voice-message-1")
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].target, "/v2/groups/group-1/files");
    assert_eq!(requests[1].body["file_type"], 3);
    assert_eq!(requests[1].body["file_name"], "reply.ogg");
    assert_eq!(
        requests[1].body["file_data"],
        base64::engine::general_purpose::STANDARD.encode(b"ogg-voice-bytes")
    );
    assert_eq!(requests[2].target, "/v2/groups/group-1/messages");
    assert_eq!(requests[2].body["msg_type"], 7);
    assert_eq!(
        requests[2].body["media"]["file_info"],
        "opaque-qq-voice-info"
    );
}

#[tokio::test]
async fn oversized_host_media_is_rejected_before_any_qq_request() {
    let server =
        HttpsServer::start(|request| panic!("unexpected QQ request: {}", request.target)).await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Image,
        "image/png",
        &vec![0; 5 * 1024 * 1024 + 1],
    ));
    let host = ChannelHostBuilder::new()
        .media_repository(media.clone())
        .build();
    let config = tinychannels_bus::ChannelsConfig {
        qq: Some(tinychannels_bus::config::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let channel = build_long_tail_channel_with_host(
        &config,
        &host,
        &FixtureHttpClients(server.client.clone()),
        "qq",
    )
    .expect("configured QQ adapter");
    let message = image_message("user-1", "too-large");

    let error =
        tinychannels_runtime::channel::ChannelTypedSend::send_typed(channel.as_ref(), &message)
            .await
            .expect_err("oversized media must be rejected during planning");
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );
    assert!(!error.sent_before_error);
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn rejected_qq_upload_does_not_send_a_message_or_claim_delivery() {
    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/v2/users/user-1/files" => (413, json!({"message": "too large"}).to_string()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Image,
        "image/png",
        b"exact-scoped-png-bytes",
    ));
    let host = ChannelHostBuilder::new().media_repository(media).build();
    let config = tinychannels_bus::ChannelsConfig {
        qq: Some(tinychannels_bus::config::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let channel = build_long_tail_channel_with_host(
        &config,
        &host,
        &FixtureHttpClients(server.client.clone()),
        "qq",
    )
    .expect("configured QQ adapter");

    let error = tinychannels_runtime::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &image_message("user-1", "upload-rejected"),
    )
    .await
    .expect_err("rejected upload must stop before final send");
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::BadFormat
    );
    assert!(!error.sent_before_error);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2, "token and upload only");
    assert_eq!(requests[1].target, "/v2/users/user-1/files");
}

#[tokio::test]
async fn qq_accepted_send_without_message_id_is_reported_as_unknown_delivery() {
    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/v2/users/user-1/messages" => (200, "{}".into()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    let config = tinychannels_bus::ChannelsConfig {
        qq: Some(tinychannels_bus::config::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let channel = build_long_tail_channel_with_host(
        &config,
        &ChannelHostBuilder::new().build(),
        &FixtureHttpClients(server.client.clone()),
        "qq",
    )
    .expect("configured QQ adapter");

    let error = tinychannels_runtime::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &content_message(
            "user-1",
            "missing-message-id",
            ContentPart::Text {
                text: "hello".into(),
                markdown_dialect: None,
            },
        ),
    )
    .await
    .expect_err("accepted response without an id has uncertain delivery");

    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(error.sent_before_error);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn qq_final_server_error_preserves_uncertain_delivery_after_upload() {
    let server = HttpsServer::start(|request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
        ),
        "/v2/users/user-1/files" => (
            200,
            json!({"file_info": "opaque-qq-file-info", "ttl": 300}).to_string(),
        ),
        "/v2/users/user-1/messages" => (500, json!({"message": "gateway error"}).to_string()),
        other => panic!("unexpected QQ request: {other}"),
    })
    .await;
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Image,
        "image/png",
        b"exact-scoped-png-bytes",
    ));
    let host = ChannelHostBuilder::new().media_repository(media).build();
    let config = tinychannels_bus::ChannelsConfig {
        qq: Some(tinychannels_bus::config::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let channel = build_long_tail_channel_with_host(
        &config,
        &host,
        &FixtureHttpClients(server.client.clone()),
        "qq",
    )
    .expect("configured QQ adapter");

    let error = tinychannels_runtime::channel::ChannelTypedSend::send_typed(
        channel.as_ref(),
        &image_message("user-1", "final-send-error"),
    )
    .await
    .expect_err("a server error after upload leaves message delivery uncertain");

    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].target, "/v2/users/user-1/files");
    assert_eq!(requests[2].target, "/v2/users/user-1/messages");
}

#[tokio::test]
async fn qq_uses_video_and_generic_file_upload_types() {
    for (kind, mime, name, expected_file_type, content_part) in [
        (
            MediaKind::Video,
            "video/mp4",
            "clip.mp4",
            2,
            ContentPart::Video {
                media: media_reference("video-1", MediaKind::Video, "video/mp4", "clip.mp4"),
                caption: None,
                duration_ms: Some(1000),
                thumbnail: None,
            },
        ),
        (
            MediaKind::Document,
            "application/pdf",
            "report.pdf",
            4,
            ContentPart::File {
                media: media_reference(
                    "file-1",
                    MediaKind::Document,
                    "application/pdf",
                    "report.pdf",
                ),
                file_name: Some("report.pdf".into()),
                caption: None,
            },
        ),
    ] {
        let server = HttpsServer::start(|request| match request.target.as_str() {
            "/app/getAppAccessToken" => (
                200,
                json!({"access_token": "qq-token", "expires_in": "7200"}).to_string(),
            ),
            "/v2/users/user-1/files" => (
                200,
                json!({"file_info": "opaque-qq-file-info", "ttl": 300}).to_string(),
            ),
            "/v2/users/user-1/messages" => (200, json!({"id": "qq-media-message-1"}).to_string()),
            other => panic!("unexpected QQ request: {other}"),
        })
        .await;
        let media = Arc::new(StubMedia::default());
        let mut stored = stored_media(kind, mime, b"exact-scoped-media-bytes");
        stored.file_name = Some(name.into());
        *media.read.lock().unwrap() = Some(stored);
        let host = ChannelHostBuilder::new().media_repository(media).build();
        let config = tinychannels_bus::ChannelsConfig {
            qq: Some(tinychannels_bus::config::QQConfig {
                app_id: "app-id".into(),
                app_secret: "app-secret".into(),
                allowed_users: vec!["*".into()],
            }),
            ..Default::default()
        };
        let channel = build_long_tail_channel_with_host(
            &config,
            &host,
            &FixtureHttpClients(server.client.clone()),
            "qq",
        )
        .expect("configured QQ adapter");
        let result = tinychannels_runtime::channel::ChannelTypedSend::send_typed(
            channel.as_ref(),
            &content_message("user-1", "media-1", content_part),
        )
        .await
        .expect("native QQ media send");
        assert!(result.error.is_none());
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].body["file_type"], expected_file_type);
        assert_eq!(requests[1].body["file_name"], name);
        assert_eq!(
            requests[2].body["media"]["file_info"],
            "opaque-qq-file-info"
        );
    }
}

fn image_message(conversation_id: &str, idempotency_key: &str) -> ChannelOutboundMessage {
    content_message(
        conversation_id,
        idempotency_key,
        ContentPart::Image {
            media: media_reference(
                "opaque-image-1",
                MediaKind::Image,
                "image/png",
                "diagram.png",
            ),
            caption: None,
            alt_text: None,
        },
    )
}

fn content_message(
    conversation_id: &str,
    idempotency_key: &str,
    part: ContentPart,
) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "qq".into(),
        conversation_id: conversation_id.into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "qq".into(),
            account_id: Some("qq-account".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: conversation_id.into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: idempotency_key.into(),
        parts: vec![OutboundContentPart::Content { part }],
    }
}

fn media_reference(id: &str, kind: MediaKind, content_type: &str, name: &str) -> MediaReference {
    MediaReference {
        opaque_id: Some(id.into()),
        kind,
        content_type: Some(content_type.into()),
        file_name: Some(name.into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn public_adapter_start_owns_qq_gateway_until_stop_and_restart() {
    use crate::providers::http_test_support::HttpsServer;
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    const GUARD: Duration = Duration::from_secs(8);

    fn dispatch(id: &str, sender: &str, content: &str, sequence: i64) -> Message {
        Message::Text(
            serde_json::json!({
                "op": 0,
                "s": sequence,
                "t": "C2C_MESSAGE_CREATE",
                "d": {
                    "id": id,
                    "timestamp": "2024-07-03T09:46:40+00:00",
                    "content": content,
                    "author": {"user_openid": sender}
                }
            })
            .to_string()
            .into(),
        )
    }

    let gateway_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_address = gateway_listener.local_addr().unwrap();
    let gateway = HttpsServer::start(move |request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            serde_json::json!({"access_token": "qq-test-token", "expires_in": "7200"}).to_string(),
        ),
        "/gateway" => {
            assert!(
                request
                    .headers
                    .to_ascii_lowercase()
                    .contains("authorization: qqbot qq-test-token")
            );
            (
                200,
                serde_json::json!({"url": format!("ws://{gateway_address}/")}).to_string(),
            )
        }
        other => panic!("unexpected QQ API request: {other}"),
    })
    .await;

    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let mock_gateway = tokio::spawn(async move {
        for (session, accepted_id, accepted_text) in [
            (1_u8, "qq-public-accepted", "first connection"),
            (2_u8, "qq-public-restarted", "after restart"),
        ] {
            let (socket, _) = tokio::time::timeout(GUARD, gateway_listener.accept())
                .await
                .expect("QQ adapter should connect to the local gateway")
                .unwrap();
            let mut websocket = tokio::time::timeout(GUARD, accept_async(socket))
                .await
                .expect("QQ WebSocket handshake should complete")
                .unwrap();
            websocket
                .send(Message::Text(
                    serde_json::json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let identify: serde_json::Value = tokio::time::timeout(GUARD, websocket.next())
                .await
                .expect("QQ adapter should identify after HELLO")
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(identify["op"], 2);
            assert_eq!(identify["d"]["token"], "QQBot qq-test-token");
            assert_eq!(identify["d"]["intents"], (1_u64 << 25) | (1_u64 << 30));

            let heartbeat: serde_json::Value = tokio::time::timeout(GUARD, websocket.next())
                .await
                .expect("QQ adapter should send its initial heartbeat")
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(heartbeat, serde_json::json!({"op": 1, "d": null}));
            ready_tx.send(session).unwrap();

            if session == 1 {
                websocket
                    .send(dispatch("qq-denied", "mallory", "must be filtered", 1))
                    .await
                    .unwrap();
                websocket
                    .send(dispatch(accepted_id, "alice", accepted_text, 2))
                    .await
                    .unwrap();
                websocket
                    .send(dispatch(accepted_id, "alice", "duplicate delivery", 3))
                    .await
                    .unwrap();
                websocket
                    .send(dispatch("qq-public-barrier", "alice", "after duplicate", 4))
                    .await
                    .unwrap();
                websocket
                    .send(Message::Text(
                        serde_json::json!({
                            "op": 0,
                            "s": 5,
                            "t": "GROUP_AT_MESSAGE_CREATE",
                            "d": {
                                "id": "qq-public-group",
                                "timestamp": "2024-07-03T09:46:40+00:00",
                                "content": "group message",
                                "author": {"member_openid": "alice"},
                                "group_openid": "group-1"
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
            } else {
                websocket
                    .send(dispatch(accepted_id, "alice", accepted_text, 1))
                    .await
                    .unwrap();
            }

            let closed = tokio::time::timeout(GUARD, websocket.next())
                .await
                .expect("QQ adapter stop should close its gateway connection");
            assert!(matches!(closed, None | Some(Ok(Message::Close(_)))));
        }
    });

    let channel = Arc::new(super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);

    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let session = tokio::select! {
        result = &mut listener => panic!("QQ adapter start returned before gateway ownership: {result:?}"),
        session = tokio::time::timeout(GUARD, ready_rx.recv()) => session
            .expect("QQ gateway handshake should finish within the guard")
            .expect("mock gateway readiness sender remains connected"),
    };
    assert_eq!(session, 1);

    let concurrent = adapter
        .start(&sink)
        .await
        .expect_err("a second listener must not replace the active gateway owner");
    assert_eq!(
        concurrent.kind,
        tinychannels_bus::channel::SendErrorKind::Forbidden
    );

    let first = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("allowed QQ event should reach the supplied typed sink")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(first) = first else {
        panic!("QQ messages must arrive as typed inbound ingress");
    };
    assert_eq!(first.message_id, "qq-public-accepted");
    assert_eq!(first.sender.id, "alice");
    assert_eq!(first.conversation.id, "user:alice");
    assert_eq!(first.text, "first connection");

    let barrier = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("barrier event should follow the allowed QQ event")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(barrier) = barrier else {
        panic!("QQ messages must arrive as typed inbound ingress");
    };
    assert_eq!(barrier.message_id, "qq-public-barrier");
    assert_eq!(barrier.sender.id, "alice");
    assert_eq!(barrier.conversation.id, "user:alice");
    assert_eq!(barrier.text, "after duplicate");

    let group = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("allowed QQ group event should reach the supplied typed sink")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(group) = group else {
        panic!("QQ group messages must arrive as typed inbound ingress");
    };
    assert_eq!(group.message_id, "qq-public-group");
    assert_eq!(group.sender.id, "alice");
    assert_eq!(group.conversation.id, "group:group-1");
    assert_eq!(group.text, "group message");

    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("QQ stop must join the gateway listener")
        .expect("QQ listener must not panic")
        .expect("QQ listener must stop cleanly");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    let mut restarted = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let session = tokio::select! {
        result = &mut restarted => panic!("QQ restart returned before gateway ownership: {result:?}"),
        session = tokio::time::timeout(GUARD, ready_rx.recv()) => session
            .expect("restarted QQ gateway handshake should finish within the guard")
            .expect("mock gateway readiness sender remains connected"),
    };
    assert_eq!(session, 2);
    let restarted_ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("restarted QQ gateway should publish admitted events")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(restarted_ingress) = restarted_ingress else {
        panic!("QQ messages must arrive as typed inbound ingress");
    };
    assert_eq!(restarted_ingress.message_id, "qq-public-restarted");
    assert_eq!(restarted_ingress.sender.id, "alice");
    assert_eq!(restarted_ingress.conversation.id, "user:alice");
    assert_eq!(restarted_ingress.text, "after restart");

    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut restarted)
        .await
        .expect("QQ stop must join the restarted gateway listener")
        .expect("restarted QQ listener must not panic")
        .expect("restarted QQ listener must stop cleanly");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    tokio::time::timeout(GUARD, mock_gateway)
        .await
        .expect("mock QQ gateway should observe both adapter stops")
        .expect("mock QQ gateway task must not panic");
}

#[tokio::test]
async fn public_adapter_stop_cancels_a_blocked_typed_sink() {
    use crate::providers::http_test_support::HttpsServer;
    use futures_util::{SinkExt, StreamExt};
    use std::future::pending;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    const GUARD: Duration = Duration::from_secs(8);

    struct BlockingSink(Arc<tokio::sync::Notify>);

    #[async_trait::async_trait]
    impl ChannelInboundSink for BlockingSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _ingress: ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.0.notify_one();
            pending::<()>().await;
            Ok(())
        }
    }

    let gateway_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_address = gateway_listener.local_addr().unwrap();
    let gateway = HttpsServer::start(move |request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            serde_json::json!({"access_token": "qq-test-token", "expires_in": "7200"}).to_string(),
        ),
        "/gateway" => (
            200,
            serde_json::json!({"url": format!("ws://{gateway_address}/")}).to_string(),
        ),
        other => panic!("unexpected QQ API request: {other}"),
    })
    .await;
    let mock_gateway = tokio::spawn(async move {
        let (socket, _) = tokio::time::timeout(GUARD, gateway_listener.accept())
            .await
            .expect("QQ adapter should connect to its gateway")
            .unwrap();
        let mut websocket = tokio::time::timeout(GUARD, accept_async(socket))
            .await
            .expect("QQ WebSocket handshake should complete")
            .unwrap();
        websocket
            .send(Message::Text(
                serde_json::json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        for expected_op in [2, 1] {
            let frame = tokio::time::timeout(GUARD, websocket.next())
                .await
                .expect("QQ adapter should identify and heartbeat")
                .unwrap()
                .unwrap();
            let value: serde_json::Value = frame.to_text().unwrap().parse().unwrap();
            assert_eq!(value["op"], expected_op);
        }
        websocket
            .send(Message::Text(
                serde_json::json!({
                    "op": 0,
                    "s": 1,
                    "t": "C2C_MESSAGE_CREATE",
                    "d": {
                        "id": "qq-blocked-sink",
                        "timestamp": "2024-07-03T09:46:40+00:00",
                        "content": "block publication",
                        "author": {"user_openid": "alice"}
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let close = tokio::time::timeout(GUARD, websocket.next())
            .await
            .expect("stop should close the gateway despite a blocked sink")
            .expect("gateway connection should close")
            .expect("close frame should be valid");
        assert!(matches!(close, Message::Close(_)));
    });

    let channel = Arc::new(super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let blocked = Arc::new(tokio::sync::Notify::new());
    let sink = BlockingSink(blocked.clone());
    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        async move { adapter.start(&sink).await }
    });
    tokio::time::timeout(GUARD, blocked.notified())
        .await
        .expect("QQ event should enter the typed sink");
    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("stop must cancel blocked typed publication and join")
        .expect("listener task must not panic")
        .expect("explicit stop should be clean");
    tokio::time::timeout(GUARD, mock_gateway)
        .await
        .expect("mock gateway should observe graceful shutdown")
        .expect("mock gateway task must not panic");
}

#[tokio::test]
async fn public_adapter_retries_a_message_after_typed_sink_rejection() {
    use crate::providers::http_test_support::HttpsServer;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    const GUARD: Duration = Duration::from_secs(8);

    struct RejectOnceSink {
        attempts: AtomicUsize,
        accepted: tokio::sync::mpsc::UnboundedSender<ChannelIngress>,
    }

    #[async_trait::async_trait]
    impl ChannelInboundSink for RejectOnceSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            ingress: ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Transient,
                    message: "temporary typed sink failure".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            self.accepted
                .send(ingress)
                .map_err(|_| tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Transient,
                    message: "test receiver closed".into(),
                    retryable: true,
                    ..Default::default()
                })
        }
    }

    let gateway_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_address = gateway_listener.local_addr().unwrap();
    let gateway = HttpsServer::start(move |request| match request.target.as_str() {
        "/app/getAppAccessToken" => (
            200,
            serde_json::json!({"access_token": "qq-test-token", "expires_in": "7200"}).to_string(),
        ),
        "/gateway" => (
            200,
            serde_json::json!({"url": format!("ws://{gateway_address}/")}).to_string(),
        ),
        other => panic!("unexpected QQ API request: {other}"),
    })
    .await;

    let mock_gateway = tokio::spawn(async move {
        for session in 0..2 {
            let (socket, _) = tokio::time::timeout(GUARD, gateway_listener.accept())
                .await
                .expect("QQ adapter should reconnect after a rejected publication")
                .unwrap();
            let mut websocket = tokio::time::timeout(GUARD, accept_async(socket))
                .await
                .expect("QQ WebSocket handshake should complete")
                .unwrap();
            websocket
                .send(Message::Text(
                    serde_json::json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            for expected_op in [2, 1] {
                let frame = tokio::time::timeout(GUARD, websocket.next())
                    .await
                    .expect("QQ adapter should identify and heartbeat")
                    .unwrap()
                    .unwrap();
                let value: serde_json::Value = frame.to_text().unwrap().parse().unwrap();
                assert_eq!(value["op"], expected_op);
            }
            websocket
                .send(Message::Text(
                    serde_json::json!({
                        "op": 0,
                        "s": session + 1,
                        "t": "C2C_MESSAGE_CREATE",
                        "d": {
                            "id": "qq-retry-after-sink-error",
                            "timestamp": "2024-07-03T09:46:40+00:00",
                            "content": "retry this accepted message",
                            "author": {"user_openid": "alice"}
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            if session == 0 {
                let closed = tokio::time::timeout(GUARD, websocket.next())
                    .await
                    .expect("a rejected typed event should terminate the current listener");
                assert!(
                    closed.is_none()
                        || matches!(closed, Some(Ok(Message::Close(_))))
                        || matches!(closed, Some(Err(_))),
                    "rejected publication should release the gateway socket, got {closed:?}"
                );
            } else {
                let closed = tokio::time::timeout(GUARD, websocket.next())
                    .await
                    .expect("explicit stop should close the restarted gateway");
                assert!(closed.is_none() || matches!(closed, Some(Ok(Message::Close(_)))));
            }
        }
    });

    let channel = Arc::new(super::super::QQChannel::with_http_client(
        "app-id".into(),
        "app-secret".into(),
        vec!["alice".into()],
        gateway.client.clone(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (accepted, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(RejectOnceSink {
        attempts: AtomicUsize::new(0),
        accepted,
    });

    let rejected = tokio::time::timeout(GUARD, adapter.start(sink.as_ref()))
        .await
        .expect("a rejected typed publication should release the listener")
        .unwrap_err();
    assert_eq!(
        rejected.kind,
        tinychannels_bus::channel::SendErrorKind::Transient
    );
    assert!(rejected.retryable);
    assert!(rx.is_empty());

    let restarted = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(sink.as_ref()).await }
    });
    let ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("the same provider message should be offered again after rejection")
        .expect("test receiver remains connected");
    let ChannelIngress::Inbound(ingress) = ingress else {
        panic!("QQ messages must arrive as typed inbound ingress");
    };
    assert_eq!(ingress.message_id, "qq-retry-after-sink-error");
    assert_eq!(ingress.sender.id, "alice");
    assert_eq!(ingress.text, "retry this accepted message");

    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, restarted)
        .await
        .expect("stop must join the restarted listener")
        .expect("listener task must not panic")
        .expect("explicit stop should be clean");
    tokio::time::timeout(GUARD, mock_gateway)
        .await
        .expect("mock gateway should observe both connections close")
        .expect("mock gateway task must not panic");
}
