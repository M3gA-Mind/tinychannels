use super::super::http_test_support::HttpsServer;
use super::*;
use crate::factory::{HttpClientFactory, build_long_tail_channel_with_host};
use crate::host::ChannelHostBuilder;
use crate::providers::native::fixtures::{StubMedia, stored_media};
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::{
    ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
};

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
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = Some(stored_media(
        MediaKind::Image,
        "image/png",
        b"exact-scoped-png-bytes",
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
    assert!(channel.capabilities().content.image.outbound);

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
