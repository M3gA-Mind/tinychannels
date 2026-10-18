use super::*;
use crate::channel::ChannelTypedSend;
use crate::host::MediaRepository;
use crate::providers::native::fixtures::{StubMedia, stored_media};
use serde_json::json;
use tinychannels_bus::channel::ConversationRef;
use tinychannels_bus::{
    ChannelOutboundMessage, ChannelRef, ContentPart, ConversationKind, MediaKind, MediaReference,
    OutboundContentPart,
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
    assert!(!bare.capabilities().content.image.outbound);
    assert!(!bare.capabilities().content.file.outbound);
}
