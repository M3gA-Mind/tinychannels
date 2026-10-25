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
async fn configured_mattermost_reaction_uses_authenticated_native_v4_requests() {
    use tinychannels_bus::channel::{ChannelRef, ConversationKind, ConversationRef};
    use wiremock::matchers::{body_json, header, method, path};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "bot-user-1",
            "username": "fixture-bot"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(json!({
            "user_id": "bot-user-1",
            "post_id": "post-42",
            "emoji_name": "thumbsup"
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "user_id": "bot-user-1",
            "post_id": "post-42",
            "emoji_name": "thumbsup"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let channel = MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        Some("channel-42".into()),
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );
    let result = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &ChannelRef {
            id: "mattermost".into(),
            account_id: None,
        },
        &ConversationRef {
            kind: ConversationKind::Channel,
            id: "channel-42".into(),
            ..Default::default()
        },
        "post-42",
        &tinychannels_bus::channel::ChannelReaction {
            value: "thumbsup".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await;
    assert!(
        result.is_ok(),
        "Mattermost reaction request failed: {result:?}"
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        request
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            == Some("Bearer test-token")
    }));
}

#[tokio::test]
async fn mattermost_reaction_removal_uses_the_post_reaction_route() {
    use tinychannels_bus::channel::{ChannelRef, ConversationKind, ConversationRef};
    use wiremock::matchers::{header, method, path};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id":"bot-user-1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(
            "/api/v4/users/bot-user-1/posts/post-42/reactions/thumbsup",
        ))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let channel = channel(&server, Arc::new(StubMedia::default()));
    crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &ChannelRef {
            id: "mattermost".into(),
            account_id: Some("account-1".into()),
        },
        &ConversationRef {
            kind: ConversationKind::Channel,
            id: "channel-1".into(),
            ..Default::default()
        },
        "post-42",
        &tinychannels_bus::channel::ChannelReaction {
            value: "thumbsup".into(),
            custom_emoji_id: None,
        },
        true,
    )
    .await
    .expect("reaction removal should use Mattermost's native route");
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn mattermost_rejects_invalid_reactions_before_any_http_request() {
    use tinychannels_bus::channel::{ChannelRef, ConversationKind, ConversationRef};

    let server = MockServer::start().await;
    let channel = channel(&server, Arc::new(StubMedia::default()));
    let channel_ref = ChannelRef {
        id: "mattermost".into(),
        account_id: Some("account-1".into()),
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Channel,
        id: "channel-1".into(),
        ..Default::default()
    };
    for (post_id, reaction) in [
        (
            "",
            tinychannels_bus::channel::ChannelReaction {
                value: "thumbsup".into(),
                custom_emoji_id: None,
            },
        ),
        (
            "post-1",
            tinychannels_bus::channel::ChannelReaction {
                value: "not an emoji".into(),
                custom_emoji_id: None,
            },
        ),
        (
            "post-1",
            tinychannels_bus::channel::ChannelReaction {
                value: "thumbsup".into(),
                custom_emoji_id: Some("custom-1".into()),
            },
        ),
    ] {
        let error = crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &channel_ref,
            &conversation,
            post_id,
            &reaction,
            false,
        )
        .await
        .expect_err("invalid reaction input must be rejected");
        assert_eq!(error.kind, SendErrorKind::BadFormat);
        assert!(!error.sent_before_error);
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn mattermost_identity_json_errors_never_submit_a_reaction() {
    use tinychannels_bus::channel::{ChannelRef, ConversationKind, ConversationRef};
    use wiremock::matchers::{method, path};

    for body in ["not-json", "{}"] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v4/users/me"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(&server)
            .await;
        let channel = channel(&server, Arc::new(StubMedia::default()));
        let error = crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &ChannelRef {
                id: "mattermost".into(),
                account_id: Some("account-1".into()),
            },
            &ConversationRef {
                kind: ConversationKind::Channel,
                id: "channel-1".into(),
                ..Default::default()
            },
            "post-1",
            &tinychannels_bus::channel::ChannelReaction {
                value: "thumbsup".into(),
                custom_emoji_id: None,
            },
            false,
        )
        .await
        .expect_err("invalid identity response must stop reaction delivery");
        assert_eq!(error.kind, SendErrorKind::Unknown);
        assert!(!error.sent_before_error);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn mattermost_native_edit_and_delete_report_ambiguous_server_failures() {
    use crate::channel::{ChannelDelete, ChannelEdit, ChannelTypedSend};
    use tinychannels_bus::channel::{ChannelRef, ConversationKind, ConversationRef};
    use tinychannels_bus::{ChannelOutboundIntent, OutboundPayload};
    use wiremock::matchers::{method, path};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id":"post-1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/v4/posts/post-1"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/v4/posts/post-1"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let channel = channel(&server, Arc::new(StubMedia::default()));
    let original = ChannelTypedSend::send_typed(
        &channel,
        &ChannelOutboundMessage {
            channel_id: "mattermost".into(),
            conversation_id: "channel-1".into(),
            channel_ref: Some(ChannelRef {
                id: "mattermost".into(),
                account_id: Some("account-1".into()),
            }),
            conversation_ref: Some(ConversationRef {
                kind: ConversationKind::Channel,
                id: "channel-1".into(),
                ..Default::default()
            }),
            turn_id: None,
            reply_to_id: None,
            thread_id: None,
            idempotency_key: "edit-test".into(),
            parts: vec![OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "before".into(),
                    markdown_dialect: None,
                },
            }],
        },
    )
    .await
    .unwrap();
    let intent = ChannelOutboundIntent {
        channel_id: "mattermost".into(),
        conversation_id: "channel-1".into(),
        channel_ref: Some(ChannelRef {
            id: "mattermost".into(),
            account_id: Some("account-1".into()),
        }),
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Channel,
            id: "channel-1".into(),
            ..Default::default()
        }),
        payload: OutboundPayload::Text {
            text: "updated".into(),
        },
        ..Default::default()
    };
    let edit_error = ChannelEdit::edit(&channel, &original.receipt, intent)
        .await
        .expect_err("server error after edit submission is ambiguous");
    assert_eq!(edit_error.kind, SendErrorKind::Unknown);
    assert!(!edit_error.retryable);
    assert!(edit_error.sent_before_error);

    let delete_error = ChannelDelete::delete(&channel, &original.receipt)
        .await
        .expect_err("server error after delete submission is ambiguous");
    assert_eq!(delete_error.kind, SendErrorKind::Unknown);
    assert!(!delete_error.retryable);
    assert!(delete_error.sent_before_error);
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}

#[tokio::test]
async fn mattermost_transport_failures_preserve_send_edit_delete_and_identity_semantics() {
    use crate::channel::{ChannelDelete, ChannelEdit, ChannelTypedSend};
    use crate::providers::native::NativeProvider;
    use tinychannels_bus::channel::{
        ChannelRef, ConversationKind, ConversationRef, MessageReceipt,
    };
    use tinychannels_bus::{ChannelOutboundIntent, OutboundPayload};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            drop(stream);
        }
    });
    let channel = MattermostChannel::with_http_client(
        format!("http://{address}"),
        "test-token".into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );
    NativeProvider::set_typing(&channel, "channel-1", true)
        .await
        .unwrap();
    NativeProvider::set_typing(&channel, "channel-1", false)
        .await
        .unwrap();

    let send_error = ChannelTypedSend::send_typed(
        &channel,
        &ChannelOutboundMessage {
            channel_id: "mattermost".into(),
            conversation_id: "channel-1".into(),
            channel_ref: Some(ChannelRef {
                id: "mattermost".into(),
                account_id: Some("account-1".into()),
            }),
            conversation_ref: Some(ConversationRef {
                kind: ConversationKind::Channel,
                id: "channel-1".into(),
                ..Default::default()
            }),
            reply_to_id: None,
            thread_id: None,
            turn_id: None,
            idempotency_key: "transport-failure".into(),
            parts: vec![OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "hello".into(),
                    markdown_dialect: None,
                },
            }],
        },
    )
    .await
    .expect_err("a refused final post has an ambiguous delivery state");
    assert_eq!(send_error.kind, SendErrorKind::Unknown);
    assert!(send_error.sent_before_error);
    assert!(!send_error.retryable);

    let receipt = MessageReceipt {
        channel: Some(ChannelRef {
            id: "mattermost".into(),
            account_id: Some("account-1".into()),
        }),
        conversation: Some(ConversationRef {
            kind: ConversationKind::Channel,
            id: "channel-1".into(),
            ..Default::default()
        }),
        primary_platform_message_id: Some("post-1".into()),
        ..Default::default()
    };
    let edit_error = ChannelEdit::edit(
        &channel,
        &receipt,
        ChannelOutboundIntent {
            channel_id: "mattermost".into(),
            conversation_id: "channel-1".into(),
            payload: OutboundPayload::Text {
                text: "edited".into(),
            },
            ..Default::default()
        },
    )
    .await
    .expect_err("a refused edit may have reached the server");
    assert_eq!(edit_error.kind, SendErrorKind::Unknown);
    assert!(edit_error.sent_before_error);
    assert!(!edit_error.retryable);

    let delete_error = ChannelDelete::delete(&channel, &receipt)
        .await
        .expect_err("a refused delete may have reached the server");
    assert_eq!(delete_error.kind, SendErrorKind::Unknown);
    assert!(delete_error.sent_before_error);
    assert!(!delete_error.retryable);

    let identity_error = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        receipt.channel.as_ref().unwrap(),
        receipt.conversation.as_ref().unwrap(),
        "post-1",
        &tinychannels_bus::channel::ChannelReaction {
            value: "thumbsup".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await
    .expect_err("a failed identity lookup must not count as a reaction mutation");
    assert_eq!(identity_error.kind, SendErrorKind::Transient);
    assert!(identity_error.retryable);
    assert!(!identity_error.sent_before_error);
    server.abort();
}

#[tokio::test]
async fn mattermost_reaction_errors_distinguish_safe_retry_from_unknown_mutation() {
    use tinychannels_bus::channel::{ChannelRef, ConversationKind, ConversationRef, SendErrorKind};
    use wiremock::matchers::{body_json, header, method, path};

    let server = MockServer::start().await;
    let identity_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let identity_request_count = identity_requests.clone();
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(move |_request: &wiremock::Request| {
            if identity_request_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200).set_body_json(json!({ "id": "bot-user-1" }))
            }
        })
        .expect(3)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(json!({
            "user_id": "bot-user-1",
            "post_id": "post-500",
            "emoji_name": "thumbsup"
        })))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(json!({
            "user_id": "bot-user-1",
            "post_id": "post-429",
            "emoji_name": "thumbsup"
        })))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "2"))
        .expect(1)
        .mount(&server)
        .await;

    let channel = channel(&server, Arc::new(StubMedia::default()));
    let channel_ref = ChannelRef {
        id: "mattermost".into(),
        account_id: Some("account-1".into()),
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Channel,
        id: "channel-1".into(),
        ..Default::default()
    };
    let reaction = tinychannels_bus::channel::ChannelReaction {
        value: "thumbsup".into(),
        custom_emoji_id: None,
    };

    let identity_error = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &channel_ref,
        &conversation,
        "post-identity",
        &reaction,
        false,
    )
    .await
    .expect_err("failed identity lookup must be reported");
    assert_eq!(identity_error.kind, SendErrorKind::Transient);
    assert!(identity_error.retryable);
    assert!(!identity_error.sent_before_error);

    let mutation_error = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &channel_ref,
        &conversation,
        "post-500",
        &reaction,
        false,
    )
    .await
    .expect_err("ambiguous server failure must be reported");
    assert_eq!(mutation_error.kind, SendErrorKind::Unknown);
    assert!(!mutation_error.retryable);
    assert!(mutation_error.sent_before_error);

    let rate_limit = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &channel_ref,
        &conversation,
        "post-429",
        &reaction,
        false,
    )
    .await
    .expect_err("rate limiting must be reported");
    assert_eq!(rate_limit.kind, SendErrorKind::RateLimited);
    assert!(rate_limit.retryable);
    assert_eq!(rate_limit.retry_after, Some(2.0));
}

#[tokio::test]
async fn typed_edit_and_delete_use_the_provider_returned_post_id() {
    use crate::channel::{ChannelDelete, ChannelEdit, ChannelTypedSend};
    use tinychannels_bus::channel::{ChannelRef, ConversationKind, ConversationRef};
    use tinychannels_bus::{ChannelOutboundIntent, OutboundPayload};
    use wiremock::matchers::{body_json, header, method, path};

    let server = MockServer::start().await;
    let next_id = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ids = next_id.clone();
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(move |_request: &wiremock::Request| {
            let id = if ids.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                "post-edit-1"
            } else {
                "post-delete-1"
            };
            ResponseTemplate::new(201).set_body_json(json!({"id": id}))
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/v4/posts/post-edit-1"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(
            json!({"id": "post-edit-1", "message": "updated"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "post-edit-1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/v4/posts/post-delete-1"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "OK"})))
        .expect(1)
        .mount(&server)
        .await;

    let channel = channel(&server, Arc::new(StubMedia::default()));
    let make_text = |key: &str, text: &str| ChannelOutboundMessage {
        channel_id: "mattermost".into(),
        conversation_id: "channel-1".into(),
        channel_ref: Some(ChannelRef {
            id: "mattermost".into(),
            account_id: Some("account-1".into()),
        }),
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Channel,
            id: "channel-1".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: None,
        idempotency_key: key.into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Text {
                text: text.into(),
                markdown_dialect: None,
            },
        }],
    };
    let original = ChannelTypedSend::send_typed(&channel, &make_text("edit-target", "before"))
        .await
        .unwrap();
    let intent = ChannelOutboundIntent {
        idempotency_key: "edit-1".into(),
        channel_id: "mattermost".into(),
        conversation_id: "channel-1".into(),
        channel_ref: Some(ChannelRef {
            id: "mattermost".into(),
            account_id: Some("account-1".into()),
        }),
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Channel,
            id: "channel-1".into(),
            ..Default::default()
        }),
        payload: OutboundPayload::Text {
            text: "updated".into(),
        },
        ..Default::default()
    };
    let edited = ChannelEdit::edit(&channel, &original.receipt, intent)
        .await
        .expect("typed edit should use the receipt returned by Mattermost");
    assert_eq!(
        edited.primary_platform_message_id.as_deref(),
        Some("post-edit-1")
    );

    let deletion_target =
        ChannelTypedSend::send_typed(&channel, &make_text("delete-target", "to delete"))
            .await
            .unwrap();
    ChannelDelete::delete(&channel, &deletion_target.receipt)
        .await
        .expect("typed delete should use the receipt returned by Mattermost");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4);
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
            codec: Some("m4a_aac".into()),
            duration_ms: Some(1500),
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

#[derive(Clone)]
struct RecordingInboundSink(tokio::sync::mpsc::UnboundedSender<tinychannels_bus::ChannelIngress>);

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for RecordingInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.0
            .send(ingress)
            .map_err(|_| crate::channel::ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "test sink closed".into(),
                retryable: true,
                ..Default::default()
            })
    }
}

#[tokio::test]
async fn public_listener_rejects_missing_channel_id_before_http_and_releases_registration() {
    let server = MockServer::start().await;
    let channel = channel(&server, Arc::new(StubMedia::default()));
    let (events, _received) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(events);

    for _ in 0..2 {
        let error = ChannelAdapter::start(&channel, &sink)
            .await
            .expect_err("listening without a channel id must be rejected");
        assert_eq!(error.kind, SendErrorKind::BadFormat);
        assert!(!error.retryable);
        assert!(!error.sent_before_error);
        assert!(error.message.contains("channel_id"));
    }

    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "invalid listener configuration must fail before any HTTP request"
    );
}

#[tokio::test]
async fn public_adapter_start_owns_mattermost_polling_until_stop_and_restart() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const GUARD: Duration = Duration::from_secs(12);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "bot-1",
            "username": "mattermost-bot"
        })))
        .mount(&server)
        .await;
    let create_at = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 10 * 60 * 1000;
    let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let poll_sequence = Arc::clone(&poll_count);
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/channel-1/posts"))
        .respond_with(move |_request: &wiremock::Request| {
            let (id, text, thread, timestamp) =
                if poll_sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    ("post-1", "first typed delivery", "thread-1", create_at)
                } else {
                    (
                        "post-2",
                        "delivery after restart",
                        "thread-2",
                        create_at + 1,
                    )
                };
            ResponseTemplate::new(200).set_body_json(json!({
                "posts": {
                    (id): {
                        "id": id,
                        "user_id": "alice",
                        "message": text,
                        "create_at": timestamp,
                        "root_id": thread
                    }
                }
            }))
        })
        .mount(&server)
        .await;

    let channel = Arc::new(MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        Some("channel-1".into()),
        vec!["alice".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);

    for expected_delivery in 1..=2 {
        let mut listener = tokio::spawn({
            let adapter = adapter.clone();
            let sink = sink.clone();
            async move { adapter.start(&sink).await }
        });
        let ingress = tokio::select! {
            result = &mut listener => panic!(
                "Mattermost adapter start returned before delivering poll {expected_delivery}: {result:?}"
            ),
            received = tokio::time::timeout(GUARD, rx.recv()) => received
                .expect("Mattermost should poll and publish through the supplied sink")
                .expect("Mattermost listener should keep the supplied sink connected"),
        };
        let tinychannels_bus::ChannelIngress::Inbound(envelope) = ingress else {
            panic!("Mattermost posts must use typed inbound envelopes");
        };
        assert_eq!(envelope.channel.id, "mattermost");
        assert_eq!(envelope.conversation.id, "channel-1");
        let expected_id = if expected_delivery == 1 {
            "post-1"
        } else {
            "post-2"
        };
        let expected_thread = if expected_delivery == 1 {
            "thread-1"
        } else {
            "thread-2"
        };
        let expected_text = if expected_delivery == 1 {
            "first typed delivery"
        } else {
            "delivery after restart"
        };
        let expected_timestamp = if expected_delivery == 1 {
            create_at
        } else {
            create_at + 1
        };
        assert_eq!(
            envelope.conversation.thread_id.as_deref(),
            Some(expected_thread)
        );
        assert_eq!(envelope.message_id, expected_id);
        assert_eq!(envelope.sender.id, "alice");
        assert_eq!(
            envelope.timestamp_unix_ms,
            Some(u64::try_from(expected_timestamp).unwrap())
        );
        assert_eq!(envelope.text, expected_text);

        adapter.stop().await.unwrap();
        tokio::time::timeout(GUARD, &mut listener)
            .await
            .expect("Mattermost stop must join its polling listener")
            .expect("Mattermost listener must not panic")
            .expect("Mattermost listener must stop cleanly");
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "one poll must publish only once"
        );
    }

    let requests = server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v4/users/me")
            .count()
            >= 2,
        "restart should re-establish authenticated bot identity"
    );
}

#[tokio::test]
async fn aborting_public_listener_releases_ownership_for_restart() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const GUARD: Duration = Duration::from_secs(12);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "bot-1",
            "username": "mattermost-bot"
        })))
        .mount(&server)
        .await;
    let create_at = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 10 * 60 * 1000;
    let restarted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let response_restarted = Arc::clone(&restarted);
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/channel-1/posts"))
        .respond_with(move |_request: &wiremock::Request| {
            let (id, text, timestamp) =
                if response_restarted.load(std::sync::atomic::Ordering::SeqCst) {
                    ("post-after-abort", "after abort", create_at + 1)
                } else {
                    ("post-before-abort", "before abort", create_at)
                };
            ResponseTemplate::new(200).set_body_json(json!({
                "posts": {
                    (id): {
                        "id": id,
                        "user_id": "alice",
                        "message": text,
                        "create_at": timestamp,
                        "root_id": "thread-1"
                    }
                }
            }))
        })
        .mount(&server)
        .await;

    let channel = Arc::new(MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        Some("channel-1".into()),
        vec!["alice".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);
    let first = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let first_ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("first listener should publish before cancellation")
        .expect("test sink remains connected");
    let tinychannels_bus::ChannelIngress::Inbound(first_envelope) = first_ingress else {
        panic!("Mattermost posts use typed inbound envelopes");
    };
    assert_eq!(first_envelope.message_id, "post-before-abort");
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    restarted.store(true, std::sync::atomic::Ordering::SeqCst);

    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let second_ingress = tokio::select! {
        result = &mut listener => panic!("restart returned before typed delivery: {result:?}"),
        received = tokio::time::timeout(GUARD, rx.recv()) => received
            .expect("aborted listener must release ownership so restart can poll")
            .expect("test sink remains connected"),
    };
    let tinychannels_bus::ChannelIngress::Inbound(second_envelope) = second_ingress else {
        panic!("Mattermost posts use typed inbound envelopes");
    };
    assert_eq!(second_envelope.message_id, "post-after-abort");
    assert_eq!(second_envelope.text, "after abort");
    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("stop must join restarted listener")
        .expect("restarted listener must not panic")
        .expect("restarted listener must stop cleanly");
}

#[tokio::test]
async fn typed_listener_delivers_distinct_posts_with_equal_timestamps() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const GUARD: Duration = Duration::from_secs(12);
    let server = MockServer::start().await;
    let create_at = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 10 * 60 * 1000;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "bot-1",
            "username": "mattermost-bot"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/channel-1/posts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": {
                "post-0-denied": {
                    "id": "post-0-denied",
                    "user_id": "mallory",
                    "message": "denied first",
                    "create_at": create_at,
                    "root_id": "thread-1"
                },
                "post-1-allowed": {
                    "id": "post-1-allowed",
                    "user_id": "alice",
                    "message": "first allowed",
                    "create_at": create_at,
                    "root_id": "thread-1"
                },
                "post-2-allowed": {
                    "id": "post-2-allowed",
                    "user_id": "alice",
                    "message": "second allowed",
                    "create_at": create_at,
                    "root_id": "thread-1"
                }
            }
        })))
        .mount(&server)
        .await;

    let channel = Arc::new(MattermostChannel::with_http_client(
        server.uri(),
        "test-token".into(),
        Some("channel-1".into()),
        vec!["alice".into()],
        true,
        false,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    ));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);
    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });

    for (expected_id, expected_text) in [
        ("post-1-allowed", "first allowed"),
        ("post-2-allowed", "second allowed"),
    ] {
        let ingress = tokio::select! {
            result = &mut listener => panic!("listener returned before {expected_id}: {result:?}"),
            received = tokio::time::timeout(GUARD, rx.recv()) => received
                .expect("same-timestamp posts in one response batch must both be delivered")
                .expect("test sink remains connected"),
        };
        let tinychannels_bus::ChannelIngress::Inbound(envelope) = ingress else {
            panic!("Mattermost posts use typed inbound envelopes");
        };
        assert_eq!(envelope.message_id, expected_id);
        assert_eq!(envelope.text, expected_text);
    }
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("stop must join Mattermost listener")
        .expect("listener must not panic")
        .expect("listener must stop cleanly");
}

#[tokio::test]
async fn public_mattermost_listener_admits_before_fetching_attachments() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const GUARD: Duration = Duration::from_secs(12);
    let server = MockServer::start().await;
    let create_at = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 10 * 60 * 1000;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "bot-1",
            "username": "mattermost-bot"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/channel-1/posts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": {
                "post-unmentioned": {
                    "id": "post-unmentioned",
                    "user_id": "alice",
                    "message": "do not fetch this file",
                    "create_at": create_at,
                    "root_id": "",
                    "file_ids": ["file-unmentioned"]
                },
                "post-denied": {
                    "id": "post-denied",
                    "user_id": "mallory",
                    "message": "@mattermost-bot denied",
                    "create_at": create_at + 1,
                    "root_id": "",
                    "file_ids": ["file-denied"]
                },
                "post-allowed": {
                    "id": "post-allowed",
                    "user_id": "alice",
                    "message": "@mattermost-bot keep this",
                    "create_at": create_at + 2,
                    "root_id": "",
                    "file_ids": ["file-allowed"]
                }
            }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/files/file-allowed/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "file-allowed",
            "mime_type": "text/plain",
            "name": "allowed.txt",
            "size": 7
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/files/file-allowed"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"allowed"))
        .expect(1)
        .mount(&server)
        .await;

    let channel = Arc::new(
        MattermostChannel::with_http_client(
            server.uri(),
            "test-token".into(),
            Some("channel-1".into()),
            vec!["alice".into()],
            true,
            true,
            reqwest::Client::builder().no_proxy().build().unwrap(),
        )
        .with_media_repository(Arc::new(StubMedia::default())),
    );
    let adapter: Arc<dyn ChannelAdapter> = channel.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);
    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let ingress = tokio::select! {
        result = &mut listener => panic!("Mattermost start returned before admitted ingress: {result:?}"),
        received = tokio::time::timeout(GUARD, rx.recv()) => received
            .expect("an admitted mention should reach the supplied sink")
            .expect("listener should preserve its sink"),
    };
    let tinychannels_bus::ChannelIngress::Inbound(envelope) = ingress else {
        panic!("admitted Mattermost posts use typed inbound envelopes");
    };
    assert_eq!(envelope.message_id, "post-allowed");
    assert_eq!(envelope.sender.id, "alice");
    assert!(envelope.text.contains("keep this"));
    assert_eq!(
        envelope.parts.len(),
        2,
        "text and allowed file are retained"
    );
    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("stop must join the Mattermost poller")
        .expect("listener must not panic")
        .expect("listener must stop cleanly");

    let requests = server.received_requests().await.unwrap();
    let file_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path().contains("/api/v4/files/"))
        .collect();
    assert_eq!(file_requests.len(), 2);
    assert!(
        file_requests
            .iter()
            .all(|request| { request.url.path().contains("file-allowed") }),
        "sender and mention filters must run before file metadata/download requests"
    );
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
async fn unreadable_upload_receipt_does_not_attempt_to_create_a_post() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/files"))
        .respond_with(ResponseTemplate::new(201).set_body_string("not-json"))
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
