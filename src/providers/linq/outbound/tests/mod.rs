use super::*;
use crate::channel::{ChannelAdapter, ChannelTypedSend, ChannelTyping};
use crate::providers::linq::fixtures::*;
use crate::providers::native::fixtures::stored_media;
use serde_json::json;
use tinychannels_bus::channel::ConversationRef;
use tinychannels_bus::{
    ChannelOutboundMessage, ChannelRef, ContentPart, ConversationKind, MediaKind, MediaReference,
    OutboundContentPart, OutboundVoice,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn message(conversation: &str, parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "linq".into(),
        conversation_id: conversation.into(),
        channel_ref: Some(ChannelRef {
            id: "linq".into(),
            account_id: None,
        }),
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Dm,
            id: conversation.into(),
            ..Default::default()
        }),
        turn_id: Some("turn-1".into()),
        reply_to_id: None,
        thread_id: None,
        idempotency_key: "k".into(),
        parts,
    }
}

fn text(body: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: body.into(),
            markdown_dialect: None,
        },
    }
}

fn media_ref(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some("media-1".into()),
        kind,
        ..Default::default()
    }
}

async fn send(
    channel: &LinqChannel,
    message: &ChannelOutboundMessage,
) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ChannelSendError> {
    ChannelTypedSend::send_typed(channel, message).await
}

#[tokio::test]
async fn text_goes_to_the_chat_and_returns_the_message_id() {
    let server = mock_api(b"").await;
    let channel = channel_for(&server);
    let outcome = send(&channel, &message(CHAT, vec![text("hi")]))
        .await
        .unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("MSG-1")
    );
    let sent = &bodies(&server, "POST", "/messages").await[0];
    assert_eq!(
        sent["message"]["parts"],
        json!([{"type": "text", "value": "hi"}])
    );
}

#[tokio::test]
async fn a_phone_number_without_a_chat_opens_a_new_chat() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/chats/{PEER}/messages")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/chats"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"chat": {"id": "c", "message": {"id": "NEW-1"}}})),
        )
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let outcome = send(&channel, &message(PEER, vec![text("hello")]))
        .await
        .unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("NEW-1")
    );
    let created = &bodies(&server, "POST", "/chats").await[0];
    assert_eq!(created["from"], FROM);
    assert_eq!(created["to"], json!([PEER]));
}

#[tokio::test]
async fn long_text_is_chunked_into_separate_messages() {
    let server = mock_api(b"").await;
    let channel = channel_for(&server);
    let long = "word ".repeat(2000);
    let outcome = send(&channel, &message(CHAT, vec![text(&long)]))
        .await
        .unwrap();
    assert!(outcome.receipt.parts.len() >= 3);
    assert_eq!(
        bodies(&server, "POST", "/messages").await.len(),
        outcome.receipt.parts.len()
    );
}

#[tokio::test]
async fn files_upload_first_then_send_the_attachment_id() {
    let server = mock_api(b"").await;
    let rig = rig(&server);
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    let outcome = send(
        &rig.channel,
        &message(
            CHAT,
            vec![OutboundContentPart::Content {
                part: ContentPart::Image {
                    media: media_ref(MediaKind::Image),
                    caption: Some("look".into()),
                    alt_text: None,
                },
            }],
        ),
    )
    .await
    .unwrap();
    assert_eq!(outcome.receipt.parts.len(), 1);
    let upload = &bodies(&server, "POST", "/attachments").await[0];
    assert_eq!(upload["content_type"], "image/png");
    assert_eq!(upload["size_bytes"], 3);
    let puts = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.method.as_str() == "PUT")
        .collect::<Vec<_>>();
    assert_eq!(puts[0].body, b"png");
    assert_eq!(puts[0].headers.get("x-amz-meta").unwrap(), "1");
    assert!(puts[0].headers.get("authorization").is_none());
    let sent = &bodies(&server, "POST", "/messages").await[0];
    assert_eq!(
        sent["message"]["parts"],
        json!([{"type": "text", "value": "look"}, {"type": "media", "attachment_id": "att-1"}])
    );
}

#[tokio::test]
async fn voice_clips_are_sent_as_audio_attachments() {
    let server = mock_api(b"").await;
    let rig = rig(&server);
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Audio, "audio/mp4", b"m4a"));
    let outcome = send(
        &rig.channel,
        &message(
            CHAT,
            vec![OutboundContentPart::Voice {
                voice: OutboundVoice::Media {
                    media: media_ref(MediaKind::Audio),
                    codec: Some("m4a_aac".into()),
                    duration_ms: None,
                },
            }],
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        outcome.receipt.parts[0].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Voice
    );
    assert_eq!(
        bodies(&server, "POST", "/attachments").await[0]["content_type"],
        "audio/mp4"
    );
}

#[tokio::test]
async fn http_failures_map_to_typed_errors_without_leaking_bodies() {
    for (status, kind, retryable) in [
        (429u16, SendErrorKind::RateLimited, true),
        (403, SendErrorKind::Forbidden, false),
        (404, SendErrorKind::NotFound, false),
        (422, SendErrorKind::BadFormat, false),
        (503, SendErrorKind::Transient, true),
        (418, SendErrorKind::Unknown, false),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).set_body_string("secret-token-body"))
            .mount(&server)
            .await;
        let error = send(&channel_for(&server), &message(CHAT, vec![text("x")]))
            .await
            .unwrap_err();
        assert_eq!((error.kind, error.retryable), (kind, retryable), "{status}");
        assert!(!error.message.contains("secret"), "{status}");
        assert!(!error.sent_before_error, "{status}");
    }
}

#[tokio::test]
async fn an_unreachable_api_is_an_uncertain_outcome_and_a_missing_id_is_too() {
    let dead = LinqChannel::with_http_client(
        "t".into(),
        FROM.into(),
        vec![],
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
    .with_api_base("http://127.0.0.1:1");
    let error = send(&dead, &message(CHAT, vec![text("x")]))
        .await
        .unwrap_err();
    assert!(error.sent_before_error && !error.retryable);

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    let error = send(&channel_for(&server), &message(CHAT, vec![text("x")]))
        .await
        .unwrap_err();
    assert!(error.sent_before_error);

    let garbled = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&garbled)
        .await;
    assert!(
        send(&channel_for(&garbled), &message(CHAT, vec![text("x")]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typed_status_uses_the_linq_health_probe() {
    let healthy_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/phonenumbers"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&healthy_server)
        .await;
    let healthy = ChannelAdapter::status(&channel_for(&healthy_server))
        .await
        .unwrap();
    assert_eq!(healthy.health_state.as_deref(), Some("healthy"));

    let rejected_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/phonenumbers"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&rejected_server)
        .await;
    let rejected = ChannelAdapter::status(&channel_for(&rejected_server))
        .await
        .unwrap();
    assert_eq!(rejected.health_state.as_deref(), Some("unhealthy"));
}

#[tokio::test]
async fn upload_failures_never_reach_a_chat() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/attachments"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let rig = rig(&server);
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    let part = OutboundContentPart::Content {
        part: ContentPart::Image {
            media: media_ref(MediaKind::Image),
            caption: None,
            alt_text: None,
        },
    };
    assert!(
        send(&rig.channel, &message(CHAT, vec![part.clone()]))
            .await
            .is_err()
    );
    assert!(bodies(&server, "POST", "/messages").await.is_empty());

    // A target without an upload url, then a rejected PUT.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/attachments"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"attachment_id": "a"})))
        .mount(&server)
        .await;
    let rig2 = self::rig(&server);
    *rig2.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    assert!(
        send(&rig2.channel, &message(CHAT, vec![part.clone()]))
            .await
            .is_err()
    );

    let server = MockServer::start().await;
    let upload = format!("{}/up", server.uri());
    Mock::given(method("POST"))
        .and(path("/attachments"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"attachment_id": "a", "upload_url": upload})),
        )
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let rig3 = self::rig(&server);
    *rig3.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    assert!(
        send(&rig3.channel, &message(CHAT, vec![part]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn conversation_ids_cannot_alter_the_request_path() {
    let server = mock_api(b"").await;
    let channel = channel_for(&server);
    for bad in ["../admin", "a/b", "a b", "x?y=1"] {
        let error = send(&channel, &message(bad, vec![text("x")]))
            .await
            .unwrap_err();
        assert!(!error.retryable && !error.sent_before_error, "{bad}");
        assert!(
            ChannelTyping::start_typing(&channel, bad).await.is_err(),
            "{bad}"
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typing_starts_and_stops_through_the_api_and_reports_failures() {
    let server = mock_api(b"").await;
    let channel = channel_for(&server);
    ChannelTyping::start_typing(&channel, CHAT).await.unwrap();
    ChannelTyping::stop_typing(&channel, CHAT).await.unwrap();
    let methods: Vec<_> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| request.method.to_string())
        .collect();
    assert_eq!(methods, ["POST", "DELETE"]);

    let failing = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&failing)
        .await;
    assert!(
        ChannelTyping::start_typing(&channel_for(&failing), CHAT)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn operations_the_api_cannot_do_are_explicitly_unsupported() {
    let server = mock_api(b"").await;
    let channel = channel_for(&server);
    let reaction = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Dm,
        id: CHAT.into(),
        ..Default::default()
    };
    let channel_ref = ChannelRef {
        id: "linq".into(),
        account_id: None,
    };
    let error = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &channel_ref,
        &conversation,
        "m",
        &reaction,
        false,
    )
    .await
    .unwrap_err();
    assert!(error.unsupported);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn the_descriptor_and_legacy_methods_use_the_native_surface() {
    let server = mock_api(b"").await;
    let channel = channel_for(&server);
    let descriptor = ChannelAdapter::descriptor(&channel);
    assert_eq!(descriptor.id, "linq");
    assert_eq!(
        descriptor.metadata.get("voiceOut").map(String::as_str),
        Some("attachment")
    );
    assert!(ChannelAdapter::declared_capabilities(&channel).remote_control);
    let outcome = ChannelTypedSend::send_typed(&channel, &message(CHAT, vec![text("legacy")]))
        .await
        .unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("MSG-1")
    );
}
