use super::*;
use crate::providers::slack::fixtures::*;
use std::sync::Arc;
use tinychannels_bus::ConversationKind;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn text(value: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: value.into(),
            markdown_dialect: None,
        },
    }
}

fn content(part: ContentPart) -> OutboundContentPart {
    OutboundContentPart::Content { part }
}

fn message(parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "slack".into(),
        conversation_id: "C1".into(),
        channel_ref: Some(ChannelRef {
            id: "slack".into(),
            account_id: Some("acct".into()),
        }),
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Channel,
            id: "C1".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "key-1".into(),
        parts,
    }
}

fn button(id: &str) -> ButtonAction {
    ButtonAction {
        id: id.into(),
        label: "Go".into(),
        style: None,
        disabled: false,
    }
}

async fn posting(server: &MockServer) {
    mount(server, "chat.postMessage", ok(json!({"ts": "9.9"}))).await;
}

async fn upload_flow(server: &MockServer) {
    mount(
        server,
        "files.getUploadURLExternal",
        ok(json!({"upload_url": format!("{}/upload/F1", server.uri()), "file_id": "F1"})),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/upload/F1"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
    mount(
        server,
        "files.completeUploadExternal",
        ok(json!({"files": [{"id": "F1"}]})),
    )
    .await;
}

async fn untouched(server: &MockServer) -> bool {
    server.received_requests().await.unwrap().is_empty()
}

#[tokio::test]
async fn text_is_sent_as_mrkdwn_and_returns_the_message_timestamp() {
    let server = mock_api().await;
    posting(&server).await;
    let channel = channel_for(&server, &[]);
    let outcome = channel
        .send_v2_with_receipt(&message(vec![text(
            "**hi** <tool_call>{\"x\":1}</tool_call> there",
        )]))
        .await
        .unwrap();
    let body = &bodies(&server, "chat.postMessage").await[0];
    assert_eq!(body["channel"], "C1");
    assert_eq!(body["text"], "*hi*  there");
    assert_eq!(body["mrkdwn"], true);
    assert!(body.get("thread_ts").is_none());
    let receipt = outcome.receipt;
    assert!(outcome.error.is_none());
    assert_eq!(receipt.platform_message_ids, ["9.9"]);
    assert_eq!(receipt.primary_platform_message_id.as_deref(), Some("9.9"));
    assert_eq!(
        (
            receipt.edit_token.as_deref(),
            receipt.delete_token.as_deref()
        ),
        (Some("9.9"), Some("9.9"))
    );
    assert_eq!(receipt.idempotency_key.as_deref(), Some("key-1"));
    assert_eq!(receipt.conversation.as_ref().unwrap().id, "C1");
    assert_eq!(receipt.parts[0].kind, MessageReceiptPartKind::Text);
    assert_eq!(
        receipt.parts[0].raw.as_ref().unwrap().channel_id.as_deref(),
        Some("C1")
    );
}

#[tokio::test]
async fn replies_and_threads_become_thread_ts() {
    let server = mock_api().await;
    posting(&server).await;
    let channel = channel_for(&server, &[]);

    let mut threaded = message(vec![text("a")]);
    threaded.thread_id = Some("1.0".into());
    channel.send_v2_with_receipt(&threaded).await.unwrap();

    let mut topic = message(vec![text("b")]);
    topic.conversation_ref.as_mut().unwrap().topic_id = Some("3.0".into());
    channel.send_v2_with_receipt(&topic).await.unwrap();

    let mut reply = message(vec![text("c")]);
    reply.reply_to_id = Some("2.0".into());
    let outcome = channel.send_v2_with_receipt(&reply).await.unwrap();
    assert_eq!(outcome.receipt.reply_to_id.as_deref(), Some("2.0"));
    assert_eq!(outcome.receipt.thread_id.as_deref(), Some("2.0"));

    let mut bare = message(vec![text("d")]);
    bare.conversation_ref = None;
    bare.thread_id = Some("4.0".into());
    channel.send_v2_with_receipt(&bare).await.unwrap();

    let threads: Vec<_> = bodies(&server, "chat.postMessage")
        .await
        .iter()
        .map(|b| b["thread_ts"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(threads, ["1.0", "3.0", "2.0", "4.0"]);
}

#[tokio::test]
async fn several_text_parts_and_long_text_become_ordered_messages() {
    let server = mock_api().await;
    posting(&server).await;
    let channel = channel_for(&server, &[]);
    let long = "word ".repeat(1000);
    let outcome = channel
        .send_v2_with_receipt(&message(vec![text("first"), text(&long)]))
        .await
        .unwrap();
    let posts = bodies(&server, "chat.postMessage").await;
    assert_eq!(posts.len(), 3);
    assert_eq!(posts[0]["text"], "first");
    assert!(posts[1]["text"].as_str().unwrap().chars().count() <= 3900);
    assert_eq!(outcome.receipt.parts.len(), 3);
    assert_eq!(outcome.receipt.parts[2].index, 2);
    assert!(
        outcome.receipt.edit_token.is_none(),
        "a multi-message send has no single edit token"
    );
}

#[tokio::test]
async fn buttons_and_approvals_render_block_kit() {
    let server = mock_api().await;
    posting(&server).await;
    let channel = channel_for(&server, &[]);
    let outcome = channel
        .send_v2_with_receipt(&message(vec![
            text("Pick"),
            content(ContentPart::Buttons {
                rows: vec![vec![button("a")]],
            }),
        ]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.parts[0].kind, MessageReceiptPartKind::Card);
    let approval =
        json!({"kind": "approval", "id": "r1", "prompt": "Run it?", "choices": ["Yes", "No"]});
    channel
        .send_v2_with_receipt(&message(vec![content(ContentPart::Presentation {
            blocks: approval,
        })]))
        .await
        .unwrap();
    channel
        .send_v2_with_receipt(&message(vec![content(ContentPart::Presentation {
            blocks: json!(["plain heading"]),
        })]))
        .await
        .unwrap();
    let posts = bodies(&server, "chat.postMessage").await;
    assert_eq!(posts[0]["blocks"][0]["text"]["text"], "Pick");
    assert_eq!(posts[0]["blocks"][1]["elements"][0]["action_id"], "a");
    assert_eq!(
        posts[1]["blocks"][1]["elements"][1]["action_id"],
        "approval-v1:r1:1"
    );
    assert!(posts[2].get("blocks").is_none());
    assert_eq!(posts[2]["text"], "plain heading");
}

#[tokio::test]
async fn invalid_content_is_rejected_before_any_request() {
    let server = mock_api().await;
    posting(&server).await;
    let channel = channel_for(&server, &[]);
    let empty_presentation = content(ContentPart::Presentation {
        blocks: json!([" "]),
    });
    let bad_presentation = content(ContentPart::Presentation { blocks: json!(7) });
    let buttons = |rows| content(ContentPart::Buttons { rows });
    let mut no_conversation = message(vec![text("x")]);
    no_conversation.conversation_ref = None;
    no_conversation.conversation_id = " ".into();
    let cases = vec![
        message(vec![]),
        message(vec![text("  ")]),
        no_conversation,
        message(vec![empty_presentation]),
        message(vec![bad_presentation]),
        message(vec![buttons(vec![vec![button("a")]])]),
        message(vec![text("t"), buttons(vec![])]),
        message(vec![
            text("t"),
            buttons(vec![vec![button("a")]]),
            content(ContentPart::Presentation { blocks: json!("x") }),
        ]),
    ];
    for case in cases {
        let error = channel.send_v2_with_receipt(&case).await.unwrap_err();
        assert!(!error.unsupported && !error.sent_before_error);
    }
    assert!(untouched(&server).await);
}

#[tokio::test]
async fn content_slack_cannot_carry_is_explicitly_unsupported() {
    let server = mock_api().await;
    let channel = channel_for(&server, &[]).with_media_repository(Arc::new(StubMedia::default()));
    let parts = [
        ContentPart::Location {
            latitude: 1.0,
            longitude: 2.0,
            label: None,
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: None,
        },
        ContentPart::Contact {
            display_name: "A".into(),
            phone_numbers: vec![],
            emails: vec![],
            organization: None,
        },
        ContentPart::Poll {
            question: "q".into(),
            options: vec![],
            multiple_choice: false,
            closes_at_unix_ms: None,
            provider_poll_id: None,
        },
        ContentPart::Sticker {
            media: Default::default(),
            pack_id: None,
            sticker_id: None,
            emoji: None,
            alt_text: None,
        },
        ContentPart::Native {
            provider: "slack".into(),
            data: json!({}),
        },
    ];
    for part in parts {
        let error = channel
            .send_v2_with_receipt(&message(vec![content(part)]))
            .await
            .unwrap_err();
        assert!(error.unsupported && !error.retryable && !error.sent_before_error);
    }
    assert!(untouched(&server).await);
}

#[tokio::test]
async fn slack_failures_surface_as_typed_errors() {
    let server = mock_api().await;
    mount(
        &server,
        "chat.postMessage",
        json!({"ok": false, "error": "not_in_channel"}),
    )
    .await;
    let channel = channel_for(&server, &[]);
    let error = channel
        .send_v2_with_receipt(&message(vec![text("x")]))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Forbidden);

    let no_ts = mock_api().await;
    mount(&no_ts, "chat.postMessage", ok(json!({}))).await;
    let error = channel_for(&no_ts, &[])
        .send_v2_with_receipt(&message(vec![text("x")]))
        .await
        .unwrap_err();
    assert!(
        error.sent_before_error,
        "an unreadable success leaves the outcome uncertain"
    );
}

#[tokio::test]
async fn a_failure_after_the_first_message_returns_a_partial_receipt() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/api/chat.postMessage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok(json!({"ts": "1.1"}))))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount(
        &server,
        "chat.postMessage",
        json!({"ok": false, "error": "internal_error"}),
    )
    .await;
    let channel = channel_for(&server, &[]);
    let outcome = channel
        .send_v2_with_receipt(&message(vec![text("one"), text("two")]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["1.1"]);
    let error = outcome.error.unwrap();
    assert!(error.sent_before_error && !error.retryable);
    assert!(error.message.contains("retry is unsafe"));
}

#[tokio::test]
async fn a_failing_later_chunk_of_one_text_is_not_retryable() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/api/chat.postMessage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok(json!({"ts": "1.1"}))))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount(
        &server,
        "chat.postMessage",
        json!({"ok": false, "error": "ratelimited"}),
    )
    .await;
    let long = "word ".repeat(1000);
    let error = channel_for(&server, &[])
        .send_v2_with_receipt(&message(vec![text(&long)]))
        .await
        .unwrap_err();
    assert!(error.sent_before_error && !error.retryable);
}

fn image(opaque: Option<&str>) -> OutboundContentPart {
    content(ContentPart::Image {
        media: MediaReference {
            opaque_id: opaque.map(str::to_owned),
            kind: MediaKind::Image,
            ..Default::default()
        },
        caption: Some("see **this**".into()),
        alt_text: None,
    })
}

fn media_channel(
    server: &MockServer,
    read: Option<crate::host::StoredMedia>,
) -> (SlackChannel, Arc<StubMedia>) {
    let media = Arc::new(StubMedia::default());
    *media.read.lock().unwrap() = read;
    (
        channel_for(server, &[]).with_media_repository(media.clone()),
        media,
    )
}

#[tokio::test]
async fn files_upload_through_the_external_flow_with_their_caption() {
    let server = mock_api().await;
    upload_flow(&server).await;
    let (channel, media) = media_channel(
        &server,
        Some(stored_media(MediaKind::Image, "image/png", b"png")),
    );
    let mut request = message(vec![text("lead in"), image(Some("m1"))]);
    request.thread_id = Some("1.0".into());
    mount(&server, "chat.postMessage", ok(json!({"ts": "2.2"}))).await;
    let outcome = channel.send_v2_with_receipt(&request).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["2.2", "F1"]);
    assert_eq!(outcome.receipt.parts[1].kind, MessageReceiptPartKind::Media);
    assert!(
        outcome.receipt.parts[1].edit_token.is_none(),
        "a bare file id cannot be edited"
    );
    assert_eq!(outcome.receipt.parts[1].delete_token.as_deref(), Some("F1"));
    let complete = &bodies(&server, "files.completeUploadExternal").await[0];
    assert_eq!(complete["initial_comment"], "see *this*");
    assert_eq!(complete["thread_ts"], "1.0");
    let scope = media.scopes.lock().unwrap()[0].clone();
    assert_eq!(
        (scope.turn_id.as_str(), scope.configured_instance.as_str()),
        ("turn-1", "acct")
    );
}

#[tokio::test]
async fn every_media_kind_maps_to_an_upload_and_the_file_name_prefers_the_stored_one() {
    let server = mock_api().await;
    upload_flow(&server).await;
    let kinds = [
        (
            ContentPart::Video {
                media: MediaReference {
                    opaque_id: Some("m".into()),
                    kind: MediaKind::Video,
                    ..Default::default()
                },
                caption: None,
                duration_ms: None,
                thumbnail: None,
            },
            MediaKind::Video,
            "video/mp4",
        ),
        (
            ContentPart::Audio {
                media: MediaReference {
                    opaque_id: Some("m".into()),
                    kind: MediaKind::Audio,
                    ..Default::default()
                },
                codec: None,
                duration_ms: None,
                title: Some("Song".into()),
                transcript: None,
            },
            MediaKind::Audio,
            "audio/mpeg",
        ),
        (
            ContentPart::File {
                media: MediaReference {
                    opaque_id: Some("m".into()),
                    kind: MediaKind::Document,
                    ..Default::default()
                },
                file_name: Some("a.pdf".into()),
                caption: None,
            },
            MediaKind::Document,
            "application/pdf",
        ),
    ];
    for (part, kind, mime) in kinds {
        let (channel, _) = media_channel(&server, Some(stored_media(kind, mime, b"x")));
        channel
            .send_v2_with_receipt(&message(vec![content(part)]))
            .await
            .unwrap();
    }
    let started = bodies(&server, "files.getUploadURLExternal").await;
    assert!(started.iter().all(|b| b["filename"] == "report.pdf"));
    let mut anonymous = stored_media(MediaKind::Document, "application/pdf", b"x");
    anonymous.file_name = None;
    let (channel, _) = media_channel(&server, Some(anonymous));
    let part = ContentPart::File {
        media: MediaReference {
            opaque_id: Some("m".into()),
            kind: MediaKind::Document,
            ..Default::default()
        },
        file_name: Some("fallback.pdf".into()),
        caption: None,
    };
    channel
        .send_v2_with_receipt(&message(vec![content(part)]))
        .await
        .unwrap();
    assert_eq!(
        bodies(&server, "files.getUploadURLExternal")
            .await
            .last()
            .unwrap()["filename"],
        "fallback.pdf"
    );
}

#[tokio::test]
async fn unusable_media_is_rejected_before_upload() {
    let server = mock_api().await;
    upload_flow(&server).await;
    let bare = channel_for(&server, &[]);
    assert!(
        bare.send_v2_with_receipt(&message(vec![image(Some("m"))]))
            .await
            .unwrap_err()
            .unsupported
    );

    let good = stored_media(MediaKind::Image, "image/png", b"png");
    let cases: Vec<(Option<crate::host::StoredMedia>, Option<&str>, Option<&str>)> = vec![
        (Some(good.clone()), None, Some("turn-1")),
        (Some(good.clone()), Some(" "), Some("turn-1")),
        (None, Some("m"), Some("turn-1")),
        (Some(good.clone()), Some("m"), None),
        (Some(good.clone()), Some("m"), Some(" ")),
        (
            Some(stored_media(MediaKind::Video, "video/mp4", b"x")),
            Some("m"),
            Some("turn-1"),
        ),
        (
            Some(stored_media(MediaKind::Image, "image/x-icon", b"x")),
            Some("m"),
            Some("turn-1"),
        ),
        (
            Some(stored_media(MediaKind::Image, "image/png", b"")),
            Some("m"),
            Some("turn-1"),
        ),
        (
            Some(stored_media(
                MediaKind::Image,
                "image/png",
                &vec![0u8; 20 * 1024 * 1024 + 1],
            )),
            Some("m"),
            Some("turn-1"),
        ),
    ];
    for (read, opaque, turn) in cases {
        let (channel, _) = media_channel(&server, read);
        let mut request = message(vec![image(opaque)]);
        request.turn_id = turn.map(str::to_owned);
        let error = channel.send_v2_with_receipt(&request).await.unwrap_err();
        assert!(!error.unsupported && !error.sent_before_error, "{error:?}");
    }
    assert!(
        bodies(&server, "files.getUploadURLExternal")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn a_media_failure_after_text_leaves_a_partial_receipt() {
    let server = mock_api().await;
    posting(&server).await;
    let (channel, _) = media_channel(&server, None);
    let outcome = channel
        .send_v2_with_receipt(&message(vec![text("hi"), image(Some("m"))]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["9.9"]);
    assert!(outcome.error.unwrap().sent_before_error);
}

fn voice(opaque: Option<&str>) -> OutboundContentPart {
    OutboundContentPart::Voice {
        voice: OutboundVoice::Media {
            media: MediaReference {
                opaque_id: opaque.map(str::to_owned),
                kind: MediaKind::Audio,
                ..Default::default()
            },
            codec: Some("m4a_aac".into()),
            duration_ms: Some(1000),
        },
    }
}

fn voice_channel(server: &MockServer, audio: Option<&str>) -> SlackChannel {
    let store = Arc::new(StubVoice::default());
    *store.audio_base64.lock().unwrap() = audio.map(str::to_owned);
    channel_for(server, &[]).with_voice_media_store(store)
}

#[tokio::test]
async fn voice_replies_upload_as_audio_files_with_the_transcript_as_caption() {
    use base64::Engine as _;
    let server = mock_api().await;
    upload_flow(&server).await;
    let audio = base64::engine::general_purpose::STANDARD.encode(b"m4a-bytes");
    let channel = voice_channel(&server, Some(&audio));
    let outcome = channel
        .send_v2_with_receipt(&message(vec![text("what I said"), voice(Some("v1"))]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.parts[0].kind, MessageReceiptPartKind::Voice);
    assert_eq!(outcome.receipt.platform_message_ids, ["F1"]);
    assert_eq!(
        bodies(&server, "files.getUploadURLExternal").await[0]["filename"],
        "reply.m4a"
    );
    assert_eq!(
        bodies(&server, "files.completeUploadExternal").await[0]["initial_comment"],
        "what I said"
    );
    let sent = server.received_requests().await.unwrap();
    let raw = sent.iter().find(|r| r.url.path() == "/upload/F1").unwrap();
    assert_eq!(raw.body, b"m4a-bytes");
    assert_eq!(raw.headers.get("content-type").unwrap(), "audio/mp4");
    channel
        .send_v2_with_receipt(&message(vec![voice(Some("v1"))]))
        .await
        .unwrap();
    assert!(
        bodies(&server, "files.completeUploadExternal").await[1]
            .get("initial_comment")
            .is_none()
    );
}

#[tokio::test]
async fn unusable_voice_is_rejected_or_unsupported_before_upload() {
    use base64::Engine as _;
    let server = mock_api().await;
    upload_flow(&server).await;
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let synth = || OutboundContentPart::Voice {
        voice: OutboundVoice::Synthesize {
            text: "hi".into(),
            voice: None,
        },
    };

    assert!(
        channel_for(&server, &[])
            .send_v2_with_receipt(&message(vec![voice(Some("v"))]))
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(
        voice_channel(&server, None)
            .send_v2_with_receipt(&message(vec![synth()]))
            .await
            .unwrap_err()
            .unsupported
    );
    let configured = voice_channel(&server, None).with_voice_synthesis(true);
    let error = configured
        .send_v2_with_receipt(&message(vec![synth()]))
        .await
        .unwrap_err();
    assert!(!error.unsupported && error.message.contains("shared module runtime"));

    let channel = voice_channel(&server, Some(&b64(b"ok")));
    for case in [
        message(vec![voice(Some("v")), voice(Some("v"))]),
        message(vec![voice(Some("v")), image(Some("m"))]),
        message(vec![voice(None)]),
        message(vec![voice(Some(" "))]),
    ] {
        assert!(
            !channel
                .send_v2_with_receipt(&case)
                .await
                .unwrap_err()
                .unsupported
        );
    }
    let mut no_turn = message(vec![voice(Some("v"))]);
    no_turn.turn_id = None;
    assert!(channel.send_v2_with_receipt(&no_turn).await.is_err());
    for audio in [
        None,
        Some("%%%not base64%%%".to_owned()),
        Some(String::new()),
        Some(b64(&vec![0u8; 8 * 1024 * 1024 + 1])),
    ] {
        let channel = voice_channel(&server, audio.as_deref());
        assert!(
            channel
                .send_v2_with_receipt(&message(vec![voice(Some("v"))]))
                .await
                .is_err()
        );
    }
    assert!(
        bodies(&server, "files.getUploadURLExternal")
            .await
            .is_empty()
    );
}

#[test]
fn legacy_text_messages_and_typed_failures_round_trip() {
    let typed = text_message("C1", Some("1.0"), "hello");
    assert_eq!(
        (typed.conversation_id.as_str(), typed.thread_id.as_deref()),
        ("C1", Some("1.0"))
    );
    assert!(typed.idempotency_key.starts_with("slack-legacy-"));
    let error = send_error_to_anyhow(pre_dispatch("nope"));
    let recovered = error
        .downcast_ref::<super::super::types::SlackFailure>()
        .unwrap();
    assert_eq!(recovered.0.message, "nope");
    assert_eq!(error.to_string(), "nope");
}
