use super::*;
use crate::providers::discord::support::*;
use crate::providers::discord::voice::encode_waveform;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_bus::{HostVoiceScope, MediaKind};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn button(id: &str, label: &str) -> ButtonAction {
    ButtonAction {
        id: id.into(),
        label: label.into(),
        style: None,
        disabled: false,
    }
}

// ── pure helpers ──────────────────────────────────────────────

#[test]
fn target_requires_numeric_channel_thread_and_reply_ids() {
    let mut bad = message(vec![text_part("x")]);
    bad.conversation_ref = None;
    bad.conversation_id = "../etc".into();
    assert!(target_for(&bad).unwrap_err().message.contains("channel id"));

    let mut bad = message(vec![text_part("x")]);
    bad.thread_id = Some("abc".into());
    assert!(target_for(&bad).unwrap_err().message.contains("thread"));

    let mut bad = message(vec![text_part("x")]);
    bad.reply_to_id = Some("1/2".into());
    assert!(target_for(&bad).unwrap_err().message.contains("reply"));
}

#[test]
fn target_posts_to_the_thread_and_keeps_the_conversation_for_the_receipt() {
    let mut threaded = message(vec![text_part("x")]);
    threaded.thread_id = Some(THREAD.into());
    let target = target_for(&threaded).unwrap();
    assert_eq!(target.post_channel, THREAD);
    assert_eq!(target.conversation.id, CHANNEL);
    assert_eq!(target.thread_id.as_deref(), Some(THREAD));

    // A bare conversation id works and defaults the account reference.
    let mut bare = message(vec![text_part("x")]);
    bare.conversation_ref = None;
    bare.channel_ref = None;
    let target = target_for(&bare).unwrap();
    assert_eq!(target.post_channel, CHANNEL);
    assert_eq!(target.channel.id, "discord");
    assert_eq!(target.channel.account_id, None);

    // A topic id addresses a thread when no thread id is set.
    let mut topic = message(vec![text_part("x")]);
    topic.conversation_ref.as_mut().unwrap().topic_id = Some(THREAD.into());
    assert_eq!(target_for(&topic).unwrap().post_channel, THREAD);
}

#[test]
fn file_names_lose_paths_and_control_characters() {
    assert_eq!(sanitize_file_name(Some("../../etc/passwd"), "f"), "passwd");
    assert_eq!(sanitize_file_name(Some("C:\\dir\\a.txt"), "f"), "a.txt");
    assert_eq!(sanitize_file_name(Some(".."), "fallback"), "fallback");
    assert_eq!(sanitize_file_name(None, "fallback"), "fallback");
    assert_eq!(sanitize_file_name(Some("a\nb.txt"), "f"), "ab.txt");
    assert_eq!(sanitize_file_name(Some(&"x".repeat(300)), "f").len(), 100);
}

#[test]
fn button_rows_map_styles_and_enforce_discord_limits() {
    let rows = button_rows(&[vec![
        ButtonAction {
            style: Some(ButtonStyle::Primary),
            ..button("a", "Go")
        },
        ButtonAction {
            style: Some(ButtonStyle::Danger),
            disabled: true,
            ..button("b", "Stop")
        },
        button("c", "Plain"),
    ]])
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["type"], 1);
    let components = rows[0]["components"].as_array().unwrap();
    assert_eq!(components[0]["style"], 1);
    assert_eq!(components[1]["style"], 4);
    assert_eq!(components[1]["disabled"], true);
    assert_eq!(components[2]["style"], 2);
    assert_eq!(components[0]["custom_id"], "a");

    assert!(button_rows(&[]).is_err());
    assert!(button_rows(&vec![vec![button("a", "A")]; 6]).is_err());
    assert!(button_rows(&[vec![]]).is_err());
    assert!(button_rows(&[(0..6).map(|i| button(&i.to_string(), "x")).collect()]).is_err());
    assert!(button_rows(&[vec![button("", "x")]]).is_err());
    assert!(button_rows(&[vec![button(&"i".repeat(101), "x")]]).is_err());
    assert!(button_rows(&[vec![button("a", " ")]]).is_err());
    assert!(button_rows(&[vec![button("a", &"l".repeat(81))]]).is_err());
}

#[test]
fn approval_presentation_becomes_an_embed_with_choice_buttons() {
    let (embed, rows) = presentation_embed(&json!({
        "kind": "approval", "id": "req-1", "prompt": "Run it?", "choices": ["Yes", "No"]
    }))
    .unwrap();
    assert_eq!(embed["description"], "Run it?");
    let buttons = rows[0]["components"].as_array().unwrap();
    assert_eq!(buttons[0]["custom_id"], "approval-v1:req-1:0");
    assert_eq!(buttons[1]["label"], "No");

    // Eleven choices span three rows of five.
    let many: Vec<String> = (0..11).map(|i| format!("c{i}")).collect();
    let (_, rows) = presentation_embed(&json!({
        "kind": "approval", "id": "r", "prompt": "p", "choices": many
    }))
    .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn approval_presentation_rejects_malformed_requests() {
    let base = json!({"kind": "approval", "id": "r", "prompt": "p", "choices": ["a"]});
    let mut extra = base.clone();
    extra["extra"] = json!(1);
    assert!(presentation_embed(&extra).is_err());
    for (key, value) in [
        ("id", json!("")),
        ("prompt", json!(" ")),
        ("choices", json!([])),
        ("choices", json!([1])),
        ("choices", json!(vec!["x"; 26])),
    ] {
        let mut case = base.clone();
        case[key] = value;
        assert!(presentation_embed(&case).is_err(), "{key}");
    }
    let long_id = json!({"kind":"approval","id":"i".repeat(80),"prompt":"p","choices":["a"]});
    assert!(
        presentation_embed(&long_id)
            .unwrap_err()
            .message
            .contains("too long")
    );
}

#[test]
fn text_presentation_blocks_join_into_one_embed() {
    let (embed, rows) = presentation_embed(&json!([
        {"type": "heading", "text": "Title"},
        "plain line",
        [{"description": "nested"}],
        {"title": " "}
    ]))
    .unwrap();
    assert_eq!(embed["description"], "Title\nplain line\nnested");
    assert!(rows.is_empty());

    assert!(presentation_embed(&json!([])).is_err());
    assert!(presentation_embed(&json!({"image": "x"})).is_err());
    assert!(presentation_embed(&json!({"text": 5})).is_err());
    assert!(presentation_embed(&json!(7)).is_err());
}

#[test]
fn thread_specs_validate_every_field() {
    let spec = thread_spec(&json!({
        "action": "create_thread", "name": " Topic ", "messageId": "42",
        "autoArchiveDuration": 60, "content": "hello"
    }))
    .unwrap();
    assert_eq!(spec.name, "Topic");
    assert_eq!(spec.message_id.as_deref(), Some("42"));
    assert_eq!(spec.auto_archive_minutes, 60);
    assert_eq!(spec.content.as_deref(), Some("hello"));

    let minimal = thread_spec(&json!({"action": "create_thread", "name": "t"})).unwrap();
    assert_eq!(minimal.auto_archive_minutes, 1440);
    assert!(minimal.message_id.is_none() && minimal.content.is_none());

    let wrong_action = thread_spec(&json!({"action": "x"})).unwrap_err();
    assert!(wrong_action.unsupported);
    for bad in [
        json!({"action": "create_thread"}),
        json!({"action": "create_thread", "name": ""}),
        json!({"action": "create_thread", "name": "n".repeat(101)}),
        json!({"action": "create_thread", "name": "n", "messageId": "x"}),
        json!({"action": "create_thread", "name": "n", "autoArchiveDuration": 5}),
        json!({"action": "create_thread", "name": "n", "content": "c".repeat(2001)}),
    ] {
        assert!(thread_spec(&bad).is_err(), "{bad}");
    }
}

// ── text sends ────────────────────────────────────────────────

#[tokio::test]
async fn text_send_posts_json_with_a_nonce_and_returns_the_receipt() {
    let server = MockServer::start().await;
    mount_message_post(&server, 1000).await;
    let channel = channel_for(&server);
    let mut msg = message(vec![text_part("hello"), text_part("world")]);
    msg.reply_to_id = Some("42".into());

    let outcome = channel.send_v2_with_receipt(&msg).await.unwrap();

    assert!(outcome.error.is_none());
    let receipt = outcome.receipt;
    assert_eq!(receipt.platform_message_ids, vec!["1000"]);
    assert_eq!(receipt.primary_platform_message_id.as_deref(), Some("1000"));
    assert_eq!(receipt.channel, Some(account()));
    assert_eq!(receipt.conversation, Some(conversation()));
    assert_eq!(receipt.reply_to_id.as_deref(), Some("42"));
    assert_eq!(receipt.edit_token.as_deref(), Some("1000"));
    assert_eq!(
        receipt.parts[0].raw.as_ref().unwrap().channel_id.as_deref(),
        Some(CHANNEL)
    );

    let sent = requests(&server, "POST").await;
    assert_eq!(sent.len(), 1);
    let body = body_json(&sent[0]);
    assert_eq!(body["content"], "hello\nworld");
    assert_eq!(
        body["message_reference"],
        json!({"message_id": "42", "fail_if_not_exists": false})
    );
    assert_eq!(body["allowed_mentions"], json!({"parse": ["users"]}));
    assert_eq!(body["enforce_nonce"], true);
    assert_eq!(body["nonce"].as_str().unwrap().len(), 24);
    assert_eq!(
        sent[0]
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        format!("Bot {BOT_TOKEN}")
    );
}

#[tokio::test]
async fn a_send_without_an_idempotency_key_carries_no_nonce() {
    let server = MockServer::start().await;
    mount_message_post(&server, 1).await;
    let mut msg = message(vec![text_part("hi")]);
    msg.idempotency_key.clear();

    channel_for(&server)
        .send_v2_with_receipt(&msg)
        .await
        .unwrap();

    let body = body_json(&requests(&server, "POST").await[0]);
    assert!(body.get("nonce").is_none() && body.get("enforce_nonce").is_none());
}

#[tokio::test]
async fn long_text_is_split_and_only_the_first_message_replies() {
    let server = MockServer::start().await;
    mount_message_post(&server, 10).await;
    let long = "word ".repeat(700);
    let mut msg = message(vec![text_part(&long)]);
    msg.reply_to_id = Some("42".into());

    let outcome = channel_for(&server)
        .send_v2_with_receipt(&msg)
        .await
        .unwrap();

    let sent = requests(&server, "POST").await;
    assert_eq!(sent.len(), 2);
    assert!(body_json(&sent[0]).get("message_reference").is_some());
    assert!(body_json(&sent[1]).get("message_reference").is_none());
    let nonces: Vec<_> = sent.iter().map(|r| body_json(r)["nonce"].clone()).collect();
    assert_ne!(nonces[0], nonces[1]);
    assert_eq!(outcome.receipt.platform_message_ids, vec!["10", "11"]);
    assert_eq!(outcome.receipt.edit_token, None);
    for request in &sent {
        let content = body_json(request)["content"].as_str().unwrap().to_owned();
        assert!(content.encode_utf16().count() <= 2000);
    }
}

#[tokio::test]
async fn thread_addressing_posts_into_the_thread_channel() {
    let server = MockServer::start().await;
    mount_message_post(&server, 5).await;
    let mut msg = message(vec![text_part("in thread")]);
    msg.conversation_ref = Some(thread_conversation());

    let outcome = channel_for(&server)
        .send_v2_with_receipt(&msg)
        .await
        .unwrap();

    let sent = requests(&server, "POST").await;
    assert_eq!(sent[0].url.path(), format!("/channels/{THREAD}/messages"));
    assert_eq!(outcome.receipt.thread_id.as_deref(), Some(THREAD));
    assert_eq!(outcome.receipt.parts[0].thread_id.as_deref(), Some(THREAD));
    assert_eq!(
        outcome.receipt.parts[0]
            .raw
            .as_ref()
            .unwrap()
            .channel_id
            .as_deref(),
        Some(THREAD)
    );
}

// ── attachments ───────────────────────────────────────────────

#[tokio::test]
async fn an_image_uploads_as_multipart_with_its_caption() {
    let server = MockServer::start().await;
    mount_message_post(&server, 20).await;
    let (channel, _, media, _) = full_channel(&server);
    let reference = media.seed(
        &media_scope(),
        MediaKind::Image,
        "image/png",
        "pic.png",
        png_bytes(),
    );
    let msg = message(vec![content(ContentPart::Image {
        media: reference,
        caption: Some("look".into()),
        alt_text: None,
    })]);

    let outcome = channel.send_v2_with_receipt(&msg).await.unwrap();

    let sent = requests(&server, "POST").await;
    let payload = multipart_payload(&sent[0]);
    assert_eq!(payload["content"], "look");
    assert_eq!(payload["attachments"][0]["filename"], "pic.png");
    let raw = String::from_utf8_lossy(&sent[0].body);
    assert!(raw.contains("name=\"files[0]\"; filename=\"pic.png\""));
    assert!(raw.contains("Content-Type: image/png"));
    assert_eq!(
        outcome.receipt.parts[0].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Media
    );
}

#[tokio::test]
async fn video_audio_and_file_attachments_share_one_message_with_overrides() {
    let server = MockServer::start().await;
    mount_message_post(&server, 30).await;
    let (channel, _, media, _) = full_channel(&server);
    let scope = media_scope();
    let video = media.seed(&scope, MediaKind::Video, "video/mp4", "v.mp4", vec![1; 8]);
    let audio = media.seed(&scope, MediaKind::Audio, "audio/mpeg", "a.mp3", vec![2; 8]);
    let file = media.seed(
        &scope,
        MediaKind::Document,
        "text/plain",
        "f.txt",
        b"hi".to_vec(),
    );
    let msg = message(vec![
        text_part("bundle"),
        content(ContentPart::Video {
            media: video,
            caption: Some("vc".into()),
            duration_ms: None,
            thumbnail: None,
        }),
        content(ContentPart::Audio {
            media: audio,
            codec: None,
            duration_ms: None,
            title: Some("song".into()),
            transcript: Some("la la".into()),
        }),
        content(ContentPart::File {
            media: file,
            file_name: Some("renamed.txt".into()),
            caption: None,
        }),
    ]);

    channel.send_v2_with_receipt(&msg).await.unwrap();

    let sent = requests(&server, "POST").await;
    assert_eq!(sent.len(), 1);
    let payload = multipart_payload(&sent[0]);
    assert_eq!(payload["content"], "bundle\nvc\nsong\nla la");
    let names: Vec<_> = payload["attachments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["filename"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, vec!["v.mp4", "a.mp3", "renamed.txt"]);
}

#[tokio::test]
async fn attachments_need_host_storage_and_a_scoped_reference() {
    let server = MockServer::start().await;
    let image = |media: MediaReference| {
        message(vec![content(ContentPart::Image {
            media,
            caption: None,
            alt_text: None,
        })])
    };

    // No repository: an explicit unsupported result, nothing sent.
    let error = channel_for(&server)
        .send_v2_with_receipt(&image(MediaReference::default()))
        .await
        .unwrap_err();
    assert!(error.unsupported && !error.sent_before_error);

    let (channel, _, media, _) = full_channel(&server);
    // URL-only references are never fetched on the adapter's behalf.
    let url_only = MediaReference {
        url: Some("https://x/y.png".into()),
        ..Default::default()
    };
    assert!(
        channel
            .send_v2_with_receipt(&image(url_only))
            .await
            .is_err()
    );

    // The reference must belong to the conversation it is sent to.
    let other = HostVoiceScope {
        conversation: ConversationRef {
            id: "1".into(),
            ..conversation()
        },
        ..media_scope()
    };
    let foreign = media.seed(&other, MediaKind::Image, "image/png", "p.png", png_bytes());
    assert!(
        channel
            .send_v2_with_receipt(&image(foreign))
            .await
            .unwrap_err()
            .message
            .contains("could not be resolved")
    );

    // A turn id is required for scoped reads.
    let own = media.seed(
        &media_scope(),
        MediaKind::Image,
        "image/png",
        "p.png",
        png_bytes(),
    );
    let mut no_turn = image(own);
    no_turn.turn_id = None;
    assert!(
        channel
            .send_v2_with_receipt(&no_turn)
            .await
            .unwrap_err()
            .message
            .contains("turn")
    );
    assert!(requests(&server, "POST").await.is_empty());
}

#[tokio::test]
async fn too_many_files_or_bytes_are_rejected_before_any_request() {
    let server = MockServer::start().await;
    let (channel, _, media, _) = full_channel(&server);
    let scope = media_scope();
    let many: Vec<_> = (0..11)
        .map(|_| {
            let r = media.seed(
                &scope,
                MediaKind::Document,
                "text/plain",
                "f.txt",
                b"x".to_vec(),
            );
            content(ContentPart::File {
                media: r,
                file_name: None,
                caption: None,
            })
        })
        .collect();
    assert!(
        channel
            .send_v2_with_receipt(&message(many))
            .await
            .unwrap_err()
            .message
            .contains("10 files")
    );

    let big = |n| {
        media.seed(
            &scope,
            MediaKind::Document,
            "text/plain",
            "b.txt",
            vec![0; n],
        )
    };
    let two_big = vec![
        content(ContentPart::File {
            media: big(6 * 1024 * 1024),
            file_name: None,
            caption: None,
        }),
        content(ContentPart::File {
            media: big(6 * 1024 * 1024),
            file_name: None,
            caption: None,
        }),
    ];
    assert!(
        channel
            .send_v2_with_receipt(&message(two_big))
            .await
            .unwrap_err()
            .message
            .contains("upload limit")
    );
    assert!(requests(&server, "POST").await.is_empty());
}

// ── buttons, embeds, threads ──────────────────────────────────

#[tokio::test]
async fn buttons_and_presentations_ride_with_the_final_message() {
    let server = MockServer::start().await;
    mount_message_post(&server, 40).await;
    let msg = message(vec![
        text_part("choose"),
        content(ContentPart::Buttons {
            rows: vec![vec![button("go", "Go")]],
        }),
        content(ContentPart::Presentation {
            blocks: json!({"kind":"approval","id":"r1","prompt":"ok?","choices":["Yes"]}),
        }),
    ]);

    let outcome = channel_for(&server)
        .send_v2_with_receipt(&msg)
        .await
        .unwrap();

    let body = body_json(&requests(&server, "POST").await[0]);
    assert_eq!(body["content"], "choose");
    assert_eq!(body["embeds"][0]["description"], "ok?");
    assert_eq!(body["components"].as_array().unwrap().len(), 2);
    assert_eq!(
        outcome.receipt.parts[0].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Card
    );
}

#[tokio::test]
async fn more_than_five_button_rows_are_rejected() {
    let server = MockServer::start().await;
    let rows = || {
        content(ContentPart::Buttons {
            rows: vec![vec![button("a", "A")]; 3],
        })
    };
    let error = channel_for(&server)
        .send_v2_with_receipt(&message(vec![rows(), rows()]))
        .await
        .unwrap_err();
    assert!(error.message.contains("5 button rows"));

    let approval = |n: usize| {
        content(ContentPart::Presentation {
            blocks: json!({"kind":"approval","id":"r","prompt":"p","choices":vec!["c"; n]}),
        })
    };
    let error = channel_for(&server)
        .send_v2_with_receipt(&message(vec![approval(20), approval(10)]))
        .await
        .unwrap_err();
    assert!(error.message.contains("5 button rows"));
    assert!(requests(&server, "POST").await.is_empty());
}

#[tokio::test]
async fn native_create_thread_from_a_message_opens_it_and_posts_the_first_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/channels/{CHANNEL}/messages/42/threads")))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": THREAD})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/channels/{THREAD}/messages")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "9001"})))
        .mount(&server)
        .await;
    let msg = message(vec![content(ContentPart::Native {
        provider: "discord:acct".into(),
        data: json!({"action":"create_thread","name":"Plan","messageId":"42","autoArchiveDuration":60,"content":"kickoff"}),
    })]);

    let outcome = channel_for(&server)
        .send_v2_with_receipt(&msg)
        .await
        .unwrap();

    let sent = requests(&server, "POST").await;
    assert_eq!(
        body_json(&sent[0]),
        json!({"name":"Plan","auto_archive_duration":60})
    );
    assert_eq!(body_json(&sent[1])["content"], "kickoff");
    assert_eq!(outcome.receipt.thread_id.as_deref(), Some(THREAD));
    assert_eq!(outcome.receipt.platform_message_ids, vec![THREAD, "9001"]);
    assert_eq!(outcome.receipt.parts[1].thread_id.as_deref(), Some(THREAD));
}

#[tokio::test]
async fn native_create_thread_without_a_message_makes_a_public_thread() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/channels/{CHANNEL}/threads")))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": THREAD})))
        .mount(&server)
        .await;
    let msg = message(vec![content(ContentPart::Native {
        provider: "discord".into(),
        data: json!({"action":"create_thread","name":"Solo"}),
    })]);

    let outcome = channel_for(&server)
        .send_v2_with_receipt(&msg)
        .await
        .unwrap();

    let body = body_json(&requests(&server, "POST").await[0]);
    assert_eq!(body["type"], 11);
    assert_eq!(body["auto_archive_duration"], 1440);
    assert_eq!(outcome.receipt.platform_message_ids, vec![THREAD]);
}

#[tokio::test]
async fn native_data_must_be_discord_and_stand_alone() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let native = |provider: &str| {
        content(ContentPart::Native {
            provider: provider.into(),
            data: json!({"action":"create_thread","name":"n"}),
        })
    };
    let other = channel
        .send_v2_with_receipt(&message(vec![native("slack")]))
        .await
        .unwrap_err();
    assert!(other.unsupported);
    let mixed = channel
        .send_v2_with_receipt(&message(vec![text_part("x"), native("discord")]))
        .await
        .unwrap_err();
    assert!(mixed.message.contains("only part"));
    assert!(requests(&server, "POST").await.is_empty());
}

// ── voice ─────────────────────────────────────────────────────

#[tokio::test]
async fn a_voice_message_uploads_with_the_voice_flag_duration_and_waveform() {
    let server = MockServer::start().await;
    mount_message_post(&server, 50).await;
    let (channel, _, _, voice) = full_channel(&server);
    let media = voice.seed(OGG_FIXTURE, "audio/ogg");
    let mut msg = message(vec![
        text_part("transcript"),
        OutboundContentPart::Voice {
            voice: OutboundVoice::Media {
                media,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
            },
        },
    ]);
    msg.reply_to_id = Some("42".into());

    let outcome = channel.send_v2_with_receipt(&msg).await.unwrap();

    let sent = requests(&server, "POST").await;
    assert_eq!(sent.len(), 2, "text first, then the voice message alone");
    assert_eq!(body_json(&sent[0])["content"], "transcript");
    let payload = multipart_payload(&sent[1]);
    assert_eq!(payload["flags"], 8192);
    assert!(payload.get("content").is_none());
    assert_eq!(payload["attachments"][0]["filename"], "voice-message.ogg");
    // The codec measured 2.5 s and a four-sample waveform.
    assert_eq!(payload["attachments"][0]["duration_secs"], 2.5);
    assert_eq!(
        payload["attachments"][0]["waveform"],
        encode_waveform(&[0.0, 0.25, 0.5, 1.0])
    );
    assert!(
        payload.get("message_reference").is_none(),
        "reply belongs to the first message"
    );
    let raw = String::from_utf8_lossy(&sent[1].body);
    assert!(raw.contains("Content-Type: audio/ogg"));
    assert_eq!(
        outcome.receipt.parts[1].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Voice
    );
}

#[tokio::test]
async fn synthesis_requests_must_be_prepared_by_the_runtime() {
    let server = MockServer::start().await;
    let (channel, ..) = full_channel(&server);
    let msg = message(vec![OutboundContentPart::Voice {
        voice: OutboundVoice::Synthesize {
            text: "hi".into(),
            voice: None,
        },
    }]);
    let error = channel.send_v2_with_receipt(&msg).await.unwrap_err();
    assert!(error.message.contains("shared module runtime"));
}

// ── unsupported and invalid ───────────────────────────────────

#[tokio::test]
async fn unsupported_content_reports_unsupported_without_touching_the_network() {
    let server = MockServer::start().await;
    let (channel, ..) = full_channel(&server);
    let parts = [
        ContentPart::Sticker {
            media: MediaReference::default(),
            pack_id: None,
            sticker_id: None,
            emoji: None,
            alt_text: None,
        },
        ContentPart::Location {
            latitude: 1.0,
            longitude: 1.0,
            label: None,
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: None,
        },
        ContentPart::Contact {
            display_name: "a".into(),
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
    ];
    for part in parts {
        let error = channel
            .send_v2_with_receipt(&message(vec![content(part)]))
            .await
            .unwrap_err();
        assert!(error.unsupported && !error.retryable && !error.sent_before_error);
    }
    let note = ContentPart::VoiceNote {
        voice_note: tinychannels_bus::VoiceNote {
            media: MediaReference::default(),
            codec: None,
            duration_ms: None,
            waveform: None,
            push_to_talk: false,
            transcript: None,
        },
    };
    assert!(
        channel
            .send_v2_with_receipt(&message(vec![content(note)]))
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(requests(&server, "POST").await.is_empty());
}

#[tokio::test]
async fn empty_sends_and_bad_addressing_are_rejected_before_dispatch() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    assert!(
        channel
            .send_v2_with_receipt(&message(vec![]))
            .await
            .unwrap_err()
            .message
            .contains("no content")
    );
    assert!(
        channel
            .send_v2_with_receipt(&message(vec![text_part("")]))
            .await
            .is_err()
    );
    let mut bad = message(vec![text_part("x")]);
    bad.conversation_ref.as_mut().unwrap().id = "nope".into();
    assert!(channel.send_v2_with_receipt(&bad).await.is_err());
}

// ── failures ──────────────────────────────────────────────────

#[tokio::test]
async fn a_rate_limited_first_message_is_a_retryable_error_with_no_receipt() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_json(json!({"retry_after": 1.5, "message": "slow"})),
        )
        .mount(&server)
        .await;

    let error = channel_for(&server)
        .send_v2_with_receipt(&message(vec![text_part("x")]))
        .await
        .unwrap_err();

    assert_eq!(error.kind, SendErrorKind::RateLimited);
    assert!(error.retryable && !error.sent_before_error);
    assert_eq!(error.retry_after, Some(1.5));
}

#[tokio::test]
async fn a_failure_after_the_first_chunk_returns_the_partial_receipt() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"^/channels/\d+/messages$"))
        .respond_with(move |_: &Request| {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200).set_body_json(json!({"id": "700"}))
            } else {
                ResponseTemplate::new(500).set_body_string("boom")
            }
        })
        .mount(&server)
        .await;
    let long = "word ".repeat(700);

    let outcome = channel_for(&server)
        .send_v2_with_receipt(&message(vec![text_part(&long)]))
        .await
        .unwrap();

    let error = outcome.error.expect("partial failure is reported");
    assert!(error.sent_before_error && !error.retryable);
    assert_eq!(error.continuation_message_ids, vec!["700"]);
    assert_eq!(outcome.receipt.platform_message_ids, vec!["700"]);
}

#[tokio::test]
async fn a_response_without_a_message_id_is_an_uncertain_outcome() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;

    let error = channel_for(&server)
        .send_v2_with_receipt(&message(vec![text_part("x")]))
        .await
        .unwrap_err();

    assert!(error.sent_before_error && !error.retryable);
    assert!(error.message.contains("omitted"));
}
