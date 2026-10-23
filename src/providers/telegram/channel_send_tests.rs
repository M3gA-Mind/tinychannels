use super::*;
use crate::providers::telegram::attachments::{TelegramAttachment, TelegramAttachmentKind};
use crate::providers::telegram::fixtures::*;
use crate::providers::telegram::text::TELEGRAM_MAX_MESSAGE_LENGTH;
use crate::traits::SendMessage;
use std::sync::Arc;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, ResponseTemplate};

fn attachment(kind: TelegramAttachmentKind, target: &str) -> TelegramAttachment {
    TelegramAttachment {
        kind,
        target: target.to_string(),
    }
}

const ALL_KINDS: [(TelegramAttachmentKind, &str, &str); 5] = [
    (TelegramAttachmentKind::Image, "sendPhoto", "photo"),
    (TelegramAttachmentKind::Document, "sendDocument", "document"),
    (TelegramAttachmentKind::Video, "sendVideo", "video"),
    (TelegramAttachmentKind::Audio, "sendAudio", "audio"),
    (TelegramAttachmentKind::Voice, "sendVoice", "voice"),
];

#[tokio::test]
async fn send_message_reaction_skips_blank_emoji_without_network() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["*"]);
    assert!(!ch.send_message_reaction("1", 2, "  ").await.unwrap());
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "no request for a blank emoji"
    );
}

#[tokio::test]
async fn send_message_reaction_success_publishes_reaction_sent_event() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    let events = Arc::new(RecordingEvents::default());
    let ch = channel_for(&server, &["*"]).with_events(events.clone());

    assert!(ch.send_message_reaction("55", 7, " 👍 ").await.unwrap());

    let bodies = json_bodies(&server, "setMessageReaction").await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["chat_id"], "55");
    assert_eq!(bodies[0]["message_id"], 7);
    assert_eq!(bodies[0]["reaction"][0]["emoji"], "👍");
    let recorded = events.events.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0, "channel");
    assert_eq!(recorded[0].1, "outbound_reaction");
    assert_eq!(recorded[0].2["type"], "outbound_reaction");
    assert_eq!(recorded[0].2["messageId"], "telegram_55_7");
    assert_eq!(recorded[0].2["providerMessageId"], "7");
    assert_eq!(recorded[0].2["conversation"]["id"], "55");
    assert_eq!(recorded[0].2["reaction"]["value"], "👍");
    assert_eq!(recorded[0].2["success"], true);
}

#[tokio::test]
async fn send_message_reaction_rejection_is_not_an_error_and_reports_failure() {
    let server = mock_api().await;
    mount_post(
        &server,
        "setMessageReaction",
        ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "ok": false,
            "description": "REACTION_INVALID"
        })),
    )
    .await;
    let events = Arc::new(RecordingEvents::default());
    let ch = channel_for(&server, &["*"]).with_events(events.clone());

    assert!(!ch.send_message_reaction("55", 7, "🦄").await.unwrap());
    let recorded = events.events.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].1, "outbound_reaction");
    assert_eq!(recorded[0].2["success"], false);
}

#[tokio::test]
async fn send_message_reaction_without_event_sink_still_succeeds() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    let ch = channel_for(&server, &["*"]);
    assert!(ch.send_message_reaction("1", 2, "🔥").await.unwrap());
}

#[tokio::test]
async fn send_message_reaction_propagates_transport_error() {
    let mut ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    ch.api_base = dead_base();
    assert!(ch.send_message_reaction("1", 2, "🔥").await.is_err());
}

#[tokio::test]
async fn send_text_chunks_single_chunk_carries_thread_and_reply_ids() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send_text_chunks("hello *world*", "100", Some("9"), Some(42))
        .await
        .unwrap();

    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(
        bodies,
        vec![serde_json::json!({
            "chat_id": "100",
            "text": "hello *world*",
            "parse_mode": "MarkdownV2",
            "message_thread_id": "9",
            "reply_to_message_id": 42,
        })]
    );
}

#[tokio::test]
async fn send_text_chunks_does_not_retry_unknown_or_non_format_rejections() {
    for status in [429, 500] {
        let server = mock_api().await;
        mount_post(
            &server,
            "sendMessage",
            ResponseTemplate::new(status).set_body_json(serde_json::json!({
                "ok": false,
                "description": "not a markdown formatting rejection"
            })),
        )
        .await;
        let ch = channel_for(&server, &["*"]);
        let error = ch
            .send_text_chunks_with_ids("hello", "1", None, None)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<PartialTextSendError>().is_some());
        assert_eq!(
            json_bodies(&server, "sendMessage").await.len(),
            1,
            "status {status}"
        );
        let error = error.downcast_ref::<PartialTextSendError>().unwrap();
        assert_eq!(error.sent_before_error, status >= 500, "status {status}");
    }
}

#[tokio::test]
async fn send_text_chunks_retries_only_explicit_markdown_parse_rejection() {
    let server = mock_api().await;
    let markdown_failure = ResponseTemplate::new(400).set_body_json(serde_json::json!({
        "ok": false,
        "description": "Bad Request: can't parse entities: can't find end of the entity"
    }));
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .and(body_partial_json(
            serde_json::json!({"parse_mode":"MarkdownV2"}),
        ))
        .respond_with(markdown_failure)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .and(body_partial_json(
            serde_json::json!({"chat_id":"1","text":"hello"}),
        ))
        .respond_with(ok_with(serde_json::json!({"message_id": 9})))
        .with_priority(2)
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);

    assert_eq!(
        ch.send_text_chunks_with_ids("hello", "1", None, None)
            .await
            .unwrap(),
        ["9"]
    );
    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0]["parse_mode"], "MarkdownV2");
    assert!(bodies[1].get("parse_mode").is_none());
}

#[tokio::test]
async fn send_text_chunks_marks_continuations_and_replies_only_on_first_chunk() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);
    let message = "word ".repeat(TELEGRAM_MAX_MESSAGE_LENGTH * 2 / 5 + 50);

    ch.send_text_chunks(&message, "100", None, Some(42))
        .await
        .unwrap();

    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(bodies.len(), 3, "message splits into three chunks");
    let texts: Vec<&str> = bodies.iter().map(|b| b["text"].as_str().unwrap()).collect();
    assert!(
        texts
            .iter()
            .all(|text| text.len() <= TELEGRAM_MAX_MESSAGE_LENGTH)
    );
    assert!(
        texts
            .iter()
            .all(|text| { !text.starts_with("(continued)") && !text.ends_with("(continues...)") })
    );
    assert_eq!(texts.concat(), message);
    assert_eq!(bodies[0]["reply_to_message_id"], 42);
    assert!(bodies[1].get("reply_to_message_id").is_none());
    assert!(bodies[2].get("reply_to_message_id").is_none());
}

#[tokio::test]
async fn send_text_chunks_retries_without_markdown_when_markdown_rejected() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .and(body_partial_json(
            serde_json::json!({ "parse_mode": "MarkdownV2" }),
        ))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "ok": false,
            "description": "Bad Request: can't parse entities: can't find end of the entity"
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);
    let message = "x".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 10);

    ch.send_text_chunks(&message, "100", Some("3"), Some(5))
        .await
        .unwrap();

    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(bodies.len(), 4, "two chunks, each sent markdown then plain");
    let plain: Vec<_> = bodies
        .iter()
        .filter(|b| b.get("parse_mode").is_none())
        .collect();
    assert_eq!(plain.len(), 2);
    assert_eq!(plain[0]["message_thread_id"], "3");
    assert_eq!(plain[0]["reply_to_message_id"], 5);
    assert!(plain[1].get("reply_to_message_id").is_none());
}

#[tokio::test]
async fn channel_send_short_message_retries_plain_preserving_thread_and_reply() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .and(body_partial_json(
            serde_json::json!({ "parse_mode": "MarkdownV2" }),
        ))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "ok": false,
            "description": "Bad Request: can't parse entities"
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send_compat_message(
        &SendMessage::new("**markdown fallback**", "100:3").in_thread(Some("5".into())),
    )
    .await
    .unwrap();

    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(
        bodies,
        vec![
            serde_json::json!({
                "chat_id": "100",
                "text": "**markdown fallback**",
                "parse_mode": "MarkdownV2",
                "message_thread_id": "3",
                "reply_to_message_id": 5,
            }),
            serde_json::json!({
                "chat_id": "100",
                "text": "**markdown fallback**",
                "message_thread_id": "3",
                "reply_to_message_id": 5,
            }),
        ],
        "short messages retry once without Markdown while preserving reply routing"
    );
    assert!(bodies[1].get("parse_mode").is_none());
}

#[tokio::test]
async fn media_upload_includes_routing_caption_title_and_rounded_duration() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendAudio",
        ok_with(serde_json::json!({"message_id": 84})),
    )
    .await;
    let ch = channel_for(&server, &["*"]);

    let id = ch
        .send_media_bytes_with_id(TelegramMediaUpload {
            method: "sendAudio",
            field: "audio",
            chat_id: "chat-1",
            thread_id: Some("topic-2"),
            reply_to_id: Some("17"),
            caption: Some("A recording"),
            title: Some("Meeting"),
            duration_ms: Some(1_001),
            bytes: vec![1, 2, 3],
            content_type: "audio/ogg",
            file_name: "voice.ogg",
        })
        .await
        .unwrap();

    assert_eq!(id, "84");
    let bodies = raw_bodies(&server, "sendAudio").await;
    assert_eq!(bodies.len(), 1);
    for field in [
        "name=\"chat_id\"\r\n\r\nchat-1",
        "name=\"message_thread_id\"\r\n\r\ntopic-2",
        "name=\"reply_parameters\"\r\n\r\n{\"message_id\":17}",
        "name=\"caption\"\r\n\r\nA recording",
        "name=\"title\"\r\n\r\nMeeting",
        "name=\"duration\"\r\n\r\n2",
        "filename=\"voice.ogg\"",
        "Content-Type: audio/ogg",
    ] {
        assert!(
            bodies[0].contains(field),
            "multipart body missing {field:?}"
        );
    }
}

#[tokio::test]
async fn media_and_voice_uploads_reject_invalid_inputs_before_network_access() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["*"]);
    let media = |bytes, reply_to_id, content_type| TelegramMediaUpload {
        method: "sendPhoto",
        field: "photo",
        chat_id: "chat-1",
        thread_id: None,
        reply_to_id,
        caption: None,
        title: None,
        duration_ms: None,
        bytes,
        content_type,
        file_name: "image.png",
    };

    assert!(
        ch.send_media_bytes_with_id(media(vec![], None, "image/png"))
            .await
            .is_err()
    );
    assert!(
        ch.send_media_bytes_with_id(media(vec![1], Some("not-an-id"), "image/png"))
            .await
            .unwrap_err()
            .to_string()
            .contains("reply target is invalid")
    );
    assert!(
        ch.send_media_bytes_with_id(media(vec![1], None, "not a mime type"))
            .await
            .is_err()
    );

    assert!(
        ch.send_voice_bytes_with_id(TelegramVoiceUpload {
            method: "sendVoice",
            field: "voice",
            chat_id: "chat-1",
            thread_id: None,
            reply_to_id: Some("bad"),
            caption: None,
            bytes: vec![1],
            file_name: "voice.ogg",
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("reply target is invalid")
    );

    let too_large = vec![0; 20 * 1024 * 1024 + 1];
    assert!(
        ch.send_media_bytes_with_id(media(too_large, None, "image/png"))
            .await
            .unwrap_err()
            .to_string()
            .contains("inline limit")
    );
    let too_large = vec![0; 8 * 1024 * 1024 + 1];
    assert!(
        ch.send_voice_bytes_with_id(TelegramVoiceUpload {
            method: "sendVoice",
            field: "voice",
            chat_id: "chat-1",
            thread_id: None,
            reply_to_id: None,
            caption: None,
            bytes: too_large,
            file_name: "voice.ogg",
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("inline limit")
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn upload_receipt_requires_success_status_and_a_message_id() {
    for response in [
        ResponseTemplate::new(400).set_body_json(serde_json::json!({"ok": false})),
        ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true, "result": {}})),
    ] {
        let server = mock_api().await;
        mount_post(&server, "sendVoice", response).await;
        let ch = channel_for(&server, &["*"]);
        let error = ch
            .send_voice_bytes_with_id(TelegramVoiceUpload {
                method: "sendVoice",
                field: "voice",
                chat_id: "chat-1",
                thread_id: Some("topic-3"),
                reply_to_id: Some("12"),
                caption: Some("spoken"),
                bytes: vec![1, 2],
                file_name: "voice.ogg",
            })
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsuccessful or unknown receipt")
                || error.to_string().contains("omitted message receipt")
        );
        let body = raw_bodies(&server, "sendVoice").await;
        assert!(body[0].contains("name=\"message_thread_id\"\r\n\r\ntopic-3"));
        assert!(body[0].contains("name=\"reply_parameters\""));
        assert!(body[0].contains("name=\"caption\"\r\n\r\nspoken"));
    }
}

#[tokio::test]
async fn custom_reaction_uses_custom_emoji_id_and_rejects_blank_values() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    let ch = channel_for(&server, &["*"]);

    assert!(!ch.send_message_custom_reaction("1", 2, "  ").await.unwrap());
    assert!(
        ch.send_message_custom_reaction("1", 2, " custom-emoji ")
            .await
            .unwrap()
    );
    let bodies = json_bodies(&server, "setMessageReaction").await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["reaction"][0]["type"], "custom_emoji");
    assert_eq!(bodies[0]["reaction"][0]["custom_emoji_id"], "custom-emoji");
}

#[tokio::test]
async fn send_text_chunks_fails_when_markdown_and_plain_both_rejected() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .and(body_partial_json(
            serde_json::json!({"parse_mode":"MarkdownV2"}),
        ))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "ok": false,
            "description": "Bad Request: can't parse entities: can't find end of the entity"
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .and(body_partial_json(serde_json::json!({"text":"hi"})))
        .respond_with(ResponseTemplate::new(403).set_body_string("bot was blocked"))
        .with_priority(2)
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);

    let err = ch
        .send_text_chunks("hi", "100", None, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Telegram sendMessage rejected with HTTP 403"),
        "{err}"
    );
    assert!(
        !err.contains("bot was blocked"),
        "provider body must remain private: {err}"
    );
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

#[tokio::test]
async fn send_attachment_by_url_routes_each_kind_to_its_method() {
    let server = mock_api().await;
    for (_, api_method, _) in ALL_KINDS {
        mount_post(&server, api_method, ok()).await;
    }
    let ch = channel_for(&server, &["*"]);

    for (kind, api_method, field) in ALL_KINDS {
        let url = format!("https://cdn.example/{field}");
        let attachment = attachment(kind, &format!(" {url} "));
        let target = attachment.target.trim();
        match kind {
            TelegramAttachmentKind::Image => {
                ch.send_photo_by_url("100", Some("4"), target, None).await
            }
            TelegramAttachmentKind::Document => {
                ch.send_document_by_url("100", Some("4"), target, None)
                    .await
            }
            TelegramAttachmentKind::Video => {
                ch.send_video_by_url("100", Some("4"), target, None).await
            }
            TelegramAttachmentKind::Audio => {
                ch.send_audio_by_url("100", Some("4"), target, None).await
            }
            TelegramAttachmentKind::Voice => {
                ch.send_voice_by_url("100", Some("4"), target, None).await
            }
        }
        .unwrap();
        let bodies = json_bodies(&server, api_method).await;
        let body = bodies.last().expect("request sent");
        assert_eq!(body[field], url, "{api_method}");
        assert_eq!(body["chat_id"], "100");
        assert_eq!(body["message_thread_id"], "4");
    }
}

#[tokio::test]
async fn send_attachment_from_local_path_uploads_multipart_for_each_kind() {
    let server = mock_api().await;
    for (_, api_method, _) in ALL_KINDS {
        mount_post(&server, api_method, ok()).await;
    }
    let ch = channel_for(&server, &["*"]);
    let dir = tempfile::tempdir().unwrap();

    for (kind, api_method, field) in ALL_KINDS {
        let file = dir.path().join(format!("{field}.bin"));
        std::fs::write(&file, b"payload-bytes").unwrap();
        match kind {
            TelegramAttachmentKind::Image => ch.send_photo("100", None, &file, None).await,
            TelegramAttachmentKind::Document => ch.send_document("100", None, &file, None).await,
            TelegramAttachmentKind::Video => ch.send_video("100", None, &file, None).await,
            TelegramAttachmentKind::Audio => ch.send_audio("100", None, &file, None).await,
            TelegramAttachmentKind::Voice => ch.send_voice("100", None, &file, None).await,
        }
        .unwrap();
        let bodies = raw_bodies(&server, api_method).await;
        let body = bodies.last().expect("upload sent");
        assert!(body.contains(&format!("name=\"{field}\"")), "{api_method}");
        assert!(body.contains(&format!("filename=\"{field}.bin\"")));
        assert!(body.contains("payload-bytes"));
        assert!(!body.contains("message_thread_id"));
    }
}

#[tokio::test]
async fn send_attachment_missing_local_path_errors_without_network() {
    let server = mock_api().await;
    let ch = channel_for(&server, &["*"]);
    let result = ch
        .send_document("100", None, std::path::Path::new("/no/such/file.pdf"), None)
        .await;
    assert!(result.is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn file_uploads_include_thread_and_caption_fields() {
    let server = mock_api().await;
    for (_, api_method, _) in ALL_KINDS {
        mount_post(&server, api_method, ok()).await;
    }
    let ch = channel_for(&server, &["*"]);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("clip.dat");
    std::fs::write(&file, b"abc").unwrap();

    ch.send_document("1", Some("2"), &file, Some("cap-doc"))
        .await
        .unwrap();
    ch.send_photo("1", Some("2"), &file, Some("cap-photo"))
        .await
        .unwrap();
    ch.send_video("1", Some("2"), &file, Some("cap-video"))
        .await
        .unwrap();
    ch.send_audio("1", Some("2"), &file, Some("cap-audio"))
        .await
        .unwrap();
    ch.send_voice("1", Some("2"), &file, Some("cap-voice"))
        .await
        .unwrap();
    ch.send_document_bytes("1", Some("2"), b"xyz".to_vec(), "a.txt", Some("cap-docb"))
        .await
        .unwrap();
    ch.send_photo_bytes("1", Some("2"), b"xyz".to_vec(), "a.png", Some("cap-photob"))
        .await
        .unwrap();

    for (api_method, captions) in [
        ("sendDocument", vec!["cap-doc", "cap-docb"]),
        ("sendPhoto", vec!["cap-photo", "cap-photob"]),
        ("sendVideo", vec!["cap-video"]),
        ("sendAudio", vec!["cap-audio"]),
        ("sendVoice", vec!["cap-voice"]),
    ] {
        let bodies = raw_bodies(&server, api_method).await;
        assert_eq!(bodies.len(), captions.len(), "{api_method}");
        for (body, caption) in bodies.iter().zip(captions) {
            assert!(body.contains("name=\"message_thread_id\""), "{api_method}");
            assert!(body.contains(caption), "{api_method}");
        }
    }
}

#[tokio::test]
async fn file_uploads_surface_api_rejection_text() {
    let server = mock_api().await;
    for (_, api_method, _) in ALL_KINDS {
        mount_post(
            &server,
            api_method,
            ResponseTemplate::new(400).set_body_string(format!("{api_method}-rejected")),
        )
        .await;
    }
    let ch = channel_for(&server, &["*"]);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("clip.dat");
    std::fs::write(&file, b"abc").unwrap();

    let results = vec![
        (
            "sendDocument",
            ch.send_document("1", None, &file, None).await,
        ),
        ("sendPhoto", ch.send_photo("1", None, &file, None).await),
        ("sendVideo", ch.send_video("1", None, &file, None).await),
        ("sendAudio", ch.send_audio("1", None, &file, None).await),
        ("sendVoice", ch.send_voice("1", None, &file, None).await),
        (
            "sendDocument",
            ch.send_document_bytes("1", None, vec![1], "a.txt", None)
                .await,
        ),
        (
            "sendPhoto",
            ch.send_photo_bytes("1", None, vec![1], "a.png", None).await,
        ),
    ];
    for (api_method, result) in results {
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(&format!("Telegram {api_method} failed")),
            "{err}"
        );
        assert!(err.contains(&format!("{api_method}-rejected")), "{err}");
    }
}

#[tokio::test]
async fn send_by_url_includes_caption_and_surfaces_rejections() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(body_partial_json(serde_json::json!({ "chat_id": "bad" })))
        .respond_with(ResponseTemplate::new(400).set_body_string("wrong file identifier"))
        .with_priority(1)
        .mount(&server)
        .await;
    for (_, api_method, _) in ALL_KINDS {
        mount_post(&server, api_method, ok()).await;
    }
    let ch = channel_for(&server, &["*"]);
    let url = "https://cdn.example/f";

    ch.send_document_by_url("1", Some("2"), url, Some("c-doc"))
        .await
        .unwrap();
    ch.send_photo_by_url("1", Some("2"), url, Some("c-photo"))
        .await
        .unwrap();
    ch.send_video_by_url("1", Some("2"), url, Some("c-video"))
        .await
        .unwrap();
    ch.send_audio_by_url("1", None, url, Some("c-audio"))
        .await
        .unwrap();
    ch.send_voice_by_url("1", None, url, None).await.unwrap();

    for (_, api_method, _) in ALL_KINDS {
        assert_eq!(
            json_bodies(&server, api_method).await.len(),
            1,
            "{api_method}"
        );
    }

    assert_eq!(
        json_bodies(&server, "sendDocument").await[0],
        serde_json::json!({
            "chat_id": "1", "document": url, "message_thread_id": "2", "caption": "c-doc"
        })
    );
    assert_eq!(
        json_bodies(&server, "sendPhoto").await[0]["caption"],
        "c-photo"
    );
    assert_eq!(
        json_bodies(&server, "sendVideo").await[0],
        serde_json::json!({
            "chat_id": "1", "video": url, "message_thread_id": "2", "caption": "c-video"
        })
    );
    assert_eq!(
        json_bodies(&server, "sendAudio").await[0]["caption"],
        "c-audio"
    );
    assert!(
        json_bodies(&server, "sendVoice").await[0]
            .get("caption")
            .is_none()
    );

    let doc = ch.send_document_by_url("bad", None, url, None).await;
    let photo = ch.send_photo_by_url("bad", None, url, None).await;
    let video = ch.send_video_by_url("bad", None, url, None).await;
    for (label, result) in [
        ("sendDocument by URL", doc),
        ("sendPhoto by URL", photo),
        ("sendVideo by URL", video),
    ] {
        let err = result.unwrap_err().to_string();
        assert!(err.contains(label), "{err}");
        assert!(err.contains("wrong file identifier"), "{err}");
    }
}

#[tokio::test]
async fn byte_uploads_preserve_configured_filenames_and_optional_thread() {
    let server = mock_api().await;
    mount_post(&server, "sendDocument", ok()).await;
    mount_post(&server, "sendPhoto", ok()).await;
    let ch = channel_for(&server, &["*"]);

    ch.send_document_bytes(
        "100",
        Some("3"),
        b"document-payload".to_vec(),
        "document.txt",
        Some("document caption"),
    )
    .await
    .unwrap();
    ch.send_photo_bytes(
        "100",
        None,
        b"photo-payload".to_vec(),
        "photo.png",
        Some("photo caption"),
    )
    .await
    .unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);

    let documents = raw_bodies(&server, "sendDocument").await;
    assert_eq!(documents.len(), 1);
    assert!(documents[0].contains("filename=\"document.txt\""));
    assert!(documents[0].contains("document-payload"));
    assert!(documents[0].contains("document caption"));
    assert!(documents[0].contains("name=\"message_thread_id\""));
    assert!(documents[0].contains("\r\n3\r\n"));

    let photos = raw_bodies(&server, "sendPhoto").await;
    assert_eq!(photos.len(), 1);
    assert!(photos[0].contains("filename=\"photo.png\""));
    assert!(photos[0].contains("photo-payload"));
    assert!(photos[0].contains("photo caption"));
    assert!(!photos[0].contains("name=\"message_thread_id\""));
}
