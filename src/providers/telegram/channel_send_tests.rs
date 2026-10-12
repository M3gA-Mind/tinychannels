use super::*;
use crate::providers::telegram::fixtures::*;
use crate::providers::telegram::text::TELEGRAM_MAX_MESSAGE_LENGTH;
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
    assert_eq!(recorded[0].1, "reaction_sent");
    assert_eq!(recorded[0].2["target_message_id"], "telegram_55_7");
    assert_eq!(recorded[0].2["success"], true);
}

#[tokio::test]
async fn send_message_reaction_rejection_is_not_an_error_and_reports_failure() {
    let server = mock_api().await;
    mount_post(
        &server,
        "setMessageReaction",
        ResponseTemplate::new(400).set_body_string("REACTION_INVALID"),
    )
    .await;
    let events = Arc::new(RecordingEvents::default());
    let ch = channel_for(&server, &["*"]).with_events(events.clone());

    assert!(!ch.send_message_reaction("55", 7, "🦄").await.unwrap());
    let recorded = events.events.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
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
            "parse_mode": "Markdown",
            "message_thread_id": "9",
            "reply_to_message_id": 42,
        })]
    );
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
    assert!(texts[0].ends_with("(continues...)"));
    assert!(!texts[0].starts_with("(continued)"));
    assert!(texts[1].starts_with("(continued)") && texts[1].ends_with("(continues...)"));
    assert!(texts[2].starts_with("(continued)") && !texts[2].ends_with("(continues...)"));
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
            serde_json::json!({ "parse_mode": "Markdown" }),
        ))
        .respond_with(ResponseTemplate::new(400).set_body_string("can't parse entities"))
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
async fn send_text_chunks_fails_when_markdown_and_plain_both_rejected() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(403).set_body_string("bot was blocked"),
    )
    .await;
    let ch = channel_for(&server, &["*"]);

    let err = ch
        .send_text_chunks("hi", "100", None, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("Telegram sendMessage failed"), "{err}");
    assert!(err.contains("bot was blocked"), "{err}");
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
        ch.send_attachment("100", Some("4"), &attachment(kind, &format!(" {url} ")))
            .await
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
        ch.send_attachment("100", None, &attachment(kind, file.to_str().unwrap()))
            .await
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
    let err = ch
        .send_attachment(
            "100",
            None,
            &attachment(TelegramAttachmentKind::Document, "/no/such/file.pdf"),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("attachment path not found"), "{err}");
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
        ("sendDocument", ch.send_document("1", None, &file, None).await),
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
        assert!(err.contains(&format!("Telegram {api_method} failed")), "{err}");
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

#[test]
fn parse_reaction_marker_without_closing_bracket_is_plain_text() {
    assert_eq!(
        TelegramChannel::parse_reaction_marker("[REACTION:👍 no close"),
        ("[REACTION:👍 no close".to_string(), None)
    );
}
