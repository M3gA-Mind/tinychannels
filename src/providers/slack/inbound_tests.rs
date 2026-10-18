use super::*;
use crate::providers::slack::fixtures::*;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, ResponseTemplate};

fn conversation() -> ConversationRef {
    SlackChannel::conversation_ref("C1", Some("channel"), Some("1.1".into()))
}

#[test]
fn timestamps_convert_to_milliseconds_and_reject_garbage() {
    assert_eq!(ts_to_ms("1700000000.000200"), Some(1_700_000_000_000));
    assert_eq!(ts_to_ms("nope"), None);
    assert_eq!(ts_to_ms("-5"), None);
}

#[test]
fn conversation_kinds_follow_channel_type_then_id_prefix() {
    let kind = |id, ty| SlackChannel::conversation_ref(id, ty, None).kind;
    assert_eq!(kind("D1", None), ConversationKind::Dm);
    assert_eq!(kind("X1", Some("im")), ConversationKind::Dm);
    assert_eq!(kind("G1", Some("mpim")), ConversationKind::Group);
    assert_eq!(kind("C1", None), ConversationKind::Channel);
    assert_eq!(kind("G1", None), ConversationKind::Channel);
    assert_eq!(kind("X1", Some("group")), ConversationKind::Channel);
    assert_eq!(kind("X1", None), ConversationKind::Unknown);
}

#[test]
fn inbound_thread_ts_prefers_explicit_thread_then_message_ts() {
    let reply = json!({"ts": "123.002", "thread_ts": "123.001"});
    assert_eq!(
        SlackChannel::inbound_thread_ts(&reply, "123.002").as_deref(),
        Some("123.001")
    );
    assert_eq!(
        SlackChannel::inbound_thread_ts(&json!({}), "123.001").as_deref(),
        Some("123.001")
    );
    assert_eq!(SlackChannel::inbound_thread_ts(&json!({}), ""), None);
}

#[test]
fn sender_ref_prefers_username_then_name() {
    let sender = SlackChannel::sender_ref(&json!({"id": "U1", "username": "ada", "name": "Ada L"}));
    assert_eq!(
        (sender.id.as_str(), sender.name.as_deref()),
        ("U1", Some("ada"))
    );
    assert_eq!(
        SlackChannel::sender_ref(&json!({"id": "U1", "name": "Ada L"}))
            .name
            .as_deref(),
        Some("Ada L")
    );
    assert_eq!(SlackChannel::sender_ref(&json!({})).id, "");
}

fn file(server_url: &str, route: &str, mime: &str) -> serde_json::Value {
    json!({
        "id": "F1", "name": "report.pdf", "mimetype": mime, "size": 4,
        "url_private_download": format!("{server_url}{route}"),
    })
}

#[tokio::test]
async fn attached_files_become_typed_parts_downloaded_with_the_bot_token() {
    let server = mock_api().await;
    for (route, mime) in [
        ("/f/a.png", "image/png"),
        ("/f/a.mp4", "video/mp4"),
        ("/f/a.mp3", "audio/mpeg"),
        ("/f/a.pdf", "application/pdf"),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .and(header("authorization", "Bearer bot-test-token"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", mime)
                    .set_body_bytes(b"data".to_vec()),
            )
            .mount(&server)
            .await;
    }
    let media = Arc::new(StubMedia::default());
    let channel = channel_for(&server, &["*"]).with_media_repository(media.clone());
    let uri = server.uri();
    let event = json!({"files": [
        file(&uri, "/f/a.png", "image/png"),
        file(&uri, "/f/a.mp4", "video/mp4"),
        file(&uri, "/f/a.mp3", "audio/mpeg"),
        file(&uri, "/f/a.pdf", "application/pdf; charset=binary"),
    ]});
    let (parts, voice) = channel
        .media_parts(&event, "slack_C1_1.1", &conversation())
        .await;
    assert!(voice.is_none());
    assert!(matches!(parts[0], ContentPart::Image { .. }));
    assert!(matches!(parts[1], ContentPart::Video { .. }));
    assert!(matches!(parts[2], ContentPart::Audio { .. }));
    assert!(
        matches!(&parts[3], ContentPart::File { file_name, .. } if file_name.as_deref() == Some("report.pdf"))
    );
    let stored = media.stored.lock().unwrap();
    assert_eq!(stored.len(), 4);
    assert_eq!(stored[0].metadata.scope.turn_id, "slack_C1_1.1");
    assert_eq!(
        stored[0].metadata.scope.conversation.thread_id.as_deref(),
        Some("1.1")
    );
    assert_eq!(
        stored[0].metadata.dedupe_key.as_deref(),
        Some("slack:slack_C1_1.1:F1")
    );
    assert_eq!(stored[0].bytes, b"data");
}

#[tokio::test]
async fn audio_clips_become_voice_notes_with_the_provider_transcript() {
    let server = mock_api().await;
    let url = mount_file(&server, "/f/clip.m4a", "audio/mp4", b"audio").await;
    let voice_store = Arc::new(StubVoice::default());
    let channel = channel_for(&server, &["*"]).with_voice_media_store(voice_store.clone());
    let event = json!({"files": [{
        "id": "F9", "name": "clip.m4a", "mimetype": "audio/mp4", "subtype": "slack_audio",
        "size": 5, "duration_ms": 2500, "url_private_download": url,
        "transcription": {"status": "complete", "locale": "en-US", "preview": {"content": "hello there"}},
    }]});
    let (parts, voice) = channel
        .media_parts(&event, "slack_C1_2.2", &conversation())
        .await;
    assert!(parts.is_empty());
    let note = voice.unwrap();
    assert_eq!(note.codec.as_deref(), Some("m4a_aac"));
    assert_eq!(note.duration_ms, Some(2500));
    assert_eq!(note.media.url, None);
    assert_eq!(note.media.kind, MediaKind::Audio);
    let transcript = note.transcript.unwrap();
    assert_eq!(transcript.text, "hello there");
    assert_eq!(transcript.language.as_deref(), Some("en-US"));
    assert_eq!(transcript.source, VoiceTranscriptSource::Provider);
    let stored = voice_store.stored.lock().unwrap();
    assert_eq!(stored[0].direction, HostVoiceMediaDirection::Inbound);
    assert_eq!(stored[0].mime_type, "audio/mp4");
}

#[test]
fn transcripts_require_a_finished_non_empty_preview() {
    let with = |value| json!({"transcription": value});
    assert!(provider_transcript(&json!({})).is_none());
    assert!(provider_transcript(&with(json!({"status": "processing"}))).is_none());
    assert!(
        provider_transcript(&with(
            json!({"status": "complete", "preview": {"content": " "}})
        ))
        .is_none()
    );
    assert!(provider_transcript(&with(json!({"status": "complete"}))).is_none());
    assert_eq!(
        provider_transcript(&with(
            json!({"status": "complete", "preview": {"content": "x"}})
        ))
        .unwrap()
        .language,
        None
    );
    assert_eq!(codec_for("audio/ogg").as_deref(), Some("ogg_opus"));
    assert_eq!(codec_for("audio/mpeg").as_deref(), Some("mp3"));
    assert_eq!(codec_for("audio/wav"), None);
}

#[tokio::test]
async fn clips_without_a_voice_store_fall_back_to_typed_audio() {
    let server = mock_api().await;
    let url = mount_file(&server, "/f/clip.m4a", "audio/mp4", b"audio").await;
    let media = Arc::new(StubMedia::default());
    let channel = channel_for(&server, &["*"]).with_media_repository(media);
    let event = json!({"files": [{"id": "F9", "mimetype": "audio/mp4", "subtype": "slack_audio", "url_private_download": url}]});
    let (parts, voice) = channel.media_parts(&event, "m", &conversation()).await;
    assert!(voice.is_none());
    assert!(matches!(parts[0], ContentPart::Audio { .. }));
}

#[tokio::test]
async fn files_that_cannot_be_trusted_or_stored_are_skipped_without_failing() {
    let server = mock_api().await;
    let good = mount_file(&server, "/f/ok.png", "image/png", b"png").await;
    let html = mount_file(
        &server,
        "/f/login.png",
        "text/html; charset=utf-8",
        b"<html>",
    )
    .await;
    let empty = mount_file(&server, "/f/empty.png", "image/png", b"").await;
    let big = mount_file(&server, "/f/big.png", "image/png", &[0u8; 64]).await;
    let uri = server.uri();
    Mock::given(method("GET"))
        .and(path("/f/missing.png"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let media = Arc::new(StubMedia::default());
    let channel = channel_for(&server, &["*"]).with_media_repository(media.clone());
    let png = |url: &str| json!({"id": "F", "mimetype": "image/png", "url_private_download": url});
    let mut oversized = png(&good);
    oversized["size"] = json!(64 * 1024 * 1024);
    let event = json!({"files": [
        png("https://evil.invalid/steal.png"),
        png(&format!("{uri}/f/missing.png")),
        png(&html),
        png(&empty),
        png(&big),
        oversized,
        json!({"id": "F", "mimetype": "image/png"}),
        json!({"id": "F", "mimetype": "application/x-msdownload", "url_private_download": good}),
        json!({"id": "F"}),
        png(&good),
    ]});
    let (parts, _) = channel.media_parts(&event, "m", &conversation()).await;
    assert_eq!(parts.len(), 2, "only the two well-formed images survive");
    assert_eq!(media.stored.lock().unwrap().len(), 2);
    assert!(
        media
            .stored
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.bytes == b"png" || r.bytes.len() == 64)
    );
}

#[tokio::test]
async fn storage_failures_and_missing_ports_drop_only_the_attachment() {
    let server = mock_api().await;
    let png = mount_file(&server, "/f/a.png", "image/png", b"png").await;
    let clip = mount_file(&server, "/f/c.m4a", "audio/mp4", b"audio").await;
    let image =
        json!({"files": [{"id": "F", "mimetype": "image/png", "url_private_download": png}]});
    let voice = json!({"files": [{"id": "F", "mimetype": "audio/mp4", "subtype": "slack_audio", "url_private_download": clip}]});

    let bare = channel_for(&server, &["*"]);
    assert!(
        bare.media_parts(&image, "m", &conversation())
            .await
            .0
            .is_empty()
    );

    let failing = channel_for(&server, &["*"]).with_media_repository(Arc::new(StubMedia {
        fail_store: true,
        ..Default::default()
    }));
    assert!(
        failing
            .media_parts(&image, "m", &conversation())
            .await
            .0
            .is_empty()
    );

    for store in [
        StubVoice {
            fail_store: true,
            ..Default::default()
        },
        StubVoice {
            empty_reference: true,
            ..Default::default()
        },
    ] {
        let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(store));
        let (parts, note) = channel.media_parts(&voice, "m", &conversation()).await;
        assert!(parts.is_empty() && note.is_none());
    }
}
