use super::*;
use crate::channel::ChannelAdapter;
use crate::providers::linq::fixtures::*;
use serde_json::json;
use tinychannels_bus::{ContentPart, ConversationKind};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

fn make_channel() -> LinqChannel {
    LinqChannel::new(
        "test-token".into(),
        "+15551234567".into(),
        vec!["+1234567890".into()],
    )
}

async fn deliver(
    channel: &LinqChannel,
    body: &str,
) -> anyhow::Result<tinychannels_bus::WebhookDeliveryOutcome> {
    ChannelAdapter::deliver_webhook(channel, Some(&signed(body)), body.as_bytes()).await
}

fn is_rejection(error: &anyhow::Error) -> bool {
    error.downcast_ref::<crate::WebhookRejected>().is_some()
}

#[tokio::test]
async fn a_signed_text_message_is_published_on_the_ordered_ingress_path() {
    let server = mock_api(b"").await;
    let rig = rig(&server);
    let body = webhook("m1", json!([{"type": "text", "value": "Hello OpenHuman!"}]));
    let outcome = deliver(&rig.channel, &body).await.unwrap();
    assert_eq!(outcome.messages, 1);
    let inbound = rig.events.inbound();
    assert_eq!(inbound[0].message_id, "m1");
    assert_eq!(inbound[0].text, "Hello OpenHuman!");
    assert_eq!(inbound[0].conversation.id, CHAT);
    assert_eq!(inbound[0].conversation.kind, ConversationKind::Dm);
    assert_eq!(inbound[0].sender.id, PEER);
    assert_eq!(inbound[0].timestamp_unix_ms, Some(1_736_942_400_000));
}

#[tokio::test]
async fn unsigned_badly_signed_stale_and_unconfigured_deliveries_are_rejected() {
    let server = mock_api(b"").await;
    let rig = rig(&server);
    let body = webhook("m1", json!([{"type": "text", "value": "hi"}]));
    let stale = signed_at(&body, "1000");
    let tampered = signed(&format!("{body} "));
    let garbled = "v1=deadbeef";
    for signature in [
        None,
        Some(stale.as_str()),
        Some(tampered.as_str()),
        Some(garbled),
    ] {
        let error = ChannelAdapter::deliver_webhook(&rig.channel, signature, body.as_bytes())
            .await
            .unwrap_err();
        assert!(is_rejection(&error), "{signature:?}");
    }
    let no_secret = channel_for(&server).with_webhook_secret(None);
    assert!(is_rejection(&deliver(&no_secret, &body).await.unwrap_err()));
    assert!(rig.events.inbound().is_empty());
}

#[tokio::test]
async fn authenticated_but_malformed_bodies_are_rejected() {
    let server = mock_api(b"").await;
    let rig = rig(&server);
    let not_utf8 = [0xff, 0xfe];
    let error = ChannelAdapter::deliver_webhook(&rig.channel, Some("t=1,v1=00"), &not_utf8)
        .await
        .unwrap_err();
    assert!(is_rejection(&error));
    assert!(is_rejection(
        &deliver(&rig.channel, "not json").await.unwrap_err()
    ));
    let no_id = json!({
        "event_type": "message.received",
        "data": {"from": PEER, "message": {"parts": [{"type": "text", "value": "x"}]}},
    })
    .to_string();
    assert!(is_rejection(
        &deliver(&rig.channel, &no_id).await.unwrap_err()
    ));
}

#[tokio::test]
async fn other_events_own_messages_and_unlisted_senders_publish_nothing() {
    let server = mock_api(b"").await;
    let rig = rig(&server);
    let restricted = channel_for(&server)
        .with_events(rig.events.clone())
        .with_webhook_secret(Some(SECRET.into()));
    let mut restricted = restricted;
    restricted.allowed_senders = vec!["+19999999999".into()];
    let text = json!([{"type": "text", "value": "x"}]);
    let body = webhook("m1", text.clone());
    assert_eq!(deliver(&restricted, &body).await.unwrap().messages, 0);

    let mut own: serde_json::Value = serde_json::from_str(&body).unwrap();
    own["data"]["is_from_me"] = true.into();
    assert_eq!(
        deliver(&rig.channel, &own.to_string())
            .await
            .unwrap()
            .messages,
        0
    );

    let mut other = own.clone();
    other["data"]["is_from_me"] = false.into();
    other["event_type"] = "message.delivered".into();
    assert_eq!(
        deliver(&rig.channel, &other.to_string())
            .await
            .unwrap()
            .messages,
        0
    );

    let mut no_data = other.clone();
    no_data.as_object_mut().unwrap().remove("data");
    no_data["event_type"] = "message.received".into();
    assert_eq!(
        deliver(&rig.channel, &no_data.to_string())
            .await
            .unwrap()
            .messages,
        0
    );

    let empty = webhook(
        "m2",
        json!([{"type": "text", "value": "  "}, {"type": "sticker"}]),
    );
    assert_eq!(deliver(&rig.channel, &empty).await.unwrap().messages, 0);
    assert!(rig.events.inbound().is_empty());
}

#[tokio::test]
async fn a_redelivered_message_is_dropped_once_published() {
    let server = mock_api(b"").await;
    let rig = rig(&server);
    let body = webhook("m1", json!([{"type": "text", "value": "once"}]));
    assert_eq!(deliver(&rig.channel, &body).await.unwrap().messages, 1);
    let again = deliver(&rig.channel, &body).await.unwrap();
    assert_eq!((again.messages, again.duplicates), (0, 1));
    assert_eq!(rig.events.inbound().len(), 1);
}

#[tokio::test]
async fn a_message_the_host_did_not_accept_fails_and_can_be_redelivered() {
    let server = mock_api(b"").await;
    let mut rig = rig(&server);
    let rejecting =
        std::sync::Arc::new(crate::providers::native::fixtures::RecordingEvents::default());
    rig.channel = channel_for(&server).with_events(rejecting.clone());
    let body = webhook("m1", json!([{"type": "text", "value": "retry me"}]));
    assert!(deliver(&rig.channel, &body).await.is_err());
    assert!(!is_rejection(
        &deliver(&rig.channel, &body).await.unwrap_err()
    ));
    let no_sink = channel_for(&server);
    assert!(deliver(&no_sink, &body).await.is_err());
}

#[tokio::test]
async fn images_are_downloaded_without_credentials_and_stored() {
    let server = mock_api(b"png-bytes").await;
    let rig = rig(&server);
    let url = format!("{}/media/clip", server.uri());
    let body = webhook(
        "m1",
        json!([
            {"type": "text", "value": "look"},
            {"type": "media", "url": url, "mime_type": "image/png", "filename": "a.png"},
        ]),
    );
    deliver(&rig.channel, &body).await.unwrap();
    let inbound = rig.events.inbound();
    assert!(matches!(inbound[0].parts[0], ContentPart::Text { .. }));
    assert!(matches!(inbound[0].parts[1], ContentPart::Image { .. }));
    assert_eq!(inbound[0].media.len(), 1);
    let downloads = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.method.as_str() == "GET")
        .collect::<Vec<_>>();
    assert!(downloads[0].headers.get("authorization").is_none());
}

#[tokio::test]
async fn voice_memos_are_stored_as_voice_notes_with_a_transcript() {
    let server = mock_api(b"\0\0\0\x20ftypM4A voice").await;
    let rig = rig(&server);
    let url = format!("{}/media/clip", server.uri());
    for (id, part) in [
        (
            "v1",
            json!({"type": "voice_memo", "url": url, "mime_type": "audio/mp4"}),
        ),
        (
            "v2",
            json!({"type": "media", "url": url, "mime_type": "audio/mp4", "voice_memo": true}),
        ),
        (
            "v3",
            json!({"type": "media", "url": url, "mime_type": "audio/mp4", "filename": "Audio Message.m4a", "duration_ms": 1200}),
        ),
    ] {
        deliver(&rig.channel, &webhook(id, json!([part])))
            .await
            .unwrap();
    }
    let inbound = rig.events.inbound();
    assert_eq!(inbound.len(), 3);
    for envelope in &inbound {
        let Some(ContentPart::VoiceNote { voice_note }) = envelope.parts.first() else {
            panic!("expected a voice note: {:?}", envelope.parts);
        };
        assert_eq!(voice_note.transcript.as_ref().unwrap().text, "hello");
    }
    assert_eq!(rig.voice.stored.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn untrusted_oversized_and_failing_media_is_skipped_but_text_is_kept() {
    let server = mock_api(b"clip").await;
    let rig = rig(&server);
    let missing = format!("{}/media/missing", server.uri());
    Mock::given(method("GET"))
        .and(path("/media/big"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![
            0u8;
            (LINQ_MAX_FILE_BYTES + 1)
                as usize
        ]))
        .mount(&server)
        .await;
    let big = format!("{}/media/big", server.uri());
    let body = webhook(
        "m1",
        json!([
            {"type": "text", "value": "kept"},
            {"type": "media", "url": "http://example.invalid/x.png", "mime_type": "image/png"},
            {"type": "media", "url": missing, "mime_type": "image/png"},
            {"type": "media", "url": big, "mime_type": "image/png"},
            {"type": "media", "mime_type": "image/png"},
            {"type": "video"},
        ]),
    );
    deliver(&rig.channel, &body).await.unwrap();
    let inbound = rig.events.inbound();
    assert_eq!(inbound[0].parts.len(), 1);
    assert_eq!(inbound[0].text, "kept");
}

#[test]
fn signature_headers_are_split_leniently_and_strictly() {
    assert_eq!(split_signature("t=1, v1=ab"), Some(("1", "ab")));
    assert_eq!(split_signature("v1=ab,t=2,x=3"), Some(("2", "ab")));
    assert_eq!(split_signature("t=1"), None);
    assert_eq!(split_signature("nonsense"), None);
}

#[test]
fn linq_sender_allowed_exact() {
    let ch = make_channel();
    assert!(ch.is_sender_allowed("+1234567890"));
    assert!(!ch.is_sender_allowed("+9876543210"));
}

#[test]
fn linq_sender_allowed_wildcard() {
    let ch = LinqChannel::new("tok".into(), "+15551234567".into(), vec!["*".into()]);
    assert!(ch.is_sender_allowed("+1234567890"));
    assert!(ch.is_sender_allowed("+9999999999"));
}

#[test]
fn linq_sender_allowed_empty() {
    let ch = LinqChannel::new("tok".into(), "+15551234567".into(), vec![]);
    assert!(!ch.is_sender_allowed("+1234567890"));
}
