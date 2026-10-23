use super::*;
use crate::providers::whatsapp_shared::fakes::{MemoryMedia, MemoryVoices, ogg_bytes, png_bytes};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_bus::{
    ContactMethod, ContentPart, MediaKind, MediaReference, OutboundContentPart,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct Counting(AtomicUsize);

impl Respond for Counting {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        ResponseTemplate::new(200)
            .set_body_json(json!({"messages": [{"id": format!("wamid.{n}")}]}))
    }
}

/// Answers the first `ok` requests with ids, then fails.
struct FailAfter {
    seen: AtomicUsize,
    ok: usize,
}

impl Respond for FailAfter {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if n <= self.ok {
            ResponseTemplate::new(200)
                .set_body_json(json!({"messages": [{"id": format!("wamid.{n}")}]}))
        } else {
            ResponseTemplate::new(500)
        }
    }
}

async fn messages_server(responder: impl Respond + 'static) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/{}/123/messages",
            crate::providers::whatsapp::api::GRAPH_VERSION
        )))
        .respond_with(responder)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/{}/123/media",
            crate::providers::whatsapp::api::GRAPH_VERSION
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "media-1"})))
        .mount(&server)
        .await;
    server
}

fn channel(server: &MockServer) -> WhatsAppChannel {
    WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "verify".into(),
        vec!["*".into()],
    )
    .with_api_base_for_test(&server.uri())
}

fn message(parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "whatsapp".into(),
        conversation_id: "+15550001111".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "whatsapp".into(),
            account_id: Some("whatsapp".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "+15550001111".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "key-1".into(),
        parts,
    }
}

fn text(value: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: value.into(),
            markdown_dialect: None,
        },
    }
}

async fn bodies(server: &MockServer, suffix: &str) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path().ends_with(suffix))
        .map(|request| serde_json::from_slice(&request.body).unwrap_or(Value::Null))
        .collect()
}

// --- body shapes ---------------------------------------------------------------

#[test]
fn recipients_are_reduced_to_digits_and_must_have_some() {
    assert_eq!(
        recipient_digits("+1 (555) 000-1111").unwrap(),
        "15550001111"
    );
    assert!(matches!(
        recipient_digits("abc"),
        Err(WaFailure::Rejected(_))
    ));
}

#[test]
fn text_and_quote_bodies_follow_the_cloud_api_shape() {
    let plain = text_body("1555", "hi", None);
    assert_eq!(plain["messaging_product"], "whatsapp");
    assert_eq!(plain["recipient_type"], "individual");
    assert_eq!(plain["type"], "text");
    assert_eq!(plain["text"]["body"], "hi");
    assert_eq!(plain["text"]["preview_url"], false);
    assert!(plain.get("context").is_none());
    let quoted = text_body("1555", "hi", Some("wamid.q"));
    assert_eq!(quoted["context"]["message_id"], "wamid.q");
}

#[test]
fn media_bodies_carry_captions_and_file_names_only_where_whatsapp_allows() {
    let image = media_body(
        "1",
        AttachmentKind::Image,
        "m",
        Some("cap"),
        Some("f.png"),
        None,
    );
    assert_eq!(image["image"]["id"], "m");
    assert_eq!(image["image"]["caption"], "cap");
    assert!(image["image"].get("filename").is_none());
    let document = media_body(
        "1",
        AttachmentKind::Document,
        "m",
        Some("cap"),
        Some("f.pdf"),
        Some("q"),
    );
    assert_eq!(document["document"]["filename"], "f.pdf");
    assert_eq!(document["context"]["message_id"], "q");
    let video = media_body("1", AttachmentKind::Video, "m", Some("c"), None, None);
    assert_eq!(video["video"]["caption"], "c");
    for kind in [AttachmentKind::Audio, AttachmentKind::Sticker] {
        let body = media_body("1", kind, "m", Some("ignored"), Some("ignored"), None);
        let name = if kind == AttachmentKind::Audio {
            "audio"
        } else {
            "sticker"
        };
        assert_eq!(body[name], json!({"id": "m"}));
    }
}

#[test]
fn location_contact_reaction_and_read_bodies_follow_the_cloud_api_shape() {
    let location = location_body("1", 1.5, -2.5, Some("Home"), Some("1 Road"), None);
    assert_eq!(location["location"]["latitude"], 1.5);
    assert_eq!(location["location"]["name"], "Home");
    assert_eq!(location["location"]["address"], "1 Road");
    let bare = location_body("1", 0.0, 0.0, None, None, None);
    assert!(bare["location"].get("name").is_none());

    let card = ContactCard {
        display_name: "Ada".into(),
        phones: vec![
            ContactMethod {
                value: "+44 20".into(),
                label: Some("work".into()),
            },
            ContactMethod {
                value: "ext".into(),
                label: Some("bogus".into()),
            },
        ],
        emails: vec![ContactMethod {
            value: "a@b.invalid".into(),
            label: Some("home".into()),
        }],
        organization: Some("Co".into()),
    };
    let contact = contact_body("1", &card, None);
    let entry = &contact["contacts"][0];
    assert_eq!(entry["name"]["formatted_name"], "Ada");
    assert_eq!(entry["phones"][0]["type"], "WORK");
    assert_eq!(entry["phones"][0]["wa_id"], "4420");
    assert_eq!(entry["phones"][1]["type"], "CELL");
    assert!(entry["phones"][1].get("wa_id").is_none());
    assert_eq!(entry["emails"][0]["type"], "HOME");
    assert_eq!(entry["org"]["company"], "Co");
    let nameless = contact_body(
        "1",
        &ContactCard {
            phones: vec![ContactMethod {
                value: "+44".into(),
                label: None,
            }],
            ..Default::default()
        },
        None,
    );
    assert_eq!(nameless["contacts"][0]["name"]["formatted_name"], "+44");
    assert!(nameless["contacts"][0].get("emails").is_none());

    let reaction = reaction_body("1", "wamid.1", "👍");
    assert_eq!(reaction["type"], "reaction");
    assert_eq!(
        reaction["reaction"],
        json!({"message_id": "wamid.1", "emoji": "👍"})
    );

    assert_eq!(read_body("wamid.1", false)["status"], "read");
    assert!(
        read_body("wamid.1", false)
            .get("typing_indicator")
            .is_none()
    );
    assert_eq!(
        read_body("wamid.1", true)["typing_indicator"]["type"],
        "text"
    );
}

// --- delivery ------------------------------------------------------------------

#[tokio::test]
async fn text_is_sent_with_the_quote_on_the_first_message_only() {
    let server = messages_server(Counting(AtomicUsize::new(0))).await;
    let mut request = message(vec![text(&format!(
        "{}\n{}",
        "a".repeat(4000),
        "b".repeat(4000)
    ))]);
    request.reply_to_id = Some("wamid.src".into());
    let outcome = channel(&server).send_v2(&request).await.unwrap();
    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.platform_message_ids, ["wamid.1", "wamid.2"]);
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("wamid.1")
    );
    assert_eq!(outcome.receipt.idempotency_key.as_deref(), Some("key-1"));
    assert_eq!(outcome.receipt.reply_to_id.as_deref(), Some("wamid.src"));
    assert_eq!(
        outcome.receipt.channel.unwrap().account_id.as_deref(),
        Some("whatsapp")
    );
    assert!(outcome.receipt.edit_token.is_none() && outcome.receipt.delete_token.is_none());
    let sent = bodies(&server, "/messages").await;
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0]["to"], "15550001111");
    assert_eq!(sent[0]["context"]["message_id"], "wamid.src");
    assert!(sent[1].get("context").is_none());
    assert_eq!(
        outcome.receipt.parts[0].reply_to_id.as_deref(),
        Some("wamid.src")
    );
    assert_eq!(outcome.receipt.parts[1].reply_to_id, None);
}

#[tokio::test]
async fn a_failure_after_some_messages_returns_the_partial_receipt_and_an_unsafe_to_retry_error() {
    let server = messages_server(FailAfter {
        seen: AtomicUsize::new(0),
        ok: 1,
    })
    .await;
    let request = message(vec![text("one"), text("two")]);
    let outcome = channel(&server).send_v2(&request).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["wamid.1"]);
    let error = outcome.error.expect("the tail failed");
    assert!(error.sent_before_error && !error.retryable);
}

#[tokio::test]
async fn a_text_split_into_chunks_keeps_the_ids_of_the_chunks_that_were_delivered() {
    let server = messages_server(FailAfter {
        seen: AtomicUsize::new(0),
        ok: 1,
    })
    .await;
    let request = message(vec![text(&format!(
        "{}\n{}",
        "a".repeat(4000),
        "b".repeat(4000)
    ))]);
    let outcome = channel(&server).send_v2(&request).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["wamid.1"]);
    assert!(outcome.error.is_some());
}

#[tokio::test]
async fn a_first_message_failure_is_a_plain_error_not_a_receipt() {
    let server = messages_server(FailAfter {
        seen: AtomicUsize::new(0),
        ok: 0,
    })
    .await;
    let result = channel(&server).send_v2(&message(vec![text("one")])).await;
    assert!(matches!(result, Err(WaFailure::Uncertain(_))));
}

#[tokio::test]
async fn planning_failures_never_reach_the_network() {
    let server = messages_server(Counting(AtomicUsize::new(0))).await;
    let mut threaded = message(vec![text("hi")]);
    threaded.thread_id = Some("t".into());
    assert!(matches!(
        channel(&server).send_v2(&threaded).await,
        Err(WaFailure::Unsupported(_))
    ));
    let mut bad_recipient = message(vec![text("hi")]);
    bad_recipient.conversation_ref = None;
    bad_recipient.conversation_id = "not-a-number".into();
    assert!(matches!(
        channel(&server).send_v2(&bad_recipient).await,
        Err(WaFailure::Rejected(_))
    ));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn attachments_are_uploaded_then_sent_by_media_id() {
    let server = messages_server(Counting(AtomicUsize::new(0))).await;
    let media = Arc::new(MemoryMedia::default());
    let voices = Arc::new(MemoryVoices::default());
    voices.seed("v1", "audio/ogg", &ogg_bytes());
    let png = media.seed(MediaKind::Image, "image/png", Some("pic.png"), &png_bytes());
    let pdf = media.seed(
        MediaKind::Document,
        "application/pdf",
        Some("doc.pdf"),
        b"%PDF-1.4 x",
    );
    let webp = media.seed(MediaKind::Image, "image/webp", None, b"RIFF----WEBPxxxx");
    let channel = channel(&server)
        .with_media_repository(media.clone())
        .with_voice_media_store(voices);
    let reference = |id: &str| MediaReference {
        opaque_id: Some(id.into()),
        ..Default::default()
    };
    let request = message(vec![
        OutboundContentPart::Content {
            part: ContentPart::Image {
                media: reference(&png),
                caption: Some("look".into()),
                alt_text: None,
            },
        },
        OutboundContentPart::Content {
            part: ContentPart::File {
                media: reference(&pdf),
                file_name: None,
                caption: None,
            },
        },
        OutboundContentPart::Content {
            part: ContentPart::Sticker {
                media: reference(&webp),
                pack_id: None,
                sticker_id: None,
                emoji: None,
                alt_text: None,
            },
        },
        OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: reference("v1"),
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
            },
        },
        OutboundContentPart::Content {
            part: ContentPart::Location {
                latitude: 1.0,
                longitude: 2.0,
                label: Some("Pin".into()),
                address: None,
                accuracy_meters: None,
                live_until_unix_ms: None,
            },
        },
        OutboundContentPart::Content {
            part: ContentPart::Contact {
                display_name: "Ada".into(),
                phone_numbers: vec![ContactMethod {
                    value: "+44".into(),
                    label: None,
                }],
                emails: vec![],
                organization: None,
            },
        },
    ]);
    let outcome = channel.send_v2(&request).await.unwrap();
    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.platform_message_ids.len(), 6);
    let kinds: Vec<_> = outcome.receipt.parts.iter().map(|part| part.kind).collect();
    assert_eq!(
        kinds,
        [
            MessageReceiptPartKind::Media,
            MessageReceiptPartKind::Media,
            MessageReceiptPartKind::Media,
            MessageReceiptPartKind::Voice,
            MessageReceiptPartKind::Unknown,
            MessageReceiptPartKind::Unknown,
        ]
    );
    let sent = bodies(&server, "/messages").await;
    let types: Vec<_> = sent
        .iter()
        .map(|body| body["type"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        types,
        [
            "image", "document", "sticker", "audio", "location", "contacts"
        ]
    );
    assert_eq!(sent[0]["image"]["caption"], "look");
    assert_eq!(sent[1]["document"]["filename"], "doc.pdf");
    assert_eq!(sent[2]["sticker"], json!({"id": "media-1"}));
    assert_eq!(sent[3]["audio"], json!({"id": "media-1"}));
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path().ends_with("/media"))
            .count(),
        4
    );
}

#[tokio::test]
async fn a_failed_upload_stops_the_send_and_is_safe_to_retry() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/{}/123/media",
            crate::providers::whatsapp::api::GRAPH_VERSION
        )))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let media = Arc::new(MemoryMedia::default());
    let png = media.seed(MediaKind::Image, "image/png", None, &png_bytes());
    let channel = channel(&server).with_media_repository(media);
    let request = message(vec![OutboundContentPart::Content {
        part: ContentPart::Image {
            media: MediaReference {
                opaque_id: Some(png),
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }]);
    let error = channel.send_v2(&request).await.unwrap_err();
    assert!(matches!(error, WaFailure::Rejected(_)), "{error}");
    assert!(!error.into_send_error().sent_before_error);
}

#[test]
fn upload_limits_match_the_cloud_api_ceilings() {
    let limits = WhatsAppChannel::media_limits();
    assert_eq!(limits.image, 5 * 1024 * 1024);
    assert_eq!(limits.video, 16 * 1024 * 1024);
    assert_eq!(limits.sticker, 500 * 1024);
    assert_eq!(limits.caption_chars, 1024);
}

#[test]
fn attachment_names_are_stable_per_kind() {
    for (kind, name) in [
        (AttachmentKind::Image, "image"),
        (AttachmentKind::Video, "video"),
        (AttachmentKind::Audio, "audio"),
        (AttachmentKind::Document, "document"),
        (AttachmentKind::Sticker, "sticker"),
    ] {
        assert_eq!(attachment_name(kind), name);
    }
}
