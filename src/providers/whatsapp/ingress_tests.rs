use super::*;
use crate::providers::whatsapp::api::GRAPH_VERSION;
use crate::providers::whatsapp_shared::fakes::{
    CollectingSink, MemoryMedia, MemoryVoices, png_bytes,
};
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::ChannelIngress;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SECRET: &str = "app-secret";

fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

struct Rig {
    channel: WhatsAppChannel,
    sink: Arc<CollectingSink>,
    server: MockServer,
    media: Arc<MemoryMedia>,
    voices: Arc<MemoryVoices>,
}

async fn rig(accept: bool) -> Rig {
    let server = MockServer::start().await;
    let sink = Arc::new(CollectingSink {
        accept,
        ..CollectingSink::new()
    });
    let media = Arc::new(MemoryMedia::default());
    let voices = Arc::new(MemoryVoices::default());
    let channel = WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "verify-me".into(),
        vec!["+15550001111".into()],
    )
    .with_api_base_for_test(&server.uri())
    .with_app_secret(Some(SECRET.into()))
    .with_events(sink.clone())
    .with_media_repository(media.clone())
    .with_voice_media_store(voices.clone());
    Rig {
        channel,
        sink,
        server,
        media,
        voices,
    }
}

fn delivery(value: Value) -> Value {
    json!({
        "object": "whatsapp_business_account",
        "entry": [{"id": "waba", "changes": [{"field": "messages", "value": value}]}]
    })
}

fn value_with(messages: Value) -> Value {
    json!({
        "messaging_product": "whatsapp",
        "metadata": {"display_phone_number": "1", "phone_number_id": "123"},
        "contacts": [{"wa_id": "15550001111", "profile": {"name": " Ada "}}],
        "messages": messages,
    })
}

async fn post(rig: &Rig, payload: &Value) -> Result<WebhookDeliveryOutcome, WaFailure> {
    let body = serde_json::to_vec(payload).unwrap();
    rig.channel
        .ingest_webhook(Some(&sign(SECRET, &body)), &body)
        .await
}

fn text_message(id: &str, from: &str, body: &str) -> Value {
    json!({"from": from, "id": id, "timestamp": "1700000000", "type": "text", "text": {"body": body}})
}

fn inbound(sink: &CollectingSink) -> Vec<ChannelInboundEnvelope> {
    sink.ingress
        .lock()
        .iter()
        .filter_map(|ingress| match ingress {
            ChannelIngress::Inbound(envelope) => Some(envelope.clone()),
            ChannelIngress::Event(_) => None,
        })
        .collect()
}

fn events(sink: &CollectingSink) -> Vec<ChannelEvent> {
    sink.ingress
        .lock()
        .iter()
        .filter_map(|ingress| match ingress {
            ChannelIngress::Event(event) => Some(event.clone()),
            ChannelIngress::Inbound(_) => None,
        })
        .collect()
}

// --- authentication --------------------------------------------------------------

#[test]
fn signatures_must_be_a_valid_hmac_of_the_exact_bytes() {
    let body = b"{\"a\":1}";
    let good = sign(SECRET, body);
    assert!(verify_signature(SECRET, Some(&good), body).is_ok());
    assert!(verify_signature(SECRET, Some(&format!("  {good}  ")), body).is_ok());
    let wrong_key = sign("other", body);
    let upper = good.to_uppercase().replace("SHA256=", "sha256=");
    for bad in [
        None,
        Some(""),
        Some("sha256="),
        Some("sha256=zz"),
        Some(&good[7..]),
        Some(wrong_key.as_str()),
        Some(&upper),
    ] {
        if bad == Some(upper.as_str()) {
            // Hex is case-insensitive; the MAC still verifies.
            assert!(verify_signature(SECRET, bad, body).is_ok());
            continue;
        }
        assert!(
            matches!(
                verify_signature(SECRET, bad, body),
                Err(WaFailure::Rejected(_))
            ),
            "{bad:?}"
        );
    }
    assert!(verify_signature(SECRET, Some(&good), b"{\"a\":2}").is_err());
}

#[test]
fn the_verify_token_only_answers_the_subscribe_handshake() {
    let channel = WhatsAppChannel::new("t".into(), "1".into(), "verify-me".into(), vec![]);
    assert_eq!(
        channel
            .verify_challenge("subscribe", "verify-me", "abc")
            .as_deref(),
        Some("abc")
    );
    assert_eq!(
        channel.verify_challenge("subscribe", "verify-mx", "abc"),
        None
    );
    assert_eq!(channel.verify_challenge("subscribe", "verify", "abc"), None);
    assert_eq!(
        channel.verify_challenge("unsubscribe", "verify-me", "abc"),
        None
    );
    assert_eq!(channel.verify_challenge("subscribe", "verify-me", ""), None);
    let unset = WhatsAppChannel::new("t".into(), "1".into(), String::new(), vec![]);
    assert_eq!(unset.verify_challenge("subscribe", "", "abc"), None);
}

#[tokio::test]
async fn unauthenticated_oversized_and_malformed_deliveries_are_refused_before_parsing() {
    let rig = rig(true).await;
    let body = serde_json::to_vec(&delivery(value_with(json!([text_message(
        "w1",
        "15550001111",
        "hi"
    )]))))
    .unwrap();
    // Missing and wrong signatures.
    assert!(rig.channel.ingest_webhook(None, &body).await.is_err());
    assert!(
        rig.channel
            .ingest_webhook(Some(&sign("other", &body)), &body)
            .await
            .is_err()
    );
    // A verify token is not a POST signature.
    assert!(
        rig.channel
            .ingest_webhook(Some("verify-me"), &body)
            .await
            .is_err()
    );
    // Oversized body: refused even with a valid signature.
    let huge = vec![b' '; MAX_WEBHOOK_BYTES + 1];
    assert!(matches!(
        rig.channel
            .ingest_webhook(Some(&sign(SECRET, &huge)), &huge)
            .await,
        Err(WaFailure::Rejected(_))
    ));
    // Signed but not JSON, and signed JSON for another object.
    let junk = b"not json";
    assert!(matches!(
        rig.channel
            .ingest_webhook(Some(&sign(SECRET, junk)), junk)
            .await,
        Err(WaFailure::Rejected(_))
    ));
    let other = serde_json::to_vec(&json!({"object": "page", "entry": []})).unwrap();
    assert!(matches!(
        rig.channel
            .ingest_webhook(Some(&sign(SECRET, &other)), &other)
            .await,
        Err(WaFailure::Rejected(_))
    ));
    assert!(rig.sink.ingress.lock().is_empty(), "nothing leaked through");
}

#[tokio::test]
async fn a_channel_without_an_app_secret_refuses_every_delivery() {
    let rig = rig(true).await;
    let channel = WhatsAppChannel::new("t".into(), "123".into(), "v".into(), vec!["*".into()])
        .with_app_secret(Some("  ".into()))
        .with_events(rig.sink.clone());
    let body = serde_json::to_vec(&delivery(value_with(json!([])))).unwrap();
    for signature in [None, Some(sign("", &body)), Some(sign(SECRET, &body))] {
        assert!(matches!(
            channel.ingest_webhook(signature.as_deref(), &body).await,
            Err(WaFailure::Rejected(_))
        ));
    }
}

#[tokio::test]
async fn a_delivery_for_another_phone_number_is_refused_not_routed() {
    let rig = rig(true).await;
    let mut other = value_with(json!([text_message("w1", "15550001111", "hi")]));
    other["metadata"]["phone_number_id"] = json!("999");
    assert!(matches!(
        post(&rig, &delivery(other.clone())).await,
        Err(WaFailure::Rejected(_))
    ));
    let mut missing = other.clone();
    missing["metadata"] = json!({});
    assert!(post(&rig, &delivery(missing)).await.is_err());
    assert!(rig.sink.ingress.lock().is_empty());

    // A mixed delivery ingests the matching change and ignores the other.
    let mixed = json!({
        "object": "whatsapp_business_account",
        "entry": [{"changes": [
            {"field": "messages", "value": other},
            {"field": "messages", "value": value_with(json!([text_message("w2", "15550001111", "mine")]))},
            {"field": "account_update", "value": {"irrelevant": true}},
            {"field": "messages"},
        ]}, {"id": "no changes"}]
    });
    let outcome = post(&rig, &mixed).await.unwrap();
    assert_eq!(outcome.messages, 1);
    assert_eq!(inbound(&rig.sink)[0].text, "mine");
}

// --- messages --------------------------------------------------------------------

#[tokio::test]
async fn an_allowed_text_message_becomes_a_typed_envelope() {
    let rig = rig(true).await;
    let mut message = text_message("wamid.1", "15550001111", "hello");
    message["context"] = json!({"id": "wamid.quoted"});
    let outcome = post(&rig, &delivery(value_with(json!([message]))))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        WebhookDeliveryOutcome {
            messages: 1,
            events: 0,
            duplicates: 0
        }
    );
    let envelope = &inbound(&rig.sink)[0];
    assert_eq!(envelope.message_id, "wamid.1");
    assert_eq!(envelope.reply_to_id.as_deref(), Some("wamid.quoted"));
    assert_eq!(envelope.timestamp_unix_ms, Some(1_700_000_000_000));
    assert_eq!(envelope.conversation.id, "+15550001111");
    assert_eq!(
        envelope.conversation.kind,
        tinychannels_bus::ConversationKind::Dm
    );
    assert_eq!(envelope.sender.id, "+15550001111");
    assert_eq!(envelope.sender.name.as_deref(), Some("Ada"));
    assert_eq!(envelope.text, "hello");
    assert_eq!(
        envelope.raw.as_ref().unwrap()["providerMessageId"],
        "wamid.1"
    );
    assert!(matches!(&envelope.parts[0], ContentPart::Text { text, .. } if text == "hello"));
    // The latest inbound id anchors typing indicators for that conversation.
    assert_eq!(
        rig.channel.anchors.lock().get("+15550001111").as_deref(),
        Some("wamid.1")
    );
}

#[tokio::test]
async fn redelivery_is_published_once() {
    let rig = rig(true).await;
    let payload = delivery(value_with(json!([text_message(
        "wamid.1",
        "15550001111",
        "hello"
    )])));
    assert_eq!(post(&rig, &payload).await.unwrap().messages, 1);
    let second = post(&rig, &payload).await.unwrap();
    assert_eq!((second.messages, second.duplicates), (0, 1));
    assert_eq!(inbound(&rig.sink).len(), 1);
}

#[tokio::test]
async fn senders_off_the_allow_list_and_unsupported_kinds_are_dropped_quietly() {
    let rig = rig(true).await;
    let payload = delivery(value_with(json!([
        text_message("a", "19998887777", "stranger"),
        {"from": "15550001111", "id": "b", "type": "order", "timestamp": "1"},
        {"from": "15550001111", "id": "c", "type": "text", "text": {"body": "  "}},
        {"from": "15550001111", "id": "d", "type": "image", "image": {}},
        {"id": "e"},
        {"from": "15550001111"},
    ])));
    let outcome = post(&rig, &payload).await.unwrap();
    assert_eq!(outcome.messages, 0);
    assert!(rig.sink.ingress.lock().is_empty());
}

#[tokio::test]
async fn button_and_interactive_replies_become_text() {
    let rig = rig(true).await;
    let payload = delivery(value_with(json!([
        {"from": "15550001111", "id": "b1", "type": "button", "button": {"text": "Yes", "payload": "p"}},
        {"from": "15550001111", "id": "b2", "type": "interactive", "interactive": {"button_reply": {"id": "x", "title": "Approve"}}},
        {"from": "15550001111", "id": "b3", "type": "interactive", "interactive": {"list_reply": {"id": "y", "title": "Option 3"}}},
        {"from": "15550001111", "id": "b4", "type": "interactive", "interactive": {}},
    ])));
    post(&rig, &payload).await.unwrap();
    let texts: Vec<_> = inbound(&rig.sink).into_iter().map(|e| e.text).collect();
    assert_eq!(texts, ["Yes", "Approve", "Option 3"]);
}

#[tokio::test]
async fn locations_and_contacts_are_typed_parts() {
    let rig = rig(true).await;
    let payload = delivery(value_with(json!([
        {"from": "15550001111", "id": "l1", "type": "location",
         "location": {"latitude": 51.5, "longitude": -0.1, "name": "Home", "address": "1 Road"}},
        {"from": "15550001111", "id": "l2", "type": "location", "location": {"latitude": 91.0, "longitude": 0.0}},
        {"from": "15550001111", "id": "c1", "type": "contacts", "contacts": [{
            "name": {"formatted_name": "Bob"},
            "phones": [{"phone": "+44 20", "type": "WORK"}, {"type": "CELL"}],
            "emails": [{"email": "b@c.invalid", "type": "HOME"}],
            "org": {"company": "Co"}
        }]},
    ])));
    let outcome = post(&rig, &payload).await.unwrap();
    assert_eq!(outcome.messages, 2, "the out-of-range location is dropped");
    let envelopes = inbound(&rig.sink);
    assert!(matches!(
        &envelopes[0].parts[0],
        ContentPart::Location { latitude, label: Some(label), address: Some(_), .. }
            if (*latitude - 51.5).abs() < f64::EPSILON && label == "Home"
    ));
    let ContentPart::Contact {
        display_name,
        phone_numbers,
        emails,
        organization,
    } = &envelopes[1].parts[0]
    else {
        panic!("expected a contact")
    };
    assert_eq!(display_name, "Bob");
    assert_eq!(phone_numbers.len(), 1);
    assert_eq!(phone_numbers[0].label.as_deref(), Some("WORK"));
    assert_eq!(emails[0].value, "b@c.invalid");
    assert_eq!(organization.as_deref(), Some("Co"));
}

// --- reactions and statuses ----------------------------------------------------

#[tokio::test]
async fn reactions_are_events_added_or_removed() {
    let rig = rig(true).await;
    let payload = delivery(value_with(json!([
        {"from": "15550001111", "id": "r1", "timestamp": "5", "type": "reaction",
         "reaction": {"message_id": "wamid.target", "emoji": "👍"}},
        {"from": "15550001111", "id": "r2", "type": "reaction", "reaction": {"message_id": "wamid.target"}},
        {"from": "15550001111", "id": "r3", "type": "reaction", "reaction": {"emoji": "x"}},
    ])));
    let outcome = post(&rig, &payload).await.unwrap();
    assert_eq!((outcome.messages, outcome.events), (0, 2));
    let sent = events(&rig.sink);
    assert!(matches!(
        &sent[0].event,
        ChannelEventKind::ReactionAdded { message_id, provider_message_id: Some(provider), reaction, actor }
            if message_id == "wamid.target" && provider == "wamid.target" && reaction.value == "👍" && actor.id == "+15550001111"
    ));
    assert!(
        matches!(&sent[1].event, ChannelEventKind::ReactionRemoved { reaction, .. } if reaction.value.is_empty())
    );
    assert_eq!(sent[0].event_id, "whatsapp:reaction:r1");
    assert_eq!(sent[0].source_timestamp, Some(5000));
}

#[tokio::test]
async fn statuses_become_receipt_events_and_failures_carry_only_the_error_code() {
    let rig = rig(true).await;
    let value = json!({
        "metadata": {"phone_number_id": "123"},
        "statuses": [
            {"id": "wamid.out", "status": "sent", "recipient_id": "15550001111", "timestamp": "10"},
            {"id": "wamid.out", "status": "delivered", "recipient_id": "15550001111", "timestamp": "11"},
            {"id": "wamid.out", "status": "read", "recipient_id": "15550001111", "timestamp": "12"},
            {"id": "wamid.bad", "status": "failed", "recipient_id": "15550001111",
             "errors": [{"code": 131047, "title": "private details"}]},
            {"id": "wamid.norecipient", "status": "read"},
            {"status": "read"},
        ]
    });
    let outcome = post(&rig, &delivery(value.clone())).await.unwrap();
    assert_eq!(outcome.events, 3);
    let sent = events(&rig.sink);
    assert!(matches!(
        &sent[0].event,
        ChannelEventKind::DeliveredReceipt { message_id, timestamp: Some(11_000), .. } if message_id == "wamid.out"
    ));
    assert!(matches!(
        &sent[1].event,
        ChannelEventKind::ReadReceipt {
            timestamp: Some(12_000),
            ..
        }
    ));
    let ChannelEventKind::Lifecycle { state, reason } = &sent[2].event else {
        panic!("expected a lifecycle event")
    };
    assert_eq!(state, "message_failed");
    assert_eq!(reason.as_deref(), Some("provider error code 131047"));
    assert_eq!(sent[0].conversation.as_ref().unwrap().id, "+15550001111");
    // The same statuses again are duplicates.
    let again = post(&rig, &delivery(value)).await.unwrap();
    assert_eq!((again.events, again.duplicates), (0, 3));
}

// --- attachments ------------------------------------------------------------------

async fn mount_media(server: &MockServer, id: &str, mime: &str, body: Vec<u8>) {
    Mock::given(method("GET"))
        .and(path(format!("/{GRAPH_VERSION}/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "url": format!("{}/cdn/{id}", server.uri()),
            "mime_type": mime,
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/cdn/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn attachments_are_downloaded_stored_and_typed() {
    let rig = rig(true).await;
    mount_media(&rig.server, "img", "image/png", png_bytes()).await;
    mount_media(&rig.server, "vid", "video/mp4", b"0000ftypmp42".to_vec()).await;
    mount_media(
        &rig.server,
        "doc",
        "application/pdf",
        b"%PDF-1.4 x".to_vec(),
    )
    .await;
    mount_media(
        &rig.server,
        "stk",
        "image/webp",
        b"RIFF----WEBPxxxx".to_vec(),
    )
    .await;
    mount_media(&rig.server, "aud", "audio/mpeg", b"ID3-----".to_vec()).await;
    let payload = delivery(value_with(json!([
        {"from": "15550001111", "id": "m1", "type": "image", "image": {"id": "img", "caption": "look", "mime_type": "image/png"}},
        {"from": "15550001111", "id": "m2", "type": "video", "video": {"id": "vid", "caption": "clip"}},
        {"from": "15550001111", "id": "m3", "type": "document", "document": {"id": "doc", "filename": "a.pdf"}},
        {"from": "15550001111", "id": "m4", "type": "sticker", "sticker": {"id": "stk"}},
        {"from": "15550001111", "id": "m5", "type": "audio", "audio": {"id": "aud", "voice": false}},
    ])));
    assert_eq!(post(&rig, &payload).await.unwrap().messages, 5);
    let envelopes = inbound(&rig.sink);
    assert!(
        matches!(&envelopes[0].parts[1], ContentPart::Image { caption: Some(c), .. } if c == "look")
    );
    assert_eq!(envelopes[0].text, "look");
    assert!(matches!(&envelopes[1].parts[1], ContentPart::Video { .. }));
    assert!(
        matches!(&envelopes[2].parts[0], ContentPart::File { file_name: Some(n), .. } if n == "a.pdf")
    );
    assert!(matches!(
        &envelopes[3].parts[0],
        ContentPart::Sticker { .. }
    ));
    assert!(matches!(&envelopes[4].parts[0], ContentPart::Audio { .. }));
    assert_eq!(envelopes[0].media.len(), 1);
    assert!(
        envelopes[0].media[0]
            .opaque_id
            .as_deref()
            .unwrap()
            .starts_with("stored-")
    );
    let stored = rig.media.stored.lock();
    assert_eq!(stored[0].dedupe_key.as_deref(), Some("whatsapp:m1:image"));
    assert_eq!(stored[0].scope.turn_id, "m1");
    assert_eq!(stored[0].scope.conversation.id, "+15550001111");
}

#[tokio::test]
async fn push_to_talk_audio_is_kept_as_a_voice_note_without_provider_urls() {
    let rig = rig(true).await;
    mount_media(
        &rig.server,
        "ptt",
        "audio/ogg; codecs=opus",
        b"OggS-----".to_vec(),
    )
    .await;
    let payload = delivery(value_with(json!([
        {"from": "15550001111", "id": "v1", "type": "audio", "audio": {"id": "ptt", "voice": true, "mime_type": "audio/ogg; codecs=opus"}}
    ])));
    post(&rig, &payload).await.unwrap();
    let envelope = &inbound(&rig.sink)[0];
    let ContentPart::VoiceNote { voice_note } = &envelope.parts[0] else {
        panic!("expected a voice note")
    };
    assert!(voice_note.push_to_talk);
    assert_eq!(voice_note.codec.as_deref(), Some("ogg_opus"));
    assert!(voice_note.media.url.is_none());
    assert!(
        voice_note.transcript.is_none(),
        "transcription is the module's job"
    );
    assert_eq!(rig.voices.stored.lock().len(), 1);
    assert!(
        rig.media.stored.lock().is_empty(),
        "voice goes to the voice store"
    );
}

#[tokio::test]
async fn without_storage_ports_attachments_are_skipped_but_the_caption_survives() {
    let rig = rig(true).await;
    let bare = WhatsAppChannel::new("tok".into(), "123".into(), "v".into(), vec!["*".into()])
        .with_api_base_for_test(&rig.server.uri())
        .with_app_secret(Some(SECRET.into()))
        .with_events(rig.sink.clone());
    let payload = delivery(value_with(json!([
        {"from": "15550001111", "id": "m1", "type": "image", "image": {"id": "img", "caption": "just words"}},
        {"from": "15550001111", "id": "m2", "type": "audio", "audio": {"id": "aud", "voice": true}},
    ])));
    let body = serde_json::to_vec(&payload).unwrap();
    let outcome = bare
        .ingest_webhook(Some(&sign(SECRET, &body)), &body)
        .await
        .unwrap();
    assert_eq!(
        outcome.messages, 1,
        "the silent voice note has nothing to deliver"
    );
    assert_eq!(inbound(&rig.sink)[0].text, "just words");
    assert!(rig.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_voice_note_falls_back_to_the_media_repository_without_a_voice_store() {
    let rig = rig(true).await;
    mount_media(&rig.server, "ptt", "audio/ogg", b"OggS-----".to_vec()).await;
    let repo_only = WhatsAppChannel::new("tok".into(), "123".into(), "v".into(), vec!["*".into()])
        .with_api_base_for_test(&rig.server.uri())
        .with_app_secret(Some(SECRET.into()))
        .with_events(rig.sink.clone())
        .with_media_repository(rig.media.clone());
    let payload = delivery(value_with(json!([
        {"from": "15550001111", "id": "v1", "type": "audio", "audio": {"id": "ptt", "voice": true}}
    ])));
    let body = serde_json::to_vec(&payload).unwrap();
    repo_only
        .ingest_webhook(Some(&sign(SECRET, &body)), &body)
        .await
        .unwrap();
    assert!(matches!(
        &inbound(&rig.sink)[0].parts[0],
        ContentPart::Audio { .. }
    ));
}

#[tokio::test]
async fn permanent_media_failures_skip_the_attachment_and_transient_ones_ask_for_redelivery() {
    let rig = rig(true).await;
    // Permanent: Meta no longer has the media.
    Mock::given(method("GET"))
        .and(path(format!("/{GRAPH_VERSION}/gone")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&rig.server)
        .await;
    let gone = delivery(value_with(json!([
        {"from": "15550001111", "id": "p1", "type": "image", "image": {"id": "gone", "caption": "still here"}}
    ])));
    assert_eq!(post(&rig, &gone).await.unwrap().messages, 1);
    assert_eq!(inbound(&rig.sink)[0].text, "still here");

    // Transient: the CDN is failing, so nothing is published or remembered.
    rig.server.reset().await;
    Mock::given(method("GET"))
        .and(path(format!("/{GRAPH_VERSION}/flaky")))
        .respond_with(ResponseTemplate::new(500))
        .mount(&rig.server)
        .await;
    let flaky = delivery(value_with(json!([
        {"from": "15550001111", "id": "t1", "type": "image", "image": {"id": "flaky"}}
    ])));
    assert!(matches!(
        post(&rig, &flaky).await,
        Err(WaFailure::Uncertain(_))
    ));
    assert_eq!(inbound(&rig.sink).len(), 1);
    // Recovery: the redelivery now succeeds because the id was not remembered.
    rig.server.reset().await;
    mount_media(&rig.server, "flaky", "image/png", png_bytes()).await;
    assert_eq!(post(&rig, &flaky).await.unwrap().messages, 1);
}

#[tokio::test]
async fn unstorable_attachments_follow_the_same_classes() {
    let rig = rig(true).await;
    mount_media(&rig.server, "odd", "application/x-unknown", b"zzz".to_vec()).await;
    let odd = delivery(value_with(json!([
        {"from": "15550001111", "id": "o1", "type": "document", "document": {"id": "odd", "caption": "cap"}}
    ])));
    assert_eq!(
        post(&rig, &odd).await.unwrap().messages,
        1,
        "unsupported type is skipped"
    );
    mount_media(&rig.server, "ok", "image/png", png_bytes()).await;
    *rig.media.fail_store.lock() = true;
    let store_down = delivery(value_with(json!([
        {"from": "15550001111", "id": "o2", "type": "image", "image": {"id": "ok"}}
    ])));
    assert!(matches!(
        post(&rig, &store_down).await,
        Err(WaFailure::Uncertain(_))
    ));
    mount_media(&rig.server, "ptt", "audio/ogg", b"OggS-----".to_vec()).await;
    *rig.voices.omit_opaque_id.lock() = true;
    let voice_down = delivery(value_with(json!([
        {"from": "15550001111", "id": "o3", "type": "audio", "audio": {"id": "ptt", "voice": true}}
    ])));
    assert!(matches!(
        post(&rig, &voice_down).await,
        Err(WaFailure::Uncertain(_))
    ));
    // A voice note the host refuses on size is skipped like other permanent failures.
    mount_media(&rig.server, "empty", "audio/ogg", Vec::new()).await;
    let empty = delivery(value_with(json!([
        {"from": "15550001111", "id": "o4", "type": "audio", "audio": {"id": "empty", "voice": true}}
    ])));
    assert_eq!(post(&rig, &empty).await.unwrap().messages, 0);
}

// --- hand-off to the host ---------------------------------------------------------

#[tokio::test]
async fn a_host_without_typed_ingress_receives_the_legacy_message_through_the_listener() {
    let rig = rig(false).await;
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    *rig.channel.listener.lock() = Some(tx);
    let payload = delivery(value_with(json!([text_message(
        "wamid.1",
        "15550001111",
        "hello"
    )])));
    assert_eq!(post(&rig, &payload).await.unwrap().messages, 1);
    #[allow(
        deprecated,
        reason = "the legacy sink is the compatibility path under test"
    )]
    let legacy = rx.recv().await.unwrap();
    assert_eq!(legacy.content, "hello");
    assert_eq!(legacy.sender, "+15550001111");
}

#[tokio::test]
async fn delivery_fails_when_the_host_cannot_take_it() {
    // No ordered sink acceptance and no listener.
    let rig_no_listener = rig(false).await;
    let payload = delivery(value_with(json!([text_message("a", "15550001111", "x")])));
    assert!(matches!(
        post(&rig_no_listener, &payload).await,
        Err(WaFailure::Uncertain(_))
    ));

    // A listener that has gone away.
    let rig_closed = rig(false).await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    *rig_closed.channel.listener.lock() = Some(tx);
    assert!(matches!(
        post(&rig_closed, &payload).await,
        Err(WaFailure::Uncertain(_))
    ));

    // A failing sink.
    let failing = Arc::new(CollectingSink {
        fail: true,
        ..CollectingSink::new()
    });
    let channel = WhatsAppChannel::new("t".into(), "123".into(), "v".into(), vec!["*".into()])
        .with_app_secret(Some(SECRET.into()))
        .with_events(failing);
    let body = serde_json::to_vec(&payload).unwrap();
    assert!(matches!(
        channel
            .ingest_webhook(Some(&sign(SECRET, &body)), &body)
            .await,
        Err(WaFailure::Uncertain(_))
    ));

    // No sink at all.
    let sinkless = WhatsAppChannel::new("t".into(), "123".into(), "v".into(), vec!["*".into()])
        .with_app_secret(Some(SECRET.into()));
    assert!(matches!(
        sinkless
            .ingest_webhook(Some(&sign(SECRET, &body)), &body)
            .await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn an_unrelated_change_field_and_empty_deliveries_do_nothing() {
    let rig = rig(true).await;
    let empty = delivery(value_with(json!([])));
    assert_eq!(
        post(&rig, &empty).await.unwrap(),
        WebhookDeliveryOutcome::default()
    );
}
