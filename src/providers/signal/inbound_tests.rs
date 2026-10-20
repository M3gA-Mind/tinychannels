use super::*;
use crate::providers::native::fixtures::RecordingEvents;
use crate::providers::signal::fixtures::*;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::{ChannelEventKind, ChannelIngress, ContentPart, ConversationKind};

struct RejectingTypedSink;

#[async_trait]
impl crate::channel::ChannelInboundSink for RejectingTypedSink {
    async fn push(&self, _ingress: ChannelIngress) -> Result<(), crate::channel::ChannelSendError> {
        Err(crate::channel::ChannelSendError {
            message: "test event rejection".into(),
            ..Default::default()
        })
    }
}

fn channel_pair() -> (mpsc::Sender<ChannelMessage>, mpsc::Receiver<ChannelMessage>) {
    mpsc::channel(8)
}

fn attachment(id: &str, mime: &str, voice: bool) -> serde_json::Value {
    json!({"id": id, "contentType": mime, "filename": "f.bin", "size": 5, "voiceNote": voice})
}

#[tokio::test]
async fn text_is_published_on_the_ordered_ingress_path() {
    let server = mock_daemon(b"").await;
    let rig = rig(&server);
    let (tx, mut rx) = channel_pair();
    let envelope = data_envelope(json!({"message": "hi", "timestamp": 1_700_000_000_400u64}));
    assert!(rig.channel.dispatch_envelope(&envelope, &tx).await);
    let inbound = rig.events.inbound();
    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].message_id, "sig_1700000000400");
    assert_eq!(inbound[0].conversation.id, PEER);
    assert_eq!(inbound[0].conversation.kind, ConversationKind::Dm);
    assert_eq!(inbound[0].sender.name.as_deref(), Some("Ada"));
    assert_eq!(inbound[0].timestamp_unix_ms, Some(1_700_000_000_400));
    assert!(
        rx.try_recv().is_err(),
        "delivered messages skip the legacy queue"
    );
}

#[tokio::test]
async fn undelivered_text_falls_back_to_the_legacy_receiver() {
    let server = mock_daemon(b"").await;
    let (tx, mut rx) = channel_pair();
    // No event sink at all.
    let plain = channel_for(&server);
    let envelope = data_envelope(json!({"message": "hi", "timestamp": 1_700_000_000_401u64}));
    assert!(plain.dispatch_envelope(&envelope, &tx).await);
    assert_eq!(rx.recv().await.unwrap().content, "hi");

    // A sink that refuses the message.
    let refusing = channel_for(&server).with_events(Arc::new(RecordingEvents::default()));
    let envelope = data_envelope(json!({"message": "again", "timestamp": 1_700_000_000_402u64}));
    assert!(refusing.dispatch_envelope(&envelope, &tx).await);
    assert_eq!(rx.recv().await.unwrap().content, "again");

    // A sink that errors, with the receiver gone.
    let failing = channel_for(&server).with_events(Arc::new(RecordingEvents {
        fail: true,
        ..Default::default()
    }));
    drop(rx);
    let envelope = data_envelope(json!({"message": "lost", "timestamp": 1_700_000_000_403u64}));
    assert!(!failing.dispatch_envelope(&envelope, &tx).await);
}

#[tokio::test]
async fn duplicate_deliveries_are_dropped() {
    let server = mock_daemon(b"").await;
    let rig = rig(&server);
    let (tx, _rx) = channel_pair();
    let envelope = data_envelope(json!({"message": "hi", "timestamp": 1_700_000_000_404u64}));
    rig.channel.dispatch_envelope(&envelope, &tx).await;
    rig.channel.dispatch_envelope(&envelope, &tx).await;
    assert_eq!(rig.events.inbound().len(), 1);
}

#[tokio::test]
async fn a_voice_note_is_stored_fetched_and_transcribed() {
    let server = mock_daemon(b"voice-bytes").await;
    let rig = rig(&server);
    let (tx, _rx) = channel_pair();
    let envelope = data_envelope(json!({
        "timestamp": 1_700_000_000_405u64,
        "attachments": [attachment("att-1", "audio/mp4", true)],
    }));
    assert!(rig.channel.dispatch_envelope(&envelope, &tx).await);
    let inbound = rig.events.inbound();
    let Some(ContentPart::VoiceNote { voice_note }) = inbound[0].parts.first() else {
        panic!("expected a voice note: {:?}", inbound[0].parts);
    };
    assert_eq!(voice_note.codec.as_deref(), Some("m4a_aac"));
    let transcript = voice_note.transcript.as_ref().unwrap();
    assert_eq!(transcript.text, "hello");
    assert_eq!(
        calls(&server, "getAttachment").await[0],
        json!({"id": "att-1", "account": ACCOUNT})
    );
    assert_eq!(rig.voice.stored.lock().unwrap().len(), 1);
    assert_eq!(inbound[0].media.len(), 1);
}

#[tokio::test]
async fn an_image_with_a_caption_becomes_typed_media() {
    let server = mock_daemon(b"png-bytes").await;
    let rig = rig(&server);
    let (tx, _rx) = channel_pair();
    let envelope = data_envelope(json!({
        "message": "look",
        "timestamp": 1_700_000_000_406u64,
        "attachments": [attachment("att-2", "image/png", false)],
    }));
    rig.channel.dispatch_envelope(&envelope, &tx).await;
    let inbound = rig.events.inbound();
    assert!(matches!(inbound[0].parts[0], ContentPart::Text { .. }));
    assert!(matches!(inbound[0].parts[1], ContentPart::Image { .. }));
    let stored = rig.media.stored.lock().unwrap();
    assert_eq!(stored[0].bytes, b"png-bytes");
    assert_eq!(
        stored[0].metadata.dedupe_key.as_deref(),
        Some("signal:sig_1700000000406:att-2")
    );
}

#[tokio::test]
async fn unusable_attachments_are_skipped_but_the_text_survives() {
    let server = mock_daemon(b"x").await;
    let rig = rig(&server);
    let (tx, _rx) = channel_pair();
    let envelope = data_envelope(json!({
        "message": "caption",
        "timestamp": 1_700_000_000_407u64,
        "attachments": [
            {"contentType": "image/png"},
            {"id": "big", "contentType": "image/png", "size": 999_999_999u64},
            {"id": "exe", "contentType": "application/x-msdownload", "size": 3},
        ],
    }));
    rig.channel.dispatch_envelope(&envelope, &tx).await;
    let inbound = rig.events.inbound();
    assert_eq!(inbound[0].parts.len(), 1);
    assert!(rig.media.stored.lock().unwrap().is_empty());

    // Without any storage the attachments are never fetched.
    let quiet = mock_daemon(b"x").await;
    let bare = channel_for(&quiet).with_events(Arc::new(RecordingEvents::delivering()));
    let envelope = data_envelope(json!({
        "message": "caption",
        "timestamp": 1_700_000_000_408u64,
        "attachments": [attachment("a", "image/png", false)],
    }));
    bare.dispatch_envelope(&envelope, &tx).await;
    assert!(calls(&quiet, "getAttachment").await.is_empty());
}

#[tokio::test]
async fn attachment_fetch_failures_and_oversize_payloads_are_dropped() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::path("/api/v1/rpc"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({"result": {}})))
        .mount(&server)
        .await;
    let rig = rig(&server);
    assert!(rig.channel.fetch_attachment("a", 10).await.is_none());

    let big = mock_daemon(&[7u8; 64]).await;
    let rig = super::super::fixtures::rig(&big);
    assert!(rig.channel.fetch_attachment("a", 10).await.is_none());
    assert_eq!(
        rig.channel.fetch_attachment("a", 100).await.unwrap().len(),
        64
    );
}

#[tokio::test]
async fn attachment_only_messages_are_dropped_when_attachments_are_ignored() {
    let server = mock_daemon(b"x").await;
    let events = Arc::new(RecordingEvents::delivering());
    let channel = SignalChannel::with_http_client(
        server.uri(),
        ACCOUNT.into(),
        None,
        vec!["*".into()],
        true,
        false,
        reqwest::Client::new(),
    )
    .with_events(events.clone());
    let (tx, _rx) = channel_pair();
    let envelope = data_envelope(json!({
        "timestamp": 1_700_000_000_409u64,
        "attachments": [attachment("a", "image/png", false)],
    }));
    channel.dispatch_envelope(&envelope, &tx).await;
    assert!(events.inbound().is_empty());
}

#[tokio::test]
async fn messages_without_content_or_from_strangers_publish_nothing() {
    let server = mock_daemon(b"").await;
    let events = Arc::new(RecordingEvents::delivering());
    let channel = SignalChannel::with_http_client(
        server.uri(),
        ACCOUNT.into(),
        Some("g1".into()),
        vec![PEER.into()],
        false,
        true,
        reqwest::Client::new(),
    )
    .with_events(events.clone());
    let (tx, _rx) = channel_pair();
    // Empty body.
    let empty = data_envelope(json!({"timestamp": 1u64}));
    channel.dispatch_envelope(&empty, &tx).await;
    // Wrong group.
    let wrong_group = data_envelope(json!({
        "message": "x", "timestamp": 2u64, "groupInfo": {"groupId": "other"},
    }));
    channel.dispatch_envelope(&wrong_group, &tx).await;
    // Unknown sender.
    let mut stranger = data_envelope(json!({
        "message": "x", "timestamp": 3u64, "groupInfo": {"groupId": "g1"},
    }));
    stranger.source_number = Some("+19990000".into());
    channel.dispatch_envelope(&stranger, &tx).await;
    // Story.
    let mut story = data_envelope(json!({
        "message": "x", "timestamp": 4u64, "groupInfo": {"groupId": "g1"},
    }));
    story.story_message = Some(json!({}));
    channel.dispatch_envelope(&story, &tx).await;
    // No data message at all.
    channel.dispatch_envelope(&Envelope::default(), &tx).await;
    assert!(events.inbound().is_empty() && events.events().is_empty());

    // The right group works and is addressed as a group.
    let ok = data_envelope(json!({
        "message": "hello", "timestamp": 5u64, "groupInfo": {"groupId": "g1"},
    }));
    channel.dispatch_envelope(&ok, &tx).await;
    let inbound = events.inbound();
    assert_eq!(inbound[0].conversation.id, "group:g1");
    assert_eq!(inbound[0].conversation.kind, ConversationKind::Group);
}

#[tokio::test]
async fn reactions_become_added_and_removed_events_once() {
    let server = mock_daemon(b"").await;
    let rig = rig(&server);
    let (tx, _rx) = channel_pair();
    let added = data_envelope(json!({
        "timestamp": 1_700_000_000_600u64,
        "reaction": {"emoji": "👍", "targetSentTimestamp": 1_700_000_000_123u64, "isRemove": false},
    }));
    rig.channel.dispatch_envelope(&added, &tx).await;
    rig.channel.dispatch_envelope(&added, &tx).await;
    let removed = data_envelope(json!({
        "timestamp": 1_700_000_000_601u64,
        "reaction": {"emoji": "👍", "targetSentTimestamp": 1_700_000_000_123u64, "isRemove": true},
    }));
    rig.channel.dispatch_envelope(&removed, &tx).await;
    let ignored = data_envelope(json!({
        "timestamp": 1_700_000_000_602u64,
        "reaction": {"emoji": ""},
    }));
    rig.channel.dispatch_envelope(&ignored, &tx).await;
    let events = rig.events.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0].event,
        ChannelEventKind::ReactionAdded { message_id, reaction, .. }
            if message_id == "sig_1700000000123" && reaction.value == "👍"
    ));
    assert!(matches!(
        &events[1].event,
        ChannelEventKind::ReactionRemoved { .. }
    ));
}

#[tokio::test]
async fn remote_deletes_become_message_deleted_events() {
    let server = mock_daemon(b"").await;
    let rig = rig(&server);
    let (tx, _rx) = channel_pair();
    let deleted = data_envelope(json!({
        "timestamp": 1_700_000_000_700u64,
        "remoteDelete": {"timestamp": 1_700_000_000_123u64},
    }));
    rig.channel.dispatch_envelope(&deleted, &tx).await;
    rig.channel.dispatch_envelope(&deleted, &tx).await;
    let missing = data_envelope(json!({"timestamp": 1_700_000_000_701u64, "remoteDelete": {}}));
    rig.channel.dispatch_envelope(&missing, &tx).await;
    let events = rig.events.events();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].event,
        ChannelEventKind::MessageDeleted { provider_message_id, .. }
            if provider_message_id.as_deref() == Some("1700000000123")
    ));
}

#[tokio::test]
async fn receipts_become_read_and_delivered_events() {
    let server = mock_daemon(b"").await;
    let rig = rig(&server);
    let (tx, _rx) = channel_pair();
    let receipt = |flag: &str| -> Envelope {
        let mut message =
            json!({"when": 1_700_000_000_800u64, "timestamps": [1_700_000_000_123u64]});
        message[flag] = true.into();
        serde_json::from_value(json!({"sourceNumber": PEER, "receiptMessage": message})).unwrap()
    };
    rig.channel.dispatch_envelope(&receipt("isRead"), &tx).await;
    rig.channel
        .dispatch_envelope(&receipt("isDelivery"), &tx)
        .await;
    rig.channel
        .dispatch_envelope(&receipt("isViewed"), &tx)
        .await;
    let mut stranger = receipt("isRead");
    stranger.source_number = None;
    stranger.source = None;
    rig.channel.dispatch_envelope(&stranger, &tx).await;
    let events = rig.events.events();
    assert_eq!(events.len(), 2);
    assert!(
        matches!(&events[0].event, ChannelEventKind::ReadReceipt { message_id, .. } if message_id == "sig_1700000000123")
    );
    assert!(matches!(
        &events[1].event,
        ChannelEventKind::DeliveredReceipt { .. }
    ));
}

#[tokio::test]
async fn rejected_typed_events_stop_reaction_receipt_and_delete_dispatch() {
    let server = mock_daemon(b"").await;
    let rejecting = RejectingTypedSink;
    let reaction = data_envelope(json!({
        "timestamp": 1_700_000_000_900u64,
        "reaction": {"emoji": "👍", "targetSentTimestamp": 1_700_000_000_123u64},
    }));
    assert!(
        !channel_for(&server)
            .dispatch_typed_envelope(&reaction, &rejecting)
            .await,
        "a rejected reaction must propagate to listener supervision"
    );

    let receipt: Envelope = serde_json::from_value(json!({
        "sourceNumber": PEER,
        "receiptMessage": {
            "when": 1_700_000_000_901u64,
            "timestamps": [1_700_000_000_123u64],
            "isRead": true,
        },
    }))
    .unwrap();
    assert!(
        !channel_for(&server)
            .dispatch_typed_envelope(&receipt, &rejecting)
            .await,
        "a rejected receipt must propagate to listener supervision"
    );

    let deleted = data_envelope(json!({
        "timestamp": 1_700_000_000_902u64,
        "remoteDelete": {"timestamp": 1_700_000_000_123u64},
    }));
    assert!(
        !channel_for(&server)
            .dispatch_typed_envelope(&deleted, &rejecting)
            .await,
        "a rejected delete must propagate to listener supervision"
    );
}
