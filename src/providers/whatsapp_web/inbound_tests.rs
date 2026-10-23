use super::*;
use crate::channel::ChannelAdapter as _;
use crate::providers::whatsapp_shared::fakes::{
    CollectingSink, MemoryMedia, MemoryVoices, png_bytes,
};
use crate::providers::whatsapp_web::client::OutboundClient;
use crate::providers::whatsapp_web::fakes::{RecordingClient, base_state};
use parking_lot::Mutex;
use std::sync::Arc;
use tinychannels_bus::{ChannelIngress, ContentPart};
use wacore::types::events::{InboundMessage, Receipt};
use wacore::types::message::{MessageInfo, MessageSource};
use wacore::types::presence::ReceiptType;
use whatsapp_rust::Jid;
use whatsapp_rust::buffa::MessageField;

fn info(id: &str, chat: &str, sender: &str) -> MessageInfo {
    MessageInfo {
        source: MessageSource {
            chat: chat.parse().unwrap(),
            sender: sender.parse().unwrap(),
            is_group: chat.ends_with("@g.us"),
            ..Default::default()
        },
        id: id.to_owned(),
        timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        push_name: "Ada".into(),
        ..Default::default()
    }
}

fn dm(id: &str, message: wa::Message) -> InboundMessage {
    InboundMessage::builder()
        .message(Arc::new(message))
        .info(Arc::new(info(
            id,
            "15550001111@s.whatsapp.net",
            "15550001111@s.whatsapp.net",
        )))
        .build()
}

fn text(body: &str) -> wa::Message {
    wa::Message {
        conversation: Some(body.into()),
        ..Default::default()
    }
}

fn image(path: &str, caption: Option<&str>) -> wa::Message {
    wa::Message {
        image_message: MessageField::some(wa::message::ImageMessage {
            direct_path: Some(path.into()),
            mimetype: Some("image/png".into()),
            caption: caption.map(str::to_owned),
            ..Default::default()
        }),
        ..Default::default()
    }
}

struct Rig {
    state: InboundState,
    sink: Arc<CollectingSink>,
    client: Arc<RecordingClient>,
    media: Arc<MemoryMedia>,
    voices: Arc<MemoryVoices>,
}

fn rig() -> Rig {
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let sink = Arc::new(CollectingSink::new());
    let client = Arc::new(RecordingClient::default());
    let media = Arc::new(MemoryMedia::default());
    let voices = Arc::new(MemoryVoices::default());
    let mut state = base_state(tx, &["*"]);
    state.events = Some(sink.clone());
    state.media_repository = Some(media.clone());
    state.voice_media_store = Some(voices.clone());
    state.client = Arc::new(Mutex::new(Some(client.clone() as Arc<dyn OutboundClient>)));
    Rig {
        state,
        sink,
        client,
        media,
        voices,
    }
}

async fn publish(rig: &Rig, inbound: &InboundMessage) -> Result<bool, WaFailure> {
    rig.state
        .publish_typed(
            inbound,
            "+15550001111",
            "+15550001111",
            Some("Ada".into()),
            false,
        )
        .await
}

#[tokio::test]
async fn typed_listener_retries_rejected_ingress_and_dedupes_only_after_acceptance() {
    let mut rig = rig();
    rig.state.events = None;
    let (typed_tx, mut deliveries) = tokio::sync::mpsc::channel(2);
    rig.state.typed_tx = Some(typed_tx);
    let inbound = dm("web-typed-1", text("hello from WhatsApp Web"));
    let sink = RetryOnceSink::default();

    let state = rig.state.clone();
    let first_inbound = inbound.clone();
    let first = tokio::spawn(async move {
        state
            .publish_typed(
                &first_inbound,
                "+15550001111",
                "+15550001111",
                Some("Ada".into()),
                false,
            )
            .await
    });
    let state = rig.state.clone();
    let duplicate_inbound = inbound.clone();
    let duplicate = tokio::spawn(async move {
        state
            .publish_typed(
                &duplicate_inbound,
                "+15550001111",
                "+15550001111",
                Some("Ada".into()),
                false,
            )
            .await
    });
    let delivery = deliveries.recv().await.expect("typed ingress was queued");
    let tinychannels_bus::ChannelIngress::Inbound(envelope) = &delivery.ingress else {
        panic!("text message must retain its inbound envelope");
    };
    assert_eq!(envelope.message_id, "web-typed-1");
    assert_eq!(envelope.channel.id, "whatsapp");
    assert_eq!(envelope.conversation.id, "+15550001111");
    assert_eq!(envelope.sender.name.as_deref(), Some("Ada"));
    assert_eq!(envelope.text, "hello from WhatsApp Web");
    let (_cancel, mut cancellation) = tokio::sync::watch::channel(false);
    super::super::deliver_typed_ingress(
        &sink,
        &delivery.ingress,
        &tokio::sync::Notify::new(),
        &mut cancellation,
    )
    .await
    .expect("retryable sink rejection should retry the pending ingress");
    delivery.accepted.send(Ok(())).unwrap();
    assert!(first.await.unwrap().unwrap());
    assert!(duplicate.await.unwrap().unwrap());
    assert_eq!(sink.attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    let accepted = sink.accepted.lock().await;
    assert_eq!(accepted.len(), 1, "one provider callback is accepted once");
    let tinychannels_bus::ChannelIngress::Inbound(accepted_envelope) = &accepted[0] else {
        panic!("accepted callback must preserve the inbound envelope");
    };
    assert_eq!(accepted_envelope.message_id, "web-typed-1");
    assert_eq!(accepted_envelope.text, "hello from WhatsApp Web");
    drop(accepted);

    assert!(
        rig.state
            .publish_typed(
                &inbound,
                "+15550001111",
                "+15550001111",
                Some("Ada".into()),
                false
            )
            .await
            .unwrap()
    );
    assert!(
        deliveries.try_recv().is_err(),
        "accepted duplicate is suppressed"
    );
}

#[tokio::test]
async fn missing_provider_message_id_uses_a_stable_deduplication_identity() {
    let mut rig = rig();
    rig.state.events = None;
    let (typed_tx, mut deliveries) = tokio::sync::mpsc::channel(2);
    rig.state.typed_tx = Some(typed_tx);
    let inbound = dm("", text("message without provider id"));
    let state = rig.state.clone();
    let first_inbound = inbound.clone();
    let first = tokio::spawn(async move {
        state
            .publish_typed(
                &first_inbound,
                "+15550001111",
                "+15550001111",
                Some("Ada".into()),
                false,
            )
            .await
    });
    let accepted = deliveries.recv().await.expect("typed message is queued");
    let tinychannels_bus::ChannelIngress::Inbound(envelope) = &accepted.ingress else {
        panic!("typed message must retain its envelope");
    };
    assert!(envelope.message_id.starts_with("wa-web-"));
    accepted.accepted.send(Ok(())).unwrap();
    assert!(first.await.unwrap().unwrap());
    assert!(publish(&rig, &inbound).await.unwrap());
    assert!(
        deliveries.try_recv().is_err(),
        "redelivery with a missing provider id must reuse its stable fallback identity"
    );
}

#[tokio::test]
async fn cancelled_typed_delivery_is_not_committed_and_provider_redelivery_succeeds() {
    let mut rig = rig();
    rig.state.events = None;
    let (typed_tx, mut deliveries) = tokio::sync::mpsc::channel(2);
    rig.state.typed_tx = Some(typed_tx);
    let inbound = dm("web-cancel-retry", text("retry after stop"));
    let state = rig.state.clone();
    let inbound_for_first = inbound.clone();
    let first = tokio::spawn(async move {
        state
            .publish_typed(
                &inbound_for_first,
                "+15550001111",
                "+15550001111",
                Some("Ada".into()),
                false,
            )
            .await
    });
    let pending = deliveries.recv().await.expect("provider callback queued");
    let channel = WhatsAppWebChannel::new("test-session.db".into(), None, None, vec!["*".into()]);
    let mut cancellation = channel.listener_cancel.subscribe();
    let sink = Arc::new(BlockingSink::default());
    let entered = sink.entered.clone();
    let shutdown = tokio::sync::Notify::new();
    let ingress = pending.ingress.clone();
    let delivery = tokio::spawn(async move {
        super::super::deliver_typed_ingress(sink.as_ref(), &ingress, &shutdown, &mut cancellation)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .expect("typed sink delivery must enter its blocked push");
    // No success acknowledgement can escape from a blocked host sink.
    channel.stop().await.unwrap();
    assert!(matches!(
        delivery.await.unwrap(),
        Err(super::super::TypedDeliveryFailure::Cancelled)
    ));
    pending
        .accepted
        .send(Err(crate::channel::ChannelSendError {
            kind: crate::channel::SendErrorKind::Transient,
            message: "listener stopped before acceptance".into(),
            retryable: true,
            ..Default::default()
        }))
        .unwrap();
    assert!(first.await.unwrap().is_err());

    let state = rig.state.clone();
    let inbound_for_retry = inbound.clone();
    let retry = tokio::spawn(async move {
        state
            .publish_typed(
                &inbound_for_retry,
                "+15550001111",
                "+15550001111",
                Some("Ada".into()),
                false,
            )
            .await
    });
    let redelivery = deliveries
        .recv()
        .await
        .expect("unaccepted event is redelivered");
    redelivery.accepted.send(Ok(())).unwrap();
    assert!(retry.await.unwrap().unwrap());
}

#[derive(Default)]
struct RetryOnceSink {
    attempts: std::sync::atomic::AtomicUsize,
    accepted: tokio::sync::Mutex<Vec<tinychannels_bus::ChannelIngress>>,
}

#[derive(Default)]
struct BlockingSink {
    entered: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for BlockingSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        _ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for RetryOnceSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), crate::channel::ChannelSendError> {
        if self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            return Err(crate::channel::ChannelSendError {
                kind: crate::channel::SendErrorKind::Transient,
                message: "temporary sink outage".into(),
                retryable: true,
                ..Default::default()
            });
        }
        self.accepted.lock().await.push(ingress);
        Ok(())
    }
}

fn envelopes(rig: &Rig) -> Vec<ChannelInboundEnvelope> {
    rig.sink
        .ingress
        .lock()
        .iter()
        .filter_map(|ingress| match ingress {
            ChannelIngress::Inbound(envelope) => Some(envelope.clone()),
            ChannelIngress::Event(_) => None,
        })
        .collect()
}

fn events(rig: &Rig) -> Vec<ChannelEvent> {
    rig.sink
        .ingress
        .lock()
        .iter()
        .filter_map(|ingress| match ingress {
            ChannelIngress::Event(event) => Some(event.clone()),
            ChannelIngress::Inbound(_) => None,
        })
        .collect()
}

// --- parsing -----------------------------------------------------------------

#[test]
fn parse_reads_text_quotes_and_unwraps_ephemeral_wrappers() {
    assert_eq!(parse(&text("hello")).text, "hello");
    let quoted = wa::Message {
        extended_text_message: MessageField::some(wa::message::ExtendedTextMessage {
            text: Some("reply".into()),
            context_info: MessageField::some(wa::ContextInfo {
                stanza_id: Some("3EB0QUOTED".into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let parsed = parse(&quoted);
    assert_eq!(parsed.text, "reply");
    assert_eq!(parsed.reply_to.as_deref(), Some("3EB0QUOTED"));

    let wrapped = wa::Message {
        ephemeral_message: MessageField::some(wa::message::FutureProofMessage {
            message: MessageField::some(text("inside")),
        }),
        ..Default::default()
    };
    assert_eq!(parse(&wrapped).text, "inside");
    let blank = wa::Message::default();
    let empty = parse(&blank);
    assert!(empty.text.is_empty() && empty.attachments.is_empty() && empty.location.is_none());
}

#[test]
fn parse_finds_every_attachment_kind_with_its_facts() {
    let message = wa::Message {
        image_message: MessageField::some(wa::message::ImageMessage {
            caption: Some("img".into()),
            mimetype: Some("image/png".into()),
            ..Default::default()
        }),
        video_message: MessageField::some(wa::message::VideoMessage {
            seconds: Some(4),
            mimetype: Some("video/mp4".into()),
            caption: Some("vid".into()),
            ..Default::default()
        }),
        audio_message: MessageField::some(wa::message::AudioMessage {
            seconds: Some(2),
            mimetype: Some("audio/ogg; codecs=opus".into()),
            ptt: Some(true),
            context_info: MessageField::some(wa::ContextInfo {
                stanza_id: Some("q".into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        document_message: MessageField::some(wa::message::DocumentMessage {
            file_name: Some("a.pdf".into()),
            mimetype: Some("application/pdf".into()),
            caption: Some("doc".into()),
            ..Default::default()
        }),
        sticker_message: MessageField::some(wa::message::StickerMessage {
            mimetype: Some("image/webp".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let parsed = parse(&message);
    assert_eq!(parsed.attachments.len(), 5);
    let facts: Vec<_> = parsed
        .attachments
        .iter()
        .map(|a| {
            (
                a.kind().field(),
                a.mime(),
                a.caption(),
                a.file_name(),
                a.duration_ms(),
            )
        })
        .collect();
    assert_eq!(
        facts[0],
        ("image", "image/png".into(), Some("img".into()), None, None)
    );
    assert_eq!(facts[1].4, Some(4000));
    assert_eq!(
        facts[2],
        (
            "audio",
            "audio/ogg; codecs=opus".into(),
            None,
            None,
            Some(2000)
        )
    );
    assert_eq!(facts[3].3.as_deref(), Some("a.pdf"));
    assert_eq!(facts[4].0, "sticker");
    assert_eq!(parsed.reply_to.as_deref(), Some("q"));
}

#[test]
fn parse_reads_locations_contacts_and_reactions() {
    let location = |lat: f64, lon: f64| wa::Message {
        location_message: MessageField::some(wa::message::LocationMessage {
            degrees_latitude: Some(lat),
            degrees_longitude: Some(lon),
            name: Some("Home".into()),
            address: Some(" ".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        parse(&location(1.0, 2.0)).location,
        Some((1.0, 2.0, Some("Home".into()), None))
    );
    assert!(parse(&location(95.0, 2.0)).location.is_none());
    assert!(parse(&location(1.0, 200.0)).location.is_none());

    let contacts = wa::Message {
        contact_message: MessageField::some(wa::message::ContactMessage {
            display_name: Some("Ada".into()),
            vcard: Some("BEGIN:VCARD\nTEL;type=CELL:+4412\nEND:VCARD".into()),
            ..Default::default()
        }),
        contacts_array_message: MessageField::some(wa::message::ContactsArrayMessage {
            contacts: vec![wa::message::ContactMessage {
                display_name: Some("Bob".into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let parsed = parse(&contacts);
    assert_eq!(parsed.contacts.len(), 2);
    assert_eq!(parsed.contacts[0].display_name, "Ada");
    assert_eq!(parsed.contacts[0].phones[0].value, "+4412");
    assert_eq!(parsed.contacts[1].display_name, "Bob");

    let reaction = |text: Option<&str>, id: Option<&str>| wa::Message {
        reaction_message: MessageField::some(wa::message::ReactionMessage {
            key: MessageField::some(wa::MessageKey {
                id: id.map(str::to_owned),
                ..Default::default()
            }),
            text: text.map(str::to_owned),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        parse(&reaction(Some("👍"), Some("3EB0X"))).reaction,
        Some(("3EB0X".into(), "👍".into()))
    );
    assert_eq!(
        parse(&reaction(None, Some("3EB0X"))).reaction,
        Some(("3EB0X".into(), String::new()))
    );
    assert!(parse(&reaction(Some("x"), None)).reaction.is_none());
}

// --- typed delivery ----------------------------------------------------------

#[tokio::test]
async fn a_text_message_is_published_once_as_a_typed_envelope() {
    let rig = rig();
    let inbound = dm("3EB0TEXT", text("hello"));
    assert!(publish(&rig, &inbound).await.unwrap());
    // The SDK may redeliver; the second copy is acknowledged but not republished.
    assert!(publish(&rig, &inbound).await.unwrap());
    let sent = envelopes(&rig);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].message_id, "3EB0TEXT");
    assert_eq!(sent[0].conversation.id, "+15550001111");
    assert_eq!(
        sent[0].conversation.kind,
        tinychannels_bus::ConversationKind::Dm
    );
    assert_eq!(sent[0].sender.name.as_deref(), Some("Ada"));
    assert_eq!(sent[0].timestamp_unix_ms, Some(1_700_000_000_000));
    assert_eq!(sent[0].text, "hello");
    assert_eq!(
        sent[0].raw.as_ref().unwrap()["providerMessageId"],
        "3EB0TEXT"
    );
}

#[tokio::test]
async fn the_legacy_path_is_used_when_typed_ingress_is_unavailable() {
    let rig = rig();
    // No message id to key on.
    assert!(!publish(&rig, &dm("", text("x"))).await.unwrap());
    // A host that cannot take typed ingress asks for the legacy path and the
    // id is not remembered.
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let mut legacy_state = base_state(tx, &["*"]);
    let declining = Arc::new(CollectingSink {
        accept: false,
        ..CollectingSink::new()
    });
    legacy_state.events = Some(declining);
    let inbound = dm("3EB0LEGACY", text("x"));
    assert!(
        !legacy_state
            .publish_typed(&inbound, "+1", "+1", None, false)
            .await
            .unwrap()
    );
    assert!(!legacy_state.seen.lock().contains("3EB0LEGACY"));
    // No sink at all.
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let bare = base_state(tx, &["*"]);
    assert!(
        !bare
            .publish_typed(&inbound, "+1", "+1", None, false)
            .await
            .unwrap()
    );
    // A failing sink is an uncertain delivery.
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let mut failing = base_state(tx, &["*"]);
    failing.events = Some(Arc::new(CollectingSink {
        fail: true,
        ..CollectingSink::new()
    }));
    assert!(matches!(
        failing
            .publish_typed(&inbound, "+1", "+1", None, false)
            .await,
        Err(WaFailure::Uncertain(_))
    ));
}

#[tokio::test]
async fn group_messages_keep_the_group_conversation_kind() {
    let rig = rig();
    let inbound = InboundMessage::builder()
        .message(Arc::new(text("hi all")))
        .info(Arc::new(info(
            "3EB0GROUP",
            "987@g.us",
            "15550001111@s.whatsapp.net",
        )))
        .build();
    assert!(
        rig.state
            .publish_typed(&inbound, "987@g.us", "+15550001111", None, true)
            .await
            .unwrap()
    );
    let sent = envelopes(&rig);
    assert_eq!(
        sent[0].conversation.kind,
        tinychannels_bus::ConversationKind::Group
    );
    assert_eq!(sent[0].conversation.id, "987@g.us");
}

#[tokio::test]
async fn reactions_are_events_and_empty_messages_publish_nothing() {
    let rig = rig();
    let reaction = |emoji: Option<&str>, id: &str| {
        dm(
            id,
            wa::Message {
                reaction_message: MessageField::some(wa::message::ReactionMessage {
                    key: MessageField::some(wa::MessageKey {
                        id: Some("3EB0TARGET".into()),
                        ..Default::default()
                    }),
                    text: emoji.map(str::to_owned),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
    };
    assert!(publish(&rig, &reaction(Some("🔥"), "r1")).await.unwrap());
    assert!(publish(&rig, &reaction(None, "r2")).await.unwrap());
    assert!(
        publish(&rig, &dm("e1", wa::Message::default()))
            .await
            .unwrap()
    );
    let sent = events(&rig);
    assert_eq!(sent.len(), 2);
    assert!(matches!(
        &sent[0].event,
        ChannelEventKind::ReactionAdded { message_id, reaction, .. } if message_id == "3EB0TARGET" && reaction.value == "🔥"
    ));
    assert!(matches!(
        &sent[1].event,
        ChannelEventKind::ReactionRemoved { .. }
    ));
    assert!(envelopes(&rig).is_empty());
}

#[tokio::test]
async fn attachments_are_downloaded_through_the_session_and_stored_by_the_host() {
    let rig = rig();
    rig.client
        .downloads
        .lock()
        .insert("/v/img1".into(), png_bytes());
    let inbound = dm("3EB0IMG", image("/v/img1", Some("look")));
    assert!(publish(&rig, &inbound).await.unwrap());
    let sent = envelopes(&rig);
    assert_eq!(sent[0].text, "look");
    assert!(
        matches!(&sent[0].parts[0], ContentPart::Image { caption: Some(c), .. } if c == "look")
    );
    assert_eq!(sent[0].media.len(), 1);
    assert_eq!(
        rig.media.stored.lock()[0].dedupe_key.as_deref(),
        Some("whatsapp:3EB0IMG:image")
    );
}

#[tokio::test]
async fn every_other_attachment_kind_becomes_its_content_part() {
    let rig = rig();
    {
        let mut downloads = rig.client.downloads.lock();
        downloads.insert("/v/vid".into(), b"0000ftypmp42".to_vec());
        downloads.insert("/v/doc".into(), b"%PDF-1.4 x".to_vec());
        downloads.insert("/v/stk".into(), b"RIFF----WEBPxxxx".to_vec());
        downloads.insert("/v/aud".into(), b"ID3-----".to_vec());
    }
    let message = wa::Message {
        video_message: MessageField::some(wa::message::VideoMessage {
            direct_path: Some("/v/vid".into()),
            mimetype: Some("video/mp4".into()),
            seconds: Some(3),
            ..Default::default()
        }),
        document_message: MessageField::some(wa::message::DocumentMessage {
            direct_path: Some("/v/doc".into()),
            mimetype: Some("application/pdf".into()),
            file_name: Some("a.pdf".into()),
            ..Default::default()
        }),
        sticker_message: MessageField::some(wa::message::StickerMessage {
            direct_path: Some("/v/stk".into()),
            mimetype: Some("image/webp".into()),
            ..Default::default()
        }),
        audio_message: MessageField::some(wa::message::AudioMessage {
            direct_path: Some("/v/aud".into()),
            mimetype: Some("audio/mpeg".into()),
            ptt: Some(false),
            ..Default::default()
        }),
        location_message: MessageField::some(wa::message::LocationMessage {
            degrees_latitude: Some(1.0),
            degrees_longitude: Some(2.0),
            ..Default::default()
        }),
        contact_message: MessageField::some(wa::message::ContactMessage {
            display_name: Some("Ada".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(publish(&rig, &dm("3EB0MANY", message)).await.unwrap());
    let parts = &envelopes(&rig)[0].parts;
    let kinds: Vec<_> = parts
        .iter()
        .map(|part| match part {
            ContentPart::Video { duration_ms, .. } => format!("video:{duration_ms:?}"),
            ContentPart::File { file_name, .. } => format!("file:{file_name:?}"),
            ContentPart::Sticker { .. } => "sticker".into(),
            ContentPart::Audio { .. } => "audio".into(),
            ContentPart::Location { .. } => "location".into(),
            ContentPart::Contact { display_name, .. } => format!("contact:{display_name}"),
            other => format!("{other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "video:Some(3000)",
            "audio",
            "file:Some(\"a.pdf\")",
            "sticker",
            "location",
            "contact:Ada"
        ]
    );
}

fn voice_message(path: &str, ptt: bool, waveform: Option<Vec<u8>>) -> wa::Message {
    wa::Message {
        audio_message: MessageField::some(wa::message::AudioMessage {
            direct_path: Some(path.into()),
            mimetype: Some("audio/ogg; codecs=opus".into()),
            seconds: Some(2),
            ptt: Some(ptt),
            waveform,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn push_to_talk_audio_is_a_voice_note_with_its_waveform() {
    let rig = rig();
    rig.client
        .downloads
        .lock()
        .insert("/v/ptt".into(), b"OggS-----".to_vec());
    let inbound = dm(
        "3EB0VOICE",
        voice_message("/v/ptt", true, Some(vec![0, 50, 100])),
    );
    assert!(publish(&rig, &inbound).await.unwrap());
    let ContentPart::VoiceNote { voice_note } = &envelopes(&rig)[0].parts[0] else {
        panic!("expected a voice note")
    };
    assert!(voice_note.push_to_talk);
    assert_eq!(voice_note.waveform, Some(vec![0.0, 0.5, 1.0]));
    assert_eq!(voice_note.duration_ms, Some(2000));
    assert!(voice_note.media.url.is_none());
    assert_eq!(rig.voices.stored.lock().len(), 1);
}

#[tokio::test]
async fn missing_storage_oversized_media_and_failed_downloads_follow_their_classes() {
    let rig = rig();
    // A voice note the host store refuses is skipped (permanent) ...
    rig.client
        .downloads
        .lock()
        .insert("/v/empty".into(), Vec::new());
    assert!(
        publish(&rig, &dm("v1", voice_message("/v/empty", true, None)))
            .await
            .unwrap()
    );
    assert!(envelopes(&rig).is_empty());
    // ... unless the host store itself is down (transient).
    rig.client
        .downloads
        .lock()
        .insert("/v/ptt".into(), b"OggS-----".to_vec());
    *rig.voices.omit_opaque_id.lock() = true;
    assert!(matches!(
        publish(&rig, &dm("v2", voice_message("/v/ptt", true, None))).await,
        Err(WaFailure::Uncertain(_))
    ));
    // A download failure is transient and the id is not remembered.
    assert!(matches!(
        publish(&rig, &dm("i1", image("/v/missing", None))).await,
        Err(WaFailure::Uncertain(_))
    ));
    assert!(!rig.state.seen.lock().contains("i1"));
    // A declared length over the limit is skipped without downloading.
    let big = wa::Message {
        image_message: MessageField::some(wa::message::ImageMessage {
            direct_path: Some("/v/big".into()),
            mimetype: Some("image/png".into()),
            file_length: Some(MAX_INBOUND_MEDIA_BYTES + 1),
            caption: Some("huge".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(publish(&rig, &dm("i2", big)).await.unwrap());
    assert_eq!(envelopes(&rig)[0].text, "huge");
    // An unsupported content type is skipped; a store outage is transient.
    rig.client
        .downloads
        .lock()
        .insert("/v/odd".into(), b"zzz".to_vec());
    let odd = wa::Message {
        image_message: MessageField::some(wa::message::ImageMessage {
            direct_path: Some("/v/odd".into()),
            mimetype: Some("application/x-unknown".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(publish(&rig, &dm("i3", odd)).await.unwrap());
    rig.client
        .downloads
        .lock()
        .insert("/v/ok".into(), png_bytes());
    *rig.media.fail_store.lock() = true;
    assert!(matches!(
        publish(&rig, &dm("i4", image("/v/ok", None))).await,
        Err(WaFailure::Uncertain(_))
    ));
}

#[tokio::test]
async fn attachments_without_ports_or_a_connected_session_are_handled() {
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let sink = Arc::new(CollectingSink::new());
    let mut no_ports = base_state(tx, &["*"]);
    no_ports.events = Some(sink.clone());
    assert!(
        no_ports
            .publish_typed(
                &dm("a1", image("/v/x", Some("words"))),
                "+1",
                "+1",
                None,
                false
            )
            .await
            .unwrap()
    );
    {
        let ingress = sink.ingress.lock();
        assert!(
            matches!(&ingress[0], ChannelIngress::Inbound(envelope) if envelope.text == "words")
        );
    }

    // Storage wired but the session is gone: transient.
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let mut disconnected = base_state(tx, &["*"]);
    disconnected.events = Some(sink);
    disconnected.media_repository = Some(Arc::new(MemoryMedia::default()));
    assert!(matches!(
        disconnected
            .publish_typed(&dm("a2", image("/v/x", None)), "+1", "+1", None, false)
            .await,
        Err(WaFailure::Uncertain(_))
    ));
    // A voice note with only the media repository wired is kept as audio.
    let rig = rig();
    let mut repo_only = rig.state.clone();
    repo_only.voice_media_store = None;
    rig.client
        .downloads
        .lock()
        .insert("/v/ptt".into(), b"OggS-----".to_vec());
    assert!(
        repo_only
            .publish_typed(
                &dm("a3", voice_message("/v/ptt", true, None)),
                "+1",
                "+1",
                None,
                false
            )
            .await
            .unwrap()
    );
    assert!(matches!(
        &envelopes(&rig)[0].parts[0],
        ContentPart::Audio { .. }
    ));
}

// --- receipts ----------------------------------------------------------------

fn receipt(kind: ReceiptType, from_me: bool, chat: &str) -> Receipt {
    Receipt::builder()
        .source(MessageSource {
            chat: chat.parse().unwrap(),
            sender: "15550001111@s.whatsapp.net".parse().unwrap(),
            is_from_me: from_me,
            ..Default::default()
        })
        .message_ids(vec!["3EB0A".to_owned(), "3EB0B".to_owned()])
        .timestamp(chrono::DateTime::from_timestamp(1_700_000_005, 0).unwrap())
        .r#type(kind)
        .offline(false)
        .build()
}

#[tokio::test]
async fn delivery_and_read_receipts_for_our_messages_become_events() {
    let rig = rig();
    rig.state
        .publish_receipt(&receipt(
            ReceiptType::Delivered,
            false,
            "15550001111@s.whatsapp.net",
        ))
        .await;
    rig.state
        .publish_receipt(&receipt(ReceiptType::Read, false, "987@g.us"))
        .await;
    // Our own receipts, other receipt kinds and a sink-less state do nothing.
    rig.state
        .publish_receipt(&receipt(
            ReceiptType::Read,
            true,
            "15550001111@s.whatsapp.net",
        ))
        .await;
    rig.state
        .publish_receipt(&receipt(
            ReceiptType::Played,
            false,
            "15550001111@s.whatsapp.net",
        ))
        .await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    base_state(tx, &["*"])
        .publish_receipt(&receipt(
            ReceiptType::Read,
            false,
            "15550001111@s.whatsapp.net",
        ))
        .await;
    let sent = events(&rig);
    assert_eq!(sent.len(), 4);
    assert!(matches!(
        &sent[0].event,
        ChannelEventKind::DeliveredReceipt { message_id, timestamp: Some(1_700_000_005_000), actor: Some(actor) }
            if message_id == "3EB0A" && actor.id == "+15550001111"
    ));
    assert_eq!(sent[0].conversation.as_ref().unwrap().id, "+15550001111");
    assert!(
        matches!(&sent[2].event, ChannelEventKind::ReadReceipt { message_id, .. } if message_id == "3EB0A")
    );
    let group = sent[2].conversation.as_ref().unwrap();
    assert_eq!(group.id, "987@g.us");
    assert_eq!(group.kind, tinychannels_bus::ConversationKind::Group);
    assert_eq!(sent[2].event_id, "whatsapp:receipt:3EB0A:read");
}

#[tokio::test]
async fn a_failing_sink_does_not_stop_receipt_delivery() {
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let mut state = base_state(tx, &["*"]);
    state.events = Some(Arc::new(CollectingSink {
        fail: true,
        ..CollectingSink::new()
    }));
    state
        .publish_receipt(&receipt(
            ReceiptType::Read,
            false,
            "15550001111@s.whatsapp.net",
        ))
        .await;
}

#[test]
fn jids_used_by_the_fixtures_parse() {
    assert!("15550001111@s.whatsapp.net".parse::<Jid>().is_ok());
}
