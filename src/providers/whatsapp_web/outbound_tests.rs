use super::*;
use crate::channel::ChannelAdapter;
use crate::providers::whatsapp_shared::fakes::{MemoryMedia, MemoryVoices, ogg_bytes, png_bytes};
use crate::providers::whatsapp_shared::vcard::ContactCard;
use crate::providers::whatsapp_web::fakes::{Call, RecordingClient};
use tinychannels_bus::{
    ChannelRef, ContactMethod, ContentPart, ConversationRef, MediaKind, MediaReference,
    OutboundContentPart, VoiceNote,
};

fn channel(allowed: &[&str]) -> (WhatsAppWebChannel, Arc<RecordingClient>) {
    let channel = WhatsAppWebChannel::new(
        "/tmp/test.db".into(),
        None,
        None,
        allowed.iter().map(|value| (*value).to_owned()).collect(),
    );
    let client = Arc::new(RecordingClient::linked("15559998888@s.whatsapp.net"));
    *channel.client.lock() = Some(client.clone() as Arc<dyn OutboundClient>);
    (channel, client)
}

fn message(parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "whatsapp".into(),
        conversation_id: "+15550001111".into(),
        channel_ref: Some(ChannelRef {
            id: "whatsapp".into(),
            account_id: Some("whatsapp".into()),
        }),
        conversation_ref: Some(ConversationRef {
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

fn reference(id: &str) -> MediaReference {
    MediaReference {
        opaque_id: Some(id.into()),
        ..Default::default()
    }
}

// --- builders ----------------------------------------------------------------

#[test]
fn quoted_text_uses_the_extended_text_message_and_plain_text_the_conversation_field() {
    let plain = text_message("hi", None);
    assert_eq!(plain.conversation.as_deref(), Some("hi"));
    assert!(plain.extended_text_message.text.is_none());
    let quoted = text_message("hi", Some(reply_context("3EB0Q", Some("1@s.whatsapp.net"))));
    assert!(quoted.conversation.is_none());
    assert_eq!(quoted.extended_text_message.text.as_deref(), Some("hi"));
    let context = &quoted.extended_text_message.context_info;
    assert_eq!(context.stanza_id.as_deref(), Some("3EB0Q"));
    assert_eq!(context.participant.as_deref(), Some("1@s.whatsapp.net"));
    assert!(reply_context("x", None).participant.is_none());
}

#[test]
fn location_and_contact_messages_carry_their_protocol_fields() {
    let location = location_message(
        1.5,
        -2.5,
        Some("Home"),
        None,
        Some(reply_context("q", None)),
    );
    let location = location.location_message.as_option().unwrap();
    assert_eq!(location.degrees_latitude, Some(1.5));
    assert_eq!(location.degrees_longitude, Some(-2.5));
    assert_eq!(location.name.as_deref(), Some("Home"));
    assert!(location.address.is_none());
    assert_eq!(location.context_info.stanza_id.as_deref(), Some("q"));

    let card = ContactCard {
        display_name: "Ada".into(),
        phones: vec![ContactMethod {
            value: "+44 20".into(),
            label: Some("work".into()),
        }],
        emails: vec![],
        organization: Some("Co".into()),
    };
    let contact = contact_message(&card, None);
    let contact = contact.contact_message.as_option().unwrap();
    assert_eq!(contact.display_name.as_deref(), Some("Ada"));
    let vcard = contact.vcard.as_deref().unwrap();
    assert!(vcard.contains("FN:Ada") && vcard.contains("waid=4420") && vcard.contains("ORG:Co"));
}

#[test]
fn media_builds_round_durations_up_to_whole_seconds() {
    use crate::providers::whatsapp_shared::plan::AttachmentKind;
    assert_eq!(
        media_build(
            AttachmentKind::Video,
            "video/mp4",
            None,
            Some("c".into()),
            Some(1001)
        ),
        MediaBuild::Video {
            caption: Some("c".into()),
            duration_seconds: Some(2)
        }
    );
    assert_eq!(
        media_build(AttachmentKind::Audio, "audio/mpeg", None, None, None),
        MediaBuild::Audio {
            mimetype: "audio/mpeg".into(),
            duration_seconds: None
        }
    );
    assert!(matches!(
        media_build(AttachmentKind::Image, "image/png", None, None, None),
        MediaBuild::Image { .. }
    ));
    assert!(matches!(
        media_build(
            AttachmentKind::Document,
            "application/pdf",
            Some("a".into()),
            None,
            None
        ),
        MediaBuild::Document { .. }
    ));
    assert_eq!(
        media_build(
            AttachmentKind::Sticker,
            "image/webp",
            None,
            None,
            Some(u64::MAX)
        ),
        MediaBuild::Sticker
    );
    let huge = media_build(
        AttachmentKind::Video,
        "video/mp4",
        None,
        None,
        Some(u64::MAX),
    );
    assert!(matches!(
        huge,
        MediaBuild::Video {
            duration_seconds: Some(u32::MAX),
            ..
        }
    ));
}

// --- admission ---------------------------------------------------------------

#[test]
fn the_outbound_policy_gates_addressing_before_any_client_call() {
    let (channel, _client) = channel(&["+15550001111"]);
    assert_eq!(
        channel.admitted_jid("+15550001111").unwrap().to_string(),
        "15550001111@s.whatsapp.net"
    );
    assert!(matches!(
        channel.admitted_jid("+19998887777"),
        Err(WaFailure::Rejected(_))
    ));
    assert!(matches!(
        channel.admitted_jid("1@g.us"),
        Err(WaFailure::Rejected(_))
    ));
    channel.allowed_groups.lock().insert("1@g.us".into());
    assert!(
        channel
            .admitted_jid("1@g.us")
            .unwrap()
            .to_string()
            .ends_with("@g.us")
    );
    let (open, _) = self::channel(&[]);
    assert!(matches!(
        open.admitted_jid("@@"),
        Err(WaFailure::BadFormat(_))
    ));
}

#[test]
fn a_disconnected_or_re_linked_session_refuses_operations() {
    let offline = WhatsAppWebChannel::new("/tmp/t.db".into(), None, None, vec![]);
    assert!(matches!(offline.live_client(), Err(WaFailure::Rejected(_))));
    let (channel, client) = channel(&[]);
    assert!(channel.live_client().is_ok());
    assert_eq!(
        ChannelAdapter::media_account_identity(&channel).as_deref(),
        Some("15559998888@s.whatsapp.net")
    );
    *client.account.lock() = Some("15557776666@s.whatsapp.net".parse().unwrap());
    assert!(matches!(channel.live_client(), Err(WaFailure::Rejected(_))));
    assert_eq!(ChannelAdapter::media_account_identity(&channel), None);
}

// --- delivery ------------------------------------------------------------------

#[tokio::test]
async fn text_goes_to_the_phone_jid_and_the_receipt_carries_the_session_ids() {
    let (channel, client) = channel(&["+15550001111"]);
    let outcome = channel
        .send_v2(&message(vec![text("hello")]))
        .await
        .unwrap();
    assert!(outcome.error.is_none());
    assert_eq!(
        client.texts(),
        [("15550001111@s.whatsapp.net".to_owned(), "hello".to_owned())]
    );
    let receipt = outcome.receipt;
    assert_eq!(receipt.platform_message_ids, ["3EB00001"]);
    assert_eq!(receipt.edit_token.as_deref(), Some("3EB00001"));
    assert_eq!(receipt.delete_token.as_deref(), Some("3EB00001"));
    assert_eq!(receipt.parts[0].edit_token.as_deref(), Some("3EB00001"));
    assert_eq!(
        receipt.channel.unwrap().account_id.as_deref(),
        Some("whatsapp")
    );
    assert_eq!(receipt.conversation.unwrap().id, "+15550001111");
}

#[tokio::test]
async fn replies_quote_the_known_author_or_the_dm_peer_and_only_on_the_first_message() {
    let (channel, client) = channel(&["+15550001111"]);
    let mut request = message(vec![text(&format!(
        "{}\n{}",
        "a".repeat(10_000),
        "b".repeat(10_000)
    ))]);
    request.reply_to_id = Some("3EB0SRC".into());
    channel
        .authors
        .lock()
        .insert("3EB0SRC", "15550001111@s.whatsapp.net");
    let outcome = channel.send_v2(&request).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids.len(), 2);
    let sent = client.messages();
    let context = &sent[0].extended_text_message.context_info;
    assert_eq!(context.stanza_id.as_deref(), Some("3EB0SRC"));
    assert_eq!(
        context.participant.as_deref(),
        Some("15550001111@s.whatsapp.net")
    );
    assert!(
        sent[1].conversation.is_some(),
        "the follow-on chunk is not quoted"
    );
    assert_eq!(
        outcome.receipt.parts[0].reply_to_id.as_deref(),
        Some("3EB0SRC")
    );
    assert_eq!(outcome.receipt.parts[1].reply_to_id, None);
    assert!(
        outcome.receipt.edit_token.is_none(),
        "a multi-message send is not editable as one"
    );

    // Unknown author: a DM quotes the peer, a group quotes without a participant.
    let (channel, client) = self::channel(&["+15550001111"]);
    let mut dm = message(vec![text("x")]);
    dm.reply_to_id = Some("3EB0UNKNOWN".into());
    channel.send_v2(&dm).await.unwrap();
    assert_eq!(
        client.messages()[0]
            .extended_text_message
            .context_info
            .participant
            .as_deref(),
        Some("15550001111@s.whatsapp.net")
    );
    channel.allowed_groups.lock().insert("9@g.us".into());
    let mut group = message(vec![text("x")]);
    group.conversation_id = "9@g.us".into();
    group.conversation_ref.as_mut().unwrap().id = "9@g.us".into();
    group.reply_to_id = Some("3EB0UNKNOWN".into());
    channel.send_v2(&group).await.unwrap();
    assert!(
        client.messages()[1]
            .extended_text_message
            .context_info
            .participant
            .is_none()
    );
}

#[tokio::test]
async fn every_content_kind_is_uploaded_or_built_and_sent_in_order() {
    let (channel, client) = channel(&["+15550001111"]);
    let media = Arc::new(MemoryMedia::default());
    let voices = Arc::new(MemoryVoices::default());
    voices.seed("v1", "audio/ogg", &ogg_bytes());
    let png = media.seed(MediaKind::Image, "image/png", None, &png_bytes());
    let mp4 = media.seed(MediaKind::Video, "video/mp4", None, b"0000ftypmp42");
    let mp3 = media.seed(MediaKind::Audio, "audio/mpeg", None, b"ID3-----");
    let pdf = media.seed(
        MediaKind::Document,
        "application/pdf",
        Some("a.pdf"),
        b"%PDF-1.4 x",
    );
    let webp = media.seed(MediaKind::Image, "image/webp", None, b"RIFF----WEBPxxxx");
    let channel = channel
        .with_media_repository(media)
        .with_voice_media_store(voices);
    let content = |part| OutboundContentPart::Content { part };
    let request = message(vec![
        content(ContentPart::Image {
            media: reference(&png),
            caption: Some("img".into()),
            alt_text: None,
        }),
        content(ContentPart::Video {
            media: reference(&mp4),
            caption: None,
            duration_ms: Some(2000),
            thumbnail: None,
        }),
        content(ContentPart::Audio {
            media: reference(&mp3),
            codec: None,
            duration_ms: None,
            title: None,
            transcript: None,
        }),
        content(ContentPart::File {
            media: reference(&pdf),
            file_name: None,
            caption: None,
        }),
        content(ContentPart::Sticker {
            media: reference(&webp),
            pack_id: None,
            sticker_id: None,
            emoji: None,
            alt_text: None,
        }),
        content(ContentPart::VoiceNote {
            voice_note: VoiceNote {
                media: reference("v1"),
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1500),
                waveform: Some(vec![0.5; 64]),
                push_to_talk: true,
                transcript: None,
            },
        }),
        content(ContentPart::Location {
            latitude: 1.0,
            longitude: 2.0,
            label: None,
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: None,
        }),
        content(ContentPart::Contact {
            display_name: "Ada".into(),
            phone_numbers: vec![],
            emails: vec![],
            organization: None,
        }),
    ]);
    let outcome = channel.send_v2(&request).await.unwrap();
    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.platform_message_ids.len(), 8);
    let builds: Vec<_> = client
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::Media { build, bytes } => Some((build, bytes)),
            _ => None,
        })
        .collect();
    assert_eq!(builds.len(), 6, "every attachment is uploaded exactly once");
    assert!(builds.iter().all(|(_, bytes)| *bytes > 0));
    assert!(matches!(builds[0].0, MediaBuild::Image { .. }));
    assert!(matches!(
        builds[5].0,
        MediaBuild::Voice {
            duration_seconds: Some(2),
            ..
        }
    ));
    let sent = client.messages();
    assert!(sent[0].image_message.as_option().is_some());
    assert_eq!(sent[1].video_message.seconds, Some(2));
    assert_eq!(sent[2].audio_message.ptt, None);
    assert_eq!(sent[3].document_message.file_name.as_deref(), Some("a.pdf"));
    assert!(sent[4].sticker_message.as_option().is_some());
    let note = &sent[5].audio_message;
    assert_eq!(note.ptt, Some(true));
    assert_eq!(note.waveform.as_ref().map(Vec::len), Some(64));
    assert_eq!(note.seconds, Some(2));
    assert!(sent[6].location_message.as_option().is_some());
    assert!(sent[7].contact_message.as_option().is_some());
    let kinds: Vec<_> = outcome.receipt.parts.iter().map(|part| part.kind).collect();
    use tinychannels_bus::channel::MessageReceiptPartKind as Kind;
    assert_eq!(
        kinds,
        [
            Kind::Media,
            Kind::Media,
            Kind::Voice,
            Kind::Media,
            Kind::Media,
            Kind::Voice,
            Kind::Unknown,
            Kind::Unknown
        ]
    );
    assert!(
        outcome
            .receipt
            .parts
            .iter()
            .all(|part| part.delete_token.is_some())
    );
    assert!(outcome.receipt.parts[0].edit_token.is_none());
}

#[tokio::test]
async fn partial_and_failed_deliveries_are_reported_honestly() {
    // Upload failure: nothing was sent, safe to retry.
    let media = Arc::new(MemoryMedia::default());
    let png = media.seed(MediaKind::Image, "image/png", None, &png_bytes());
    let (channel, client) = {
        let (channel, _) = channel(&["+15550001111"]);
        let client = Arc::new(RecordingClient {
            fail_media: true,
            ..RecordingClient::linked("15559998888@s.whatsapp.net")
        });
        *channel.client.lock() = Some(client.clone() as Arc<dyn OutboundClient>);
        (channel.with_media_repository(media.clone()), client)
    };
    let image = message(vec![OutboundContentPart::Content {
        part: ContentPart::Image {
            media: reference(&png),
            caption: None,
            alt_text: None,
        },
    }]);
    let error = channel.send_v2(&image).await.unwrap_err();
    assert!(matches!(error, WaFailure::Rejected(_)));
    assert!(client.calls().is_empty());

    // The first send fails outright.
    let (channel, _) = {
        let (channel, _) = self::channel(&["+15550001111"]);
        let client = Arc::new(RecordingClient {
            fail: true,
            ..RecordingClient::linked("15559998888@s.whatsapp.net")
        });
        *channel.client.lock() = Some(client.clone() as Arc<dyn OutboundClient>);
        (channel, client)
    };
    assert!(matches!(
        channel.send_v2(&message(vec![text("x")])).await,
        Err(WaFailure::Uncertain(_))
    ));

    // The second message fails after the first was delivered.
    let (channel, _) = {
        let (channel, _) = self::channel(&["+15550001111"]);
        let client = Arc::new(RecordingClient {
            fail_sends_after: Some(1),
            ..RecordingClient::linked("15559998888@s.whatsapp.net")
        });
        *channel.client.lock() = Some(client.clone() as Arc<dyn OutboundClient>);
        (channel, client)
    };
    let outcome = channel
        .send_v2(&message(vec![text("one"), text("two")]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids.len(), 1);
    let failure = outcome.error.unwrap();
    assert!(failure.sent_before_error && !failure.retryable);
}

#[tokio::test]
async fn unadmitted_or_unaddressable_sends_never_reach_the_session() {
    let (channel, client) = channel(&["+15550001111"]);
    let mut stranger = message(vec![text("x")]);
    stranger.conversation_id = "+19998887777".into();
    stranger.conversation_ref.as_mut().unwrap().id = "+19998887777".into();
    assert!(matches!(
        channel.send_v2(&stranger).await,
        Err(WaFailure::Rejected(_))
    ));
    let mut threaded = message(vec![text("x")]);
    threaded.thread_id = Some("t".into());
    assert!(matches!(
        channel.send_v2(&threaded).await,
        Err(WaFailure::Unsupported(_))
    ));
    let offline = WhatsAppWebChannel::new("/tmp/t.db".into(), None, None, vec![]);
    assert!(matches!(
        offline.send_v2(&message(vec![text("x")])).await,
        Err(WaFailure::Rejected(_))
    ));
    assert!(client.calls().is_empty());
}

// --- edit, delete, react, read ---------------------------------------------------

async fn sent_text(channel: &WhatsAppWebChannel) -> tinychannels_bus::channel::MessageReceipt {
    channel
        .send_v2(&message(vec![text("first")]))
        .await
        .unwrap()
        .receipt
}

fn edit_intent(body: &str) -> tinychannels_bus::ChannelOutboundIntent {
    tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "edit".into(),
        channel_id: "whatsapp".into(),
        conversation_id: "+15550001111".into(),
        channel_ref: Some(ChannelRef {
            id: "whatsapp".into(),
            account_id: Some("whatsapp".into()),
        }),
        conversation_ref: Some(ConversationRef {
            id: "+15550001111".into(),
            ..Default::default()
        }),
        payload: tinychannels_bus::OutboundPayload::Text { text: body.into() },
        ..Default::default()
    }
}

#[tokio::test]
async fn text_messages_can_be_edited_and_keep_their_identity() {
    let (channel, client) = channel(&["+15550001111"]);
    let receipt = sent_text(&channel).await;
    let edited = channel
        .edit_v2(&receipt, &edit_intent("fixed <tool>x</tool>"))
        .await
        .unwrap();
    assert_eq!(edited.platform_message_ids, receipt.platform_message_ids);
    let edit = client.calls().into_iter().find_map(|call| match call {
        Call::Edit {
            chat,
            original_id,
            content,
        } => Some((chat, original_id, content)),
        _ => None,
    });
    let (chat, original_id, content) = edit.unwrap();
    assert_eq!(chat, "15550001111@s.whatsapp.net");
    assert_eq!(original_id, "3EB00001");
    assert_eq!(
        content.conversation.as_deref().map(str::trim),
        Some("fixed")
    );
}

#[tokio::test]
async fn edits_are_refused_for_anything_but_a_single_text_message() {
    let (channel, client) = channel(&["+15550001111"]);
    let receipt = sent_text(&channel).await;
    let mut non_text = receipt.clone();
    non_text.parts[0].kind = tinychannels_bus::channel::MessageReceiptPartKind::Media;
    assert!(matches!(
        channel.edit_v2(&non_text, &edit_intent("x")).await,
        Err(WaFailure::Unsupported(_))
    ));
    let mut two = receipt.clone();
    two.parts.push(two.parts[0].clone());
    assert!(matches!(
        channel.edit_v2(&two, &edit_intent("x")).await,
        Err(WaFailure::Unsupported(_))
    ));
    let mut nowhere = receipt.clone();
    nowhere.conversation = None;
    assert!(matches!(
        channel.edit_v2(&nowhere, &edit_intent("x")).await,
        Err(WaFailure::Rejected(_))
    ));
    let mut poll = edit_intent("x");
    poll.payload = tinychannels_bus::OutboundPayload::Poll {
        question: "q".into(),
        options: vec!["a".into()],
    };
    assert!(matches!(
        channel.edit_v2(&receipt, &poll).await,
        Err(WaFailure::Unsupported(_))
    ));
    for body in ["   ", &"x".repeat(TEXT_LIMIT + 1)] {
        assert!(matches!(
            channel.edit_v2(&receipt, &edit_intent(body)).await,
            Err(WaFailure::Rejected(_))
        ));
    }
    assert!(
        !client
            .calls()
            .iter()
            .any(|call| matches!(call, Call::Edit { .. }))
    );
    // The session failing is an uncertain outcome.
    let failing = Arc::new(RecordingClient {
        fail: true,
        ..RecordingClient::linked("15559998888@s.whatsapp.net")
    });
    *channel.client.lock() = Some(failing as Arc<dyn OutboundClient>);
    assert!(matches!(
        channel.edit_v2(&receipt, &edit_intent("x")).await,
        Err(WaFailure::Uncertain(_))
    ));
}

#[tokio::test]
async fn sent_messages_can_be_unsent_for_everyone() {
    let (channel, client) = channel(&["+15550001111"]);
    let receipt = sent_text(&channel).await;
    channel.delete_v2(&receipt).await.unwrap();
    assert!(client.calls().iter().any(|call| matches!(
        call,
        Call::Revoke { chat, id } if chat == "15550001111@s.whatsapp.net" && id == "3EB00001"
    )));
    let mut nowhere = receipt.clone();
    nowhere.conversation = None;
    assert!(matches!(
        channel.delete_v2(&nowhere).await,
        Err(WaFailure::Rejected(_))
    ));
    let failing = Arc::new(RecordingClient {
        fail: true,
        ..RecordingClient::linked("15559998888@s.whatsapp.net")
    });
    *channel.client.lock() = Some(failing as Arc<dyn OutboundClient>);
    assert!(matches!(
        channel.delete_v2(&receipt).await,
        Err(WaFailure::Uncertain(_))
    ));
}

#[tokio::test]
async fn reactions_name_the_target_and_group_reactions_name_its_author() {
    let (channel, client) = channel(&["+15550001111"]);
    let conversation = ConversationRef {
        id: "+15550001111".into(),
        ..Default::default()
    };
    let thumbs = tinychannels_bus::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    channel
        .react_v2(&conversation, "3EB0T", &thumbs, false)
        .await
        .unwrap();
    channel
        .react_v2(&conversation, "3EB0T", &thumbs, true)
        .await
        .unwrap();
    channel.allowed_groups.lock().insert("9@g.us".into());
    channel
        .authors
        .lock()
        .insert("3EB0G", "15551112222@s.whatsapp.net");
    let group = ConversationRef {
        id: "9@g.us".into(),
        ..Default::default()
    };
    channel
        .react_v2(&group, "3EB0G", &thumbs, false)
        .await
        .unwrap();
    let reactions: Vec<_> = client
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::Reaction { chat, key, emoji } => Some((chat, key, emoji)),
            _ => None,
        })
        .collect();
    assert_eq!(reactions[0].2, "👍");
    assert_eq!(reactions[0].1.id.as_deref(), Some("3EB0T"));
    assert_eq!(reactions[0].1.from_me, Some(false));
    assert!(reactions[0].1.participant.is_none());
    assert_eq!(reactions[1].2, "", "an empty emoji removes the reaction");
    assert_eq!(
        reactions[2].1.participant.as_deref(),
        Some("15551112222@s.whatsapp.net")
    );
}

#[tokio::test]
async fn invalid_reactions_never_reach_the_session() {
    let (channel, client) = channel(&["+15550001111"]);
    let conversation = ConversationRef {
        id: "+15550001111".into(),
        ..Default::default()
    };
    let custom = tinychannels_bus::ChannelReaction {
        value: "x".into(),
        custom_emoji_id: Some("c".into()),
    };
    let blank = tinychannels_bus::ChannelReaction::default();
    let thumbs = tinychannels_bus::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    assert!(matches!(
        channel.react_v2(&conversation, "t", &custom, false).await,
        Err(WaFailure::Unsupported(_))
    ));
    assert!(matches!(
        channel.react_v2(&conversation, "t", &blank, false).await,
        Err(WaFailure::Rejected(_))
    ));
    assert!(matches!(
        channel.react_v2(&conversation, " ", &thumbs, false).await,
        Err(WaFailure::Rejected(_))
    ));
    assert!(client.calls().is_empty());
    let failing = Arc::new(RecordingClient {
        fail: true,
        ..RecordingClient::linked("15559998888@s.whatsapp.net")
    });
    *channel.client.lock() = Some(failing as Arc<dyn OutboundClient>);
    assert!(matches!(
        channel.react_v2(&conversation, "t", &thumbs, false).await,
        Err(WaFailure::Uncertain(_))
    ));
}

#[tokio::test]
async fn read_receipts_name_the_chat_and_the_group_author() {
    let (channel, client) = channel(&["+15550001111"]);
    let dm = ConversationRef {
        id: "+15550001111".into(),
        ..Default::default()
    };
    channel.read_v2(&dm, "3EB0A").await.unwrap();
    channel.allowed_groups.lock().insert("9@g.us".into());
    channel
        .authors
        .lock()
        .insert("3EB0B", "15551112222@s.whatsapp.net");
    let group = ConversationRef {
        id: "9@g.us".into(),
        ..Default::default()
    };
    channel.read_v2(&group, "3EB0B").await.unwrap();
    let reads: Vec<_> = client
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::Read { chat, sender, ids } => Some((chat, sender, ids)),
            _ => None,
        })
        .collect();
    assert_eq!(
        reads[0],
        (
            "15550001111@s.whatsapp.net".into(),
            None,
            vec!["3EB0A".into()]
        )
    );
    assert_eq!(
        reads[1],
        (
            "9@g.us".into(),
            Some("15551112222@s.whatsapp.net".into()),
            vec!["3EB0B".into()]
        )
    );
    assert!(matches!(
        channel.read_v2(&dm, "  ").await,
        Err(WaFailure::Rejected(_))
    ));
    let failing = Arc::new(RecordingClient {
        fail: true,
        ..RecordingClient::linked("15559998888@s.whatsapp.net")
    });
    *channel.client.lock() = Some(failing as Arc<dyn OutboundClient>);
    assert!(matches!(
        channel.read_v2(&dm, "3EB0A").await,
        Err(WaFailure::Uncertain(_))
    ));
}
