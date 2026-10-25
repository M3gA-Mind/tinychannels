use super::*;
use crate::providers::whatsapp_shared::fakes::{MemoryMedia, MemoryVoices, ogg_bytes, png_bytes};
use tinychannels_bus::{ButtonAction, ContactMethod, VoiceNote};

const LIMITS: MediaLimits = MediaLimits {
    image: 1024 * 1024,
    video: 1024 * 1024,
    audio: 1024 * 1024,
    document: 1024 * 1024,
    sticker: 1024 * 1024,
    caption_chars: 20,
};

fn message(parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
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

fn content(part: ContentPart) -> OutboundContentPart {
    OutboundContentPart::Content { part }
}

fn reference(id: &str) -> MediaReference {
    MediaReference {
        opaque_id: Some(id.into()),
        ..Default::default()
    }
}

async fn plan(
    message: &ChannelOutboundMessage,
    media: Option<&MemoryMedia>,
    voices: Option<&MemoryVoices>,
) -> Result<Vec<PlannedPart>, WaFailure> {
    let scope = outbound_scope(message, "whatsapp")?;
    plan_parts(
        message,
        &scope,
        PlanPorts {
            media: media.map(|media| media as &dyn MediaRepository),
            voices: voices.map(|voices| voices as &dyn VoiceMediaStore),
        },
        &LIMITS,
    )
    .await
}

// --- addressing -------------------------------------------------------------

#[test]
fn scope_requires_a_conversation_and_matching_channel() {
    let mut blank = message(vec![text("hi")]);
    blank.conversation_ref = None;
    blank.conversation_id = "  ".into();
    assert!(matches!(
        outbound_scope(&blank, "whatsapp"),
        Err(WaFailure::Rejected(_))
    ));

    let mut other = message(vec![text("hi")]);
    other.channel_ref = Some(ChannelRef {
        id: "telegram".into(),
        account_id: None,
    });
    assert!(matches!(
        outbound_scope(&other, "whatsapp"),
        Err(WaFailure::Rejected(_))
    ));
}

#[test]
fn scope_refuses_thread_and_topic_addressing_as_unsupported() {
    let mut threaded = message(vec![text("hi")]);
    threaded.thread_id = Some("t".into());
    assert!(matches!(
        outbound_scope(&threaded, "whatsapp"),
        Err(WaFailure::Unsupported(_))
    ));
    let mut topic = message(vec![text("hi")]);
    topic.conversation_ref.as_mut().unwrap().topic_id = Some("9".into());
    assert!(matches!(
        outbound_scope(&topic, "whatsapp"),
        Err(WaFailure::Unsupported(_))
    ));
}

#[test]
fn scope_defaults_the_channel_trims_reply_and_turn_ids() {
    let mut bare = message(vec![text("hi")]);
    bare.channel_ref = None;
    bare.conversation_ref = None;
    bare.reply_to_id = Some("  wamid.1 ".into());
    bare.turn_id = Some("  ".into());
    let scope = outbound_scope(&bare, "whatsapp").unwrap();
    assert_eq!(scope.channel.id, "whatsapp");
    assert_eq!(scope.conversation.id, "+15550001111");
    assert_eq!(scope.reply_to_id.as_deref(), Some("wamid.1"));
    assert_eq!(scope.turn_id, None);
}

// --- text and structural parts ---------------------------------------------

#[tokio::test]
async fn text_is_stripped_of_tool_markup_and_blank_text_is_skipped() {
    let m = message(vec![text("   "), text("hello <tool_call>x</tool_call>")]);
    let planned = plan(&m, None, None).await.unwrap();
    assert_eq!(planned.len(), 1);
    assert!(matches!(&planned[0], PlannedPart::Text(value) if value.trim() == "hello"));
}

#[tokio::test]
async fn a_message_with_no_content_is_rejected() {
    let m = message(vec![text("  ")]);
    assert!(matches!(
        plan(&m, None, None).await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn locations_are_validated_and_blank_labels_dropped() {
    let good = message(vec![content(ContentPart::Location {
        latitude: 51.5,
        longitude: -0.12,
        label: Some(" ".into()),
        address: Some("1 Road".into()),
        accuracy_meters: None,
        live_until_unix_ms: None,
    })]);
    let planned = plan(&good, None, None).await.unwrap();
    assert_eq!(
        planned,
        [PlannedPart::Location {
            latitude: 51.5,
            longitude: -0.12,
            name: None,
            address: Some("1 Road".into()),
        }]
    );
    for (latitude, longitude) in [(91.0, 0.0), (0.0, 181.0), (f64::NAN, 0.0)] {
        let bad = message(vec![content(ContentPart::Location {
            latitude,
            longitude,
            label: None,
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: None,
        })]);
        assert!(matches!(
            plan(&bad, None, None).await,
            Err(WaFailure::Rejected(_))
        ));
    }
}

#[tokio::test]
async fn contacts_need_a_name_or_a_number() {
    let empty = message(vec![content(ContentPart::Contact {
        display_name: " ".into(),
        phone_numbers: vec![],
        emails: vec![],
        organization: None,
    })]);
    assert!(matches!(
        plan(&empty, None, None).await,
        Err(WaFailure::Rejected(_))
    ));
    let named = message(vec![content(ContentPart::Contact {
        display_name: "Ada".into(),
        phone_numbers: vec![ContactMethod {
            value: "+155".into(),
            label: None,
        }],
        emails: vec![],
        organization: Some("Co".into()),
    })]);
    let planned = plan(&named, None, None).await.unwrap();
    let PlannedPart::Contact(card) = &planned[0] else {
        panic!("expected a contact")
    };
    assert_eq!(card.display_name, "Ada");
    assert_eq!(card.phones.len(), 1);
}

#[tokio::test]
async fn buttons_polls_presentations_native_data_and_synthesis_are_unsupported() {
    let unsupported = [
        content(ContentPart::Buttons {
            rows: vec![vec![ButtonAction {
                id: "a".into(),
                label: "A".into(),
                style: None,
                disabled: false,
            }]],
        }),
        content(ContentPart::Poll {
            question: "q".into(),
            options: vec!["a".into()],
            multiple_choice: false,
            closes_at_unix_ms: None,
            provider_poll_id: None,
        }),
        content(ContentPart::Presentation {
            blocks: serde_json::json!([]),
        }),
        content(ContentPart::Native {
            provider: "whatsapp".into(),
            data: serde_json::json!({}),
        }),
        OutboundContentPart::Voice {
            voice: OutboundVoice::Synthesize {
                text: "say".into(),
                voice: None,
            },
        },
    ];
    for part in unsupported {
        let m = message(vec![part]);
        assert!(matches!(
            plan(&m, None, None).await,
            Err(WaFailure::Unsupported(_))
        ));
    }
}

// --- stored media ------------------------------------------------------------

#[tokio::test]
async fn media_without_storage_is_unsupported_and_without_an_opaque_id_rejected() {
    let image = |id: Option<&str>| {
        message(vec![content(ContentPart::Image {
            media: MediaReference {
                opaque_id: id.map(str::to_owned),
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        })])
    };
    assert!(matches!(
        plan(&image(Some("x")), None, None).await,
        Err(WaFailure::Unsupported(_))
    ));
    let media = MemoryMedia::default();
    assert!(matches!(
        plan(&image(None), Some(&media), None).await,
        Err(WaFailure::Rejected(_))
    ));
    assert!(matches!(
        plan(&image(Some("  ")), Some(&media), None).await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn media_reads_need_the_configured_account_and_originating_turn() {
    let media = MemoryMedia::default();
    let id = media.seed(MediaKind::Image, "image/png", None, &png_bytes());
    let part = || {
        content(ContentPart::Image {
            media: reference(&id),
            caption: None,
            alt_text: None,
        })
    };
    let mut no_turn = message(vec![part()]);
    no_turn.turn_id = None;
    assert!(matches!(
        plan(&no_turn, Some(&media), None).await,
        Err(WaFailure::Rejected(_))
    ));
    let mut no_account = message(vec![part()]);
    no_account.channel_ref = Some(ChannelRef {
        id: "whatsapp".into(),
        account_id: Some(" ".into()),
    });
    assert!(matches!(
        plan(&no_account, Some(&media), None).await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn images_map_to_photos_or_documents_and_unreadable_media_is_rejected() {
    let media = MemoryMedia::default();
    let png = media.seed(MediaKind::Image, "image/png", Some("a.png"), &png_bytes());
    let gif = media.seed(MediaKind::Image, "image/gif", None, b"GIF89a-----");
    let wrong_kind = media.seed(MediaKind::Video, "video/mp4", None, b"0000ftyp");
    let empty = media.seed(MediaKind::Image, "image/png", None, b"");
    let big = media.seed(
        MediaKind::Image,
        "image/png",
        None,
        &vec![1u8; (LIMITS.image + 1) as usize],
    );
    let image = |id: &str, caption: Option<&str>| {
        message(vec![content(ContentPart::Image {
            media: reference(id),
            caption: caption.map(str::to_owned),
            alt_text: None,
        })])
    };
    let planned = plan(
        &image(&png, Some(" cap <tool>x</tool>")),
        Some(&media),
        None,
    )
    .await
    .unwrap();
    assert!(matches!(
        &planned[0],
        PlannedPart::Attachment { kind: AttachmentKind::Image, content_type, caption, file_name, .. }
            if content_type == "image/png" && caption.as_deref() == Some("cap") && file_name.as_deref() == Some("a.png")
    ));
    let planned = plan(&image(&gif, None), Some(&media), None).await.unwrap();
    assert!(matches!(
        &planned[0],
        PlannedPart::Attachment {
            kind: AttachmentKind::Document,
            ..
        }
    ));
    for bad in [&wrong_kind, &empty, &big, &"missing".to_owned()] {
        assert!(matches!(
            plan(&image(bad, None), Some(&media), None).await,
            Err(WaFailure::Rejected(_))
        ));
    }
    // Every read is bound to the configured account and turn.
    let scopes = media.read_scopes.lock();
    assert!(
        scopes
            .iter()
            .all(|scope| { scope.configured_instance == "whatsapp" && scope.turn_id == "turn-1" })
    );
}

#[tokio::test]
async fn long_captions_travel_as_a_follow_up_text() {
    let media = MemoryMedia::default();
    let png = media.seed(MediaKind::Image, "image/png", None, &png_bytes());
    let long = "x".repeat(LIMITS.caption_chars + 1);
    let m = message(vec![content(ContentPart::Image {
        media: reference(&png),
        caption: Some(long.clone()),
        alt_text: None,
    })]);
    let planned = plan(&m, Some(&media), None).await.unwrap();
    assert_eq!(planned.len(), 2);
    assert!(matches!(
        &planned[0],
        PlannedPart::Attachment { caption: None, .. }
    ));
    assert_eq!(planned[1], PlannedPart::Text(long));
}

#[tokio::test]
async fn video_audio_and_files_map_to_the_right_attachment_kinds() {
    let media = MemoryMedia::default();
    let mp4 = media.seed(MediaKind::Video, "video/mp4", None, b"0000ftypmp42");
    let webm = media.seed(
        MediaKind::Video,
        "video/webm",
        None,
        &[0x1a, 0x45, 0xdf, 0xa3, 0],
    );
    let mp3 = media.seed(MediaKind::Audio, "audio/mpeg", None, b"ID3-----");
    let wav = media.seed(MediaKind::Audio, "audio/wav", None, b"RIFF----WAVE");
    let pdf = media.seed(
        MediaKind::Document,
        "application/pdf",
        Some("src.pdf"),
        b"%PDF-1.4 x",
    );
    let m = message(vec![
        content(ContentPart::Video {
            media: reference(&mp4),
            caption: Some("v".into()),
            duration_ms: Some(2500),
            thumbnail: None,
        }),
        content(ContentPart::Video {
            media: reference(&webm),
            caption: None,
            duration_ms: None,
            thumbnail: None,
        }),
        content(ContentPart::Audio {
            media: reference(&mp3),
            codec: None,
            duration_ms: Some(1000),
            title: None,
            transcript: Some("the words".into()),
        }),
        content(ContentPart::Audio {
            media: reference(&wav),
            codec: None,
            duration_ms: None,
            title: None,
            transcript: None,
        }),
        content(ContentPart::File {
            media: reference(&pdf),
            file_name: Some("renamed.pdf".into()),
            caption: Some("doc".into()),
        }),
    ]);
    let planned = plan(&m, Some(&media), None).await.unwrap();
    let kinds: Vec<_> = planned
        .iter()
        .map(|part| match part {
            PlannedPart::Attachment { kind, .. } => format!("{kind:?}"),
            PlannedPart::Text(value) => format!("text:{value}"),
            other => format!("{other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "Video",
            "Document",
            "Audio",
            "text:the words",
            "Document",
            "Document"
        ]
    );
    assert!(matches!(
        planned.last(),
        Some(PlannedPart::Attachment { file_name: Some(name), .. }) if name == "renamed.pdf"
    ));
}

#[tokio::test]
async fn only_webp_images_can_be_stickers() {
    let media = MemoryMedia::default();
    let webp = media.seed(MediaKind::Image, "image/webp", None, b"RIFF----WEBPxxxx");
    let png = media.seed(MediaKind::Image, "image/png", None, &png_bytes());
    let sticker = |id: &str| {
        message(vec![content(ContentPart::Sticker {
            media: reference(id),
            pack_id: None,
            sticker_id: None,
            emoji: None,
            alt_text: None,
        })])
    };
    let planned = plan(&sticker(&webp), Some(&media), None).await.unwrap();
    assert!(matches!(
        &planned[0],
        PlannedPart::Attachment {
            kind: AttachmentKind::Sticker,
            ..
        }
    ));
    assert!(matches!(
        plan(&sticker(&png), Some(&media), None).await,
        Err(WaFailure::Rejected(_))
    ));
}

// --- voice -------------------------------------------------------------------

fn voice(id: &str, _waveform: Option<Vec<f32>>) -> OutboundContentPart {
    OutboundContentPart::Voice {
        voice: OutboundVoice::Media {
            media: reference(id),
            codec: Some("ogg_opus".into()),
            duration_ms: Some(1500),
        },
    }
}

#[tokio::test]
async fn voice_notes_are_read_through_the_voice_store_with_a_resampled_waveform() {
    let voices = MemoryVoices::default();
    voices.seed("v1", "audio/ogg", &ogg_bytes());
    let m = message(vec![OutboundContentPart::Content {
        part: ContentPart::VoiceNote {
            voice_note: VoiceNote {
                media: reference("v1"),
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1500),
                waveform: Some(vec![0.0, 0.5, 1.0, 2.0]),
                ..Default::default()
            },
        },
    }]);
    let planned = plan(&m, None, Some(&voices)).await.unwrap();
    let PlannedPart::Voice {
        bytes,
        content_type,
        duration_ms,
        waveform,
    } = &planned[0]
    else {
        panic!("expected a voice note")
    };
    assert!(bytes.starts_with(b"OggS"));
    assert_eq!(content_type, "audio/ogg; codecs=opus");
    assert_eq!(*duration_ms, Some(1500));
    assert_eq!(waveform.as_ref().unwrap().len(), WAVEFORM_BUCKETS);
}

#[tokio::test]
async fn voice_failures_are_classified() {
    let voices = MemoryVoices::default();
    voices.seed("mp3", "audio/mpeg", b"ID3-----");
    voices.seed("fake-ogg", "audio/ogg", b"not ogg at all");
    voices.seed("ok", "audio/ogg", &ogg_bytes());
    // No store wired.
    assert!(matches!(
        plan(&message(vec![voice("ok", None)]), None, None).await,
        Err(WaFailure::Unsupported(_))
    ));
    // Wrong container, and a wrong body behind an Ogg label.
    for id in ["mp3", "fake-ogg"] {
        assert!(matches!(
            plan(&message(vec![voice(id, None)]), None, Some(&voices)).await,
            Err(WaFailure::Unsupported(_))
        ));
    }
    // Unknown reference.
    assert!(matches!(
        plan(&message(vec![voice("missing", None)]), None, Some(&voices)).await,
        Err(WaFailure::Rejected(_))
    ));
    // Missing opaque id.
    let blank = message(vec![OutboundContentPart::Voice {
        voice: OutboundVoice::Media {
            media: MediaReference::default(),
            codec: None,
            duration_ms: None,
        },
    }]);
    assert!(matches!(
        plan(&blank, None, Some(&voices)).await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn oversized_and_undecodable_voice_media_is_rejected() {
    let voices = MemoryVoices::default();
    let mut huge = b"OggS".to_vec();
    huge.resize((MAX_VOICE_BYTES + 1) as usize, 0);
    voices.seed("huge", "audio/ogg", &huge);
    assert!(matches!(
        plan(&message(vec![voice("huge", None)]), None, Some(&voices)).await,
        Err(WaFailure::Rejected(_))
    ));
    // Stored payloads that are not base64 cannot be decoded.
    voices.served_raw("garbled", "audio/ogg", "!!!not-base64!!!");
    assert!(matches!(
        plan(&message(vec![voice("garbled", None)]), None, Some(&voices)).await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn voice_note_parts_are_planned_like_voice_media() {
    let voices = MemoryVoices::default();
    voices.seed("v1", "audio/ogg", &ogg_bytes());
    let m = message(vec![content(ContentPart::VoiceNote {
        voice_note: VoiceNote {
            media: reference("v1"),
            codec: Some("ogg_opus".into()),
            duration_ms: Some(900),
            waveform: Some(vec![0.25; 10]),
            push_to_talk: true,
            transcript: None,
        },
    })]);
    let planned = plan(&m, None, Some(&voices)).await.unwrap();
    assert!(matches!(
        &planned[0],
        PlannedPart::Voice {
            waveform: Some(_),
            ..
        }
    ));
}

// --- helpers -------------------------------------------------------------------

#[test]
fn waveform_buckets_follow_the_peak_and_reject_unusable_input() {
    assert_eq!(waveform_bytes(&[]), None);
    assert_eq!(waveform_bytes(&[0.1, f32::NAN]), None);
    let wide = waveform_bytes(&vec![0.5; 640]).unwrap();
    assert_eq!(wide.len(), WAVEFORM_BUCKETS);
    assert!(wide.iter().all(|value| *value == 50));
    let narrow = waveform_bytes(&[1.0, -3.0]).unwrap();
    assert_eq!(narrow.len(), WAVEFORM_BUCKETS);
    assert!(narrow.iter().all(|value| *value <= 100));
    assert_eq!(narrow[0], 100);
}

#[test]
fn base_mime_drops_parameters_and_case() {
    assert_eq!(base_mime("Audio/OGG; codecs=opus"), "audio/ogg");
    assert_eq!(base_mime(""), "");
}
