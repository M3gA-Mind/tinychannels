use super::*;
use crate::providers::whatsapp_shared::fakes::{MemoryMedia, MemoryVoices, png_bytes};

fn conversation() -> ConversationRef {
    ConversationRef {
        kind: ConversationKind::Dm,
        id: "+15550001111".into(),
        ..Default::default()
    }
}

fn download(content_type: &str, bytes: Vec<u8>) -> DownloadedAttachment {
    DownloadedAttachment {
        bytes,
        content_type: content_type.into(),
        file_name: Some("name.bin".into()),
        duration_ms: Some(2000),
    }
}

#[test]
fn scope_carries_the_message_id_as_the_turn_and_leaves_the_account_to_the_module() {
    let scope = inbound_scope("whatsapp", &conversation(), "wamid.1");
    assert_eq!(scope.turn_id, "wamid.1");
    assert_eq!(scope.channel.id, "whatsapp");
    assert_eq!(scope.channel.account_id, None);
    assert!(scope.configured_instance.is_empty());
}

#[tokio::test]
async fn each_attachment_kind_is_stored_and_shaped_into_its_content_part() {
    let repository = MemoryMedia::default();
    let scope = inbound_scope("whatsapp", &conversation(), "wamid.1");
    let cases = [
        (InboundKind::Image, "image/png", png_bytes()),
        (
            InboundKind::Sticker,
            "image/webp",
            b"RIFF----WEBPxxxx".to_vec(),
        ),
        (InboundKind::Video, "video/mp4", b"0000ftypmp42".to_vec()),
        (InboundKind::Audio, "audio/mpeg", b"ID3-----".to_vec()),
        (
            InboundKind::Document,
            "application/pdf",
            b"%PDF-1.4 x".to_vec(),
        ),
    ];
    let mut shapes = Vec::new();
    for (kind, content_type, bytes) in cases {
        let part = store_attachment(
            &repository,
            scope.clone(),
            kind,
            download(content_type, bytes),
            Some("cap".into()),
            format!("key-{}", kind.field()),
        )
        .await
        .unwrap();
        shapes.push(match part {
            ContentPart::Image { caption, .. } => format!("image:{caption:?}"),
            ContentPart::Sticker { .. } => "sticker".into(),
            ContentPart::Video { duration_ms, .. } => format!("video:{duration_ms:?}"),
            ContentPart::Audio { duration_ms, .. } => format!("audio:{duration_ms:?}"),
            ContentPart::File { file_name, .. } => format!("file:{file_name:?}"),
            other => panic!("unexpected part {other:?}"),
        });
    }
    assert_eq!(
        shapes,
        [
            "image:Some(\"cap\")",
            "sticker",
            "video:Some(2000)",
            "audio:Some(2000)",
            "file:Some(\"name.bin\")"
        ]
    );
    let stored = repository.stored.lock();
    assert_eq!(stored.len(), 5);
    assert_eq!(stored[0].dedupe_key.as_deref(), Some("key-image"));
}

#[tokio::test]
async fn unvalidatable_types_fall_back_to_documents_and_unknown_types_are_refused() {
    let repository = MemoryMedia::default();
    let scope = inbound_scope("whatsapp", &conversation(), "wamid.2");
    // An image type the host cannot validate as an image but accepts as a document.
    let part = store_attachment(
        &repository,
        scope.clone(),
        InboundKind::Image,
        download("application/pdf; charset=binary", b"%PDF-1.4 x".to_vec()),
        None,
        "k".into(),
    )
    .await
    .unwrap();
    assert!(matches!(part, ContentPart::File { .. }));
    let refused = store_attachment(
        &repository,
        scope,
        InboundKind::Image,
        download("application/x-unknown", b"x".to_vec()),
        None,
        "k".into(),
    )
    .await;
    assert!(matches!(refused, Err(WaFailure::Rejected(_))));
}

#[tokio::test]
async fn empty_oversized_and_unstorable_attachments_fail_by_class() {
    let repository = MemoryMedia::default();
    let scope = inbound_scope("whatsapp", &conversation(), "wamid.3");
    for bytes in [Vec::new(), vec![0u8; MAX_INBOUND_MEDIA_BYTES as usize + 1]] {
        let result = store_attachment(
            &repository,
            scope.clone(),
            InboundKind::Document,
            download("application/pdf", bytes),
            None,
            "k".into(),
        )
        .await;
        assert!(matches!(result, Err(WaFailure::Rejected(_))));
    }
    *repository.fail_store.lock() = true;
    let result = store_attachment(
        &repository,
        scope,
        InboundKind::Document,
        download("application/pdf", b"%PDF-1.4 x".to_vec()),
        None,
        "k".into(),
    )
    .await;
    assert!(matches!(result, Err(WaFailure::Uncertain(_))));
}

#[tokio::test]
async fn voice_notes_keep_only_the_host_reference_and_push_to_talk_facts() {
    let store = MemoryVoices::default();
    let scope = inbound_scope("whatsapp", &conversation(), "wamid.4");
    let note = store_voice_note(
        &store,
        scope.clone(),
        download("audio/ogg; codecs=opus", b"OggS-----".to_vec()),
        Some(vec![0.5]),
    )
    .await
    .unwrap();
    assert!(note.push_to_talk);
    assert_eq!(note.codec.as_deref(), Some("ogg_opus"));
    assert_eq!(note.media.url, None, "provider URLs never reach history");
    assert_eq!(note.media.content_type.as_deref(), Some("audio/ogg"));
    assert_eq!(note.waveform, Some(vec![0.5]));
    assert!(note.transcript.is_none());
    assert_eq!(store.stored.lock()[0].codec.as_deref(), Some("ogg_opus"));

    let other = store_voice_note(
        &store,
        scope.clone(),
        download("audio/mp4", b"....ftyp".to_vec()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(other.codec, None);
}

#[tokio::test]
async fn voice_notes_with_bad_sizes_or_no_durable_reference_fail() {
    let store = MemoryVoices::default();
    let scope = inbound_scope("whatsapp", &conversation(), "wamid.5");
    for bytes in [Vec::new(), vec![0u8; MAX_INBOUND_VOICE_BYTES as usize + 1]] {
        let result =
            store_voice_note(&store, scope.clone(), download("audio/ogg", bytes), None).await;
        assert!(matches!(result, Err(WaFailure::Rejected(_))));
    }
    *store.omit_opaque_id.lock() = true;
    let result =
        store_voice_note(&store, scope, download("audio/ogg", b"OggS".to_vec()), None).await;
    assert!(matches!(result, Err(WaFailure::Uncertain(_))));
}

#[test]
fn envelope_media_lists_every_part_reference_in_order() {
    let media = |id: &str| MediaReference {
        opaque_id: Some(id.into()),
        ..Default::default()
    };
    let parts = vec![
        ContentPart::Text {
            text: "t".into(),
            markdown_dialect: None,
        },
        ContentPart::Image {
            media: media("i"),
            caption: None,
            alt_text: None,
        },
        ContentPart::VoiceNote {
            voice_note: VoiceNote {
                media: media("v"),
                ..Default::default()
            },
        },
        ContentPart::Video {
            media: media("m"),
            caption: None,
            duration_ms: None,
            thumbnail: None,
        },
    ];
    let ids: Vec<_> = envelope_media(&parts)
        .into_iter()
        .map(|media| media.opaque_id.unwrap())
        .collect();
    assert_eq!(ids, ["i", "v", "m"]);
}

#[test]
fn small_helpers_convert_units_and_kinds() {
    assert_eq!(conversation_kind(true), ConversationKind::Group);
    assert_eq!(conversation_kind(false), ConversationKind::Dm);
    assert_eq!(seconds_to_ms(3), 3000);
    assert_eq!(seconds_to_ms(u64::MAX), u64::MAX);
    assert_eq!(waveform_samples(&[]), None);
    assert_eq!(waveform_samples(&[0, 50, 200]), Some(vec![0.0, 0.5, 1.0]));
    for kind in [
        InboundKind::Image,
        InboundKind::Video,
        InboundKind::Audio,
        InboundKind::Document,
        InboundKind::Sticker,
    ] {
        assert!(!kind.field().is_empty());
    }
    assert_eq!(InboundKind::Sticker.media_kind(), MediaKind::Image);
}
