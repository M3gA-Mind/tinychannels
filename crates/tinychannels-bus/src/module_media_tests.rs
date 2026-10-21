use super::*;

#[test]
fn media_upload_bounds_keep_each_serialized_chunk_below_the_bus_frame_limit() {
    let chunk = MediaUploadChunk {
        handle: MediaUploadHandle("0".repeat(32)),
        sequence: MEDIA_UPLOAD_MAX_CHUNKS as u32 - 1,
        bytes: vec![u8::MAX; MEDIA_UPLOAD_CHUNK_BYTES],
    };
    let encoded = serde_json::to_vec(&chunk).unwrap();
    assert!(encoded.len() < 3 * 1024 * 1024);
    assert!(encoded.len() < 16 * 1024 * 1024);
}

#[test]
fn media_upload_caps_bound_bytes_chunks_metadata_and_runtime_capacity() {
    assert_eq!(MEDIA_UPLOAD_MAX_BYTES, 20 * 1024 * 1024);
    assert_eq!(MEDIA_UPLOAD_CHUNK_BYTES, 512 * 1024);
    assert_eq!(MEDIA_UPLOAD_MAX_CHUNKS, 40);
    assert_eq!(MEDIA_UPLOAD_CAPACITY, 2);
    assert_eq!(MEDIA_UPLOAD_BEGIN_MAX_BYTES, 64 * 1024);
}

#[test]
fn media_upload_begin_preserves_full_conversation_and_turn_facts() {
    let begin = MediaUploadBegin {
        instance: "telegram:primary".into(),
        scope: HostVoiceScope {
            configured_instance: "caller-supplied".into(),
            channel: crate::ChannelRef {
                id: "caller-channel".into(),
                account_id: Some("caller-account".into()),
            },
            conversation: crate::ConversationRef {
                kind: crate::ConversationKind::Thread,
                id: "conversation".into(),
                scope_id: Some("scope".into()),
                parent_id: Some("parent".into()),
                thread_id: Some("thread".into()),
                topic_id: Some("topic".into()),
            },
            turn_id: "turn".into(),
        },
        kind: MediaKind::Image,
        content_type: "image/png".into(),
        file_name: Some("image.png".into()),
        duration_ms: None,
        codec: None,
        byte_count: 1,
    };
    let decoded: MediaUploadBegin =
        serde_json::from_slice(&serde_json::to_vec(&begin).unwrap()).unwrap();
    assert_eq!(decoded.scope.conversation, begin.scope.conversation);
    assert_eq!(decoded.scope.turn_id, "turn");
}

#[test]
fn media_read_limits_keep_serialized_confidential_chunks_bounded() {
    assert_eq!(MEDIA_READ_CHUNK_BYTES, 512 * 1024);
    assert_eq!(MEDIA_READ_CAPACITY, 2);
    let chunk = MediaReadChunkResult {
        sequence: 0,
        bytes: vec![u8::MAX; MEDIA_READ_CHUNK_BYTES],
        done: false,
    };
    let encoded = serde_json::to_vec(&chunk).unwrap();
    assert!(encoded.len() < 3 * 1024 * 1024);
    assert!(encoded.len() < 16 * 1024 * 1024);
}

#[test]
fn media_read_begin_preserves_full_logical_scope_and_opaque_reference() {
    let begin = MediaReadBegin {
        instance: "telegram:primary".into(),
        scope: HostVoiceScope {
            configured_instance: "caller-instance".into(),
            channel: crate::ChannelRef {
                id: "caller-channel".into(),
                account_id: Some("caller-account".into()),
            },
            conversation: crate::ConversationRef {
                kind: crate::ConversationKind::Thread,
                id: "conversation".into(),
                scope_id: Some("scope".into()),
                parent_id: Some("parent".into()),
                thread_id: Some("thread".into()),
                topic_id: Some("topic".into()),
            },
            turn_id: "turn".into(),
        },
        media: MediaReference {
            opaque_id: Some("tinychannels_media_v1_fixture".into()),
            ..Default::default()
        },
        max_bytes: 20 * 1024 * 1024,
    };
    let decoded: MediaReadBegin =
        serde_json::from_slice(&serde_json::to_vec(&begin).unwrap()).unwrap();
    assert_eq!(decoded.scope.conversation, begin.scope.conversation);
    assert_eq!(decoded.scope.turn_id, begin.scope.turn_id);
    assert_eq!(decoded.media.opaque_id, begin.media.opaque_id);
}

#[test]
fn opaque_media_reference_round_trips_without_a_shared_path() {
    let media = MediaReference {
        opaque_id: Some("tinychannels_media_v1_fixture".into()),
        content_type: Some("image/png".into()),
        kind: MediaKind::Image,
        ..Default::default()
    };
    let part = crate::ContentPart::Image {
        media: media.clone(),
        caption: Some("first attachment".into()),
        alt_text: None,
    };
    let envelope = crate::ChannelInboundEnvelope {
        parts: vec![
            crate::ContentPart::Text {
                text: "look".into(),
                markdown_dialect: None,
            },
            part,
        ],
        ..Default::default()
    };

    let encoded = serde_json::to_value(&envelope).unwrap();
    let decoded: crate::ChannelInboundEnvelope = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(decoded.parts, envelope.parts);
    assert_eq!(decoded.parts[1], envelope.parts[1]);
    let crate::ContentPart::Image {
        media: decoded_media,
        ..
    } = &decoded.parts[1]
    else {
        panic!("ordered media part changed kind");
    };
    assert_eq!(decoded_media.opaque_id, media.opaque_id);
    assert!(decoded_media.path.is_none());
    assert!(decoded_media.url.is_none());
    assert!(decoded_media.local_cache_path.is_none());
    assert!(
        encoded
            .to_string()
            .contains("tinychannels_media_v1_fixture")
    );
}
