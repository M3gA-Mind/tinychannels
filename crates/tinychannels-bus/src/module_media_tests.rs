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
    let decoded: MediaUploadBegin = serde_json::from_slice(&serde_json::to_vec(&begin).unwrap()).unwrap();
    assert_eq!(decoded.scope.conversation, begin.scope.conversation);
    assert_eq!(decoded.scope.turn_id, "turn");
}
