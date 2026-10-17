use super::*;
use tinychannels_bus::{ChannelRef, ConversationKind, ConversationRef};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn media_scope() -> MediaScope {
    MediaScope {
        configured_instance: "telegram-main".into(),
        channel: ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram-main".into()),
        },
        conversation: ConversationRef {
            kind: ConversationKind::Group,
            id: "chat-17".into(),
            topic_id: Some("topic-3".into()),
            ..Default::default()
        },
        turn_id: "message-91".into(),
    }
}

fn store() -> ScopedMediaStore {
    let storage = MemoryStorage::new()
        .for_scope(&Scope::new("module:tinychannels@test").unwrap())
        .unwrap();
    ScopedMediaStore::new(
        Arc::clone(storage.documents()),
        Arc::clone(storage.blobs()),
    )
}

#[tokio::test]
async fn stored_media_roundtrips_through_a_scoped_opaque_blob_reference() {
    let store = store();
    let reference = store
        .store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            Some("photo.png".into()),
            None,
            vec![0x89, b'P', b'N', b'G'],
            Some("telegram:91:photo:0".into()),
            1024,
        )
        .await
        .unwrap();

    assert_eq!(reference.kind, tinychannels_bus::MediaKind::Image);
    assert!(reference.local_cache_path.is_none());
    let resolved = store
        .read(media_scope(), reference.opaque_id.as_deref().unwrap(), 1024)
        .await
        .unwrap();
    assert_eq!(resolved.media, reference);
    assert_eq!(resolved.content_type, "image/png");
    assert_eq!(resolved.file_name.as_deref(), Some("photo.png"));
    assert_eq!(resolved.bytes, [0x89, b'P', b'N', b'G']);
}

#[tokio::test]
async fn media_reference_cannot_be_resolved_from_a_different_channel_scope() {
    let store = store();
    let reference = store
        .store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            None,
            None,
            vec![0x89, b'P', b'N', b'G'],
            None,
            1024,
        )
        .await
        .unwrap();
    let mut wrong_scope = media_scope();
    wrong_scope.conversation.id = "other-chat".into();

    assert!(store
        .read(wrong_scope, reference.opaque_id.as_deref().unwrap(), 1024)
        .await
        .is_err());
}

#[tokio::test]
async fn media_store_rejects_oversized_bytes_before_persisting_them() {
    let store = store();
    assert!(store
        .store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            None,
            None,
            vec![0; 1025],
            None,
            1024,
        )
        .await
        .is_err());
}
