use super::*;
use base64::Engine;
use std::ops::Range;
use std::sync::Arc;
use tinychannels_bus::{ChannelRef, ConversationKind, ConversationRef};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobStore, MemoryStorage, Scope, StorageBackend,
    error::{Result as StorageResult, StorageError},
};

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
            scope_id: Some("group-scope".into()),
            parent_id: Some("parent-chat".into()),
            thread_id: Some("thread-2".into()),
            topic_id: Some("topic-3".into()),
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

fn shared_stores() -> (ScopedMediaStore, ScopedMediaStore) {
    let backend = MemoryStorage::new();
    let scope = Scope::new("module:tinychannels@test").unwrap();
    let first = backend.for_scope(&scope).unwrap();
    let second = backend.for_scope(&scope).unwrap();
    (
        ScopedMediaStore::new(Arc::clone(first.documents()), Arc::clone(first.blobs())),
        ScopedMediaStore::new(Arc::clone(second.documents()), Arc::clone(second.blobs())),
    )
}

#[derive(Debug)]
struct FailDeleteOnce {
    inner: Arc<dyn BlobStore>,
    failed: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl BlobStore for FailDeleteOnce {
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> StorageResult<BlobMeta> {
        self.inner.put(key, bytes, content_type).await
    }

    async fn get(&self, key: &str) -> StorageResult<Option<Blob>> {
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> StorageResult<Option<BlobMeta>> {
        self.inner.head(key).await
    }

    async fn delete(&self, key: &str) -> StorageResult<bool> {
        if !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(StorageError::backend("injected blob deletion failure"));
        }
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<BlobMeta>> {
        self.inner.list(prefix).await
    }
}

fn valid_png() -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/lZcAAAAASUVORK5CYII=")
        .unwrap()
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
            valid_png(),
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
    assert_eq!(resolved.bytes, valid_png());
}

#[tokio::test]
async fn media_reference_requires_the_original_configured_full_turn_scope() {
    let store = store();
    let reference = store
        .store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            None,
            None,
            valid_png(),
            None,
            1024,
        )
        .await
        .unwrap();
    let mut wrong_scopes = Vec::new();
    let mut wrong = media_scope();
    wrong.configured_instance = "telegram-other".into();
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.channel.account_id = Some("telegram-other".into());
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.channel.id = "signal".into();
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.conversation.id = "other-chat".into();
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.conversation.scope_id = Some("other-scope".into());
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.conversation.parent_id = Some("other-parent".into());
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.conversation.thread_id = Some("other-thread".into());
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.conversation.topic_id = Some("other-topic".into());
    wrong_scopes.push(wrong);
    let mut wrong = media_scope();
    wrong.turn_id = "other-turn".into();
    wrong_scopes.push(wrong);

    for wrong_scope in wrong_scopes {
        assert!(store
            .read(wrong_scope, reference.opaque_id.as_deref().unwrap(), 1024)
            .await
            .is_err());
    }
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

#[tokio::test]
async fn media_store_rejects_spoofed_mime_and_truncated_image_bodies() {
    let store = store();
    for (content_type, bytes) in [
        ("image/png", b"<html>no</html>".to_vec()),
        ("application/pdf", valid_png()),
        ("image/png", b"\x89PNG\r\n\x1a\n".to_vec()),
    ] {
        assert!(store
            .store(
                media_scope(),
                tinychannels_bus::MediaKind::Image,
                content_type.into(),
                None,
                None,
                bytes,
                None,
                1024,
            )
            .await
            .is_err());
    }
}

#[tokio::test]
async fn independent_store_instances_cannot_replace_a_deduplicated_blob() {
    let (first, second) = shared_stores();
    let key = Some("telegram:91:photo:concurrent".to_owned());
    let (left, right) = tokio::join!(
        first.store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            Some("photo.png".into()),
            None,
            valid_png(),
            key.clone(),
            1024,
        ),
        second.store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            Some("photo.png".into()),
            None,
            valid_png(),
            key,
            1024,
        )
    );
    let successes = usize::from(left.is_ok()) + usize::from(right.is_ok());
    let reference = left
        .or(right)
        .expect("one store must reserve the shared id");
    let opaque_id = reference.opaque_id.as_deref().unwrap();
    let resolved = first.read(media_scope(), opaque_id, 1024).await.unwrap();
    assert_eq!(resolved.bytes, valid_png());
    assert_eq!(successes, 1);
}

#[tokio::test]
async fn failed_blob_cleanup_can_be_retried_after_reopening_the_store() {
    let backend = MemoryStorage::new();
    let storage = backend
        .for_scope(&Scope::new("module:tinychannels@test").unwrap())
        .unwrap();
    let failing_blobs = Arc::new(FailDeleteOnce {
        inner: Arc::clone(storage.blobs()),
        failed: std::sync::atomic::AtomicBool::new(false),
    });
    let writer = ScopedMediaStore::new(
        Arc::clone(storage.documents()),
        failing_blobs.clone(),
    );
    let reference = writer
        .store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            None,
            None,
            valid_png(),
            None,
            1024,
        )
        .await
        .unwrap();
    let opaque_id = reference.opaque_id.unwrap();
    assert!(writer.delete(media_scope(), &opaque_id).await.is_err());

    let reopened = ScopedMediaStore::new(
        Arc::clone(storage.documents()),
        failing_blobs,
    );
    assert!(reopened.delete(media_scope(), &opaque_id).await.unwrap());
    assert!(!reopened.delete(media_scope(), &opaque_id).await.unwrap());
    assert!(reopened.read(media_scope(), &opaque_id, 1024).await.is_err());
}
