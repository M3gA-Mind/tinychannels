use super::*;
use base64::Engine;
use std::ops::Range;
use std::sync::Arc;
use tinychannels_bus::{ChannelRef, ConversationKind, ConversationRef};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobReader, BlobStore, MemoryStorage, Scope, StorageBackend, Result as StorageResult,
    StorageError,
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

#[derive(Debug)]
struct FailDeleteOnce {
    inner: Arc<dyn BlobStore>,
    failed: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct PausedFirstPut {
    inner: Arc<dyn BlobStore>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    first: std::sync::atomic::AtomicBool,
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

#[async_trait::async_trait]
impl BlobStore for PausedFirstPut {
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> StorageResult<BlobMeta> {
        self.inner.put(key, bytes, content_type).await
    }

    async fn put_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> StorageResult<BlobMeta> {
        if self.first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.put_stream(key, reader, len, content_type).await
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
    let backend = MemoryStorage::new();
    let scope = Scope::new("module:tinychannels@test").unwrap();
    let first_storage = backend.for_scope(&scope).unwrap();
    let second_storage = backend.for_scope(&scope).unwrap();
    let paused = Arc::new(PausedFirstPut {
        inner: Arc::clone(first_storage.blobs()),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        first: std::sync::atomic::AtomicBool::new(true),
    });
    let first = Arc::new(ScopedMediaStore::new(
        Arc::clone(first_storage.documents()),
        paused.clone(),
    ));
    let second = ScopedMediaStore::new(
        Arc::clone(second_storage.documents()),
        Arc::clone(second_storage.blobs()),
    );
    let key = Some("telegram:91:photo:concurrent".to_owned());
    let left = {
        let first = Arc::clone(&first);
        let key = key.clone();
        tokio::spawn(async move {
            first.store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            Some("photo.png".into()),
            None,
            valid_png(),
            key.clone(),
            1024,
            ).await
        })
    };
    paused.entered.notified().await;
    let competing = second
        .store(
            media_scope(),
            tinychannels_bus::MediaKind::Image,
            "image/png".into(),
            Some("photo.png".into()),
            None,
            valid_png(),
            key,
            1024,
        )
        .await;
    paused.release.notify_one();
    let reference = left.await.unwrap().expect("first writer should finish");
    let opaque_id = reference.opaque_id.as_deref().unwrap();
    let resolved = first.read(media_scope(), opaque_id, 1024).await.unwrap();
    assert_eq!(resolved.bytes, valid_png());
    assert!(competing.is_err(), "a second writer cannot replace a reservation");
}

#[tokio::test]
async fn deleting_an_inflight_media_write_cannot_orphan_its_blob() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("module:tinychannels@test").unwrap();
    let writer_storage = backend.for_scope(&scope).unwrap();
    let deleter_storage = backend.for_scope(&scope).unwrap();
    let paused = Arc::new(PausedFirstPut {
        inner: Arc::clone(writer_storage.blobs()),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        first: std::sync::atomic::AtomicBool::new(true),
    });
    let writer = Arc::new(ScopedMediaStore::new(
        Arc::clone(writer_storage.documents()),
        paused.clone(),
    ));
    let deleter = ScopedMediaStore::new(
        Arc::clone(deleter_storage.documents()),
        Arc::clone(deleter_storage.blobs()),
    );
    let dedupe_key = "telegram:91:photo:inflight";
    let opaque_id = scoped_id(&media_scope(), dedupe_key).unwrap();
    let writer_task = {
        let writer = Arc::clone(&writer);
        tokio::spawn(async move {
            writer
                .store(
                    media_scope(),
                    tinychannels_bus::MediaKind::Image,
                    "image/png".into(),
                    None,
                    None,
                    valid_png(),
                    Some(dedupe_key.into()),
                    1024,
                )
                .await
        })
    };
    paused.entered.notified().await;
    assert!(deleter.delete(media_scope(), &opaque_id).await.is_err());
    paused.release.notify_one();
    writer_task.await.unwrap().unwrap();
    let stored = deleter.read(media_scope(), &opaque_id, 1024).await.unwrap();
    assert_eq!(stored.bytes, valid_png());
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
