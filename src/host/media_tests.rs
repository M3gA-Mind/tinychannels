use super::super::MediaRepository;
use super::*;
use base64::Engine;
use futures_util::StreamExt;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;
use tinychannels_bus::{ChannelRef, ConversationKind, ConversationRef};
use tinystoragedrivers_api::{BlobReader, BlobStore, StorageBackend};
use tinystoragedrivers_testkit::{MemoryBackend, memory_storage};
use tinystoragedrivers_types::{
    Blob, BlobMeta, Precondition, Result as StorageResult, Scope, StorageError,
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
    let storage = memory_storage(&Scope::new("module:tinychannels@test").unwrap());
    ScopedMediaStore::new(Arc::clone(storage.documents()), Arc::clone(storage.blobs()))
}

fn byte_stream(bytes: Vec<u8>) -> BlobReader {
    Box::pin(futures_util::stream::once(async move {
        Ok::<bytes::Bytes, StorageError>(bytes.into())
    }))
}

fn panic_reader() -> BlobReader {
    Box::pin(futures_util::stream::poll_fn(
        |_| -> Poll<Option<StorageResult<bytes::Bytes>>> {
            panic!("invalid scope must be rejected before reading the media body")
        },
    ))
}

#[derive(Debug)]
struct FailDeleteOnce {
    inner: Arc<dyn BlobStore>,
    failed: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct PartialPutAndDeleteOnce {
    inner: Arc<dyn BlobStore>,
    failed_delete: std::sync::atomic::AtomicBool,
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
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities()
    }

    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }

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
impl BlobStore for PartialPutAndDeleteOnce {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities()
    }

    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }

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
        // Model a backend that committed bytes before reporting a transport
        // failure. The metadata tombstone must retain enough information for
        // restart cleanup even when the immediate delete also fails.
        self.inner
            .put_stream(key, reader, len, content_type)
            .await?;
        Err(StorageError::backend("injected post-write failure"))
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
        if !self
            .failed_delete
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StorageError::backend("injected cleanup failure"));
        }
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<BlobMeta>> {
        self.inner.list(prefix).await
    }
}

#[async_trait::async_trait]
impl BlobStore for PausedFirstPut {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities()
    }

    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }

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

fn media_request(
    bytes: Vec<u8>,
    file_name: Option<&str>,
    dedupe_key: Option<&str>,
) -> MediaStoreRequest {
    MediaStoreRequest {
        metadata: MediaMetadata {
            scope: media_scope(),
            kind: tinychannels_bus::MediaKind::Image,
            content_type: "image/png".into(),
            file_name: file_name.map(str::to_owned),
            duration_ms: None,
            codec: None,
            voice_direction: None,
            dedupe_key: dedupe_key.map(str::to_owned),
            max_bytes: 1024,
        },
        bytes,
    }
}

#[tokio::test]
async fn stored_media_roundtrips_through_a_scoped_opaque_blob_reference() {
    let store = store();
    let reference = store
        .store(media_request(
            valid_png(),
            Some("photo.png"),
            Some("telegram:91:photo:0"),
        ))
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
async fn media_repository_trait_routes_bounded_store_read_stream_and_delete_operations() {
    let store = store();
    let reference =
        MediaRepository::store_media(&store, media_request(valid_png(), Some("photo.png"), None))
            .await
            .unwrap();
    let id = reference.opaque_id.clone().unwrap();
    let resolved = MediaRepository::read_media(&store, media_scope(), id.clone(), 1024)
        .await
        .unwrap();
    assert_eq!(resolved.bytes, valid_png());

    let streamed = MediaRepository::store_media_stream(
        &store,
        MediaStreamRequest {
            metadata: media_request(valid_png(), None, None).metadata,
            reader: byte_stream(valid_png()),
        },
    )
    .await
    .unwrap();
    assert!(
        MediaRepository::delete_media(&store, media_scope(), id)
            .await
            .unwrap()
    );
    assert!(
        MediaRepository::delete_media(&store, media_scope(), streamed.opaque_id.unwrap())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn deleting_media_refuses_foreign_scope_and_malformed_opaque_ids() {
    let store = store();
    let reference = store
        .store(media_request(valid_png(), None, None))
        .await
        .unwrap();
    let id = reference.opaque_id.unwrap();
    let mut foreign = media_scope();
    foreign.turn_id = "another-turn".into();
    assert!(store.delete(foreign, &id).await.is_err());
    assert!(
        store
            .read(media_scope(), "not-an-opaque-media-id", 1024)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn media_reference_requires_the_original_configured_full_turn_scope() {
    let store = store();
    let reference = store
        .store(media_request(valid_png(), None, None))
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
        assert!(
            store
                .read(wrong_scope, reference.opaque_id.as_deref().unwrap(), 1024)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn media_store_rejects_oversized_bytes_before_persisting_them() {
    let store = store();
    assert!(
        store
            .store(media_request(vec![0; 1025], None, None))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn streamed_media_accepts_actual_length_and_rejects_unknown_length_over_cap() {
    let store = store();
    let reference = store
        .store_stream(MediaStreamRequest {
            metadata: media_request(valid_png(), None, None).metadata,
            reader: byte_stream(valid_png()),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .read(media_scope(), reference.opaque_id.as_deref().unwrap(), 1024)
            .await
            .unwrap()
            .bytes,
        valid_png()
    );

    let mut metadata = media_request(Vec::new(), None, None).metadata;
    metadata.max_bytes = 64;
    let bytes_read = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&bytes_read);
    let reader = futures_util::stream::iter(
        (0..4096).map(|_| Ok::<_, StorageError>(bytes::Bytes::from_static(b"x"))),
    )
    .inspect(move |chunk| {
        if let Ok(chunk) = chunk {
            observed.fetch_add(chunk.len(), std::sync::atomic::Ordering::SeqCst);
        }
    });
    assert!(
        store
            .store_stream(MediaStreamRequest {
                metadata,
                reader: Box::pin(reader),
            })
            .await
            .is_err()
    );
    assert!(bytes_read.load(std::sync::atomic::Ordering::SeqCst) <= 65);
}

#[tokio::test]
async fn invalid_stream_scope_is_rejected_before_reading_body() {
    let store = store();
    let mut metadata = media_request(Vec::new(), None, None).metadata;
    metadata.scope.turn_id.clear();
    let result = store
        .store_stream(MediaStreamRequest {
            metadata,
            reader: panic_reader(),
        })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn whitespace_only_account_scope_is_rejected_before_reading_body() {
    let store = store();
    let mut metadata = media_request(Vec::new(), None, None).metadata;
    metadata.scope.channel.account_id = Some("   ".into());
    let result = store
        .store_stream(MediaStreamRequest {
            metadata,
            reader: panic_reader(),
        })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn deduplicated_media_rejects_changed_codec_or_voice_direction() {
    let store = store();
    let key = Some("voice-dedupe-metadata");
    let first = store
        .store(media_request(valid_png(), None, key))
        .await
        .unwrap();

    let mut changed_codec = media_request(valid_png(), None, key);
    changed_codec.metadata.codec = Some("codec-a".into());
    assert!(store.store(changed_codec).await.is_err());

    let mut changed_direction = media_request(valid_png(), None, key);
    changed_direction.metadata.voice_direction =
        Some(tinychannels_bus::HostVoiceMediaDirection::Inbound);
    assert!(store.store(changed_direction).await.is_err());

    let same = store
        .store(media_request(valid_png(), None, key))
        .await
        .unwrap();
    assert_eq!(same, first);
}

#[tokio::test]
async fn unsupported_stream_mime_is_rejected_before_reading_body() {
    let store = store();
    let mut metadata = media_request(Vec::new(), None, None).metadata;
    metadata.content_type = "image/svg+xml".into();
    let result = store
        .store_stream(MediaStreamRequest {
            metadata,
            reader: panic_reader(),
        })
        .await;
    assert!(result.is_err());
}

#[test]
fn amr_audio_requires_its_storage_magic_header() {
    assert!(is_allowed_content_type(
        tinychannels_bus::MediaKind::Audio,
        "audio/amr"
    ));
    assert!(content_matches("audio/amr", b"#!AMR\nframe"));
    assert!(!content_matches("audio/amr", b"not an AMR stream"));
}

#[test]
fn mpeg_audio_accepts_valid_frame_header_variants() {
    for header in [
        [0xff, 0xfb, 0x90, 0x64], // MPEG-1 Layer III, no CRC
        [0xff, 0xfa, 0x90, 0x64], // MPEG-1 Layer III, CRC protected
        [0xff, 0xf3, 0x80, 0xc4], // MPEG-2 Layer III
        [0xff, 0xe3, 0x80, 0xc4], // MPEG-2.5 Layer III
    ] {
        assert!(content_matches("audio/mpeg", &header));
    }

    for header in [
        [0xff, 0x7b, 0x90, 0x64], // invalid sync
        [0xff, 0xf9, 0x90, 0x64], // reserved layer
        [0xff, 0xf3, 0x8c, 0xc4], // reserved sample rate
        [0xff, 0xf3, 0xf0, 0xc4], // reserved bitrate
    ] {
        assert!(!content_matches("audio/mpeg", &header));
    }
    assert!(!content_matches("audio/mpeg", &[0xff, 0xfb]));
}

#[tokio::test]
async fn media_store_rejects_spoofed_mime_and_truncated_image_bodies() {
    let store = store();
    for (content_type, bytes) in [
        ("image/png", b"<html>no</html>".to_vec()),
        ("application/pdf", valid_png()),
        ("image/png", b"\x89PNG\r\n\x1a\n".to_vec()),
    ] {
        let mut request = media_request(bytes, None, None);
        request.metadata.content_type = content_type.into();
        assert!(store.store(request).await.is_err());
    }
}

#[tokio::test]
async fn independent_store_instances_cannot_replace_a_deduplicated_blob() {
    let backend = MemoryBackend::new();
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
            first
                .store(media_request(
                    valid_png(),
                    Some("photo.png"),
                    key.as_deref(),
                ))
                .await
        })
    };
    paused.entered.notified().await;
    let competing = second
        .store(media_request(
            valid_png(),
            Some("photo.png"),
            key.as_deref(),
        ))
        .await;
    paused.release.notify_one();
    let reference = left.await.unwrap().expect("first writer should finish");
    let opaque_id = reference.opaque_id.as_deref().unwrap();
    let resolved = first.read(media_scope(), opaque_id, 1024).await.unwrap();
    assert_eq!(resolved.bytes, valid_png());
    assert!(
        competing.is_err(),
        "a second writer cannot replace a reservation"
    );
}

#[tokio::test]
async fn deleting_an_inflight_media_write_cannot_orphan_its_blob() {
    let backend = MemoryBackend::new();
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
                .store(media_request(valid_png(), None, Some(dedupe_key)))
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
    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("module:tinychannels@test").unwrap())
        .unwrap();
    let failing_blobs = Arc::new(FailDeleteOnce {
        inner: Arc::clone(storage.blobs()),
        failed: std::sync::atomic::AtomicBool::new(false),
    });
    let writer = ScopedMediaStore::new(Arc::clone(storage.documents()), failing_blobs.clone());
    let reference = writer
        .store(media_request(valid_png(), None, None))
        .await
        .unwrap();
    let opaque_id = reference.opaque_id.unwrap();
    assert!(writer.delete(media_scope(), &opaque_id).await.is_err());

    let reopened = ScopedMediaStore::new(Arc::clone(storage.documents()), failing_blobs);
    assert!(reopened.delete(media_scope(), &opaque_id).await.unwrap());
    assert!(!reopened.delete(media_scope(), &opaque_id).await.unwrap());
    assert!(
        reopened
            .read(media_scope(), &opaque_id, 1024)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn partially_committed_failed_upload_is_cleaned_after_restart() {
    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("module:tinychannels@test").unwrap())
        .unwrap();
    let failing_blobs = Arc::new(PartialPutAndDeleteOnce {
        inner: Arc::clone(storage.blobs()),
        failed_delete: std::sync::atomic::AtomicBool::new(false),
    });
    let writer = ScopedMediaStore::new(Arc::clone(storage.documents()), failing_blobs.clone());

    assert!(
        writer
            .store(media_request(valid_png(), None, Some("partial-commit")))
            .await
            .is_err()
    );
    let pending = storage.blobs().list(MEDIA_KEY_PREFIX).await.unwrap();
    assert_eq!(pending.len(), 1, "the injected delete failure leaves bytes");
    let opaque_id = pending[0]
        .key
        .strip_prefix(MEDIA_KEY_PREFIX)
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_owned();
    let record = storage
        .documents()
        .get(MEDIA_COLLECTION, &opaque_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.doc["state"], "deleting");

    let reopened = ScopedMediaStore::new(Arc::clone(storage.documents()), failing_blobs);
    assert_eq!(
        reopened
            .cleanup_pending(media_scope(), now_unix_ms(), 0)
            .await
            .unwrap(),
        1
    );
    assert!(
        storage
            .blobs()
            .list(MEDIA_KEY_PREFIX)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        storage
            .documents()
            .get(MEDIA_COLLECTION, &opaque_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn pending_cleanup_filters_scope_state_and_age_before_applying_its_limit() {
    let main_store = store();
    let target = main_store
        .store(media_request(valid_png(), None, Some("cleanup-target")))
        .await
        .unwrap();
    let ready = main_store
        .store(media_request(valid_png(), None, Some("cleanup-ready")))
        .await
        .unwrap();
    let mut foreign_request = media_request(valid_png(), None, Some("cleanup-foreign"));
    foreign_request.metadata.scope.turn_id = "different-turn".into();
    let foreign = main_store.store(foreign_request).await.unwrap();

    for (id, age, state) in [
        (target.opaque_id.as_deref().unwrap(), 1, "writing"),
        (foreign.opaque_id.as_deref().unwrap(), 1, "writing"),
    ] {
        let found = main_store
            .documents
            .get(MEDIA_COLLECTION, id)
            .await
            .unwrap()
            .unwrap();
        let mut document = found.doc;
        document["created_at_unix_ms"] = serde_json::json!(age);
        document["state"] = serde_json::json!(state);
        main_store
            .documents
            .put(
                MEDIA_COLLECTION,
                id,
                document,
                Precondition::Version(found.version),
            )
            .await
            .unwrap();
    }

    assert_eq!(
        main_store
            .cleanup_pending(media_scope(), 2, 1)
            .await
            .unwrap(),
        1
    );
    assert!(
        main_store
            .documents
            .get(MEDIA_COLLECTION, target.opaque_id.as_deref().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        main_store
            .documents
            .get(MEDIA_COLLECTION, foreign.opaque_id.as_deref().unwrap())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        main_store
            .documents
            .get(MEDIA_COLLECTION, ready.opaque_id.as_deref().unwrap())
            .await
            .unwrap()
            .is_some()
    );
}
