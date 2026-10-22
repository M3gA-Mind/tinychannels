use super::*;

fn stored(bytes: Vec<u8>) -> tinychannels::host::StoredMedia {
    tinychannels::host::StoredMedia {
        media: tinychannels_bus::MediaReference {
            opaque_id: Some("opaque".into()),
            content_type: Some("application/octet-stream".into()),
            size_bytes: Some(bytes.len() as u64),
            ..Default::default()
        },
        content_type: "application/octet-stream".into(),
        file_name: Some("data.bin".into()),
        bytes,
    }
}

fn read_request_fixture() -> tinychannels_bus::MediaReadBegin {
    tinychannels_bus::MediaReadBegin {
        instance: "telegram:primary".into(),
        scope: tinychannels_bus::HostVoiceScope {
            conversation: tinychannels_bus::ConversationRef {
                id: "conversation".into(),
                ..Default::default()
            },
            turn_id: "turn".into(),
            ..Default::default()
        },
        media: tinychannels_bus::MediaReference {
            opaque_id: Some("opaque".into()),
            ..Default::default()
        },
        max_bytes: 1024,
    }
}

#[tokio::test]
async fn media_reads_reserve_capacity_before_resolution_and_release_it_on_cancel() {
    let reads = MediaReads::default();
    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop(first);
    let third = reads.reserve().await.unwrap();
    drop((second, third));
    assert!(reads.reserve().await.is_ok());
}

#[tokio::test]
async fn media_reads_return_bounded_ordered_retryable_chunks_and_release_bytes() {
    let reads = MediaReads::default();
    let mut begin = read_request_fixture();
    let bytes = vec![0x5a; tinychannels_bus::module_media::MEDIA_READ_CHUNK_BYTES + 7];
    begin.max_bytes = bytes.len() as u64;
    let info = reads
        .complete(
            begin,
            "activation".into(),
            stored(bytes.clone()),
            reads.reserve().await.unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(info.byte_count, bytes.len());
    assert_eq!(info.chunk_count, 2);
    let first_request = tinychannels_bus::MediaReadChunk {
        handle: info.handle.clone(),
        sequence: 0,
    };
    let first = reads.chunk(first_request.clone()).await.unwrap();
    assert_eq!(
        first.bytes.len(),
        tinychannels_bus::module_media::MEDIA_READ_CHUNK_BYTES
    );
    assert!(!first.done);
    assert_eq!(reads.chunk(first_request).await.unwrap(), first);
    assert!(
        reads
            .chunk(tinychannels_bus::MediaReadChunk {
                handle: info.handle.clone(),
                sequence: 2,
            })
            .await
            .is_err()
    );
    let last = reads
        .chunk(tinychannels_bus::MediaReadChunk {
            handle: info.handle.clone(),
            sequence: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        last.bytes,
        bytes[tinychannels_bus::module_media::MEDIA_READ_CHUNK_BYTES..]
    );
    assert!(last.done);
    assert_eq!(
        reads
            .chunk(tinychannels_bus::MediaReadChunk {
                handle: info.handle.clone(),
                sequence: 1,
            })
            .await
            .unwrap(),
        last,
        "a lost final reply must remain retryable until explicit close"
    );
    let held = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    reads
        .abort(tinychannels_bus::MediaReadFinish(info.handle))
        .await
        .unwrap();
    assert!(reads.reserve().await.is_ok());
    drop(held);
}

#[tokio::test]
async fn media_reads_reject_claimed_size_mismatch_without_retaining_the_slot() {
    let reads = MediaReads::default();
    let mut wrong = stored(vec![1, 2, 3]);
    wrong.media.size_bytes = Some(4);
    assert!(
        reads
            .complete(
                read_request_fixture(),
                "activation".into(),
                wrong,
                reads.reserve().await.unwrap()
            )
            .await
            .is_err()
    );
    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop((first, second));
}

#[tokio::test]
async fn expired_read_leases_release_capacity_without_being_touched() {
    let reads = MediaReads::default();
    for handle in ["expired-a", "expired-b"] {
        let permit = reads.capacity.clone().try_acquire_owned().unwrap();
        reads.state.lock().await.reads.insert(
            handle.into(),
            Read {
                instance: "telegram".into(),
                activation: "old".into(),
                expires: Instant::now() - Duration::from_secs(1),
                bytes: vec![1],
                next_sequence: 0,
                last_chunk: None,
                _permit: permit,
            },
        );
    }
    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop((first, second));
}

#[tokio::test]
async fn oversized_resolved_metadata_drops_the_reserved_capacity() {
    let reads = MediaReads::default();
    let mut media = stored(vec![1, 2, 3]);
    media.file_name =
        Some("x".repeat(tinychannels_bus::module_media::MEDIA_UPLOAD_BEGIN_MAX_BYTES + 1));
    assert!(
        reads
            .complete(
                read_request_fixture(),
                "activation".into(),
                media,
                reads.reserve().await.unwrap()
            )
            .await
            .is_err()
    );
    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop((first, second));
}

#[tokio::test]
async fn canceled_and_timed_out_scoped_reads_release_their_pending_slots() {
    use std::{
        future::pending,
        sync::Arc,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Semaphore;

    let reads = Arc::new(MediaReads::default());
    let entered = Arc::new(Semaphore::new(0));
    let mut pending_reads = Vec::new();
    for _ in 0..tinychannels_bus::module_media::MEDIA_READ_CAPACITY {
        let task_reads = Arc::clone(&reads);
        let task_entered = Arc::clone(&entered);
        pending_reads.push(tokio::spawn(async move {
            task_reads
                .resolve_with_deadline(
                    read_request_fixture(),
                    "activation".into(),
                    Duration::from_secs(600),
                    || async move {
                        task_entered.add_permits(1);
                        pending::<tinybus::Result<tinychannels::host::StoredMedia>>().await
                    },
                    || async { true },
                )
                .await
        }));
    }
    entered
        .acquire_many(tinychannels_bus::module_media::MEDIA_READ_CAPACITY as u32)
        .await
        .unwrap()
        .forget();
    let loads = Arc::new(AtomicUsize::new(0));
    let third_loads = Arc::clone(&loads);
    let third = reads
        .resolve(
            read_request_fixture(),
            "activation".into(),
            || async move {
                third_loads.fetch_add(1, Ordering::SeqCst);
                Ok(stored(vec![1]))
            },
            || async { true },
        )
        .await;
    assert!(third.is_err());
    assert_eq!(
        loads.load(Ordering::SeqCst),
        0,
        "capacity must be reserved before storage I/O"
    );
    for pending_read in pending_reads {
        pending_read.abort();
        let _ = pending_read.await;
    }

    let entered = Arc::new(Semaphore::new(0));
    let task_reads = Arc::clone(&reads);
    let task_entered = Arc::clone(&entered);
    let timed_out = task_reads
        .resolve_with_deadline(
            read_request_fixture(),
            "activation".into(),
            Duration::from_millis(1),
            || async move {
                task_entered.add_permits(1);
                pending::<tinybus::Result<tinychannels::host::StoredMedia>>().await
            },
            || async { true },
        )
        .await;
    assert!(timed_out.is_err());

    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop((first, second));
}

#[tokio::test]
async fn failed_and_panicking_scoped_reads_release_the_pending_reservation() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let reads = MediaReads::default();
    let failed = reads
        .resolve(
            read_request_fixture(),
            "activation".into(),
            || async { Err(tinybus::Error::failed("fixture storage failure")) },
            || async { true },
        )
        .await;
    assert!(failed.is_err());
    let panicked = AssertUnwindSafe(reads.resolve(
        read_request_fixture(),
        "activation".into(),
        || async { panic!("fixture storage panic") },
        || async { true },
    ))
    .catch_unwind()
    .await;
    assert!(panicked.is_err());
    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop((first, second));
}

#[tokio::test]
async fn activation_replacement_discards_the_pending_read_before_retaining_bytes() {
    use std::future::ready;

    let reads = MediaReads::default();
    let result = reads
        .resolve(
            read_request_fixture(),
            "old-activation".into(),
            || async { Ok(stored(vec![1, 2, 3])) },
            || ready(false),
        )
        .await;
    assert!(result.is_err());
    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop((first, second));
}

#[tokio::test]
async fn rapid_read_close_cycles_keep_expiry_cleanup_bounded() {
    let runtime = tokio::runtime::Handle::current();
    let baseline_tasks = runtime.metrics().num_alive_tasks();
    let reads = MediaReads::default();
    for _ in 0..64 {
        let info = reads
            .complete(
                read_request_fixture(),
                "activation".into(),
                stored(vec![7]),
                reads.reserve().await.unwrap(),
            )
            .await
            .unwrap();
        reads
            .abort(tinychannels_bus::MediaReadFinish(info.handle))
            .await
            .unwrap();
    }
    tokio::task::yield_now().await;
    assert!(
        runtime.metrics().num_alive_tasks() <= baseline_tasks + 1,
        "closing reads must not leave one detached expiry task per handle"
    );
    let first = reads.reserve().await.unwrap();
    let second = reads.reserve().await.unwrap();
    assert!(reads.reserve().await.is_err());
    drop((first, second));
}

#[tokio::test]
async fn expiry_janitor_does_not_keep_read_state_alive_after_resolver_drop() {
    let reads = MediaReads::default();
    let info = reads
        .complete(
            read_request_fixture(),
            "activation".into(),
            stored(vec![7; 1024]),
            reads.reserve().await.unwrap(),
        )
        .await
        .unwrap();
    let state = Arc::downgrade(&reads.state);
    // Let the single janitor enter its expiry wait while the resolver remains
    // the only strong owner of the bytes and their capacity permit.
    tokio::task::yield_now().await;
    assert!(state.upgrade().is_some());
    drop(reads);
    assert!(state.upgrade().is_none());
    drop(info);
}
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

fn request(byte_count: usize) -> MediaUploadBegin {
    MediaUploadBegin {
        instance: "telegram:primary".into(),
        scope: tinychannels_bus::HostVoiceScope {
            configured_instance: "untrusted-instance".into(),
            channel: tinychannels_bus::ChannelRef {
                id: "untrusted-channel".into(),
                account_id: Some("untrusted-account".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Thread,
                id: "conversation".into(),
                scope_id: Some("scope".into()),
                parent_id: Some("parent".into()),
                thread_id: Some("thread".into()),
                topic_id: Some("topic".into()),
            },
            turn_id: "turn".into(),
        },
        kind: tinychannels_bus::MediaKind::Image,
        content_type: "image/png".into(),
        file_name: Some("image.png".into()),
        duration_ms: None,
        codec: None,
        byte_count,
    }
}

#[tokio::test]
async fn chunks_are_ordered_bounded_idempotent_and_abort_replays() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let handle = uploads
        .begin(request(3), "activation-a".into())
        .await
        .unwrap();
    assert!(
        uploads
            .append(MediaUploadChunk {
                handle: handle.clone(),
                sequence: 1,
                bytes: vec![3],
            })
            .await
            .is_err()
    );
    let chunk = MediaUploadChunk {
        handle: handle.clone(),
        sequence: 0,
        bytes: vec![1, 2, 3],
    };
    assert_eq!(uploads.append(chunk.clone()).await.unwrap(), 1);
    assert_eq!(uploads.append(chunk.clone()).await.unwrap(), 1);
    let mut conflict = chunk;
    conflict.bytes[0] = 9;
    assert!(uploads.append(conflict).await.is_err());
    assert!(
        uploads
            .abort(MediaUploadFinish(handle.clone()))
            .await
            .is_ok()
    );
    assert!(
        uploads
            .abort(MediaUploadFinish(handle.clone()))
            .await
            .is_ok()
    );
    assert!(uploads.binding_handle(&handle).await.is_err());
}

#[tokio::test]
async fn reservations_and_metadata_are_bounded_before_retaining_media() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let oversized = request(1);
    let mut oversized_metadata = oversized.clone();
    oversized_metadata.scope.conversation.thread_id =
        Some("x".repeat(tinychannels_bus::module_media::MEDIA_UPLOAD_BEGIN_MAX_BYTES));
    assert!(
        uploads
            .begin(oversized_metadata, "activation-a".into())
            .await
            .is_err()
    );
    assert!(
        uploads
            .begin(
                request(tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_BYTES + 1),
                "activation-a".into(),
            )
            .await
            .is_err()
    );
    uploads
        .begin(oversized, "activation-a".into())
        .await
        .unwrap();
    uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
    assert!(
        uploads
            .begin(request(1), "activation-a".into())
            .await
            .is_err()
    );
    assert_eq!(uploads.state.lock().await.uploads.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn expired_reservations_are_rejected_and_release_their_capacity() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let handle = uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
    tokio::time::advance(UPLOAD_LIFETIME + Duration::from_millis(1)).await;
    assert!(uploads.binding_handle(&handle).await.is_err());
    assert!(
        uploads
            .append(MediaUploadChunk {
                handle,
                sequence: 0,
                bytes: vec![0],
            })
            .await
            .is_err()
    );
    uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
}

#[derive(Default)]
struct BlockingPersistence {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
    dedupe_keys: std::sync::Mutex<Vec<Option<String>>>,
}

#[async_trait::async_trait]
impl MediaPersistence for BlockingPersistence {
    async fn store_public_media(
        &self,
        request: tinychannels::host::MediaStoreRequest,
    ) -> Result<MediaUploadResult> {
        assert_eq!(
            request.metadata.scope.conversation.thread_id.as_deref(),
            Some("thread")
        );
        assert_eq!(request.metadata.scope.turn_id, "turn");
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.dedupe_keys
            .lock()
            .unwrap()
            .push(request.metadata.dedupe_key.clone());
        self.entered.notify_one();
        self.release.notified().await;
        Ok(tinychannels_bus::MediaReference {
            opaque_id: Some("tinychannels_media_v1_saved".into()),
            content_type: Some("image/png".into()),
            size_bytes: Some(1),
            kind: tinychannels_bus::MediaKind::Image,
            ..Default::default()
        })
    }
}

async fn ready_upload(uploads: &std::sync::Arc<MediaUploads>) -> MediaUploadHandle {
    let handle = uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
    uploads
        .append(MediaUploadChunk {
            handle: handle.clone(),
            sequence: 0,
            bytes: vec![42],
        })
        .await
        .unwrap();
    handle
}

#[tokio::test]
async fn finalizing_upload_keeps_capacity_and_abort_cannot_race_persistence() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let repository = std::sync::Arc::new(BlockingPersistence::default());
    let handle = ready_upload(&uploads).await;
    let _second = uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
    let finishing = {
        let uploads = uploads.clone();
        let repository = repository.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            uploads
                .finish(MediaUploadFinish(handle), "activation-a".into(), repository)
                .await
        })
    };
    repository.entered.notified().await;
    assert!(
        uploads
            .begin(request(1), "activation-a".into())
            .await
            .is_err()
    );
    assert!(
        uploads
            .abort(MediaUploadFinish(handle.clone()))
            .await
            .is_err()
    );
    let duplicate = uploads.clone().finish(
        MediaUploadFinish(handle),
        "activation-a".into(),
        repository.clone(),
    );
    tokio::pin!(duplicate);
    tokio::select! {
        biased;
        result = &mut duplicate => panic!("duplicate finalize unexpectedly completed: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    repository.release.notify_one();
    let first = finishing.await.unwrap().unwrap();
    let second = duplicate.await.unwrap();
    assert_eq!(first, second);
    assert_eq!(repository.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_the_bus_waiter_does_not_cancel_or_duplicate_finalize() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let repository = std::sync::Arc::new(BlockingPersistence::default());
    let handle = ready_upload(&uploads).await;
    let second = uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
    let waiter = {
        let uploads = uploads.clone();
        let repository = repository.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            uploads
                .finish(MediaUploadFinish(handle), "activation-a".into(), repository)
                .await
        })
    };
    repository.entered.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        uploads
            .begin(request(1), "activation-a".into())
            .await
            .is_err()
    );
    repository.release.notify_one();
    let replay = uploads
        .clone()
        .finish(
            MediaUploadFinish(handle),
            "activation-a".into(),
            repository.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        replay.opaque_id.as_deref(),
        Some("tinychannels_media_v1_saved")
    );
    assert_eq!(repository.calls.load(Ordering::SeqCst), 1);
    uploads
        .begin(request(1), "activation-a".into())
        .await
        .expect("finalization must release its reservation");
    uploads.abort(MediaUploadFinish(second)).await.unwrap();
    uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
}

#[tokio::test]
async fn invalidating_instance_releases_idle_uploads_and_keeps_finalizing_uploads() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let repository = std::sync::Arc::new(BlockingPersistence::default());
    let finalizing = ready_upload(&uploads).await;
    let pending = ready_upload(&uploads).await;
    let finishing = {
        let uploads = uploads.clone();
        let repository = repository.clone();
        let finalizing = finalizing.clone();
        tokio::spawn(async move {
            uploads
                .finish(
                    MediaUploadFinish(finalizing),
                    "activation-a".into(),
                    repository,
                )
                .await
        })
    };
    repository.entered.notified().await;

    uploads.invalidate_instance("telegram:primary").await;

    assert!(
        uploads
            .binding(&MediaUploadFinish(finalizing))
            .await
            .is_ok()
    );
    assert!(uploads.binding(&MediaUploadFinish(pending)).await.is_err());
    let mut other_instance = request(1);
    other_instance.instance = "other-instance".into();
    assert!(
        uploads
            .begin(other_instance, "activation-b".into())
            .await
            .is_ok()
    );

    repository.release.notify_one();
    assert!(finishing.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn stalled_storage_keeps_upload_finalizing_until_persistence_finishes() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let repository = std::sync::Arc::new(BlockingPersistence::default());
    let handle = ready_upload(&uploads).await;
    let finishing = {
        let uploads = uploads.clone();
        let repository = repository.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            uploads
                .finish(MediaUploadFinish(handle), "activation-a".into(), repository)
                .await
        })
    };
    repository.entered.notified().await;
    tokio::time::advance(UPLOAD_LIFETIME + Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(!finishing.is_finished());
    assert_eq!(repository.calls.load(Ordering::SeqCst), 1);
    repository.release.notify_one();
    let media = finishing.await.unwrap().unwrap();
    assert_eq!(
        media.opaque_id.as_deref(),
        Some("tinychannels_media_v1_saved")
    );
    let keys = repository.dedupe_keys.lock().unwrap();
    assert_eq!(keys.len(), 1);
}

struct PanickingPersistence;

#[async_trait::async_trait]
impl MediaPersistence for PanickingPersistence {
    async fn store_public_media(
        &self,
        _request: tinychannels::host::MediaStoreRequest,
    ) -> Result<MediaUploadResult> {
        panic!("fixture panic during media persistence");
    }
}

#[tokio::test]
async fn panicking_storage_releases_the_reservation_without_ready_media() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let handle = ready_upload(&uploads).await;
    assert!(
        uploads
            .clone()
            .finish(
                MediaUploadFinish(handle.clone()),
                "activation-a".into(),
                std::sync::Arc::new(PanickingPersistence),
            )
            .await
            .is_err()
    );
    assert!(
        uploads
            .abort(MediaUploadFinish(handle.clone()))
            .await
            .is_ok()
    );
    assert_eq!(uploads.state.lock().await.uploads.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn terminal_success_replays_and_terminal_state_is_pruned_and_bounded() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let repository = std::sync::Arc::new(BlockingPersistence::default());
    repository.release.notify_one();

    let handle = ready_upload(&uploads).await;
    assert_eq!(
        uploads.binding_handle(&handle).await.unwrap(),
        ("telegram:primary".into(), "activation-a".into())
    );
    assert_eq!(
        uploads
            .binding(&MediaUploadFinish(handle.clone()))
            .await
            .unwrap(),
        ("telegram:primary".into(), "activation-a".into())
    );
    let stored = uploads
        .clone()
        .finish(
            MediaUploadFinish(handle.clone()),
            "activation-a".into(),
            repository.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        uploads
            .clone()
            .finish(
                MediaUploadFinish(handle.clone()),
                "activation-a".into(),
                repository.clone(),
            )
            .await
            .unwrap(),
        stored
    );
    assert!(
        uploads
            .abort(MediaUploadFinish(handle.clone()))
            .await
            .is_err()
    );
    assert_eq!(repository.calls.load(Ordering::SeqCst), 1);

    for _ in 0..(TERMINAL_CAPACITY + 1) {
        let aborted = uploads
            .begin(request(1), "activation-a".into())
            .await
            .unwrap();
        uploads.abort(MediaUploadFinish(aborted)).await.unwrap();
    }
    assert_eq!(uploads.state.lock().await.terminal.len(), TERMINAL_CAPACITY);
    tokio::time::advance(TERMINAL_LIFETIME + Duration::from_millis(1)).await;
    assert!(uploads.binding(&MediaUploadFinish(handle)).await.is_err());
    assert!(uploads.state.lock().await.terminal.is_empty());
}

#[tokio::test]
async fn metadata_and_chunk_rejection_paths_do_not_retain_invalid_state() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    for invalid in [
        MediaUploadBegin {
            instance: "  ".into(),
            ..request(1)
        },
        MediaUploadBegin {
            scope: tinychannels_bus::HostVoiceScope {
                conversation: tinychannels_bus::ConversationRef {
                    id: " ".into(),
                    ..request(1).scope.conversation
                },
                ..request(1).scope
            },
            ..request(1)
        },
        MediaUploadBegin {
            scope: tinychannels_bus::HostVoiceScope {
                turn_id: " ".into(),
                ..request(1).scope
            },
            ..request(1)
        },
        request(0),
    ] {
        assert!(uploads.begin(invalid, "activation-a".into()).await.is_err());
    }
    let handle = ready_upload(&uploads).await;
    for (sequence, bytes) in [
        (0, vec![]),
        (0, vec![0, 1]),
        (1, vec![0]),
        (u32::MAX, vec![0]),
    ] {
        assert!(
            uploads
                .append(MediaUploadChunk {
                    handle: handle.clone(),
                    sequence,
                    bytes,
                })
                .await
                .is_err()
        );
    }
    assert!(
        uploads
            .clone()
            .finish(
                MediaUploadFinish(handle.clone()),
                "another-activation".into(),
                std::sync::Arc::new(BlockingPersistence::default()),
            )
            .await
            .is_err()
    );
    assert!(
        uploads
            .clone()
            .finish(
                MediaUploadFinish(MediaUploadHandle("unknown".into())),
                "activation-a".into(),
                std::sync::Arc::new(BlockingPersistence::default()),
            )
            .await
            .is_err()
    );
    assert!(
        uploads
            .abort(MediaUploadFinish(MediaUploadHandle("unknown".into())))
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn terminal_replay_expires_after_its_retention_window() {
    let uploads = std::sync::Arc::new(MediaUploads::default());
    let handle = ready_upload(&uploads).await;
    uploads
        .abort(MediaUploadFinish(handle.clone()))
        .await
        .unwrap();
    tokio::time::advance(TERMINAL_LIFETIME + Duration::from_millis(1)).await;
    assert!(
        uploads
            .abort(MediaUploadFinish(handle.clone()))
            .await
            .is_err()
    );
    assert!(uploads.binding(&MediaUploadFinish(handle)).await.is_err());
}
