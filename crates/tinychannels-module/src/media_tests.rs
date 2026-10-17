use super::*;
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
    assert!(uploads
        .begin(oversized_metadata, "activation-a".into())
        .await
        .is_err());
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
    assert!(uploads
        .begin(request(1), "activation-a".into())
        .await
        .is_err());
    uploads.abort(MediaUploadFinish(second)).await.unwrap();
    uploads
        .begin(request(1), "activation-a".into())
        .await
        .unwrap();
}
