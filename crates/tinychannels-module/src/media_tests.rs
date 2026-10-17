use super::*;

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
    let uploads = MediaUploads::default();
    let handle = uploads.begin(request(3), 17).await.unwrap();
    assert!(uploads
        .append(MediaUploadChunk {
            handle: handle.clone(),
            sequence: 1,
            bytes: vec![3],
        })
        .await
        .is_err());
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
    assert!(uploads
        .abort(MediaUploadFinish(handle.clone()))
        .await
        .is_ok());
    assert!(uploads
        .abort(MediaUploadFinish(handle.clone()))
        .await
        .is_ok());
    assert!(uploads
        .binding_handle(&handle)
        .await
        .is_err());
}

#[tokio::test]
async fn reservations_and_metadata_are_bounded_before_retaining_media() {
    let uploads = MediaUploads::default();
    let oversized = request(1);
    let mut oversized_metadata = oversized.clone();
    oversized_metadata.scope.conversation.thread_id = Some(
        "x".repeat(tinychannels_bus::module_media::MEDIA_UPLOAD_BEGIN_MAX_BYTES),
    );
    assert!(uploads.begin(oversized_metadata, 1).await.is_err());
    assert!(uploads
        .begin(
            request(tinychannels_bus::module_media::MEDIA_UPLOAD_MAX_BYTES + 1),
            1,
        )
        .await
        .is_err());
    uploads.begin(oversized, 1).await.unwrap();
    uploads.begin(request(1), 1).await.unwrap();
    assert!(uploads.begin(request(1), 1).await.is_err());
    assert_eq!(uploads.state.lock().await.uploads.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn expired_reservations_are_rejected_and_release_their_capacity() {
    let uploads = MediaUploads::default();
    let handle = uploads.begin(request(1), 5).await.unwrap();
    tokio::time::advance(UPLOAD_LIFETIME + Duration::from_millis(1)).await;
    assert!(uploads.binding_handle(&handle).await.is_err());
    assert!(uploads.append(MediaUploadChunk {
        handle,
        sequence: 0,
        bytes: vec![0],
    }).await.is_err());
    uploads.begin(request(1), 5).await.unwrap();
}
