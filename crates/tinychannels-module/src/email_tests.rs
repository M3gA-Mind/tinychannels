use super::*;
use tinychannels_bus::module_email::{EmailAttachmentUpload, EmailUploadChunk};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

async fn smtp_mock() -> (
    u16,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    JoinHandle<Vec<String>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (data_started, data_started_rx) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut data_started = Some(data_started);
        let mut released = Some(released);
        let (socket, _) = listener.accept().await.unwrap();
        let (read, mut write) = socket.into_split();
        let mut lines = BufReader::new(read).lines();
        let mut received = Vec::new();
        write.write_all(b"220 fixture ESMTP\r\n").await.unwrap();
        let mut in_data = false;
        while let Ok(Some(line)) = lines.next_line().await {
            received.push(line.clone());
            if in_data {
                if line == "." {
                    in_data = false;
                    write.write_all(b"250 accepted\r\n").await.unwrap();
                }
                continue;
            }
            let upper = line.to_ascii_uppercase();
            let response = if upper.starts_with("EHLO") {
                b"250-fixture\r\n250 SIZE 30000000\r\n".as_slice()
            } else if upper.starts_with("MAIL") || upper.starts_with("RCPT") {
                b"250 accepted\r\n".as_slice()
            } else if upper.starts_with("DATA") {
                in_data = true;
                if let Some(data_started) = data_started.take() {
                    let _ = data_started.send(());
                }
                b"354 continue\r\n".as_slice()
            } else if upper.starts_with("QUIT") {
                write.write_all(b"221 bye\r\n").await.unwrap();
                break;
            } else {
                b"250 ok\r\n".as_slice()
            };
            write.write_all(response).await.unwrap();
            if upper.starts_with("DATA")
                && let Some(released) = released.take()
            {
                let _ = released.await;
            }
        }
        received
    });
    (port, data_started_rx, release, task)
}

fn request(port: u16) -> EmailSendRequest {
    EmailSendRequest {
        config: tinychannels_bus::config::EmailConfig {
            smtp_host: "127.0.0.1".into(),
            smtp_port: port,
            smtp_tls: false,
            from_address: "from@example.test".into(),
            ..Default::default()
        },
        recipient: "to@example.test".into(),
        subject: "Report".into(),
        body: "The report is attached.".into(),
        attachments: vec![tinychannels_bus::EmailAttachment {
            filename: "report.txt".into(),
            content_type: "text/plain".into(),
            bytes: b"attachment-data".to_vec(),
        }],
    }
}

fn begin(port: u16, byte_count: usize) -> EmailUploadBegin {
    EmailUploadBegin {
        config: tinychannels_bus::config::EmailConfig {
            smtp_host: "127.0.0.1".into(),
            smtp_port: port,
            smtp_tls: false,
            from_address: "from@example.test".into(),
            ..Default::default()
        },
        recipient: "to@example.test".into(),
        subject: "Report".into(),
        body: "The report is attached.".into(),
        attachments: vec![EmailAttachmentUpload {
            filename: "report.txt".into(),
            content_type: "text/plain".into(),
            byte_count,
        }],
    }
}

async fn append(
    sender: &EmailSender,
    handle: &EmailUploadHandle,
    sequence: u32,
    bytes: &[u8],
) -> Result<u32> {
    sender
        .append_upload_chunk(EmailUploadChunk {
            handle: handle.clone(),
            attachment_index: 0,
            sequence,
            bytes: bytes.to_vec(),
        })
        .await
}

#[tokio::test]
async fn canceled_send_waiter_keeps_smtp_delivery_owned_until_shutdown_joins() {
    let (port, data_started, release, server) = smtp_mock().await;
    let sender = EmailSender::new();
    let waiter = tokio::spawn({
        let sender = sender.clone();
        async move { sender.send(request(port)).await }
    });
    data_started.await.unwrap();
    waiter.abort();
    let _ = waiter.await;
    release.send(()).unwrap();
    sender.shutdown().await.unwrap();
    let captured = server.await.unwrap().join("\n");
    assert!(captured.contains("multipart/mixed"), "{captured}");
    assert!(captured.contains("filename=\"report.txt\""), "{captured}");
    assert!(captured.contains("attachment-data"), "{captured}");
    assert!(sender.send(request(port)).await.is_err());
}

#[test]
fn malformed_or_oversized_email_requests_are_refused_before_enqueue() {
    let mut value = request(2525);
    value.recipient.clear();
    assert!(validate(&value).is_err());

    let mut value = request(2525);
    value.attachments[0]
        .bytes
        .resize(EMAIL_ATTACHMENT_BYTES + 1, 0);
    assert!(validate(&value).is_err());
}

#[tokio::test]
async fn upload_chunks_replay_any_prior_sequence_and_reject_conflicts() {
    let sender = EmailSender::new();
    let handle = sender.begin_upload(begin(2525, 6)).await.unwrap();
    assert_eq!(append(&sender, &handle, 0, b"ab").await.unwrap(), 1);
    assert_eq!(append(&sender, &handle, 1, b"cd").await.unwrap(), 2);
    assert_eq!(append(&sender, &handle, 0, b"ab").await.unwrap(), 2);
    assert!(append(&sender, &handle, 0, b"zz").await.is_err());
    assert!(append(&sender, &handle, 3, b"ef").await.is_err());
    assert!(
        sender
            .append_upload_chunk(EmailUploadChunk {
                handle: handle.clone(),
                attachment_index: 1,
                sequence: 2,
                bytes: b"ef".to_vec(),
            })
            .await
            .is_err()
    );
    assert!(sender.send_upload(handle.clone()).await.is_err());
    assert_eq!(append(&sender, &handle, 2, b"ef").await.unwrap(), 3);
    assert!(
        sender
            .append_upload_chunk(EmailUploadChunk {
                handle: handle.clone(),
                attachment_index: 0,
                sequence: 3,
                bytes: vec![0; EMAIL_UPLOAD_CHUNK_BYTES + 1],
            })
            .await
            .is_err()
    );
    assert!(
        sender
            .send_upload(EmailUploadHandle("not-a-uuid".into()))
            .await
            .is_err()
    );
    sender.abort_upload(handle.clone()).await.unwrap();
    sender.abort_upload(handle).await.unwrap();
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn upload_capacity_expiry_and_abort_reclaim_bounded_slots() {
    let sender = EmailSender::new();
    let largest = sender
        .begin_upload(begin(2525, EMAIL_ATTACHMENT_BYTES))
        .await
        .unwrap();
    assert!(sender.begin_upload(begin(2525, 1)).await.is_err());
    sender.abort_upload(largest).await.unwrap();
    let mut uploads = Vec::new();
    for _ in 0..UPLOAD_CAPACITY {
        uploads.push(sender.begin_upload(begin(2525, 0)).await.unwrap());
    }
    assert!(sender.begin_upload(begin(2525, 0)).await.is_err());
    sender.abort_upload(uploads.remove(0)).await.unwrap();
    let replacement = sender.begin_upload(begin(2525, 0)).await.unwrap();
    sender.abort_upload(replacement).await.unwrap();

    let expired = uploads.remove(0);
    {
        let mut store = sender.0.uploads.lock().await;
        store.uploads.get_mut(&expired.0).unwrap().expires_at = Instant::now();
    }
    assert!(append(&sender, &expired, 0, b"x").await.is_err());
    let after_expiry = sender.begin_upload(begin(2525, 0)).await.unwrap();
    sender.abort_upload(after_expiry).await.unwrap();
    for handle in uploads {
        sender.abort_upload(handle).await.unwrap();
    }
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn upload_begin_refuses_invalid_message_and_attachment_metadata() {
    let sender = EmailSender::new();
    let mut invalid = begin(2525, 0);
    invalid.recipient.clear();
    assert!(sender.begin_upload(invalid).await.is_err());

    let mut invalid = begin(2525, 0);
    invalid.attachments[0].filename.clear();
    assert!(sender.begin_upload(invalid).await.is_err());

    let mut invalid = begin(2525, EMAIL_ATTACHMENT_BYTES + 1);
    assert!(sender.begin_upload(invalid.clone()).await.is_err());
    invalid.attachments = (0..=EMAIL_ATTACHMENT_COUNT)
        .map(|_| EmailAttachmentUpload {
            filename: "part.bin".into(),
            content_type: "application/octet-stream".into(),
            byte_count: 0,
        })
        .collect();
    assert!(sender.begin_upload(invalid).await.is_err());

    let handle = sender.begin_upload(begin(2525, 129)).await.unwrap();
    for sequence in 0..EMAIL_UPLOAD_MAX_CHUNKS as u32 {
        assert_eq!(
            append(&sender, &handle, sequence, b"x").await.unwrap(),
            sequence + 1
        );
    }
    assert!(
        append(&sender, &handle, EMAIL_UPLOAD_MAX_CHUNKS as u32, b"x")
            .await
            .is_err()
    );
    sender.abort_upload(handle).await.unwrap();
    sender.shutdown().await.unwrap();
}

#[tokio::test]
async fn upload_send_cancellation_abort_and_retry_share_one_smtp_operation() {
    let (port, data_started, release, server) = smtp_mock().await;
    let sender = EmailSender::new();
    let handle = sender.begin_upload(begin(port, 6)).await.unwrap();
    append(&sender, &handle, 0, b"attach").await.unwrap();
    let first_waiter = tokio::spawn({
        let sender = sender.clone();
        let handle = handle.clone();
        async move { sender.send_upload(handle).await }
    });
    data_started.await.unwrap();
    let abort_waiter = tokio::spawn({
        let sender = sender.clone();
        let handle = handle.clone();
        async move { sender.abort_upload(handle).await }
    });
    tokio::task::yield_now().await;
    assert!(!abort_waiter.is_finished());
    first_waiter.abort();
    let _ = first_waiter.await;
    release.send(()).unwrap();
    assert!(abort_waiter.await.unwrap().is_err());
    sender.send_upload(handle.clone()).await.unwrap();
    sender.send_upload(handle).await.unwrap();
    sender.shutdown().await.unwrap();
    let captured = server.await.unwrap();
    assert_eq!(captured.iter().filter(|line| line == &"DATA").count(), 1);
    assert!(captured.join("\n").contains("attach"));
}

#[tokio::test]
async fn failed_upload_send_is_replayed_without_a_second_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let sender = EmailSender::new();
    let handle = sender.begin_upload(begin(port, 0)).await.unwrap();
    assert!(sender.send_upload(handle.clone()).await.is_err());
    assert!(sender.send_upload(handle).await.is_err());
    sender.shutdown().await.unwrap();
}
