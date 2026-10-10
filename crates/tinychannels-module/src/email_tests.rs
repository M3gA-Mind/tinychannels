use super::*;
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
