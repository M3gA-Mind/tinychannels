use super::*;

fn mock_signal(url: String) -> SignalChannel {
    SignalChannel::with_http_client(
        url,
        "+1234567890".into(),
        None,
        vec!["+1111111111".into()],
        false,
        false,
        Client::builder().no_proxy().build().unwrap(),
    )
}

#[tokio::test]
async fn rpc_routes_direct_and_group_messages_and_typing() {
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let channel = mock_signal(format!("{}/", server.uri()));
    for (recipient, target) in [
        (
            "+1111111111",
            serde_json::json!({"recipient": ["+1111111111"]}),
        ),
        ("group:team", serde_json::json!({"groupId": "team"})),
    ] {
        for method_name in ["send", "sendTyping"] {
            let mut params = target.clone();
            params["account"] = serde_json::json!("+1234567890");
            if method_name == "send" {
                params["message"] = serde_json::json!("hello");
            }
            Mock::given(method("POST"))
                .and(path("/api/v1/rpc"))
                .and(body_partial_json(serde_json::json!({
                    "jsonrpc": "2.0", "method": method_name, "params": params
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "result": {"timestamp": 1_700_000_000_123u64}
                })))
                .expect(
                    if method_name == "sendTyping" && recipient == "group:team" {
                        2
                    } else {
                        1
                    },
                )
                .mount(&server)
                .await;
            if method_name == "send" {
                ChannelAdapter::send(&channel, typed_text_intent(recipient, "hello"))
                    .await
                    .unwrap();
            } else {
                ChannelTyping::start_typing(&channel, recipient)
                    .await
                    .unwrap();
            }
        }
    }
    ChannelTyping::stop_typing(&channel, "team").await.unwrap();
    server.verify().await;
    let requests = server.received_requests().await.unwrap();
    for request in requests {
        let body: serde_json::Value = request.body_json().unwrap();
        assert!(Uuid::parse_str(body["id"].as_str().unwrap()).is_ok());
    }
}

#[tokio::test]
async fn rpc_reports_errors_and_handles_empty_and_result_responses() {
    use wiremock::matchers::path;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let channel = mock_signal(server.uri());
    for (body, expected) in [
        ("", None),
        (
            r#"{"result":{"timestamp":42}}"#,
            Some(serde_json::json!({"timestamp": 42})),
        ),
        (r#"{}"#, None),
    ] {
        server.reset().await;
        Mock::given(path("/api/v1/rpc"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        assert_eq!(
            channel
                .rpc_request("send", serde_json::json!({}))
                .await
                .unwrap(),
            expected
        );
    }
    for (body, expected) in [
        (
            r#"{"error":{"code":-5,"message":"unknown recipient"}}"#,
            "Signal RPC error -5: unknown recipient",
        ),
        (r#"{"error":{}}"#, "Signal RPC error -1: unknown"),
        ("invalid json", "expected value"),
    ] {
        server.reset().await;
        Mock::given(path("/api/v1/rpc"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        assert!(
            channel
                .rpc_request("send", serde_json::json!({}))
                .await
                .unwrap_err()
                .to_string()
                .contains(expected)
        );
    }
    server.reset().await;
    for (status, healthy) in [(200, true), (503, false)] {
        server.reset().await;
        Mock::given(path("/api/v1/check"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        assert_eq!(
            NativeProvider::health_state(&channel).await,
            if healthy { "healthy" } else { "unhealthy" }
        );
    }
    let unavailable = mock_signal("http://127.0.0.1:0".into());
    assert_eq!(
        NativeProvider::health_state(&unavailable).await,
        "unhealthy"
    );
    assert!(
        unavailable
            .rpc_request("send", serde_json::json!({}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typed_sse_retries_failures_and_stops_after_accepted_ingress() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let channel = mock_signal(server.uri());
    let requests = AtomicUsize::new(0);
    Mock::given(method("GET"))
        .and(path("/api/v1/events"))
        .and(header("accept", "text/event-stream"))
        .and(query_param("account", "+1234567890"))
        .respond_with(move |_: &wiremock::Request| match requests.fetch_add(1, Ordering::SeqCst) {
            0 => ResponseTemplate::new(503).set_body_string("retry"),
            1 => ResponseTemplate::new(200).set_body_string(concat!(
                ": keepalive\r\n\n",
                "data: invalid\n\n",
                "data: {}\n\n",
                "event: message\n",
                "data: {\n",
                "data: \"envelope\": {\"sourceNumber\":\"+1111111111\",\"sourceName\":\" Alice \",\"dataMessage\":{\"message\":\"trailing\",\"timestamp\":1000}}}\n"
            )),
            _ => ResponseTemplate::new(200).set_body_string(
                "data: {\"envelope\":{\"sourceNumber\":\"+1111111111\",\"dataMessage\":{\"message\":\"after reconnect\",\"timestamp\":2000}}}\n\n"
            ),
        })
        .expect(2).mount(&server).await;
    struct StopAfterFirst {
        channel: Arc<SignalChannel>,
        tx: mpsc::Sender<tinychannels_bus::ChannelIngress>,
    }
    #[async_trait]
    impl ChannelInboundSink for StopAfterFirst {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            ingress: tinychannels_bus::ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.tx.send(ingress).await.map_err(|_| {
                tinychannels_bus::channel::ChannelSendError {
                    message: "test receiver closed".into(),
                    ..Default::default()
                }
            })?;
            NativeProvider::stop_listener(self.channel.as_ref()).await
        }
    }
    let channel = Arc::new(channel);
    let (tx, mut rx) = mpsc::channel(1);
    let sink = Arc::new(StopAfterFirst {
        channel: channel.clone(),
        tx,
    });
    let listener_channel = channel.clone();
    let listener = tokio::spawn(async move {
        NativeProvider::start_listener(listener_channel.as_ref(), sink.as_ref()).await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        let message = match rx.recv().await.unwrap() {
            tinychannels_bus::ChannelIngress::Inbound(message) => message,
            other => panic!("expected typed inbound, got {other:?}"),
        };
        assert_eq!(message.text, "trailing");
        assert_eq!(message.timestamp_unix_ms, Some(1000));
        assert_eq!(message.sender.name.as_deref(), Some("Alice"));
        assert_eq!(message.conversation.id, "+1111111111");
        listener.await.unwrap().unwrap();
        server.verify().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn typed_sse_returns_without_connecting_when_already_cancelled() {
    use crate::channel::ChannelInboundSink;
    use async_trait::async_trait;
    use tokio::sync::watch;
    use wiremock::MockServer;

    struct IgnoreSink;
    #[async_trait]
    impl ChannelInboundSink for IgnoreSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _ingress: tinychannels_bus::ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            Ok(())
        }
    }

    let server = MockServer::start().await;
    let channel = mock_signal(server.uri());
    let (_tx, rx) = watch::channel(true);
    let sink = IgnoreSink;

    tokio::time::timeout(Duration::from_secs(2), channel.listen_typed(&sink, rx))
        .await
        .expect("pre-cancelled listener should stop promptly")
        .unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_sse_cancellation_interrupts_trailing_event_delivery() {
    use std::sync::Arc;
    use tokio::sync::{Notify, watch};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::path("/api/v1/events"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "data: {\"envelope\":{\"sourceNumber\":\"+1111111111\",\"dataMessage\":{\"message\":\"tail\",\"timestamp\":1}}}\n",
        ))
        .expect(1)
        .mount(&server)
        .await;

    struct BlockingSink {
        entered: Notify,
    }
    #[async_trait]
    impl ChannelInboundSink for BlockingSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _ingress: tinychannels_bus::ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.entered.notify_one();
            std::future::pending().await
        }
    }

    let channel = mock_signal(server.uri());
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let sink = Arc::new(BlockingSink {
        entered: Notify::new(),
    });
    let task = tokio::spawn({
        let sink = sink.clone();
        async move { channel.listen_typed(sink.as_ref(), cancel_rx).await }
    });
    tokio::time::timeout(Duration::from_secs(5), sink.entered.notified())
        .await
        .expect("trailing event should reach the sink");
    cancel_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancellation should interrupt pending sink delivery")
        .unwrap()
        .unwrap();
    server.verify().await;
}

#[tokio::test]
async fn typed_sse_skips_invalid_utf8_and_retries_malformed_trailing_data() {
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, watch};

    async fn read_request(stream: &mut tokio::net::TcpStream) {
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let reconnect_seen = Arc::new(Notify::new());
    let server_reconnect_seen = reconnect_seen.clone();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_request(&mut first).await;
        first
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        // The invalid chunk is skipped, while the following valid bytes leave
        // malformed trailing event data for the end-of-stream parser.
        first.write_all(b"1\r\n\xff\r\n").await.unwrap();
        first
            .write_all(b"10\r\ndata: {not-json\n\r\n0\r\n\r\n")
            .await
            .unwrap();
        drop(first);

        let (mut second, _) = listener.accept().await.unwrap();
        read_request(&mut second).await;
        second
            .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        server_reconnect_seen.notify_one();
    });

    struct IgnoreSink;
    #[async_trait]
    impl ChannelInboundSink for IgnoreSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _ingress: tinychannels_bus::ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            Ok(())
        }
    }

    let channel = mock_signal(format!("http://{address}/"));
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let task = tokio::spawn(async move { channel.listen_typed(&IgnoreSink, cancel_rx).await });
    tokio::time::timeout(Duration::from_secs(6), reconnect_seen.notified())
        .await
        .expect("listener should reconnect after malformed trailing data");
    cancel_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancel should interrupt retry backoff")
        .unwrap()
        .unwrap();
    server.await.unwrap();
}
