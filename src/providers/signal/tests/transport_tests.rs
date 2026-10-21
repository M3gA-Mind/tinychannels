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
