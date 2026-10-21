use super::*;

#[tokio::test]
async fn typed_listener_connects_to_sse_publishes_and_stops() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/v1/events"))
        .and(wiremock::matchers::query_param("account", ACCOUNT))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(
            format!(
                "data: {}\n\n",
                json!({"envelope": {
                    "sourceNumber": PEER,
                    "sourceName": "Ada",
                    "timestamp": 1_700_000_000_500u64,
                    "dataMessage": {"message": "typed hello", "timestamp": 1_700_000_000_500u64}
                }})
            ),
            "text/event-stream",
        ))
        .mount(&server)
        .await;
    let channel = Arc::new(channel_for(&server));
    let sink = Arc::new(InboundRecorder::default());
    let listener_channel = channel.clone();
    let listener_sink = sink.clone();
    let listener = tokio::spawn(async move {
        ChannelAdapter::start(listener_channel.as_ref(), listener_sink.as_ref()).await
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !sink.0.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed Signal SSE event should reach the adapter sink");
    {
        let got = sink.0.lock().unwrap();
        let ChannelIngress::Inbound(envelope) = &got[0] else {
            panic!("expected typed inbound message, got {:?}", got[0]);
        };
        assert_eq!(envelope.text, "typed hello");
        assert_eq!(envelope.sender.name.as_deref(), Some("Ada"));
    }

    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), listener)
        .await
        .expect("Signal listener should acknowledge cancellation")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stop_cancels_a_blocked_typed_ingress_push() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/v1/events"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(
            format!(
                "data: {}\n\n",
                json!({"envelope": {"sourceNumber": PEER, "timestamp": 10,
                    "dataMessage": {"message": "blocked", "timestamp": 10}}})
            ),
            "text/event-stream",
        ))
        .mount(&server)
        .await;
    let channel = Arc::new(channel_for(&server));
    let sink = Arc::new(BlockingInboundSink(Notify::new()));
    let listener_channel = channel.clone();
    let listener_sink = sink.clone();
    let listener = tokio::spawn(async move {
        ChannelAdapter::start(listener_channel.as_ref(), listener_sink.as_ref()).await
    });
    tokio::time::timeout(Duration::from_secs(2), sink.0.notified())
        .await
        .expect("listener should reach the deliberately blocked sink");

    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), listener)
        .await
        .expect("stop must interrupt sink backpressure")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn aborting_listener_start_releases_registration_for_restart() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/v1/events"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(
            format!(
                "data: {}\n\n",
                json!({"envelope": {"sourceNumber": PEER, "timestamp": 11,
                    "dataMessage": {"message": "restart", "timestamp": 11}}})
            ),
            "text/event-stream",
        ))
        .mount(&server)
        .await;
    let channel = Arc::new(channel_for(&server));
    let blocking = Arc::new(BlockingInboundSink(Notify::new()));
    let first_channel = channel.clone();
    let first_sink = blocking.clone();
    let first = tokio::spawn(async move {
        ChannelAdapter::start(first_channel.as_ref(), first_sink.as_ref()).await
    });
    tokio::time::timeout(Duration::from_secs(2), blocking.0.notified())
        .await
        .expect("first listener should reach the blocked sink");
    first.abort();
    let _ = first.await;

    let restart_sink = Arc::new(InboundRecorder::default());
    let restart_channel = channel.clone();
    let sink_for_restart = restart_sink.clone();
    let restart = tokio::spawn(async move {
        ChannelAdapter::start(restart_channel.as_ref(), sink_for_restart.as_ref()).await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let requests = server.received_requests().await.unwrap_or_default();
            if requests.len() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a dropped start future must not strand listener registration");
    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), restart)
        .await
        .expect("restarted listener should stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn typed_listener_propagates_rejected_lifecycle_events() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/v1/events"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(
            format!(
                "data: {}\n\n",
                json!({"envelope": {"sourceNumber": PEER, "timestamp": 12,
                "dataMessage": {"timestamp": 12, "reaction": {
                    "emoji": "👍", "targetSentTimestamp": 9
                }}}})
            ),
            "text/event-stream",
        ))
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let error = ChannelAdapter::start(&channel, &RejectingInboundSink)
        .await
        .expect_err("a rejected ordered event must reach listener supervision");
    assert!(error.message.contains("listener"));
}
