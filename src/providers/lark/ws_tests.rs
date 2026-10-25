//! WebSocket long-connection tests for the Lark channel. The endpoint lookup
//! is answered by the local TLS fixture, which hands out a loopback `ws://`
//! URL served by a scripted pbbp2 gateway.

use super::*;
use crate::channel::{ChannelAdapter, ChannelInboundSink};
use crate::providers::http_test_support::HttpsServer;
use tinychannels_bus::ChannelIngress;
use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;

/// Hang guard only; every step below is driven by an explicit read.
const GUARD: Duration = Duration::from_secs(10);

struct Gateway {
    _endpoint: HttpsServer,
    listener: TcpListener,
    channel: LarkChannel,
}

async fn gateway(allowed_users: Vec<String>) -> Gateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint = HttpsServer::start(move |_| {
        let body = serde_json::json!({
            "code": 0,
            "data": {
                "URL": format!("ws://127.0.0.1:{port}/ws?device_id=d1&service_id=42"),
                "ClientConfig": { "PingInterval": 120 }
            }
        });
        (200, body.to_string())
    })
    .await;
    let mut channel = LarkChannel::new(
        "cli_app".into(),
        "secret".into(),
        "token".into(),
        None,
        allowed_users,
    );
    channel.http_client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .timeout(GUARD)
        .danger_accept_invalid_certs(true)
        .resolve("open.feishu.cn", endpoint.address)
        .build()
        .unwrap();
    Gateway {
        _endpoint: endpoint,
        listener,
        channel,
    }
}

async fn accept(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (socket, _) = tokio::time::timeout(GUARD, listener.accept())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(GUARD, tokio_tungstenite::accept_async(socket))
        .await
        .unwrap()
        .unwrap()
}

async fn next_msg(ws: &mut WebSocketStream<TcpStream>) -> WsMsg {
    tokio::time::timeout(GUARD, ws.next())
        .await
        .unwrap()
        .expect("client closed the socket")
        .unwrap()
}

async fn next_frame(ws: &mut WebSocketStream<TcpStream>) -> PbFrame {
    match next_msg(ws).await {
        WsMsg::Binary(bytes) => PbFrame::decode(&bytes[..]).unwrap(),
        other => panic!("expected a binary frame, got {other:?}"),
    }
}

/// Next frame that is not a periodic heartbeat ping.
async fn next_non_ping(ws: &mut WebSocketStream<TcpStream>) -> PbFrame {
    loop {
        let frame = next_frame(ws).await;
        if !(frame.method == 0 && frame.header_value("type") == "ping") {
            return frame;
        }
    }
}

fn frame(seq_id: u64, method: i32, headers: &[(&str, &str)], payload: Option<Vec<u8>>) -> WsMsg {
    let frame = PbFrame {
        seq_id,
        log_id: 7,
        service: 42,
        method,
        headers: headers
            .iter()
            .map(|(key, value)| PbHeader {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect(),
        payload,
    };
    WsMsg::Binary(frame.encode_to_vec().into())
}

fn event(message_id: &str, open_id: &str, message: serde_json::Value) -> Vec<u8> {
    let mut msg = serde_json::json!({
        "message_id": message_id,
        "chat_id": "oc_chat",
        "chat_type": "p2p",
        "message_type": "text",
    });
    msg.as_object_mut()
        .unwrap()
        .extend(message.as_object().unwrap().clone());
    serde_json::json!({
        "header": { "event_type": "im.message.receive_v1", "event_id": format!("ev-{message_id}") },
        "event": {
            "sender": { "sender_id": { "open_id": open_id } },
            "message": msg,
        }
    })
    .to_string()
    .into_bytes()
}

fn text(t: &str) -> serde_json::Value {
    serde_json::json!({ "content": serde_json::json!({ "text": t }).to_string() })
}

/// Send one DATA frame and check the client's immediate ACK.
async fn send_data(
    ws: &mut WebSocketStream<TcpStream>,
    seq_id: u64,
    headers: &[(&str, &str)],
    payload: Vec<u8>,
) {
    ws.send(frame(seq_id, 1, headers, Some(payload)))
        .await
        .unwrap();
    let ack = next_non_ping(ws).await;
    assert_eq!(ack.seq_id, seq_id);
    assert_eq!(ack.method, 1);
    assert_eq!(ack.header_value("biz_rt"), "0");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(ack.payload.as_deref().unwrap()).unwrap(),
        serde_json::json!({"code": 200, "headers": {}, "data": []})
    );
}

async fn send_event(ws: &mut WebSocketStream<TcpStream>, seq_id: u64, payload: Vec<u8>) {
    send_data(ws, seq_id, &[("type", "event")], payload).await;
}

#[derive(Clone)]
struct RecordingInboundSink(tokio::sync::mpsc::UnboundedSender<ChannelIngress>);

#[async_trait::async_trait]
impl ChannelInboundSink for RecordingInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(&self, ingress: ChannelIngress) -> Result<(), ChannelSendError> {
        self.0.send(ingress).map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "test sink closed".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

struct RejectFirstInboundSink {
    attempts: std::sync::atomic::AtomicUsize,
    tx: tokio::sync::mpsc::UnboundedSender<ChannelIngress>,
}

#[async_trait::async_trait]
impl ChannelInboundSink for RejectFirstInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(&self, ingress: ChannelIngress) -> Result<(), ChannelSendError> {
        if self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            return Err(ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "retry this event".into(),
                retryable: true,
                ..Default::default()
            });
        }
        self.tx.send(ingress).map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "test sink closed".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

struct BlockingInboundSink(tokio::sync::Notify);

#[async_trait::async_trait]
impl ChannelInboundSink for BlockingInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(&self, _ingress: ChannelIngress) -> Result<(), ChannelSendError> {
        self.0.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn stale_lark_listener_cleanup_does_not_release_a_new_owner() {
    let channel = LarkChannel::new(
        "cli_app".into(),
        "secret".into(),
        "token".into(),
        None,
        vec!["*".into()],
    );
    let (_old_cancel, old_id) = channel.begin_listener().unwrap();
    let old_registration = LarkListenerRegistration {
        channel: &channel,
        id: old_id,
    };
    channel.cancel_listener();
    channel.end_listener(old_id);

    let (_new_cancel, new_id) = channel.begin_listener().unwrap();
    let new_registration = LarkListenerRegistration {
        channel: &channel,
        id: new_id,
    };
    drop(old_registration);
    assert!(channel.begin_listener().is_err());

    channel.cancel_listener();
    drop(new_registration);
    let (_cancel, id) = channel.begin_listener().unwrap();
    channel.end_listener(id);
}

#[tokio::test]
async fn public_adapter_retries_lark_message_after_typed_sink_rejection() {
    let gateway = gateway(vec!["ou_alice".into()]).await;
    let adapter: Arc<dyn ChannelAdapter> = Arc::new(gateway.channel.clone());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(RejectFirstInboundSink {
        attempts: std::sync::atomic::AtomicUsize::new(0),
        tx,
    });
    let sink_trait: Arc<dyn ChannelInboundSink> = sink;
    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink_trait.clone();
        async move { adapter.start(sink.as_ref()).await }
    });

    let mut first_connection = accept(&gateway.listener).await;
    let initial_ping = next_frame(&mut first_connection).await;
    send_event(
        &mut first_connection,
        initial_ping.seq_id + 1,
        event("om-retry", "ou_alice", text("retry me")),
    )
    .await;
    drop(first_connection);
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("sink rejection should release the failed WebSocket attempt")
        .expect("Lark listener must not panic")
        .expect("Lark listener must stop cleanly after the rejected event");

    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink_trait.clone();
        async move { adapter.start(sink.as_ref()).await }
    });
    let mut retry_connection = accept(&gateway.listener).await;
    let retry_ping = next_frame(&mut retry_connection).await;
    send_event(
        &mut retry_connection,
        retry_ping.seq_id + 1,
        event("om-retry", "ou_alice", text("retry me")),
    )
    .await;
    let ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("the retry should reach the sink")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(envelope) = ingress else {
        panic!("Lark retry should arrive as typed ingress");
    };
    assert_eq!(envelope.message_id, "om-retry");
    assert_eq!(envelope.text, "retry me");

    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("Lark stop must join the retried listener")
        .expect("Lark listener must not panic")
        .expect("Lark listener must stop cleanly");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn public_webhook_stop_cancels_blocked_lark_sink() {
    use crate::config::{LarkConfig, LarkReceiveMode};

    let port_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = port_probe.local_addr().unwrap().port();
    drop(port_probe);
    let channel = Arc::new(LarkChannel::from_config(&LarkConfig {
        app_id: "app-id".into(),
        app_secret: "app-secret".into(),
        encrypt_key: None,
        verification_token: Some("verify-token".into()),
        allowed_users: vec!["ou-alice".into()],
        use_feishu: true,
        receive_mode: LarkReceiveMode::Webhook,
        port: Some(port),
    }));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let sink = Arc::new(BlockingInboundSink(tokio::sync::Notify::new()));
    let sink_trait: Arc<dyn ChannelInboundSink> = sink.clone();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(GUARD)
        .build()
        .unwrap();
    let listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink_trait.clone();
        async move { adapter.start(sink.as_ref()).await }
    });

    let payload = serde_json::json!({
        "header": {"event_type": "im.message.receive_v1", "token": "verify-token"},
        "event": {
            "sender": {"sender_type": "user", "sender_id": {"open_id": "ou-alice"}},
            "message": {
                "message_id": "om-blocked", "chat_id": "oc-chat", "chat_type": "p2p",
                "message_type": "text", "content": "{\"text\":\"blocked\"}",
                "create_time": "1760900000000"
            }
        }
    });
    let challenge = serde_json::json!({
        "type": "url_verification", "challenge": "ready", "token": "verify-token"
    });
    tokio::time::timeout(GUARD, async {
        loop {
            if client
                .post(format!("http://127.0.0.1:{port}/lark"))
                .json(&challenge)
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Lark webhook should bind before the event request");
    let request = tokio::spawn(async move {
        client
            .post(format!("http://127.0.0.1:{port}/lark"))
            .json(&payload)
            .send()
            .await
    });
    tokio::time::timeout(GUARD, sink.0.notified())
        .await
        .expect("listener should enter the blocked sink");

    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, listener)
        .await
        .expect("stop must cancel processing blocked in the typed sink")
        .expect("Lark listener must not panic")
        .expect("Lark listener must stop cleanly");
    assert_eq!(
        tokio::time::timeout(GUARD, request)
            .await
            .expect("webhook handler must be released when listener stops")
            .expect("request task must not panic")
            .expect("local webhook request must complete")
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn public_adapter_start_owns_lark_websocket_until_stop_and_restart() {
    let gateway = gateway(vec!["ou_alice".into()]).await;
    let channel = Arc::new(gateway.channel.clone());
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);

    for (message_id, message_text) in [
        ("om-first", "first connection"),
        ("om-second", "after restart"),
    ] {
        let mut listener = tokio::spawn({
            let adapter = adapter.clone();
            let sink = sink.clone();
            async move { adapter.start(&sink).await }
        });
        let mut ws = tokio::select! {
            result = &mut listener => panic!("Lark adapter start returned before WebSocket ownership: {result:?}"),
            socket = accept(&gateway.listener) => socket,
        };
        let initial_ping = next_frame(&mut ws).await;
        assert_eq!(initial_ping.header_value("type"), "ping");

        send_event(
            &mut ws,
            initial_ping.seq_id + 1,
            event(message_id, "ou_alice", text(message_text)),
        )
        .await;
        let ingress = tokio::select! {
            result = &mut listener => panic!("Lark adapter returned before typed ingress: {result:?}"),
            received = tokio::time::timeout(GUARD, rx.recv()) => received
                .expect("admitted Lark event should reach supplied typed sink")
                .expect("test sink remains connected"),
        };
        let ChannelIngress::Inbound(envelope) = ingress else {
            panic!("Lark message must arrive as typed inbound ingress");
        };
        assert_eq!(envelope.message_id, message_id);
        assert_eq!(envelope.sender.id, "ou_alice");
        assert_eq!(envelope.conversation.id, "oc_chat");
        assert_eq!(envelope.text, message_text);

        adapter.stop().await.unwrap();
        tokio::time::timeout(GUARD, &mut listener)
            .await
            .expect("Lark stop must join its WebSocket listener")
            .expect("Lark listener must not panic")
            .expect("Lark listener must stop cleanly");
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn public_adapter_webhook_authenticates_admits_deduplicates_and_restarts() {
    use crate::config::{LarkConfig, LarkReceiveMode};

    async fn post_until_ready(
        client: &reqwest::Client,
        port: u16,
        payload: &serde_json::Value,
    ) -> reqwest::Response {
        tokio::time::timeout(GUARD, async {
            for _ in 0..100 {
                match client
                    .post(format!("http://127.0.0.1:{port}/lark"))
                    .json(payload)
                    .send()
                    .await
                {
                    Ok(response) => return response,
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
            panic!("public Lark webhook did not become ready");
        })
        .await
        .expect("public Lark webhook readiness request must finish within the guard")
    }

    fn challenge(token: &str, value: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "url_verification",
            "challenge": value,
            "token": token
        })
    }

    fn message(message_id: &str, open_id: &str, token: &str, body: &str) -> serde_json::Value {
        serde_json::json!({
            "header": {
                "event_type": "im.message.receive_v1",
                "event_id": format!("event-{message_id}"),
                "token": token
            },
            "event": {
                "sender": {
                    "sender_type": "user",
                    "sender_id": {"open_id": open_id}
                },
                "message": {
                    "message_id": message_id,
                    "chat_id": "oc-webhook-chat",
                    "chat_type": "p2p",
                    "message_type": "text",
                    "content": serde_json::json!({"text": body}).to_string(),
                    "create_time": "1760900000000"
                }
            }
        })
    }

    let port_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = port_probe.local_addr().unwrap().port();
    drop(port_probe);
    let channel = Arc::new(LarkChannel::from_config(&LarkConfig {
        app_id: "app-id".into(),
        app_secret: "app-secret".into(),
        encrypt_key: None,
        verification_token: Some("verify-token".into()),
        allowed_users: vec!["ou-alice".into()],
        use_feishu: true,
        receive_mode: LarkReceiveMode::Webhook,
        port: Some(port),
    }));
    let adapter: Arc<dyn ChannelAdapter> = channel;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = RecordingInboundSink(tx);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(GUARD)
        .build()
        .unwrap();

    let mut listener = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let initial_challenge = challenge("verify-token", "challenge-1");
    let challenge_response = tokio::select! {
        result = &mut listener => panic!("Lark adapter start returned before webhook ownership: {result:?}"),
        response = post_until_ready(&client, port, &initial_challenge) => response,
    };
    assert_eq!(challenge_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        challenge_response
            .json::<serde_json::Value>()
            .await
            .unwrap(),
        serde_json::json!({"challenge": "challenge-1"})
    );

    let rejected = client
        .post(format!("http://127.0.0.1:{port}/lark"))
        .json(&message("om-invalid-token", "ou-alice", "wrong", "reject"))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::FORBIDDEN);

    let denied = post_until_ready(
        &client,
        port,
        &message("om-denied", "ou-mallory", "verify-token", "deny"),
    )
    .await;
    assert_eq!(denied.status(), reqwest::StatusCode::OK);
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    for _ in 0..2 {
        let accepted = post_until_ready(
            &client,
            port,
            &message("om-accepted", "ou-alice", "verify-token", "admitted"),
        )
        .await;
        assert_eq!(accepted.status(), reqwest::StatusCode::OK);
    }
    let barrier = post_until_ready(
        &client,
        port,
        &message("om-barrier", "ou-alice", "verify-token", "after duplicate"),
    )
    .await;
    assert_eq!(barrier.status(), reqwest::StatusCode::OK);
    let first_ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("admitted webhook message should reach the supplied sink")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(envelope) = first_ingress else {
        panic!("Lark webhook messages must use typed inbound ingress");
    };
    assert_eq!(envelope.message_id, "om-accepted");
    assert_eq!(envelope.sender.id, "ou-alice");
    assert_eq!(envelope.conversation.id, "oc-webhook-chat");
    assert_eq!(envelope.text, "admitted");
    let barrier_ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("barrier event should follow the first admitted event")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(envelope) = barrier_ingress else {
        panic!("Lark webhook messages must use typed inbound ingress");
    };
    assert_eq!(envelope.message_id, "om-barrier");
    assert_eq!(envelope.conversation.id, "oc-webhook-chat");
    assert_eq!(envelope.text, "after duplicate");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut listener)
        .await
        .expect("Lark webhook stop must join its listener")
        .expect("Lark webhook listener must not panic")
        .expect("Lark webhook listener must stop cleanly");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(
        client
            .post(format!("http://127.0.0.1:{port}/lark"))
            .json(&challenge("verify-token", "stopped"))
            .send()
            .await
            .is_err()
    );

    let mut restarted = tokio::spawn({
        let adapter = adapter.clone();
        let sink = sink.clone();
        async move { adapter.start(&sink).await }
    });
    let restart_challenge = challenge("verify-token", "challenge-2");
    let challenge_response = tokio::select! {
        result = &mut restarted => panic!("Lark restart returned before webhook ownership: {result:?}"),
        response = post_until_ready(&client, port, &restart_challenge) => response,
    };
    assert_eq!(challenge_response.status(), reqwest::StatusCode::OK);
    let accepted = post_until_ready(
        &client,
        port,
        &message("om-after-restart", "ou-alice", "verify-token", "restarted"),
    )
    .await;
    assert_eq!(accepted.status(), reqwest::StatusCode::OK);
    let ingress = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("restarted webhook should publish new admitted messages")
        .expect("test sink remains connected");
    let ChannelIngress::Inbound(envelope) = ingress else {
        panic!("Lark webhook messages must use typed inbound ingress");
    };
    assert_eq!(envelope.message_id, "om-after-restart");
    assert_eq!(envelope.conversation.id, "oc-webhook-chat");
    adapter.stop().await.unwrap();
    tokio::time::timeout(GUARD, &mut restarted)
        .await
        .expect("stop must join restarted Lark webhook listener")
        .expect("restarted Lark listener must not panic")
        .expect("restarted Lark listener must stop cleanly");
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn ws_listener_acks_reassembles_filters_and_forwards_messages() {
    let gw = gateway(vec!["ou_alice".into()]).await;
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let channel = gw.channel;
    let listen =
        tokio::spawn(async move { crate::provider_test_support::listen_v1(&channel, tx).await });
    let mut ws = accept(&gw.listener).await;

    let initial = next_frame(&mut ws).await;
    assert_eq!(
        (initial.seq_id, initial.method, initial.service),
        (1, 0, 42)
    );
    assert_eq!(initial.header_value("type"), "ping");

    // Transport-level traffic: pings are answered, the rest ignored.
    ws.send(WsMsg::Ping(b"hb".to_vec().into())).await.unwrap();
    assert_eq!(next_msg(&mut ws).await, WsMsg::Pong(b"hb".to_vec().into()));
    ws.send(WsMsg::Pong(Vec::new().into())).await.unwrap();
    ws.send(WsMsg::Text("ignored".into())).await.unwrap();
    ws.send(WsMsg::Binary(vec![0xff, 0xff, 0xff].into()))
        .await
        .unwrap();

    // A pong carrying a new interval restarts the heartbeat, whose first
    // tick sends a ping straight away.
    let pong_cfg = br#"{"PingInterval":15}"#.to_vec();
    ws.send(frame(2, 0, &[("type", "pong")], Some(pong_cfg.clone())))
        .await
        .unwrap();
    let ping = next_frame(&mut ws).await;
    assert_eq!((ping.seq_id, ping.method), (2, 0));
    assert_eq!(ping.header_value("type"), "ping");
    // Same interval again, and a bare pong: no restart.
    ws.send(frame(3, 0, &[("type", "pong")], Some(pong_cfg)))
        .await
        .unwrap();
    ws.send(frame(4, 0, &[("type", "pong")], None))
        .await
        .unwrap();

    // Non-event DATA is acknowledged but not dispatched.
    send_data(&mut ws, 100, &[("type", "card")], b"{}".to_vec()).await;

    // Fragmented event; the first fragment's slot count is superseded.
    let full = event("om_frag", "ou_alice", text("fragmented hello"));
    let (head, tail) = full.split_at(full.len() / 2);
    send_data(
        &mut ws,
        101,
        &[
            ("type", "event"),
            ("message_id", "om_frag"),
            ("sum", "3"),
            ("seq", "0"),
        ],
        head.to_vec(),
    )
    .await;
    send_data(
        &mut ws,
        102,
        &[
            ("type", "event"),
            ("message_id", "om_frag"),
            ("sum", "2"),
            ("seq", "0"),
        ],
        head.to_vec(),
    )
    .await;
    send_data(
        &mut ws,
        103,
        &[
            ("type", "event"),
            ("message_id", "om_frag"),
            ("sum", "2"),
            ("seq", "1"),
        ],
        tail.to_vec(),
    )
    .await;

    // Events that are dropped after the ACK.
    send_event(&mut ws, 104, b"not json".to_vec()).await;
    send_event(
        &mut ws,
        105,
        br#"{"header":{"event_type":"im.chat.disbanded_v1","event_id":"e"},"event":{}}"#.to_vec(),
    )
    .await;
    send_event(
        &mut ws,
        106,
        br#"{"header":{"event_type":"im.message.receive_v1","event_id":"e"},"event":{}}"#.to_vec(),
    )
    .await;
    let mut from_bot: serde_json::Value =
        serde_json::from_slice(&event("om_bot", "ou_alice", text("beep"))).unwrap();
    from_bot["event"]["sender"]["sender_type"] = "bot".into();
    send_event(&mut ws, 107, from_bot.to_string().into_bytes()).await;
    send_event(&mut ws, 108, event("om_m", "ou_mallory", text("let me in"))).await;

    send_event(
        &mut ws,
        109,
        event("om_2", "ou_alice", text("hello @_user_1 there")),
    )
    .await;
    send_event(
        &mut ws,
        110,
        event("om_2", "ou_alice", text("hello @_user_1 there")),
    )
    .await;
    send_event(
        &mut ws,
        111,
        event("om_3", "ou_alice", serde_json::json!({"content": "nope"})),
    )
    .await;
    send_event(&mut ws, 112, event("om_4", "ou_alice", text(""))).await;
    let post = serde_json::json!({
        "en_us": { "title": "Title", "content": [[{"tag": "text", "text": "Body"}]] }
    })
    .to_string();
    send_event(
        &mut ws,
        113,
        event(
            "om_5",
            "ou_alice",
            serde_json::json!({"message_type": "post", "content": post}),
        ),
    )
    .await;
    send_event(
        &mut ws,
        114,
        event(
            "om_6",
            "ou_alice",
            serde_json::json!({"message_type": "post", "content": "{}"}),
        ),
    )
    .await;
    send_event(
        &mut ws,
        115,
        event(
            "om_7",
            "ou_alice",
            serde_json::json!({"message_type": "image", "content": "{}"}),
        ),
    )
    .await;
    send_event(&mut ws, 116, event("om_8", "ou_alice", text("@_user_1"))).await;
    let mut group = text("group without mention");
    group["chat_type"] = "group".into();
    send_event(&mut ws, 117, event("om_9", "ou_alice", group)).await;
    let mut group = text("@_user_1 group hi");
    group["chat_type"] = "group".into();
    group["mentions"] = serde_json::json!([{"key": "@_user_1"}]);
    send_event(&mut ws, 118, event("om_10", "ou_alice", group)).await;

    ws.close(None).await.unwrap();
    tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .expect("a closed socket ends listen cleanly so the caller reconnects");

    let mut delivered = Vec::new();
    while let Ok(message) = rx.try_recv() {
        assert_eq!(message.channel, "lark");
        assert_eq!(message.sender, "ou_alice");
        assert_eq!(message.reply_target, "oc_chat");
        delivered.push(message.content);
    }
    assert_eq!(
        delivered,
        [
            "fragmented hello",
            "hello there",
            "Title\n\nBody",
            "group hi"
        ]
    );
}

#[tokio::test]
async fn ws_listener_stops_when_the_receiver_is_dropped() {
    let gw = gateway(vec!["*".into()]).await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let channel = gw.channel;
    let listen =
        tokio::spawn(async move { crate::provider_test_support::listen_v1(&channel, tx).await });
    let mut ws = accept(&gw.listener).await;
    next_frame(&mut ws).await;

    drop(rx);
    send_event(
        &mut ws,
        1,
        event("om_1", "ou_anyone", text("into the void")),
    )
    .await;
    tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn ws_listener_returns_after_an_abrupt_disconnect() {
    let gw = gateway(vec!["*".into()]).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let channel = gw.channel;
    let listen =
        tokio::spawn(async move { crate::provider_test_support::listen_v1(&channel, tx).await });
    let mut ws = accept(&gw.listener).await;
    next_frame(&mut ws).await;

    // Drop the TCP socket without a closing handshake.
    drop(ws);
    tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
