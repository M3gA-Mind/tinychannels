use super::*;
use crate::relay::{AuthenticatedRelayInboundEvent, PassthroughForward};
use async_trait::async_trait;
use std::future::Future;
use tokio::sync::mpsc;

struct TestIo {
    sent: Mutex<Vec<GatewayToConnectorFrame>>,
    incoming: Mutex<mpsc::UnboundedReceiver<Result<ConnectorToGatewayFrame, RelayTransportError>>>,
    send_error: bool,
    response: Option<Value>,
    responded: AtomicBool,
}

impl TestIo {
    fn new(
        send_error: bool,
        response: Option<Value>,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedSender<Result<ConnectorToGatewayFrame, RelayTransportError>>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
                incoming: Mutex::new(rx),
                send_error,
                response,
                responded: AtomicBool::new(false),
            }),
            tx,
        )
    }
}

#[async_trait]
impl RelayFrameIo for TestIo {
    async fn send(&self, frame: GatewayToConnectorFrame) -> Result<(), RelayTransportError> {
        if self.send_error {
            return Err(RelayTransportError::Io("send failed".into()));
        }
        self.sent.lock().await.push(frame);
        Ok(())
    }

    async fn recv(&self) -> Result<Option<ConnectorToGatewayFrame>, RelayTransportError> {
        if let Some(result) = self
            .response
            .as_ref()
            .filter(|_| !self.responded.load(Ordering::SeqCst))
        {
            loop {
                if let Some(GatewayToConnectorFrame::Outbound { request_id, .. }) =
                    self.sent.lock().await.last().cloned()
                {
                    self.responded.store(true, Ordering::SeqCst);
                    return Ok(Some(ConnectorToGatewayFrame::OutboundResult {
                        request_id,
                        result: result.clone(),
                    }));
                }
                tokio::task::yield_now().await;
            }
        }
        self.incoming.lock().await.recv().await.transpose()
    }
}

fn transport(io: Arc<TestIo>) -> RelayTransport {
    RelayTransport::new(
        vec![RelayIdentity {
            platform: "discord".into(),
            bot_id: "bot".into(),
        }],
        io,
        RelayTransportTimeouts {
            handshake_ms: 5,
            outbound_ms: 5,
            idle_ms: 5,
        },
    )
}

#[tokio::test]
async fn send_failures_remove_outbound_and_idle_waiters() {
    let (io, _tx) = TestIo::new(true, None);
    let transport = transport(io);
    let error = RelayTransportError::Io("send failed".into());
    assert_eq!(transport.connect().await, Err(error.clone()));
    assert_eq!(
        transport.send_outbound(json!({}), Some("missing")).await,
        Err(error.clone())
    );
    assert!(transport.state.pending.lock().await.is_empty());
    assert_eq!(transport.go_idle().await, Err(error));
    assert!(transport.state.going_idle.lock().await.is_none());
    transport.shutdown().await;
}

#[tokio::test]
async fn timed_out_operations_remove_waiters_and_closed_handshake_fails() {
    let (io, _tx) = TestIo::new(false, None);
    let transport = transport(io);
    transport.connect().await.unwrap();
    assert_eq!(
        transport.handshake().await,
        Err(RelayTransportError::Timeout {
            operation: "handshake"
        })
    );
    assert_eq!(
        transport.send_follow_up(json!({}), None).await,
        Err(RelayTransportError::Timeout {
            operation: "outbound_result"
        })
    );
    assert!(transport.state.pending.lock().await.is_empty());
    assert_eq!(transport.go_idle().await, Ok(false));
    assert!(transport.state.going_idle.lock().await.is_none());
    transport.shutdown().await;
    assert_eq!(
        transport.handshake().await,
        Err(RelayTransportError::Closed)
    );
    transport.start_reader().await;
    assert!(transport.reader.lock().await.is_none());
}

#[tokio::test]
async fn reconnect_stays_closed_while_cached_capabilities_are_invalidated() {
    let (io, _tx) = TestIo::new(false, None);
    let transport = transport(io);
    let identity = RelayIdentity {
        platform: "discord".into(),
        bot_id: "bot".into(),
    };
    transport.state.closed.store(true, Ordering::SeqCst);
    transport
        .state
        .capabilities_v2
        .lock()
        .await
        .insert(identity, tinychannels_bus::ChannelCapabilities::unknown());
    let cache = transport.state.capabilities_v2.lock().await;

    let mut preparing = Box::pin(transport.prepare_connect());
    let reached_cache_lock =
        std::future::poll_fn(|cx| std::task::Poll::Ready(preparing.as_mut().poll(cx).is_pending()))
            .await;
    assert!(reached_cache_lock);
    assert!(
        transport.is_closed(),
        "a reconnect must not expose stale cached facts"
    );

    drop(cache);
    preparing.await;
    assert!(!transport.is_closed());
    assert!(transport.capabilities_v2().await.is_empty());
}

#[tokio::test]
async fn reader_failure_releases_pending_outbound_and_idle_operations() {
    let (io, tx) = TestIo::new(false, None);
    let mut transport = transport(io.clone());
    transport.timeouts.outbound_ms = 1000;
    transport.timeouts.idle_ms = 1000;
    transport.connect().await.unwrap();
    let outbound = transport.send_outbound(json!({}), None);
    let idle = transport.go_idle();
    let close = async {
        timeout(Duration::from_secs(1), async {
            while io.sent.lock().await.len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tx.send(Err(RelayTransportError::Io("read failed".into())))
            .unwrap();
    };
    let (outbound, idle, ()) = tokio::join!(outbound, idle, close);
    assert_eq!(outbound, Err(RelayTransportError::Closed));
    assert_eq!(idle, Err(RelayTransportError::Closed));
    assert!(transport.is_closed());
}

#[tokio::test]
async fn chat_info_normalizes_nested_flat_and_missing_fields() {
    for (response, expected) in [
        (
            json!({"chat_info":{"name":"Room","type":"group"}}),
            json!({"name":"Room","type":"group"}),
        ),
        (
            json!({"name":"Alice","type":"dm"}),
            json!({"name":"Alice","type":"dm"}),
        ),
        (json!({"name":1}), json!({"name":"chat-1","type":"dm"})),
    ] {
        let (io, _tx) = TestIo::new(false, Some(response));
        let mut transport = transport(io.clone());
        transport.timeouts.outbound_ms = 1000;
        transport.connect().await.unwrap();
        assert_eq!(transport.get_chat_info("chat-1").await.unwrap(), expected);
        assert!(
            matches!(&io.sent.lock().await[1], GatewayToConnectorFrame::Outbound { action, platform: None, bot_id: None, .. } if action == &json!({"op":"get_chat_info","chat_id":"chat-1"}))
        );
        transport.shutdown().await;
    }
}

#[tokio::test]
async fn interrupt_preserves_session_and_optional_reason() {
    let (io, _tx) = TestIo::new(false, None);
    let transport = transport(io.clone());
    transport
        .send_interrupt("session", Some("cancel".into()))
        .await
        .unwrap();
    assert_eq!(
        io.sent.lock().await.as_slice(),
        &[GatewayToConnectorFrame::Interrupt {
            session_key: "session".into(),
            reason: Some("cancel".into())
        }]
    );
}

#[derive(Default)]
struct Handlers {
    calls: Mutex<Vec<Value>>,
    fail: AtomicBool,
}

#[async_trait]
impl RelayInboundHandler for Handlers {
    async fn handle(
        &self,
        event: AuthenticatedRelayInboundEvent,
    ) -> Result<(), RelayTransportError> {
        self.calls.lock().await.push(event.event);
        if self.fail.load(Ordering::SeqCst) {
            return Err(RelayTransportError::Handler("rejected".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl RelayPassthroughHandler for Handlers {
    async fn handle(
        &self,
        forward: PassthroughForward,
        buffer_id: Option<String>,
    ) -> Result<(), RelayTransportError> {
        self.calls
            .lock()
            .await
            .push(json!({"path":forward.path,"buffer_id":buffer_id}));
        if self.fail.load(Ordering::SeqCst) {
            return Err(RelayTransportError::Handler("rejected".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl RelayInterruptInboundHandler for Handlers {
    async fn handle(
        &self,
        session_key: String,
        chat_id: String,
    ) -> Result<(), RelayTransportError> {
        self.calls
            .lock()
            .await
            .push(json!({"session":session_key,"chat":chat_id}));
        Ok(())
    }
}

fn passthrough(buffer_id: Option<&str>) -> ConnectorToGatewayFrame {
    ConnectorToGatewayFrame::PassthroughForward {
        forward: PassthroughForward {
            platform: "discord".into(),
            bot_id: "bot".into(),
            method: "POST".into(),
            path: "/interactions".into(),
            headers: vec![],
            body: vec![],
        },
        buffer_id: buffer_id.map(str::to_owned),
    }
}

#[tokio::test]
async fn passthrough_ack_requires_successful_handler_and_buffer_id() {
    let (io, _tx) = TestIo::new(false, None);
    let transport = transport(io.clone());
    let dyn_io: Arc<dyn RelayFrameIo> = io.clone();
    handle_frame(&dyn_io, &transport.state, passthrough(Some("unhandled")))
        .await
        .unwrap();
    assert!(io.sent.lock().await.is_empty());
    let handler = Arc::new(Handlers::default());
    transport.set_passthrough_handler(handler.clone()).await;
    handle_frame(&dyn_io, &transport.state, passthrough(Some("buffer")))
        .await
        .unwrap();
    handle_frame(&dyn_io, &transport.state, passthrough(None))
        .await
        .unwrap();
    assert_eq!(
        *handler.calls.lock().await,
        vec![
            json!({"path":"/interactions","buffer_id":"buffer"}),
            json!({"path":"/interactions","buffer_id":null})
        ]
    );
    handler.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        handle_frame(&dyn_io, &transport.state, passthrough(Some("failed"))).await,
        Err(RelayTransportError::Handler("rejected".into()))
    );
    assert_eq!(
        *io.sent.lock().await,
        vec![GatewayToConnectorFrame::InboundAck {
            buffer_id: "buffer".into()
        }]
    );
}

#[tokio::test]
async fn interrupt_handler_receives_session_and_chat_and_unsolicited_replies_are_ignored() {
    let (io, _tx) = TestIo::new(false, None);
    let transport = transport(io.clone());
    let dyn_io: Arc<dyn RelayFrameIo> = io.clone();
    let interrupt = ConnectorToGatewayFrame::InterruptInbound {
        session_key: "session".into(),
        chat_id: "chat".into(),
    };
    handle_frame(&dyn_io, &transport.state, interrupt.clone())
        .await
        .unwrap();
    let handler = Arc::new(Handlers::default());
    transport
        .set_interrupt_inbound_handler(handler.clone())
        .await;
    handle_frame(&dyn_io, &transport.state, interrupt)
        .await
        .unwrap();
    assert_eq!(
        *handler.calls.lock().await,
        vec![json!({"session":"session","chat":"chat"})]
    );
    handle_frame(
        &dyn_io,
        &transport.state,
        ConnectorToGatewayFrame::GoingIdleAck,
    )
    .await
    .unwrap();
    handle_frame(
        &dyn_io,
        &transport.state,
        ConnectorToGatewayFrame::OutboundResult {
            request_id: "unknown".into(),
            result: json!({}),
        },
    )
    .await
    .unwrap();
    assert!(io.sent.lock().await.is_empty());
}

#[tokio::test]
async fn inbound_handler_failure_never_acknowledges_buffer() {
    let (io, _tx) = TestIo::new(false, None);
    let transport = transport(io.clone());
    let dyn_io: Arc<dyn RelayFrameIo> = io.clone();
    let handler = Arc::new(Handlers::default());
    transport.set_inbound_handler(handler.clone()).await;
    handler.fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        handle_frame(
            &dyn_io,
            &transport.state,
            ConnectorToGatewayFrame::Inbound {
                event: json!({"text":"hello"}),
                buffer_id: Some("failed".into())
            }
        )
        .await,
        Err(RelayTransportError::Handler(_))
    ));
    assert!(io.sent.lock().await.is_empty());
}

#[test]
fn reconnect_backoff_doubles_saturates_and_caps_at_maximum() {
    assert_eq!(next_backoff(2, 10), 4);
    assert_eq!(next_backoff(8, 10), 10);
    assert_eq!(next_backoff(u64::MAX, u64::MAX), u64::MAX);
}

struct ScriptedDialer {
    attempts: Mutex<std::collections::VecDeque<Result<Arc<dyn RelayFrameIo>, RelayTransportError>>>,
    dialed: mpsc::UnboundedSender<()>,
}

#[async_trait]
impl RelayFrameDialer for ScriptedDialer {
    async fn dial(&self) -> Result<Arc<dyn RelayFrameIo>, RelayTransportError> {
        let result = self
            .attempts
            .lock()
            .await
            .pop_front()
            .expect("unexpected dial");
        self.dialed.send(()).unwrap();
        result
    }
}

#[tokio::test]
async fn reconnect_retries_dial_and_hello_failures_then_replaces_reader() {
    let (initial, initial_tx) = TestIo::new(false, None);
    let transport = Arc::new(transport(initial));
    transport.connect().await.unwrap();
    let (bad, _bad_tx) = TestIo::new(true, None);
    let (good, good_tx) = TestIo::new(false, None);
    let (dialed, mut attempts) = mpsc::unbounded_channel();
    let dialer = Arc::new(ScriptedDialer {
        attempts: Mutex::new(std::collections::VecDeque::from([
            Err(RelayTransportError::Io("dial failed".into())),
            Ok(bad as Arc<dyn RelayFrameIo>),
            Ok(good.clone() as Arc<dyn RelayFrameIo>),
        ])),
        dialed,
    });
    let supervisor = transport.spawn_reconnect_supervisor(
        dialer,
        RelayReconnectPolicy {
            backoff_ms: 1,
            max_backoff_ms: 4,
        },
    );
    drop(initial_tx);
    // A hello-send failure does not close its I/O; emulate the peer closing it.
    drop(_bad_tx);
    timeout(Duration::from_secs(1), async {
        for _ in 0..3 {
            attempts.recv().await.unwrap();
        }
        while good.sent.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *good.sent.lock().await,
        vec![GatewayToConnectorFrame::Hello {
            platform: "discord".into(),
            bot_id: "bot".into()
        }]
    );
    assert!(!transport.is_closed());
    assert!(transport.descriptor().await.is_none());
    supervisor.shutdown().await;
    drop(good_tx);
    transport.shutdown().await;
}
