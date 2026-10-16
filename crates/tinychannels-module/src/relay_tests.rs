use super::*;
use futures_util::{SinkExt, StreamExt};
use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinychannels::relay::{ConnectorToGatewayFrame, GatewayToConnectorFrame};
use tinychannels_bus::config::RelayRuntimeIdentityConfig;
use tinychannels_bus::relay::RelayReconnectPolicy;
use tokio_tungstenite::tungstenite::Message;

struct IdleRelayIo;

#[async_trait::async_trait]
impl tinychannels::relay::RelayFrameIo for IdleRelayIo {
    async fn send(
        &self,
        _frame: GatewayToConnectorFrame,
    ) -> std::result::Result<(), tinychannels::relay::RelayTransportError> {
        Ok(())
    }

    async fn recv(
        &self,
    ) -> std::result::Result<
        Option<ConnectorToGatewayFrame>,
        tinychannels::relay::RelayTransportError,
    > {
        std::future::pending().await
    }
}

#[test]
fn v2_reconnect_is_not_connected_until_every_configured_identity_replies() {
    let identities = vec![
        RelayIdentity {
            platform: "telegram".into(),
            bot_id: "first".into(),
        },
        RelayIdentity {
            platform: "telegram".into(),
            bot_id: "second".into(),
        },
    ];
    let mut descriptors = std::collections::HashMap::new();
    descriptors.insert(
        identities[1].clone(),
        tinychannels_bus::ChannelCapabilities::unknown(),
    );
    assert!(!v2_capabilities_complete(&identities, &descriptors));
    // Responses may arrive in either order; publishing connected requires the
    // second identity's descriptor as well.
    descriptors.insert(
        identities[0].clone(),
        tinychannels_bus::ChannelCapabilities::unknown(),
    );
    assert!(v2_capabilities_complete(&identities, &descriptors));
}

#[derive(Clone)]
struct Callback {
    events: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    blocked: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
}
#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn deliver_inbound(&self, channel: String, message: serde_json::Value) -> Result<()> {
        while self.blocked.load(Ordering::SeqCst) {
            self.gate.notified().await;
        }
        self.events.lock().await.push((channel, message));
        Ok(())
    }
    async fn report_status(
        &self,
        channel: String,
        state: String,
        detail: Option<String>,
    ) -> Result<()> {
        self.events
            .lock()
            .await
            .push((channel, serde_json::json!({"state":state,"detail":detail})));
        Ok(())
    }
}
async fn manager() -> (Arc<Relays>, Callback, Connection, HostChannels) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let callback = Callback {
        events: Arc::new(Mutex::new(vec![])),
        blocked: Arc::new(AtomicBool::new(false)),
        gate: Arc::new(tokio::sync::Notify::new()),
    };
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    server
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callback.clone(),
        )
        .await
        .unwrap();
    server
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    (Arc::new(Relays::new(host.clone())), callback, server, host)
}
struct SocketFixture {
    config: RelayRuntimeConfig,
    hello: mpsc::Receiver<()>,
    outbound_count: Arc<std::sync::atomic::AtomicUsize>,
    release: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}
impl Drop for SocketFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn socket(delayed: bool) -> SocketFixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (hello, heard) = mpsc::channel(8);
    let outbound_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen_outbound = outbound_count.clone();
    let (release, wait) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap();
        let frame = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            serde_json::from_str::<GatewayToConnectorFrame>(frame.to_text().unwrap()).unwrap(),
            GatewayToConnectorFrame::Hello { .. }
        ));
        hello.send(()).await.unwrap();
        if delayed {
            tokio::select! { _=wait=>{}, _=stream.next()=>return }
        }
        let descriptor = CapabilityDescriptor {
            platform: "telegram".into(),
            label: "fixture".into(),
            ..Default::default()
        };
        stream
            .send(Message::Text(
                format!(
                    "{}\n",
                    serde_json::to_string(&ConnectorToGatewayFrame::Descriptor { descriptor })
                        .unwrap()
                )
                .into(),
            ))
            .await
            .unwrap();
        let mut envelope = ChannelInboundEnvelope::default();
        envelope.channel.id = "telegram".into();
        envelope.text = "first".into();
        stream
            .send(Message::Text(
                format!(
                    "{}\n",
                    serde_json::to_string(&ConnectorToGatewayFrame::Inbound {
                        event: serde_json::to_value(envelope).unwrap(),
                        buffer_id: Some("buffer".into()),
                    })
                    .unwrap()
                )
                .into(),
            ))
            .await
            .unwrap();
        while let Some(Ok(message)) = stream.next().await {
            if message.is_close() {
                break;
            }
            let Ok(text) = message.to_text() else {
                continue;
            };
            if let GatewayToConnectorFrame::Outbound { request_id, .. } =
                serde_json::from_str(text).unwrap()
            {
                seen_outbound.fetch_add(1, Ordering::SeqCst);
                let result = ConnectorToGatewayFrame::OutboundResult {
                    request_id,
                    result: serde_json::json!({"success":true,"message_id":"fixture"}),
                };
                stream
                    .send(Message::Text(
                        format!("{}\n", serde_json::to_string(&result).unwrap()).into(),
                    ))
                    .await
                    .unwrap();
            }
        }
    });
    SocketFixture {
        config: RelayRuntimeConfig {
            url,
            identities: vec![RelayRuntimeIdentityConfig {
                platform: "telegram".into(),
                bot_id: "fixture".into(),
            }],
            ..Default::default()
        },
        hello: heard,
        outbound_count,
        release: Some(release),
        task,
    }
}
fn intent() -> ChannelOutboundIntent {
    tinychannels::outbound_intent_from_legacy_message(
        "telegram",
        serde_json::json!({"content":"outbound","recipient":"chat"}),
    )
}

#[tokio::test]
async fn required_relay_delivery_is_refused_before_socket_send() {
    let (manager, _, _server, host) = manager().await;
    let mut socket = socket(false).await;
    let id = manager.reserve().await.unwrap();
    manager
        .start(id.clone(), socket.config.clone())
        .await
        .unwrap();
    socket.hello.recv().await.unwrap();

    let mut required = intent();
    required.durability = tinychannels::DeliveryDurability::Required;
    assert!(manager.send(id.clone(), required).await.is_err());
    manager.stop(id).await.unwrap();
    (&mut socket.task).await.unwrap();
    assert_eq!(socket.outbound_count.load(Ordering::SeqCst), 0);
    manager.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn v2_reconnect_waits_for_each_configured_identity_before_publishing_connected() {
    let (manager, _callback, _server, _host) = manager().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (partial_sent, partial_seen) = oneshot::channel();
    let (release_initial, initial_released) = oneshot::channel();
    let (release_final, final_released) = oneshot::channel();
    let peer = tokio::spawn(async move {
        // Initial connection declares both accounts and becomes connected.
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = tokio_tungstenite::accept_async(stream).await.unwrap();
        for _ in 0..2 {
            let hello = first.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<GatewayToConnectorFrame>(hello.to_text().unwrap()).unwrap(),
                GatewayToConnectorFrame::HelloV2 { .. }
            ));
        }
        send_v2_descriptor(&mut first, "telegram", "first", "initial").await;
        send_v2_descriptor(&mut first, "telegram", "second", "initial").await;
        let _ = initial_released.await;
        drop(first);

        // On reconnect, return only the second identity first. The transport
        // may retain neither the old set nor claim the new connection is ready.
        let (stream, _) = listener.accept().await.unwrap();
        let mut second = tokio_tungstenite::accept_async(stream).await.unwrap();
        for _ in 0..2 {
            let hello = second.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<GatewayToConnectorFrame>(hello.to_text().unwrap()).unwrap(),
                GatewayToConnectorFrame::HelloV2 { .. }
            ));
        }
        send_v2_descriptor(&mut second, "telegram", "second", "partial").await;
        let _ = partial_sent.send(());
        let _ = final_released.await;
        send_v2_descriptor(&mut second, "telegram", "first", "reconnected").await;
        while let Some(Ok(message)) = second.next().await {
            if message.is_close() {
                break;
            }
        }
    });
    let identities = vec![
        RelayRuntimeIdentityConfig {
            platform: "telegram".into(),
            bot_id: "first".into(),
        },
        RelayRuntimeIdentityConfig {
            platform: "telegram".into(),
            bot_id: "second".into(),
        },
    ];
    let reservation = manager.reserve().await.unwrap();
    let config = RelayRuntimeConfig {
        url,
        identities,
        reconnect: RelayReconnectPolicy {
            backoff_ms: 10,
            max_backoff_ms: 50,
        },
        ..Default::default()
    };
    let initial = manager.start_v2(reservation.clone(), config).await.unwrap();
    assert_eq!(initial.instances.len(), 2);
    release_initial.send(()).unwrap();
    partial_seen.await.unwrap();
    assert_eq!(
        manager.capabilities_v2(reservation.clone()).await.unwrap(),
        None,
        "a reconnect with only one of two configured descriptors must not expose cached facts"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        manager.status(reservation.clone()).await.unwrap().state,
        RelayPhase::Reconnecting
    );
    assert_eq!(
        manager.capabilities_v2(reservation.clone()).await.unwrap(),
        None
    );

    release_final.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if manager.status(reservation.clone()).await.unwrap().state == RelayPhase::Connected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all configured identities reconnect");
    let refreshed = manager
        .capabilities_v2(reservation.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.instances.len(), 2);
    assert_eq!(
        refreshed
            .instances
            .iter()
            .find(|instance| instance.bot_id == "first")
            .unwrap()
            .capabilities
            .extensions
            .get("marker"),
        Some(&serde_json::json!("reconnected")),
    );
    let ids: std::collections::BTreeSet<_> = refreshed
        .instances
        .iter()
        .map(|instance| instance.bot_id.as_str())
        .collect();
    assert_eq!(ids, ["first", "second"].into_iter().collect());
    manager.stop(reservation).await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn v2_capabilities_query_hides_cached_slot_after_immediate_transport_close() {
    let (manager, _callback, _server, host) = manager().await;
    let reservation = manager.reserve().await.unwrap();
    let slot = manager.get(&reservation).await.unwrap();
    let capabilities = tinychannels_bus::ChannelCapabilities::unknown();
    slot.state
        .send_replace(State::ConnectedV2(RelayCapabilitiesV2 {
            contract_version: tinychannels_bus::ChannelCapabilities::VERSION,
            instances: vec![RelayInstanceCapabilitiesV2 {
                platform: "telegram".into(),
                bot_id: "fixture".into(),
                capabilities,
            }],
        }));
    let transport = Arc::new(tinychannels::relay::RelayTransport::new(
        vec![tinychannels::relay::RelayIdentity {
            platform: "telegram".into(),
            bot_id: "fixture".into(),
        }],
        Arc::new(IdleRelayIo),
        tinychannels::relay::RelayTransportTimeouts::default(),
    ));
    *slot.transport.lock().await = Some(transport.clone());
    assert!(!transport.is_closed());
    assert_eq!(
        manager.status(reservation.clone()).await.unwrap().state,
        RelayPhase::Reconnecting,
        "an open transport without its new descriptor snapshot is not connected"
    );
    assert_eq!(
        manager.capabilities_v2(reservation.clone()).await.unwrap(),
        None,
        "an open transport without its new descriptor snapshot must not expose cached facts"
    );
    transport.shutdown().await;

    assert!(transport.is_closed());
    assert_eq!(
        manager.capabilities_v2(reservation.clone()).await.unwrap(),
        None
    );
    manager.stop(reservation).await.unwrap();
    manager.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
}

async fn send_v2_descriptor(
    stream: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    platform: &str,
    bot_id: &str,
    marker: &str,
) {
    let mut capabilities = tinychannels_bus::ChannelCapabilities::unknown();
    capabilities
        .extensions
        .insert("marker".into(), serde_json::json!(marker));
    let frame = ConnectorToGatewayFrame::DescriptorV2 {
        platform: platform.into(),
        bot_id: bot_id.into(),
        capabilities: Box::new(capabilities),
    };
    stream
        .send(Message::Text(
            format!("{}\n", serde_json::to_string(&frame).unwrap()).into(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn stopped_reservations_never_reopen_and_sequential_cycles_reclaim_capacity() {
    let (manager, _, _server, host) = manager().await;
    let first = manager.reserve().await.unwrap();
    assert!(manager.reserve().await.is_err());
    manager
        .stop(RelayReservation("unrelated".into()))
        .await
        .unwrap();
    assert_eq!(
        manager.status(first.clone()).await.unwrap().state,
        RelayPhase::Reserved
    );
    manager.stop(first.clone()).await.unwrap();
    for _ in 0..4100 {
        let id = manager.reserve().await.unwrap();
        manager.stop(id).await.unwrap();
    }
    assert!(
        manager
            .start(first, RelayRuntimeConfig::default())
            .await
            .is_err()
    );
    manager.shutdown().await.unwrap();
    assert!(manager.reserve().await.is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn stop_releases_reservation_after_worker_panics() {
    let (manager, _, _server, host) = manager().await;
    let id = manager.reserve().await.unwrap();
    let slot = manager.get(&id).await.unwrap();
    let worker = tokio::spawn(async { panic!("fixture relay worker panic") });
    while !worker.is_finished() {
        tokio::task::yield_now().await;
    }
    *slot.worker.lock().await = Some(worker);

    manager.stop(id.clone()).await.unwrap();
    assert!(manager.status(id).await.is_err());
    let next = manager.reserve().await.unwrap();
    manager.stop(next).await.unwrap();
    manager.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
}
#[tokio::test]
async fn pending_start_can_be_stopped_and_lost_waiter_does_not_orphan_socket() {
    let (manager, _, _server, host) = manager().await;
    let mut socket = socket(true).await;
    let id = manager.reserve().await.unwrap();
    let start = tokio::spawn({
        let manager = manager.clone();
        let id = id.clone();
        let config = socket.config.clone();
        async move { manager.start(id, config).await }
    });
    socket.hello.recv().await.unwrap();
    start.abort();
    let _ = start.await;
    manager.stop(id.clone()).await.unwrap();
    (&mut socket.task).await.unwrap();
    assert!(manager.status(id.clone()).await.is_err());
    assert!(manager.start(id, socket.config.clone()).await.is_err());
    host.shutdown().await.unwrap();
}
#[tokio::test]
async fn same_reservation_observes_start_and_changed_config_conflicts() {
    let (manager, callback, _server, host) = manager().await;
    let mut socket = socket(true).await;
    let id = manager.reserve().await.unwrap();
    let start = tokio::spawn({
        let manager = manager.clone();
        let id = id.clone();
        let config = socket.config.clone();
        async move { manager.start(id, config).await }
    });
    socket.hello.recv().await.unwrap();
    let mut changed = socket.config.clone();
    changed.identities[0].bot_id = "different".into();
    assert!(manager.start(id.clone(), changed).await.is_err());
    socket.release.take().unwrap().send(()).unwrap();
    let descriptor = start.await.unwrap().unwrap();
    assert_eq!(
        descriptor,
        manager
            .start(id.clone(), socket.config.clone())
            .await
            .unwrap()
    );
    assert_eq!(
        manager.status(id.clone()).await.unwrap().state,
        RelayPhase::Connected
    );
    let sent = manager.send(id.clone(), intent()).await.unwrap();
    assert_eq!(sent.message_id.as_deref(), Some("fixture"));
    manager.stop(id.clone()).await.unwrap();
    (&mut socket.task).await.unwrap();
    let events = callback.events.lock().await.clone();
    assert!(
        events
            .iter()
            .any(|(_, event)| event["delivered_via_authenticated_relay"] == true)
    );
    let count = events.len();
    tokio::task::yield_now().await;
    assert_eq!(callback.events.lock().await.len(), count);
    assert!(manager.send(id, intent()).await.is_err());
    host.shutdown().await.unwrap();
}
#[tokio::test]
async fn terminal_shutdown_cancels_pending_start_without_late_publication() {
    let (manager, _, _server, host) = manager().await;
    let mut socket = socket(true).await;
    let id = manager.reserve().await.unwrap();
    let start = tokio::spawn({
        let manager = manager.clone();
        let id = id.clone();
        let config = socket.config.clone();
        async move { manager.start(id, config).await }
    });
    socket.hello.recv().await.unwrap();
    manager.shutdown().await.unwrap();
    assert!(start.await.unwrap().is_err());
    (&mut socket.task).await.unwrap();
    assert!(manager.reserve().await.is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_websocket_disconnect_redials_and_publishes_reconnected_status() {
    let (manager, callback, _server, host) = manager().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut first_descriptor = CapabilityDescriptor {
            platform: "telegram".into(),
            label: "first connection".into(),
            ..Default::default()
        };
        let (tcp, _) = listener.accept().await.unwrap();
        let mut first = tokio_tungstenite::accept_async(tcp).await.unwrap();
        assert!(matches!(
            serde_json::from_str::<GatewayToConnectorFrame>(
                first.next().await.unwrap().unwrap().to_text().unwrap()
            )
            .unwrap(),
            GatewayToConnectorFrame::Hello { .. }
        ));
        first
            .send(Message::Text(
                format!(
                    "{}\n",
                    serde_json::to_string(&ConnectorToGatewayFrame::Descriptor {
                        descriptor: first_descriptor.clone()
                    })
                    .unwrap()
                )
                .into(),
            ))
            .await
            .unwrap();
        drop(first);

        let (tcp, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("relay should redial")
            .unwrap();
        let mut second = tokio_tungstenite::accept_async(tcp).await.unwrap();
        assert!(matches!(
            serde_json::from_str::<GatewayToConnectorFrame>(
                second.next().await.unwrap().unwrap().to_text().unwrap()
            )
            .unwrap(),
            GatewayToConnectorFrame::Hello { .. }
        ));
        tokio::time::sleep(Duration::from_millis(250)).await;
        first_descriptor.label = "reconnected".into();
        second
            .send(Message::Text(
                format!(
                    "{}\n",
                    serde_json::to_string(&ConnectorToGatewayFrame::Descriptor {
                        descriptor: first_descriptor
                    })
                    .unwrap()
                )
                .into(),
            ))
            .await
            .unwrap();
        while let Some(Ok(message)) = second.next().await {
            if message.is_close() {
                break;
            }
        }
    });

    let id = manager.reserve().await.unwrap();
    let config = RelayRuntimeConfig {
        url,
        identities: vec![RelayRuntimeIdentityConfig {
            platform: "telegram".into(),
            bot_id: "fixture".into(),
        }],
        reconnect: tinychannels_bus::relay::RelayReconnectPolicy {
            backoff_ms: 10,
            max_backoff_ms: 20,
        },
        ..Default::default()
    };
    let descriptor = manager.start(id.clone(), config).await.unwrap();
    assert_eq!(descriptor.label, "first connection");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let connected = manager
                .status(id.clone())
                .await
                .is_ok_and(|status| status.state == RelayPhase::Connected);
            let events = callback.events.lock().await;
            let reconnecting = events
                .iter()
                .any(|(_, event)| event["state"] == "reconnecting");
            let connected_reports = events
                .iter()
                .filter(|(_, event)| event["state"] == "connected")
                .count();
            drop(events);
            if connected && reconnecting && connected_reports >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reconnected status should become visible");
    let statuses = callback
        .events
        .lock()
        .await
        .iter()
        .filter_map(|(_, event)| event["state"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    assert!(
        statuses
            .windows(3)
            .any(|states| states == ["connected", "reconnecting", "connected"]),
        "unexpected status sequence: {statuses:?}"
    );

    manager.stop(id).await.unwrap();
    server.await.unwrap();
    host.shutdown().await.unwrap();
}
#[test]
fn config_bounds_refuse_unconfigured_and_oversized_native_inputs() {
    assert!(validate_config(&RelayRuntimeConfig::default()).is_err());
    let mut config = RelayRuntimeConfig {
        url: "ws://localhost".into(),
        identities: vec![RelayRuntimeIdentityConfig {
            platform: "x".into(),
            bot_id: "bot".into(),
        }],
        ..Default::default()
    };
    validate_config(&config).unwrap();
    config.timeouts.handshake_ms = 0;
    assert!(validate_config(&config).is_err());
    config.timeouts.handshake_ms = 30_001;
    assert!(validate_config(&config).is_err());
}
