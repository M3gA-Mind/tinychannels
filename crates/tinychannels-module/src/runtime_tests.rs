use super::*;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinychannels_bus::{BUS_NAME, methods};

async fn fixture() -> (Arc<Runtimes>, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    module.request_name(BUS_NAME).await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (Arc::new(Runtimes::new(module)), client)
}

#[tokio::test]
async fn activated_runtime_dispatch_policy_is_immutable_and_legacy_start_stays_serial() {
    let (runtimes, _client) = fixture().await;
    let reservation = runtimes.reserve().await.unwrap();
    let policy = tinychannels_bus::ChannelRuntimeDispatchPolicy { listener_count: 2 };
    runtimes
        .start_with_dispatch(reservation.handle.clone(), policy)
        .await
        .unwrap();
    runtimes
        .start_with_dispatch(reservation.handle.clone(), policy)
        .await
        .unwrap();
    assert!(runtimes.start(reservation.handle.clone()).await.is_err());
    assert!(
        runtimes
            .start_with_dispatch(
                reservation.handle.clone(),
                tinychannels_bus::ChannelRuntimeDispatchPolicy { listener_count: 3 }
            )
            .await
            .is_err()
    );
    assert_eq!(
        *runtimes
            .get(&reservation.handle)
            .await
            .unwrap()
            .dispatch_limit
            .lock()
            .await,
        Some(Some(8))
    );
    runtimes.stop(reservation.handle).await.unwrap();
    let many = runtimes.reserve().await.unwrap();
    runtimes
        .start_with_dispatch(
            many.handle.clone(),
            tinychannels_bus::ChannelRuntimeDispatchPolicy {
                listener_count: u32::MAX,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        *runtimes
            .get(&many.handle)
            .await
            .unwrap()
            .dispatch_limit
            .lock()
            .await,
        Some(Some(64))
    );
    runtimes.stop(many.handle).await.unwrap();
    let legacy = runtimes.reserve().await.unwrap();
    runtimes.start(legacy.handle.clone()).await.unwrap();
    assert_eq!(
        *runtimes
            .get(&legacy.handle)
            .await
            .unwrap()
            .dispatch_limit
            .lock()
            .await,
        Some(None)
    );
    runtimes.shutdown().await.unwrap();
}

#[derive(Clone)]
struct HeldTurn {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}
#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl HeldTurn {
    async fn process_inbound(&self, _channel: String, _message: serde_json::Value) -> Result<()> {
        self.started.notify_one();
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn held_turn_returns_pending_stop_and_retains_a_retryable_runtime_owner() {
    let (runtimes, client) = fixture().await;
    let reservation = runtimes.reserve().await.unwrap();
    let callback = HeldTurn {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    client
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    client
        .serve_at(
            reservation.host_object_path.clone().try_into().unwrap(),
            callback.clone(),
        )
        .await
        .unwrap();
    runtimes
        .start_with_dispatch(
            reservation.handle.clone(),
            tinychannels_bus::ChannelRuntimeDispatchPolicy { listener_count: 1 },
        )
        .await
        .unwrap();
    let slot = runtimes.get(&reservation.handle).await.unwrap();
    let child = slot.child.lock().await.as_ref().unwrap().clone();
    child
        .host
        .deliver_inbound(
            "telegram",
            tinychannels_bus::ChannelInboundEnvelope {
                raw: Some(serde_json::json!({"id":1})),
                ..Default::default()
            },
        )
        .await;
    callback.started.notified().await;
    let owner = runtimes.clone();
    let id = reservation.handle.clone();
    let stopping = tokio::spawn(async move { owner.stop(id).await });
    while !child.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(11)).await;
    let error = stopping.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("cleanup is pending"), "{error}");
    assert!(runtimes.get(&reservation.handle).await.is_ok());
    callback.release.add_permits(1);
    runtimes.stop(reservation.handle.clone()).await.unwrap();
    assert!(runtimes.get(&reservation.handle).await.is_err());
}

#[tokio::test]
async fn stopping_one_runtime_does_not_close_another_runtime_or_reuse_its_path() {
    let (runtimes, client) = fixture().await;
    let first = runtimes.reserve().await.unwrap();
    let second = runtimes.reserve().await.unwrap();
    assert_ne!(first.handle, second.handle);
    assert_ne!(first.host_object_path, second.host_object_path);
    runtimes.start(first.handle.clone()).await.unwrap();
    runtimes.start(second.handle.clone()).await.unwrap();
    let first_proxy = client
        .proxy(BUS_NAME, &first.object_path, BUS_NAME)
        .unwrap();
    let second_proxy = client
        .proxy(BUS_NAME, &second.object_path, BUS_NAME)
        .unwrap();
    let _: Vec<String> = first_proxy.call(methods::LIST_CHANNELS, ()).await.unwrap();
    runtimes.stop(first.handle.clone()).await.unwrap();
    assert!(
        first_proxy
            .call::<Vec<String>>(methods::LIST_CHANNELS, ())
            .await
            .is_err()
    );
    let _: Vec<String> = second_proxy.call(methods::LIST_CHANNELS, ()).await.unwrap();
    runtimes.stop(first.handle.clone()).await.unwrap();
    assert!(runtimes.start(first.handle).await.is_err());
    runtimes.shutdown().await.unwrap();
    assert!(runtimes.reserve().await.is_err());
}

#[tokio::test(start_paused = true)]
async fn lost_inert_reservations_expire_but_activated_runtime_ownership_does_not() {
    let (runtimes, _client) = fixture().await;
    let active = runtimes.reserve().await.unwrap();
    runtimes.start(active.handle.clone()).await.unwrap();
    let mut expired = String::new();
    for _ in 1..CAPACITY {
        expired = runtimes.reserve().await.unwrap().handle;
    }
    assert!(runtimes.reserve().await.is_err());
    tokio::time::advance(RESERVATION_LIFETIME + Duration::from_secs(1)).await;
    let next = runtimes.reserve().await.unwrap();
    assert!(runtimes.start(expired).await.is_err());
    runtimes.start(active.handle.clone()).await.unwrap();
    assert_eq!(runtimes.slots.lock().await.len(), 2);
    runtimes.stop(next.handle).await.unwrap();
    runtimes.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_activation_waiter_leaves_a_joinable_owner() {
    let (runtimes, _client) = fixture().await;
    let reservation = runtimes.reserve().await.unwrap();
    let slot = runtimes.get(&reservation.handle).await.unwrap();
    let child_admission = slot.child.lock().await;
    let owner = runtimes.clone();
    let id = reservation.handle.clone();
    let waiter = tokio::spawn(async move { owner.start(id).await });
    loop {
        if slot.activated.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let owner = runtimes.clone();
    let id = reservation.handle.clone();
    let cleanup = tokio::spawn(async move { owner.stop(id).await });
    loop {
        if slot.canceled.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        !cleanup.is_finished(),
        "cleanup must join the admitted activation"
    );
    drop(child_admission);
    cleanup.await.unwrap().unwrap();
    assert!(runtimes.get(&reservation.handle).await.is_err());
    assert!(
        slot.child
            .lock()
            .await
            .as_ref()
            .unwrap()
            .terminal
            .load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn reservations_are_inert_and_children_cannot_allocate_nested_runtimes() {
    let (runtimes, client) = fixture().await;
    let reservation = runtimes.reserve().await.unwrap();
    let slot = runtimes.get(&reservation.handle).await.unwrap();
    assert!(slot.child.lock().await.is_none());
    assert!(slot.worker.lock().await.is_none());
    let proxy = client
        .proxy(BUS_NAME, &reservation.object_path, BUS_NAME)
        .unwrap();
    assert!(
        proxy
            .call::<Vec<String>>(methods::LIST_CHANNELS, ())
            .await
            .is_err()
    );
    runtimes.start(reservation.handle.clone()).await.unwrap();
    let child = slot.child.lock().await.as_ref().unwrap().clone();
    let error = tinybus::Interface::call(
        child.as_ref(),
        &methods::RESERVE_RUNTIME.try_into().unwrap(),
        serde_json::json!([]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.wire_message(),
        "nested channel runtimes are unavailable"
    );
    runtimes.shutdown().await.unwrap();
}

struct MockProvider(tokio::sync::mpsc::Sender<String>);

#[async_trait::async_trait]
impl crate::provider::TestProviderBehavior for MockProvider {
    fn test_provider_id(&self) -> &'static str {
        "same-provider"
    }
    async fn test_send(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> std::result::Result<
        tinychannels_bus::channel::ChannelSendOutcome,
        tinychannels::channel::ChannelSendError,
    > {
        let text = message
            .parts
            .iter()
            .find_map(|part| match part {
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text { text, .. },
                } => Some(text.clone()),
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Native { data, .. },
                } => data
                    .get("content")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                _ => None,
            })
            .unwrap_or_default();
        self.0
            .send(text)
            .await
            .map_err(|error| tinychannels::channel::ChannelSendError {
                message: error.to_string(),
                ..Default::default()
            })?;
        Ok(Default::default())
    }
}

#[tokio::test]
async fn same_named_providers_have_separate_queues_and_native_cleanup() {
    let (runtimes, client) = fixture().await;
    let first = runtimes.reserve().await.unwrap();
    let second = runtimes.reserve().await.unwrap();
    let mut receivers = Vec::new();
    for reservation in [&first, &second] {
        runtimes.start(reservation.handle.clone()).await.unwrap();
        let slot = runtimes.get(&reservation.handle).await.unwrap();
        let child = slot.child.lock().await.as_ref().unwrap().clone();
        let (sent, received) = tokio::sync::mpsc::channel(2);
        let running = crate::provider::Running::start_test(
            Arc::new(MockProvider(sent)),
            Arc::new(crate::provider::Hooks::default()),
            child.host.clone(),
        );
        child
            .running
            .lock()
            .await
            .insert("same-provider".into(), running);
        receivers.push(received);
    }
    let first_proxy = client
        .proxy(BUS_NAME, &first.object_path, BUS_NAME)
        .unwrap();
    let second_proxy = client
        .proxy(BUS_NAME, &second.object_path, BUS_NAME)
        .unwrap();
    for (proxy, text) in [(&first_proxy, "first"), (&second_proxy, "second")] {
        proxy
            .call::<()>(
                methods::SEND_MESSAGE,
                (
                    "same-provider",
                    tinychannels_bus::SendMessage::new(text, "room"),
                ),
            )
            .await
            .unwrap();
    }
    assert_eq!(receivers[0].recv().await.as_deref(), Some("first"));
    assert_eq!(receivers[1].recv().await.as_deref(), Some("second"));
    runtimes.stop(first.handle).await.unwrap();
    assert!(receivers[0].recv().await.is_none());
    second_proxy
        .call::<()>(
            methods::SEND_MESSAGE,
            (
                "same-provider",
                tinychannels_bus::SendMessage::new("still-owned", "room"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(receivers[1].recv().await.as_deref(), Some("still-owned"));
    runtimes.shutdown().await.unwrap();
    assert!(receivers[1].recv().await.is_none());
}

#[tokio::test]
async fn failed_native_cleanup_retains_ownership_and_capacity_until_retry_succeeds() {
    let (runtimes, client) = fixture().await;
    let reservation = runtimes.reserve().await.unwrap();
    let storage = crate::storage_fixture::StorageHost::serve(&client, "runtime").await;
    storage.faults.fail_queue_read.store(true, Ordering::SeqCst);
    client
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    runtimes.start(reservation.handle.clone()).await.unwrap();
    for _ in 1..CAPACITY {
        runtimes.reserve().await.unwrap();
    }
    assert!(runtimes.stop(reservation.handle.clone()).await.is_err());
    assert!(runtimes.get(&reservation.handle).await.is_ok());
    assert!(
        runtimes.reserve().await.is_err(),
        "failed cleanup must retain its capacity slot"
    );
    storage
        .faults
        .fail_queue_read
        .store(false, Ordering::SeqCst);
    runtimes.stop(reservation.handle.clone()).await.unwrap();
    assert!(runtimes.get(&reservation.handle).await.is_err());
    runtimes.reserve().await.unwrap();
    runtimes.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn expired_reservation_cannot_activate_before_the_next_capacity_scan() {
    let (runtimes, _client) = fixture().await;
    let reservation = runtimes.reserve().await.unwrap();
    tokio::time::advance(RESERVATION_LIFETIME + Duration::from_secs(1)).await;
    let error = runtimes
        .start(reservation.handle.clone())
        .await
        .unwrap_err();
    assert_eq!(error.wire_message(), "unknown or expired channel runtime");
    let slot = runtimes.get(&reservation.handle).await.unwrap();
    assert!(!slot.activated.load(Ordering::SeqCst));
    assert!(slot.child.lock().await.is_none());
    runtimes.stop(reservation.handle).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn pending_startup_keeps_its_worker_and_stop_can_retry_after_timeout() {
    let (runtimes, _client) = fixture().await;
    let reservation = runtimes.reserve().await.unwrap();
    let slot = runtimes.get(&reservation.handle).await.unwrap();
    let child_guard = slot.child.lock().await;
    let error = runtimes
        .start(reservation.handle.clone())
        .await
        .unwrap_err();
    assert!(error.wire_message().contains("startup is pending"));
    assert!(slot.worker.lock().await.is_some());
    let error = runtimes.stop(reservation.handle.clone()).await.unwrap_err();
    assert!(error.wire_message().contains("startup cleanup is pending"));
    assert!(slot.worker.lock().await.is_some());
    assert!(runtimes.get(&reservation.handle).await.is_ok());
    let error = runtimes
        .start(reservation.handle.clone())
        .await
        .unwrap_err();
    assert_eq!(error.wire_message(), "channel runtime is closed");
    drop(child_guard);
    runtimes.stop(reservation.handle.clone()).await.unwrap();
    assert!(runtimes.get(&reservation.handle).await.is_err());
}

#[tokio::test]
async fn shutdown_reports_cleanup_failure_and_retains_only_the_failed_owner() {
    let (runtimes, client) = fixture().await;
    let failed = runtimes.reserve().await.unwrap();
    let inert = runtimes.reserve().await.unwrap();
    let storage = crate::storage_fixture::StorageHost::serve(&client, "runtime").await;
    storage.faults.fail_queue_read.store(true, Ordering::SeqCst);
    client
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    runtimes.start(failed.handle.clone()).await.unwrap();
    assert!(runtimes.shutdown().await.is_err());
    assert!(runtimes.get(&failed.handle).await.is_ok());
    assert!(runtimes.get(&inert.handle).await.is_err());
    assert!(runtimes.reserve().await.is_err());
    storage
        .faults
        .fail_queue_read
        .store(false, Ordering::SeqCst);
    runtimes.shutdown().await.unwrap();
    assert!(runtimes.slots.lock().await.is_empty());
}
