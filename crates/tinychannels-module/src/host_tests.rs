use super::*;
use std::collections::HashMap;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;

#[derive(Default)]
struct DeliveryFixtureDb {
    pending: HashMap<String, tinychannels_bus::DeliveryQueueEntry>,
    failed: HashMap<String, tinychannels_bus::DeliveryQueueEntry>,
    results: HashMap<String, tinychannels_bus::DeliveryTerminalResult>,
}

#[derive(Clone)]
struct Callback {
    events: Arc<Mutex<Vec<String>>>,
    blocked: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
    persist_failures: Arc<std::sync::atomic::AtomicUsize>,
    delivery_scope: Arc<Mutex<String>>,
    delivery: Arc<Mutex<HashMap<String, DeliveryFixtureDb>>>,
    pending_save_failures: Arc<std::sync::atomic::AtomicUsize>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn deliver_inbound(&self, channel: String, message: serde_json::Value) -> Result<()> {
        while self.blocked.load(Ordering::SeqCst) {
            self.gate.notified().await;
        }
        self.events.lock().await.push(format!(
            "inbound:{channel}:{}",
            message["text"].as_str().unwrap()
        ));
        Ok(())
    }

    async fn report_status(
        &self,
        channel: String,
        state: String,
        _detail: Option<String>,
    ) -> Result<()> {
        self.events
            .lock()
            .await
            .push(format!("status:{channel}:{state}"));
        Ok(())
    }

    async fn persist_allowed_identity(&self, channel: String, identity: String) -> Result<()> {
        if self
            .persist_failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(tinybus::Error::failed("fixture persistence failure"));
        }
        self.events
            .lock()
            .await
            .push(format!("allowlist:{channel}:{identity}"));
        Ok(())
    }

    async fn save_pending_delivery(
        &self,
        entry: tinychannels_bus::DeliveryQueueEntry,
    ) -> Result<()> {
        if self
            .pending_save_failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(tinybus::Error::failed("fixture queue persistence failure"));
        }
        let scope = self.delivery_scope.lock().await.clone();
        let mut delivery = self.delivery.lock().await;
        let db = delivery.entry(scope).or_default();
        if db.pending.len() >= tinychannels_bus::module_delivery::DELIVERY_PENDING_CAPACITY
            && !db.pending.contains_key(&entry.id)
        {
            return Err(tinybus::Error::failed("fixture queue capacity exceeded"));
        }
        if let Some(existing) = db.pending.get(&entry.id)
            && existing.intent != entry.intent
        {
            return Err(tinybus::Error::failed("fixture delivery key conflicts"));
        }
        db.pending.insert(entry.id.clone(), entry);
        Ok(())
    }

    async fn load_pending_delivery(
        &self,
        id: String,
    ) -> Result<Option<tinychannels_bus::DeliveryQueueEntry>> {
        let scope = self.delivery_scope.lock().await.clone();
        Ok(self
            .delivery
            .lock()
            .await
            .get(&scope)
            .and_then(|db| db.pending.get(&id).cloned()))
    }

    async fn load_pending_deliveries(&self) -> Result<Vec<tinychannels_bus::DeliveryQueueEntry>> {
        let scope = self.delivery_scope.lock().await.clone();
        let mut entries: Vec<_> = self
            .delivery
            .lock()
            .await
            .get(&scope)
            .map(|db| db.pending.values().cloned().collect())
            .unwrap_or_default();
        entries.sort_by_key(|entry| entry.enqueued_at_unix_ms);
        Ok(entries)
    }

    async fn remove_pending_delivery(&self, id: String) -> Result<()> {
        let scope = self.delivery_scope.lock().await.clone();
        if let Some(db) = self.delivery.lock().await.get_mut(&scope) {
            db.pending.remove(&id);
        }
        Ok(())
    }

    async fn save_failed_delivery(
        &self,
        entry: tinychannels_bus::DeliveryQueueEntry,
    ) -> Result<()> {
        let scope = self.delivery_scope.lock().await.clone();
        self.delivery
            .lock()
            .await
            .entry(scope)
            .or_default()
            .failed
            .insert(entry.id.clone(), entry);
        Ok(())
    }

    async fn load_failed_delivery(
        &self,
        id: String,
    ) -> Result<Option<tinychannels_bus::DeliveryQueueEntry>> {
        let scope = self.delivery_scope.lock().await.clone();
        Ok(self
            .delivery
            .lock()
            .await
            .get(&scope)
            .and_then(|db| db.failed.get(&id).cloned()))
    }

    async fn save_delivery_result(
        &self,
        result: tinychannels_bus::DeliveryTerminalResult,
    ) -> Result<()> {
        let scope = self.delivery_scope.lock().await.clone();
        let mut delivery = self.delivery.lock().await;
        let db = delivery.entry(scope).or_default();
        if db.results.len() >= tinychannels_bus::module_delivery::DELIVERY_RESULT_CAPACITY
            && !db.results.contains_key(&result.id)
        {
            return Err(tinybus::Error::failed("fixture result capacity exceeded"));
        }
        if let Some(existing) = db.results.get(&result.id)
            && existing != &result
        {
            return Err(tinybus::Error::failed("fixture result key conflicts"));
        }
        db.results.insert(result.id.clone(), result);
        Ok(())
    }

    async fn load_delivery_result(
        &self,
        id: String,
    ) -> Result<Option<tinychannels_bus::DeliveryTerminalResult>> {
        let scope = self.delivery_scope.lock().await.clone();
        Ok(self
            .delivery
            .lock()
            .await
            .get(&scope)
            .and_then(|db| db.results.get(&id).cloned()))
    }
}

async fn connected_host() -> (HostChannels, Callback, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let callback = Callback {
        events: Arc::new(Mutex::new(Vec::new())),
        blocked: Arc::new(AtomicBool::new(false)),
        gate: Arc::new(tokio::sync::Notify::new()),
        persist_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        delivery_scope: Arc::new(Mutex::new("scope-a".into())),
        delivery: Arc::new(Mutex::new(HashMap::new())),
        pending_save_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    server
        .serve_at(HOST_OBJECT_PATH.try_into().unwrap(), callback.clone())
        .await
        .unwrap();
    server.request_name(HOST_BUS_NAME).await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (HostChannels::new(client), callback, server)
}

#[tokio::test]
async fn callback_consumer_preserves_inbound_then_status_order() {
    let (host, callback, _server) = connected_host().await;
    host.deliver_inbound("telegram", serde_json::json!({"text":"hello"}))
        .await;
    host.report_status("telegram", "connected", None).await;
    host.flush().await.unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["inbound:telegram:hello", "status:telegram:connected"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn allowlist_persistence_returns_only_after_host_acknowledges() {
    let (host, callback, _server) = connected_host().await;
    host.persist_allowed_identity("telegram", "12345")
        .await
        .unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["allowlist:telegram:12345"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_allowlist_persistence_is_reported_and_can_be_retried() {
    let (host, callback, _server) = connected_host().await;
    callback.persist_failures.store(1, Ordering::SeqCst);
    assert!(
        host.persist_allowed_identity("telegram", "12345")
            .await
            .is_err()
    );
    host.persist_allowed_identity("telegram", "12345")
        .await
        .unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["allowlist:telegram:12345"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_store_round_trips_records_inside_the_active_host_scope() {
    use tinychannels::delivery::{
        DeliveryQueueStore as _, DeliveryResultStore as _, QueuedDelivery, enqueue_delivery,
    };

    let (host, callback, _server) = connected_host().await;
    let store = crate::delivery::HostDeliveryStore::new(host.connection());
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "op-scope-test".into(),
        channel_id: "telegram".into(),
        conversation_id: "room-1".into(),
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };
    let entry = QueuedDelivery::new("op-scope-test", intent.clone(), 42);
    enqueue_delivery(&store, entry.clone()).await.unwrap();
    assert_eq!(store.load_pending(&entry.id).await.unwrap(), Some(entry));

    let result = tinychannels_bus::DeliveryTerminalResult {
        id: "op-scope-test".into(),
        intent_fingerprint: "a".repeat(64),
        completed_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };
    store.save_result(result.clone()).await.unwrap();
    assert_eq!(store.load_result(&result.id).await.unwrap(), Some(result));

    *callback.delivery_scope.lock().await = "scope-b".into();
    assert!(store.load_pending_all().await.unwrap().is_empty());
    assert!(store.load_result("op-scope-test").await.unwrap().is_none());
    *callback.delivery_scope.lock().await = "scope-a".into();
    assert_eq!(store.load_pending_all().await.unwrap().len(), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_store_rejects_invalid_and_expired_records() {
    use tinychannels::delivery::{
        DeliveryQueueStore as _, DeliveryResultStore as _, QueuedDelivery,
    };

    let (host, _callback, _server) = connected_host().await;
    let store = crate::delivery::HostDeliveryStore::new(host.connection());
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "op-validate".into(),
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };

    assert!(store.load_pending("").await.is_err());
    assert!(store.load_failed(&"x".repeat(257)).await.is_err());

    let mismatched = QueuedDelivery::new("different-id", intent.clone(), 1);
    assert!(store.save_pending(mismatched).await.is_err());
    let oversized = QueuedDelivery::new(
        "op-validate",
        tinychannels_bus::ChannelOutboundIntent {
            payload: tinychannels::OutboundPayload::Text {
                text: "x".repeat(tinychannels_bus::module_delivery::DELIVERY_RECORD_BYTES),
            },
            ..intent
        },
        1,
    );
    assert!(store.save_pending(oversized).await.is_err());

    let expired = tinychannels_bus::DeliveryTerminalResult {
        id: "op-validate".into(),
        intent_fingerprint: "a".repeat(64),
        completed_at_unix_ms: 1,
    };
    store.save_result(expired).await.unwrap();
    assert!(store.load_result("op-validate").await.unwrap().is_none());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_flush_waiter_keeps_callback_work_and_can_retry() {
    let (host, callback, _server) = connected_host().await;
    callback.blocked.store(true, Ordering::SeqCst);
    host.deliver_inbound("telegram", serde_json::json!({"text":"hello"}))
        .await;
    let waiter = tokio::spawn({
        let host = host.clone();
        async move { host.flush().await }
    });
    tokio::task::yield_now().await;
    waiter.abort();
    let _ = waiter.await;
    callback.blocked.store(false, Ordering::SeqCst);
    callback.gate.notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(2), host.flush())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(callback.events.lock().await.len(), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_optional_host_ignores_status_but_reports_inbound_failure() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    host.report_status("telegram", "connecting", None).await;
    host.flush().await.unwrap();
    host.deliver_inbound("telegram", serde_json::json!({"text":"hello"}))
        .await;
    assert!(host.flush().await.is_err());
    host.shutdown().await.unwrap();
}
