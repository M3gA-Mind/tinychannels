use super::*;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;

#[derive(Clone)]
struct Callback {
    events: Arc<Mutex<Vec<String>>>,
    blocked: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
    persist_failures: Arc<std::sync::atomic::AtomicUsize>,
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
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
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
}

async fn connected_host() -> (HostChannels, Callback, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let callback = Callback {
        events: Arc::new(Mutex::new(Vec::new())),
        blocked: Arc::new(AtomicBool::new(false)),
        gate: Arc::new(tokio::sync::Notify::new()),
        persist_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
