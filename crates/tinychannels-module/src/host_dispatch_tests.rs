use super::*;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;

#[derive(Clone)]
struct TurnCallback {
    started: Arc<AtomicUsize>,
    changed: Arc<Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl TurnCallback {
    async fn process_inbound(&self, _channel: String, message: serde_json::Value) -> Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        if message["raw"]["fail"].as_bool() == Some(true) {
            return Err(Error::failed("fixture rejected turn"));
        }
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
    async fn deliver_inbound(&self, channel: String, message: serde_json::Value) -> Result<()> {
        self.process_inbound(channel, message).await
    }
}

async fn fixture(limit: usize) -> (HostChannels, TurnCallback, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    server.request_name(HOST_BUS_NAME).await.unwrap();
    let callback = TurnCallback {
        started: Arc::new(AtomicUsize::new(0)),
        changed: Arc::new(Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    server
        .serve_at(HOST_OBJECT_PATH.try_into().unwrap(), callback.clone())
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (
        HostChannels::new_at_with_dispatch(client, HOST_OBJECT_PATH.to_owned(), limit),
        callback,
        server,
    )
}

async fn started(host: &HostChannels, callback: &TurnCallback, count: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while callback.started.load(Ordering::SeqCst) < count {
            callback.changed.notified().await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("held first turn must not prevent another admitted turn: {error}; delivery failure: {:?}", host.owner.failure.lock().unwrap()));
}

#[tokio::test]
async fn bounded_dispatch_admits_other_turns_while_the_first_is_held() {
    let (host, callback, _server) = fixture(2).await;
    for id in 0..3 {
        host.deliver_inbound("telegram", inbound(serde_json::json!({"id":id})))
            .await;
    }
    started(&host, &callback, 2).await;
    assert_eq!(callback.started.load(Ordering::SeqCst), 2);
    callback.release.add_permits(1);
    started(&host, &callback, 3).await;
    callback.release.add_permits(2);
    host.flush().await.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn stopped_dispatch_drains_accepted_turns_even_when_the_waiter_is_canceled() {
    let (host, callback, _server) = fixture(2).await;
    host.deliver_inbound("discord", inbound(serde_json::json!({"id":1})))
        .await;
    started(&host, &callback, 1).await;
    let closing = host.clone();
    let waiter = tokio::spawn(async move { closing.shutdown().await });
    while !host.owner.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(
        !waiter.is_finished(),
        "stop must retain accepted turn ownership"
    );
    waiter.abort();
    let _ = waiter.await;
    callback.release.add_permits(1);
    host.shutdown().await.unwrap();
    host.deliver_inbound("discord", inbound(serde_json::json!({"id":2})))
        .await;
    assert_eq!(callback.started.load(Ordering::SeqCst), 1);
    assert!(host.flush().await.is_err());
}

#[tokio::test]
async fn failed_turns_do_not_stop_dispatch_and_flush_observes_accepted_frontier() {
    let (host, callback, _server) = fixture(2).await;
    host.deliver_inbound("telegram", inbound(serde_json::json!({"fail":true})))
        .await;
    host.deliver_inbound("telegram", serde_json::json!({"id":2}))
        .await;
    started(&host, &callback, 2).await;
    let flushing = host.clone();
    let waiter = tokio::spawn(async move { flushing.flush().await });
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "a failed turn cannot satisfy the held turn frontier"
    );
    callback.release.add_permits(1);
    assert!(
        waiter.await.unwrap().is_err(),
        "flush reports observed turn failure"
    );
    host.deliver_inbound("telegram", inbound(serde_json::json!({"id":3})))
        .await;
    started(&host, &callback, 3).await;
    callback.release.add_permits(1);
    host.shutdown().await.unwrap();
}

fn inbound(raw: serde_json::Value) -> tinychannels_bus::ChannelInboundEnvelope {
    tinychannels_bus::ChannelInboundEnvelope {
        raw: Some(raw),
        ..Default::default()
    }
}
