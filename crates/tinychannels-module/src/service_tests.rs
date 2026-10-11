use super::*;
use std::collections::BTreeMap;
use tinychannels_bus::ChannelsConfig;
use tinychannels_bus::config::WhatsAppConfig;

/// Mirrors the predicate in `start_channel`.
///
/// Extracted rather than duplicated in the test so the two cannot disagree:
/// the point of these cases is the *classification*, and a copy of the rule
/// would keep passing after the real one changed.
use super::is_webhook_backed as webhook_backed;

fn whatsapp(cloud: bool) -> ChannelsConfig {
    ChannelsConfig {
        whatsapp: Some(WhatsAppConfig {
            access_token: None,
            phone_number_id: cloud.then(|| "1".to_owned()),
            verify_token: None,
            app_secret: None,
            session_path: (!cloud).then(|| "/tmp/session".to_owned()),
            pair_phone: None,
            pair_code: None,
            allowed_numbers: Vec::new(),
        }),
        ..ChannelsConfig::default()
    }
}

#[test]
fn linq_is_refused_because_its_inbound_path_is_a_webhook() {
    assert!(webhook_backed("linq", &ChannelsConfig::default()));
}

#[test]
fn whatsapp_cloud_is_refused_but_whatsapp_web_is_not() {
    // Both report `name() == "whatsapp"`, so only the config shape separates
    // them. Getting this backwards would either refuse a working provider or
    // silently accept a dead one.
    assert!(webhook_backed("whatsapp", &whatsapp(true)));
    assert!(!webhook_backed("whatsapp", &whatsapp(false)));
}

#[test]
fn providers_with_a_real_listen_loop_are_unaffected() {
    for name in ["telegram", "discord", "slack", "irc", "signal"] {
        assert!(
            !webhook_backed(name, &ChannelsConfig::default()),
            "{name} should not be refused"
        );
    }
}

struct CaptureChannel(tokio::sync::mpsc::Sender<SendMessage>);

#[async_trait::async_trait]
impl tinychannels::Channel for CaptureChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.0.send(message.clone()).await?;
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn typed_intent_preserves_durable_key_and_thread_in_legacy_send() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let channel = Arc::new(CaptureChannel(sent));
    let hooks = Arc::new(Hooks::default());
    let mut running = HashMap::new();
    running.insert(
        "fixture".to_owned(),
        Running::start(channel, hooks, host.clone()),
    );
    let channels = Channels {
        progressive: Arc::new(crate::progressive::Replies::new(host.clone())),
        host: host.clone(),
        email: crate::email::EmailSender::new(),
        running: Arc::new(Mutex::new(running)),
        relays: Arc::new(Relays::new(host.clone())),
        terminal: AtomicBool::new(false),
        runtimes: None,
        shutdown_task: Mutex::new(None),
        delivery_admission: Arc::new(Mutex::new(())),
    };
    let intent = ChannelOutboundIntent {
        idempotency_key: "delivery-42".to_owned(),
        channel_id: "fixture".to_owned(),
        conversation_id: "room-7".to_owned(),
        reply_to_id: Some("message-3".to_owned()),
        thread_id: Some("thread-9".to_owned()),
        durability: tinychannels::DeliveryDurability::BestEffort,
        payload: tinychannels::OutboundPayload::Text {
            text: "hello".to_owned(),
        },
    };
    let result = channels
        .send_channel_intent("fixture".to_owned(), intent)
        .await
        .unwrap();
    assert!(result.message_id.is_none());
    assert!(result.receipt.is_none());
    assert!(result.raw.is_none());
    let sent = received.recv().await.unwrap();
    assert_eq!(sent.content, "hello");
    assert_eq!(sent.recipient, "room-7");
    assert_eq!(sent.thread_ts.as_deref(), Some("thread-9"));
    assert_eq!(sent.idempotency_key.as_deref(), Some("delivery-42"));
    channels.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn required_durability_is_refused_before_provider_send_without_host_queue() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let channel = Arc::new(CaptureChannel(sent));
    let hooks = Arc::new(Hooks::default());
    let mut running = HashMap::new();
    running.insert(
        "fixture".to_owned(),
        Running::start(channel, hooks, host.clone()),
    );
    let channels = Channels {
        progressive: Arc::new(crate::progressive::Replies::new(host.clone())),
        host: host.clone(),
        email: crate::email::EmailSender::new(),
        running: Arc::new(Mutex::new(running)),
        relays: Arc::new(Relays::new(host.clone())),
        terminal: AtomicBool::new(false),
        runtimes: None,
        shutdown_task: Mutex::new(None),
        delivery_admission: Arc::new(Mutex::new(())),
    };
    let result = channels
        .send_channel_intent(
            "fixture".to_owned(),
            ChannelOutboundIntent {
                channel_id: "fixture".to_owned(),
                conversation_id: "room-7".to_owned(),
                idempotency_key: "delivery-required".to_owned(),
                durability: tinychannels::DeliveryDurability::Required,
                payload: tinychannels::OutboundPayload::Text {
                    text: "must remain durable".to_owned(),
                },
                ..Default::default()
            },
        )
        .await;
    assert!(
        result.is_err(),
        "Required must not degrade to a direct send"
    );
    assert!(
        received.try_recv().is_err(),
        "provider must not see refused intent"
    );
    channels.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
}

#[derive(Default)]
struct QueueScope {
    pending: BTreeMap<String, tinychannels_bus::DeliveryQueueEntry>,
    failed: BTreeMap<String, tinychannels_bus::DeliveryQueueEntry>,
    results: BTreeMap<String, tinychannels_bus::DeliveryTerminalResult>,
}

#[derive(Clone, Default)]
struct DurableHost {
    current_scope: Arc<Mutex<String>>,
    scopes: Arc<Mutex<HashMap<String, QueueScope>>>,
    fail_pending_write: Arc<std::sync::atomic::AtomicBool>,
    fail_queue_read: Arc<std::sync::atomic::AtomicBool>,
    lose_result_reply: Arc<std::sync::atomic::AtomicBool>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl DurableHost {
    async fn deliver_inbound(
        &self,
        _channel: String,
        _message: serde_json::Value,
    ) -> tinybus::Result<()> {
        Ok(())
    }

    async fn report_status(
        &self,
        _channel: String,
        _state: String,
        _detail: Option<String>,
    ) -> tinybus::Result<()> {
        Ok(())
    }

    async fn persist_allowed_identity(
        &self,
        _channel: String,
        _identity: String,
    ) -> tinybus::Result<()> {
        Ok(())
    }

    async fn save_pending_delivery(
        &self,
        entry: tinychannels_bus::DeliveryQueueEntry,
    ) -> tinybus::Result<()> {
        if self.fail_pending_write.swap(false, Ordering::SeqCst) {
            return Err(tinybus::Error::failed("fixture pending write failed"));
        }
        let scope = self.current_scope.lock().await.clone();
        let mut scopes = self.scopes.lock().await;
        let queue = scopes.entry(scope).or_default();
        if queue.pending.len() >= tinychannels_bus::module_delivery::DELIVERY_PENDING_CAPACITY
            && !queue.pending.contains_key(&entry.id)
        {
            return Err(tinybus::Error::failed("fixture pending queue is full"));
        }
        if let Some(existing) = queue.pending.get(&entry.id)
            && existing.intent != entry.intent
        {
            return Err(tinybus::Error::failed("fixture idempotency conflict"));
        }
        queue.pending.insert(entry.id.clone(), entry);
        Ok(())
    }

    async fn load_pending_delivery(
        &self,
        id: String,
    ) -> tinybus::Result<Option<tinychannels_bus::DeliveryQueueEntry>> {
        let scope = self.current_scope.lock().await.clone();
        Ok(self
            .scopes
            .lock()
            .await
            .get(&scope)
            .and_then(|queue| queue.pending.get(&id).cloned()))
    }

    async fn load_pending_deliveries(
        &self,
    ) -> tinybus::Result<Vec<tinychannels_bus::DeliveryQueueEntry>> {
        if self.fail_queue_read.load(Ordering::SeqCst) {
            return Err(tinybus::Error::failed("fixture pending read failed"));
        }
        let scope = self.current_scope.lock().await.clone();
        Ok(self
            .scopes
            .lock()
            .await
            .get(&scope)
            .map(|queue| queue.pending.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn remove_pending_delivery(&self, id: String) -> tinybus::Result<()> {
        let scope = self.current_scope.lock().await.clone();
        if let Some(queue) = self.scopes.lock().await.get_mut(&scope) {
            queue.pending.remove(&id);
        }
        Ok(())
    }

    async fn save_failed_delivery(
        &self,
        entry: tinychannels_bus::DeliveryQueueEntry,
    ) -> tinybus::Result<()> {
        let scope = self.current_scope.lock().await.clone();
        self.scopes
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
    ) -> tinybus::Result<Option<tinychannels_bus::DeliveryQueueEntry>> {
        let scope = self.current_scope.lock().await.clone();
        Ok(self
            .scopes
            .lock()
            .await
            .get(&scope)
            .and_then(|queue| queue.failed.get(&id).cloned()))
    }

    async fn save_delivery_result(
        &self,
        result: tinychannels_bus::DeliveryTerminalResult,
    ) -> tinybus::Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let scope = self.current_scope.lock().await.clone();
        let mut scopes = self.scopes.lock().await;
        let queue = scopes.entry(scope).or_default();
        queue.results.retain(|_, saved| {
            saved.completed_at_unix_ms.saturating_add(
                tinychannels_bus::module_delivery::DELIVERY_RESULT_TTL_SECONDS * 1000,
            ) > now
        });
        if queue.results.len() >= tinychannels_bus::module_delivery::DELIVERY_RESULT_CAPACITY
            && !queue.results.contains_key(&result.id)
        {
            return Err(tinybus::Error::failed("fixture result capacity exceeded"));
        }
        if let Some(existing) = queue.results.get(&result.id)
            && existing != &result
        {
            return Err(tinybus::Error::failed("fixture result key conflicts"));
        }
        queue.results.insert(result.id.clone(), result);
        if self.lose_result_reply.swap(false, Ordering::SeqCst) {
            return Err(tinybus::Error::failed("fixture lost terminal reply"));
        }
        Ok(())
    }

    async fn load_delivery_result(
        &self,
        id: String,
    ) -> tinybus::Result<Option<tinychannels_bus::DeliveryTerminalResult>> {
        let scope = self.current_scope.lock().await.clone();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Ok(self
            .scopes
            .lock()
            .await
            .get(&scope)
            .and_then(|queue| queue.results.get(&id).cloned())
            .filter(|result| {
                result.completed_at_unix_ms.saturating_add(
                    tinychannels_bus::module_delivery::DELIVERY_RESULT_TTL_SECONDS * 1000,
                ) > now
            }))
    }
}

struct FailingChannel(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl tinychannels::Channel for FailingChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!("fixture provider failure"))
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

struct EndedChannel(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl tinychannels::Channel for EndedChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn channels_with_store(
    channel: Arc<dyn tinychannels::Channel>,
) -> (Channels, DurableHost, tinybus::Connection) {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let callbacks = DurableHost {
        current_scope: Arc::new(Mutex::new("scope-a".into())),
        ..DurableHost::default()
    };
    server
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callbacks.clone(),
        )
        .await
        .unwrap();
    server
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let client = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    let hooks = Arc::new(Hooks::default());
    let running = HashMap::from([(
        "fixture".to_owned(),
        Running::start(channel, hooks, host.clone()),
    )]);
    let relays = Arc::new(Relays::new(host.clone()));
    let channels = Channels {
        progressive: Arc::new(crate::progressive::Replies::new(host.clone())),
        host,
        email: crate::email::EmailSender::new(),
        running: Arc::new(Mutex::new(running)),
        relays,
        terminal: AtomicBool::new(false),
        runtimes: None,
        shutdown_task: Mutex::new(None),
        delivery_admission: Arc::new(Mutex::new(())),
    };
    (channels, callbacks, server)
}

fn required_intent(id: &str, text: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: id.into(),
        channel_id: "fixture".into(),
        conversation_id: "room-1".into(),
        durability: tinychannels::DeliveryDurability::Required,
        payload: tinychannels::OutboundPayload::Text { text: text.into() },
        ..Default::default()
    }
}

#[tokio::test]
async fn required_send_persists_before_delivery_and_replays_lost_final_reply() {
    let (sent, mut received) = tokio::sync::mpsc::channel(4);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks.lose_result_reply.store(true, Ordering::SeqCst);
    let intent = required_intent("delivery-replay", "confidential body");

    channels
        .send_channel_intent("fixture".into(), intent.clone())
        .await
        .unwrap();
    assert_eq!(received.recv().await.unwrap().content, "confidential body");
    channels
        .send_channel_intent("fixture".into(), intent)
        .await
        .unwrap();
    assert!(received.try_recv().is_err(), "retry must not resend");
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("delivery-replay", "changed payload"),
            )
            .await
            .is_err()
    );

    let scopes = callbacks.scopes.lock().await;
    let scope = scopes.get("scope-a").unwrap();
    assert!(scope.pending.is_empty());
    assert!(scope.failed.is_empty());
    assert_eq!(scope.results.len(), 1);
}

#[tokio::test]
async fn pending_persistence_failure_refuses_before_provider_send() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks.fail_pending_write.store(true, Ordering::SeqCst);
    assert!(
        channels
            .send_channel_intent("fixture".into(), required_intent("persist-fail", "body"))
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    assert!(
        channels.delivery_admission.try_lock().is_ok(),
        "admission guard leaked"
    );
    let scopes = callbacks.scopes.lock().await;
    assert!(scopes.get("scope-a").is_none());
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn required_send_refuses_invalid_and_oversized_intents_before_persistence() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;

    let mut invalid_key = required_intent("invalid-key", "body");
    invalid_key.idempotency_key.clear();
    assert!(
        channels
            .send_channel_intent("fixture".into(), invalid_key)
            .await
            .is_err()
    );

    let mut invalid_channel = required_intent("invalid-channel", "body");
    invalid_channel.channel_id = " fixture".into();
    assert!(
        channels
            .send_channel_intent(" fixture".into(), invalid_channel)
            .await
            .is_err()
    );

    let oversized = required_intent(
        "oversized",
        &"x".repeat(tinychannels_bus::module_delivery::DELIVERY_RECORD_BYTES),
    );
    assert!(
        channels
            .send_channel_intent("fixture".into(), oversized)
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    assert!(callbacks.scopes.lock().await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_required_delivery_target_keeps_the_intent_pending() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    channels.running.lock().await.remove("fixture");

    assert!(
        channels
            .send_channel_intent("fixture".into(), required_intent("no-target", "body"))
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    let scopes = callbacks.scopes.lock().await;
    assert_eq!(scopes.get("scope-a").unwrap().pending.len(), 1);
    assert!(scopes.get("scope-a").unwrap().failed.is_empty());
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn ended_provider_does_not_mark_unstarted_delivery_as_unknown_send() {
    let send_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channels, callbacks, _server) =
        channels_with_store(Arc::new(EndedChannel(send_calls.clone()))).await;
    let provider = channels
        .running
        .lock()
        .await
        .get("fixture")
        .unwrap()
        .clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !provider.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(
        channels
            .send_channel_intent("fixture".into(), required_intent("ended-provider", "body"))
            .await
            .is_err()
    );
    assert_eq!(send_calls.load(Ordering::SeqCst), 0);
    let scopes = callbacks.scopes.lock().await;
    let queue = scopes.get("scope-a").unwrap();
    assert!(queue.failed.is_empty());
    let pending = queue.pending.get("ended-provider").unwrap();
    assert_eq!(pending.recovery_state, None);
    assert!(pending.platform_send_started_at_unix_ms.is_none());
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn replayed_terminal_result_acknowledges_a_remaining_pending_record() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = required_intent("terminal-cleanup", "body");
    let fingerprint = intent_fingerprint(&intent).unwrap();
    let store = crate::delivery::HostDeliveryStore::new(channels.host.connection());
    store
        .save_pending(QueuedDelivery::new(
            "terminal-cleanup",
            intent.clone(),
            now_unix_ms(),
        ))
        .await
        .unwrap();
    store
        .save_result(tinychannels_bus::DeliveryTerminalResult {
            id: "terminal-cleanup".into(),
            intent_fingerprint: fingerprint,
            completed_at_unix_ms: now_unix_ms(),
        })
        .await
        .unwrap();

    channels
        .send_channel_intent("fixture".into(), intent)
        .await
        .unwrap();
    assert!(received.try_recv().is_err());
    assert!(
        callbacks
            .scopes
            .lock()
            .await
            .get("scope-a")
            .unwrap()
            .pending
            .is_empty()
    );
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_recovery_acks_a_matching_terminal_result_without_resending() {
    use tinychannels::delivery::{DeliveryQueueStore as _, DeliveryRecoveryState, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = required_intent("recovery-terminal", "already sent");
    let store = crate::delivery::HostDeliveryStore::new(channels.host.connection());
    let mut entry = QueuedDelivery::new("recovery-terminal", intent.clone(), now_unix_ms());
    entry.recovery_state = Some(DeliveryRecoveryState::SendAttemptStarted);
    entry.platform_send_started_at_unix_ms = Some(now_unix_ms());
    store.save_pending(entry).await.unwrap();
    store
        .save_result(tinychannels_bus::DeliveryTerminalResult {
            id: "recovery-terminal".into(),
            intent_fingerprint: intent_fingerprint(&intent).unwrap(),
            completed_at_unix_ms: now_unix_ms(),
        })
        .await
        .unwrap();

    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();

    let scopes = callbacks.scopes.lock().await;
    let scope = scopes.get("scope-a").unwrap();
    assert!(scope.pending.is_empty());
    assert!(scope.failed.is_empty());
    drop(scopes);
    assert!(
        received.try_recv().is_err(),
        "recovery must not resend success"
    );
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_recovery_rejects_a_conflicting_terminal_result() {
    use tinychannels::delivery::{DeliveryQueueStore as _, DeliveryRecoveryState, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = required_intent("recovery-conflict", "pending intent");
    let store = crate::delivery::HostDeliveryStore::new(channels.host.connection());
    let mut entry = QueuedDelivery::new("recovery-conflict", intent, now_unix_ms());
    entry.recovery_state = Some(DeliveryRecoveryState::SendAttemptStarted);
    entry.platform_send_started_at_unix_ms = Some(now_unix_ms());
    store.save_pending(entry).await.unwrap();
    store
        .save_result(tinychannels_bus::DeliveryTerminalResult {
            id: "recovery-conflict".into(),
            intent_fingerprint: "f".repeat(64),
            completed_at_unix_ms: now_unix_ms(),
        })
        .await
        .unwrap();

    assert!(
        recover_delivery_channel(&channels.host, &channels.running, "fixture")
            .await
            .is_err(),
        "a terminal result for a different intent is a key conflict"
    );
    let scopes = callbacks.scopes.lock().await;
    let scope = scopes.get("scope-a").unwrap();
    assert_eq!(scope.pending.len(), 1, "conflicts stay retryable");
    assert!(scope.failed.is_empty());
    drop(scopes);
    assert!(received.try_recv().is_err());
    channels.shutdown().await.unwrap_err();
}

#[tokio::test]
async fn failed_and_pending_keys_refuse_reuse_with_different_intents() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let failed = required_intent("failed-conflict", "original");
    let pending = required_intent("pending-conflict", "original");
    let store = crate::delivery::HostDeliveryStore::new(channels.host.connection());
    store
        .save_failed(QueuedDelivery::new("failed-conflict", failed, 1))
        .await
        .unwrap();
    store
        .save_pending(QueuedDelivery::new("pending-conflict", pending, 1))
        .await
        .unwrap();

    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("failed-conflict", "original"),
            )
            .await
            .is_err()
    );
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("failed-conflict", "changed"),
            )
            .await
            .is_err()
    );
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("pending-conflict", "changed"),
            )
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    let scopes = callbacks.scopes.lock().await;
    let queue = scopes.get("scope-a").unwrap();
    assert_eq!(queue.failed.len(), 1);
    assert_eq!(queue.pending.len(), 1);
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn changed_intent_cannot_reuse_a_terminal_result_key() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks
        .scopes
        .lock()
        .await
        .entry("scope-a".into())
        .or_default()
        .results
        .insert(
            "result-conflict".into(),
            tinychannels_bus::DeliveryTerminalResult {
                id: "result-conflict".into(),
                intent_fingerprint: "a".repeat(64),
                completed_at_unix_ms: now_unix_ms(),
            },
        );

    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("result-conflict", "different intent"),
            )
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn stop_channel_keeps_provider_running_when_queue_recovery_fails() {
    let (sent, _received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks.fail_queue_read.store(true, Ordering::SeqCst);

    assert!(channels.stop_channel("fixture".into()).await.is_err());
    assert!(channels.running.lock().await.contains_key("fixture"));

    callbacks.fail_queue_read.store(false, Ordering::SeqCst);
    channels.stop_channel("fixture".into()).await.unwrap();
    assert!(!channels.running.lock().await.contains_key("fixture"));
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn best_effort_structured_payload_is_encoded_for_legacy_provider_send() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, _callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = ChannelOutboundIntent {
        idempotency_key: "structured-1".into(),
        channel_id: "fixture".into(),
        durability: tinychannels::DeliveryDurability::BestEffort,
        payload: tinychannels::OutboundPayload::NativeChannelData {
            data: serde_json::json!({"kind": "card", "safe": true}),
        },
        ..Default::default()
    };

    channels
        .send_channel_intent("fixture".into(), intent)
        .await
        .unwrap();
    let message = received.recv().await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&message.content).unwrap()["kind"],
        "card"
    );
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_failure_stays_pending_then_recovery_fails_closed_once() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channels, callbacks, _server) =
        channels_with_store(Arc::new(FailingChannel(calls.clone()))).await;
    let intent = required_intent("provider-fail", "body");
    assert!(
        channels
            .send_channel_intent("fixture".into(), intent.clone())
            .await
            .is_err()
    );
    {
        let scopes = callbacks.scopes.lock().await;
        let scope = scopes.get("scope-a").unwrap();
        assert_eq!(scope.pending.len(), 1);
        assert!(scope.failed.is_empty());
    }

    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let scopes = callbacks.scopes.lock().await;
    let scope = scopes.get("scope-a").unwrap();
    assert!(scope.pending.is_empty());
    assert_eq!(scope.failed.len(), 1);
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_recovery_is_ordered_and_respects_host_user_scope() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(4);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let store = crate::delivery::HostDeliveryStore::new(channels.host.connection());
    let later = required_intent("recovery-later", "later");
    let earlier = required_intent("recovery-earlier", "earlier");
    store
        .save_pending(QueuedDelivery::new("recovery-later", later, 20))
        .await
        .unwrap();
    store
        .save_pending(QueuedDelivery::new("recovery-earlier", earlier, 10))
        .await
        .unwrap();
    *callbacks.current_scope.lock().await = "scope-b".into();
    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();
    assert!(
        received.try_recv().is_err(),
        "another user scope stays isolated"
    );

    *callbacks.current_scope.lock().await = "scope-a".into();
    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();
    assert_eq!(received.recv().await.unwrap().content, "earlier");
    assert_eq!(received.recv().await.unwrap().content, "later");
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_retryable_queue_before_stopping_provider() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, _callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let store = crate::delivery::HostDeliveryStore::new(channels.host.connection());
    let intent = required_intent("shutdown-drain", "drain before stop");
    store
        .save_pending(QueuedDelivery::new("shutdown-drain", intent, 1))
        .await
        .unwrap();

    channels.shutdown().await.unwrap();
    assert_eq!(received.recv().await.unwrap().content, "drain before stop");
}

#[tokio::test]
async fn shutdown_store_failure_still_joins_provider_and_can_retry_persistence() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, _received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let provider = channels
        .running
        .lock()
        .await
        .get("fixture")
        .unwrap()
        .clone();
    let store = crate::delivery::HostDeliveryStore::new(channels.host.connection());
    let intent = required_intent("shutdown-store-outage", "retain for restart");
    store
        .save_pending(QueuedDelivery::new(
            "shutdown-store-outage",
            intent,
            now_unix_ms(),
        ))
        .await
        .unwrap();
    channels.relays.reserve().await.unwrap();
    callbacks.fail_queue_read.store(true, Ordering::SeqCst);

    assert!(channels.shutdown().await.is_err());
    assert!(
        provider.is_finished(),
        "native provider must be joined on error"
    );
    assert!(channels.running.lock().await.is_empty());
    assert!(
        channels.relays.is_empty().await,
        "relay owner must be stopped"
    );
    assert_eq!(
        callbacks
            .scopes
            .lock()
            .await
            .get("scope-a")
            .unwrap()
            .pending
            .len(),
        1,
        "queue data remains host-owned and retryable"
    );
    assert!(
        channels.host.flush().await.is_err(),
        "host callback owner must be closed after shutdown cleanup"
    );

    callbacks.fail_queue_read.store(false, Ordering::SeqCst);
    channels.shutdown().await.unwrap();
    assert_eq!(
        callbacks
            .scopes
            .lock()
            .await
            .get("scope-a")
            .unwrap()
            .pending
            .len(),
        1,
        "a stopped provider leaves the durable entry for the next start"
    );
}

#[tokio::test]
async fn start_waiting_for_delivery_admission_is_rejected_after_shutdown_freezes() {
    let (sent, _received) = tokio::sync::mpsc::channel(1);
    let (channels, _callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let provider = channels
        .running
        .lock()
        .await
        .get("fixture")
        .unwrap()
        .clone();
    let admission = channels.delivery_admission.clone();
    let guard = admission.lock().await;
    let admission_future = finish_start_admission(
        &channels.host,
        &channels.running,
        &admission,
        &channels.terminal,
        "fixture",
    );
    tokio::pin!(admission_future);
    tokio::select! {
        result = &mut admission_future => panic!("admission should wait for its lock: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }

    channels.terminal.store(true, Ordering::SeqCst);
    drop(guard);

    assert!(admission_future.await.is_err());
    assert!(provider.is_finished());
    assert!(channels.running.lock().await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn full_durable_queue_applies_backpressure_before_provider_send() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    {
        let mut scopes = callbacks.scopes.lock().await;
        let scope = scopes.entry("scope-a".into()).or_default();
        for index in 0..tinychannels_bus::module_delivery::DELIVERY_PENDING_CAPACITY {
            let id = format!("queued-{index}");
            scope.pending.insert(
                id.clone(),
                tinychannels_bus::DeliveryQueueEntry::new(
                    id.clone(),
                    required_intent(&id, "queued"),
                    index as u64,
                ),
            );
        }
    }
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("queue-overflow", "must stay queued"),
            )
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    callbacks
        .scopes
        .lock()
        .await
        .get_mut("scope-a")
        .unwrap()
        .pending
        .clear();
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_catalog_reports_webhook_mode_without_starting_a_listener() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let channels = Channels::new(host.clone(), None);
    let config = ChannelsConfig {
        linq: Some(tinychannels_bus::config::LinqConfig {
            api_token: "fixture-token".into(),
            from_phone: "+15550000001".into(),
            allowed_senders: vec![],
        }),
        ..Default::default()
    };
    let result = tinybus::Interface::call(
        &channels,
        &tinybus::MemberName::try_from("DescribeConfiguredChannels").unwrap(),
        serde_json::json!([config, HttpProxyPolicy::default()]),
    )
    .await
    .unwrap();
    assert_eq!(result.as_array().unwrap().len(), 1);
    assert_eq!(result[0]["name"], "linq");
    assert_eq!(result[0]["ingress"], "external_webhook");
    assert!(channels.running.lock().await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn preparation_preserves_explicit_keys_and_typed_thread_before_provider_io() {
    use tinychannels_bus::module_runtime::ChannelMessagePreparation;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let channels = Channels::new(host.clone(), None);
    let legacy =
        serde_json::json!({"text":"fixture", "idempotencyKey":"original-key", "threadId":"topic"});
    let prepared = channels
        .prepare_outbound_intent(
            "telegram".into(),
            ChannelMessagePreparation::Legacy(legacy.clone()),
        )
        .await
        .unwrap();
    assert_eq!(prepared.intent.idempotency_key, "original-key");
    assert_eq!(prepared.legacy_message, legacy);
    let typed = SendMessage::new("fixture", "recipient").in_thread(Some("topic".into()));
    let expected = tinychannels::outbound_intent_from_send_message("telegram", &typed);
    let prepared = channels
        .prepare_outbound_intent("telegram".into(), ChannelMessagePreparation::Typed(typed))
        .await
        .unwrap();
    assert_eq!(prepared.intent.idempotency_key, expected.idempotency_key);
    assert_eq!(prepared.intent.thread_id.as_deref(), Some("topic"));
    assert_eq!(
        prepared.legacy_message["idempotencyKey"],
        prepared.intent.idempotency_key
    );
    assert!(
        channels
            .prepare_outbound_intent(
                "telegram".into(),
                ChannelMessagePreparation::Legacy(serde_json::json!({"text":"a".repeat(tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES)}))
            )
            .await
            .is_err()
    );
    assert!(
        channels
            .prepare_outbound_intent(
                "telegram".into(),
                ChannelMessagePreparation::Legacy(
                    serde_json::json!({"text": "a".repeat(256*1024)})
                ),
            )
            .await
            .is_ok(),
        "large legacy sends must not acquire a smaller callback-queue limit"
    );
    let original = tinychannels_bus::ChannelOutboundIntent {
        channel_id: "telegram".into(),
        idempotency_key: "existing-required-key".into(),
        durability: tinychannels_bus::DeliveryDurability::Required,
        payload: tinychannels_bus::OutboundPayload::Text {
            text: "fixture".into(),
        },
        ..Default::default()
    };
    let rendered = channels
        .prepare_outbound_intent(
            "telegram".into(),
            ChannelMessagePreparation::Intent(original.clone()),
        )
        .await
        .unwrap();
    assert_eq!(rendered.intent.idempotency_key, original.idempotency_key);
    assert_eq!(rendered.intent.durability, original.durability);
    assert_eq!(rendered.legacy_message["text"], "fixture");
    assert_eq!(
        rendered.legacy_message["idempotencyKey"],
        "existing-required-key"
    );
    assert!(
        channels
            .prepare_outbound_intent(
                "discord".into(),
                ChannelMessagePreparation::Intent(original)
            )
            .await
            .is_err()
    );
    assert!(channels.running.lock().await.is_empty());
    channels.shutdown().await.unwrap();
}

struct OutboundOnlyFixture(tokio::sync::mpsc::Sender<SendMessage>);

#[async_trait::async_trait]
impl tinychannels::Channel for OutboundOnlyFixture {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.0.send(message.clone()).await?;
        Ok(())
    }
    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        panic!("external webhook providers must not start a native listener")
    }
}

#[tokio::test]
async fn outbound_only_worker_sends_and_joins_without_polling_webhook_listener() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let running = Running::start_outbound(
        Arc::new(OutboundOnlyFixture(sent)),
        Arc::new(Hooks::default()),
        host.clone(),
    );
    running
        .send(SendMessage::new("fixture", "recipient"))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(received.recv().await.unwrap().content, "fixture");
    assert!(!running.is_finished());
    running.stop().await.unwrap();
    assert!(running.is_finished());
    assert!(
        running
            .send(SendMessage::new("closed", "recipient"))
            .await
            .is_err()
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_preparation_preserves_escaped_persisted_keys_and_provider_thread_rules() {
    use tinychannels_bus::module_inbound::{ChannelInboundSource, PrepareChannelInboundRequest};
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = Channels::new(HostChannels::new(connection), None);
    let message = tinychannels_bus::ChannelMessage {
        id: "message-1".into(),
        channel: "discord".into(),
        sender: "sender/a".into(),
        reply_target: "room#b".into(),
        content: "fixture content".into(),
        timestamp: 42,
        thread_ts: Some("thread/c".into()),
        sender_name: Some("Display".into()),
    };
    let prepared = service
        .prepare_channel_inbound(PrepareChannelInboundRequest {
            source: ChannelInboundSource::Legacy {
                message: message.clone(),
                envelope: None,
            },
            namespace: "openhuman".into(),
            session_policy: Default::default(),
        })
        .await
        .unwrap();
    assert_eq!(
        prepared.thread_id,
        "channel:discord/sender%2Fa/room%23b#thread:thread%2Fc"
    );
    assert_eq!(prepared.client_id, "inbound:discord:sender/a");
    assert_eq!(
        prepared.history_key,
        "discord_sender/a_room#b_thread:thread/c"
    );
    assert_eq!(prepared.message.timestamp, 42);
    assert_eq!(prepared.message.sender_name.as_deref(), Some("Display"));
    assert_eq!(prepared.envelope.text, "fixture content");
    assert_eq!(
        prepared.session_key,
        "openhuman:discord:default:unknown:room#b:thread/c"
    );
    service.shutdown().await.unwrap();
}

async fn inbound_preparation_service() -> Channels {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    Channels::new(HostChannels::new(connection), None)
}

#[tokio::test]
async fn hosted_identity_preserves_absent_sender_and_telegram_topic_rules() {
    use tinychannels_bus::module_inbound::PrepareChannelIdentityRequest;
    let service = inbound_preparation_service().await;
    let prepared = service
        .prepare_channel_identity(PrepareChannelIdentityRequest {
            channel: "telegram:bot".into(),
            sender: None,
            reply_target: Some("chat".into()),
            thread_ts: Some("topic".into()),
        })
        .await
        .unwrap();
    assert_eq!(prepared.thread_id, "channel:telegram:bot/chat");
    assert_eq!(prepared.client_id, "inbound");
    let prepared = service
        .prepare_channel_identity(PrepareChannelIdentityRequest {
            channel: "discord:bot".into(),
            sender: Some("user:one".into()),
            reply_target: Some("chat".into()),
            thread_ts: Some("topic".into()),
        })
        .await
        .unwrap();
    assert_eq!(
        prepared.thread_id,
        "channel:discord:bot/user:one/chat#thread:topic"
    );
    assert_eq!(prepared.client_id, "inbound:discord%3Abot:user%3Aone");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_envelope_preserves_media_account_and_group_isolation_policy() {
    use tinychannels_bus::module_inbound::{ChannelInboundSource, PrepareChannelInboundRequest};
    let service = inbound_preparation_service().await;
    let mut envelope = tinychannels_bus::ChannelInboundEnvelope::default();
    envelope.channel.id = "discord".into();
    envelope.channel.account_id = Some("account".into());
    envelope.message_id = "msg".into();
    envelope.sender.id = "user".into();
    envelope.sender.name = Some("Display".into());
    envelope.conversation.id = "room".into();
    envelope.conversation.kind = tinychannels_bus::channel::ConversationKind::Group;
    envelope.conversation.thread_id = Some("thread".into());
    envelope.text = "content".into();
    envelope
        .media
        .push(tinychannels_bus::channel::MediaReference {
            url: Some("https://fixture.invalid/media".into()),
            kind: tinychannels_bus::channel::MediaKind::Image,
            ..Default::default()
        });
    let request = PrepareChannelInboundRequest {
        source: ChannelInboundSource::Envelope {
            envelope: envelope.clone(),
            timestamp: 123,
        },
        namespace: "tenant".into(),
        session_policy: Default::default(),
    };
    let prepared = service
        .prepare_channel_inbound(request.clone())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(prepared.envelope).unwrap(),
        serde_json::to_value(envelope).unwrap()
    );
    assert_eq!(prepared.message.timestamp, 123);
    assert_eq!(prepared.message.sender_name.as_deref(), Some("Display"));
    assert_eq!(
        prepared.session_key,
        "tenant:discord:account:group:room:thread"
    );
    let mut per_user = request;
    per_user.session_policy.thread_sessions_per_user = true;
    let prepared = service.prepare_channel_inbound(per_user).await.unwrap();
    assert_eq!(
        prepared.session_key,
        "tenant:discord:account:group:room:thread:user"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_preparation_refuses_mismatched_identity_oversized_payload_and_shutdown() {
    use tinychannels_bus::module_inbound::{
        ChannelInboundSource, PrepareChannelIdentityRequest, PrepareChannelInboundRequest,
    };
    let service = inbound_preparation_service().await;
    let message = tinychannels_bus::ChannelMessage {
        channel: "discord".into(),
        id: "msg".into(),
        sender: "user".into(),
        reply_target: "room".into(),
        ..Default::default()
    };
    let mut envelope = tinychannels::inbound_envelope_from_legacy_message(&message);
    envelope.sender.id = "other".into();
    let request = PrepareChannelInboundRequest {
        source: ChannelInboundSource::Legacy {
            message: message.clone(),
            envelope: Some(envelope),
        },
        namespace: "tenant".into(),
        session_policy: Default::default(),
    };
    let error = service
        .prepare_channel_inbound(request)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("identity does not match"));
    let mut huge = message;
    huge.content = "x".repeat(tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES);
    let request = PrepareChannelInboundRequest {
        source: ChannelInboundSource::Legacy {
            message: huge,
            envelope: None,
        },
        namespace: "tenant".into(),
        session_policy: Default::default(),
    };
    assert!(
        service
            .prepare_channel_inbound(request.clone())
            .await
            .is_err()
    );
    let identity = PrepareChannelIdentityRequest {
        channel: "discord".into(),
        sender: Some("x".repeat(4096)),
        reply_target: None,
        thread_ts: None,
    };
    assert!(
        service
            .prepare_channel_identity(identity.clone())
            .await
            .is_err()
    );
    service.shutdown().await.unwrap();
    assert!(service.prepare_channel_inbound(request).await.is_err());
    assert!(service.prepare_channel_identity(identity).await.is_err());
}

#[cfg(feature = "static-link")]
#[test]
fn exported_manifest_preserves_the_complete_contract_member_order() {
    let linked = exports::linked_module().unwrap();
    let provided = linked.manifest.provides.first().unwrap();
    let actual: Vec<_> = provided
        .methods
        .iter()
        .map(|method| method.as_str())
        .collect();
    assert_eq!(actual, tinychannels_bus::METHODS);
}

#[tokio::test]
async fn progressive_reply_is_opened_owned_and_closed_inside_the_module() {
    use tinychannels_bus::module_progressive::{ProgressiveCommand, ProgressiveResult};
    let service = inbound_preparation_service().await;
    let opened = service
        .execute_progressive(ProgressiveCommand::Open {
            channel: "telegram:fixture".into(),
        })
        .await
        .unwrap();
    let ProgressiveResult::Opened { handle } = opened else {
        panic!("missing progressive handle")
    };
    assert_eq!(
        service
            .execute_progressive(ProgressiveCommand::Close {
                handle: handle.clone()
            })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );
    assert_eq!(
        service
            .execute_progressive(ProgressiveCommand::Close { handle })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );
    service.shutdown().await.unwrap();
    assert!(
        service
            .execute_progressive(ProgressiveCommand::Open {
                channel: "telegram:fixture".into()
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn remote_help_is_parsed_and_rendered_inside_the_module() {
    use tinychannels_bus::module_remote::{RemoteOperation, RemoteRoute};
    let service = inbound_preparation_service().await;
    let message = serde_json::from_value(serde_json::json!({
        "id":"fixture", "sender":"sender", "reply_target":"chat",
        "content":"/HELP@FixtureBot", "channel":"telegram", "timestamp":1
    }))
    .unwrap();
    let result = service
        .execute_remote(RemoteOperation::Execute {
            workspace_dir: ".".into(),
            message,
            route: RemoteRoute {
                provider: "fixture".into(),
                model: "fixture".into(),
            },
            history_len: 0,
        })
        .await
        .unwrap();
    assert_eq!(
        result.reply.as_deref(),
        Some(tinychannels::remote::build_remote_help_response().as_str())
    );
    service.shutdown().await.unwrap();
}
