use super::*;
use std::sync::atomic::AtomicUsize;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;

fn consume_failure(counter: &AtomicUsize) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    loop {
        if remaining == 0 {
            return false;
        }
        match counter.compare_exchange_weak(
            remaining,
            remaining - 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
}

#[derive(Clone)]
struct Callback {
    events: Arc<Mutex<Vec<String>>>,
    blocked: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
    transcription_blocked: Arc<AtomicBool>,
    transcriptions_started: Arc<AtomicUsize>,
    transcription_started: Arc<tokio::sync::Notify>,
    transcription_gate: Arc<tokio::sync::Notify>,
    controller_blocked: Arc<AtomicBool>,
    controllers_started: Arc<AtomicUsize>,
    controller_started: Arc<tokio::sync::Notify>,
    controller_gate: Arc<tokio::sync::Notify>,
    persist_failures: Arc<std::sync::atomic::AtomicUsize>,
    event_failures: Arc<AtomicUsize>,
    storage: Arc<crate::storage_fixture::StorageHost>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn transcribe(
        &self,
        request: tinychannels_bus::HostTranscriptionRequest,
    ) -> Result<tinychannels_bus::HostTranscriptionResult> {
        assert_eq!(request.mime_type.as_deref(), Some("audio/wav"));
        self.transcriptions_started.fetch_add(1, Ordering::SeqCst);
        self.transcription_started.notify_one();
        while self.transcription_blocked.load(Ordering::SeqCst) {
            self.transcription_gate.notified().await;
        }
        self.events
            .lock()
            .await
            .push("transcription:complete".into());
        Ok(tinychannels_bus::HostTranscriptionResult {
            text: "spoken words".into(),
            language: request.language,
            duration_secs: Some(1.25),
        })
    }

    async fn publish_event(
        &self,
        publication: tinychannels_bus::HostEventPublication,
    ) -> Result<()> {
        if consume_failure(&self.event_failures) {
            return Err(tinybus::Error::failed("fixture event failure"));
        }
        self.events
            .lock()
            .await
            .push(format!("event:{}:{}", publication.domain, publication.kind));
        Ok(())
    }

    async fn dispatch_controller_backend(
        &self,
        request: tinychannels_bus::ChannelBackendRequest,
    ) -> Result<tinychannels_bus::ChannelBackendResult> {
        self.controllers_started.fetch_add(1, Ordering::SeqCst);
        self.controller_started.notify_one();
        while self.controller_blocked.load(Ordering::SeqCst) {
            self.controller_gate.notified().await;
        }
        match request {
            tinychannels_bus::ChannelBackendRequest::SendReaction { channel, .. } => {
                self.events
                    .lock()
                    .await
                    .push(format!("controller:{channel}:reaction"));
                Ok(tinychannels_bus::ChannelBackendResult::SendReaction(
                    tinychannels_bus::controllers::ChannelReactionResult {
                        success: true,
                        message: None,
                        raw: None,
                    },
                ))
            }
            _ => Err(tinybus::Error::failed(
                "unexpected controller fixture request",
            )),
        }
    }

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
        if consume_failure(&self.persist_failures) {
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
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let storage = Arc::new(crate::storage_fixture::StorageHost::serve(&server, "scope-a").await);
    let callback = Callback {
        events: Arc::new(Mutex::new(Vec::new())),
        blocked: Arc::new(AtomicBool::new(false)),
        gate: Arc::new(tokio::sync::Notify::new()),
        transcription_blocked: Arc::new(AtomicBool::new(false)),
        transcriptions_started: Arc::new(AtomicUsize::new(0)),
        transcription_started: Arc::new(tokio::sync::Notify::new()),
        transcription_gate: Arc::new(tokio::sync::Notify::new()),
        controller_blocked: Arc::new(AtomicBool::new(false)),
        controllers_started: Arc::new(AtomicUsize::new(0)),
        controller_started: Arc::new(tokio::sync::Notify::new()),
        controller_gate: Arc::new(tokio::sync::Notify::new()),
        persist_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        event_failures: Arc::new(AtomicUsize::new(0)),
        storage,
    };
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

fn inbound_envelope(text: &str) -> tinychannels_bus::ChannelInboundEnvelope {
    tinychannels_bus::ChannelInboundEnvelope {
        text: text.into(),
        parts: vec![tinychannels_bus::ContentPart::Text {
            text: text.into(),
            markdown_dialect: None,
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn shutdown_fences_new_transcription_and_joins_accepted_callback() {
    let (host, callback, _server) = connected_host().await;
    callback.transcription_blocked.store(true, Ordering::SeqCst);

    let transcriber = host.clone();
    let transcription = tokio::spawn(async move {
        transcriber
            .transcribe(tinychannels_bus::HostTranscriptionRequest {
                audio_base64: "UklGRg==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
    });
    callback.transcription_started.notified().await;

    let shutdown_host = host.clone();
    let shutdown = tokio::spawn(async move { shutdown_host.shutdown().await });
    while !host.owner.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "shutdown must retain ownership until an accepted transcription completes"
    );
    assert!(
        host.transcribe(tinychannels_bus::HostTranscriptionRequest {
            audio_base64: "UklGRg==".into(),
            mime_type: Some("audio/wav".into()),
            file_name: None,
            language: None,
        })
        .await
        .is_err(),
        "new transcription must be rejected after shutdown fences publication"
    );

    callback
        .transcription_blocked
        .store(false, Ordering::SeqCst);
    callback.transcription_gate.notify_one();
    assert_eq!(transcription.await.unwrap().unwrap().text, "spoken words");
    shutdown.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_joins_owned_transcription_after_waiter_cancellation() {
    let (host, callback, _server) = connected_host().await;
    callback.transcription_blocked.store(true, Ordering::SeqCst);

    let transcriber = host.clone();
    let waiter = tokio::spawn(async move {
        transcriber
            .transcribe(tinychannels_bus::HostTranscriptionRequest {
                audio_base64: "UklGRg==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
    });
    callback.transcription_started.notified().await;
    waiter.abort();
    let _ = waiter.await;

    let shutdown_host = host.clone();
    let shutdown = tokio::spawn(async move { shutdown_host.shutdown().await });
    while !host.owner.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "canceling the caller must not release the accepted callback owner"
    );
    callback
        .transcription_blocked
        .store(false, Ordering::SeqCst);
    callback.transcription_gate.notify_one();
    shutdown.await.unwrap().unwrap();
    assert!(
        callback
            .events
            .lock()
            .await
            .contains(&"transcription:complete".into()),
        "shutdown must wait for the host callback to finish"
    );
}

#[tokio::test]
async fn transcription_admission_refuses_when_its_owned_task_limit_is_reached() {
    let (host, callback, _server) = connected_host().await;
    callback.transcription_blocked.store(true, Ordering::SeqCst);

    let mut active = Vec::new();
    for _ in 0..super::MAX_ACTIVE_HOST_CALLBACKS {
        let host = host.clone();
        active.push(tokio::spawn(async move {
            host.transcribe(tinychannels_bus::HostTranscriptionRequest {
                audio_base64: "UklGRg==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
        }));
    }

    while callback.transcriptions_started.load(Ordering::SeqCst) < super::MAX_ACTIVE_HOST_CALLBACKS
    {
        let notified = callback.transcription_started.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if callback.transcriptions_started.load(Ordering::SeqCst)
            >= super::MAX_ACTIVE_HOST_CALLBACKS
        {
            break;
        }
        notified.await;
    }

    let refused = host
        .transcribe(tinychannels_bus::HostTranscriptionRequest {
            audio_base64: "UklGRg==".into(),
            mime_type: Some("audio/wav".into()),
            file_name: None,
            language: None,
        })
        .await;
    assert!(refused.is_err(), "the seventeenth held callback is refused");

    callback
        .transcription_blocked
        .store(false, Ordering::SeqCst);
    callback.transcription_gate.notify_waiters();
    for transcription in active {
        assert_eq!(transcription.await.unwrap().unwrap().text, "spoken words");
    }
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn callback_consumer_preserves_inbound_then_status_order() {
    let (host, callback, _server) = connected_host().await;
    host.deliver_inbound("telegram", inbound_envelope("hello"))
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
async fn module_host_exposes_typed_transcription_and_event_capabilities() {
    let (host, callback, _server) = connected_host().await;
    let provider_host = Arc::new(crate::provider::Hooks::default()).host(host.clone());
    let transcription = provider_host
        .transcriber()
        .unwrap()
        .transcribe(tinychannels::host::TranscriptionRequest {
            audio_base64: "UklGRg==".into(),
            mime_type: Some("audio/wav".into()),
            file_name: Some("voice.wav".into()),
            language: Some("en".into()),
        })
        .await
        .unwrap();
    assert_eq!(transcription.text, "spoken words");
    assert_eq!(transcription.language.as_deref(), Some("en"));

    provider_host
        .events()
        .unwrap()
        .publish(
            "channel",
            "reaction_sent",
            serde_json::json!({"target_message_id":"tg_42"}),
        )
        .await
        .unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["transcription:complete", "event:channel:reaction_sent"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn module_host_forwards_typed_backend_requests_and_bounds_payloads() {
    let (host, callback, _server) = connected_host().await;
    let request = tinychannels_bus::ChannelBackendRequest::SendReaction {
        channel: "discord".into(),
        reaction: serde_json::json!({"message_id":"fixture-message","emoji":"✅"}),
    };
    let result = host.dispatch_controller_backend(request).await.unwrap();
    assert!(matches!(
        result,
        tinychannels_bus::ChannelBackendResult::SendReaction(
            tinychannels_bus::controllers::ChannelReactionResult { success: true, .. }
        )
    ));
    assert_eq!(
        callback.events.lock().await.last().map(String::as_str),
        Some("controller:discord:reaction")
    );

    let oversized = tinychannels_bus::ChannelBackendRequest::SendReaction {
        channel: "discord".into(),
        reaction: serde_json::json!({"payload":"x".repeat(super::MAX_CONTROLLER_PAYLOAD_BYTES)}),
    };
    assert!(host.dispatch_controller_backend(oversized).await.is_err());
    assert_eq!(callback.events.lock().await.len(), 1);
    host.shutdown().await.unwrap();
    assert!(
        host.dispatch_controller_backend(tinychannels_bus::ChannelBackendRequest::SendReaction {
            channel: "discord".into(),
            reaction: serde_json::json!({"message_id":"fixture-message","emoji":"✅"}),
        })
        .await
        .is_err(),
        "the terminal host callback owner must refuse new controller work"
    );
}

#[tokio::test]
async fn shutdown_joins_owned_controller_callback_after_waiter_cancellation() {
    let (host, callback, _server) = connected_host().await;
    callback.controller_blocked.store(true, Ordering::SeqCst);

    let controller_host = host.clone();
    let waiter = tokio::spawn(async move {
        controller_host
            .dispatch_controller_backend(tinychannels_bus::ChannelBackendRequest::SendReaction {
                channel: "discord".into(),
                reaction: serde_json::json!({"message_id":"fixture-message","emoji":"✅"}),
            })
            .await
    });
    callback.controller_started.notified().await;
    waiter.abort();
    let _ = waiter.await;

    let shutdown_host = host.clone();
    let shutdown = tokio::spawn(async move { shutdown_host.shutdown().await });
    while !host.owner.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "shutdown must join an accepted host controller callback"
    );
    callback.controller_blocked.store(false, Ordering::SeqCst);
    callback.controller_gate.notify_one();
    shutdown.await.unwrap().unwrap();
    assert!(
        callback
            .events
            .lock()
            .await
            .contains(&"controller:discord:reaction".into()),
        "shutdown must retain the callback after its waiter is canceled"
    );
}

#[tokio::test]
async fn optional_event_callback_failure_does_not_poison_cleanup() {
    let (host, callback, _server) = connected_host().await;
    callback.event_failures.store(1, Ordering::SeqCst);
    let result = host
        .publish_event(tinychannels_bus::HostEventPublication {
            domain: "channel".into(),
            kind: "reaction_sent".into(),
            payload: serde_json::json!({"target_message_id":"tg_42"}),
        })
        .await;
    assert!(result.is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_host_event_sink_is_an_optional_capability() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    host.publish_event(tinychannels_bus::HostEventPublication {
        domain: "channel".into(),
        kind: "reaction_sent".into(),
        payload: serde_json::json!({"target_message_id":"tg_42"}),
    })
    .await
    .unwrap();
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
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "op-scope-test".into(),
        channel_id: "telegram".into(),
        conversation_id: "room-1".into(),
        channel_ref: None,
        conversation_ref: None,
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

    callback.storage.set_scope("scope-b");
    assert!(store.load_pending_all().await.unwrap().is_empty());
    assert!(store.load_result("op-scope-test").await.unwrap().is_none());
    callback.storage.set_scope("scope-a");
    assert_eq!(store.load_pending_all().await.unwrap().len(), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn updating_pending_delivery_does_not_read_the_full_queue() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (host, callback, _server) = connected_host().await;
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "op-update-only".into(),
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };
    let entry = QueuedDelivery::new("op-update-only", intent, 42);
    store.save_pending(entry.clone()).await.unwrap();
    let full_reads_after_insert = callback.storage.faults.pending_reads.load(Ordering::SeqCst);

    let mut update = entry;
    update.retry_count = 1;
    store.save_pending(update).await.unwrap();

    assert_eq!(
        callback.storage.faults.pending_reads.load(Ordering::SeqCst),
        full_reads_after_insert,
        "an update should load its existing record by id, not transfer the whole queue"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_store_rejects_invalid_and_expired_records() {
    use tinychannels::delivery::{
        DeliveryQueueStore as _, DeliveryResultStore as _, QueuedDelivery,
    };

    let (host, _callback, _server) = connected_host().await;
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
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
    host.deliver_inbound("telegram", inbound_envelope("hello"))
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
    host.deliver_inbound("telegram", inbound_envelope("hello"))
        .await;
    assert!(host.flush().await.is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_callbacks_do_not_follow_the_default_user() {
    let (legacy, default_user, server) = connected_host().await;
    let mut runtime_user = default_user.clone();
    runtime_user.events = Arc::new(Mutex::new(Vec::new()));
    let path = format!("{HOST_OBJECT_PATH}/runtime_a");
    server
        .serve_at(path.clone().try_into().unwrap(), runtime_user.clone())
        .await
        .unwrap();
    let scoped = HostChannels::new_at(legacy.connection(), path);
    scoped
        .deliver_inbound("telegram", inbound_envelope("private"))
        .await;
    scoped.report_status("telegram", "connected", None).await;
    scoped
        .persist_allowed_identity("telegram", "allowed")
        .await
        .unwrap();
    scoped.flush().await.unwrap();
    assert!(
        default_user.events.lock().await.is_empty(),
        "runtime callbacks must never reach the current default user"
    );
    assert_eq!(
        runtime_user.events.lock().await.as_slice(),
        [
            "inbound:telegram:private",
            "status:telegram:connected",
            "allowlist:telegram:allowed"
        ]
    );
    scoped.shutdown().await.unwrap();
    legacy.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_without_a_storage_object_has_no_delivery_store_and_refuses_required_send() {
    use tinychannels::delivery::{
        DeliveryQueueError, DeliveryQueueStore as _, QueuedDelivery, enqueue_delivery,
    };
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
    assert!(matches!(
        store.load_pending_all().await,
        Err(DeliveryQueueError::Unsupported)
    ));
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "missing-store".into(),
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };
    assert!(
        enqueue_delivery(&store, QueuedDelivery::new("missing-store", intent, 0))
            .await
            .is_err()
    );
    host.shutdown().await.unwrap();
}
