use super::*;
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
        host: host.clone(),
        email: crate::email::EmailSender::new(),
        running: Arc::new(Mutex::new(running)),
        relays: Arc::new(Relays::new(host.clone())),
        terminal: AtomicBool::new(false),
        shutdown_task: Mutex::new(None),
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
        host: host.clone(),
        email: crate::email::EmailSender::new(),
        running: Arc::new(Mutex::new(running)),
        relays: Arc::new(Relays::new(host.clone())),
        terminal: AtomicBool::new(false),
        shutdown_task: Mutex::new(None),
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
