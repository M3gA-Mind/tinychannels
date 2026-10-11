use super::*;

struct SlowChannel {
    started: StdMutex<Option<oneshot::Sender<()>>>,
    release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl Channel for SlowChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        if let Some(release) = self.release.lock().await.take() {
            let _ = release.await;
        }
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
async fn canceled_send_waiter_does_not_cancel_owned_send_or_shutdown_join() {
    let (started, started_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let channel = Arc::new(SlowChannel {
        started: StdMutex::new(Some(started)),
        release: tokio::sync::Mutex::new(Some(release_rx)),
    });
    let connection = tinybus::Connection::connect({
        let bus = tinybus::transport::memory::MemoryBus::new();
        tinybus::broker::Broker::new().spawn(bus.clone());
        bus.connect().await.unwrap()
    })
    .await
    .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start(channel, Arc::new(Hooks::default()), host);
    let send_waiter = tokio::spawn({
        let running = running.clone();
        async move { running.send(SendMessage::new("body", "recipient")).await }
    });
    started_rx.await.unwrap();
    send_waiter.abort();
    let _ = send_waiter.await;

    let stop = tokio::spawn({
        let running = running.clone();
        async move { running.stop().await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), async {
            while !stop.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err()
    );
    release.send(()).unwrap();
    stop.await.unwrap().unwrap();
}

#[derive(Default)]
struct InteractionChannel(StdMutex<Vec<String>>);
#[async_trait::async_trait]
impl Channel for InteractionChannel {
    fn name(&self) -> &str {
        "interaction-fixture"
    }
    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }
    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("send:{}", message.content));
        Ok(())
    }
    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("typing:{recipient}"));
        Ok(())
    }
    async fn send_draft(&self, message: &SendMessage) -> anyhow::Result<Option<String>> {
        self.0
            .lock()
            .unwrap()
            .push(format!("draft:{}", message.content));
        Ok(Some("module-draft".into()))
    }
    async fn stop_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("stop:{recipient}"));
        Ok(())
    }
}

#[tokio::test]
async fn provider_actor_orders_interactions_with_sends_and_rejects_after_stop() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel.clone(),
        Arc::new(Hooks::default()),
        HostChannels::new(connection),
    );
    running
        .interact(Op::StartTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    running
        .send(SendMessage::new("ordinary", "room"))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let draft = running
        .interact(Op::SendDraft {
            message: SendMessage::new("partial", "room"),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(matches!(draft, ChannelInteractionResult::Draft(Some(id)) if id == "module-draft"));
    running
        .interact(Op::StopTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    running.stop().await.unwrap();
    assert_eq!(
        *channel.0.lock().unwrap(),
        ["typing:room", "send:ordinary", "draft:partial", "stop:room"]
    );
    assert!(matches!(
        running.interact(Op::Describe).await,
        Err(RunningSendError::NotDispatched(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_is_bounded_and_joined_by_provider_owner() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel.clone(),
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );

    running
        .interact(Op::StartTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap();
    running
        .interact(Op::StartTypingRefresh {
            recipient: "room".into(),
            interval_secs: 4,
        })
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(4)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        *channel.0.lock().unwrap(),
        ["typing:room", "typing:room"],
        "the module refreshes the indicator after its interval"
    );

    assert!(
        running
            .interact(Op::StartTypingRefresh {
                recipient: "other".into(),
                interval_secs: 0,
            })
            .await
            .is_err()
    );
    running
        .interact(Op::StopTypingRefresh {
            recipient: "room".into(),
        })
        .await
        .unwrap();
    running
        .interact(Op::StopTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap();
    running.stop().await.unwrap();
    assert!(matches!(
        running
            .interact(Op::StopTypingRefresh {
                recipient: "room".into(),
            })
            .await,
        Err(RunningSendError::NotDispatched(_))
    ));
    assert_eq!(
        *channel.0.lock().unwrap(),
        ["typing:room", "typing:room", "stop:room"]
    );
    connection.close().await;
}

struct PendingTypingChannel {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl Channel for PendingTypingChannel {
    fn name(&self) -> &str {
        "pending-typing"
    }

    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }

    async fn start_typing(&self, _: &str) -> anyhow::Result<()> {
        struct DropMark(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for DropMark {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let _drop_mark = DropMark(Arc::clone(&self.dropped));
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_stop_and_shutdown_cancel_inflight_provider_calls() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let channel = Arc::new(PendingTypingChannel {
        entered: Arc::clone(&entered),
        dropped: Arc::clone(&dropped),
    });
    let running = Running::start(
        channel,
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );

    running
        .interact(Op::StartTypingRefresh {
            recipient: "stop-case".into(),
            interval_secs: 1,
        })
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    entered.notified().await;
    running
        .interact(Op::StopTypingRefresh {
            recipient: "stop-case".into(),
        })
        .await
        .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    running
        .interact(Op::StartTypingRefresh {
            recipient: "shutdown-case".into(),
            interval_secs: 1,
        })
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    entered.notified().await;
    running.stop().await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    connection.close().await;
}

struct ReconnectingChannel {
    attempts: StdMutex<Vec<oneshot::Sender<()>>>,
}
#[async_trait::async_trait]
impl Channel for ReconnectingChannel {
    fn name(&self) -> &str {
        "reconnect-fixture"
    }
    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        let last = {
            let mut attempts = self.attempts.lock().unwrap();
            let receipt = attempts.remove(0);
            let _ = receipt.send(());
            attempts.is_empty()
        };
        if last {
            std::future::pending::<()>().await;
        }
        anyhow::bail!("fixture disconnected")
    }
}

#[tokio::test(start_paused = true)]
async fn disconnected_module_provider_reconnects_and_stop_joins_the_new_listener() {
    let (first, first_rx) = oneshot::channel();
    let (second, mut second_rx) = oneshot::channel();
    let channel = Arc::new(ReconnectingChannel {
        attempts: StdMutex::new(vec![first, second]),
    });
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start_supervised(
        channel,
        Arc::new(Hooks::default()),
        host.clone(),
        ChannelSupervisionPolicy::default(),
    );
    first_rx.await.unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        second_rx.try_recv().is_ok(),
        "the module must own provider reconnects"
    );
    assert!(!running.is_finished());
    running.stop().await.unwrap();
    assert!(running.is_finished());
    host.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn stopping_during_selected_backoff_prevents_another_provider_attempt() {
    let (first, first_rx) = oneshot::channel();
    let (second, mut second_rx) = oneshot::channel();
    let channel = Arc::new(ReconnectingChannel {
        attempts: StdMutex::new(vec![first, second]),
    });
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start_supervised(
        channel.clone(),
        Arc::new(Hooks::default()),
        host.clone(),
        ChannelSupervisionPolicy {
            initial_backoff_secs: 30,
            max_backoff_secs: 60,
        },
    );
    first_rx.await.unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        second_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    running.stop().await.unwrap();
    tokio::time::advance(Duration::from_secs(120)).await;
    assert_eq!(channel.attempts.lock().unwrap().len(), 1);
    assert!(running.is_finished());
    host.shutdown().await.unwrap();
}
