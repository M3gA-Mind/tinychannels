use super::*;

#[test]
fn acknowledgement_selection_is_stable_and_contextual() {
    assert!(matches!(
        select_acknowledgment_reaction("thanks"),
        "❤️" | "🙏"
    ));
    assert_eq!(
        select_acknowledgment_reaction("thanks"),
        select_acknowledgment_reaction("thanks")
    );
    assert!(jitter_millis(1) < MAX_JITTER_MS);
    assert_eq!(jitter_millis(0), 0);
    assert_eq!(compute_max_in_flight_messages(100), 64);
}

#[test]
fn acknowledgement_categories_keep_their_context_and_priority() {
    for (content, choices) in [
        ("THANKS for the code", ["❤️", "🙏"]),
        ("awesome!!", ["🔥", "🎉"]),
        ("btc price?", ["💯", "⚡"]),
        ("rust function?", ["👨‍💻", "🤓"]),
        ("hello there", ["🤗", "😁"]),
        ("yo", ["🤗", "😁"]),
        ("yo friend", ["🤗", "😁"]),
        ("what happened", ["🤔", "✍️"]),
        ("really?", ["🤔", "✍️"]),
        ("ordinary message", ["👀", "✍️"]),
        ("", ["👀", "✍️"]),
    ] {
        assert!(
            choices.contains(&select_acknowledgment_reaction(content)),
            "{content}"
        );
    }
    assert_eq!(compute_max_in_flight_messages(0), 8);
    assert_eq!(compute_max_in_flight_messages(3), 12);
    assert_eq!(compute_max_in_flight_messages(usize::MAX), 64);
    assert!(jitter_millis(u64::MAX) < MAX_JITTER_MS);
}

#[test]
fn jitter_millis_is_bounded_for_short_and_capped_backoffs() {
    for _ in 0..256 {
        assert!(jitter_millis(1) < 1_000);
        assert!(jitter_millis(60) < MAX_JITTER_MS);
    }
    assert_eq!(jitter_millis(0), 0);
}

#[derive(Default)]
struct RecordingObserver(std::sync::Mutex<Vec<String>>);

impl ListenerObserver for RecordingObserver {
    fn connected(&self, channel: &str) {
        self.0.lock().unwrap().push(format!("connected:{channel}"));
    }
    fn disconnected(&self, channel: &str, reason: &str, failed: bool) {
        self.0
            .lock()
            .unwrap()
            .push(format!("disconnected:{channel}:{reason}:{failed}"));
    }
    fn restarted(&self, channel: &str) {
        self.0.lock().unwrap().push(format!("restarted:{channel}"));
    }
}

struct ExitingListener {
    calls: std::sync::atomic::AtomicUsize,
    receiver: std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<ChannelMessage>>>,
}

#[async_trait::async_trait]
impl Channel for ExitingListener {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, _: &crate::traits::SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        match self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => anyhow::bail!("connection lost"),
            1 => Ok(()),
            _ => {
                self.receiver.lock().unwrap().take();
                Ok(())
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn listener_retries_failed_and_clean_exits_then_stops_when_receiver_closes() {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let channel = Arc::new(ExitingListener {
        calls: Default::default(),
        receiver: std::sync::Mutex::new(Some(rx)),
    });
    let observer = Arc::new(RecordingObserver::default());
    let start = tokio::time::Instant::now();
    // Zero bounds are normalized to a one-second retry interval.
    spawn_supervised_listener(channel.clone(), tx, 0, 0, observer.clone())
        .await
        .unwrap();
    assert_eq!(channel.calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert!(start.elapsed() >= Duration::from_secs(2));
    assert!(start.elapsed() < Duration::from_secs(4));
    assert_eq!(
        *observer.0.lock().unwrap(),
        [
            "connected:fixture",
            "disconnected:fixture:connection lost:true",
            "restarted:fixture",
            "connected:fixture",
            "disconnected:fixture:exited unexpectedly:false",
            "restarted:fixture",
            "connected:fixture",
        ]
    );
}

struct CleanExitListener {
    entries: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
    stop_after: usize,
    receiver: std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<ChannelMessage>>>,
}

#[async_trait::async_trait]
impl Channel for CleanExitListener {
    fn name(&self) -> &str {
        "clean-exit"
    }

    async fn send(&self, _: &crate::traits::SendMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn listen(&self, _: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        let count = {
            let mut entries = self.entries.lock().expect("entries lock");
            entries.push(tokio::time::Instant::now());
            entries.len()
        };
        if count >= self.stop_after {
            self.receiver.lock().expect("receiver lock").take();
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn clean_listener_exits_escalate_backoff_under_paused_clock() {
    let entries = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (tx, rx) = tokio::sync::mpsc::channel::<ChannelMessage>(1);
    let channel: Arc<dyn Channel> = Arc::new(CleanExitListener {
        entries: Arc::clone(&entries),
        stop_after: 4,
        receiver: std::sync::Mutex::new(Some(rx)),
    });

    spawn_supervised_listener(channel, tx, 1, 8, Arc::new(NoopListenerObserver))
        .await
        .expect("supervised listener task");

    let entries = entries.lock().expect("entries lock");
    assert_eq!(entries.len(), 4);
    let whole_second_gaps: Vec<u64> = entries
        .windows(2)
        .map(|pair| pair[1].duration_since(pair[0]).as_secs())
        .collect();
    assert_eq!(
        whole_second_gaps,
        vec![1, 2, 4],
        "clean listener exits must use the escalating retry backoff"
    );
}

struct FailingTypingChannel {
    started: tokio::sync::Notify,
    stops: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Channel for FailingTypingChannel {
    fn name(&self) -> &str {
        "typing-fixture"
    }
    async fn send(&self, _: &crate::traits::SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        assert_eq!(recipient, "recipient");
        self.started.notify_one();
        anyhow::bail!("typing unavailable")
    }
    async fn stop_typing(&self, recipient: &str) -> anyhow::Result<()> {
        assert_eq!(recipient, "recipient");
        self.stops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        anyhow::bail!("stop unavailable")
    }
}

#[tokio::test(start_paused = true)]
async fn typing_failure_does_not_prevent_cancellation_cleanup() {
    let channel = Arc::new(FailingTypingChannel {
        started: Default::default(),
        stops: Default::default(),
    });
    let token = CancellationToken::new();
    let task = spawn_scoped_typing_task(
        channel.clone(),
        "recipient".into(),
        token.clone(),
        Duration::from_secs(1),
    );
    channel.started.notified().await;
    channel.started.notified().await;
    token.cancel();
    task.await.unwrap();
    assert_eq!(channel.stops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn worker_join_logging_accepts_success_and_panic() {
    log_worker_join_result(Ok(()));
    let failure = tokio::spawn(async { panic!("worker failed") }).await;
    assert!(failure.as_ref().unwrap_err().is_panic());
    log_worker_join_result(failure);
    let observer = NoopListenerObserver;
    observer.connected("fixture");
    observer.disconnected("fixture", "closed", false);
    observer.restarted("fixture");
}
