//! Opt-in bounded host turn callbacks owned by the channel module.
use super::*;

struct Completion {
    finished: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.finished.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
    }
}

pub(super) struct InboundDispatcher {
    sender: Option<mpsc::Sender<(String, serde_json::Value)>>,
    worker: Option<JoinHandle<()>>,
    accepted: usize,
    finished: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
impl InboundDispatcher {
    pub(super) fn new(
        connection: Connection,
        path: Arc<str>,
        failure: Arc<StdMutex<Option<String>>>,
        limit: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(OUTBOX_CAPACITY);
        let finished = Arc::new(AtomicUsize::new(0));
        let changed = Arc::new(Notify::new());
        let task_finished = finished.clone();
        let task_changed = changed.clone();
        let worker = tokio::spawn(async move {
            tinychannels::runtime::run_dispatch_loop(receiver, limit, move |message| {
                let connection = connection.clone();
                let path = path.clone();
                let failure = failure.clone();
                let completion = Completion {
                    finished: task_finished.clone(),
                    changed: task_changed.clone(),
                };
                async move {
                    let _completion = completion;
                    let result = async {
                        connection
                            .proxy(HOST_BUS_NAME, path.as_ref(), HOST_BUS_NAME)?
                            .call::<()>(host_methods::PROCESS_INBOUND, message)
                            .await
                    }
                    .await;
                    if let Err(error) = result {
                        record_failure(&failure, error.to_string());
                    }
                }
            })
            .await;
        });
        Self {
            sender: Some(sender),
            worker: Some(worker),
            accepted: 0,
            finished,
            changed,
        }
    }
    pub(super) async fn enqueue(&mut self, channel: String, message: serde_json::Value) {
        if self
            .sender
            .as_ref()
            .expect("dispatcher is open")
            .send((channel, message))
            .await
            .is_ok()
        {
            self.accepted += 1;
        }
    }
    // The sole producer waits here before admitting later notifications, so
    // completion counts cannot let a later turn satisfy an earlier frontier.
    pub(super) async fn flush(&self) {
        while self.finished.load(Ordering::SeqCst) < self.accepted {
            self.changed.notified().await;
        }
    }
    pub(super) async fn shutdown(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.as_mut() {
            let _ = worker.await;
        }
        self.worker.take();
    }
}
impl Drop for InboundDispatcher {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}
