use super::*;
use std::sync::Arc;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tokio::sync::Mutex;

#[derive(Clone)]
struct Callback {
    workspace: PathBuf,
    calls: Arc<Mutex<Vec<String>>>,
}
#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn dispatch_controller_backend(
        &self,
        request: ChannelBackendRequest,
    ) -> Result<ChannelBackendResult> {
        let ChannelBackendRequest::RemoteControl(request) = request else {
            return Err(Error::failed("unexpected callback"));
        };
        let mut calls = self.calls.lock().await;
        let result = match request {
            RemoteHostRequest::CreateThread(thread) => {
                assert!(thread.id.starts_with("thread-"));
                assert_eq!(thread.labels, vec!["telegram", "remote"]);
                calls.push("create".into());
                RemoteHostResult::Completed
            }
            RemoteHostRequest::ClearHistory { sender_key } => {
                assert_eq!(sender_key, "telegram_sender");
                tinychannels::remote::session_store::with_store_read(&self.workspace, |store| {
                    assert!(store.binding("telegram", "chat").is_none());
                    Ok(())
                })
                .unwrap();
                calls.push("clear".into());
                RemoteHostResult::Completed
            }
            RemoteHostRequest::InvalidateThread { thread_id } => {
                tinychannels::remote::session_store::with_store_read(&self.workspace, |store| {
                    assert_eq!(
                        store.binding("telegram", "chat").unwrap().thread_id,
                        thread_id
                    );
                    Ok(())
                })
                .unwrap();
                calls.push("invalidate".into());
                RemoteHostResult::Completed
            }
            RemoteHostRequest::ListThreads => {
                calls.push("list".into());
                RemoteHostResult::Threads(vec![
                    RemoteThreadSummary {
                        id: "old".into(),
                        title: "Older".into(),
                        message_count: 1,
                        last_message_at: "2025".into(),
                    },
                    RemoteThreadSummary {
                        id: "new".into(),
                        title: "Newer".into(),
                        message_count: 2,
                        last_message_at: "2026".into(),
                    },
                ])
            }
        };
        Ok(ChannelBackendResult::RemoteControl(result))
    }
}
async fn fixture(workspace: &std::path::Path) -> (HostChannels, Callback) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    connection
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let callback = Callback {
        workspace: workspace.to_owned(),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    connection
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callback.clone(),
        )
        .await
        .unwrap();
    (HostChannels::new(connection), callback)
}
fn command(workspace: &std::path::Path, content: &str, channel: &str) -> RemoteOperation {
    RemoteOperation::Execute {
        workspace_dir: workspace.to_str().unwrap().into(),
        message: tinychannels_bus::ChannelMessage {
            id: "fixture".into(),
            sender: "sender".into(),
            reply_target: "chat".into(),
            content: content.into(),
            channel: channel.into(),
            timestamp: 1,
            thread_ts: None,
            sender_name: None,
        },
        route: RemoteRoute {
            provider: "fixture-provider".into(),
            model: "fixture-model".into(),
        },
        history_len: 7,
    }
}
#[tokio::test]
async fn new_command_awaits_host_history_clear_before_persisting_binding() {
    let workspace = tempfile::tempdir().unwrap();
    let (host, callback) = fixture(workspace.path()).await;
    let result = execute(&host, command(workspace.path(), "/new", "telegram"))
        .await
        .unwrap();
    assert!(result.reply.unwrap().contains("Started new session"));
    assert_eq!(
        *callback.calls.lock().await,
        vec!["create", "clear", "invalidate"]
    );
    let result = execute(&host, command(workspace.path(), "/status", "telegram"))
        .await
        .unwrap();
    let text = result.reply.unwrap();
    assert!(text.contains("thread-"));
    assert!(text.contains("fixture-provider"));
    assert!(text.contains("In-memory turns: 7"));
    host.shutdown().await.unwrap();
}
#[tokio::test]
async fn sessions_are_sorted_and_turn_state_is_owned_by_the_module() {
    let workspace = tempfile::tempdir().unwrap();
    let (host, callback) = fixture(workspace.path()).await;
    execute(
        &host,
        RemoteOperation::MarkTurn {
            workspace_dir: workspace.path().to_str().unwrap().into(),
            channel: "telegram".into(),
            reply_target: "chat".into(),
            busy: true,
        },
    )
    .await
    .unwrap();
    let status = execute(&host, command(workspace.path(), "/status", "telegram"))
        .await
        .unwrap()
        .reply
        .unwrap();
    assert!(status.contains("in progress"));
    let text = execute(&host, command(workspace.path(), "/sessions", "telegram"))
        .await
        .unwrap()
        .reply
        .unwrap();
    assert!(text.find("Newer").unwrap() < text.find("Older").unwrap());
    assert_eq!(*callback.calls.lock().await, vec!["list"]);
    host.shutdown().await.unwrap();
}
#[tokio::test]
async fn ordinary_messages_and_unsupported_channels_do_not_call_host_state() {
    let workspace = tempfile::tempdir().unwrap();
    let (host, callback) = fixture(workspace.path()).await;
    for (text, channel) in [("ordinary message", "telegram"), ("/new", "email")] {
        assert_eq!(
            execute(&host, command(workspace.path(), text, channel))
                .await
                .unwrap()
                .reply,
            None
        );
    }
    assert!(callback.calls.lock().await.is_empty());
    host.shutdown().await.unwrap();
}
