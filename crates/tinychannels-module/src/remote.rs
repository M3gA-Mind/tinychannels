//! Module-owned remote command parsing, rendering, and persisted chat bindings.
use crate::host::HostChannels;
use async_trait::async_trait;
use std::path::PathBuf;
use tinybus::{Error, Result};
use tinychannels::remote::session_store::RemoteSessionStore;
use tinychannels::remote::{RemoteCommandContext, RemoteControlHost};
use tinychannels_bus::module_remote::{
    NewRemoteThread, RemoteHostRequest, RemoteHostResult, RemoteOperation, RemoteOperationResult,
    RemoteRoute, RemoteSessionScope, RemoteThreadSummary,
};
use tinychannels_bus::{ChannelBackendRequest, ChannelBackendResult};

struct Host {
    callback: HostChannels,
    route: RemoteRoute,
    history_len: usize,
}
impl Host {
    async fn invoke(&self, request: RemoteHostRequest) -> anyhow::Result<RemoteHostResult> {
        match self
            .callback
            .dispatch_controller_backend(ChannelBackendRequest::RemoteControl(request))
            .await
        {
            Ok(ChannelBackendResult::RemoteControl(result)) => Ok(result),
            _ => Err(anyhow::anyhow!("remote host state is unavailable")),
        }
    }
    async fn completed(&self, request: RemoteHostRequest) -> anyhow::Result<()> {
        match self.invoke(request).await? {
            RemoteHostResult::Completed => Ok(()),
            _ => Err(anyhow::anyhow!("invalid remote host state reply")),
        }
    }
}
#[async_trait]
impl RemoteControlHost for Host {
    fn route(&self, _sender_key: &str) -> RemoteRoute {
        self.route.clone()
    }
    fn history_len(&self, _sender_key: &str) -> usize {
        self.history_len
    }
    async fn clear_history_async(&self, scope: &RemoteSessionScope) -> anyhow::Result<()> {
        self.completed(RemoteHostRequest::ClearHistory {
            session_scope: scope.clone(),
        })
        .await
    }
    async fn list_threads(&self) -> anyhow::Result<Vec<RemoteThreadSummary>> {
        match self.invoke(RemoteHostRequest::ListThreads).await? {
            RemoteHostResult::Threads(threads) => Ok(threads),
            _ => Err(anyhow::anyhow!("invalid remote host state reply")),
        }
    }
    async fn create_thread(&self, thread: NewRemoteThread) -> anyhow::Result<()> {
        self.completed(RemoteHostRequest::CreateThread(thread))
            .await
    }
    async fn invalidate_thread(&self, thread_id: &str) {
        let _ = self
            .completed(RemoteHostRequest::InvalidateThread {
                thread_id: thread_id.into(),
            })
            .await;
    }
}
fn workspace(path: String) -> Result<PathBuf> {
    if path.is_empty() || path.len() > 4096 || path.contains('\0') {
        return Err(Error::failed("invalid remote state root"));
    }
    Ok(path.into())
}
/// The remote-session store on the module's scoped host storage, with the
/// legacy per-workspace JSON files imported once. A host without storage gets
/// a store that reports itself unavailable, so `/help` and ordinary messages
/// still work and the commands that need bindings say why they cannot.
async fn sessions(callback: &HostChannels, workspace: &std::path::Path) -> RemoteSessionStore {
    let docs = match callback.documents().await {
        Ok(docs) => docs,
        Err(error) => {
            tracing::warn!(
                "[channel-remote] host storage unavailable: {:?}",
                error.kind()
            );
            return RemoteSessionStore::unavailable();
        }
    };
    let store = RemoteSessionStore::new(docs);
    if let Err(error) = store.adopt_legacy(workspace).await {
        tracing::warn!("[channel-remote] legacy session import failed: {error}");
    }
    store
}
pub(crate) async fn execute(
    callback: &HostChannels,
    request: RemoteOperation,
    capabilities: tinychannels_bus::ChannelCapabilities,
) -> Result<RemoteOperationResult> {
    if serde_json::to_vec(&request).map_or(true, |bytes| {
        bytes.len() > tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES
    }) {
        return Err(Error::failed("remote operation exceeds byte limit"));
    }
    let reply = match request {
        RemoteOperation::MarkTurn {
            workspace_dir,
            channel,
            reply_target,
            busy,
        } => {
            let workspace = workspace(workspace_dir)?;
            if capabilities.remote_control {
                let store = sessions(callback, &workspace).await;
                tinychannels::remote::mark_turn(&store, &channel, &reply_target, busy).await;
            }
            None
        }
        RemoteOperation::Execute {
            workspace_dir,
            message,
            route,
            history_len,
            session_scope,
        } => {
            let workspace = workspace(workspace_dir)?;
            let command = capabilities
                .remote_control
                .then(|| tinychannels::remote::parse_remote_command(&message.content))
                .flatten();
            match command {
                None => None,
                Some(command) => {
                    let context = RemoteCommandContext {
                        channel: message.channel.clone(),
                        reply_target: message.reply_target.clone(),
                        sender_key: tinychannels::context::conversation_history_key(&message),
                        session_scope: session_scope.clone(),
                        sessions: sessions(callback, &workspace).await,
                    };
                    Some(
                        tinychannels::remote::execute_remote_command(
                            &Host {
                                callback: callback.clone(),
                                route,
                                history_len,
                            },
                            &context,
                            command,
                        )
                        .await,
                    )
                }
            }
        }
    };
    let result = RemoteOperationResult { reply };
    if serde_json::to_vec(&result).map_or(true, |bytes| {
        bytes.len() > tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES
    }) {
        return Err(Error::failed("remote result exceeds byte limit"));
    }
    Ok(result)
}

#[cfg(test)]
#[path = "remote_tests.rs"]
mod tests;
