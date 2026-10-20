use super::*;
use std::sync::Arc;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinybus::{Connection, Result};
use tinychannels_bus::controllers::ChannelThreadResult;
use tokio::sync::Mutex;

trait FixtureFact<T> {
    fn ok_or_default(self) -> std::result::Result<T, tinychannels_bus::channel::ChannelSendError>;
}

impl<T> FixtureFact<T> for Option<T> {
    fn ok_or_default(self) -> std::result::Result<T, tinychannels_bus::channel::ChannelSendError> {
        self.ok_or_else(tinychannels_bus::channel::ChannelSendError::default)
    }
}

#[derive(Clone, Default)]
struct BackendFixture(
    Arc<Mutex<Vec<ChannelBackendRequest>>>,
    Arc<Mutex<Vec<ChannelBackendResult>>>,
);

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl BackendFixture {
    async fn dispatch_controller_backend(
        &self,
        request: ChannelBackendRequest,
    ) -> Result<ChannelBackendResult> {
        self.0.lock().await.push(request.clone());
        let result = match request {
            ChannelBackendRequest::RemoteControl(_) => {
                Err(tinybus::Error::failed("unexpected remote callback"))
            }
            ChannelBackendRequest::ProgressiveTransport(request) => {
                use tinychannels_bus::module_progressive::{
                    ProgressiveTransportRequest as Request,
                    ProgressiveTransportResult as Response,
                };
                match *request {
                    Request::SendTyped {
                        channel_ref,
                        conversation,
                        ..
                    } => {
                        let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                        outcome.receipt.channel = channel_ref;
                        outcome.receipt.conversation = conversation;
                        outcome.receipt.primary_platform_message_id =
                            Some("managed-message-1".into());
                        outcome.receipt.platform_message_ids = vec!["managed-message-1".into()];
                        outcome.receipt.parts.push(
                            tinychannels_bus::channel::MessageReceiptPart {
                                platform_message_id: "managed-message-1".into(),
                                ..Default::default()
                            },
                        );
                        Ok(ChannelBackendResult::ProgressiveTransport(Box::new(
                            Response::Outcome(Box::new(outcome)),
                        )))
                    }
                    Request::Edit {
                        channel_ref,
                        conversation,
                        ..
                    } => Ok(ChannelBackendResult::ProgressiveTransport(Box::new(
                        Response::Success(serde_json::json!({
                            "message_id": "managed-edited-1",
                            "channel": channel_ref,
                            "conversation": conversation,
                        })),
                    ))),
                    Request::OutputEvent { .. }
                    | Request::Delete { .. }
                    | Request::Typing { .. }
                    | Request::Send { .. } => Ok(ChannelBackendResult::ProgressiveTransport(
                        Box::new(Response::Acknowledged),
                    )),
                }
            }
            ChannelBackendRequest::Disconnect {
                channel, auth_mode, ..
            } => Ok(ChannelBackendResult::Disconnected(
                tinychannels_bus::controllers::ChannelDisconnectResult {
                    channel,
                    auth_mode,
                    disconnected: true,
                    ..Default::default()
                },
            )),
            ChannelBackendRequest::Status { channel } => Ok(ChannelBackendResult::Status(
                channel
                    .into_iter()
                    .map(
                        |channel_id| tinychannels_bus::controllers::ChannelStatusEntry {
                            channel_id,
                            auth_mode: ChannelAuthMode::BotToken,
                            connected: true,
                            has_credentials: true,
                            error: None,
                        },
                    )
                    .collect(),
            )),
            ChannelBackendRequest::SendReaction { .. } => Ok(ChannelBackendResult::SendReaction(
                tinychannels_bus::controllers::ChannelReactionResult {
                    success: true,
                    ..Default::default()
                },
            )),
            ChannelBackendRequest::CreateThread { title, .. } => {
                Ok(ChannelBackendResult::Thread(ChannelThreadResult {
                    thread_id: "thread-fixture".into(),
                    title: Some(title),
                    raw: None,
                }))
            }
            ChannelBackendRequest::UpdateThread { thread_id, .. } => {
                Ok(ChannelBackendResult::Thread(ChannelThreadResult {
                    thread_id,
                    title: Some("updated fixture".into()),
                    raw: None,
                }))
            }
            ChannelBackendRequest::ListThreads { .. } => Ok(ChannelBackendResult::Threads(
                tinychannels_bus::controllers::ChannelThreadListResult {
                    threads: vec![tinychannels_bus::controllers::ChannelThreadEntry {
                        thread_id: "thread-fixture".into(),
                        title: Some("fixture".into()),
                        active: Some(true),
                        raw: None,
                    }],
                    raw: None,
                },
            )),
        };
        if let Ok(result) = &result {
            self.1.lock().await.push(result.clone());
        }
        result
    }
}

async fn connected_host() -> (HostChannels, BackendFixture, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let fixture = BackendFixture::default();
    server
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            fixture.clone(),
        )
        .await
        .unwrap();
    server
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (HostChannels::new(client), fixture, server)
}

struct ManagedConformanceChannel(tinychannels_bus::ChannelCapabilities);

#[async_trait::async_trait]
#[allow(deprecated, reason = "managed fixture exposes module capability facts")]
impl tinychannels::Channel for ManagedConformanceChannel {
    fn name(&self) -> &str {
        "mattermost"
    }

    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.0.clone()
    }

    async fn send(&self, _message: &tinychannels::SendMessage) -> anyhow::Result<()> {
        anyhow::bail!("managed output must use the host callback")
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

struct ManagedConformanceFixture {
    host: HostChannels,
    backend: BackendFixture,
    replies: crate::progressive::Replies,
    running: Arc<Running>,
    observed: std::sync::Mutex<Vec<tinychannels_conformance::WireObservation>>,
}

impl ManagedConformanceFixture {
    async fn new() -> (Self, Connection) {
        let (host, backend, server) = connected_host().await;
        let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
        capabilities.reactions = true;
        capabilities.progressive_edits = true;
        capabilities
            .actions
            .supported
            .extend(["edit", "thread-create", "thread-update", "thread-list"].map(str::to_owned));
        let running = Running::start_outbound(
            Arc::new(ManagedConformanceChannel(capabilities)),
            Arc::new(crate::provider::Hooks::default()),
            host.clone(),
        );
        let mut configured = std::collections::HashMap::new();
        configured.insert("mattermost:managed-fixture".to_owned(), running.clone());
        let replies = crate::progressive::Replies::new(
            host.clone(),
            Arc::new(Mutex::new(configured)),
        );
        (
            Self {
                host,
                backend,
                replies,
                running,
                observed: std::sync::Mutex::new(Vec::new()),
            },
            server,
        )
    }

    fn record(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
        operation: &str,
        channel: tinychannels_bus::ChannelRef,
        conversation: tinychannels_bus::ConversationRef,
        platform_message_ids: Vec<String>,
        request: serde_json::Value,
    ) {
        self.observed.lock().unwrap().push(tinychannels_conformance::WireObservation {
            feature,
            operation: operation.to_owned(),
            channel,
            conversation,
            reply_to_id: None,
            thread_id: None,
            payload_kinds: Vec::new(),
            platform_message_ids,
            request,
        });
    }

    async fn shutdown(self, server: Connection) {
        self.replies.shutdown().await.unwrap();
        self.running.stop().await.unwrap();
        self.host.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}

#[async_trait::async_trait]
impl tinychannels_conformance::ConformanceFixture for ManagedConformanceFixture {
    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.running.declared_capabilities()
    }

    fn expected_scope(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
    ) -> tinychannels_conformance::ExpectedScope {
        use tinychannels_conformance::ConformanceFeature as Feature;
        let channel = tinychannels_bus::ChannelRef {
            id: "mattermost".into(),
            account_id: (feature == Feature::Edit)
                .then(|| "mattermost:managed-fixture".into()),
        };
        let conversation = match feature {
            Feature::ManagedReaction => tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Channel,
                id: "managed-room-1".into(),
                ..Default::default()
            },
            Feature::Edit => tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Channel,
                id: "managed-room-1".into(),
                scope_id: Some("mattermost:managed-fixture".into()),
                ..Default::default()
            },
            _ => tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Channel,
                id: "mattermost".into(),
                ..Default::default()
            },
        };
        tinychannels_conformance::ExpectedScope {
            channel,
            conversation,
            reply_to_id: None,
            thread_id: None,
        }
    }

    fn receipt_required(&self, feature: tinychannels_conformance::ConformanceFeature) -> bool {
        feature == tinychannels_conformance::ConformanceFeature::Edit
    }

    fn clear_observations(&self) {
        self.observed.lock().unwrap().clear();
    }

    fn observations(&self) -> Vec<tinychannels_conformance::WireObservation> {
        self.observed.lock().unwrap().clone()
    }

    async fn exercise(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
    ) -> std::result::Result<
        Option<tinychannels_bus::channel::ChannelSendOutcome>,
        tinychannels_bus::channel::ChannelSendError,
    > {
        use tinychannels_bus::module_controller::{ChannelControllerRequest as Request, ChannelControllerResult as Response};
        use tinychannels_bus::module_progressive::{
            ChannelOutputEvent, ProgressiveCommand, ProgressiveResult,
            ProgressiveTransportRequest, ProgressiveTransportResult,
        };
        use tinychannels_conformance::ConformanceFeature as Feature;

        let config = ChannelsConfig::default();
        match feature {
            Feature::ManagedReaction => {
                let result = dispatch(&self.host, Request::SendReaction {
                    config,
                    channel: "mattermost".into(),
                    reaction: serde_json::json!({
                        "channel_id": "managed-room-1",
                        "message_id": "managed-source-1",
                        "emoji": "eyes"
                    }),
                })
                .await
                .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let request = self.backend.0.lock().await.last().cloned().ok_or_default()?;
                let response = self.backend.1.lock().await.last().cloned().ok_or_default()?;
                let (channel, reaction) = match request {
                    ChannelBackendRequest::SendReaction { channel, reaction } => (channel, reaction),
                    _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                };
                if !matches!(result, Response::SendReaction(ref result) if result.success)
                    || !matches!(response, ChannelBackendResult::SendReaction(ref result) if result.success)
                {
                    return Err(tinychannels_bus::channel::ChannelSendError::default());
                }
                let conversation_id = reaction
                    .get("channel_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_default()?;
                let message_id = reaction
                    .get("message_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_default()?;
                let channel_ref = tinychannels_bus::ChannelRef { id: channel, account_id: None };
                let conversation = tinychannels_bus::ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Channel,
                    id: conversation_id.to_owned(),
                    ..Default::default()
                };
                self.record(
                    feature,
                    "reaction_send",
                    channel_ref,
                    conversation,
                    vec![message_id.to_owned()],
                    serde_json::json!({"request": reaction, "response": response}),
                );
                Ok(None)
            }
            Feature::ManagedThreadCreate => {
                let result = dispatch(&self.host, Request::CreateThread {
                    config,
                    channel: "mattermost".into(),
                    title: "managed fixture thread".into(),
                })
                .await
                .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let request = self.backend.0.lock().await.last().cloned().ok_or_default()?;
                let response = self.backend.1.lock().await.last().cloned().ok_or_default()?;
                let (channel, title) = match request {
                    ChannelBackendRequest::CreateThread { channel, title } => (channel, title),
                    _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                };
                let thread_id = match (&result, &response) {
                    (Response::Thread(result), ChannelBackendResult::Thread(wire))
                        if result.thread_id == wire.thread_id && result.title == wire.title =>
                    {
                        result.thread_id.clone()
                    }
                    _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                };
                self.record(
                    feature,
                    "thread_create",
                    tinychannels_bus::ChannelRef { id: channel.clone(), account_id: None },
                    tinychannels_bus::ConversationRef {
                        kind: tinychannels_bus::ConversationKind::Channel,
                        id: channel,
                        ..Default::default()
                    },
                    vec![thread_id],
                    serde_json::json!({"title": title, "response": response}),
                );
                Ok(None)
            }
            Feature::ManagedThreadUpdate => {
                let result = dispatch(&self.host, Request::UpdateThread {
                    config,
                    channel: "mattermost".into(),
                    thread_id: "managed-thread-1".into(),
                    action: "close".into(),
                })
                .await
                .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let request = self.backend.0.lock().await.last().cloned().ok_or_default()?;
                let response = self.backend.1.lock().await.last().cloned().ok_or_default()?;
                let (channel, thread_id, action) = match request {
                    ChannelBackendRequest::UpdateThread { channel, thread_id, action } => (channel, thread_id, action),
                    _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                };
                if !matches!((&result, &response),
                    (Response::Thread(result), ChannelBackendResult::Thread(wire))
                        if result.thread_id == thread_id && wire.thread_id == thread_id)
                {
                    return Err(tinychannels_bus::channel::ChannelSendError::default());
                }
                self.record(
                    feature,
                    "thread_update",
                    tinychannels_bus::ChannelRef { id: channel.clone(), account_id: None },
                    tinychannels_bus::ConversationRef {
                        kind: tinychannels_bus::ConversationKind::Channel,
                        id: channel,
                        ..Default::default()
                    },
                    vec![thread_id],
                    serde_json::json!({"action": action, "response": response}),
                );
                Ok(None)
            }
            Feature::ManagedThreadList => {
                let result = dispatch(&self.host, Request::ListThreads {
                    config,
                    channel: "mattermost".into(),
                    active: Some(true),
                })
                .await
                .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let request = self.backend.0.lock().await.last().cloned().ok_or_default()?;
                let response = self.backend.1.lock().await.last().cloned().ok_or_default()?;
                let (channel, active) = match request {
                    ChannelBackendRequest::ListThreads { channel, active } => (channel, active),
                    _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                };
                let ids = match (&result, &response) {
                    (Response::Threads(result), ChannelBackendResult::Threads(wire))
                        if result
                            .threads
                            .iter()
                            .map(|thread| &thread.thread_id)
                            .eq(wire.threads.iter().map(|thread| &thread.thread_id)) => result
                            .threads
                            .iter()
                            .map(|thread| thread.thread_id.clone())
                            .collect::<Vec<_>>(),
                    _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                };
                self.record(
                    feature,
                    "thread_list",
                    tinychannels_bus::ChannelRef { id: channel.clone(), account_id: None },
                    tinychannels_bus::ConversationRef {
                        kind: tinychannels_bus::ConversationKind::Channel,
                        id: channel,
                        ..Default::default()
                    },
                    ids,
                    serde_json::json!({"active": active, "response": response}),
                );
                Ok(None)
            }
            Feature::Edit => {
                let opened = self
                    .replies
                    .execute(ProgressiveCommand::OpenOutput {
                        channel: tinychannels_bus::ChannelRef {
                            id: "mattermost".into(),
                            account_id: Some("mattermost:managed-fixture".into()),
                        },
                        conversation: tinychannels_bus::ConversationRef {
                            kind: tinychannels_bus::ConversationKind::Channel,
                            id: "managed-room-1".into(),
                            scope_id: Some("mattermost:managed-fixture".into()),
                            ..Default::default()
                        },
                        turn_id: "managed-conformance-turn".into(),
                        lifetime_ms: 30_000,
                    })
                    .await
                    .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let ProgressiveResult::Opened { handle } = opened else {
                    return Err(tinychannels_bus::channel::ChannelSendError::default());
                };
                for (text, iteration) in [("managed draft", 1), (" managed edit", 2)] {
                    self.replies
                        .execute(ProgressiveCommand::FeedOutput {
                            handle: handle.clone(),
                            events: vec![ChannelOutputEvent::TextDelta {
                                text: text.into(),
                                iteration: Some(iteration),
                            }],
                        })
                        .await
                        .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                    tokio::time::advance(std::time::Duration::from_secs(1)).await;
                    for _ in 0..5 {
                        tokio::task::yield_now().await;
                    }
                }
                let requests = self.backend.0.lock().await.clone();
                let responses = self.backend.1.lock().await.clone();
                let index = requests
                    .iter()
                    .rposition(|request| matches!(request,
                        ChannelBackendRequest::ProgressiveTransport(request)
                            if matches!(request.as_ref(), ProgressiveTransportRequest::Edit { .. })))
                    .ok_or_default()?;
                let ChannelBackendRequest::ProgressiveTransport(request) = &requests[index] else {
                    unreachable!()
                };
                let ProgressiveTransportRequest::Edit {
                    channel_ref,
                    conversation,
                    message_id,
                    text,
                    ..
                } = request.as_ref()
                else {
                    unreachable!()
                };
                let response = responses.get(index).cloned().ok_or_default()?;
                let response_value = match &response {
                    ChannelBackendResult::ProgressiveTransport(response) => match response.as_ref() {
                        ProgressiveTransportResult::Success(value) => value.clone(),
                        _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                    },
                    _ => return Err(tinychannels_bus::channel::ChannelSendError::default()),
                };
                let returned_id = response_value
                    .get("message_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_default()?
                    .to_owned();
                let channel = channel_ref.clone().ok_or_default()?;
                let conversation = conversation.clone().ok_or_default()?;
                self.record(
                    feature,
                    "edit",
                    channel.clone(),
                    conversation.clone(),
                    vec![returned_id.clone()],
                    serde_json::json!({"request": request, "response": response}),
                );
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                outcome.receipt.channel = Some(channel);
                outcome.receipt.conversation = Some(conversation);
                outcome.receipt.primary_platform_message_id = Some(returned_id.clone());
                outcome.receipt.platform_message_ids = vec![returned_id.clone()];
                outcome.receipt.parts.push(tinychannels_bus::channel::MessageReceiptPart {
                    platform_message_id: returned_id,
                    ..Default::default()
                });
                outcome.receipt.text = Some(text.clone());
                outcome.receipt.idempotency_key = Some(message_id.clone());
                Ok(Some(outcome))
            }
            _ => Err(tinychannels_bus::channel::ChannelSendError::default()),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn managed_controller_and_progressive_paths_pass_shared_conformance_features() {
    let (fixture, server) = ManagedConformanceFixture::new().await;
    let results = tinychannels_conformance::check_managed_features(&fixture).await;
    for (feature, result) in &results {
        assert!(result.is_ok(), "{feature:?}: {}", result.as_ref().unwrap_err());
    }
    assert_eq!(
        results.iter().map(|(feature, _)| *feature).collect::<Vec<_>>(),
        tinychannels_conformance::ConformanceFeature::MANAGED,
    );
    let requests = fixture.backend.0.lock().await.clone();
    assert!(requests.iter().any(|request| matches!(request,
        ChannelBackendRequest::SendReaction { channel, reaction }
            if channel == "mattermost" && reaction["message_id"] == "managed-source-1"
    )));
    assert!(requests.iter().any(|request| matches!(request,
        ChannelBackendRequest::CreateThread { channel, title }
            if channel == "mattermost" && title == "managed fixture thread"
    )));
    assert!(requests.iter().any(|request| matches!(request,
        ChannelBackendRequest::UpdateThread { channel, thread_id, action }
            if channel == "mattermost" && thread_id == "managed-thread-1" && action == "close"
    )));
    assert!(requests.iter().any(|request| matches!(request,
        ChannelBackendRequest::ListThreads { channel, active: Some(true) }
            if channel == "mattermost"
    )));
    assert!(requests.iter().any(|request| matches!(request,
        ChannelBackendRequest::ProgressiveTransport(request)
            if matches!(request.as_ref(), tinychannels_bus::module_progressive::ProgressiveTransportRequest::Edit {
                channel_ref: Some(channel), conversation: Some(conversation), text, ..
            } if channel.account_id.as_deref() == Some("mattermost:managed-fixture")
                && conversation.id == "managed-room-1" && text.contains("managed edit"))
    )));
    fixture.shutdown(server).await;
}

#[tokio::test]
async fn controller_manager_validates_credentials_before_host_backend_dispatch() {
    let (host, fixture, _server) = connected_host().await;
    let invalid = dispatch(
        &host,
        ChannelControllerRequest::TestChannel {
            config: ChannelsConfig::default(),
            channel: "telegram".into(),
            auth_mode: ChannelAuthMode::BotToken,
            credentials: Value::Object(Default::default()),
        },
    )
    .await;
    assert!(invalid.is_err());
    assert!(fixture.0.lock().await.is_empty());

    let result = dispatch(
        &host,
        ChannelControllerRequest::TestChannel {
            config: ChannelsConfig::default(),
            channel: "telegram".into(),
            auth_mode: ChannelAuthMode::BotToken,
            credentials: serde_json::json!({"bot_token":"fixture-secret"}),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        ChannelControllerResult::TestChannel(ChannelTestResult { success: true, .. })
    ));
    let calls = fixture.0.lock().await;
    assert!(calls.is_empty(), "structural validation is module-owned");
    drop(calls);

    let pending = dispatch(
        &host,
        ChannelControllerRequest::Connect {
            config: ChannelsConfig::default(),
            channel: "telegram".into(),
            auth_mode: ChannelAuthMode::ManagedDm,
            credentials: Value::Null,
            proxy: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        pending,
        ChannelControllerResult::Connected(ChannelConnectionResult {
            status,
            auth_action: Some(_),
            ..
        }) if status == "pending_auth"
    ));
    assert!(fixture.0.lock().await.is_empty());

    let oauth_pending = dispatch(
        &host,
        ChannelControllerRequest::Connect {
            config: ChannelsConfig::default(),
            channel: "discord".into(),
            auth_mode: ChannelAuthMode::OAuth,
            credentials: Value::Null,
            proxy: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        oauth_pending,
        ChannelControllerResult::Connected(ChannelConnectionResult {
            status,
            auth_action: Some(action),
            ..
        }) if status == "pending_auth" && action == "discord_oauth"
    ));
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[test]
fn controller_failures_keep_diagnostics_and_redact_request_credentials() {
    let request = ChannelControllerRequest::Connect {
        config: ChannelsConfig::default(),
        channel: "telegram".into(),
        auth_mode: ChannelAuthMode::BotToken,
        credentials: serde_json::json!({"bot_token": "fixture-secret"}),
        proxy: Default::default(),
    };

    let error = failed(
        anyhow::anyhow!("provider rejected credential fixture-secret"),
        &sensitive_values(&request),
    );

    assert_eq!(
        error.wire_message(),
        "provider rejected credential [redacted]"
    );
    assert!(!error.wire_message().contains("fixture-secret"));
}

#[tokio::test]
async fn email_connect_verifies_provider_config_inside_module_before_host_persistence() {
    let (host, fixture, _server) = connected_host().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let verifier = Arc::new(RecordingEmailVerifier {
        observed: observed.clone(),
        fail: true,
    });
    let result = dispatch_with(
        &host,
        ChannelControllerRequest::Connect {
            config: ChannelsConfig::default(),
            channel: "email".into(),
            auth_mode: ChannelAuthMode::ApiKey,
            credentials: email_credentials(),
            proxy: Default::default(),
        },
        Arc::new(FakeDiscordApi::default()),
        verifier,
        Arc::new(NativeYuanbaoVerifier),
    )
    .await;
    assert!(
        result.is_err(),
        "provider verification must gate persistence"
    );
    assert!(
        fixture.0.lock().await.is_empty(),
        "failed verification must not reach host storage"
    );
    let observed = observed.lock().await;
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].imap_port, 993);
    assert_eq!(observed[0].smtp_port, 465);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn successful_email_test_reports_verified_login() {
    let (host, fixture, _server) = connected_host().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let result = dispatch_with(
        &host,
        ChannelControllerRequest::TestChannel {
            config: ChannelsConfig::default(),
            channel: "email".into(),
            auth_mode: ChannelAuthMode::ApiKey,
            credentials: email_credentials(),
        },
        Arc::new(FakeDiscordApi::default()),
        Arc::new(RecordingEmailVerifier {
            observed: observed.clone(),
            fail: false,
        }),
        Arc::new(NativeYuanbaoVerifier),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        ChannelControllerResult::TestChannel(ChannelTestResult { success: true, .. })
    ));
    assert_eq!(observed.lock().await.len(), 1);
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_email_ports_are_rejected_before_verifier_or_host_persistence() {
    let (host, fixture, _server) = connected_host().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    for field in ["imap_port", "smtp_port"] {
        let mut credentials = email_credentials();
        credentials[field] = serde_json::json!("not-a-port");
        for request in [
            ChannelControllerRequest::TestChannel {
                config: ChannelsConfig::default(),
                channel: "email".into(),
                auth_mode: ChannelAuthMode::ApiKey,
                credentials: credentials.clone(),
            },
            ChannelControllerRequest::Connect {
                config: ChannelsConfig::default(),
                channel: "email".into(),
                auth_mode: ChannelAuthMode::ApiKey,
                credentials: credentials.clone(),
                proxy: Default::default(),
            },
        ] {
            let result = dispatch_with(
                &host,
                request,
                Arc::new(FakeDiscordApi::default()),
                Arc::new(RecordingEmailVerifier {
                    observed: observed.clone(),
                    fail: false,
                }),
                Arc::new(NativeYuanbaoVerifier),
            )
            .await;
            assert!(result.is_err(), "invalid {field} must be refused");
        }
    }
    assert!(
        observed.lock().await.is_empty(),
        "invalid ports must never reach IMAP verification"
    );
    assert!(
        fixture.0.lock().await.is_empty(),
        "invalid ports must never reach host persistence"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn yuanbao_connect_verifies_with_host_proxy_before_reporting_connected() {
    let (host, fixture, _server) = connected_host().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let verifier = Arc::new(RecordingYuanbaoVerifier {
        observed: observed.clone(),
        fail: true,
    });
    let proxy = tinychannels_bus::HttpProxyPolicy {
        https_proxy: Some("http://proxy.invalid:8080".into()),
        service_selectors: vec!["channel.yuanbao".into()],
        ..Default::default()
    };
    let result = dispatch_with(
        &host,
        ChannelControllerRequest::Connect {
            config: ChannelsConfig::default(),
            channel: "yuanbao".into(),
            auth_mode: ChannelAuthMode::ApiKey,
            credentials: serde_json::json!({
                "app_key": "fixture-key",
                "app_secret": "fixture-secret",
                "env": "pre",
                "api_domain": "https://fixture.invalid",
            }),
            proxy: proxy.clone(),
        },
        Arc::new(FakeDiscordApi::default()),
        Arc::new(NativeEmailVerifier),
        verifier,
    )
    .await;
    assert!(
        result.is_err(),
        "failed credential verification must fail connect"
    );
    assert!(
        fixture.0.lock().await.is_empty(),
        "verification must precede host persistence"
    );
    let observed = observed.lock().await;
    assert_eq!(observed.len(), 1, "Yuanbao verifier must run in the module");
    assert_eq!(observed[0].app_key, "fixture-key");
    assert_eq!(observed[0].app_secret, "fixture-secret");
    assert_eq!(observed[0].env, "pre");
    assert_eq!(observed[0].api_domain, "https://fixture.invalid");
    assert!(observed[0].proxy == proxy);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn controller_manager_routes_thread_and_discord_results_through_backend_port() {
    let (host, fixture, _server) = connected_host().await;
    let thread = dispatch(
        &host,
        ChannelControllerRequest::CreateThread {
            config: ChannelsConfig::default(),
            channel: "discord".into(),
            title: "release planning".into(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        thread,
        ChannelControllerResult::Thread(ChannelThreadResult {
            thread_id,
            title: Some(title),
            ..
        }) if thread_id == "thread-fixture" && title == "release planning"
    ));

    let observed_proxy = Arc::new(Mutex::new(Vec::new()));
    let discord_api = Arc::new(FakeDiscordApi {
        observed_proxy: observed_proxy.clone(),
    });
    let proxy = tinychannels_bus::HttpProxyPolicy {
        https_proxy: Some("http://proxy.example.invalid:8080".into()),
        service_selectors: vec!["channel.discord".into()],
        ..Default::default()
    };
    let guilds = dispatch_with(
        &host,
        ChannelControllerRequest::DiscordListGuilds {
            config: ChannelsConfig::default(),
            bot_token: "fixture-secret".into(),
            proxy: proxy.clone(),
        },
        discord_api.clone(),
        Arc::new(NativeEmailVerifier),
        Arc::new(NativeYuanbaoVerifier),
    )
    .await
    .unwrap();
    assert!(matches!(
        guilds,
        ChannelControllerResult::DiscordGuilds(result)
            if result.guilds.len() == 1
                && result.guilds[0].id == "guild-1"
                && result.guilds[0].name == "Fixture"
    ));
    let channels = dispatch_with(
        &host,
        ChannelControllerRequest::DiscordListChannels {
            config: ChannelsConfig::default(),
            guild_id: "guild-1".into(),
            bot_token: "fixture-secret".into(),
            proxy: proxy.clone(),
        },
        discord_api.clone(),
        Arc::new(NativeEmailVerifier),
        Arc::new(NativeYuanbaoVerifier),
    )
    .await
    .unwrap();
    assert!(matches!(
        channels,
        ChannelControllerResult::DiscordChannels(result)
            if result.channels.len() == 1
                && result.channels[0].id == "channel-1"
                && result.channels[0].kind.as_deref() == Some("text")
    ));
    let permissions = dispatch_with(
        &host,
        ChannelControllerRequest::DiscordCheckPermissions {
            config: ChannelsConfig::default(),
            guild_id: "guild-1".into(),
            channel_id: "channel-1".into(),
            bot_token: "fixture-secret".into(),
            proxy: proxy.clone(),
        },
        discord_api,
        Arc::new(NativeEmailVerifier),
        Arc::new(NativeYuanbaoVerifier),
    )
    .await
    .unwrap();
    assert!(matches!(
        permissions,
        ChannelControllerResult::DiscordPermissions(result)
            if result.can_send_messages && result.missing_permissions.is_empty()
    ));
    let observed_proxy = observed_proxy.lock().await;
    assert_eq!(observed_proxy.len(), 3);
    assert!(observed_proxy.iter().all(|observed| observed == &proxy));
    assert!(matches!(
        fixture.0.lock().await.as_slice(),
        [ChannelBackendRequest::CreateThread { .. }]
    ));
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_discord_controller_validates_host_proxy_before_network_access() {
    let (host, fixture, _server) = connected_host().await;
    let proxy = tinychannels_bus::HttpProxyPolicy {
        https_proxy: Some("http://user:password@not a valid proxy".into()),
        service_selectors: vec!["channel.discord".into()],
        ..Default::default()
    };
    let requests = [
        ChannelControllerRequest::DiscordListGuilds {
            config: ChannelsConfig::default(),
            bot_token: "fixture-secret".into(),
            proxy: proxy.clone(),
        },
        ChannelControllerRequest::DiscordListChannels {
            config: ChannelsConfig::default(),
            guild_id: "guild-1".into(),
            bot_token: "fixture-secret".into(),
            proxy: proxy.clone(),
        },
        ChannelControllerRequest::DiscordCheckPermissions {
            config: ChannelsConfig::default(),
            guild_id: "guild-1".into(),
            channel_id: "channel-1".into(),
            bot_token: "fixture-secret".into(),
            proxy,
        },
    ];

    for request in requests {
        let error = match dispatch(&host, request).await {
            Ok(_) => panic!("invalid Discord proxy should be rejected before network access"),
            Err(error) => error.to_string(),
        };
        assert!(error.ends_with(": invalid HTTPS proxy URL"));
    }
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn controller_manager_routes_managed_state_and_thread_operations() {
    let (host, fixture, _server) = connected_host().await;

    let disconnected = dispatch(
        &host,
        ChannelControllerRequest::Disconnect {
            config: ChannelsConfig::default(),
            channel: "telegram".into(),
            auth_mode: ChannelAuthMode::BotToken,
            clear_memory: false,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        disconnected,
        ChannelControllerResult::Disconnected(result)
            if result.channel == "telegram" && result.disconnected
    ));

    let status = dispatch(
        &host,
        ChannelControllerRequest::Status {
            config: ChannelsConfig::default(),
            channel: Some("telegram".into()),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        status,
        ChannelControllerResult::Status(entries)
            if entries.len() == 1 && entries[0].channel_id == "telegram" && entries[0].connected
    ));

    let reaction = dispatch(
        &host,
        ChannelControllerRequest::SendReaction {
            config: ChannelsConfig::default(),
            channel: "discord".into(),
            reaction: serde_json::json!({"message_id":"fixture", "emoji":"✅"}),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        reaction,
        ChannelControllerResult::SendReaction(result) if result.success
    ));

    let updated = dispatch(
        &host,
        ChannelControllerRequest::UpdateThread {
            config: ChannelsConfig::default(),
            channel: "discord".into(),
            thread_id: "thread-fixture".into(),
            action: "close".into(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        updated,
        ChannelControllerResult::Thread(result)
            if result.thread_id == "thread-fixture" && result.title.as_deref() == Some("updated fixture")
    ));

    let threads = dispatch(
        &host,
        ChannelControllerRequest::ListThreads {
            config: ChannelsConfig::default(),
            channel: "discord".into(),
            active: Some(true),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        threads,
        ChannelControllerResult::Threads(result)
            if result.threads.len() == 1 && result.threads[0].active == Some(true)
    ));

    assert_eq!(fixture.0.lock().await.len(), 5);
    host.shutdown().await.unwrap();
}

#[derive(Default)]
struct FakeDiscordApi {
    observed_proxy: Arc<Mutex<Vec<tinychannels_bus::HttpProxyPolicy>>>,
}

fn email_credentials() -> Value {
    serde_json::json!({
        "imap_host": "imap.example.invalid",
        "username": "member@example.invalid",
        "password": "fixture-secret",
        "smtp_host": "smtp.example.invalid"
    })
}

struct RecordingEmailVerifier {
    observed: Arc<Mutex<Vec<tinychannels::config::EmailConfig>>>,
    fail: bool,
}

struct RecordingYuanbaoVerifier {
    observed: Arc<Mutex<Vec<ObservedYuanbaoVerification>>>,
    fail: bool,
}

#[derive(Clone)]
struct ObservedYuanbaoVerification {
    app_key: String,
    app_secret: String,
    env: String,
    api_domain: String,
    proxy: tinychannels_bus::HttpProxyPolicy,
}

#[async_trait::async_trait]
impl YuanbaoVerifier for RecordingYuanbaoVerifier {
    async fn verify(
        &self,
        config: &tinychannels::config::YuanbaoConfig,
        app_secret: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<()> {
        self.observed
            .lock()
            .await
            .push(ObservedYuanbaoVerification {
                app_key: config.app_key.clone(),
                app_secret: app_secret.to_owned(),
                env: config.env.clone(),
                api_domain: config.api_domain.clone(),
                proxy: proxy.clone(),
            });
        if self.fail {
            Err(anyhow::anyhow!("fixture verification failed"))
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl EmailVerifier for RecordingEmailVerifier {
    async fn verify(&self, config: &tinychannels::config::EmailConfig) -> anyhow::Result<()> {
        self.observed.lock().await.push(config.clone());
        if self.fail {
            Err(anyhow::anyhow!("fixture verification failed"))
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl DiscordApi for FakeDiscordApi {
    async fn list_guilds(
        &self,
        token: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<Vec<tinychannels::providers::discord::api::DiscordGuild>> {
        assert_eq!(token, "fixture-secret");
        self.observed_proxy.lock().await.push(proxy.clone());
        Ok(vec![tinychannels::providers::discord::api::DiscordGuild {
            id: "guild-1".into(),
            name: "Fixture".into(),
            icon: None,
        }])
    }

    async fn list_channels(
        &self,
        token: &str,
        _guild_id: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<Vec<tinychannels::providers::discord::api::DiscordTextChannel>> {
        assert_eq!(token, "fixture-secret");
        self.observed_proxy.lock().await.push(proxy.clone());
        Ok(vec![
            tinychannels::providers::discord::api::DiscordTextChannel {
                id: "channel-1".into(),
                name: "general".into(),
                channel_type: 0,
                position: 1,
                parent_id: None,
            },
        ])
    }

    async fn check_permissions(
        &self,
        token: &str,
        _guild_id: &str,
        _channel_id: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<tinychannels::providers::discord::api::BotPermissionCheck> {
        assert_eq!(token, "fixture-secret");
        self.observed_proxy.lock().await.push(proxy.clone());
        Ok(tinychannels::providers::discord::api::BotPermissionCheck {
            can_view_channel: true,
            can_send_messages: true,
            can_read_message_history: true,
            missing_permissions: vec![],
        })
    }
}

#[tokio::test]
async fn prepared_discord_connection_preserves_omitted_and_cleared_allowlists() {
    let (host, fixture, _server) = connected_host().await;
    for cleared in [false, true] {
        let mut config = ChannelsConfig::default();
        config.discord = Some(tinychannels_bus::config::DiscordConfig {
            bot_token: "old-token".into(),
            guild_id: None,
            channel_id: None,
            allowed_users: vec!["saved-user".into()],
            listen_to_bots: true,
            mention_only: true,
        });
        let mut credentials = serde_json::json!({"bot_token": "new-token"});
        if cleared {
            credentials["allowed_users"] = serde_json::json!("");
        }
        let response = dispatch(
            &host,
            ChannelControllerRequest::PrepareConnect {
                config,
                channel: "discord".into(),
                auth_mode: ChannelAuthMode::BotToken,
                credentials,
                proxy: Default::default(),
            },
        )
        .await
        .unwrap();
        let ChannelControllerResult::PreparedConnect(prepared) = response else {
            panic!("wrong reply")
        };
        let Some(tinychannels_bus::module_controller::ChannelConfigUpdate::Discord(config)) =
            prepared.config_update
        else {
            panic!("missing config")
        };
        assert_eq!(config.bot_token, "new-token");
        assert!(config.listen_to_bots && config.mention_only);
        assert_eq!(
            config.allowed_users,
            if cleared { vec![] } else { vec!["saved-user"] }
        );
        assert_eq!(prepared.result.status, "connected");
    }
    assert!(
        fixture.0.lock().await.is_empty(),
        "preparation must not persist host state"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_email_connection_returns_verified_config_without_password() {
    let (host, fixture, _server) = connected_host().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let response = dispatch_with(
        &host,
        ChannelControllerRequest::PrepareConnect {
            config: ChannelsConfig::default(),
            channel: "email".into(),
            auth_mode: ChannelAuthMode::ApiKey,
            credentials: email_credentials(),
            proxy: Default::default(),
        },
        Arc::new(FakeDiscordApi::default()),
        Arc::new(RecordingEmailVerifier {
            observed: observed.clone(),
            fail: false,
        }),
        Arc::new(NativeYuanbaoVerifier),
    )
    .await
    .unwrap();
    let ChannelControllerResult::PreparedConnect(prepared) = response else {
        panic!("wrong reply")
    };
    let Some(tinychannels_bus::module_controller::ChannelConfigUpdate::Email(config)) =
        prepared.config_update
    else {
        panic!("missing config")
    };
    assert!(
        config.password.is_empty(),
        "password must remain in host custody"
    );
    let verified = observed.lock().await;
    assert_eq!(verified.len(), 1);
    assert_eq!(config.imap_host, verified[0].imap_host);
    assert_eq!(config.smtp_host, verified[0].smtp_host);
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_connection_refuses_bad_credentials_without_host_persistence() {
    let (host, fixture, _server) = connected_host().await;
    let result = dispatch(
        &host,
        ChannelControllerRequest::PrepareConnect {
            config: ChannelsConfig::default(),
            channel: "telegram".into(),
            auth_mode: ChannelAuthMode::BotToken,
            credentials: serde_json::json!({}),
            proxy: Default::default(),
        },
    )
    .await;
    assert!(result.is_err());
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_yuanbao_configuration_matches_the_verified_override_cluster() {
    let (host, fixture, _server) = connected_host().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let response = dispatch_with(&host, ChannelControllerRequest::PrepareConnect {
        config: ChannelsConfig::default(), channel: "yuanbao".into(),
        auth_mode: ChannelAuthMode::ApiKey,
        credentials: serde_json::json!({ "app_key": " fixture-key ", "app_secret": " fixture-secret ",
            "env": "pre", "api_domain": "https://fixture.invalid" }),
        proxy: Default::default(),
    }, Arc::new(FakeDiscordApi::default()), Arc::new(NativeEmailVerifier),
        Arc::new(RecordingYuanbaoVerifier { observed: observed.clone(), fail: false })).await.unwrap();
    let ChannelControllerResult::PreparedConnect(prepared) = response else {
        panic!("wrong reply")
    };
    let Some(tinychannels_bus::module_controller::ChannelConfigUpdate::Yuanbao(config)) =
        prepared.config_update
    else {
        panic!("missing config")
    };
    assert!(config.app_secret.is_empty());
    let verified = observed.lock().await;
    assert_eq!(verified.len(), 1);
    assert_eq!(config.app_key, verified[0].app_key);
    assert_eq!(config.env, verified[0].env);
    assert_eq!(config.api_domain, verified[0].api_domain);
    assert_eq!(verified[0].app_secret, "fixture-secret");
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_yuanbao_connection_preserves_saved_endpoint_overrides() {
    let (host, fixture, _server) = connected_host().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut config = ChannelsConfig::default();
    config.yuanbao = Some(tinychannels_bus::config::YuanbaoConfig {
        env: "pre".into(),
        api_domain: "https://saved-api.invalid".into(),
        ws_domain: "wss://saved-ws.invalid".into(),
        ..Default::default()
    });
    let response = dispatch_with(&host, ChannelControllerRequest::PrepareConnect {
        config, channel: "yuanbao".into(), auth_mode: ChannelAuthMode::ApiKey,
        credentials: serde_json::json!({"app_key":"fixture-key", "app_secret":"fixture-secret"}),
        proxy: Default::default(),
    }, Arc::new(FakeDiscordApi::default()), Arc::new(NativeEmailVerifier),
        Arc::new(RecordingYuanbaoVerifier { observed: observed.clone(), fail: false })).await.unwrap();
    let ChannelControllerResult::PreparedConnect(prepared) = response else {
        panic!("wrong reply")
    };
    let Some(tinychannels_bus::module_controller::ChannelConfigUpdate::Yuanbao(config)) =
        prepared.config_update
    else {
        panic!("missing config")
    };
    let verified = observed.lock().await;
    assert_eq!(verified.len(), 1);
    assert_eq!(config.api_domain, "https://saved-api.invalid");
    assert_eq!(config.env, "pre");
    assert_eq!(config.api_domain, verified[0].api_domain);
    assert_eq!(config.env, verified[0].env);
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_telegram_connection_preserves_streaming_policy_and_clears_omitted_target() {
    let (host, fixture, _server) = connected_host().await;
    let mut config = ChannelsConfig::default();
    config.telegram = Some(tinychannels_bus::config::TelegramConfig {
        bot_token: "old-token".into(),
        chat_id: Some("old-target".into()),
        allowed_users: vec!["old-user".into()],
        stream_mode: tinychannels_bus::config::StreamMode::Partial,
        draft_update_interval_ms: 750,
        silent_streaming: false,
        mention_only: true,
    });
    let response = dispatch(&host, ChannelControllerRequest::PrepareConnect {
        config, channel: "telegram".into(), auth_mode: ChannelAuthMode::BotToken,
        credentials: serde_json::json!({"bot_token":"new-token", "allowed_users":"@Alice, ALICE"}), proxy: Default::default(),
    }).await.unwrap();
    let ChannelControllerResult::PreparedConnect(prepared) = response else {
        panic!("wrong reply")
    };
    let Some(tinychannels_bus::module_controller::ChannelConfigUpdate::Telegram(config)) =
        prepared.config_update
    else {
        panic!("missing config")
    };
    assert_eq!(config.bot_token, "new-token");
    assert_eq!(config.chat_id, None);
    assert_eq!(config.allowed_users, ["alice"]);
    assert_eq!(
        config.stream_mode,
        tinychannels_bus::config::StreamMode::Partial
    );
    assert_eq!(config.draft_update_interval_ms, 750);
    assert!(!config.silent_streaming);
    assert!(config.mention_only);
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_managed_connection_preserves_pending_auth_without_configuration() {
    let (host, fixture, _server) = connected_host().await;
    let response = dispatch(
        &host,
        ChannelControllerRequest::PrepareConnect {
            config: ChannelsConfig::default(),
            channel: "discord".into(),
            auth_mode: ChannelAuthMode::OAuth,
            credentials: Value::Null,
            proxy: Default::default(),
        },
    )
    .await
    .unwrap();
    let ChannelControllerResult::PreparedConnect(prepared) = response else {
        panic!("wrong reply")
    };
    assert_eq!(prepared.result.status, "pending_auth");
    assert_eq!(
        prepared.result.auth_action.as_deref(),
        Some("discord_oauth")
    );
    assert!(!prepared.result.restart_required);
    assert!(prepared.config_update.is_none());
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn backend_preserves_host_owned_operations_and_default_selection() {
    let (host, _, _server) = connected_host().await;
    let backend = HostBackend::new(
        host.clone(),
        Arc::new(FakeDiscordApi::default()),
        Arc::new(RecordingEmailVerifier {
            observed: Default::default(),
            fail: false,
        }),
        None,
    );
    let config = ChannelsConfig {
        active_channel: Some("telegram".into()),
        ..Default::default()
    };
    assert_eq!(
        backend
            .get_default_channel(&config)
            .await
            .unwrap()
            .as_deref(),
        Some("telegram")
    );
    assert!(
        backend
            .set_default_channel(&config, "discord")
            .await
            .unwrap_err()
            .to_string()
            .contains("host-owned")
    );
    assert!(
        backend
            .telegram_login_start(&config)
            .await
            .unwrap_err()
            .to_string()
            .contains("host-owned")
    );
    assert!(
        backend
            .telegram_login_check(&config, "link")
            .await
            .unwrap_err()
            .to_string()
            .contains("host-owned")
    );
    assert!(
        backend
            .discord_link_start(&config)
            .await
            .unwrap_err()
            .to_string()
            .contains("host-owned")
    );
    assert!(
        backend
            .discord_link_check(&config, "link")
            .await
            .unwrap_err()
            .to_string()
            .contains("host-owned")
    );
    assert!(
        backend
            .send_outbound_intent(
                &config,
                tinychannels::outbound_intent_from_send_message(
                    "telegram",
                    &tinychannels::SendMessage::new("body", "room"),
                ),
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("module provider path")
    );
    assert!(
        backend
            .discord_list_guilds(&config)
            .await
            .unwrap_err()
            .to_string()
            .contains("token is empty")
    );
    host.shutdown().await.unwrap();
}

fn telegram_config(chat_id: Option<&str>) -> ChannelsConfig {
    ChannelsConfig {
        telegram: Some(tinychannels::config::TelegramConfig {
            bot_token: "tok".into(),
            chat_id: chat_id.map(str::to_owned),
            allowed_users: vec!["*".into()],
            stream_mode: Default::default(),
            draft_update_interval_ms: 1000,
            silent_streaming: true,
            mention_only: false,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn bot_token_telegram_reactions_and_threads_never_reach_the_host_backend() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    for (name, body) in [
        (
            "setMessageReaction",
            serde_json::json!({"ok": true, "result": true}),
        ),
        (
            "createForumTopic",
            serde_json::json!({"ok": true, "result": {"message_thread_id": 9, "name": "Plan"}}),
        ),
        (
            "closeForumTopic",
            serde_json::json!({"ok": true, "result": true}),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(format!("/bottok/{name}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
    }
    let (host, fixture, _server) = connected_host().await;
    let mut backend = HostBackend::new(
        host.clone(),
        Arc::new(FakeDiscordApi::default()),
        Arc::new(NativeEmailVerifier),
        None,
    );
    backend.telegram_api_base = Some(server.uri());
    let config = telegram_config(Some("-100"));

    let reaction = backend
        .send_reaction(
            &config,
            "telegram",
            serde_json::json!({"chatId":"-100","messageId":4,"emoji":"👍"}),
        )
        .await
        .unwrap();
    assert!(reaction.success);
    let created = backend
        .create_thread(&config, "telegram", "Plan")
        .await
        .unwrap();
    assert_eq!(
        (created.thread_id.as_str(), created.title.as_deref()),
        ("9", Some("Plan"))
    );
    let closed = backend
        .update_thread(&config, "telegram", "9", "close")
        .await
        .unwrap();
    assert_eq!(closed.thread_id, "9");
    assert!(
        backend
            .list_threads(&config, "telegram", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot list")
    );
    assert!(
        backend
            .update_thread(&config, "telegram", "9", "bogus")
            .await
            .is_err()
    );
    assert!(
        backend
            .send_reaction(&config, "telegram", serde_json::json!({"emoji":"x"}))
            .await
            .is_err()
    );
    assert!(
        fixture.0.lock().await.is_empty(),
        "native telegram must not call the host backend"
    );

    let no_chat = telegram_config(None);
    assert!(
        backend
            .create_thread(&no_chat, "telegram", "x")
            .await
            .unwrap_err()
            .to_string()
            .contains("chat_id")
    );
    assert!(
        backend
            .update_thread(&no_chat, "telegram", "9", "close")
            .await
            .is_err()
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_slack_reactions_use_native_web_api_without_managed_fallback() {
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/reactions.add"))
        .and(header("authorization", "Bearer s"))
        .and(body_json(serde_json::json!({
            "channel": "C1",
            "timestamp": "171.2",
            "name": "thumbsup"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/reactions.remove"))
        .and(header("authorization", "Bearer s"))
        .and(body_json(serde_json::json!({
            "channel": "C1",
            "timestamp": "171.2",
            "name": "tada"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/reactions.add"))
        .and(header("authorization", "Bearer s"))
        .and(body_json(serde_json::json!({
            "channel": "C2",
            "timestamp": "172.3",
            "name": "eyes"
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"ok": false, "error": "channel_not_found"})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let (host, fixture, _server) = connected_host().await;
    let mut backend = HostBackend::new(
        host.clone(),
        Arc::new(FakeDiscordApi::default()),
        Arc::new(NativeEmailVerifier),
        None,
    );
    backend.slack_api_base = Some(server.uri());
    let config = ChannelsConfig {
        slack: Some(tinychannels::config::SlackConfig {
            bot_token: "s".into(),
            app_token: None,
            channel_id: Some("C1".into()),
            allowed_users: vec![],
        }),
        ..Default::default()
    };

    let result = backend
        .send_reaction(
            &config,
            "slack",
            serde_json::json!({
                "channelId": "C1",
                "messageId": "171.2",
                "emoji": "👍"
            }),
        )
        .await
        .expect("configured Slack reaction should use the native adapter");
    assert!(result.success);
    let removed = backend
        .send_reaction(
            &config,
            "slack",
            serde_json::json!({
                "channelId": "C1",
                "messageId": "171.2",
                "emoji": ":tada:",
                "remove": true
            }),
        )
        .await
        .expect("configured Slack reaction removal should use the native adapter");
    assert!(removed.success);

    let api_error = backend
        .send_reaction(
            &config,
            "slack",
            serde_json::json!({
                "channelId": "C2",
                "messageId": "172.3",
                "emoji": "👀"
            }),
        )
        .await
        .expect_err("Slack API rejection must be returned to the caller");
    assert!(api_error.to_string().contains("channel_not_found"));

    let before_invalid = server.received_requests().await.unwrap().len();
    assert!(
        backend
            .send_reaction(&config, "slack", serde_json::json!({}))
            .await
            .is_err()
    );
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        before_invalid
    );
    assert!(fixture.0.lock().await.is_empty());
    assert_eq!(before_invalid, 3);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn slack_threads_without_native_port_fail_explicitly_not_via_host() {
    let (host, fixture, _server) = connected_host().await;
    let backend = HostBackend::new(
        host.clone(),
        Arc::new(FakeDiscordApi::default()),
        Arc::new(NativeEmailVerifier),
        None,
    );
    let config = ChannelsConfig {
        slack: Some(tinychannels::config::SlackConfig {
            bot_token: "s".into(),
            app_token: None,
            channel_id: None,
            allowed_users: vec![],
        }),
        ..Default::default()
    };
    for error in [
        backend
            .create_thread(&config, "slack", "t")
            .await
            .map(|_| ()),
        backend
            .update_thread(&config, "slack", "1", "close")
            .await
            .map(|_| ()),
        backend
            .list_threads(&config, "slack", None)
            .await
            .map(|_| ()),
    ] {
        assert!(error.unwrap_err().to_string().contains("no native"));
    }
    assert!(fixture.0.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

fn discord_config(channel_id: Option<&str>) -> ChannelsConfig {
    ChannelsConfig {
        discord: Some(tinychannels::config::DiscordConfig {
            bot_token: "dtok".into(),
            guild_id: Some("900".into()),
            channel_id: channel_id.map(str::to_owned),
            allowed_users: vec![],
            listen_to_bots: false,
            mention_only: false,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn bot_token_discord_reactions_and_threads_never_reach_the_host_backend() {
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path_regex(r"^/channels/55/messages/66/reactions/.+/@me$"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/channels/55/threads"))
        .respond_with(
            ResponseTemplate::new(201)
                .set_body_json(serde_json::json!({"id": "77", "name": "Plan"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/channels/77"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": "77"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/guilds/900/threads/active"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "threads": [{"id": "77", "name": "Plan", "parent_id": "55", "thread_metadata": {"archived": false}}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let (host, fixture, _server) = connected_host().await;
    let mut backend = HostBackend::new(
        host.clone(),
        Arc::new(FakeDiscordApi::default()),
        Arc::new(NativeEmailVerifier),
        None,
    );
    backend.discord_api_base = Some(server.uri());
    let config = discord_config(Some("55"));

    let reaction = backend
        .send_reaction(
            &config,
            "discord",
            serde_json::json!({"channelId": "55", "messageId": "66", "emoji": "🔥"}),
        )
        .await
        .unwrap();
    assert!(reaction.success);
    let created = backend
        .create_thread(&config, "discord", "Plan")
        .await
        .unwrap();
    assert_eq!(
        (created.thread_id.as_str(), created.title.as_deref()),
        ("77", Some("Plan"))
    );
    let archived = backend
        .update_thread(&config, "discord", "77", "archive")
        .await
        .unwrap();
    assert_eq!(archived.thread_id, "77");
    let listed = backend
        .list_threads(&config, "discord", None)
        .await
        .unwrap();
    assert_eq!(listed.threads.len(), 1);
    assert_eq!(listed.threads[0].thread_id, "77");

    assert!(
        backend
            .update_thread(&config, "discord", "77", "bogus")
            .await
            .is_err()
    );
    assert!(
        backend
            .send_reaction(&config, "discord", serde_json::json!({"emoji": "x"}))
            .await
            .is_err()
    );
    let no_channel = discord_config(None);
    assert!(
        backend
            .create_thread(&no_channel, "discord", "x")
            .await
            .unwrap_err()
            .to_string()
            .contains("channel_id")
    );
    assert!(
        fixture.0.lock().await.is_empty(),
        "native discord must not call the host backend"
    );
    host.shutdown().await.unwrap();
}
