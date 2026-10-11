use super::*;
use std::sync::Arc;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinybus::{Connection, Result};
use tinychannels_bus::controllers::ChannelThreadResult;
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct BackendFixture(Arc<Mutex<Vec<ChannelBackendRequest>>>);

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl BackendFixture {
    async fn dispatch_controller_backend(
        &self,
        request: ChannelBackendRequest,
    ) -> Result<ChannelBackendResult> {
        self.0.lock().await.push(request.clone());
        match request {
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
        }
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
