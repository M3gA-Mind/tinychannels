//! Channel controller dispatch through TinyChannels' owning manager.

use crate::host::HostChannels;
use serde_json::Value;
use std::sync::Arc;
use tinychannels::backend::{ChannelBackend, ChannelManager};
use tinychannels::controllers::{
    ChannelAuthMode, ChannelConnectionResult, ChannelDisconnectResult, ChannelReactionResult,
    ChannelSendMessageResult, ChannelStatusEntry, ChannelTestResult, ChannelThreadListResult,
    ChannelThreadResult, DiscordChannelListResult, DiscordGuildListResult, DiscordLinkCheckResult,
    DiscordLinkStartResult, DiscordPermissionCheckResult, TelegramLoginCheckResult,
    TelegramLoginStartResult,
};
use tinychannels::{ChannelOutboundIntent, ChannelsConfig};
use tinychannels_bus::{
    ChannelBackendRequest, ChannelBackendResult, ChannelControllerRequest, ChannelControllerResult,
};

/// Run a channel controller operation through the module-owned manager.
pub(crate) async fn dispatch(
    host: &HostChannels,
    request: ChannelControllerRequest,
) -> tinybus::Result<ChannelControllerResult> {
    dispatch_with(
        host,
        request,
        Arc::new(NativeDiscordApi),
        Arc::new(NativeEmailVerifier),
        Arc::new(NativeYuanbaoVerifier),
    )
    .await
}

async fn dispatch_with(
    host: &HostChannels,
    request: ChannelControllerRequest,
    discord: Arc<dyn DiscordApi>,
    email: Arc<dyn EmailVerifier>,
    yuanbao: Arc<dyn YuanbaoVerifier>,
) -> tinybus::Result<ChannelControllerResult> {
    let sensitive_values = sensitive_values(&request);
    match request {
        ChannelControllerRequest::Connect {
            config,
            channel,
            auth_mode,
            credentials,
            proxy,
        } => {
            let mut backend = HostBackend::new(host.clone(), discord.clone(), email.clone(), None);
            backend.yuanbao = yuanbao.clone();
            backend.proxy = proxy;
            let manager = ChannelManager::new(config, backend);
            manager
                .connect(&channel, auth_mode, credentials)
                .await
                .map(ChannelControllerResult::Connected)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::PrepareConnect {
            config,
            channel,
            auth_mode,
            credentials,
            proxy,
        } => {
            let mut backend = HostBackend::new(host.clone(), discord.clone(), email.clone(), None);
            backend.yuanbao = yuanbao.clone();
            backend.proxy = proxy;
            let manager = ChannelManager::new(config.clone(), backend);
            let result = manager
                .prepare_connect(&channel, auth_mode, credentials.clone())
                .await
                .map_err(|error| failed(error, &sensitive_values))?;
            let config_update = if result.status == "connected" {
                crate::prepare_connect::config_update(&config, &channel, auth_mode, &credentials)
                    .map_err(|error| failed(error, &sensitive_values))?
            } else {
                None
            };
            Ok(ChannelControllerResult::PreparedConnect(Box::new(
                tinychannels_bus::module_controller::PreparedChannelConnection {
                    result,
                    config_update,
                },
            )))
        }
        ChannelControllerRequest::Disconnect {
            config,
            channel,
            auth_mode,
            clear_memory,
        } => {
            let manager = ChannelManager::new(
                config,
                HostBackend::new(host.clone(), discord.clone(), email.clone(), None),
            );
            manager
                .disconnect(&channel, auth_mode, clear_memory)
                .await
                .map(ChannelControllerResult::Disconnected)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::Status { config, channel } => {
            let manager = ChannelManager::new(
                config,
                HostBackend::new(host.clone(), discord.clone(), email.clone(), None),
            );
            manager
                .status(channel.as_deref())
                .await
                .map(ChannelControllerResult::Status)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::TestChannel {
            config,
            channel,
            auth_mode,
            credentials,
        } => {
            let manager = ChannelManager::new(
                config,
                HostBackend::new(host.clone(), discord.clone(), email.clone(), None),
            );
            manager
                .test(&channel, auth_mode, credentials)
                .await
                .map(ChannelControllerResult::TestChannel)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::SendReaction {
            config,
            channel,
            reaction,
        } => {
            let manager = ChannelManager::new(
                config,
                HostBackend::new(host.clone(), discord.clone(), email.clone(), None),
            );
            manager
                .send_reaction(&channel, reaction)
                .await
                .map(ChannelControllerResult::SendReaction)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::CreateThread {
            config,
            channel,
            title,
        } => {
            let manager = ChannelManager::new(
                config,
                HostBackend::new(host.clone(), discord.clone(), email.clone(), None),
            );
            manager
                .create_thread(&channel, &title)
                .await
                .map(ChannelControllerResult::Thread)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::UpdateThread {
            config,
            channel,
            thread_id,
            action,
        } => {
            let manager = ChannelManager::new(
                config,
                HostBackend::new(host.clone(), discord.clone(), email.clone(), None),
            );
            manager
                .update_thread(&channel, &thread_id, &action)
                .await
                .map(ChannelControllerResult::Thread)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::ListThreads {
            config,
            channel,
            active,
        } => {
            let manager = ChannelManager::new(
                config,
                HostBackend::new(host.clone(), discord.clone(), email.clone(), None),
            );
            manager
                .list_threads(&channel, active)
                .await
                .map(ChannelControllerResult::Threads)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::DiscordListGuilds {
            config,
            bot_token,
            proxy,
        } => {
            let mut backend = HostBackend::new(
                host.clone(),
                discord.clone(),
                email.clone(),
                Some(bot_token),
            );
            backend.proxy = proxy;
            let manager = ChannelManager::new(config, backend);
            manager
                .discord_list_guilds()
                .await
                .map(ChannelControllerResult::DiscordGuilds)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::DiscordListChannels {
            config,
            guild_id,
            bot_token,
            proxy,
        } => {
            let mut backend = HostBackend::new(
                host.clone(),
                discord.clone(),
                email.clone(),
                Some(bot_token),
            );
            backend.proxy = proxy;
            let manager = ChannelManager::new(config, backend);
            manager
                .discord_list_channels(&guild_id)
                .await
                .map(ChannelControllerResult::DiscordChannels)
                .map_err(|error| failed(error, &sensitive_values))
        }
        ChannelControllerRequest::DiscordCheckPermissions {
            config,
            guild_id,
            channel_id,
            bot_token,
            proxy,
        } => {
            let mut backend = HostBackend::new(
                host.clone(),
                discord.clone(),
                email.clone(),
                Some(bot_token),
            );
            backend.proxy = proxy;
            let manager = ChannelManager::new(config, backend);
            manager
                .discord_check_permissions(&guild_id, &channel_id)
                .await
                .map(ChannelControllerResult::DiscordPermissions)
                .map_err(|error| failed(error, &sensitive_values))
        }
    }
}

fn failed(error: anyhow::Error, sensitive_values: &[String]) -> tinybus::Error {
    let mut message = error.to_string();
    for secret in sensitive_values {
        if !secret.is_empty() {
            message = message.replace(secret, "[redacted]");
        }
    }
    tinybus::Error::failed(message)
}

fn sensitive_values(request: &ChannelControllerRequest) -> Vec<String> {
    fn collect(value: &Value, inherited_sensitive: bool, output: &mut Vec<String>) {
        match value {
            Value::Object(fields) => {
                for (name, value) in fields {
                    let normalized = name.to_ascii_lowercase();
                    let sensitive = inherited_sensitive
                        || ["credential", "token", "secret", "password", "api_key"]
                            .iter()
                            .any(|marker| normalized.contains(marker));
                    collect(value, sensitive, output);
                }
            }
            Value::Array(values) => {
                for value in values {
                    collect(value, inherited_sensitive, output);
                }
            }
            Value::String(value) if inherited_sensitive && !value.is_empty() => {
                output.push(value.clone());
            }
            _ => {}
        }
    }

    let Ok(value) = serde_json::to_value(request) else {
        return Vec::new();
    };
    let mut values = Vec::new();
    collect(&value, false, &mut values);
    values.sort_by_key(|value| std::cmp::Reverse(value.len()));
    values.dedup();
    values
}

#[derive(Clone)]
struct HostBackend {
    host: HostChannels,
    discord: Arc<dyn DiscordApi>,
    email: Arc<dyn EmailVerifier>,
    yuanbao: Arc<dyn YuanbaoVerifier>,
    proxy: tinychannels_bus::HttpProxyPolicy,
    discord_token: Option<String>,
    /// Bot API base override (tests and self-hosted Bot API servers).
    telegram_api_base: Option<String>,
    /// Discord REST base override (tests and proxies).
    discord_api_base: Option<String>,
    /// Slack Web API base override (tests and proxies).
    slack_api_base: Option<String>,
}

impl HostBackend {
    fn new(
        host: HostChannels,
        discord: Arc<dyn DiscordApi>,
        email: Arc<dyn EmailVerifier>,
        discord_token: Option<String>,
    ) -> Self {
        Self {
            host,
            discord,
            email,
            yuanbao: Arc::new(NativeYuanbaoVerifier),
            proxy: tinychannels_bus::HttpProxyPolicy::default(),
            discord_token,
            telegram_api_base: None,
            discord_api_base: None,
            slack_api_base: None,
        }
    }

    /// A bot-token Discord client for controller operations, when the config
    /// carries a bot token. `None` means the managed transport applies.
    fn discord_native(
        &self,
        config: &ChannelsConfig,
        channel: &str,
    ) -> anyhow::Result<Option<tinychannels::providers::DiscordChannel>> {
        use tinychannels::factory::HttpClientFactory as _;
        if channel != "discord" {
            return Ok(None);
        }
        let Some(discord) = config
            .discord
            .as_ref()
            .filter(|discord| !discord.bot_token.trim().is_empty())
        else {
            return Ok(None);
        };
        let clients = tinychannels::factory::ConfiguredHttpClients::new(self.proxy.clone())?;
        let mut native = tinychannels::providers::DiscordChannel::with_http_client(
            discord.bot_token.clone(),
            discord.guild_id.clone(),
            discord.channel_id.clone(),
            discord.allowed_users.clone(),
            discord.listen_to_bots,
            discord.mention_only,
            clients.client_for("channel.discord"),
        );
        if let Some(base) = &self.discord_api_base {
            native = native.with_api_base(base.clone());
        }
        Ok(Some(native))
    }

    /// Bot-token Telegram client plus its default chat, when the config carries
    /// a bot token. `None` means the managed (host-backend) transport applies.
    fn telegram_native(
        &self,
        config: &ChannelsConfig,
        channel: &str,
    ) -> anyhow::Result<
        Option<(
            tinychannels::providers::telegram::TelegramBotApi,
            Option<String>,
        )>,
    > {
        use tinychannels::factory::HttpClientFactory as _;
        if channel != "telegram" {
            return Ok(None);
        }
        let Some(telegram) = config
            .telegram
            .as_ref()
            .filter(|telegram| !telegram.bot_token.trim().is_empty())
        else {
            return Ok(None);
        };
        let clients = tinychannels::factory::ConfiguredHttpClients::new(self.proxy.clone())?;
        let mut api = tinychannels::providers::telegram::TelegramBotApi::new(
            telegram.bot_token.clone(),
            clients.client_for("channel.telegram"),
        );
        if let Some(base) = &self.telegram_api_base {
            api = api.with_api_base(base.clone());
        }
        Ok(Some((api, telegram.chat_id.clone())))
    }

    /// Bot-token providers that have no native port for `operation` fail
    /// explicitly instead of being routed to the managed host backend.
    fn reject_unported(
        config: &ChannelsConfig,
        channel: &str,
        operation: &str,
    ) -> anyhow::Result<()> {
        let bot_token_configured = match channel {
            "slack" => config.slack.is_some(),
            "mattermost" => config.mattermost.is_some(),
            _ => false,
        };
        if bot_token_configured {
            tracing::debug!(
                channel,
                operation,
                "[channels] no native port for bot-token channel"
            );
            anyhow::bail!(
                "{channel} has no native {operation} support yet; it is not routed to the managed backend"
            );
        }
        Ok(())
    }

    fn slack_native(
        &self,
        config: &ChannelsConfig,
        channel: &str,
    ) -> anyhow::Result<Option<tinychannels::providers::slack::SlackChannel>> {
        use tinychannels::factory::HttpClientFactory as _;
        if channel != "slack" {
            return Ok(None);
        }
        let Some(slack) = config
            .slack
            .as_ref()
            .filter(|slack| !slack.bot_token.trim().is_empty())
        else {
            return Ok(None);
        };
        let clients = tinychannels::factory::ConfiguredHttpClients::new(self.proxy.clone())?;
        let mut native = tinychannels::providers::slack::SlackChannel::with_http_client(
            slack.bot_token.clone(),
            slack.channel_id.clone(),
            slack.allowed_users.clone(),
            clients.client_for("channel.slack"),
        );
        if let Some(app_token) = &slack.app_token {
            native = native.with_app_token(Some(app_token.clone()));
        }
        if let Some(base) = &self.slack_api_base {
            native = native.with_api_base(base.clone());
        }
        Ok(Some(native))
    }

    /// A configured Mattermost bot whose controller operations use the
    /// provider's native v4 API rather than the managed backend.
    fn mattermost_native(
        &self,
        config: &ChannelsConfig,
        channel: &str,
    ) -> anyhow::Result<Option<tinychannels::providers::MattermostChannel>> {
        use tinychannels::factory::HttpClientFactory as _;
        if channel != "mattermost" {
            return Ok(None);
        }
        let Some(mattermost) = config
            .mattermost
            .as_ref()
            .filter(|mattermost| !mattermost.bot_token.trim().is_empty())
        else {
            return Ok(None);
        };
        let clients = tinychannels::factory::ConfiguredHttpClients::new(self.proxy.clone())?;
        Ok(Some(
            tinychannels::providers::MattermostChannel::with_http_client(
                mattermost.url.clone(),
                mattermost.bot_token.clone(),
                mattermost.channel_id.clone(),
                mattermost.allowed_users.clone(),
                mattermost.thread_replies.unwrap_or(true),
                mattermost.mention_only.unwrap_or(false),
                clients.client_for("channel.mattermost"),
            ),
        ))
    }

    async fn invoke(&self, request: ChannelBackendRequest) -> anyhow::Result<ChannelBackendResult> {
        self.host
            .dispatch_controller_backend(request)
            .await
            .map_err(anyhow::Error::msg)
    }
}

#[async_trait::async_trait]
trait DiscordApi: Send + Sync {
    async fn list_guilds(
        &self,
        token: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<Vec<tinychannels::providers::discord::api::DiscordGuild>>;
    async fn list_channels(
        &self,
        token: &str,
        guild_id: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<Vec<tinychannels::providers::discord::api::DiscordTextChannel>>;
    async fn check_permissions(
        &self,
        token: &str,
        guild_id: &str,
        channel_id: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<tinychannels::providers::discord::api::BotPermissionCheck>;
}

struct NativeDiscordApi;

#[async_trait::async_trait]
impl DiscordApi for NativeDiscordApi {
    async fn list_guilds(
        &self,
        token: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<Vec<tinychannels::providers::discord::api::DiscordGuild>> {
        use tinychannels::factory::HttpClientFactory as _;
        let clients = tinychannels::factory::ConfiguredHttpClients::new(proxy.clone())?;
        let client = clients.client_for("channel.discord");
        tinychannels::providers::discord::api::list_bot_guilds_with_client(&client, token).await
    }

    async fn list_channels(
        &self,
        token: &str,
        guild_id: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<Vec<tinychannels::providers::discord::api::DiscordTextChannel>> {
        use tinychannels::factory::HttpClientFactory as _;
        let clients = tinychannels::factory::ConfiguredHttpClients::new(proxy.clone())?;
        let client = clients.client_for("channel.discord");
        tinychannels::providers::discord::api::list_guild_channels_with_client(
            &client, token, guild_id,
        )
        .await
    }

    async fn check_permissions(
        &self,
        token: &str,
        guild_id: &str,
        channel_id: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<tinychannels::providers::discord::api::BotPermissionCheck> {
        use tinychannels::factory::HttpClientFactory as _;
        let clients = tinychannels::factory::ConfiguredHttpClients::new(proxy.clone())?;
        let client = clients.client_for("channel.discord");
        tinychannels::providers::discord::api::check_channel_permissions_with_client(
            &client, token, guild_id, channel_id,
        )
        .await
    }
}

#[async_trait::async_trait]
trait EmailVerifier: Send + Sync {
    async fn verify(&self, config: &tinychannels::config::EmailConfig) -> anyhow::Result<()>;
}

struct NativeEmailVerifier;

#[async_trait::async_trait]
impl EmailVerifier for NativeEmailVerifier {
    async fn verify(&self, config: &tinychannels::config::EmailConfig) -> anyhow::Result<()> {
        tinychannels::providers::verify_email_credentials(config)
            .await
            .map_err(anyhow::Error::msg)
    }
}

#[async_trait::async_trait]
trait YuanbaoVerifier: Send + Sync {
    async fn verify(
        &self,
        config: &tinychannels::config::YuanbaoConfig,
        app_secret: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<()>;
}

struct NativeYuanbaoVerifier;

#[async_trait::async_trait]
impl YuanbaoVerifier for NativeYuanbaoVerifier {
    async fn verify(
        &self,
        config: &tinychannels::config::YuanbaoConfig,
        app_secret: &str,
        proxy: &tinychannels_bus::HttpProxyPolicy,
    ) -> anyhow::Result<()> {
        use tinychannels::factory::HttpClientFactory as _;
        let clients = tinychannels::factory::ConfiguredHttpClients::new(proxy.clone())?;
        tinychannels::providers::yuanbao::connect::verify_yuanbao_credentials(
            clients.client_for("channel.yuanbao"),
            config,
            app_secret,
        )
        .await
        .map_err(anyhow::Error::msg)
    }
}

#[async_trait::async_trait]
impl ChannelBackend for HostBackend {
    async fn connect_channel(
        &self,
        config: &ChannelsConfig,
        channel: &str,
        auth_mode: ChannelAuthMode,
        credentials: Value,
    ) -> anyhow::Result<ChannelConnectionResult> {
        if channel == "email" && auth_mode == ChannelAuthMode::ApiKey {
            let credentials = credentials
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("credentials must be a JSON object"))?;
            let email =
                tinychannels::controllers::build_email_config(credentials, config.email.as_ref())
                    .map_err(anyhow::Error::msg)?;
            self.email.verify(&email).await?;
        }
        if channel == "yuanbao" && auth_mode == ChannelAuthMode::ApiKey {
            let credential_fields = credentials
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("credentials must be a JSON object"))?;
            let app_key = tinychannels::providers::yuanbao::connect::require_yuanbao_field(
                credential_fields,
                "app_key",
            )
            .map_err(anyhow::Error::msg)?;
            let app_secret = tinychannels::providers::yuanbao::connect::require_yuanbao_field(
                credential_fields,
                "app_secret",
            )
            .map_err(anyhow::Error::msg)?;
            let effective =
                tinychannels::providers::yuanbao::connect::build_effective_yuanbao_config(
                    config.yuanbao.clone().unwrap_or_default(),
                    credential_fields,
                    app_key.to_owned(),
                );
            self.yuanbao
                .verify(&effective, &app_secret, &self.proxy)
                .await?;
        }
        let definition = tinychannels::controllers::find_channel_definition(channel)
            .ok_or_else(|| anyhow::anyhow!("unknown channel: {channel}"))?;
        let auth = definition
            .auth_mode_spec(auth_mode)
            .ok_or_else(|| anyhow::anyhow!("unsupported channel authentication mode"))?;
        if let Some(action) = auth.auth_action {
            return Ok(ChannelConnectionResult {
                status: "pending_auth".to_owned(),
                restart_required: false,
                auth_action: Some(action.to_owned()),
                message: Some(format!(
                    "Initiate '{action}' auth flow on the frontend. Ignore if you are already in the auth flow."
                )),
            });
        }
        let message = if channel == "imessage" && auth_mode == ChannelAuthMode::ManagedDm {
            "iMessage channel configured. Grant Full Disk Access and restart the service to activate."
                .to_owned()
        } else {
            format!("Channel '{channel}' credentials stored. Restart the service to activate.")
        };
        Ok(ChannelConnectionResult {
            status: "connected".to_owned(),
            restart_required: true,
            auth_action: None,
            message: Some(message),
        })
    }

    async fn disconnect_channel(
        &self,
        _config: &ChannelsConfig,
        channel: &str,
        auth_mode: ChannelAuthMode,
        clear_memory: bool,
    ) -> anyhow::Result<ChannelDisconnectResult> {
        match self
            .invoke(ChannelBackendRequest::Disconnect {
                channel: channel.to_owned(),
                auth_mode,
                clear_memory,
            })
            .await?
        {
            ChannelBackendResult::Disconnected(result) => Ok(result),
            _ => Err(unexpected_result("disconnect")),
        }
    }

    async fn channel_status(
        &self,
        _config: &ChannelsConfig,
        channel: Option<&str>,
    ) -> anyhow::Result<Vec<ChannelStatusEntry>> {
        match self
            .invoke(ChannelBackendRequest::Status {
                channel: channel.map(str::to_owned),
            })
            .await?
        {
            ChannelBackendResult::Status(result) => Ok(result),
            _ => Err(unexpected_result("status")),
        }
    }

    async fn test_channel(
        &self,
        _config: &ChannelsConfig,
        channel: &str,
        auth_mode: ChannelAuthMode,
        credentials: Value,
    ) -> anyhow::Result<ChannelTestResult> {
        if channel == "email" && auth_mode == ChannelAuthMode::ApiKey {
            let credentials = credentials
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("credentials must be a JSON object"))?;
            let email = tinychannels::controllers::build_email_config(credentials, None)
                .map_err(anyhow::Error::msg)?;
            self.email.verify(&email).await?;
            return Ok(ChannelTestResult {
                success: true,
                message: "IMAP login succeeded.".to_owned(),
            });
        }
        Ok(ChannelTestResult {
            success: true,
            message: format!("Credentials for '{channel}' ({auth_mode}) are structurally valid."),
        })
    }

    async fn send_outbound_intent(
        &self,
        _config: &ChannelsConfig,
        _intent: ChannelOutboundIntent,
    ) -> anyhow::Result<ChannelSendMessageResult> {
        Err(anyhow::anyhow!(
            "message sends use the module provider path"
        ))
    }

    async fn send_reaction(
        &self,
        config: &ChannelsConfig,
        channel: &str,
        reaction: Value,
    ) -> anyhow::Result<ChannelReactionResult> {
        if let Some((api, _)) = self.telegram_native(config, channel)? {
            let request =
                tinychannels::providers::telegram::TelegramReactionRequest::from_value(&reaction)
                    .map_err(anyhow::Error::msg)?;
            api.send_reaction(&request).await?;
            return Ok(ChannelReactionResult {
                success: true,
                ..Default::default()
            });
        }
        if let Some(discord) = self.discord_native(config, channel)? {
            let request =
                tinychannels::providers::discord::DiscordReactionRequest::from_value(&reaction)
                    .map_err(anyhow::Error::msg)?;
            discord
                .controller_reaction(&request)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
            return Ok(ChannelReactionResult {
                success: true,
                ..Default::default()
            });
        }
        if let Some(slack) = self.slack_native(config, channel)? {
            let request =
                tinychannels::providers::discord::DiscordReactionRequest::from_value(&reaction)
                    .map_err(anyhow::Error::msg)?;
            let reaction = tinychannels_bus::channel::ChannelReaction {
                value: request.emoji.unwrap_or_default(),
                custom_emoji_id: request.custom_emoji_id,
            };
            tinychannels::channel::LegacyChannelReaction::send_reaction(
                &slack,
                &tinychannels_bus::channel::ChannelRef {
                    id: "slack".into(),
                    account_id: None,
                },
                &tinychannels_bus::channel::ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Channel,
                    id: request.channel_id,
                    ..Default::default()
                },
                &request.message_id,
                &reaction,
                request.remove,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?;
            return Ok(ChannelReactionResult {
                success: true,
                ..Default::default()
            });
        }
        if let Some(mattermost) = self.mattermost_native(config, channel)? {
            let request =
                tinychannels::providers::discord::DiscordReactionRequest::from_value(&reaction)
                    .map_err(anyhow::Error::msg)?;
            let reaction = tinychannels_bus::channel::ChannelReaction {
                value: request.emoji.unwrap_or_default(),
                custom_emoji_id: request.custom_emoji_id,
            };
            tinychannels::channel::LegacyChannelReaction::send_reaction(
                &mattermost,
                &tinychannels_bus::channel::ChannelRef {
                    id: "mattermost".into(),
                    account_id: None,
                },
                &tinychannels_bus::channel::ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Channel,
                    id: request.channel_id,
                    ..Default::default()
                },
                &request.message_id,
                &reaction,
                request.remove,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?;
            return Ok(ChannelReactionResult {
                success: true,
                ..Default::default()
            });
        }
        Self::reject_unported(config, channel, "reaction")?;
        match self
            .invoke(ChannelBackendRequest::SendReaction {
                channel: channel.to_owned(),
                reaction,
            })
            .await?
        {
            ChannelBackendResult::SendReaction(result) => Ok(result),
            _ => Err(unexpected_result("send reaction")),
        }
    }

    async fn create_thread(
        &self,
        config: &ChannelsConfig,
        channel: &str,
        title: &str,
    ) -> anyhow::Result<ChannelThreadResult> {
        if let Some((api, chat_id)) = self.telegram_native(config, channel)? {
            let chat_id = chat_id.ok_or_else(|| {
                anyhow::anyhow!("Telegram threads need a default chat_id in the channel config")
            })?;
            let (thread_id, name) = api.create_topic(&chat_id, title).await?;
            return Ok(ChannelThreadResult {
                thread_id,
                title: Some(name),
                raw: None,
            });
        }
        if let Some(discord) = self.discord_native(config, channel)? {
            let (thread_id, name) = discord
                .create_thread(title)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
            return Ok(ChannelThreadResult {
                thread_id,
                title: Some(name),
                raw: None,
            });
        }
        Self::reject_unported(config, channel, "thread create")?;
        match self
            .invoke(ChannelBackendRequest::CreateThread {
                channel: channel.to_owned(),
                title: title.to_owned(),
            })
            .await?
        {
            ChannelBackendResult::Thread(result) => Ok(result),
            _ => Err(unexpected_result("create thread")),
        }
    }

    async fn update_thread(
        &self,
        config: &ChannelsConfig,
        channel: &str,
        thread_id: &str,
        action: &str,
    ) -> anyhow::Result<ChannelThreadResult> {
        if let Some((api, chat_id)) = self.telegram_native(config, channel)? {
            let chat_id = chat_id.ok_or_else(|| {
                anyhow::anyhow!("Telegram threads need a default chat_id in the channel config")
            })?;
            let action = tinychannels::providers::telegram::TopicAction::parse(action)
                .map_err(anyhow::Error::msg)?;
            api.update_topic(&chat_id, thread_id, action).await?;
            return Ok(ChannelThreadResult {
                thread_id: thread_id.to_owned(),
                title: None,
                raw: None,
            });
        }
        if let Some(discord) = self.discord_native(config, channel)? {
            let action = tinychannels::providers::discord::ThreadAction::parse(action)
                .map_err(anyhow::Error::msg)?;
            discord
                .update_thread(thread_id, action)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
            return Ok(ChannelThreadResult {
                thread_id: thread_id.to_owned(),
                title: None,
                raw: None,
            });
        }
        Self::reject_unported(config, channel, "thread update")?;
        match self
            .invoke(ChannelBackendRequest::UpdateThread {
                channel: channel.to_owned(),
                thread_id: thread_id.to_owned(),
                action: action.to_owned(),
            })
            .await?
        {
            ChannelBackendResult::Thread(result) => Ok(result),
            _ => Err(unexpected_result("update thread")),
        }
    }

    async fn list_threads(
        &self,
        config: &ChannelsConfig,
        channel: &str,
        active: Option<bool>,
    ) -> anyhow::Result<ChannelThreadListResult> {
        if self.telegram_native(config, channel)?.is_some() {
            anyhow::bail!("Telegram's Bot API cannot list forum topics");
        }
        if let Some(discord) = self.discord_native(config, channel)? {
            let threads = discord
                .list_threads(active)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
            return Ok(ChannelThreadListResult { threads, raw: None });
        }
        Self::reject_unported(config, channel, "thread list")?;
        match self
            .invoke(ChannelBackendRequest::ListThreads {
                channel: channel.to_owned(),
                active,
            })
            .await?
        {
            ChannelBackendResult::Threads(result) => Ok(result),
            _ => Err(unexpected_result("list threads")),
        }
    }

    async fn telegram_login_start(
        &self,
        _config: &ChannelsConfig,
    ) -> anyhow::Result<TelegramLoginStartResult> {
        Err(anyhow::anyhow!("Telegram login is host-owned"))
    }

    async fn telegram_login_check(
        &self,
        _config: &ChannelsConfig,
        _link_token: &str,
    ) -> anyhow::Result<TelegramLoginCheckResult> {
        Err(anyhow::anyhow!("Telegram login is host-owned"))
    }

    async fn discord_link_start(
        &self,
        _config: &ChannelsConfig,
    ) -> anyhow::Result<DiscordLinkStartResult> {
        Err(anyhow::anyhow!("Discord linking is host-owned"))
    }

    async fn discord_link_check(
        &self,
        _config: &ChannelsConfig,
        _link_token: &str,
    ) -> anyhow::Result<DiscordLinkCheckResult> {
        Err(anyhow::anyhow!("Discord linking is host-owned"))
    }

    async fn discord_list_guilds(
        &self,
        _config: &ChannelsConfig,
    ) -> anyhow::Result<DiscordGuildListResult> {
        let token = self.discord_bot_token()?;
        let guilds = self.discord.list_guilds(token, &self.proxy).await?;
        let raw = serde_json::to_value(&guilds).ok();
        Ok(DiscordGuildListResult {
            guilds: guilds
                .into_iter()
                .map(|guild| {
                    let raw = serde_json::to_value(&guild).ok();
                    tinychannels_bus::controllers::DiscordGuildEntry {
                        id: guild.id,
                        name: guild.name,
                        raw,
                    }
                })
                .collect(),
            raw,
        })
    }

    async fn discord_list_channels(
        &self,
        _config: &ChannelsConfig,
        guild_id: &str,
    ) -> anyhow::Result<DiscordChannelListResult> {
        let token = self.discord_bot_token()?;
        let channels = self
            .discord
            .list_channels(token, guild_id, &self.proxy)
            .await?;
        let raw = serde_json::to_value(&channels).ok();
        Ok(DiscordChannelListResult {
            channels: channels
                .into_iter()
                .map(|channel| {
                    let raw = serde_json::to_value(&channel).ok();
                    tinychannels_bus::controllers::DiscordChannelEntry {
                        id: channel.id,
                        name: channel.name,
                        kind: Some("text".to_owned()),
                        raw,
                    }
                })
                .collect(),
            raw,
        })
    }

    async fn discord_check_permissions(
        &self,
        _config: &ChannelsConfig,
        guild_id: &str,
        channel_id: &str,
    ) -> anyhow::Result<DiscordPermissionCheckResult> {
        let token = self.discord_bot_token()?;
        let permissions = self
            .discord
            .check_permissions(token, guild_id, channel_id, &self.proxy)
            .await?;
        let raw = serde_json::to_value(&permissions).ok();
        Ok(DiscordPermissionCheckResult {
            can_send_messages: permissions.can_send_messages,
            missing_permissions: permissions.missing_permissions,
            raw,
        })
    }

    async fn set_default_channel(
        &self,
        _config: &ChannelsConfig,
        _channel: &str,
    ) -> anyhow::Result<()> {
        Err(anyhow::anyhow!(
            "default channel storage remains host-owned"
        ))
    }

    async fn get_default_channel(&self, config: &ChannelsConfig) -> anyhow::Result<Option<String>> {
        Ok(config.active_channel.clone())
    }
}

impl HostBackend {
    fn discord_bot_token(&self) -> anyhow::Result<&str> {
        self.discord_token
            .as_deref()
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| anyhow::anyhow!("Discord bot token is empty"))
    }
}

fn unexpected_result(operation: &str) -> anyhow::Error {
    anyhow::anyhow!("host returned an unexpected result for {operation}")
}

#[cfg(test)]
#[path = "controller_tests.rs"]
mod tests;
