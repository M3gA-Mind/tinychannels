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
use tinychannels::{ChannelsConfig, SendMessage};
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
        }
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
            let app_key = credential_fields
                .get("app_key")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing required app_key"))?;
            let app_secret = credential_fields
                .get("app_secret")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing required app_secret"))?;
            let effective =
                tinychannels::providers::yuanbao::connect::build_effective_yuanbao_config(
                    config.yuanbao.clone().unwrap_or_default(),
                    credential_fields,
                    app_key.to_owned(),
                );
            self.yuanbao
                .verify(&effective, app_secret, &self.proxy)
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

    async fn send_message(
        &self,
        _config: &ChannelsConfig,
        _channel: &str,
        _message: SendMessage,
    ) -> anyhow::Result<ChannelSendMessageResult> {
        Err(anyhow::anyhow!(
            "message sends use the module provider path"
        ))
    }

    async fn send_reaction(
        &self,
        _config: &ChannelsConfig,
        channel: &str,
        reaction: Value,
    ) -> anyhow::Result<ChannelReactionResult> {
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
        _config: &ChannelsConfig,
        channel: &str,
        title: &str,
    ) -> anyhow::Result<ChannelThreadResult> {
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
        _config: &ChannelsConfig,
        channel: &str,
        thread_id: &str,
        action: &str,
    ) -> anyhow::Result<ChannelThreadResult> {
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
        _config: &ChannelsConfig,
        channel: &str,
        active: Option<bool>,
    ) -> anyhow::Result<ChannelThreadListResult> {
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
