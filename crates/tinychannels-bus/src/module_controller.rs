//! Typed channel controller operations dispatched inside the module.
use crate::ChannelsConfig;
use crate::controllers::{
    ChannelAuthMode, ChannelConnectionResult, ChannelDisconnectResult, ChannelReactionResult,
    ChannelStatusEntry, ChannelTestResult, ChannelThreadListResult, ChannelThreadResult,
    DiscordChannelListResult, DiscordGuildListResult, DiscordPermissionCheckResult,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A channel controller operation dispatched through TinyChannels' own
/// `tinychannels::ChannelManager` inside the loaded module.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum ChannelControllerRequest {
    /// Connect one channel after validating credentials in the module.
    Connect {
        config: ChannelsConfig,
        channel: String,
        auth_mode: ChannelAuthMode,
        credentials: Value,
        /// Host-selected HTTP proxy policy, forwarded only over this
        /// confidential host-to-module operation.
        #[serde(default)]
        proxy: crate::HttpProxyPolicy,
    },
    /// Validate and verify a connection, returning its provider configuration
    /// for host-owned persistence. The existing Connect operation is unchanged.
    PrepareConnect {
        config: ChannelsConfig,
        channel: String,
        auth_mode: ChannelAuthMode,
        credentials: Value,
        #[serde(default)]
        proxy: crate::HttpProxyPolicy,
    },
    /// Disconnect one channel through the configured host backend.
    Disconnect {
        config: ChannelsConfig,
        channel: String,
        auth_mode: ChannelAuthMode,
        clear_memory: bool,
    },
    /// Read status for one channel or the configured channel set.
    Status {
        config: ChannelsConfig,
        channel: Option<String>,
    },
    /// Validate credentials in TinyChannels before asking the backend to test them.
    TestChannel {
        config: ChannelsConfig,
        channel: String,
        auth_mode: ChannelAuthMode,
        credentials: Value,
    },
    /// Send a reaction through the host's channel adapter.
    SendReaction {
        config: ChannelsConfig,
        channel: String,
        reaction: Value,
    },
    /// Create one provider thread.
    CreateThread {
        config: ChannelsConfig,
        channel: String,
        title: String,
    },
    /// Update or close one provider thread.
    UpdateThread {
        config: ChannelsConfig,
        channel: String,
        thread_id: String,
        action: String,
    },
    /// List provider threads with an optional active-state filter.
    ListThreads {
        config: ChannelsConfig,
        channel: String,
        active: Option<bool>,
    },
    /// List Discord guilds available to the configured host identity.
    DiscordListGuilds {
        config: ChannelsConfig,
        /// Host-hydrated bot credential; this request is confidential.
        bot_token: String,
        /// Host-selected proxy policy for Discord API requests.
        #[serde(default)]
        proxy: crate::HttpProxyPolicy,
    },
    /// List Discord channels in one guild.
    DiscordListChannels {
        config: ChannelsConfig,
        guild_id: String,
        /// Host-hydrated bot credential; this request is confidential.
        bot_token: String,
        /// Host-selected proxy policy for Discord API requests.
        #[serde(default)]
        proxy: crate::HttpProxyPolicy,
    },
    /// Check the configured bot's permissions in one Discord channel.
    DiscordCheckPermissions {
        config: ChannelsConfig,
        guild_id: String,
        channel_id: String,
        /// Host-hydrated bot credential; this request is confidential.
        bot_token: String,
        /// Host-selected proxy policy for Discord API requests.
        #[serde(default)]
        proxy: crate::HttpProxyPolicy,
    },
}

/// Typed result from a module-dispatched channel controller operation.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ChannelControllerResult {
    /// Result for a Connect request.
    Connected(ChannelConnectionResult),
    /// Verified result and provider configuration for host persistence.
    PreparedConnect(Box<PreparedChannelConnection>),
    /// Result for a Disconnect request.
    Disconnected(ChannelDisconnectResult),
    /// Result for a Status request.
    Status(Vec<ChannelStatusEntry>),
    /// Result for a TestChannel request.
    TestChannel(ChannelTestResult),
    /// Result for a SendReaction request.
    SendReaction(ChannelReactionResult),
    /// Result for a CreateThread or UpdateThread request.
    Thread(ChannelThreadResult),
    /// Result for a ListThreads request.
    Threads(ChannelThreadListResult),
    /// Result for a DiscordListGuilds request.
    DiscordGuilds(DiscordGuildListResult),
    /// Result for a DiscordListChannels request.
    DiscordChannels(DiscordChannelListResult),
    /// Result for a DiscordCheckPermissions request.
    DiscordPermissions(DiscordPermissionCheckResult),
}

/// Narrow host-owned capabilities needed by the module's controller adapter.
/// Provider validation and protocol calls stay in TinyChannels; the host only
/// supplies config persistence and managed backend operations.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum ChannelBackendRequest {
    /// Access only host-owned conversations, history, and session caches.
    RemoteControl(crate::module_remote::RemoteHostRequest),
    /// Execute only managed transport; module owns progressive state and idempotency.
    ProgressiveTransport(Box<crate::module_progressive::ProgressiveTransportRequest>),
    /// Disconnect through the host credential/configuration store.
    Disconnect {
        channel: String,
        auth_mode: ChannelAuthMode,
        clear_memory: bool,
    },
    /// Read status maintained by the host's credentials and configuration.
    Status { channel: Option<String> },
    /// Run the managed channel reaction endpoint.
    SendReaction { channel: String, reaction: Value },
    /// Run a managed channel thread operation.
    CreateThread { channel: String, title: String },
    /// Update or close a managed channel thread.
    UpdateThread {
        channel: String,
        thread_id: String,
        action: String,
    },
    /// List managed channel threads.
    ListThreads {
        channel: String,
        active: Option<bool>,
    },
}

/// Results from narrow host-owned controller capabilities.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ChannelBackendResult {
    RemoteControl(crate::module_remote::RemoteHostResult),
    ProgressiveTransport(crate::module_progressive::ProgressiveTransportResult),
    Disconnected(ChannelDisconnectResult),
    Status(Vec<ChannelStatusEntry>),
    SendReaction(ChannelReactionResult),
    Thread(ChannelThreadResult),
    Threads(ChannelThreadListResult),
}

/// Verified connection facts. The host retains credential custody and decides
/// whether and where to persist the returned provider configuration.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct PreparedChannelConnection {
    pub result: ChannelConnectionResult,
    pub config_update: Option<ChannelConfigUpdate>,
}

/// A single provider configuration prepared by the owning module.
/// Email passwords and Yuanbao application secrets are deliberately absent.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "provider", content = "config", rename_all = "snake_case")]
pub enum ChannelConfigUpdate {
    Telegram(crate::config::TelegramConfig),
    Discord(crate::config::DiscordConfig),
    IMessage(crate::config::IMessageConfig),
    Email(crate::config::EmailConfig),
    Yuanbao(Box<crate::config::YuanbaoConfig>),
}
