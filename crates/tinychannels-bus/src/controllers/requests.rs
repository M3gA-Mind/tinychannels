//! Typed parameter values for the public channel controller operations.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::ChannelAuthMode;

/// Parameters accepted by `channels.describe`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDescribeRequest {
    pub channel: String,
}

/// Parameters accepted by `channels.connect`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelConnectRequest {
    pub channel: String,
    pub auth_mode: ChannelAuthMode,
    #[serde(default)]
    pub credentials: Option<Value>,
}

/// Parameters accepted by `channels.disconnect`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDisconnectRequest {
    pub channel: String,
    pub auth_mode: ChannelAuthMode,
    #[serde(default)]
    pub clear_memory: bool,
}

/// Parameters accepted by `channels.status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelStatusRequest {
    #[serde(default)]
    pub channel: Option<String>,
}

/// Parameters accepted by `channels.set_default`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSetDefaultRequest {
    pub channel: String,
}

/// Parameters accepted by `channels.test`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelTestRequest {
    pub channel: String,
    pub auth_mode: ChannelAuthMode,
    pub credentials: Value,
}

/// Parameters accepted by `channels.discord_list_channels`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscordListChannelsRequest {
    pub guild_id: String,
}

/// Parameters accepted by `channels.discord_check_permissions`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiscordCheckPermissionsRequest {
    pub guild_id: String,
    pub channel_id: String,
}

/// Parameters accepted by `channels.send_message`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSendMessageRequest {
    pub channel: String,
    pub message: Value,
}

/// Parameters accepted by `channels.send_reaction`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSendReactionRequest {
    pub channel: String,
    pub reaction: Value,
}

/// Parameters accepted by `channels.create_thread`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelCreateThreadRequest {
    pub channel: String,
    pub title: String,
}

/// Parameters accepted by `channels.update_thread`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelUpdateThreadRequest {
    pub channel: String,
    pub thread_id: String,
    pub action: String,
}

/// Parameters accepted by `channels.list_threads`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChannelListThreadsRequest {
    pub channel: String,
    #[serde(default)]
    pub active: Option<bool>,
}
