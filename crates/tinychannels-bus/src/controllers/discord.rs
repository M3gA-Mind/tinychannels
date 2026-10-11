//! Complete Discord setup metadata exchanged with the module.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Minimal guild (server) info returned by `GET /users/@me/guilds`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiscordGuild {
    pub id: String,
    pub name: String,
    pub icon: Option<String>,
}

/// Minimal channel info returned by `GET /guilds/{guild_id}/channels`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiscordTextChannel {
    pub id: String,
    pub name: String,
    /// Discord channel type — 0 = text, 2 = voice, 4 = category, etc.
    #[serde(rename = "type")]
    pub channel_type: u64,
    #[serde(default)]
    pub position: u64,
    /// Parent category ID (if nested under a category).
    pub parent_id: Option<String>,
}

/// Result of a bot permission check for a given channel.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BotPermissionCheck {
    pub can_view_channel: bool,
    pub can_send_messages: bool,
    pub can_read_message_history: bool,
    pub missing_permissions: Vec<String>,
}
