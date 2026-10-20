//! Channel controller metadata and backend response types.

pub mod definitions;
pub mod discord;
pub mod requests;
pub mod schemas;
pub mod types;

pub use definitions::{
    AuthModeSpec, ChannelAuthMode, ChannelCapability, ChannelDefinition, FieldRequirement,
    all_channel_definitions, find_channel_definition,
};
pub use requests::{
    ChannelConnectRequest, ChannelCreateThreadRequest, ChannelDescribeRequest,
    ChannelDisconnectRequest, ChannelListThreadsRequest, ChannelSendMessageRequest,
    ChannelSendReactionRequest, ChannelStatusRequest, ChannelTestRequest,
    ChannelUpdateThreadRequest, DiscordCheckPermissionsRequest, DiscordListChannelsRequest,
};
pub use schemas::{
    ChannelControllerField, ChannelControllerFieldType, ChannelControllerSchema,
    all_channel_controller_schemas, channel_controller_schema,
};
pub use types::{
    ChannelAccountSnapshot, ChannelAccountState, ChannelConnectionResult, ChannelDisconnectResult,
    ChannelLastDisconnect, ChannelReactionResult, ChannelSendMessageResult, ChannelStatusEntry,
    ChannelTestResult, ChannelThreadEntry, ChannelThreadListResult, ChannelThreadResult,
    DiscordChannelEntry, DiscordChannelListResult, DiscordGuildEntry, DiscordGuildListResult,
    DiscordLinkCheckResult, DiscordLinkStartResult, DiscordPermissionCheckResult,
    TelegramLoginCheckResult, TelegramLoginStartResult,
};

pub use discord::{BotPermissionCheck, DiscordGuild, DiscordTextChannel};
