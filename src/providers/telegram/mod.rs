//! Telegram channel — long-polls the Bot API for updates.
//!
//! This is the Telegram transport. Remote control and in-chat approvals are
//! provider-independent and live in [`crate::remote`] and
//! [`crate::approvals`]; the names below keep the older Telegram-specific
//! paths compiling.

mod attachments;
mod bot_api;
mod capabilities;
mod channel;
mod channel_core;
mod channel_ops;
mod channel_recv;
mod channel_send;
mod channel_types;
pub(crate) mod text;

pub use bot_api::{TelegramBotApi, TelegramReactionRequest, TopicAction};
pub use channel_types::TelegramChannel;
pub(crate) use text::strip_tool_call_tags;

/// Approval-context client id for Telegram turns: the channel name, as for
/// every channel.
pub const TELEGRAM_APPROVAL_CLIENT_ID: &str = "telegram";

pub use crate::approvals::format_approval_prompt;
pub use crate::remote::{
    RemoteCommand as TelegramRemoteCommand, SESSIONS_LIST_LIMIT, build_new_session_response,
    build_remote_help_response, build_status_response, format_session_line,
    parse_remote_command as parse_telegram_remote_command,
};

#[cfg(test)]
#[path = "fixtures_tests.rs"]
mod fixtures;
