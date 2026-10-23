//! Portable channel provider implementations.

pub mod dingtalk;
pub mod discord;
#[cfg(feature = "email-send")]
pub mod email_channel;
#[cfg(feature = "email")]
pub mod email_verify;
#[cfg(feature = "imessage")]
pub mod imessage;
pub mod irc;
#[cfg(feature = "lark")]
pub mod lark;
pub mod linq;
pub mod long_tail;
pub mod mail;
pub mod mattermost;
pub(crate) mod native;
pub mod qq;
pub mod signal;
pub mod slack;
pub mod telegram;
pub mod whatsapp;
pub(crate) mod whatsapp_shared;
pub mod whatsapp_web;
pub mod yuanbao;

pub use dingtalk::DingTalkChannel;
pub use discord::DiscordChannel;
#[cfg(feature = "email-send")]
pub use email_channel::EmailChannel;
#[cfg(feature = "email")]
pub use email_verify::verify_email_credentials;
#[cfg(feature = "imessage")]
pub use imessage::IMessageChannel;
pub use irc::{IrcChannel, IrcChannelConfig};
#[cfg(feature = "lark")]
pub use lark::LarkChannel;
pub use linq::{LinqChannel, verify_linq_signature};
pub use mattermost::MattermostChannel;
pub use qq::QQChannel;
pub use signal::SignalChannel;
pub use slack::SlackChannel;
pub use telegram::TelegramChannel;
pub use tinychannels_runtime::CliChannel;
pub use whatsapp::WhatsAppChannel;
pub use whatsapp_web::WhatsAppWebChannel;
pub use yuanbao::YuanbaoChannel;

#[cfg(test)]
#[path = "http_test_support_tests.rs"]
mod http_test_support;

#[cfg(test)]
#[path = "no_text_markers_tests.rs"]
mod no_text_markers;
