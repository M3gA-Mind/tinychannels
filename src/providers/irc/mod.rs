//! IRC provider: configuration, inbound protocol, capabilities, and outbound text.

mod capabilities;
mod config;
mod inbound;
mod outbound;

pub use config::IrcChannelConfig;
#[cfg(test)]
use inbound::IRC_STYLE_PREFIX;
use inbound::ListenerRegistration;
#[cfg(any(test, debug_assertions))]
use inbound::{IrcMessage, encode_sasl_plain};
use outbound::split_message;

use std::sync::Arc;
use tokio::sync::Mutex;

/// IRC over TLS channel.
///
/// Connects to an IRC server using TLS, joins configured channels,
/// and forwards PRIVMSG messages to the `OpenHuman` message bus.
/// Supports both channel messages and private messages (DMs).
pub struct IrcChannel {
    server: String,
    port: u16,
    nickname: String,
    username: String,
    channels: Vec<String>,
    allowed_users: Vec<String>,
    server_password: Option<String>,
    nickserv_password: Option<String>,
    sasl_password: Option<String>,
    verify_tls: bool,
    /// Shared write half of the TLS stream for sending messages.
    writer: Arc<Mutex<Option<WriteHalf>>>,
    /// Cancellation for the single provider-owned native listener.
    listener_cancel: std::sync::Mutex<Option<tokio::sync::watch::Sender<bool>>>,
}

pub(super) type WriteHalf =
    tokio::io::WriteHalf<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "listen_tests.rs"]
mod listen_tests;

#[cfg(any(test, debug_assertions))]
#[path = "test_support_tests.rs"]
pub mod test_support;
