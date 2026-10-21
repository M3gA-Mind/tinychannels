use super::IrcChannel;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Configuration for constructing an `IrcChannel`.
pub struct IrcChannelConfig {
    pub server: String,
    pub port: u16,
    pub nickname: String,
    pub username: Option<String>,
    pub channels: Vec<String>,
    pub allowed_users: Vec<String>,
    pub server_password: Option<String>,
    pub nickserv_password: Option<String>,
    pub sasl_password: Option<String>,
    pub verify_tls: bool,
}

impl IrcChannel {
    pub fn new(cfg: IrcChannelConfig) -> Self {
        let username = cfg.username.unwrap_or_else(|| cfg.nickname.clone());
        Self {
            server: cfg.server,
            port: cfg.port,
            nickname: cfg.nickname,
            username,
            channels: cfg.channels,
            allowed_users: cfg.allowed_users,
            server_password: cfg.server_password,
            nickserv_password: cfg.nickserv_password,
            sasl_password: cfg.sasl_password,
            verify_tls: cfg.verify_tls,
            writer: Arc::new(Mutex::new(None)),
            listener_cancel: std::sync::Mutex::new(None),
        }
    }
}
