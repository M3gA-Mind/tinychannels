//! Runtime connection and credential validation helpers.
use crate::config::ChannelsConfig;
pub use tinychannels_bus::controllers::definitions::*;
/// Validate provider credentials against the declared schema.
pub trait ChannelDefinitionExt {
    fn validate_credentials(
        &self,
        mode: ChannelAuthMode,
        credentials: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), String>;
}
impl ChannelDefinitionExt for ChannelDefinition {
    /// Validate that `credentials` contains all required fields for `mode`.
    /// Returns `Ok(())` or an error listing missing fields.
    fn validate_credentials(
        &self,
        mode: ChannelAuthMode,
        credentials: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), String> {
        let spec = self.auth_mode_spec(mode).ok_or_else(|| {
            format!(
                "channel '{}' does not support auth mode '{}'",
                self.id, mode
            )
        })?;

        let missing: Vec<&str> = spec
            .fields
            .iter()
            .filter(|f| f.required)
            .filter(|f| {
                credentials
                    .get(f.key)
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|value| value.trim().is_empty())
            })
            .map(|f| f.key)
            .collect();

        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "missing required fields for {}.{}: {}",
                self.id,
                mode,
                missing.join(", ")
            ))
        }
    }
}
/// Whether the supplied channel/auth-mode is connected by runtime config.
pub fn channel_config_connected(
    channels: &ChannelsConfig,
    channel_id: &str,
    mode: ChannelAuthMode,
) -> bool {
    let configured: Vec<String> = [
        ("telegram", channels.telegram.is_some()),
        ("discord", channels.discord.is_some()),
        ("slack", channels.slack.is_some()),
        ("mattermost", channels.mattermost.is_some()),
        ("imessage", channels.imessage.is_some()),
        ("signal", channels.signal.is_some()),
        ("whatsapp", channels.whatsapp.is_some()),
        ("linq", channels.linq.is_some()),
        ("email", channels.email.is_some()),
        ("irc", channels.irc.is_some()),
        ("lark", channels.lark.is_some()),
        ("dingtalk", channels.dingtalk.is_some()),
        ("qq", channels.qq.is_some()),
        ("yuanbao", channels.yuanbao.is_some()),
    ]
    .into_iter()
    .filter(|(_, present)| *present)
    .map(|(name, _)| name.to_owned())
    .collect();
    channel_presence_connected(&configured, channel_id, mode)
}

/// Interpret configuration presence without accepting channel credentials.
pub fn channel_presence_connected(
    configured: &[String],
    channel_id: &str,
    mode: ChannelAuthMode,
) -> bool {
    let applicable = matches!(
        (channel_id, mode),
        ("telegram" | "discord", ChannelAuthMode::BotToken)
            | ("imessage", ChannelAuthMode::ManagedDm)
            | ("yuanbao", ChannelAuthMode::ApiKey)
            | (
                "slack"
                    | "mattermost"
                    | "signal"
                    | "whatsapp"
                    | "linq"
                    | "email"
                    | "irc"
                    | "lark"
                    | "dingtalk"
                    | "qq",
                _
            )
    );
    applicable && configured.iter().any(|name| name == channel_id)
}

#[cfg(test)]
#[path = "definitions_tests.rs"]
mod tests;
