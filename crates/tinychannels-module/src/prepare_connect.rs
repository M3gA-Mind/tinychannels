//! Provider configuration preparation after module-owned connection verification.
use serde_json::Value;
use tinychannels::controllers::{build_email_config, parse_allowed_users, parse_optional_bool};
use tinychannels_bus::config::{DiscordConfig, IMessageConfig, TelegramConfig};
use tinychannels_bus::module_controller::ChannelConfigUpdate;
use tinychannels_bus::{ChannelAuthMode, ChannelsConfig};

pub(crate) fn config_update(
    config: &ChannelsConfig,
    channel: &str,
    auth_mode: ChannelAuthMode,
    credentials: &Value,
) -> anyhow::Result<Option<ChannelConfigUpdate>> {
    let fields = credentials
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("credentials must be a JSON object"))?;
    let optional = |name: &str| {
        fields
            .get(name)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let update = match (channel, auth_mode) {
        ("telegram", ChannelAuthMode::BotToken) => {
            let existing = config.telegram.as_ref();
            ChannelConfigUpdate::Telegram(TelegramConfig {
                bot_token: optional("bot_token")
                    .ok_or_else(|| anyhow::anyhow!("missing required bot_token"))?,
                chat_id: optional("chat_id"),
                allowed_users: parse_allowed_users(fields.get("allowed_users")),
                stream_mode: existing.map(|c| c.stream_mode).unwrap_or_default(),
                draft_update_interval_ms: existing
                    .map(|c| c.draft_update_interval_ms)
                    .unwrap_or(1000),
                silent_streaming: existing.map(|c| c.silent_streaming).unwrap_or(true),
                mention_only: existing.map(|c| c.mention_only).unwrap_or(false),
            })
        }
        ("discord", ChannelAuthMode::BotToken) => {
            let existing = config.discord.as_ref();
            ChannelConfigUpdate::Discord(DiscordConfig {
                bot_token: optional("bot_token")
                    .ok_or_else(|| anyhow::anyhow!("missing required bot_token"))?,
                guild_id: optional("guild_id"),
                channel_id: optional("channel_id"),
                allowed_users: match fields.get("allowed_users") {
                    Some(value) => parse_allowed_users(Some(value)),
                    None => existing
                        .map(|c| c.allowed_users.clone())
                        .unwrap_or_default(),
                },
                listen_to_bots: parse_optional_bool(fields.get("listen_to_bots"))
                    .unwrap_or_else(|| existing.is_some_and(|c| c.listen_to_bots)),
                mention_only: parse_optional_bool(fields.get("mention_only"))
                    .unwrap_or_else(|| existing.is_some_and(|c| c.mention_only)),
            })
        }
        ("imessage", ChannelAuthMode::ManagedDm) => ChannelConfigUpdate::IMessage(IMessageConfig {
            allowed_contacts: parse_allowed_users(fields.get("allowed_contacts")),
        }),
        ("email", ChannelAuthMode::ApiKey) => {
            let mut email =
                build_email_config(fields, config.email.as_ref()).map_err(anyhow::Error::msg)?;
            email.password.clear();
            ChannelConfigUpdate::Email(email)
        }
        ("yuanbao", ChannelAuthMode::ApiKey) => {
            let key =
                tinychannels::providers::yuanbao::connect::require_yuanbao_field(fields, "app_key")
                    .map_err(anyhow::Error::msg)?;
            let yuanbao = tinychannels::providers::yuanbao::connect::build_effective_yuanbao_config(
                config.yuanbao.clone().unwrap_or_default(),
                fields,
                key,
            );
            ChannelConfigUpdate::Yuanbao(Box::new(yuanbao))
        }
        _ => return Ok(None),
    };
    Ok(Some(update))
}
