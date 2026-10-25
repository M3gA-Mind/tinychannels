//! Provider-owned Telegram Bot API calls for the channel controllers.
//!
//! Reactions and forum-topic threads are driven straight at the Bot API with
//! the configured bot token, so a bot-token Telegram channel never needs the
//! host backend. `ChannelBackendRequest` stays only for the managed
//! (TinyHumans-hosted bot) transport.

use super::channel_core::resolve_api_base;
use serde_json::{Value, json};

/// A thread-state change accepted by [`TelegramBotApi::update_topic`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicAction {
    Close,
    Reopen,
}

impl TopicAction {
    /// Parse the controller `action` string (`close` / `reopen`).
    pub fn parse(action: &str) -> Result<Self, String> {
        match action.trim().to_ascii_lowercase().as_str() {
            "close" | "closed" | "archive" => Ok(Self::Close),
            "reopen" | "open" | "unarchive" => Ok(Self::Reopen),
            other => Err(format!("unsupported Telegram thread action '{other}'")),
        }
    }

    fn method(self) -> &'static str {
        match self {
            Self::Close => "closeForumTopic",
            Self::Reopen => "reopenForumTopic",
        }
    }
}

/// Reaction request decoded from the controller's opaque JSON value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramReactionRequest {
    pub chat_id: String,
    pub message_id: i64,
    pub emoji: Option<String>,
    pub custom_emoji_id: Option<String>,
    pub remove: bool,
}

impl TelegramReactionRequest {
    /// Decode `{chatId|conversationId, messageId, emoji|value, customEmojiId, remove}`.
    pub fn from_value(value: &Value) -> Result<Self, String> {
        let text = |keys: &[&str]| {
            keys.iter()
                .filter_map(|key| value.get(*key))
                .find_map(|field| match field {
                    Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
                    Value::Number(number) => Some(number.to_string()),
                    _ => None,
                })
        };
        let chat_id = text(&["chatId", "conversationId", "chat_id"])
            .ok_or("reaction requires chatId (or conversationId)")?;
        let message_id = text(&["messageId", "message_id"])
            .ok_or("reaction requires messageId")?
            .parse::<i64>()
            .map_err(|_| "reaction messageId must be a Telegram message number".to_owned())?;
        let emoji = text(&["emoji", "value"]);
        let custom_emoji_id = text(&["customEmojiId", "custom_emoji_id"]);
        let remove = value
            .get("remove")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !remove && emoji.is_none() && custom_emoji_id.is_none() {
            return Err("reaction requires emoji or customEmojiId".into());
        }
        Ok(Self {
            chat_id,
            message_id,
            emoji,
            custom_emoji_id,
            remove,
        })
    }
}

/// Minimal bot-token Telegram client for controller operations.
pub struct TelegramBotApi {
    bot_token: String,
    api_base: String,
    client: reqwest::Client,
}

impl TelegramBotApi {
    #[must_use]
    pub fn new(bot_token: impl Into<String>, client: reqwest::Client) -> Self {
        Self {
            bot_token: bot_token.into(),
            api_base: resolve_api_base(
                std::env::var("OPENHUMAN_TELEGRAM_BOT_API_BASE")
                    .ok()
                    .or_else(|| std::env::var("OPENHUMAN_TELEGRAM_API_BASE").ok()),
            ),
            client,
        }
    }

    /// Point the client at another Bot API base (tests and self-hosted servers).
    #[must_use]
    pub fn with_api_base(mut self, base: impl Into<String>) -> Self {
        self.api_base = base.into().trim_end_matches('/').to_owned();
        self
    }

    async fn call(&self, method: &str, body: Value) -> anyhow::Result<Value> {
        tracing::debug!(target: "telegram::bot_api", method, "[telegram] controller bot api call");
        let response = self
            .client
            .post(format!("{}/bot{}/{method}", self.api_base, self.bot_token))
            .json(&body)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Telegram {method} request failed"))?;
        let result = response
            .json::<Value>()
            .await
            .map_err(|_| anyhow::anyhow!("Telegram {method} returned an unreadable response"))?;
        if result.get("ok").and_then(Value::as_bool) != Some(true) {
            anyhow::bail!(
                "Telegram {method} failed: {}",
                result["description"].as_str().unwrap_or("unknown error")
            );
        }
        Ok(result["result"].clone())
    }

    /// Set or clear the bot's reaction on one message.
    pub async fn send_reaction(&self, request: &TelegramReactionRequest) -> anyhow::Result<()> {
        let reaction = if request.remove {
            json!([])
        } else if let Some(id) = request.custom_emoji_id.as_deref() {
            json!([{"type": "custom_emoji", "custom_emoji_id": id}])
        } else {
            json!([{"type": "emoji", "emoji": request.emoji.as_deref().unwrap_or_default()}])
        };
        self.call(
            "setMessageReaction",
            json!({
                "chat_id": request.chat_id,
                "message_id": request.message_id,
                "reaction": reaction,
            }),
        )
        .await
        .map(|_| ())
    }

    /// Create a forum topic and return `(message_thread_id, name)`.
    pub async fn create_topic(
        &self,
        chat_id: &str,
        title: &str,
    ) -> anyhow::Result<(String, String)> {
        let result = self
            .call(
                "createForumTopic",
                json!({"chat_id": chat_id, "name": title}),
            )
            .await?;
        let thread_id = result["message_thread_id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("Telegram createForumTopic returned no thread id"))?;
        let name = result["name"].as_str().unwrap_or(title).to_owned();
        Ok((thread_id.to_string(), name))
    }

    /// Close or reopen a forum topic.
    pub async fn update_topic(
        &self,
        chat_id: &str,
        thread_id: &str,
        action: TopicAction,
    ) -> anyhow::Result<()> {
        let thread_id = thread_id
            .trim()
            .parse::<i64>()
            .map_err(|_| anyhow::anyhow!("Telegram thread id must be a number"))?;
        self.call(
            action.method(),
            json!({"chat_id": chat_id, "message_thread_id": thread_id}),
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
#[path = "bot_api_tests.rs"]
mod tests;
