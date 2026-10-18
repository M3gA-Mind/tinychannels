//! Controller operations for a bot-token Discord channel: reactions and
//! threads driven straight at the REST API, so the host backend is never
//! involved.

use super::channel::DiscordChannel;
use super::http::{Payload, pre_dispatch};
use super::inbound::is_snowflake;
use serde_json::{Value, json};
use tinychannels_bus::channel::{ChannelReaction, ChannelSendError, ConversationRef};
use tinychannels_bus::controllers::ChannelThreadEntry;

/// Thread-state change accepted by [`DiscordChannel::update_thread`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadAction {
    Archive,
    Unarchive,
    Lock,
    Unlock,
}

impl ThreadAction {
    /// Parse the controller `action` string.
    pub fn parse(action: &str) -> Result<Self, String> {
        match action.trim().to_ascii_lowercase().as_str() {
            "archive" | "close" | "closed" => Ok(Self::Archive),
            "unarchive" | "reopen" | "open" => Ok(Self::Unarchive),
            "lock" => Ok(Self::Lock),
            "unlock" => Ok(Self::Unlock),
            other => Err(format!("unsupported Discord thread action '{other}'")),
        }
    }

    fn body(self) -> Value {
        match self {
            Self::Archive => json!({"archived": true}),
            Self::Unarchive => json!({"archived": false}),
            Self::Lock => json!({"locked": true}),
            Self::Unlock => json!({"locked": false}),
        }
    }
}

/// Reaction request decoded from the controller's opaque JSON value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscordReactionRequest {
    pub channel_id: String,
    pub message_id: String,
    pub emoji: Option<String>,
    pub custom_emoji_id: Option<String>,
    pub remove: bool,
}

impl DiscordReactionRequest {
    /// Decode `{channelId|conversationId, messageId, emoji|value, customEmojiId, remove}`.
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
        let channel_id = text(&["channelId", "conversationId", "channel_id"])
            .ok_or("reaction requires channelId (or conversationId)")?;
        let message_id = text(&["messageId", "message_id"]).ok_or("reaction requires messageId")?;
        let emoji = text(&["emoji", "value"]);
        let custom_emoji_id = text(&["customEmojiId", "custom_emoji_id"]);
        let remove = value
            .get("remove")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if emoji.is_none() {
            return Err("reaction requires emoji".into());
        }
        Ok(Self {
            channel_id,
            message_id,
            emoji,
            custom_emoji_id,
            remove,
        })
    }
}

fn thread_entry(thread: &Value) -> Option<ChannelThreadEntry> {
    let thread_id = thread.get("id").and_then(Value::as_str)?.to_owned();
    Some(ChannelThreadEntry {
        thread_id,
        title: thread
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned),
        active: thread
            .pointer("/thread_metadata/archived")
            .and_then(Value::as_bool)
            .map(|archived| !archived),
        raw: Some(thread.clone()),
    })
}

impl DiscordChannel {
    /// Add or remove the bot's reaction on one message.
    pub async fn controller_reaction(
        &self,
        request: &DiscordReactionRequest,
    ) -> Result<(), ChannelSendError> {
        let reaction = ChannelReaction {
            value: request.emoji.clone().unwrap_or_default(),
            custom_emoji_id: request.custom_emoji_id.clone(),
        };
        let conversation = ConversationRef {
            id: request.channel_id.clone(),
            ..Default::default()
        };
        self.send_reaction_v2(
            &conversation,
            &request.message_id,
            &reaction,
            request.remove,
        )
        .await
    }

    /// Start a public thread under the configured channel; returns `(id, name)`.
    pub async fn create_thread(&self, title: &str) -> Result<(String, String), ChannelSendError> {
        let channel_id = self.configured_channel("create a thread")?;
        let name: String = title.trim().chars().take(100).collect();
        if name.is_empty() {
            return Err(pre_dispatch("Discord thread name is empty"));
        }
        let url = self.endpoint_url(&["channels", channel_id, "threads"])?;
        let created = self
            .call(
                reqwest::Method::POST,
                url,
                Payload::Json(json!({"name": name, "auto_archive_duration": 1440, "type": 11})),
                "create thread",
                false,
            )
            .await?;
        let id = created
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| is_snowflake(id))
            .ok_or_else(|| pre_dispatch("Discord create thread returned no thread id"))?;
        let name = created
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(&name)
            .to_owned();
        Ok((id.to_owned(), name))
    }

    /// Archive, unarchive, lock or unlock a thread.
    pub async fn update_thread(
        &self,
        thread_id: &str,
        action: ThreadAction,
    ) -> Result<(), ChannelSendError> {
        if !is_snowflake(thread_id) {
            return Err(pre_dispatch("Discord thread id must be numeric"));
        }
        let url = self.endpoint_url(&["channels", thread_id])?;
        self.call(
            reqwest::Method::PATCH,
            url,
            Payload::Json(action.body()),
            "update thread",
            true,
        )
        .await
        .map(|_| ())
    }

    /// List threads under the configured channel: active ones from the guild,
    /// archived public ones from the channel.
    pub async fn list_threads(
        &self,
        active: Option<bool>,
    ) -> Result<Vec<ChannelThreadEntry>, ChannelSendError> {
        let channel_id = self.configured_channel("list threads")?;
        let archived = active == Some(false);
        let url = if archived {
            self.endpoint_url(&["channels", channel_id, "threads", "archived", "public"])?
        } else {
            let guild_id = self
                .guild_id
                .as_deref()
                .filter(|id| is_snowflake(id))
                .ok_or_else(|| pre_dispatch("Discord listing active threads needs a guild_id"))?;
            self.endpoint_url(&["guilds", guild_id, "threads", "active"])?
        };
        let listing = self
            .call(
                reqwest::Method::GET,
                url,
                Payload::None,
                "list threads",
                true,
            )
            .await?;
        Ok(listing
            .get("threads")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|thread| {
                archived || thread.get("parent_id").and_then(Value::as_str) == Some(channel_id)
            })
            .filter_map(thread_entry)
            .collect())
    }

    fn configured_channel(&self, operation: &str) -> Result<&str, ChannelSendError> {
        self.channel_id
            .as_deref()
            .map(str::trim)
            .filter(|id| is_snowflake(id))
            .ok_or_else(|| {
                pre_dispatch(format!(
                    "Discord needs a configured channel_id to {operation}"
                ))
            })
    }
}

#[cfg(test)]
#[path = "bot_api_tests.rs"]
mod tests;
