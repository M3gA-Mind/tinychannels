use super::api::DISCORD_API_BASE;
use crate::channel::LengthUnit;
use crate::text::{ChunkMode, TextChunkOptions, chunk_text_with_options};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Minimum spacing between streaming-draft edits (Discord allows roughly five
/// edits per five seconds per channel).
/// Discord channel — connects via Gateway WebSocket for real-time messages
pub struct DiscordChannel {
    pub(super) bot_token: String,
    pub(super) guild_id: Option<String>,
    pub(super) channel_id: Option<String>,
    pub(super) allowed_users: Vec<String>,
    pub(super) listen_to_bots: bool,
    pub(super) mention_only: bool,
    /// Cancellation for the single Gateway session owned by the typed adapter.
    pub(super) listener_cancel: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    http_client: reqwest::Client,
    pub(super) api_base: String,
    /// Host speech-to-text for inbound voice messages. `None` leaves the
    /// transcript `Unavailable`; the audio is still delivered.
    pub(super) transcriber: Option<Arc<dyn crate::host::Transcriber>>,
    /// Durable voice storage for voice messages in and out.
    pub(super) voice_media_store: Option<Arc<dyn crate::host::VoiceMediaStore>>,
    /// Scoped media storage for attachments in and out.
    pub(super) media_repository: Option<Arc<dyn crate::host::MediaRepository>>,
    /// TinyVoice codec access used for voice duration and waveform.
    pub(super) voice_codec: Option<Arc<dyn crate::host::VoiceCodec>>,
    pub(super) voice_synthesis: bool,
    /// Module ingress and event sink for provider-specific events.
    pub(super) events: Option<Arc<dyn crate::host::EventSink>>,
    /// Channel id -> thread parent, filled from gateway events and REST lookups.
    pub(super) channel_cache: Mutex<HashMap<String, super::inbound::ChannelInfo>>,
}

impl DiscordChannel {
    pub fn new(
        bot_token: String,
        guild_id: Option<String>,
        channel_id: Option<String>,
        allowed_users: Vec<String>,
        listen_to_bots: bool,
        mention_only: bool,
    ) -> Self {
        Self {
            bot_token,
            guild_id,
            channel_id,
            allowed_users,
            listen_to_bots,
            mention_only,
            listener_cancel: Mutex::new(None),
            http_client: reqwest::Client::new(),
            api_base: DISCORD_API_BASE.to_string(),
            transcriber: None,
            voice_media_store: None,
            media_repository: None,
            voice_codec: None,
            voice_synthesis: false,
            events: None,
            channel_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_http_client(
        bot_token: String,
        guild_id: Option<String>,
        channel_id: Option<String>,
        allowed_users: Vec<String>,
        listen_to_bots: bool,
        mention_only: bool,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            bot_token,
            guild_id,
            channel_id,
            allowed_users,
            listen_to_bots,
            mention_only,
            listener_cancel: Mutex::new(None),
            http_client,
            api_base: DISCORD_API_BASE.to_string(),
            transcriber: None,
            voice_media_store: None,
            media_repository: None,
            voice_codec: None,
            voice_synthesis: false,
            events: None,
            channel_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Override the Discord REST base (default `https://discord.com/api/v10`),
    /// e.g. to route through a proxy or a local mock. Trailing slashes are
    /// trimmed.
    #[must_use]
    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = api_base.into().trim_end_matches('/').to_string();
        self
    }

    /// Attach the host speech-to-text capability for inbound voice messages.
    #[must_use]
    pub fn with_transcriber(mut self, transcriber: Arc<dyn crate::host::Transcriber>) -> Self {
        self.transcriber = Some(transcriber);
        self
    }

    /// Attach durable voice storage (voice messages in and out).
    #[must_use]
    pub fn with_voice_media_store(mut self, store: Arc<dyn crate::host::VoiceMediaStore>) -> Self {
        self.voice_media_store = Some(store);
        self
    }

    /// Attach scoped media storage (attachments in and out).
    #[must_use]
    pub fn with_media_repository(
        mut self,
        repository: Arc<dyn crate::host::MediaRepository>,
    ) -> Self {
        self.media_repository = Some(repository);
        self
    }

    /// Attach the TinyVoice codec used for voice duration and waveform.
    #[must_use]
    pub fn with_voice_codec(mut self, codec: Arc<dyn crate::host::VoiceCodec>) -> Self {
        self.voice_codec = Some(codec);
        self
    }

    /// Declare that the host can synthesize and encode spoken replies.
    #[must_use]
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.voice_synthesis = enabled;
        self
    }

    /// Attach the module ingress/event sink.
    #[must_use]
    pub fn with_events(mut self, events: Arc<dyn crate::host::EventSink>) -> Self {
        self.events = Some(events);
        self
    }

    pub(super) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    pub(crate) async fn health_status(&self) -> bool {
        self.http_client()
            .get(format!("{}/users/@me", self.api_base))
            .header("Authorization", format!("Bot {}", self.bot_token))
            .send()
            .await
            .map(|response| response.status().is_success())
            .unwrap_or(false)
    }

    /// Check if a Discord user ID is in the allowlist.
    ///
    /// Empty list ⇒ allow-all: an unconfigured allowlist applies no per-user
    /// restriction (the bot is still scoped to its configured guild/channel).
    /// Previously an empty list denied *everyone*, so a bot connected via the UI
    /// with the default-empty allowlist silently ignored every message and never
    /// replied (issue #3712). This now matches the WhatsApp provider's
    /// empty-⇒-allow-all convention. `"*"` also allows everyone; populate the
    /// list with specific user IDs to restrict.
    pub(super) fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.is_empty() || self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Decide whether a message passes the bot's guild scoping.
    ///
    /// - No configured guild → all messages pass (guild filter inactive).
    /// - Configured guild, message in a *different* guild → blocked.
    /// - Configured guild, message in that guild → allowed.
    /// - Configured guild, DM (no `guild_id`) → only when the allowlist is
    ///   non-empty (explicit). `is_user_allowed` treats an empty list as
    ///   allow-all (the intended *within-guild* default), but DMs bypass the
    ///   guild filter — so a blank allowlist must not open a guild-scoped bot to
    ///   arbitrary DMs (#3794 review — Codex P1). `"*"` (non-empty) still allows.
    pub(super) fn passes_guild_scope(
        configured_guild: Option<&str>,
        msg_guild: Option<&str>,
        allowlist_empty: bool,
    ) -> bool {
        let Some(gid) = configured_guild else {
            return true;
        };
        match msg_guild {
            Some(g) => g == gid,
            None => !allowlist_empty,
        }
    }

    /// Resolve the outbound recipient channel id. Prefer the message's explicit
    /// recipient (e.g. the channel a reply targets); fall back to the bot's
    /// configured `channel_id` for recipient-less sends such as proactive
    /// cron/heartbeat delivery (#3794 review — Codex P2). `None` when neither is
    /// available, so the caller surfaces an error instead of POSTing to an empty
    /// channel id.
    #[cfg(test)]
    pub(super) fn resolve_recipient<'a>(
        msg_recipient: &'a str,
        configured: Option<&'a str>,
    ) -> Option<&'a str> {
        let recipient = if msg_recipient.is_empty() {
            configured.unwrap_or("")
        } else {
            msg_recipient
        };
        (!recipient.is_empty()).then_some(recipient)
    }

    pub(super) fn bot_user_id_from_token(token: &str) -> Option<String> {
        // Discord bot tokens are base64(bot_user_id).timestamp.hmac
        let part = token.split('.').next()?;
        base64_decode(part)
    }
}

const BASE64_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Discord's maximum message length for regular messages.
///
/// Discord rejects longer payloads with `50035 Invalid Form Body`.
pub(super) const DISCORD_MAX_MESSAGE_LENGTH: usize = 2000;

/// Split a message into chunks that respect Discord's 2000-character limit.
/// Tries to split at word boundaries when possible.
pub(super) fn split_message_for_discord(message: &str) -> Vec<String> {
    if message.is_empty() {
        return vec![String::new()];
    }
    chunk_text_with_options(
        message,
        TextChunkOptions {
            limit: DISCORD_MAX_MESSAGE_LENGTH,
            length_unit: LengthUnit::Utf16Units,
            mode: ChunkMode::Length,
            markdown: true,
            indicators: false,
        },
    )
}

pub(super) fn mention_tags(bot_user_id: &str) -> [String; 2] {
    [format!("<@{bot_user_id}>"), format!("<@!{bot_user_id}>")]
}

pub(super) fn contains_bot_mention(content: &str, bot_user_id: &str) -> bool {
    let tags = mention_tags(bot_user_id);
    content.contains(&tags[0]) || content.contains(&tags[1])
}

pub(super) fn normalize_incoming_content(
    content: &str,
    mention_only: bool,
    bot_user_id: &str,
) -> Option<String> {
    if content.is_empty() {
        return None;
    }

    if mention_only && !contains_bot_mention(content, bot_user_id) {
        return None;
    }

    let mut normalized = content.to_string();
    if mention_only {
        for tag in mention_tags(bot_user_id) {
            normalized = normalized.replace(&tag, " ");
        }
    }

    let normalized = normalized.trim().to_string();
    if normalized.is_empty() {
        return None;
    }

    Some(normalized)
}

/// Minimal base64 decode (no extra dep) — only needs to decode the user ID portion
#[allow(clippy::cast_possible_truncation)]
pub(super) fn base64_decode(input: &str) -> Option<String> {
    let padded = match input.len() % 4 {
        2 => format!("{input}=="),
        3 => format!("{input}="),
        _ => input.to_string(),
    };

    let mut bytes = Vec::new();
    let chars: Vec<u8> = padded.bytes().collect();

    for chunk in chars.chunks(4) {
        if chunk.len() < 4 {
            break;
        }

        let mut v = [0usize; 4];
        for (i, &b) in chunk.iter().enumerate() {
            if b == b'=' {
                v[i] = 0;
            } else {
                v[i] = BASE64_ALPHABET.iter().position(|&a| a == b)?;
            }
        }

        bytes.push(((v[0] << 2) | (v[1] >> 4)) as u8);
        if chunk[2] != b'=' {
            bytes.push((((v[1] & 0xF) << 4) | (v[2] >> 2)) as u8);
        }
        if chunk[3] != b'=' {
            bytes.push((((v[2] & 0x3) << 6) | v[3]) as u8);
        }
    }

    String::from_utf8(bytes).ok()
}

#[cfg(test)]
#[path = "channel_tests.rs"]
mod tests;
