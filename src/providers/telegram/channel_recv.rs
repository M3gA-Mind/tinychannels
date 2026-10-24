//! Telegram channel — inbound message/reaction parsing, allowlist checks, mention filtering,
//! unauthorized-message handling, and typing-action helpers.

use super::channel_types::{
    APPROVAL_PROMPT_DEBOUNCE_SECS, TELEGRAM_MAX_VOICE_FILE_BYTES, TelegramChannel,
    TelegramReactionEvent, TelegramVoiceAttachment,
};
use crate::traits::{ChannelMessage, SendMessage};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) struct TelegramIncomingMessageContext {
    pub(crate) sender_identity: String,
    /// The sender's first and last name, else their username.
    pub(crate) sender_name: Option<String>,
    pub(crate) reply_target: String,
    pub(crate) chat_id: String,
    pub(crate) message_id: i64,
    pub(crate) mention_text: Option<String>,
}

pub(crate) struct TelegramDownloadedMedia {
    bytes: Vec<u8>,
    file_name: Option<String>,
    content_type: Option<String>,
}

struct TelegramMediaAttachment {
    kind: tinychannels_bus::MediaKind,
    file_id: String,
    unique_id: Option<String>,
    file_name: Option<String>,
    mime_hint: Option<String>,
    duration_ms: Option<u64>,
    field: &'static str,
}

impl TelegramChannel {
    fn update_media_attachment(update: &serde_json::Value) -> Option<TelegramMediaAttachment> {
        let message = update
            .get("message")
            .or_else(|| update.get("edited_message"))?;
        if let Some(photo) = message.get("photo").and_then(serde_json::Value::as_array) {
            let photo = photo.last()?;
            let file_id = photo.get("file_id")?.as_str()?.to_owned();
            return Some(TelegramMediaAttachment {
                kind: tinychannels_bus::MediaKind::Image,
                file_id,
                unique_id: photo
                    .get("file_unique_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                file_name: None,
                mime_hint: None,
                duration_ms: None,
                field: "photo",
            });
        }
        for (field, kind) in [
            ("document", tinychannels_bus::MediaKind::Document),
            ("video", tinychannels_bus::MediaKind::Video),
            ("audio", tinychannels_bus::MediaKind::Audio),
            ("sticker", tinychannels_bus::MediaKind::Image),
        ] {
            let Some(attachment) = message.get(field) else {
                continue;
            };
            let Some(file_id) = attachment
                .get("file_id")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            return Some(TelegramMediaAttachment {
                kind,
                file_id: file_id.to_owned(),
                unique_id: attachment
                    .get("file_unique_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                file_name: attachment
                    .get("file_name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                mime_hint: attachment
                    .get("mime_type")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                duration_ms: attachment
                    .get("duration")
                    .and_then(serde_json::Value::as_u64)
                    .map(|seconds| seconds.saturating_mul(1000)),
                field,
            });
        }
        None
    }

    pub(crate) async fn media_parts_for_ingress(
        &self,
        update: &serde_json::Value,
        message: &ChannelMessage,
    ) -> anyhow::Result<Vec<tinychannels_bus::ContentPart>> {
        let Some(TelegramMediaAttachment {
            kind,
            file_id,
            unique_id,
            file_name,
            mime_hint,
            duration_ms,
            field,
        }) = Self::update_media_attachment(update)
        else {
            return Ok(Vec::new());
        };
        let repository = self
            .media_repository
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("host media storage is unavailable"))?;
        let downloaded = self
            .download_telegram_media_file(&file_id, TELEGRAM_MAX_VOICE_FILE_BYTES)
            .await?;
        let source = update
            .get("message")
            .or_else(|| update.get("edited_message"))
            .unwrap_or(&serde_json::Value::Null);
        let chat = source.get("chat").unwrap_or(&serde_json::Value::Null);
        let chat_id = chat
            .get("id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
            .unwrap_or_else(|| {
                message
                    .reply_target
                    .split(':')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            });
        let topic_id = source
            .get("message_thread_id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());
        let scope = tinychannels_bus::HostVoiceScope {
            configured_instance: String::new(),
            channel: tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: None,
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: match chat.get("type").and_then(serde_json::Value::as_str) {
                    Some("private") => tinychannels_bus::ConversationKind::Dm,
                    Some("group" | "supergroup") => tinychannels_bus::ConversationKind::Group,
                    Some("channel") => tinychannels_bus::ConversationKind::Channel,
                    _ => tinychannels_bus::ConversationKind::Unknown,
                },
                id: chat_id,
                thread_id: topic_id.clone(),
                topic_id,
                ..Default::default()
            },
            turn_id: message.id.clone(),
        };
        let source_file_name = file_name;
        let file_name = source_file_name
            .clone()
            .or_else(|| downloaded.file_name.clone());
        let inferred_source_mime = source_file_name
            .as_deref()
            .and_then(super::attachments::infer_attachment_mime);
        let typed_mime_hint = mime_hint
            .as_deref()
            .filter(|mime| !mime.eq_ignore_ascii_case("application/octet-stream"));
        let hint = inferred_source_mime
            .or(typed_mime_hint)
            .or_else(|| {
                file_name
                    .as_deref()
                    .and_then(super::attachments::infer_attachment_mime)
            })
            .or(mime_hint.as_deref())
            .or(downloaded.content_type.as_deref())
            .unwrap_or("");
        if let Some(actual) = downloaded.content_type.as_deref()
            && actual != "application/octet-stream"
            && !actual.eq_ignore_ascii_case(hint)
        {
            anyhow::bail!("Telegram media response MIME does not match its declared type");
        }
        let dedupe_key = format!("telegram:{}:{field}", message.id);
        let media = repository
            .store_media(crate::host::MediaStoreRequest {
                metadata: crate::host::MediaMetadata {
                    scope,
                    kind,
                    content_type: hint.to_owned(),
                    file_name: file_name.clone(),
                    duration_ms,
                    codec: None,
                    voice_direction: None,
                    dedupe_key: Some(dedupe_key),
                    max_bytes: TELEGRAM_MAX_VOICE_FILE_BYTES,
                },
                bytes: downloaded.bytes,
            })
            .await?;
        let part = match kind {
            tinychannels_bus::MediaKind::Image if field == "sticker" => {
                tinychannels_bus::ContentPart::Sticker {
                    media,
                    pack_id: None,
                    sticker_id: unique_id,
                    emoji: source
                        .get("sticker")
                        .and_then(|sticker| sticker.get("emoji"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    alt_text: None,
                }
            }
            tinychannels_bus::MediaKind::Image => tinychannels_bus::ContentPart::Image {
                media,
                caption: None,
                alt_text: None,
            },
            tinychannels_bus::MediaKind::Video => tinychannels_bus::ContentPart::Video {
                media,
                caption: None,
                duration_ms,
                thumbnail: None,
            },
            tinychannels_bus::MediaKind::Audio => tinychannels_bus::ContentPart::Audio {
                media,
                codec: None,
                duration_ms,
                title: None,
                transcript: None,
            },
            tinychannels_bus::MediaKind::Document | tinychannels_bus::MediaKind::Unknown => {
                tinychannels_bus::ContentPart::File {
                    media,
                    file_name,
                    caption: None,
                }
            }
        };
        Ok(vec![part])
    }

    pub(crate) fn typing_body_for_recipient(recipient: &str) -> serde_json::Value {
        let (chat_id, thread_id) = Self::parse_reply_target(recipient);
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "action": "typing"
        });
        if let Some(thread_id) = thread_id {
            body["message_thread_id"] = serde_json::Value::String(thread_id);
        }
        body
    }

    pub(crate) async fn send_typing_action_once(&self, recipient: &str) -> anyhow::Result<bool> {
        tracing::info!(recipient, "Telegram typing action attempt");
        let body = Self::typing_body_for_recipient(recipient);
        let has_thread_id = body.get("message_thread_id").is_some();
        let send = |body: serde_json::Value| async move {
            let response = self
                .http_client()
                .post(self.api_url("sendChatAction"))
                .json(&body)
                .send()
                .await
                .map_err(|_| anyhow::Error::new(super::channel_ops::TelegramAttemptedError))?;
            let status = response.status();
            let value = match response.json::<serde_json::Value>().await {
                Ok(value) => value,
                Err(_) if status.is_client_error() => return Ok(false),
                Err(_) => {
                    return Err(anyhow::Error::new(
                        super::channel_ops::TelegramAttemptedError,
                    ));
                }
            };
            if status.is_server_error() {
                return Err(anyhow::Error::new(
                    super::channel_ops::TelegramAttemptedError,
                ));
            }
            Ok(status.is_success() && super::channel_ops::response_status_ok(&value))
        };
        let accepted = send(body).await?;
        if accepted {
            tracing::info!(recipient, "Telegram typing action sent");
            return Ok(true);
        }
        tracing::warn!(recipient, "Telegram typing action rejected");
        // Some chats reject thread-scoped actions; retry at chat scope once.
        if has_thread_id {
            let (chat_id, _) = Self::parse_reply_target(recipient);
            let fallback_body = serde_json::json!({ "chat_id": chat_id, "action": "typing" });
            return send(fallback_body).await;
        }
        Ok(false)
    }

    pub(crate) fn is_telegram_username_char(ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }

    pub(crate) fn find_bot_mention_spans(text: &str, bot_username: &str) -> Vec<(usize, usize)> {
        let bot_username = bot_username.trim_start_matches('@');
        if bot_username.is_empty() {
            return Vec::new();
        }

        let mut spans = Vec::new();

        for (at_idx, ch) in text.char_indices() {
            if ch != '@' {
                continue;
            }

            if at_idx > 0 {
                let prev = text[..at_idx].chars().next_back().unwrap_or(' ');
                if Self::is_telegram_username_char(prev) {
                    continue;
                }
            }

            let username_start = at_idx + 1;
            let mut username_end = username_start;

            for (rel_idx, candidate_ch) in text[username_start..].char_indices() {
                if Self::is_telegram_username_char(candidate_ch) {
                    username_end = username_start + rel_idx + candidate_ch.len_utf8();
                } else {
                    break;
                }
            }

            if username_end == username_start {
                continue;
            }

            let mention_username = &text[username_start..username_end];
            if mention_username.eq_ignore_ascii_case(bot_username) {
                spans.push((at_idx, username_end));
            }
        }

        spans
    }

    pub(crate) fn contains_bot_mention(text: &str, bot_username: &str) -> bool {
        !Self::find_bot_mention_spans(text, bot_username).is_empty()
    }

    pub(crate) fn normalize_incoming_content(text: &str, bot_username: &str) -> Option<String> {
        let spans = Self::find_bot_mention_spans(text, bot_username);
        if spans.is_empty() {
            let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
            return (!normalized.is_empty()).then_some(normalized);
        }

        let mut normalized = String::with_capacity(text.len());
        let mut cursor = 0;
        for (start, end) in spans {
            normalized.push_str(&text[cursor..start]);
            cursor = end;
        }
        normalized.push_str(&text[cursor..]);

        let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
        (!normalized.is_empty()).then_some(normalized)
    }

    pub(crate) fn is_group_message(message: &serde_json::Value) -> bool {
        message
            .get("chat")
            .and_then(|c| c.get("type"))
            .and_then(|t| t.as_str())
            .map(|t| t == "group" || t == "supergroup")
            .unwrap_or(false)
    }

    pub(crate) fn is_user_allowed(&self, username: &str) -> bool {
        let identity = Self::normalize_identity(username);
        self.allowed_users
            .read()
            .map(|users| {
                users
                    .iter()
                    .any(|u| u == "*" || u.eq_ignore_ascii_case(&identity))
            })
            .unwrap_or(false)
    }

    pub(crate) fn is_any_user_allowed<'a, I>(&self, identities: I) -> bool
    where
        I: IntoIterator<Item = &'a str>,
    {
        identities.into_iter().any(|id| self.is_user_allowed(id))
    }

    /// Check whether an approval prompt should be suppressed due to the restart-race
    /// condition signature: `pairing.is_none()` (channel was constructed with a non-empty
    /// allowlist) AND the runtime `allowed_users` list is currently empty.
    ///
    /// This happens when the replacement process reads its config allowlist, stores it in
    /// `allowed_users`, but the old process has not yet shut down — Telegram redelivers
    /// the update to both.  The racing instance has `pairing = None` (correct — allowlist
    /// was non-empty at construction) but the runtime list may briefly show as empty before
    /// the config is loaded.
    ///
    /// Legitimate first-run pairing (`allowed_users=[]` at construction) always sets
    /// `pairing = Some(...)` so it is never suppressed here.
    pub(crate) fn is_race_condition_instance(&self) -> bool {
        self.allowlist_is_empty() && self.pairing.is_none()
    }

    /// Whether the runtime allowlist currently has no entries. A poisoned lock is
    /// treated as non-empty (fail-closed) so we never widen access on a lock error.
    pub(crate) fn allowlist_is_empty(&self) -> bool {
        self.allowed_users
            .read()
            .map(|users| users.is_empty())
            .unwrap_or(false)
    }

    /// Build the de-bounce key for approval prompts: `"{chat_id}:{sender}"`.
    pub(crate) fn approval_debounce_key(chat_id: &str, sender: &str) -> String {
        format!("{chat_id}:{sender}")
    }

    /// Returns `true` if an approval prompt was already sent to this chat+sender within the
    /// de-bounce window, and updates the last-sent timestamp when returning `false`.
    pub(crate) fn check_and_update_approval_debounce(&self, chat_id: &str, sender: &str) -> bool {
        let key = Self::approval_debounce_key(chat_id, sender);
        let mut prompts = self.recent_approval_prompts.lock();
        if let Some(last_sent) = prompts.get(&key)
            && last_sent.elapsed().as_secs() < APPROVAL_PROMPT_DEBOUNCE_SECS
        {
            return true; // still within de-bounce window
        }
        // Evict entries older than the de-bounce window before inserting. Anything
        // past the window can never suppress again, so retaining it would let the
        // map grow without bound if the bot is exposed to a public group or spam
        // (review note on #1948). This caps the map to senders seen within the
        // last APPROVAL_PROMPT_DEBOUNCE_SECS.
        prompts
            .retain(|_, last_sent| last_sent.elapsed().as_secs() < APPROVAL_PROMPT_DEBOUNCE_SECS);
        prompts.insert(key, Instant::now());
        false
    }

    pub(crate) async fn handle_unauthorized_message(&self, update: &serde_json::Value) {
        let Some(message) = update.get("message") else {
            return;
        };

        if !Self::is_supported_unauthorized_message(message) {
            tracing::debug!("[telegram][approval] ignoring unsupported unauthorized update");
            return;
        }

        let text = message.get("text").and_then(serde_json::Value::as_str);

        let username_opt = message
            .get("from")
            .and_then(|from| from.get("username"))
            .and_then(serde_json::Value::as_str);
        let username = username_opt.unwrap_or("unknown");
        let normalized_username = Self::normalize_identity(username);

        let sender_id = message
            .get("from")
            .and_then(|from| from.get("id"))
            .and_then(serde_json::Value::as_i64);
        let sender_id_str = sender_id.map(|id| id.to_string());
        let normalized_sender_id = sender_id_str.as_deref().map(Self::normalize_identity);

        let chat_id = message
            .get("chat")
            .and_then(|chat| chat.get("id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());

        let Some(chat_id) = chat_id else {
            tracing::warn!("[telegram][approval] missing chat_id in message, skipping");
            return;
        };

        let mut identities = vec![normalized_username.as_str()];
        if let Some(ref id) = normalized_sender_id {
            identities.push(id.as_str());
        }

        if self.is_any_user_allowed(identities.iter().copied()) {
            tracing::debug!(
                chat_id,
                username,
                sender_id = sender_id_str.as_deref().unwrap_or("unknown"),
                "[telegram][approval] message sender is allowed — no action"
            );
            return;
        }

        // ── Race-condition guard ─────────────────────────────────────────────────
        // Signature: pairing.is_none() (channel constructed with non-empty allowlist)
        // AND runtime allowed_users is currently empty.  This means we are the racing
        // instance spawned during a restart whose config hasn't propagated yet.
        // Sending an approval prompt here would spam the allowlisted user with false
        // "operator approval required" messages.  Log and suppress instead.
        if self.is_race_condition_instance() {
            tracing::warn!(
                chat_id,
                username,
                sender_id = sender_id_str.as_deref().unwrap_or("unknown"),
                "[telegram][approval] race-condition guard: allowlist is empty at runtime \
                 but channel was constructed with a non-empty allowlist (pairing=None). \
                 Suppressing approval prompt — this is a restart-overlap false positive."
            );
            return;
        }

        // ── First-run onboarding: `/start` pairs the operator ────────────────────
        // On the self-bot-token path a blank allowlist arms `pairing = Some(..)` (a
        // fresh bot is world-reachable by @username, so we must not allow-all like
        // Discord). The one-time bind code, however, is only printed to core stdout
        // and is invisible to a desktop operator — leaving the gate un-openable and
        // every message stuck on the approval prompt (openhuman#4381).
        //
        // The operator's first `/start` is their explicit "I'm setting up my bot"
        // signal. While pairing is still pending we treat that sender as the owner,
        // add them to the allowlist, and let their subsequent messages reach the
        // agent — matching the "first sender after /start" behaviour the issue
        // sanctions. The guard is tight: `pairing.is_some()` excludes an
        // explicitly-configured allowlist, and `allowlist_is_empty()` restricts
        // onboarding to the genuine first sender — once the operator is bound the
        // list is non-empty, so a later stranger's `/start` falls through to the
        // normal approval prompt instead of being auto-approved.
        // SECURITY (first-sender-wins TOFU): unlike `/bind <code>` — which
        // requires the stdout secret and goes through `try_pair`'s lockout —
        // `/start` onboarding trusts the first sender with no secret and no
        // rate-limit. Anyone who learns the bot's `@username` before the operator
        // sends the first message could claim ownership. The private-chat guard
        // below removes the group attack surface (the common hijack); the residual
        // window is a stale, world-reachable un-paired bot. Bounding onboarding to
        // a startup time-window is a reasonable future hardening (see openhuman#4381).
        if self.pairing.is_some()
            && self.allowlist_is_empty()
            // Private chats only: operator setup for a self-bot-token is a DM
            // action. Onboarding the first `/start` sender in a *group* would let
            // any member claim operator ownership (the un-paired bot may be added
            // to a group mid-setup), so a group `/start` falls through to the
            // normal approval prompt instead.
            && !Self::is_group_message(message)
            && text.map(Self::is_start_command).unwrap_or(false)
        {
            match Self::bindable_identity(&normalized_username, normalized_sender_id.as_deref()) {
                Some(identity) => {
                    tracing::info!(
                        chat_id,
                        identity,
                        "[telegram][approval] /start onboarding: pairing first sender as operator"
                    );
                    self.approve_and_persist_sender(&identity, &chat_id).await;
                    // Finish the one-time pairing flow: the operator is bound
                    // via /start rather than /bind <code>, so consume the code
                    // here too — otherwise the stdout code stays live and a
                    // later sender who obtains it could still /bind themselves.
                    if let Some(pairing) = self.pairing.as_ref() {
                        pairing.invalidate_code();
                    }
                }
                None => {
                    let _ = self
                        .send_compat_message(&SendMessage::new(
                            "❌ Could not identify your Telegram account from /start. Ensure your account has a username or stable user ID, then try again.",
                            &chat_id,
                        ))
                        .await;
                }
            }
            return;
        }

        if let Some(code) = text.and_then(Self::extract_bind_code) {
            if let Some(pairing) = self.pairing.as_ref() {
                match pairing.try_pair(code).await {
                    Ok(Some(_token)) => {
                        match Self::bindable_identity(
                            &normalized_username,
                            normalized_sender_id.as_deref(),
                        ) {
                            Some(identity) => {
                                tracing::info!(
                                    chat_id,
                                    identity,
                                    "[telegram][approval] paired via bind code and allowlisted identity"
                                );
                                self.approve_and_persist_sender(&identity, &chat_id).await;
                            }
                            None => {
                                let _ = self
                                    .send_compat_message(&SendMessage::new(
                                        "❌ Could not identify your Telegram account. Ensure your account has a username or stable user ID, then retry.",
                                        &chat_id,
                                    ))
                                    .await;
                            }
                        }
                    }
                    Ok(None) => {
                        let _ = self
                            .send_compat_message(&SendMessage::new(
                                "❌ Invalid binding code. Ask operator for the latest code and retry.",
                                &chat_id,
                            ))
                            .await;
                    }
                    Err(lockout_secs) => {
                        let _ = self
                            .send_compat_message(&SendMessage::new(
                                format!("⏳ Too many invalid attempts. Retry in {lockout_secs}s."),
                                &chat_id,
                            ))
                            .await;
                    }
                }
            } else {
                let _ = self
                    .send_compat_message(&SendMessage::new(
                        "ℹ️ Telegram pairing is not active. Ask operator to update allowlist in config.toml.",
                        &chat_id,
                    ))
                    .await;
            }
            return;
        }

        // ── De-bounce: suppress duplicate approval prompts within the window ────────
        // Key by chat_id + sender so multiple different senders are tracked independently.
        let sender_key = normalized_sender_id
            .as_deref()
            .unwrap_or(normalized_username.as_str());
        if self.check_and_update_approval_debounce(&chat_id, sender_key) {
            tracing::debug!(
                chat_id,
                sender = sender_key,
                "[telegram][approval] de-bounce: suppressing duplicate approval prompt \
                 (sent within {}s window)",
                APPROVAL_PROMPT_DEBOUNCE_SECS
            );
            return;
        }

        tracing::warn!(
            chat_id,
            username,
            sender_id = sender_id_str.as_deref().unwrap_or("unknown"),
            "[telegram][approval] unauthorized user; sending approval prompt. \
             Allowlist Telegram username (without '@') or numeric user ID."
        );

        // Copy depends on whether first-run pairing is armed. In pairing mode the
        // operator unlocks the bot by sending `/start` (or `/bind <code>` if they
        // have the code from the app); there is no "approve in the web UI" action for
        // the self-bot-token path, so we must not point the user at one (openhuman#4381).
        //
        // Only advertise the `/start` onboarding hint in a private chat — in a group
        // it would invite any member to claim operator ownership, matching the
        // private-only onboarding gate above.
        if self.pairing_code_active() && !Self::is_group_message(message) {
            tracing::debug!(
                chat_id,
                sender = sender_key,
                "[telegram][approval] pairing pending — sending /start onboarding prompt"
            );
            let _ = self
                .send_compat_message(&SendMessage::new(
                    "🔐 This bot isn't set up yet.\n\nIf you're the operator, send /start to finish connecting your bot. \
                     Otherwise ask the operator to add your Telegram username (without '@') or numeric user ID to the bot's Allowed Users, then message again.\n\n\
                     If the operator gave you a one-time pairing code, run `/bind <code>`.".to_string(),
                    &chat_id,
                ))
                .await;
        } else {
            let _ = self
                .send_compat_message(&SendMessage::new(
                    "🔐 This bot requires operator approval.\n\nAsk the operator to add your Telegram username (without '@') or numeric user ID to the bot's Allowed Users, then send your message again.".to_string(),
                    &chat_id,
                ))
                .await;
        }
    }

    /// Resolve a stable identity to allowlist for a sender: prefer the numeric user
    /// ID (immutable), fall back to a real username. Returns `None` when the sender
    /// has neither (`normalized_username` empty or the `"unknown"` sentinel and no id).
    pub(crate) fn bindable_identity(
        normalized_username: &str,
        normalized_sender_id: Option<&str>,
    ) -> Option<String> {
        if let Some(id) = normalized_sender_id.filter(|id| !id.is_empty()) {
            return Some(id.to_string());
        }
        if normalized_username.is_empty() || normalized_username == "unknown" {
            return None;
        }
        Some(normalized_username.to_string())
    }

    /// Add `identity` to the allowlist (runtime + persisted config) and acknowledge
    /// to the chat. Shared by the `/start` onboarding and `/bind <code>` paths so
    /// both stay in lock-step on persistence and messaging.
    pub(crate) async fn approve_and_persist_sender(&self, identity: &str, chat_id: &str) {
        self.add_allowed_identity_runtime(identity);
        // Promote into the persisted allowlist via the injected host store so the
        // identity survives restarts. With no store wired, allowlisting is
        // runtime-only and we still acknowledge success.
        let persist_result = match &self.allowlist {
            Some(store) => store.persist_allowed_identity("telegram", identity).await,
            None => Ok(()),
        };
        match persist_result {
            Ok(()) => {
                let _ = self
                    .send_compat_message(&SendMessage::new(
                        "✅ You're all set — OpenHuman is connected. Send me a message and I'll take it from here.",
                        chat_id,
                    ))
                    .await;
                tracing::info!(
                    chat_id,
                    identity,
                    "[telegram][approval] allowlisted identity (runtime + persisted)"
                );
            }
            Err(e) => {
                tracing::error!(
                    chat_id,
                    error = %e,
                    "[telegram][approval] failed to persist allowlist after approval"
                );
                let _ = self
                    .send_compat_message(&SendMessage::new(
                        "⚠️ Connected for now, but I couldn't save it — access may be lost after a restart. Check the config file permissions.",
                        chat_id,
                    ))
                    .await;
            }
        }
    }

    pub(crate) fn is_supported_unauthorized_message(message: &serde_json::Value) -> bool {
        message
            .get("text")
            .and_then(serde_json::Value::as_str)
            .is_some()
            || message.get("voice").is_some()
    }

    pub(crate) fn parse_update_message(
        &self,
        update: &serde_json::Value,
    ) -> Option<ChannelMessage> {
        let message = update
            .get("message")
            .or_else(|| update.get("edited_message"))?;

        let text = message.get("text").and_then(serde_json::Value::as_str)?;
        let ctx = self.parse_incoming_message_context(message, Some(text))?;
        let content = match ctx.mention_text.clone() {
            Some(content) => content,
            None if self.mention_only && Self::is_group_message(message) => return None,
            None => text.to_string(),
        };

        (!content.trim().is_empty()).then(|| self.channel_message_from_context(ctx, content))
    }

    pub(crate) async fn parse_update_message_or_voice(
        &self,
        update: &serde_json::Value,
    ) -> Option<ChannelMessage> {
        let update_id = update.get("update_id").and_then(serde_json::Value::as_i64);
        let message = update
            .get("message")
            .or_else(|| update.get("edited_message"));
        let chat_id = message
            .and_then(|message| message.get("chat"))
            .and_then(|chat| chat.get("id"))
            .and_then(serde_json::Value::as_i64);
        let message_id = message
            .and_then(|message| message.get("message_id"))
            .and_then(serde_json::Value::as_i64);
        let has_text = message
            .and_then(|message| message.get("text"))
            .and_then(serde_json::Value::as_str)
            .is_some();
        let has_voice = message.and_then(|message| message.get("voice")).is_some();
        let has_media = message.is_some_and(|message| {
            [
                "photo", "document", "video", "audio", "sticker", "location", "venue", "contact",
            ]
            .iter()
            .any(|field| message.get(field).is_some())
        });

        tracing::debug!(
            update_id = ?update_id,
            chat_id = ?chat_id,
            message_id = ?message_id,
            has_text,
            has_voice,
            "[telegram:voice] parse update dispatch"
        );

        if let Some(msg) = self.parse_update_message(update) {
            tracing::debug!(
                update_id = ?update_id,
                chat_id = ?chat_id,
                message_id = ?message_id,
                "[telegram:voice] selected text parser"
            );
            return Some(msg);
        }

        if has_voice {
            tracing::debug!(
                update_id = ?update_id,
                chat_id = ?chat_id,
                message_id = ?message_id,
                "[telegram:voice] selected voice parser"
            );
        } else {
            tracing::debug!(
                update_id = ?update_id,
                chat_id = ?chat_id,
                message_id = ?message_id,
                "[telegram:voice] update has no supported text or voice payload"
            );
        }

        if has_media {
            return self.parse_update_media_message_for_pipeline(update);
        }

        let msg = self.parse_update_voice_message_for_pipeline(update);
        tracing::debug!(
            update_id = ?update_id,
            chat_id = ?chat_id,
            message_id = ?message_id,
            parsed = msg.is_some(),
            "[telegram:voice] parse update result"
        );
        msg
    }

    fn parse_update_media_message_for_pipeline(
        &self,
        update: &serde_json::Value,
    ) -> Option<ChannelMessage> {
        let message = update
            .get("message")
            .or_else(|| update.get("edited_message"))?;
        let message_has_structured_content = ["location", "venue", "contact"]
            .iter()
            .any(|field| message.get(field).is_some());
        if Self::update_media_attachment(update).is_none() && !message_has_structured_content {
            return None;
        }
        let caption = message.get("caption").and_then(serde_json::Value::as_str);
        let ctx = self.parse_incoming_message_context(message, caption)?;
        if self.mention_only && Self::is_group_message(message) && ctx.mention_text.is_none() {
            return None;
        }
        Some(self.channel_message_from_context(ctx, caption.unwrap_or_default().to_owned()))
    }

    fn parse_update_voice_message_for_pipeline(
        &self,
        update: &serde_json::Value,
    ) -> Option<ChannelMessage> {
        let message = update
            .get("message")
            .or_else(|| update.get("edited_message"))?;
        Self::parse_update_voice_attachment(update)?;
        let caption = message.get("caption").and_then(serde_json::Value::as_str);
        let ctx = self.parse_incoming_message_context(message, caption)?;
        if self.mention_only && Self::is_group_message(message) && ctx.mention_text.is_none() {
            return None;
        }
        Some(self.channel_message_from_context(ctx, caption.unwrap_or_default().to_owned()))
    }

    pub(crate) fn parse_update_voice_attachment(
        update: &serde_json::Value,
    ) -> Option<TelegramVoiceAttachment> {
        let voice = update
            .get("message")
            .or_else(|| update.get("edited_message"))?
            .get("voice")?;

        let file_id = voice
            .get("file_id")
            .and_then(serde_json::Value::as_str)?
            .trim();
        if file_id.is_empty() {
            return None;
        }

        Some(TelegramVoiceAttachment {
            file_id: file_id.to_string(),
            file_unique_id: voice
                .get("file_unique_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            file_size: voice.get("file_size").and_then(serde_json::Value::as_u64),
            mime_type: voice
                .get("mime_type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        })
    }

    pub(crate) async fn voice_note_for_ingress(
        &self,
        update: &serde_json::Value,
        message: &ChannelMessage,
    ) -> anyhow::Result<Option<tinychannels_bus::VoiceNote>> {
        let Some(voice) = Self::parse_update_voice_attachment(update) else {
            return Ok(None);
        };
        let Some(store) = self.voice_media_store.as_ref() else {
            anyhow::bail!("host voice media storage is unavailable");
        };
        let (bytes, file_name, _) = self
            .download_telegram_voice_file(&voice.file_id, voice.file_unique_id.as_deref())
            .await?;
        if bytes.len() as u64 > super::channel_types::TELEGRAM_MAX_STORED_VOICE_BYTES {
            anyhow::bail!("voice media exceeds the inline host storage callback limit");
        }
        let source = update
            .get("message")
            .or_else(|| update.get("edited_message"))
            .unwrap_or(&serde_json::Value::Null);
        let chat = source.get("chat").unwrap_or(&serde_json::Value::Null);
        let chat_id = chat
            .get("id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
            .unwrap_or_else(|| {
                message
                    .reply_target
                    .split(':')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            });
        let topic_id = source
            .get("message_thread_id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());
        let audio_base64 = BASE64.encode(&bytes);
        let mime_type = voice
            .mime_type
            .clone()
            .unwrap_or_else(|| "audio/ogg".into());
        let scope = tinychannels_bus::HostVoiceScope {
            configured_instance: String::new(),
            channel: tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: None,
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: match chat.get("type").and_then(serde_json::Value::as_str) {
                    Some("private") => tinychannels_bus::ConversationKind::Dm,
                    Some("group" | "supergroup") => tinychannels_bus::ConversationKind::Group,
                    Some("channel") => tinychannels_bus::ConversationKind::Channel,
                    _ => tinychannels_bus::ConversationKind::Unknown,
                },
                id: chat_id,
                thread_id: None,
                topic_id,
                ..Default::default()
            },
            // Match ChannelInboundEnvelope.message_id so the host's
            // scoped read/STT callbacks resolve the same turn.
            turn_id: message.id.clone(),
        };
        let transcription = crate::host::TranscriptionRequest {
            audio_base64: audio_base64.clone(),
            mime_type: Some(mime_type.clone()),
            file_name: Some(file_name.clone()),
            language: None,
        };
        let result = store
            .store_voice(crate::host::VoiceMediaRequest {
                audio_base64,
                mime_type,
                codec: Some("ogg_opus".into()),
                duration_ms: source
                    .get("voice")
                    .and_then(|voice| voice.get("duration"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|seconds| seconds.saturating_mul(1000)),
                file_name: Some(file_name),
                direction: tinychannels_bus::HostVoiceMediaDirection::Inbound,
                scope: scope.clone(),
            })
            .await?;
        let mut media = result.media;
        if media
            .opaque_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
        {
            anyhow::bail!("host voice media storage returned no durable opaque reference");
        }
        // Provider-originated identifiers and URLs are private/expiring; only
        // the host's opaque reference crosses into history.
        media.url = None;
        media.path = None;
        media.local_cache_path = None;
        media.kind = tinychannels_bus::MediaKind::Audio;
        media.content_type = Some(voice.mime_type.unwrap_or_else(|| "audio/ogg".into()));
        media.codec = Some("ogg_opus".into());
        let duration_ms = media.duration_ms.or_else(|| {
            source
                .get("voice")?
                .get("duration")?
                .as_u64()
                .map(|s| s.saturating_mul(1000))
        });
        let mut note = tinychannels_bus::VoiceNote {
            media,
            codec: Some("ogg_opus".into()),
            duration_ms,
            waveform: None,
            push_to_talk: true,
            transcript: None,
        };
        // The host owns speech-to-text; keep the stored note even when it is
        // unavailable so the turn can still say the voice could not be read.
        let status = crate::host::enrich_voice_note(
            &mut note,
            scope,
            Some(transcription),
            self.transcriber.clone(),
        )
        .await;
        tracing::debug!(?status, "[telegram] voice note enriched");
        Ok(Some(note))
    }

    /// A Telegram user's display name: first and last name, else their
    /// username. `None` when the user object has neither.
    pub(crate) fn sender_display_name(from: Option<&serde_json::Value>) -> Option<String> {
        let from = from?;
        let part = |key: &str| {
            from.get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|part| !part.is_empty())
        };
        let full = [part("first_name"), part("last_name")]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ");
        if full.is_empty() {
            part("username").map(str::to_string)
        } else {
            Some(full)
        }
    }

    pub(crate) fn parse_incoming_message_context(
        &self,
        message: &serde_json::Value,
        mention_source: Option<&str>,
    ) -> Option<TelegramIncomingMessageContext> {
        let username = message
            .get("from")
            .and_then(|from| from.get("username"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string();

        let sender_id = message
            .get("from")
            .and_then(|from| from.get("id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());

        let sender_name = Self::sender_display_name(message.get("from"));

        let sender_identity = if username == "unknown" {
            sender_id.clone().unwrap_or_else(|| "unknown".to_string())
        } else {
            username.clone()
        };

        let mut identities = vec![username.as_str()];
        if let Some(id) = sender_id.as_deref() {
            identities.push(id);
        }

        if !self.is_any_user_allowed(identities.iter().copied()) {
            tracing::debug!(
                username = %username,
                sender_id = sender_id.as_deref().unwrap_or("none"),
                message_len = mention_source.map(str::len).unwrap_or_default(),
                "[telegram] dropped message: sender not in allowed_users (unauthorized handler may reply)"
            );
            return None;
        }

        let is_group = Self::is_group_message(message);
        let mention_text = if self.mention_only && is_group {
            let mention_source = mention_source?;
            let bot_username_guard = self.bot_username.lock();
            let bot_username = bot_username_guard.as_ref()?;
            if !Self::contains_bot_mention(mention_source, bot_username) {
                return None;
            }
            Self::normalize_incoming_content(mention_source, bot_username)
        } else {
            None
        };

        let chat_id = message
            .get("chat")
            .and_then(|chat| chat.get("id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())?;

        let message_id = message
            .get("message_id")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);

        // Extract thread/topic ID for forum support
        let thread_id = message
            .get("message_thread_id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());

        // reply_target: chat_id or chat_id:thread_id format
        let reply_target = if let Some(tid) = thread_id {
            format!("{}:{}", chat_id, tid)
        } else {
            chat_id.clone()
        };

        let replied_parent_message_id = message
            .get("reply_to_message")
            .and_then(|reply| reply.get("message_id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());

        // Telegram "reply" targeting should point to the inbound message itself so the
        // assistant response is visibly attached in chat. We still retain the inbound
        // parent reference in logs for reply-context diagnostics.
        tracing::debug!(
            chat_id,
            message_id,
            reply_to_parent = replied_parent_message_id.as_deref().unwrap_or("none"),
            "Telegram inbound message parsed for reply mapping"
        );

        Some(TelegramIncomingMessageContext {
            sender_identity,
            sender_name,
            reply_target,
            chat_id,
            message_id,
            mention_text,
        })
    }

    pub(crate) fn channel_message_from_context(
        &self,
        ctx: TelegramIncomingMessageContext,
        content: String,
    ) -> ChannelMessage {
        ChannelMessage {
            id: format!("telegram_{}_{}", ctx.chat_id, ctx.message_id),
            sender: ctx.sender_identity,
            reply_target: ctx.reply_target,
            content,
            channel: "telegram".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            topic_id: None,
            reply_to_id: None,
            thread_ts: Some(ctx.message_id.to_string()),
            sender_name: ctx.sender_name,
        }
    }

    pub(crate) fn redact_bot_token(&self, value: impl AsRef<str>) -> String {
        if self.bot_token.is_empty() {
            return value.as_ref().to_string();
        }
        value.as_ref().replace(&self.bot_token, "<redacted>")
    }

    pub(crate) async fn download_telegram_media_file(
        &self,
        file_id: &str,
        max_bytes: u64,
    ) -> anyhow::Result<TelegramDownloadedMedia> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        let file_response = tokio::time::timeout_at(
            deadline,
            self.http_client()
                .post(self.api_url("getFile"))
                .json(&serde_json::json!({ "file_id": file_id }))
                .send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Telegram media request timed out"))?
        .map_err(|_| anyhow::anyhow!("Telegram getFile request failed"))?;
        let file_response: serde_json::Value =
            tokio::time::timeout_at(deadline, file_response.json())
                .await
                .map_err(|_| anyhow::anyhow!("Telegram media request timed out"))?
                .map_err(|_| anyhow::anyhow!("Telegram getFile response was invalid"))?;
        if !file_response
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            anyhow::bail!("Telegram getFile rejected the media request");
        }
        let result = file_response
            .get("result")
            .ok_or_else(|| anyhow::anyhow!("Telegram getFile response omitted its result"))?;
        let file_path = result
            .get("file_path")
            .and_then(serde_json::Value::as_str)
            .filter(|path| !path.is_empty() && !path.starts_with('/') && !path.contains(".."))
            .ok_or_else(|| anyhow::anyhow!("Telegram getFile response had an invalid path"))?;
        if result
            .get("file_size")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|size| size > max_bytes)
        {
            anyhow::bail!("Telegram media exceeds the configured size limit");
        }
        let file_name = file_path.rsplit('/').next().map(str::to_owned);
        let url = format!("{}/file/bot{}/{}", self.api_base, self.bot_token, file_path);
        let mut response = tokio::time::timeout_at(deadline, self.http_client().get(url).send())
            .await
            .map_err(|_| anyhow::anyhow!("Telegram media request timed out"))?
            .map_err(|_| anyhow::anyhow!("Telegram media download failed"))?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > max_bytes)
        {
            anyhow::bail!("Telegram media download was rejected");
        }
        let response_mime = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| {
                value
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase()
            });
        let mut bytes = Vec::new();
        loop {
            let chunk = tokio::time::timeout_at(deadline, response.chunk())
                .await
                .map_err(|_| anyhow::anyhow!("Telegram media download timed out"))?
                .map_err(|_| anyhow::anyhow!("Telegram media download failed"))?;
            let Some(chunk) = chunk else { break };
            if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
                anyhow::bail!("Telegram media exceeds the configured size limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            anyhow::bail!("Telegram returned an empty media body");
        }
        Ok(TelegramDownloadedMedia {
            bytes,
            file_name,
            content_type: response_mime,
        })
    }

    pub(crate) async fn download_telegram_voice_file(
        &self,
        file_id: &str,
        file_unique_id: Option<&str>,
    ) -> anyhow::Result<(Vec<u8>, String, Option<u64>)> {
        tracing::debug!(
            file_unique_id = file_unique_id.unwrap_or("unknown"),
            file_id_present = !file_id.trim().is_empty(),
            "[telegram:voice:download] requesting Telegram getFile"
        );

        let resp = self
            .http_client()
            .post(self.api_url("getFile"))
            .json(&serde_json::json!({ "file_id": file_id }))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{}", self.redact_bot_token(e.to_string())))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        tracing::debug!(
            file_unique_id = file_unique_id.unwrap_or("unknown"),
            status = %status,
            response_bytes = body.len(),
            "[telegram:voice:download] Telegram getFile response received"
        );

        if !status.is_success() {
            tracing::debug!(
                file_unique_id = file_unique_id.unwrap_or("unknown"),
                status = %status,
                error = %self.redact_bot_token(&body),
                "[telegram:voice:download] Telegram getFile failed"
            );
            anyhow::bail!(
                "Telegram getFile failed ({status}): {}",
                self.redact_bot_token(body)
            );
        }

        let payload: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("Telegram getFile returned invalid JSON: {e}"))?;
        if !payload
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let description = payload
                .get("description")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown Telegram getFile error");
            tracing::debug!(
                file_unique_id = file_unique_id.unwrap_or("unknown"),
                description,
                "[telegram:voice:download] Telegram getFile returned ok=false"
            );
            anyhow::bail!("Telegram getFile returned ok=false: {description}");
        }

        let result = payload
            .get("result")
            .ok_or_else(|| anyhow::anyhow!("Telegram getFile response missing result"))?;
        let file_path = result
            .get("file_path")
            .and_then(serde_json::Value::as_str)
            .filter(|path| !path.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("Telegram getFile response missing file_path"))?;
        let file_size = result.get("file_size").and_then(serde_json::Value::as_u64);
        if let Some(file_size) = file_size
            && file_size > TELEGRAM_MAX_VOICE_FILE_BYTES
        {
            tracing::debug!(
                file_unique_id = file_unique_id.unwrap_or("unknown"),
                file_size,
                max_bytes = TELEGRAM_MAX_VOICE_FILE_BYTES,
                "[telegram:voice:download] Telegram getFile file_size exceeds cap"
            );
            anyhow::bail!(
                "Telegram getFile reported voice file too large: {file_size} bytes (max {TELEGRAM_MAX_VOICE_FILE_BYTES})"
            );
        }
        let file_name = file_path
            .rsplit('/')
            .next()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("voice.ogg")
            .to_string();
        tracing::debug!(
            file_unique_id = file_unique_id.unwrap_or("unknown"),
            file_name = %file_name,
            file_size = ?file_size,
            "[telegram:voice:download] Telegram getFile result parsed"
        );

        let download_url = format!("{}/file/bot{}/{}", self.api_base, self.bot_token, file_path);
        tracing::debug!(
            file_unique_id = file_unique_id.unwrap_or("unknown"),
            file_name = %file_name,
            "[telegram:voice:download] starting Telegram voice file download"
        );
        let mut file_resp = self
            .http_client()
            .get(download_url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{}", self.redact_bot_token(e.to_string())))?;
        if let Some(content_length) = file_resp.content_length()
            && content_length > TELEGRAM_MAX_VOICE_FILE_BYTES
        {
            tracing::debug!(
                file_unique_id = file_unique_id.unwrap_or("unknown"),
                content_length,
                max_bytes = TELEGRAM_MAX_VOICE_FILE_BYTES,
                "[telegram:voice:download] voice download Content-Length exceeds cap"
            );
            anyhow::bail!(
                "Telegram voice download too large: {content_length} bytes (max {TELEGRAM_MAX_VOICE_FILE_BYTES})"
            );
        }
        tracing::debug!(
            file_unique_id = file_unique_id.unwrap_or("unknown"),
            content_length = ?file_resp.content_length(),
            "[telegram:voice:download] voice download headers received"
        );

        let status = file_resp.status();
        if !status.is_success() {
            let body = file_resp.text().await.unwrap_or_default();
            tracing::debug!(
                file_unique_id = file_unique_id.unwrap_or("unknown"),
                status = %status,
                error = %self.redact_bot_token(&body),
                "[telegram:voice:download] Telegram voice download failed"
            );
            anyhow::bail!(
                "Telegram voice download failed ({status}): {}",
                self.redact_bot_token(body)
            );
        }

        let mut bytes = Vec::new();
        while let Some(chunk) = file_resp
            .chunk()
            .await
            .map_err(|e| anyhow::anyhow!("{}", self.redact_bot_token(e.to_string())))?
        {
            Self::append_telegram_voice_download_chunk(
                &mut bytes,
                &chunk,
                TELEGRAM_MAX_VOICE_FILE_BYTES,
            )?;
            tracing::debug!(
                file_unique_id = file_unique_id.unwrap_or("unknown"),
                file_name = %file_name,
                chunk_bytes = chunk.len(),
                downloaded_bytes = bytes.len(),
                "[telegram:voice:download] received voice file chunk"
            );
        }
        tracing::debug!(
            file_unique_id = file_unique_id.unwrap_or("unknown"),
            file_name = %file_name,
            downloaded_bytes = bytes.len(),
            "[telegram:voice:download] completed Telegram voice file download"
        );

        Ok((bytes, file_name, file_size))
    }

    pub(crate) fn append_telegram_voice_download_chunk(
        bytes: &mut Vec<u8>,
        chunk: &[u8],
        max_bytes: u64,
    ) -> anyhow::Result<()> {
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| anyhow::anyhow!("Telegram voice download size overflow"))?;
        if u64::try_from(next_len).unwrap_or(u64::MAX) > max_bytes {
            anyhow::bail!("Telegram voice download too large: {next_len} bytes (max {max_bytes})");
        }

        bytes.extend_from_slice(chunk);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn parse_update_reaction(
        &self,
        update: &serde_json::Value,
    ) -> Option<TelegramReactionEvent> {
        self.parse_update_reactions(update).into_iter().next()
    }

    pub(crate) fn parse_update_reactions(
        &self,
        update: &serde_json::Value,
    ) -> Vec<TelegramReactionEvent> {
        let Some(reaction) = update.get("message_reaction") else {
            return Vec::new();
        };
        let Some(chat_id) = reaction
            .get("chat")
            .and_then(|chat| chat.get("id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
        else {
            return Vec::new();
        };
        let Some(message_id) = reaction
            .get("message_id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
        else {
            return Vec::new();
        };
        let conversation_kind = match reaction
            .get("chat")
            .and_then(|chat| chat.get("type"))
            .and_then(serde_json::Value::as_str)
        {
            Some("private") => tinychannels_bus::ConversationKind::Dm,
            Some("group" | "supergroup") => tinychannels_bus::ConversationKind::Group,
            Some("channel") => tinychannels_bus::ConversationKind::Channel,
            _ => tinychannels_bus::ConversationKind::Unknown,
        };
        let actor_chat = reaction.get("actor_chat");
        let actor_chat_id = actor_chat.and_then(|chat| chat.get("id")).and_then(|id| {
            id.as_i64()
                .map(|id| id.to_string())
                .or_else(|| id.as_str().map(ToString::to_string))
        });
        let actor = reaction
            .get("user")
            .and_then(|user| user.get("username"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string)
            .or_else(|| {
                reaction
                    .get("user")
                    .and_then(|user| user.get("id"))
                    .and_then(serde_json::Value::as_i64)
                    .map(|id| id.to_string())
            })
            .or_else(|| {
                actor_chat
                    .and_then(|chat| chat.get("username"))
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string)
            })
            .or_else(|| {
                actor_chat
                    .and_then(|chat| chat.get("title"))
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string)
            })
            .or_else(|| actor_chat_id.clone())
            .unwrap_or_else(|| "unknown".to_string());

        let user_id = reaction
            .get("user")
            .and_then(|user| user.get("id"))
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string())
            .or_else(|| actor_chat_id.clone());

        let actor_allowed = self.is_user_allowed(&actor);
        let user_id_allowed = user_id
            .as_deref()
            .is_some_and(|id| self.is_user_allowed(id));

        if !(actor_allowed || user_id_allowed) {
            tracing::debug!(
                actor,
                message_id,
                "Telegram reaction ignored: actor is not allowlisted"
            );
            return Vec::new();
        }

        let new_reactions = reaction
            .get("new_reaction")
            .and_then(serde_json::Value::as_array);
        let old_reactions = reaction
            .get("old_reaction")
            .and_then(serde_json::Value::as_array);
        let sender_id = user_id.clone().unwrap_or_else(|| actor.clone());
        let thread_id = reaction
            .get("message_thread_id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());
        let identity = |entry: &serde_json::Value| {
            (
                entry
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                entry
                    .get("emoji")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                entry
                    .get("custom_emoji_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
            )
        };
        let old = old_reactions
            .into_iter()
            .flatten()
            .map(identity)
            .collect::<std::collections::BTreeSet<_>>();
        let new = new_reactions
            .into_iter()
            .flatten()
            .map(identity)
            .collect::<std::collections::BTreeSet<_>>();
        let update_id = update
            .get("update_id")
            .and_then(serde_json::Value::as_i64)
            .map(|id| id.to_string());
        let timestamp = reaction
            .get("date")
            .and_then(serde_json::Value::as_u64)
            .map(|seconds| seconds.saturating_mul(1000));
        let make =
            |(_kind, emoji, custom): (String, String, String), added| TelegramReactionEvent {
                sender: actor.clone(),
                sender_id: sender_id.clone(),
                reply_target: chat_id.clone(),
                target_message_id: message_id.clone(),
                emoji: if !custom.is_empty() {
                    format!("custom:{custom}")
                } else if emoji.is_empty() {
                    _kind
                } else {
                    emoji
                },
                custom_emoji_id: (!custom.is_empty()).then_some(custom),
                added,
                update_id: update_id.clone(),
                timestamp,
                thread_id: thread_id.clone(),
                conversation_kind,
            };
        let mut events = old
            .difference(&new)
            .cloned()
            .map(|change| make(change, false))
            .collect::<Vec<_>>();
        events.extend(
            new.difference(&old)
                .cloned()
                .map(|change| make(change, true)),
        );
        if reaction.get("old_reaction").is_none() {
            events = new.into_iter().map(|change| make(change, true)).collect();
        }
        events
    }
}

#[cfg(test)]
#[path = "channel_recv_tests.rs"]
mod tests;
