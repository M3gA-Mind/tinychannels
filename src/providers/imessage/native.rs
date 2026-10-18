//! iMessage as a [`NativeProvider`]: sends run through `osascript`, and the
//! message id comes back from `chat.db` because AppleScript returns none.

use super::IMessageChannel;
use super::db::newest_sent_after;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::native::{
    FileStep, Limits, NativeProvider, Sent, Step, Target, pre_dispatch,
};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};

pub(crate) const IMESSAGE_MAX_FILE_BYTES: u64 = 12 * 1024 * 1024;
/// Voice bytes cross TinyBus inline, so they stay below the frame cap.
pub(crate) const IMESSAGE_MAX_VOICE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const IMESSAGE_TEXT_CHUNK_CHARS: usize = 8000;
/// How often the new row is looked for after a send.
const RESOLVE_STEP: Duration = Duration::from_millis(50);

fn failed(message: &str, sent: bool) -> ChannelSendError {
    ChannelSendError {
        kind: if sent {
            SendErrorKind::Unknown
        } else {
            SendErrorKind::Transient
        },
        message: message.into(),
        sent_before_error: sent,
        retryable: !sent,
        ..Default::default()
    }
}

/// A file name that cannot escape its directory or carry script syntax.
fn safe_name(name: &str) -> String {
    let cleaned: String = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let cleaned = cleaned.trim_start_matches('.');
    if cleaned.is_empty() {
        "attachment".into()
    } else {
        cleaned.to_owned()
    }
}

/// A scratch directory holding one outbound attachment, removed on drop.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl IMessageChannel {
    /// AppleScript sending `body` (a quoted string or `POSIX file` expression)
    /// to `recipient`: a participant for a person, a chat for a group.
    fn send_script(recipient: &str, body: &str) -> Result<String, ChannelSendError> {
        use super::{escape_applescript, is_valid_chat_guid, is_valid_imessage_target};
        let target = escape_applescript(recipient);
        if is_valid_imessage_target(recipient) {
            Ok(format!(
                r#"tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetBuddy to participant "{target}" of targetService
    send {body} to targetBuddy
end tell"#
            ))
        } else if is_valid_chat_guid(recipient) {
            Ok(format!(
                r#"tell application "Messages"
    set targetChat to chat id "{target}"
    send {body} to targetChat
end tell"#
            ))
        } else {
            Err(pre_dispatch(
                "iMessage target must be a phone number, an email address or a chat id",
            ))
        }
    }

    async fn run_script(&self, script: &str) -> Result<(), ChannelSendError> {
        let (program, leading) = self
            .script_command
            .split_first()
            .ok_or_else(|| pre_dispatch("no AppleScript runner configured"))?;
        let output = tokio::process::Command::new(program)
            .args(leading)
            .arg("-e")
            .arg(script)
            .output()
            .await
            .map_err(|_| failed("iMessage could not start the AppleScript runner", false))?;
        if output.status.success() {
            return Ok(());
        }
        tracing::warn!("[imessage] the AppleScript runner reported a failure");
        Err(failed("iMessage send failed", false))
    }

    /// The newest sent ROWID, so the new row can be told apart afterwards.
    async fn sent_baseline(&self) -> Result<i64, ChannelSendError> {
        let path = self
            .messages_db_path()
            .filter(|p| p.exists())
            .ok_or_else(|| {
                pre_dispatch("iMessage sends need the Messages database to confirm delivery")
            })?;
        tokio::task::spawn_blocking(move || {
            super::open_read_only(&path).and_then(|conn| super::db::max_sent_rowid(&conn))
        })
        .await
        .map_err(|_| failed("iMessage database worker failed", false))?
        .map_err(|_| pre_dispatch("iMessage could not read the Messages database"))
    }

    /// Wait for Messages to record the message just sent and return its guid.
    async fn resolve_sent(&self, baseline: i64) -> Result<String, ChannelSendError> {
        let path = self
            .messages_db_path()
            .ok_or_else(|| failed("iMessage database path is unavailable", true))?;
        let deadline = tokio::time::Instant::now() + self.resolve_timeout;
        loop {
            let probe = path.clone();
            let found = tokio::task::spawn_blocking(move || {
                super::open_read_only(&probe).and_then(|conn| newest_sent_after(&conn, baseline))
            })
            .await
            .ok()
            .and_then(Result::ok)
            .flatten();
            if let Some((rowid, guid)) = found {
                return Ok(if guid.is_empty() {
                    rowid.to_string()
                } else {
                    guid
                });
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failed(
                    "iMessage accepted the message but Messages did not record it",
                    true,
                ));
            }
            tokio::time::sleep(RESOLVE_STEP).await;
        }
    }

    async fn deliver_text(&self, recipient: &str, text: &str) -> Result<String, ChannelSendError> {
        let body = format!("\"{}\"", super::escape_applescript(text));
        let script = Self::send_script(recipient, &body)?;
        let baseline = self.sent_baseline().await?;
        self.run_script(&script).await?;
        self.resolve_sent(baseline).await
    }

    async fn deliver_file(
        &self,
        recipient: &str,
        file: &FileStep,
        kind: MessageReceiptPartKind,
    ) -> Result<Vec<Sent>, ChannelSendError> {
        let mut sent = Vec::new();
        if let Some(caption) = file.caption.as_deref().filter(|c| !c.trim().is_empty()) {
            let id = self.deliver_text(recipient, caption).await?;
            sent.push(Sent::new(id, MessageReceiptPartKind::Text));
        }
        let dir = std::env::temp_dir()
            .join("tinychannels-imessage")
            .join(uuid::Uuid::new_v4().to_string());
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|_| failed("iMessage could not stage the attachment", false))?;
        let scratch = Scratch(dir.clone());
        let path = dir.join(safe_name(&file.name));
        tokio::fs::write(&path, &file.bytes)
            .await
            .map_err(|_| failed("iMessage could not stage the attachment", false))?;
        let body = format!(
            "(POSIX file \"{}\")",
            super::escape_applescript(&path.to_string_lossy())
        );
        let script = Self::send_script(recipient, &body)?;
        let baseline = self.sent_baseline().await?;
        let result = async {
            self.run_script(&script).await?;
            self.resolve_sent(baseline).await
        }
        .await;
        drop(scratch);
        match result {
            Ok(id) => {
                sent.push(Sent::new(id, kind));
                Ok(sent)
            }
            // The caption already went out, so the failure is a partial send.
            Err(error) if !sent.is_empty() => Err(ChannelSendError {
                sent_before_error: true,
                retryable: false,
                ..error
            }),
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl NativeProvider for IMessageChannel {
    fn id(&self) -> &'static str {
        "imessage"
    }

    fn display(&self) -> &'static str {
        "iMessage"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: IMESSAGE_TEXT_CHUNK_CHARS,
            file_bytes: IMESSAGE_MAX_FILE_BYTES,
            voice_bytes: IMESSAGE_MAX_VOICE_BYTES,
        }
    }

    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        self.native.media_repository.as_ref()
    }

    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        self.native.voice_media_store.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        self.native.voice_synthesis
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.declared()
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        vec![("voiceOut", "attachment")]
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        let recipient = target.conversation.id.as_str();
        match step {
            Step::Text(text) => {
                let id = self.deliver_text(recipient, text).await?;
                Ok(vec![Sent::new(id, MessageReceiptPartKind::Text)])
            }
            Step::File(file) => {
                self.deliver_file(recipient, file, MessageReceiptPartKind::Media)
                    .await
            }
            Step::Voice(file) => {
                self.deliver_file(recipient, file, MessageReceiptPartKind::Voice)
                    .await
            }
        }
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
