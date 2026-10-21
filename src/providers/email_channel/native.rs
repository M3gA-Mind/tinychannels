//! Email as a [`NativeProvider`]: one reply is one message. The planned text
//! becomes the body, files and voice clips become attachments, and the reply
//! and thread targets become `In-Reply-To` and `References`.

use super::EmailChannel;
use crate::channel::ChannelInboundSink;
use crate::host::{MediaRepository, VoiceMediaStore};
use crate::providers::mail::{
    LettreMailSender, MailAttachment, MailCredentials, MailError, MailHeaders, MailSender,
    OutboundEmail,
};
use crate::providers::native::{Limits, NativeProvider, Sent, Step, Target, pre_dispatch};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tinychannels_bus::ChannelCapabilities;
use tinychannels_bus::channel::{ChannelSendError, MessageReceiptPartKind, SendErrorKind};

/// Total attachment bytes of one message; relays commonly cap near 25 MiB.
pub(crate) const EMAIL_MAX_FILE_BYTES: u64 = 12 * 1024 * 1024;
/// Voice bytes cross TinyBus inline, so they stay below the frame cap.
pub(crate) const EMAIL_MAX_VOICE_BYTES: u64 = 8 * 1024 * 1024;
/// Bodies are never split: a reply is one message.
pub(crate) const EMAIL_TEXT_CHUNK_CHARS: usize = 1_000_000;
const DEFAULT_SUBJECT: &str = "OpenHuman Message";
/// Inbound subjects remembered so replies keep them.
const SUBJECT_WINDOW: usize = 512;

/// The subjects and the order they arrived in, oldest first.
type SubjectWindow = (HashMap<String, String>, VecDeque<String>);

/// Subjects of inbound messages by message id.
#[derive(Clone, Default)]
pub(crate) struct Subjects {
    inner: Arc<Mutex<SubjectWindow>>,
}

impl Subjects {
    pub(crate) fn remember(&self, message_id: &str, subject: &str) {
        let mut guard = self.inner.lock();
        let (map, order) = &mut *guard;
        if map
            .insert(message_id.to_owned(), subject.to_owned())
            .is_none()
        {
            order.push_back(message_id.to_owned());
            if order.len() > SUBJECT_WINDOW
                && let Some(oldest) = order.pop_front()
            {
                map.remove(&oldest);
            }
        }
    }

    pub(crate) fn get(&self, message_id: &str) -> Option<String> {
        self.inner.lock().0.get(message_id).cloned()
    }
}

/// `Re: subject`, without stacking prefixes.
fn reply_subject(subject: &str) -> String {
    let trimmed = subject.trim();
    if trimmed.len() >= 3 && trimmed[..3].eq_ignore_ascii_case("re:") {
        trimmed.to_owned()
    } else {
        format!("Re: {trimmed}")
    }
}

/// Strip angle brackets from a message id.
pub(crate) fn bare_id(id: &str) -> &str {
    id.trim().trim_start_matches('<').trim_end_matches('>')
}

/// A recipient that cannot smuggle a header or a second address.
fn checked_recipient(recipient: &str) -> Result<&str, ChannelSendError> {
    let valid = recipient.contains('@')
        && recipient.len() <= 320
        && !recipient
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, ',' | ';' | '<' | '>'));
    if valid {
        Ok(recipient)
    } else {
        Err(pre_dispatch("Email recipient is not a single address"))
    }
}

fn mail_error(error: MailError) -> ChannelSendError {
    match error {
        MailError::InvalidAddress(_) | MailError::Config(_) => {
            pre_dispatch("Email message was rejected before sending")
        }
        _ => ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "Email relay did not accept the message".into(),
            retryable: true,
            ..Default::default()
        },
    }
}

impl EmailChannel {
    fn message_id_domain(&self) -> String {
        self.config
            .from_address
            .rsplit_once('@')
            .map(|(_, domain)| domain.trim_end_matches('>').to_owned())
            .filter(|domain| !domain.is_empty())
            .unwrap_or_else(|| "localhost".into())
    }

    /// The subject: an explicit `Subject:` first line, else the thread's, else
    /// the default.
    fn subject_for(&self, target: &Target, body: &mut String) -> String {
        if let Some(rest) = body.strip_prefix("Subject: ")
            && let Some((subject, remainder)) = rest.split_once('\n')
        {
            let subject = subject.trim().replace(['\r', '\n'], " ");
            *body = remainder.trim().to_owned();
            if !subject.is_empty() {
                return subject;
            }
        }
        [target.reply_to.as_deref(), target.thread.as_deref()]
            .into_iter()
            .flatten()
            .find_map(|id| self.subjects.get(bare_id(id)))
            .map(|subject| reply_subject(&subject))
            .unwrap_or_else(|| DEFAULT_SUBJECT.into())
    }
}

#[async_trait]
impl NativeProvider for EmailChannel {
    fn id(&self) -> &'static str {
        "email"
    }

    fn display(&self) -> &'static str {
        "Email"
    }

    fn limits(&self) -> Limits {
        Limits {
            text_chars: EMAIL_TEXT_CHUNK_CHARS,
            file_bytes: EMAIL_MAX_FILE_BYTES,
            voice_bytes: EMAIL_MAX_VOICE_BYTES,
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

    async fn health_state(&self) -> &'static str {
        self.typed_health_state(self.connect_imap(), super::HEALTH_CHECK_TIMEOUT)
            .await
    }

    fn descriptor_extra(&self) -> Vec<(&'static str, &'static str)> {
        vec![("voiceOut", "attachment")]
    }

    fn bundles_steps(&self) -> bool {
        true
    }

    async fn start_listener(
        &self,
        sink: &(dyn ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        self.listen_typed_with(sink, || self.connect_imap()).await
    }

    async fn stop_listener(&self) -> Result<(), ChannelSendError> {
        if let Some((_, cancel)) = self
            .listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = cancel.send(true);
        }
        Ok(())
    }

    async fn deliver(&self, target: &Target, step: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        self.deliver_bundle(target, std::slice::from_ref(step))
            .await
    }

    async fn deliver_bundle(
        &self,
        target: &Target,
        steps: &[Step],
    ) -> Result<Vec<Sent>, ChannelSendError> {
        let recipient = checked_recipient(&target.conversation.id)?;
        let mut texts = Vec::new();
        let mut attachments = Vec::new();
        let mut kind = MessageReceiptPartKind::Text;
        for step in steps {
            match step {
                Step::Text(text) => texts.push(text.clone()),
                Step::File(file) | Step::Voice(file) => {
                    if let Some(caption) = file.caption.as_deref().filter(|c| !c.trim().is_empty())
                    {
                        texts.push(caption.to_owned());
                    }
                    attachments.push(MailAttachment {
                        filename: file.name.clone(),
                        content_type: file.mime.clone(),
                        bytes: file.bytes.clone(),
                    });
                    kind = if matches!(step, Step::Voice(_)) {
                        MessageReceiptPartKind::Voice
                    } else if kind == MessageReceiptPartKind::Voice {
                        kind
                    } else {
                        MessageReceiptPartKind::Media
                    };
                }
            }
        }
        let mut body = texts.join("\n\n");
        let subject = self.subject_for(target, &mut body);
        let message_id = format!("{}@{}", uuid::Uuid::new_v4(), self.message_id_domain());
        let reply_to = target.reply_to.as_deref().map(bare_id).map(str::to_owned);
        let thread = target.thread.as_deref().map(bare_id).map(str::to_owned);
        let mut references = Vec::new();
        references.extend(thread.clone());
        references.extend(reply_to.clone().filter(|id| Some(id) != thread.as_ref()));
        let email = OutboundEmail {
            to: recipient.to_owned(),
            subject,
            body,
            attachments,
            headers: MailHeaders {
                message_id: Some(message_id.clone()),
                in_reply_to: reply_to.or(thread),
                references,
            },
        };
        LettreMailSender::new()
            .send(&MailCredentials::Smtp(self.smtp_credentials()), &email)
            .await
            .map_err(|error| {
                tracing::warn!("[email] send failed: {error}");
                mail_error(error)
            })?;
        Ok(vec![Sent::new(message_id, kind)])
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
