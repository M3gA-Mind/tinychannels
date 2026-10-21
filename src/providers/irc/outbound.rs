use super::{IrcChannel, ListenerRegistration};
use async_trait::async_trait;

pub(super) const SENDER_PREFIX_RESERVE: usize = 64;

/// Split a message into lines safe for IRC transmission.
///
/// IRC is a line-based protocol — `\r\n` terminates each command, so any
/// newline inside a PRIVMSG payload would truncate the message and turn the
/// remainder into garbled/invalid IRC commands.
///
/// This function:
/// 1. Splits on `\n` (and strips `\r`) so each logical line becomes its own PRIVMSG.
/// 2. Splits any line that exceeds `max_bytes` at a safe UTF-8 boundary.
/// 3. Skips empty lines to avoid sending blank PRIVMSGs.
pub(super) fn split_message(message: &str, max_bytes: usize) -> Vec<String> {
    let mut chunks = Vec::new();

    // Guard against max_bytes == 0 to prevent infinite loop
    if max_bytes == 0 {
        let mut full = String::new();
        for l in message
            .lines()
            .map(|l| l.trim_end_matches('\r'))
            .filter(|l| !l.is_empty())
        {
            if !full.is_empty() {
                full.push(' ');
            }
            full.push_str(l);
        }
        if full.is_empty() {
            chunks.push(String::new());
        } else {
            chunks.push(full);
        }
        return chunks;
    }

    for line in message.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }

        if line.len() <= max_bytes {
            chunks.push(line.to_string());
            continue;
        }

        // Line exceeds max_bytes — split at safe UTF-8 boundaries
        let mut remaining = line;
        while !remaining.is_empty() {
            if remaining.len() <= max_bytes {
                chunks.push(remaining.to_string());
                break;
            }

            let mut split_at = max_bytes;
            while split_at > 0 && !remaining.is_char_boundary(split_at) {
                split_at -= 1;
            }
            if split_at == 0 {
                // No valid boundary found going backward — advance forward instead
                split_at = max_bytes;
                while split_at < remaining.len() && !remaining.is_char_boundary(split_at) {
                    split_at += 1;
                }
            }

            chunks.push(remaining[..split_at].to_string());
            remaining = &remaining[split_at..];
        }
    }

    if chunks.is_empty() {
        chunks.push(String::new());
    }

    chunks
}

#[async_trait]
impl crate::providers::long_tail::LongTail for IrcChannel {
    const ID: &'static str = "irc";
    const DISPLAY: &'static str = "IRC";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        IrcChannel::capabilities()
    }

    async fn send_typed_message(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> Result<
        tinychannels_bus::channel::ChannelSendOutcome,
        tinychannels_bus::channel::ChannelSendError,
    > {
        use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

        if message.conversation_id.trim().is_empty() {
            return Err(ChannelSendError {
                kind: SendErrorKind::BadFormat,
                message: "conversation address is empty".into(),
                ..Default::default()
            });
        }
        let has_thread = message.reply_to_id.is_some()
            || message.thread_id.is_some()
            || message
                .conversation_ref
                .as_ref()
                .is_some_and(|conversation| {
                    conversation.thread_id.is_some() || conversation.topic_id.is_some()
                });
        if has_thread {
            return Err(crate::providers::long_tail::unsupported::<Self>(
                "reply or thread addressing",
            ));
        }
        let text = message
            .legacy_text()
            .map_err(|_| crate::providers::long_tail::unsupported::<Self>("non-text content"))?;
        let recipient = message
            .conversation_ref
            .as_ref()
            .map_or(message.conversation_id.as_str(), |conversation| {
                conversation.id.as_str()
            });

        let mut guard = self.writer.lock().await;
        let Some(writer) = guard.as_mut() else {
            return Err(ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "IRC is not connected".into(),
                retryable: true,
                sent_before_error: false,
                ..Default::default()
            });
        };
        Self::send_text_connected(writer, recipient, &text)
            .await
            .map_err(Self::typed_send_failure)?;
        Ok(tinychannels_bus::channel::ChannelSendOutcome::default())
    }

    async fn start_listener(
        &self,
        sink: &(dyn crate::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        let cancellation =
            self.begin_listener()
                .map_err(|error| tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Forbidden,
                    message: error.to_string(),
                    retryable: false,
                    ..Default::default()
                })?;
        let _registration = ListenerRegistration(self);
        let result = self.listen_owned(sink, Some(cancellation)).await;
        result.map_err(|error| {
            tracing::debug!(%error, "IRC typed listener stopped");
            tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "IRC listener stopped".into(),
                retryable: true,
                ..Default::default()
            }
        })
    }

    async fn stop_listener(&self) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        if let Some(cancel) = self
            .listener_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            cancel.send_replace(true);
        }
        self.stop_writer().await.map_err(|error| {
            tracing::debug!(%error, "IRC listener shutdown failed");
            tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Transient,
                message: "IRC listener shutdown failed".into(),
                retryable: true,
                ..Default::default()
            }
        })
    }
}

crate::providers::long_tail::impl_long_tail_adapter!(IrcChannel);
