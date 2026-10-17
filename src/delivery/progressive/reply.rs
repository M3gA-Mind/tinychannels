//! [`ProgressiveReply`]: one turn's progressive delivery, driven by the host.

use std::sync::Arc;

use super::bubbles::{flush_thinking_message, send_filler_message, send_typing_indicator};
use super::draft::flush_streaming_edit;
use super::finalize::{abort_channel_reply, finalize_channel_reply, send_channel_reply};
use super::state::{StreamingState, TypingState};
use super::{ProgressiveOutputContext, ProgressiveSender, channel_supports_progressive_ui};
use tinychannels_bus::ChannelCapabilities;

/// Progressive delivery of one agent turn to one channel.
///
/// The host owns the event loop: it feeds agent stream events in through the
/// `on_*` methods, calls the `*_tick` methods from timers at
/// [`super::EDIT_FLUSH_INTERVAL`], [`super::TYPING_REFRESH_INTERVAL`] and
/// [`super::FILLER_INTERVAL`], and ends the turn with [`Self::finalize`].
pub struct ProgressiveReply {
    sender: Arc<dyn ProgressiveSender>,
    channel: String,
    progressive_ui: bool,
    pub(super) state: StreamingState,
    typing: TypingState,
    output_context: Option<ProgressiveOutputContext>,
    output_sequence: u64,
    use_global_edit_latch: bool,
    text_limit: Option<(u64, tinychannels_bus::LengthUnit)>,
}

impl ProgressiveReply {
    /// Start a turn on `channel`. Progressive bubbles are enabled only when the
    /// channel's provider supports edit and delete.
    pub fn new(
        sender: Arc<dyn ProgressiveSender>,
        channel: impl Into<String>,
        capabilities: ChannelCapabilities,
    ) -> Self {
        let channel = channel.into();
        let progressive_ui = channel_supports_progressive_ui(&capabilities);
        tracing::debug!(
            "[channel-inbound] progressive reply channel='{}' progressive_ui={}",
            channel,
            progressive_ui
        );
        Self {
            sender,
            channel,
            progressive_ui,
            state: StreamingState::default(),
            typing: TypingState::default(),
            output_context: None,
            output_sequence: 0,
            use_global_edit_latch: true,
            text_limit: capabilities
                .text
                .text_limit
                .map(|limit| (limit, capabilities.text.length_unit)),
        }
    }

    /// Start a turn with its complete host-authoritative address and identity.
    pub fn new_scoped(
        sender: Arc<dyn ProgressiveSender>,
        channel: impl Into<String>,
        capabilities: ChannelCapabilities,
        context: ProgressiveOutputContext,
    ) -> Self {
        let mut reply = Self::new(sender, channel, capabilities);
        reply.output_context = Some(context);
        reply.use_global_edit_latch = false;
        reply
    }

    /// The channel this turn replies to.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Forward one canonical output event to the host event adapter.
    pub async fn on_output_event(
        &mut self,
        event: &tinychannels_bus::module_progressive::ChannelOutputEvent,
    ) -> Result<(), super::ProgressiveSendError> {
        self.output_sequence = self.output_sequence.saturating_add(1);
        self.sender
            .output_event(
                &self.channel,
                self.output_context.as_ref(),
                self.output_sequence,
                event,
            )
            .await
    }

    /// Current per-turn state (for logging and tests).
    pub fn state(&self) -> &StreamingState {
        &self.state
    }

    /// Visible assistant text arrived.
    pub fn on_text_delta(&mut self, delta: &str) {
        self.state.content.push_str(delta);
        self.state.dirty = true;
    }

    /// A tool call started.
    pub fn on_tool_call(&mut self, tool_name: &str) {
        self.state.last_tool = Some(format!("🔧 {tool_name}…"));
        self.state.dirty = true;
    }

    /// A tool call finished.
    pub fn on_tool_result(&mut self, tool_name: &str, success: bool) {
        let mark = if success { "✓" } else { "✗" };
        self.state.last_tool = Some(format!("🔧 {tool_name} {mark}"));
        self.state.dirty = true;
    }

    /// Model reasoning arrived.
    pub fn on_thinking_delta(&mut self, delta: &str) {
        self.state.thinking_accumulator.push_str(delta);
        self.state.thinking_dirty = true;
    }

    /// Flush pending draft and thinking updates.
    pub async fn edit_tick(&mut self) {
        if !self.progressive_ui {
            return;
        }
        let sender = Arc::clone(&self.sender);
        if self.state.thinking_dirty && !self.state.thinking_edit_disabled {
            flush_thinking_message(
                sender.as_ref(),
                &self.channel,
                &mut self.state,
                self.use_global_edit_latch,
            )
            .await;
        }
        if self.state.dirty && !self.state.edit_disabled {
            flush_streaming_edit(
                sender.as_ref(),
                &self.channel,
                &mut self.state,
                self.use_global_edit_latch,
            )
            .await;
        }
    }

    /// Refresh the typing indicator.
    pub async fn typing_tick(&mut self) {
        let sender = Arc::clone(&self.sender);
        send_typing_indicator(sender.as_ref(), &self.channel, &mut self.typing).await;
    }

    /// Post a "still working" filler, where the channel can clean it up.
    pub async fn filler_tick(&mut self) {
        if !self.progressive_ui || self.state.filler_disabled {
            return;
        }
        let sender = Arc::clone(&self.sender);
        send_filler_message(sender.as_ref(), &self.channel, &mut self.state).await;
    }

    /// Deliver the canonical reply and remove every ephemeral bubble.
    pub async fn finalize(&mut self, final_text: &str) {
        let sender = Arc::clone(&self.sender);
        finalize_channel_reply(
            sender.as_ref(),
            &self.channel,
            &mut self.state,
            final_text,
            self.use_global_edit_latch,
        )
        .await;
    }

    /// Finish a text-only turn by editing the existing draft in place and
    /// return the receipt for that already delivered provider message.
    pub async fn finalize_existing_text(
        &mut self,
        final_text: &str,
    ) -> Result<Option<tinychannels_bus::channel::ChannelSendOutcome>, super::ProgressiveSendError>
    {
        let Some(message_id) = self.state.message_id.clone() else {
            return Ok(None);
        };
        let message_ids = if self.state.message_ids.is_empty() {
            vec![message_id.clone()]
        } else {
            self.state.message_ids.clone()
        };
        if !self.progressive_ui {
            return Ok(None);
        }
        match self
            .sender
            .edit(&self.channel, &message_id, final_text)
            .await
        {
            Ok(()) => {
                for id in message_ids.iter().skip(1) {
                    super::finalize::delete_channel_message(
                        self.sender.as_ref(),
                        &self.channel,
                        id,
                    )
                    .await;
                }
                for id in std::mem::take(&mut self.state.filler_message_ids) {
                    super::finalize::delete_channel_message(
                        self.sender.as_ref(),
                        &self.channel,
                        &id,
                    )
                    .await;
                }
                if let Some(id) = self.state.thinking_message_id.take() {
                    super::finalize::delete_channel_message(
                        self.sender.as_ref(),
                        &self.channel,
                        &id,
                    )
                    .await;
                }
                self.state.content = final_text.to_owned();
                self.state.dirty = false;
                self.state.message_ids = vec![message_id.clone()];
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                outcome.receipt.primary_platform_message_id = Some(message_id.clone());
                outcome.receipt.platform_message_ids.push(message_id);
                Ok(Some(outcome))
            }
            Err(error) => {
                self.abort().await;
                Err(error)
            }
        }
    }

    /// Close local drafts without sending another message after an ambiguous
    /// or failed event delivery.
    pub async fn abort(&mut self) {
        abort_channel_reply(self.sender.as_ref(), &self.channel, &mut self.state).await;
    }

    /// Deliver ordered typed content through the provider send port, then
    /// clean up drafts. The returned outcome is the provider's retained receipt.
    pub async fn send_parts(
        &mut self,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, super::ProgressiveSendError> {
        let text_only = parts.iter().all(|part| {
            matches!(
                part,
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text { .. }
                }
            )
        });
        let segments = match self.text_limit.filter(|_| text_only) {
            Some((limit, unit)) => split_text_by_limit(text, limit, unit)?,
            None => vec![text.to_owned()],
        };
        let dialect = parts.iter().find_map(|part| match part {
            tinychannels_bus::OutboundContentPart::Content {
                part:
                    tinychannels_bus::ContentPart::Text {
                        markdown_dialect, ..
                    },
            } => *markdown_dialect,
            _ => None,
        });
        let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
        for (index, segment) in segments.iter().enumerate() {
            let segment_parts = if text_only && segments.len() > 1 {
                vec![tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: segment.clone(),
                        markdown_dialect: dialect,
                    },
                }]
            } else {
                parts.to_vec()
            };
            let next = match self
                .sender
                .send_parts_with_segment(
                    &self.channel,
                    &segment_parts,
                    segment,
                    self.output_context.as_ref(),
                    self.output_sequence,
                    index as u32,
                )
                .await
            {
                Ok(next) => next,
                Err(super::ProgressiveSendError::PartialOutcome(next)) => next,
                Err(error) if index > 0 => {
                    outcome.error = Some(tinychannels_bus::channel::ChannelSendError {
                        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                        message: error.to_string(),
                        sent_before_error: true,
                        ..Default::default()
                    });
                    break;
                }
                Err(error) => return Err(error),
            };
            let no_receipt = next.receipt.platform_message_ids.is_empty()
                && next.receipt.parts.is_empty()
                && next.error.is_none();
            merge_receipt(&mut outcome.receipt, next.receipt, index == 0, index);
            if next.error.is_some() {
                outcome.error = next.error;
                break;
            }
            if no_receipt {
                outcome.error = Some(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                    message: "provider accepted a progressive send without a receipt id".into(),
                    sent_before_error: true,
                    ..Default::default()
                });
                break;
            }
        }
        abort_channel_reply(self.sender.as_ref(), &self.channel, &mut self.state).await;
        Ok(outcome)
    }

    /// Deliver typed content during a turn without finalizing its text draft.
    pub async fn send_content_parts(
        &self,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
    ) -> Result<tinychannels_bus::channel::ChannelSendOutcome, super::ProgressiveSendError> {
        self.sender
            .send_parts_with_segment(
                &self.channel,
                parts,
                text,
                self.output_context.as_ref(),
                self.output_sequence,
                0,
            )
            .await
    }

    /// Send a standalone reply without touching the turn's draft state (for a
    /// turn that failed to start).
    pub async fn send_reply(&self, text: &str) {
        send_channel_reply(self.sender.as_ref(), &self.channel, text).await;
    }
}

fn split_text_by_limit(
    text: &str,
    limit: u64,
    unit: tinychannels_bus::LengthUnit,
) -> Result<Vec<String>, super::ProgressiveSendError> {
    if limit == 0 {
        return Err(super::ProgressiveSendError::Other(anyhow::anyhow!(
            "configured text limit is zero"
        )));
    }
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut used = 0u64;
    for scalar in text.chars() {
        let cost = match unit {
            tinychannels_bus::LengthUnit::Characters => 1,
            tinychannels_bus::LengthUnit::Utf8Bytes => scalar.len_utf8() as u64,
            tinychannels_bus::LengthUnit::Utf16Units => scalar.len_utf16() as u64,
        };
        if cost > limit {
            return Err(super::ProgressiveSendError::Other(anyhow::anyhow!(
                "one text scalar exceeds configured text limit"
            )));
        }
        if used > 0 && used + cost > limit {
            segments.push(std::mem::take(&mut current));
            used = 0;
        }
        current.push(scalar);
        used += cost;
    }
    if !current.is_empty() || segments.is_empty() {
        segments.push(current);
    }
    Ok(segments)
}

fn merge_receipt(
    aggregate: &mut tinychannels_bus::channel::MessageReceipt,
    next: tinychannels_bus::channel::MessageReceipt,
    first: bool,
    segment_index: usize,
) {
    if first {
        *aggregate = next;
        return;
    }
    if next.parts.is_empty() && (next.edit_token.is_some() || next.delete_token.is_some()) {
        for (offset, platform_message_id) in next.platform_message_ids.iter().enumerate() {
            aggregate
                .parts
                .push(tinychannels_bus::channel::MessageReceiptPart {
                    platform_message_id: platform_message_id.clone(),
                    kind: tinychannels_bus::channel::MessageReceiptPartKind::Text,
                    index: segment_index + offset,
                    edit_token: next.edit_token.clone(),
                    delete_token: next.delete_token.clone(),
                    ..Default::default()
                });
        }
    }
    aggregate
        .platform_message_ids
        .extend(next.platform_message_ids);
    aggregate.parts.extend(next.parts);
    if aggregate.primary_platform_message_id.is_none() {
        aggregate.primary_platform_message_id = next.primary_platform_message_id;
    }
    if aggregate.channel.is_none() {
        aggregate.channel = next.channel;
    }
    if aggregate.conversation.is_none() {
        aggregate.conversation = next.conversation;
    }
    if aggregate.idempotency_key.is_none() {
        aggregate.idempotency_key = next.idempotency_key;
    }
    if aggregate.thread_id.is_none() {
        aggregate.thread_id = next.thread_id;
    }
    if aggregate.reply_to_id.is_none() {
        aggregate.reply_to_id = next.reply_to_id;
    }
    if aggregate.edit_token.is_none() {
        aggregate.edit_token = next.edit_token;
    }
    if aggregate.delete_token.is_none() {
        aggregate.delete_token = next.delete_token;
    }
    aggregate.sent_at = aggregate.sent_at.max(next.sent_at);
    if let Some(mut raw) = next.raw {
        aggregate.raw.get_or_insert_with(Vec::new).append(&mut raw);
    }
}
