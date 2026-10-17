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
            flush_thinking_message(sender.as_ref(), &self.channel, &mut self.state).await;
        }
        if self.state.dirty && !self.state.edit_disabled {
            flush_streaming_edit(sender.as_ref(), &self.channel, &mut self.state).await;
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
        finalize_channel_reply(sender.as_ref(), &self.channel, &mut self.state, final_text).await;
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
        let outcome = self
            .sender
            .send_parts(
                &self.channel,
                parts,
                text,
                self.output_context.as_ref(),
                self.output_sequence,
            )
            .await?;
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
            .send_parts(
                &self.channel,
                parts,
                text,
                self.output_context.as_ref(),
                self.output_sequence,
            )
            .await
    }

    /// Send a standalone reply without touching the turn's draft state (for a
    /// turn that failed to start).
    pub async fn send_reply(&self, text: &str) {
        send_channel_reply(self.sender.as_ref(), &self.channel, text).await;
    }
}
