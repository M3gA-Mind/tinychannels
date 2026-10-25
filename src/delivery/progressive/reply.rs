//! [`ProgressiveReply`]: one turn's progressive delivery, driven by the host.

use std::sync::Arc;

use super::bubbles::{flush_thinking_message, send_filler_message, send_typing_indicator};
use super::draft::flush_streaming_edit;
use super::finalize::{
    abort_channel_reply, cleanup_after_external_reply, finalize_channel_reply, send_channel_reply,
};
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
    native_approval: bool,
    pub(super) state: StreamingState,
    typing: TypingState,
    output_context: Option<ProgressiveOutputContext>,
    output_sequence: u64,
    use_global_edit_latch: bool,
    text_limit: Option<(u64, tinychannels_bus::LengthUnit)>,
    allow_receiptless_delivery: bool,
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
        let native_approval = capabilities.chat_approvals
            && capabilities.content.buttons.outbound
            && capabilities.content.presentation.outbound;
        tracing::debug!(
            "[channel-inbound] progressive reply channel='{}' progressive_ui={}",
            channel,
            progressive_ui
        );
        Self {
            sender,
            channel,
            progressive_ui,
            native_approval,
            state: StreamingState::default(),
            typing: TypingState::default(),
            output_context: None,
            output_sequence: 0,
            use_global_edit_latch: true,
            text_limit: capabilities
                .text
                .text_limit
                .map(|limit| (limit, capabilities.text.length_unit)),
            allow_receiptless_delivery: false,
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

    /// Start a scoped turn whose trusted host callback accepts delivery while
    /// the provider receipt arrives asynchronously. The module callback must
    /// use the distinct `Accepted` result; an empty provider `Outcome` remains
    /// an error.
    pub fn new_hosted_scoped(
        sender: Arc<dyn ProgressiveSender>,
        channel: impl Into<String>,
        capabilities: ChannelCapabilities,
        context: ProgressiveOutputContext,
    ) -> Self {
        let mut reply = Self::new_scoped(sender, channel, capabilities, context);
        reply.allow_receiptless_delivery = true;
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

    /// Apply one canonical progress event to this shared delivery engine.
    /// Output observation is a separate call so its acknowledgement can never
    /// be mistaken for a provider-send receipt.
    pub async fn apply_output_event(
        &mut self,
        event: &tinychannels_bus::module_progressive::ChannelOutputEvent,
        deliver_typed_content: bool,
    ) -> Result<super::ProgressiveOutputDispatch, super::ProgressiveSendError> {
        use tinychannels_bus::module_progressive::ChannelOutputEvent as Event;
        let mut dispatch = super::ProgressiveOutputDispatch::default();
        match event {
            Event::TextDelta { text, .. } => self.on_text_delta(text),
            Event::ThinkingDelta { text, .. } => self.on_thinking_delta(text),
            Event::ToolProgress { label, message } => {
                self.on_tool_progress(message.as_deref().unwrap_or(label))
            }
            Event::ToolCallStarted {
                tool_name,
                display_label,
                ..
            } => self.on_tool_call(display_label.as_deref().unwrap_or(tool_name)),
            Event::ToolCallProgress { call_id, message } => {
                let _ = call_id;
                self.on_tool_progress(message)
            }
            Event::ToolCallCompleted {
                tool_name,
                success,
                display_label,
                ..
            } => self.on_tool_result(display_label.as_deref().unwrap_or(tool_name), *success),
            Event::ApprovalRequest {
                id,
                prompt,
                choices,
            } if deliver_typed_content => {
                if self.native_approval {
                    let part = tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Presentation {
                            blocks: serde_json::json!({
                                "kind": "approval",
                                "id": id,
                                "prompt": prompt,
                                "choices": choices,
                            }),
                        },
                    };
                    match self
                        .send_content_parts(std::slice::from_ref(&part), prompt)
                        .await
                    {
                        Ok(outcome) => dispatch.push_outcome(outcome),
                        Err(super::ProgressiveSendError::UnsupportedContent) => {
                            dispatch.unsupported.push(part)
                        }
                        Err(error) => return Err(error),
                    }
                    return Ok(dispatch);
                }
                let mut text = format!("Approval requested: {prompt}");
                if !choices.is_empty() {
                    text.push_str("\nChoices: ");
                    text.push_str(&choices.join(", "));
                }
                self.dispatch_notice(&text, &mut dispatch).await?;
            }
            Event::Clarification { prompt } if deliver_typed_content => {
                self.dispatch_notice(prompt, &mut dispatch).await?;
            }
            Event::Media { text, parts } if deliver_typed_content => {
                let mut outbound = Vec::new();
                if let Some(text) = text {
                    outbound.push(tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text: text.clone(),
                            markdown_dialect: None,
                        },
                    });
                }
                outbound.extend(
                    parts
                        .iter()
                        .cloned()
                        .map(|part| tinychannels_bus::OutboundContentPart::Content { part }),
                );
                match self.send_content_parts(&outbound, "").await {
                    Ok(outcome) => dispatch.push_outcome(outcome),
                    Err(super::ProgressiveSendError::UnsupportedContent) => {
                        dispatch.unsupported.extend(outbound)
                    }
                    Err(error) => return Err(error),
                }
            }
            Event::VoiceNote { text, voice } if deliver_typed_content => {
                let mut outbound = Vec::new();
                if let Some(text) = text.clone().or_else(|| {
                    voice
                        .transcript
                        .as_ref()
                        .map(|transcript| transcript.text.clone())
                }) {
                    outbound.push(tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text,
                            markdown_dialect: None,
                        },
                    });
                }
                outbound.push(tinychannels_bus::OutboundContentPart::Voice {
                    voice: tinychannels_bus::OutboundVoice::Media {
                        media: voice.media.clone(),
                        codec: voice.codec.clone(),
                        duration_ms: voice.duration_ms,
                    },
                });
                match self.send_content_parts(&outbound, "").await {
                    Ok(outcome) => dispatch.push_outcome(outcome),
                    Err(super::ProgressiveSendError::UnsupportedContent) => {
                        dispatch.unsupported.extend(outbound)
                    }
                    Err(error) => return Err(error),
                }
            }
            _ => {}
        }
        Ok(dispatch)
    }

    /// Consume the canonical terminal event and retain the real provider
    /// receipt. Text-only output edits an existing draft when possible;
    /// ordered typed parts remain authoritative when present.
    pub async fn finalize_output_event(
        &mut self,
        event: &tinychannels_bus::module_progressive::ChannelOutputEvent,
    ) -> Result<super::ProgressiveOutputDispatch, super::ProgressiveSendError> {
        let tinychannels_bus::module_progressive::ChannelOutputEvent::FinalMessage {
            text,
            parts,
            ..
        } = event
        else {
            return Ok(super::ProgressiveOutputDispatch::default());
        };
        let mut dispatch = super::ProgressiveOutputDispatch::default();
        if self.state.delivery_uncertain {
            let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
            if let Some(receipt) = &self.state.message_receipt {
                outcome.receipt = receipt.clone();
            }
            outcome
                .error
                .get_or_insert_with(|| tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                    message: "an earlier progressive send has an uncertain outcome".into(),
                    sent_before_error: true,
                    ..Default::default()
                });
            return Err(super::ProgressiveSendError::PartialOutcome(Box::new(
                outcome,
            )));
        }
        let should_synthesize = parts.is_empty()
            && self
                .sender
                .should_synthesize_final(&self.channel, self.output_context.as_ref(), text)
                .await?;
        if parts.is_empty()
            && !should_synthesize
            && let Some(outcome) = self.finalize_existing_text(text).await?
        {
            dispatch.push_outcome(outcome);
            return Ok(dispatch);
        }
        let parts = if parts.is_empty() {
            vec![tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: text.clone(),
                    markdown_dialect: None,
                },
            }]
        } else {
            parts.clone()
        };
        match self.send_parts(&parts, text).await {
            Ok(outcome) => dispatch.push_outcome(outcome),
            Err(super::ProgressiveSendError::UnsupportedContent) => {
                dispatch.unsupported.extend(parts)
            }
            Err(error) => return Err(error),
        }
        Ok(dispatch)
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

    /// A tool is still running; keep its status active in the draft.
    pub fn on_tool_progress(&mut self, message: &str) {
        self.state.last_tool = Some(format!("🔧 {message}…"));
        self.state.dirty = true;
    }

    async fn dispatch_notice(
        &self,
        text: &str,
        dispatch: &mut super::ProgressiveOutputDispatch,
    ) -> Result<(), super::ProgressiveSendError> {
        let part = tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: text.to_owned(),
                markdown_dialect: None,
            },
        };
        match self
            .send_content_parts(std::slice::from_ref(&part), text)
            .await
        {
            Ok(outcome) => dispatch.push_outcome(outcome),
            Err(super::ProgressiveSendError::UnsupportedContent) => dispatch.unsupported.push(part),
            Err(error) => return Err(error),
        }
        Ok(())
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
        let edit = match self.state.message_receipt.as_ref() {
            Some(receipt) => {
                self.sender
                    .edit_with_receipt(&self.channel, receipt, final_text)
                    .await
            }
            None => {
                self.sender
                    .edit(&self.channel, &message_id, final_text)
                    .await
            }
        };
        match edit {
            Ok(()) => {
                for id in message_ids.iter().skip(1) {
                    let part_receipt = self
                        .state
                        .message_receipt
                        .as_ref()
                        .map(|receipt| super::finalize::receipt_for_part(receipt, id));
                    super::finalize::delete_channel_message_with_receipt(
                        self.sender.as_ref(),
                        &self.channel,
                        id,
                        part_receipt.as_ref(),
                    )
                    .await;
                }
                super::finalize::delete_ephemeral_messages(
                    self.sender.as_ref(),
                    &self.channel,
                    std::mem::take(&mut self.state.filler_message_ids),
                    std::mem::take(&mut self.state.filler_message_receipts),
                )
                .await;
                let mut thinking_ids = std::mem::take(&mut self.state.thinking_message_ids);
                if let Some(id) = self.state.thinking_message_id.take()
                    && !thinking_ids.contains(&id)
                {
                    thinking_ids.push(id);
                }
                let thinking_receipts = std::mem::take(&mut self.state.thinking_message_receipt)
                    .into_iter()
                    .collect();
                super::finalize::delete_ephemeral_messages(
                    self.sender.as_ref(),
                    &self.channel,
                    thinking_ids,
                    thinking_receipts,
                )
                .await;
                self.state.content = final_text.to_owned();
                self.state.dirty = false;
                self.state.message_ids = vec![message_id.clone()];
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome {
                    receipt: self.state.message_receipt.clone().unwrap_or_default(),
                    ..Default::default()
                };
                if outcome.receipt.primary_platform_message_id.is_none() {
                    outcome.receipt.primary_platform_message_id = Some(message_id.clone());
                }
                if outcome.receipt.platform_message_ids.is_empty() {
                    outcome.receipt.platform_message_ids.push(message_id);
                }
                self.state.message_receipt = Some(outcome.receipt.clone());
                Ok(Some(outcome))
            }
            Err(error) => {
                let mut outcome = match error {
                    super::ProgressiveSendError::PartialOutcome(outcome) => *outcome,
                    super::ProgressiveSendError::Unavailable => progressive_edit_error(
                        tinychannels_bus::channel::SendErrorKind::Transient,
                        "progressive edit was unavailable",
                        true,
                        false,
                    ),
                    super::ProgressiveSendError::EditUnsupported => progressive_edit_error(
                        tinychannels_bus::channel::SendErrorKind::Unknown,
                        "progressive edit is unsupported",
                        false,
                        false,
                    ),
                    super::ProgressiveSendError::MessageGone => progressive_edit_error(
                        tinychannels_bus::channel::SendErrorKind::NotFound,
                        "progressive draft no longer exists",
                        false,
                        false,
                    ),
                    _ => progressive_edit_error(
                        tinychannels_bus::channel::SendErrorKind::Unknown,
                        "progressive edit outcome is uncertain",
                        false,
                        true,
                    ),
                };
                let mut retained = self.state.message_receipt.clone().unwrap_or_default();
                if retained.primary_platform_message_id.is_none() {
                    retained.primary_platform_message_id = Some(message_id.clone());
                }
                if retained.platform_message_ids.is_empty() {
                    retained.platform_message_ids = message_ids.clone();
                }
                let mut merged = tinychannels_bus::channel::MessageReceipt::default();
                merge_receipt(&mut merged, retained, true, 0);
                merge_receipt(&mut merged, outcome.receipt, false, message_ids.len());
                outcome.receipt = merged;
                outcome
                    .error
                    .get_or_insert_with(|| tinychannels_bus::channel::ChannelSendError {
                        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                        message: "progressive edit failed".into(),
                        sent_before_error: true,
                        ..Default::default()
                    });
                self.abort().await;
                Err(super::ProgressiveSendError::PartialOutcome(Box::new(
                    outcome,
                )))
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
        let segments = match self.text_limit {
            Some((limit, unit)) => segment_text_parts(parts, text, limit, unit)?,
            None => vec![(parts.to_vec(), text.to_owned())],
        };
        let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
        for (index, (segment_parts, segment_text)) in segments.iter().enumerate() {
            let next = match self
                .sender
                .send_final_parts_with_segment(
                    &self.channel,
                    segment_parts,
                    segment_text,
                    self.output_context.as_ref(),
                    self.output_sequence,
                    index as u32,
                )
                .await
            {
                Ok(next) => next,
                Err(super::ProgressiveSendError::PartialOutcome(next)) => *next,
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
            let no_receipt = next.receipt.primary_platform_message_id.is_none()
                && next.receipt.platform_message_ids.is_empty()
                && next.receipt.parts.is_empty()
                && next.error.is_none();
            merge_receipt(&mut outcome.receipt, next.receipt, index == 0, index);
            if next.error.is_some() {
                outcome.error = next.error;
                break;
            }
            if no_receipt && !self.allow_receiptless_delivery {
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
        let mut outcome = self
            .sender
            .send_parts_with_segment(
                &self.channel,
                parts,
                text,
                self.output_context.as_ref(),
                self.output_sequence,
                0,
            )
            .await?;
        if outcome.error.is_none()
            && !self.allow_receiptless_delivery
            && outcome.receipt.primary_platform_message_id.is_none()
            && outcome.receipt.platform_message_ids.is_empty()
            && outcome.receipt.parts.is_empty()
        {
            outcome.error = Some(tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                message: "provider accepted a progressive send without a receipt id".into(),
                sent_before_error: true,
                ..Default::default()
            });
        }
        Ok(outcome)
    }

    /// Clean transient UI after a final message was delivered by another
    /// transport path. A successful delivery also removes the now-stale draft;
    /// a failed delivery leaves it visible while still removing filler and
    /// thinking bubbles.
    pub async fn cleanup_after_external_final(&mut self, final_delivered: bool) {
        let sender = Arc::clone(&self.sender);
        cleanup_after_external_reply(
            sender.as_ref(),
            &self.channel,
            &mut self.state,
            final_delivered,
        )
        .await;
    }

    /// Send a standalone reply without touching the turn's draft state (for a
    /// turn that failed to start).
    pub async fn send_reply(&self, text: &str) {
        send_channel_reply(self.sender.as_ref(), &self.channel, text).await;
    }
}

pub(super) fn split_text_by_limit(
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

pub(super) fn segment_text_parts(
    parts: &[tinychannels_bus::OutboundContentPart],
    projection: &str,
    limit: u64,
    unit: tinychannels_bus::LengthUnit,
) -> Result<Vec<(Vec<tinychannels_bus::OutboundContentPart>, String)>, super::ProgressiveSendError>
{
    let text_parts = parts
        .iter()
        .map(|part| match part {
            tinychannels_bus::OutboundContentPart::Content {
                part:
                    tinychannels_bus::ContentPart::Text {
                        text,
                        markdown_dialect,
                    },
            } => Some((text.as_str(), markdown_dialect.clone())),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    let Some(text_parts) = text_parts else {
        return Ok(vec![(parts.to_vec(), projection.to_owned())]);
    };
    if parts.is_empty() {
        return split_text_by_limit(projection, limit, unit).map(|chunks| {
            chunks
                .into_iter()
                .map(|chunk| {
                    let part = tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text: chunk.clone(),
                            markdown_dialect: None,
                        },
                    };
                    (vec![part], chunk)
                })
                .collect()
        });
    }
    let combined_size = text_parts
        .iter()
        .map(|(text, _)| measure_text(text, unit))
        .sum::<u64>();
    if combined_size <= limit {
        return Ok(vec![(parts.to_vec(), projection.to_owned())]);
    }
    let mut segments = Vec::new();
    for (text, dialect) in text_parts {
        for chunk in split_text_by_limit(text, limit, unit)? {
            let part = tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: chunk.clone(),
                    markdown_dialect: dialect.clone(),
                },
            };
            segments.push((vec![part], chunk));
        }
    }
    if segments.is_empty() {
        segments.push((parts.to_vec(), projection.to_owned()));
    }
    Ok(segments)
}

fn measure_text(text: &str, unit: tinychannels_bus::LengthUnit) -> u64 {
    match unit {
        tinychannels_bus::LengthUnit::Characters => text.chars().count() as u64,
        tinychannels_bus::LengthUnit::Utf8Bytes => text.len() as u64,
        tinychannels_bus::LengthUnit::Utf16Units => text.encode_utf16().count() as u64,
    }
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

fn progressive_edit_error(
    kind: tinychannels_bus::channel::SendErrorKind,
    message: &str,
    retryable: bool,
    sent_before_error: bool,
) -> tinychannels_bus::channel::ChannelSendOutcome {
    tinychannels_bus::channel::ChannelSendOutcome {
        error: Some(tinychannels_bus::channel::ChannelSendError {
            kind,
            message: message.to_owned(),
            retryable,
            sent_before_error,
            ..Default::default()
        }),
        ..Default::default()
    }
}
