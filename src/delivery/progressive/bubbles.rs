//! The ephemeral bubbles around a draft: typing indicator, "💭 Thinking:"
//! bubble and rotating "still working" fillers.

use super::latch::{channel_edits_unsupported, mark_channel_edits_unsupported};
use super::state::{
    StreamingState, TypingState, extract_message_ids, extract_message_receipt,
    extract_message_receipt_or_ids, latest_thinking_snippet, message_receipt_ids,
};
use super::{
    MAX_THINKING_DISPLAY_CHARS, MAX_TYPING_FAILURES, ProgressiveSendError, ProgressiveSender,
    STATIC_FILLERS,
};

/// Fire one typing indicator. Latches `disabled` after repeated failures so a
/// timer can keep calling this without piling up warnings.
pub(super) async fn send_typing_indicator(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut TypingState,
) {
    if state.disabled {
        return;
    }
    match sender.typing(channel).await {
        Ok(()) => {
            if state.failures > 0 {
                tracing::debug!(
                    "[channel-inbound][typing] recovered channel='{}' after {} failure(s)",
                    channel,
                    state.failures,
                );
            }
            state.failures = 0;
        }
        Err(ProgressiveSendError::Unavailable) => {}
        Err(_err) => {
            state.failures += 1;
            tracing::debug!(
                "[channel-inbound][typing] indicator failed channel='{}' (failures={}/{})",
                channel,
                state.failures,
                MAX_TYPING_FAILURES,
            );
            if state.failures >= MAX_TYPING_FAILURES {
                tracing::info!(
                    "[channel-inbound][typing] disabling typing indicator for channel='{}' — backend unsupported",
                    channel,
                );
                state.disabled = true;
            }
        }
    }
}

/// Render the thinking bubble text, truncated to the display cap.
pub(super) fn thinking_bubble_text(accumulator: &str) -> Option<String> {
    let trimmed = accumulator.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut snippet = trimmed.to_string();
    if snippet.len() > MAX_THINKING_DISPLAY_CHARS {
        let mut cut = MAX_THINKING_DISPLAY_CHARS;
        while !snippet.is_char_boundary(cut) {
            cut -= 1;
        }
        snippet.truncate(cut);
        snippet.push('…');
    }
    Some(format!("💭 Thinking:\n_{snippet}_"))
}

/// Post or edit the ephemeral thinking bubble. It is deleted at finalization.
pub(super) async fn flush_thinking_message(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
    use_global_edit_latch: bool,
) {
    state.thinking_dirty = false;
    let Some(text) = thinking_bubble_text(&state.thinking_accumulator) else {
        return;
    };

    if let Some(msg_id) = state.thinking_message_id.clone() {
        if use_global_edit_latch && channel_edits_unsupported(channel) {
            tracing::debug!(
                "[channel-inbound][thinking] skipping edit channel='{}' msg_id={} — no edit route on this backend, bubble stays until finalize deletes it",
                channel,
                msg_id,
            );
            state.latch_thinking_edits_unsupported();
            return;
        }
        let edit = match state.thinking_message_receipt.as_ref() {
            Some(receipt) => sender.edit_with_receipt(channel, receipt, &text).await,
            None => sender.edit(channel, &msg_id, &text).await,
        };
        match edit {
            Ok(()) | Err(ProgressiveSendError::Unavailable) => {}
            Err(ProgressiveSendError::EditUnsupported) => {
                // Keep the id so finalization still deletes the bubble (#5230).
                tracing::info!(
                    "[channel-inbound][thinking] edit channel='{}' msg_id={} — backend has no edit route, keeping id so finalize still deletes the bubble",
                    channel,
                    msg_id,
                );
                if use_global_edit_latch {
                    mark_channel_edits_unsupported(channel);
                }
                state.latch_thinking_edits_unsupported();
            }
            Err(ProgressiveSendError::MessageGone) => {
                tracing::info!(
                    "[channel-inbound][thinking] edit channel='{}' msg_id={} — thinking msg gone provider-side (404), clearing id and disabling further thinking edits",
                    channel,
                    msg_id,
                );
                state.forget_thinking();
            }
            Err(ProgressiveSendError::Other(_)) => {
                tracing::debug!(
                    "[channel-inbound][thinking] edit failed channel='{}' msg_id={}",
                    channel,
                    msg_id,
                );
            }
            Err(ProgressiveSendError::PartialOutcome(_)) => {
                tracing::error!(
                    "[channel-inbound][thinking] edit returned an uncertain outcome; retaining bubble id channel='{}' msg_id={}",
                    channel,
                    msg_id,
                );
                state.latch_thinking_edits_unsupported();
            }
            Err(ProgressiveSendError::UnsupportedContent) => {
                state.latch_thinking_edits_unsupported();
            }
        }
        return;
    }

    match sender.send(channel, &text).await {
        Ok(resp) => {
            state.thinking_sent = true;
            let receipt = extract_message_receipt_or_ids(&resp);
            let ids = receipt
                .as_ref()
                .map(message_receipt_ids)
                .unwrap_or_else(|| extract_message_ids(&resp));
            if let Some(id) = ids.first().cloned() {
                tracing::debug!(
                    "[channel-inbound][thinking] thinking msg sent channel='{}' msg_id={}",
                    channel,
                    id,
                );
                state.thinking_message_id = Some(id);
                state.thinking_message_ids = ids;
                state.thinking_message_receipt = receipt;
            } else {
                tracing::warn!(
                    "[channel-inbound][thinking] thinking msg sent but response lacked id — disabling further thinking flushes (message won't be deletable) channel='{}' receipt_parts={} response_fields={}",
                    channel,
                    extract_message_receipt(&resp)
                        .as_ref()
                        .map_or(0, |receipt| receipt.parts.len()),
                    resp.as_object().map_or(0, serde_json::Map::len),
                );
                state.thinking_edit_disabled = true;
                state.thinking_delivery_uncertain = true;
            }
        }
        Err(ProgressiveSendError::Unavailable) => {}
        Err(ProgressiveSendError::PartialOutcome(outcome)) => {
            retain_thinking_receipt(state, outcome.receipt);
            state.thinking_sent = true;
            state.thinking_delivery_uncertain = true;
            state.thinking_edit_disabled = true;
        }
        Err(_err) => {
            tracing::warn!(
                "[channel-inbound][thinking] failed to send thinking msg channel='{}' — disabling further thinking flushes",
                channel,
            );
            state.thinking_sent = true;
            state.thinking_delivery_uncertain = true;
            state.thinking_edit_disabled = true;
        }
    }
}

/// Choose the next filler: a fresh snippet of the reasoning stream when there
/// is one, otherwise the next entry of the static pool.
pub(super) fn next_filler_text(state: &mut StreamingState) -> String {
    match latest_thinking_snippet(state) {
        Some(snippet) if state.last_filler_snippet.as_deref() != Some(snippet.as_str()) => {
            let text = format!("💭 _{snippet}…_");
            state.last_filler_snippet = Some(snippet);
            text
        }
        _ => {
            let idx = state.filler_index % STATIC_FILLERS.len();
            state.filler_index = state.filler_index.wrapping_add(1);
            STATIC_FILLERS[idx].to_string()
        }
    }
}

/// Post a filler and record its id for deletion at finalization.
pub(super) async fn send_filler_message(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
) {
    const MAX_FILLER_SENDS_PER_TURN: u8 = 8;
    if state.filler_send_count >= MAX_FILLER_SENDS_PER_TURN {
        state.filler_disabled = true;
        return;
    }
    state.filler_send_count += 1;
    if state.filler_send_count >= MAX_FILLER_SENDS_PER_TURN {
        state.filler_disabled = true;
    }
    let text = next_filler_text(state);
    match sender.send(channel, &text).await {
        Ok(resp) => {
            state.filler_failures = 0;
            if let Some(receipt) = extract_message_receipt_or_ids(&resp) {
                let ids = message_receipt_ids(&receipt);
                state.filler_message_ids.extend(ids.iter().cloned());
                state.filler_message_receipts.push(receipt);
                let id = ids.first().cloned().expect("receipt has an id");
                tracing::debug!(
                    "[channel-inbound][filler] sent channel='{}' len={} msg_id={}",
                    channel,
                    text.len(),
                    id,
                );
            } else {
                tracing::warn!(
                    "[channel-inbound][filler] sent but response lacked id — cannot clean up on finalize channel='{}' receipt_parts={} response_fields={}",
                    channel,
                    extract_message_receipt(&resp)
                        .as_ref()
                        .map_or(0, |receipt| receipt.parts.len()),
                    resp.as_object().map_or(0, serde_json::Map::len),
                );
                state.filler_delivery_uncertain = true;
                state.filler_disabled = true;
            }
        }
        Err(ProgressiveSendError::Unavailable) => {}
        Err(ProgressiveSendError::PartialOutcome(outcome)) => {
            let ids = message_receipt_ids(&outcome.receipt);
            state.filler_message_ids.extend(ids);
            if !message_receipt_ids(&outcome.receipt).is_empty() {
                state.filler_message_receipts.push(outcome.receipt);
            }
            state.filler_delivery_uncertain = true;
            state.filler_disabled = true;
        }
        Err(_err) => {
            state.filler_delivery_uncertain = true;
            state.filler_disabled = true;
            tracing::warn!(
                "[channel-inbound][filler] send outcome uncertain; disabling future fillers channel='{}'",
                channel,
            );
        }
    }
}

fn retain_thinking_receipt(
    state: &mut StreamingState,
    receipt: tinychannels_bus::channel::MessageReceipt,
) {
    let ids = message_receipt_ids(&receipt);
    state.thinking_message_id = ids.first().cloned();
    state.thinking_message_ids = ids;
    if !state.thinking_message_ids.is_empty() {
        state.thinking_message_receipt = Some(receipt);
    }
}
