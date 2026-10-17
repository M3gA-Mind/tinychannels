//! Posting and progressively editing the evolving draft reply bubble.

use super::latch::{channel_edits_unsupported, mark_channel_edits_unsupported};
use super::state::{
    StreamingState, extract_message_ids, extract_message_receipt, extract_message_receipt_or_ids,
    message_receipt_ids,
};
use super::{MAX_EDIT_FAILURES, ProgressiveSendError, ProgressiveSender};

/// Post or edit the draft carrying the latest buffered text. The first call
/// posts a new message and records its id; later calls edit it.
pub(super) async fn flush_streaming_edit(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
    use_global_edit_latch: bool,
) {
    let draft = state.compose_draft();
    if draft.is_empty() {
        return;
    }
    state.dirty = false;
    if state.edit_disabled || state.delivery_uncertain {
        return;
    }

    if let Some(message_id) = state.message_id.clone() {
        // Known-missing edit route: skip the guaranteed 404 and let
        // finalization replace the draft (delete + fresh atomic reply).
        if use_global_edit_latch && channel_edits_unsupported(channel) {
            tracing::debug!(
                "[channel-inbound][stream] skipping edit channel='{}' msg_id={} — no edit route on this backend, draft stays as-is until finalize",
                channel,
                message_id,
            );
            state.latch_draft_edits_unsupported();
            return;
        }
        let edit = match state.message_receipt.as_ref() {
            Some(receipt) => sender.edit_with_receipt(channel, receipt, &draft).await,
            None => sender.edit(channel, &message_id, &draft).await,
        };
        match edit {
            Ok(()) => {
                tracing::debug!(
                    "[channel-inbound][stream] edit ok channel='{}' msg_id={} chars={}",
                    channel,
                    message_id,
                    draft.len(),
                );
                state.edit_failures = 0;
            }
            Err(ProgressiveSendError::Unavailable) => {}
            Err(ProgressiveSendError::EditUnsupported) => {
                // Keep `message_id`: the draft is still on screen and
                // finalization must be able to delete it (#5230).
                tracing::info!(
                    "[channel-inbound][stream] edit channel='{}' msg_id={} — backend has no edit route, keeping id for finalize cleanup and disabling progressive edits",
                    channel,
                    message_id,
                );
                if use_global_edit_latch {
                    mark_channel_edits_unsupported(channel);
                }
                state.latch_draft_edits_unsupported();
            }
            Err(ProgressiveSendError::MessageGone) => {
                tracing::info!(
                    "[channel-inbound][stream] edit channel='{}' msg_id={} — message gone provider-side (404), clearing stale id and disabling further edits",
                    channel,
                    message_id,
                );
                state.forget_draft();
            }
            Err(ProgressiveSendError::Other(_)) => {
                state.edit_failures += 1;
                tracing::warn!(
                    "[channel-inbound][stream] edit failed channel='{}' msg_id={} (failures={}/{})",
                    channel,
                    message_id,
                    state.edit_failures,
                    MAX_EDIT_FAILURES,
                );
                if state.edit_failures >= MAX_EDIT_FAILURES {
                    tracing::info!(
                        "[channel-inbound][stream] giving up on progressive edits for channel='{}', falling back to atomic delivery",
                        channel,
                    );
                    state.edit_disabled = true;
                }
            }
            Err(ProgressiveSendError::PartialOutcome(_)) => {
                tracing::error!(
                    "[channel-inbound][stream] edit returned an uncertain outcome; retaining draft id and disabling edits channel='{}' msg_id={}",
                    channel,
                    message_id,
                );
                state.latch_draft_edits_unsupported();
            }
            Err(ProgressiveSendError::UnsupportedContent) => {
                state.latch_draft_edits_unsupported();
            }
        }
        return;
    }

    match sender.send(channel, &draft).await {
        Ok(resp) => {
            // A message reached the user: record that before looking for an
            // id, so finalize never posts a second bubble.
            state.draft_sent = true;
            state.message_receipt =
                extract_message_receipt_or_ids(&resp).or_else(|| extract_message_receipt(&resp));
            let ids = state
                .message_receipt
                .as_ref()
                .map(message_receipt_ids)
                .unwrap_or_else(|| extract_message_ids(&resp));
            if let Some(id) = ids.first().cloned() {
                tracing::debug!(
                    "[channel-inbound][stream] initial draft sent channel='{}' msg_id={}",
                    channel,
                    id,
                );
                state.message_id = Some(id.clone());
                state.message_ids = ids;
                if state.message_receipt.is_none() {
                    state.message_receipt = Some(tinychannels_bus::channel::MessageReceipt {
                        primary_platform_message_id: Some(id.clone()),
                        platform_message_ids: state.message_ids.clone(),
                        ..Default::default()
                    });
                }
                if state.message_ids.len() > 1 {
                    // A draft assembled from several provider messages is not
                    // safely editable through the single-ID legacy port.
                    state.edit_disabled = true;
                }
            } else {
                tracing::warn!(
                    "[channel-inbound][stream] initial draft response lacked an id — disabling progressive edits and preserving the response as uncertain channel='{}' receipt_parts={} response_fields={}",
                    channel,
                    state
                        .message_receipt
                        .as_ref()
                        .map_or(0, |receipt| receipt.parts.len()),
                    resp.as_object().map_or(0, serde_json::Map::len),
                );
                state.edit_disabled = true;
                state.delivery_uncertain = true;
            }
        }
        Err(ProgressiveSendError::Unavailable) => {}
        Err(ProgressiveSendError::PartialOutcome(outcome)) => {
            state.draft_sent = true;
            state.delivery_uncertain = true;
            state.edit_disabled = true;
            let receipt = outcome.receipt;
            let mut ids = receipt.platform_message_ids.clone();
            ids.extend(
                receipt
                    .parts
                    .iter()
                    .map(|part| part.platform_message_id.clone()),
            );
            ids.dedup();
            state.message_id = receipt
                .primary_platform_message_id
                .clone()
                .or_else(|| ids.first().cloned());
            state.message_ids = ids;
            state.message_receipt = Some(receipt);
        }
        Err(_err) => {
            state.draft_sent = true;
            state.delivery_uncertain = true;
            state.edit_disabled = true;
            tracing::warn!(
                "[channel-inbound][stream] initial send outcome is uncertain channel='{}'",
                channel,
            );
        }
    }
}
