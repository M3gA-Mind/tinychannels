//! Delivering the canonical reply and cleaning up ephemeral bubbles.

use super::latch::{channel_edits_unsupported, mark_channel_edits_unsupported};
use super::state::StreamingState;
use super::{ProgressiveSendError, ProgressiveSender};

/// Send a reply as a fresh message, logging the outcome.
pub(super) async fn send_channel_reply(sender: &dyn ProgressiveSender, channel: &str, text: &str) {
    match sender.send(channel, text).await {
        Ok(resp) => {
            tracing::info!(
                "[channel-inbound] reply sent to channel='{}' response_fields={}",
                channel,
                resp.as_object().map_or(0, serde_json::Map::len)
            );
        }
        Err(ProgressiveSendError::Unavailable) => {
            tracing::error!(
                "[channel-inbound] cannot reply to channel='{}' — sender unavailable",
                channel
            );
        }
        Err(e) => {
            tracing::error!(
                "[channel-inbound] failed to send reply to channel='{}': {}",
                channel,
                e
            );
        }
    }
}

/// Delete a posted message (an ephemeral bubble or an orphaned draft).
pub(super) async fn delete_channel_message(
    sender: &dyn ProgressiveSender,
    channel: &str,
    message_id: &str,
) {
    delete_channel_message_with_receipt(sender, channel, message_id, None).await;
}

pub(super) async fn delete_channel_message_with_receipt(
    sender: &dyn ProgressiveSender,
    channel: &str,
    message_id: &str,
    receipt: Option<&tinychannels_bus::channel::MessageReceipt>,
) {
    let result = match receipt {
        Some(receipt) => sender.delete_with_receipt(channel, receipt).await,
        None => sender.delete(channel, message_id).await,
    };
    match result {
        Ok(()) => {
            tracing::info!(
                "[channel-inbound] deleted ephemeral msg channel='{}' msg_id={}",
                channel,
                message_id,
            );
        }
        Err(ProgressiveSendError::Unavailable) => {}
        Err(ProgressiveSendError::MessageGone) => {
            tracing::info!(
                "[channel-inbound] delete channel='{}' msg_id={} — message already gone provider-side (404), nothing to clean up",
                channel,
                message_id,
            );
        }
        Err(_err) => {
            tracing::warn!(
                "[channel-inbound] failed to delete ephemeral msg channel='{}' msg_id={}",
                channel,
                message_id,
            );
        }
    }
}

/// Delete an ephemeral output and preserve per-part provider authority.
pub(super) async fn delete_ephemeral_messages(
    sender: &dyn ProgressiveSender,
    channel: &str,
    message_ids: Vec<String>,
    receipts: Vec<tinychannels_bus::channel::MessageReceipt>,
) {
    let mut deleted = Vec::new();
    for receipt in receipts {
        for message_id in super::state::message_receipt_ids(&receipt) {
            if deleted.contains(&message_id) {
                continue;
            }
            let part_receipt = receipt_for_part(&receipt, &message_id);
            delete_channel_message_with_receipt(sender, channel, &message_id, Some(&part_receipt))
                .await;
            deleted.push(message_id);
        }
    }
    for message_id in message_ids {
        if !deleted.contains(&message_id) {
            delete_channel_message(sender, channel, &message_id).await;
            deleted.push(message_id);
        }
    }
}

/// Replace an on-screen draft: delete it, then post the reply fresh.
async fn replace_draft(
    sender: &dyn ProgressiveSender,
    channel: &str,
    message_id: &str,
    final_text: &str,
) {
    delete_channel_message(sender, channel, message_id).await;
    send_channel_reply(sender, channel, final_text).await;
}

/// Deliver the final canonical reply, then delete fillers and the thinking
/// bubble.
///
/// **Invariant**: once a draft is on the user's screen, never leave it next to
/// a second copy of the reply. With an id we edit it one last time (or delete
/// and replace it when editing is impossible). Without an id, a second bubble
/// is accepted because the draft holds only a clean prefix and a missing final
/// reply is worse. Only when no draft exists is the reply a first message.
pub(super) async fn finalize_channel_reply(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
    final_text: &str,
    use_global_edit_latch: bool,
) {
    // Reply first, clean up second: the chat is never momentarily empty.
    if let Some(message_id) = state.message_id.clone() {
        if use_global_edit_latch && channel_edits_unsupported(channel) {
            tracing::info!(
                "[channel-inbound] final edit skipped channel='{}' msg_id={} — no edit route on this backend, replacing the draft with a fresh atomic reply",
                channel,
                message_id,
            );
            replace_draft(sender, channel, &message_id, final_text).await;
        } else {
            let edit = match state.message_receipt.as_ref() {
                Some(receipt) => sender.edit_with_receipt(channel, receipt, final_text).await,
                None => sender.edit(channel, &message_id, final_text).await,
            };
            match edit {
                Ok(()) => {
                    tracing::info!(
                        "[channel-inbound] final edit ok channel='{}' msg_id={} chars={}",
                        channel,
                        message_id,
                        final_text.len(),
                    );
                }
                Err(ProgressiveSendError::Unavailable) => {
                    tracing::warn!(
                        "[channel-inbound] cannot finalize channel='{}' msg_id={} — backend client unavailable, draft left in place",
                        channel,
                        message_id,
                    );
                }
                Err(ProgressiveSendError::MessageGone) => {
                    tracing::info!(
                        "[channel-inbound] final edit channel='{}' msg_id={} — draft already gone provider-side (404), sending fresh atomic reply",
                        channel,
                        message_id,
                    );
                    send_channel_reply(sender, channel, final_text).await;
                }
                Err(ProgressiveSendError::EditUnsupported) => {
                    tracing::warn!(
                        "[channel-inbound] final edit channel='{}' msg_id={} — backend has no edit route, deleting the draft and sending a fresh atomic reply",
                        channel,
                        message_id,
                    );
                    if use_global_edit_latch {
                        mark_channel_edits_unsupported(channel);
                    }
                    replace_draft(sender, channel, &message_id, final_text).await;
                }
                Err(ProgressiveSendError::Other(_)) => {
                    tracing::warn!(
                        "[channel-inbound] final edit failed channel='{}' msg_id={} — deleting orphan draft and sending fresh atomic reply so user still sees the canonical response",
                        channel,
                        message_id,
                    );
                    replace_draft(sender, channel, &message_id, final_text).await;
                }
                Err(ProgressiveSendError::PartialOutcome(_)) => {
                    tracing::error!(
                        "[channel-inbound] final edit returned a partial or uncertain receipt; skipping replacement to avoid duplicate delivery channel='{}' msg_id={}",
                        channel,
                        message_id
                    );
                }
                Err(ProgressiveSendError::UnsupportedContent) => {
                    tracing::error!(
                        "[channel-inbound] final edit unsupported by transport; retaining draft channel='{}' msg_id={}",
                        channel,
                        message_id
                    );
                }
            }
        }
    } else {
        if state.draft_sent {
            tracing::warn!(
                "[channel-inbound] sending fresh reply on channel='{}' — id-less draft exists but user needs the final response",
                channel,
            );
        }
        send_channel_reply(sender, channel, final_text).await;
    }

    cleanup_ephemeral_bubbles(sender, channel, state).await;
}

/// Remove transient filler and thinking messages after another path has
/// handled the canonical final delivery. A delivered final also removes the
/// now-stale draft (every part, using its retained receipt authority); a failed
/// delivery leaves the draft visible.
pub(super) async fn cleanup_after_external_reply(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
    final_delivered: bool,
) {
    if final_delivered {
        delete_draft_parts(sender, channel, state).await;
        state.draft_sent = false;
        state.dirty = false;
    }
    cleanup_ephemeral_bubbles(sender, channel, state).await;
}

async fn cleanup_ephemeral_bubbles(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
) {
    // Fillers first (oldest first), then the thinking bubble.
    delete_ephemeral_messages(
        sender,
        channel,
        std::mem::take(&mut state.filler_message_ids),
        std::mem::take(&mut state.filler_message_receipts),
    )
    .await;
    let mut thinking_ids = std::mem::take(&mut state.thinking_message_ids);
    if let Some(id) = state.thinking_message_id.take()
        && !thinking_ids.contains(&id)
    {
        thinking_ids.push(id);
    }
    let thinking_receipts = std::mem::take(&mut state.thinking_message_receipt)
        .into_iter()
        .collect();
    delete_ephemeral_messages(sender, channel, thinking_ids, thinking_receipts).await;
    state.thinking_dirty = false;
}

/// Delete every known draft part, using its retained receipt authority.
async fn delete_draft_parts(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
) {
    if state.message_ids.is_empty() {
        if let Some(message_id) = state.message_id.take() {
            delete_channel_message(sender, channel, &message_id).await;
        }
    } else {
        let message_ids = std::mem::take(&mut state.message_ids);
        let receipt = state.message_receipt.take();
        for message_id in message_ids {
            let part_receipt = receipt
                .as_ref()
                .map(|receipt| receipt_for_part(receipt, &message_id));
            delete_channel_message_with_receipt(
                sender,
                channel,
                &message_id,
                part_receipt.as_ref(),
            )
            .await;
        }
        state.message_id = None;
    }
}

/// Remove only drafts and ephemeral status messages after an ambiguous or
/// failed delivery. This path never sends a replacement reply, because the
/// host may have observed a partial or unknown result already.
pub(super) async fn abort_channel_reply(
    sender: &dyn ProgressiveSender,
    channel: &str,
    state: &mut StreamingState,
) {
    delete_draft_parts(sender, channel, state).await;
    cleanup_ephemeral_bubbles(sender, channel, state).await;
}

pub(super) fn receipt_for_part(
    receipt: &tinychannels_bus::channel::MessageReceipt,
    message_id: &str,
) -> tinychannels_bus::channel::MessageReceipt {
    let mut part_receipt = receipt.clone();
    part_receipt.primary_platform_message_id = Some(message_id.to_owned());
    part_receipt.platform_message_ids = vec![message_id.to_owned()];
    part_receipt
        .parts
        .retain(|part| part.platform_message_id == message_id);
    part_receipt
}
