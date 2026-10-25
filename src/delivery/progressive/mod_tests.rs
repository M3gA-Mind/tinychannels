//! Progressive delivery against a scripted fake sender.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};

use super::bubbles::{next_filler_text, thinking_bubble_text};
use super::*;

#[derive(Debug, Clone, PartialEq)]
enum Call {
    Send(String),
    Edit(String, String),
    Delete(String),
    Typing,
}

/// Scripted outcome for the next call of a kind. Unscripted calls succeed;
/// sends answer `{"id": "m<n>"}`.
enum Outcome {
    Ok(Value),
    Err(fn() -> ProgressiveSendError),
}

#[derive(Default)]
struct FakeSender {
    calls: Mutex<Vec<Call>>,
    sends: Mutex<VecDeque<Outcome>>,
    edits: Mutex<VecDeque<Outcome>>,
    deletes: Mutex<VecDeque<Outcome>>,
    typings: Mutex<VecDeque<Outcome>>,
}

impl FakeSender {
    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
    fn script(queue: &Mutex<VecDeque<Outcome>>, outcome: Outcome) {
        queue.lock().unwrap().push_back(outcome);
    }
    fn next(queue: &Mutex<VecDeque<Outcome>>) -> Option<Outcome> {
        queue.lock().unwrap().pop_front()
    }
    fn unit(outcome: Option<Outcome>) -> Result<(), ProgressiveSendError> {
        match outcome {
            Some(Outcome::Err(make)) => Err(make()),
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl ProgressiveSender for FakeSender {
    async fn send(&self, _channel: &str, text: &str) -> Result<Value, ProgressiveSendError> {
        let n = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(Call::Send(text.into()));
            calls.iter().filter(|c| matches!(c, Call::Send(_))).count()
        };
        match Self::next(&self.sends) {
            Some(Outcome::Ok(v)) => Ok(v),
            Some(Outcome::Err(make)) => Err(make()),
            None => Ok(json!({ "id": format!("m{n}") })),
        }
    }
    async fn edit(&self, _c: &str, id: &str, text: &str) -> Result<(), ProgressiveSendError> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::Edit(id.into(), text.into()));
        Self::unit(Self::next(&self.edits))
    }
    async fn delete(&self, _c: &str, id: &str) -> Result<(), ProgressiveSendError> {
        self.calls.lock().unwrap().push(Call::Delete(id.into()));
        Self::unit(Self::next(&self.deletes))
    }
    async fn typing(&self, _c: &str) -> Result<(), ProgressiveSendError> {
        self.calls.lock().unwrap().push(Call::Typing);
        Self::unit(Self::next(&self.typings))
    }
}

fn other() -> ProgressiveSendError {
    ProgressiveSendError::Other(anyhow::anyhow!("502 Bad Gateway"))
}
fn unsupported() -> ProgressiveSendError {
    ProgressiveSendError::EditUnsupported
}
fn gone() -> ProgressiveSendError {
    ProgressiveSendError::MessageGone
}
fn unavailable() -> ProgressiveSendError {
    ProgressiveSendError::Unavailable
}

fn turn(channel: &str) -> (Arc<FakeSender>, ProgressiveReply) {
    let sender = Arc::new(FakeSender::default());
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: channel.starts_with("telegram:")
            || channel.starts_with("tg:")
            || channel == "telegram"
            || channel == "tg",
        ..Default::default()
    };
    let reply = ProgressiveReply::new(sender.clone(), channel, capabilities);
    (sender, reply)
}

// ── capability gating ──────────────────────────────────────────────────────

#[test]
fn progressive_ui_is_an_allowlist_failing_safe_for_unknown_channels() {
    let supported = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let unsupported = tinychannels_bus::ChannelCapabilities::unknown();
    assert!(channel_supports_progressive_ui(&supported));
    assert!(!channel_supports_progressive_ui(&unsupported));
}

#[tokio::test]
async fn channels_without_progressive_ui_get_no_bubbles_but_still_a_reply() {
    let (sender, mut reply) = turn("discord:guild-1");
    reply.on_text_delta("partial");
    reply.on_thinking_delta("reasoning");
    reply.edit_tick().await;
    reply.filler_tick().await;
    reply.finalize("final").await;
    assert_eq!(sender.calls(), vec![Call::Send("final".into())]);
}

// ── state ──────────────────────────────────────────────────────────────────

#[test]
fn compose_draft_uses_placeholder_until_text_arrives() {
    let mut state = StreamingState::default();
    assert_eq!(state.compose_draft(), "_working…_");
    state.last_tool = Some("🔧 search…".into());
    assert_eq!(state.compose_draft(), "_working…_");
    state.content = "hello  \n".into();
    assert_eq!(state.compose_draft(), "hello");
}

#[test]
fn route_absence_keeps_ids_and_message_absence_forgets_them() {
    let mut state = StreamingState {
        message_id: Some("1103".into()),
        thinking_message_id: Some("2201".into()),
        ..Default::default()
    };
    state.latch_draft_edits_unsupported();
    state.latch_thinking_edits_unsupported();
    assert!(state.edit_disabled && state.thinking_edit_disabled);
    assert_eq!(state.message_id.as_deref(), Some("1103"));
    assert_eq!(state.thinking_message_id.as_deref(), Some("2201"));

    state.forget_draft();
    state.forget_thinking();
    assert_eq!(state.message_id, None);
    assert_eq!(state.thinking_message_id, None);
}

#[test]
fn extract_message_id_handles_every_backend_shape() {
    assert_eq!(
        extract_message_id(&json!({"id": "a"})).as_deref(),
        Some("a")
    );
    assert_eq!(
        extract_message_id(&json!({"messageId": 1456, "success": true})).as_deref(),
        Some("1456")
    );
    assert_eq!(
        extract_message_id(&json!({"data": {"id": "b"}})).as_deref(),
        Some("b")
    );
    assert_eq!(
        extract_message_id(&json!({"data": {"messageId": 7}})).as_deref(),
        Some("7")
    );
    assert_eq!(
        extract_message_id(&json!({"id": u64::MAX})).as_deref(),
        Some(u64::MAX.to_string().as_str())
    );
    assert_eq!(extract_message_id(&json!({"id": true})), None);
    assert_eq!(extract_message_id(&json!({})), None);
    assert_eq!(
        extract_message_id(&json!({"platformMessageIds": ["receipt-1", "receipt-2"]})).as_deref(),
        Some("receipt-1")
    );
    assert_eq!(
        extract_message_id(&json!({"parts": [{"platformMessageId": "part-1"}]})).as_deref(),
        Some("part-1")
    );
}

#[test]
fn thinking_snippet_trims_partial_leading_word() {
    let state = |acc: &str| StreamingState {
        thinking_accumulator: acc.into(),
        ..Default::default()
    };
    assert_eq!(latest_thinking_snippet(&state("   ")), None);
    assert_eq!(
        latest_thinking_snippet(&state("short thought")).as_deref(),
        Some("short thought")
    );
    let long = format!("{} tail words here", "x".repeat(MAX_FILLER_CHARS));
    let snippet = latest_thinking_snippet(&state(&long)).unwrap();
    assert_eq!(snippet, "tail words here");
    assert_eq!(latest_thinking_snippet(&state(&"y".repeat(500))), None);
}

#[test]
fn thinking_bubble_truncates_on_a_char_boundary() {
    assert_eq!(thinking_bubble_text("  "), None);
    assert_eq!(
        thinking_bubble_text("plan").as_deref(),
        Some("💭 Thinking:\n_plan_")
    );
    // Multi-byte text straddling the cap must not panic.
    let text = thinking_bubble_text(&"é".repeat(MAX_THINKING_DISPLAY_CHARS)).unwrap();
    assert!(text.ends_with("…_"));
}

#[test]
fn filler_prefers_fresh_thinking_then_rotates_static_pool() {
    let mut state = StreamingState {
        thinking_accumulator: "checking the calendar".into(),
        ..Default::default()
    };
    assert_eq!(next_filler_text(&mut state), "💭 _checking the calendar…_");
    // Same snippet again → fall through to the static pool.
    assert_eq!(next_filler_text(&mut state), STATIC_FILLERS[0]);
    assert_eq!(next_filler_text(&mut state), STATIC_FILLERS[1]);
    assert_eq!(next_filler_text(&mut state), STATIC_FILLERS[2]);
    assert_eq!(next_filler_text(&mut state), STATIC_FILLERS[0]);
}

// ── latch ──────────────────────────────────────────────────────────────────

#[test]
fn edit_capability_latches_per_provider_not_per_chat() {
    assert!(!channel_edits_unsupported("noedit-a:chat-1"));
    mark_channel_edits_unsupported("noedit-a:chat-1");
    mark_channel_edits_unsupported("noedit-a:chat-1");
    assert!(channel_edits_unsupported("noedit-a:chat-2"));
    assert!(channel_edits_unsupported("noedit-a"));
    assert!(!channel_edits_unsupported("noedit-b:chat-1"));
    assert_eq!(edit_capability_key("discord:guild:chan"), "discord");
    assert_eq!(edit_capability_key(""), "");
}

// ── streaming turn ─────────────────────────────────────────────────────────

#[tokio::test]
async fn draft_is_posted_once_then_edited_then_finalized_in_place() {
    let (sender, mut reply) = turn("telegram:1");
    reply.on_tool_call("search");
    reply.on_text_delta("Hel");
    reply.edit_tick().await;
    reply.on_text_delta("lo");
    reply.on_tool_result("search", true);
    reply.edit_tick().await;
    // Nothing dirty → no call.
    reply.edit_tick().await;
    reply.finalize("Hello!").await;
    assert_eq!(
        sender.calls(),
        vec![
            Call::Send("Hel".into()),
            Call::Edit("m1".into(), "Hello".into()),
            Call::Edit("m1".into(), "Hello!".into()),
        ]
    );
    assert_eq!(reply.state().last_tool.as_deref(), Some("🔧 search ✓"));
}

#[tokio::test]
async fn transient_edit_failures_disable_edits_after_budget() {
    let (sender, mut reply) = turn("telegram:2");
    reply.on_text_delta("a");
    reply.edit_tick().await;
    FakeSender::script(&sender.edits, Outcome::Err(other));
    FakeSender::script(&sender.edits, Outcome::Err(other));
    reply.on_text_delta("b");
    reply.edit_tick().await;
    assert!(!reply.state().edit_disabled);
    reply.on_text_delta("c");
    reply.edit_tick().await;
    assert!(reply.state().edit_disabled);
    assert_eq!(reply.state().message_id.as_deref(), Some("m1"));
}

#[tokio::test]
async fn message_gone_on_edit_forgets_the_draft_and_finalize_sends_fresh() {
    let (sender, mut reply) = turn("telegram:3");
    reply.on_text_delta("a");
    reply.edit_tick().await;
    FakeSender::script(&sender.edits, Outcome::Err(gone));
    reply.on_text_delta("b");
    reply.edit_tick().await;
    assert_eq!(reply.state().message_id, None);
    reply.finalize("done").await;
    // id-less but already-sent draft → fresh reply is still delivered.
    assert_eq!(sender.calls().last(), Some(&Call::Send("done".into())));
}

#[tokio::test]
async fn edit_route_absence_latches_provider_and_finalize_replaces_the_draft() {
    let (sender, mut reply) = turn("tg:latch-route");
    reply.on_text_delta("a");
    reply.edit_tick().await;
    FakeSender::script(&sender.edits, Outcome::Err(unsupported));
    reply.on_text_delta("b");
    reply.edit_tick().await;
    assert!(reply.state().edit_disabled);
    assert_eq!(reply.state().message_id.as_deref(), Some("m1"));
    reply.finalize("final").await;
    let calls = sender.calls();
    assert_eq!(
        &calls[calls.len() - 2..],
        &[Call::Delete("m1".into()), Call::Send("final".into())]
    );
    // The latch is per provider ("tg"), so the next turn skips edits entirely.
    assert!(channel_edits_unsupported("tg:other-chat"));
}

#[tokio::test]
async fn finalize_edit_failures_each_recover_with_a_visible_reply() {
    for (err, expect_delete) in [
        (other as fn() -> ProgressiveSendError, true),
        (gone as fn() -> ProgressiveSendError, false),
    ] {
        let (sender, mut reply) = turn("telegram:4");
        reply.on_text_delta("a");
        reply.edit_tick().await;
        FakeSender::script(&sender.edits, Outcome::Err(err));
        reply.finalize("final").await;
        let calls = sender.calls();
        assert_eq!(calls.last(), Some(&Call::Send("final".into())));
        assert_eq!(calls.contains(&Call::Delete("m1".into())), expect_delete);
    }
}

#[tokio::test]
async fn finalize_edit_route_absence_marks_the_latch() {
    let (sender, mut reply) = turn("latch-final:1");
    // Force progressive state as if the draft had been posted.
    reply.state.message_id = Some("d1".into());
    FakeSender::script(&sender.edits, Outcome::Err(unsupported));
    reply.finalize("final").await;
    assert!(channel_edits_unsupported("latch-final:2"));
    assert_eq!(
        sender.calls()[1..],
        [Call::Delete("d1".into()), Call::Send("final".into())]
    );
}

#[tokio::test]
async fn unavailable_sender_leaves_the_draft_in_place() {
    let (sender, mut reply) = turn("telegram:5");
    reply.on_text_delta("a");
    reply.edit_tick().await;
    FakeSender::script(&sender.edits, Outcome::Err(unavailable));
    reply.finalize("final").await;
    assert_eq!(sender.calls().len(), 2, "no delete/resend when unavailable");
}

#[tokio::test]
async fn id_less_initial_draft_disables_edits() {
    let (sender, mut reply) = turn("telegram:6");
    FakeSender::script(&sender.sends, Outcome::Ok(json!({"success": true})));
    reply.on_text_delta("a");
    reply.edit_tick().await;
    assert!(reply.state().draft_sent);
    assert!(reply.state().edit_disabled);
}

#[tokio::test]
async fn uncertain_initial_draft_stops_retries_and_preserves_possible_delivery() {
    let (sender, mut reply) = turn("telegram:7");
    FakeSender::script(&sender.sends, Outcome::Err(other));
    reply.on_text_delta("a");
    reply.edit_tick().await;
    reply.on_text_delta("b");
    reply.edit_tick().await;
    assert_eq!(
        sender.calls().len(),
        1,
        "uncertain initial send is not retried"
    );
    assert!(reply.state().edit_disabled);
    assert!(reply.state().draft_sent);
    assert!(reply.state().delivery_uncertain);
}

#[tokio::test]
async fn thinking_bubble_is_posted_edited_and_deleted_with_fillers() {
    let (sender, mut reply) = turn("telegram:8");
    reply.on_thinking_delta("step one");
    reply.edit_tick().await;
    reply.on_thinking_delta(" and two");
    reply.edit_tick().await;
    reply.filler_tick().await;
    reply.finalize("answer").await;
    assert_eq!(
        sender.calls(),
        vec![
            Call::Send("💭 Thinking:\n_step one_".into()),
            Call::Edit("m1".into(), "💭 Thinking:\n_step one and two_".into()),
            Call::Send("💭 _step one and two…_".into()),
            Call::Send("answer".into()),
            Call::Delete("m2".into()),
            Call::Delete("m1".into()),
        ]
    );
}

#[tokio::test]
async fn thinking_edit_failures_follow_the_same_recoveries() {
    let (sender, mut reply) = turn("telegram:9");
    reply.on_thinking_delta("x");
    reply.edit_tick().await;
    FakeSender::script(&sender.edits, Outcome::Err(gone));
    reply.on_thinking_delta("y");
    reply.edit_tick().await;
    assert_eq!(reply.state().thinking_message_id, None);
    assert!(reply.state().thinking_edit_disabled);

    let (sender, mut reply) = turn("tg-think:1");
    reply.state.thinking_message_id = Some("t1".into());
    reply.state.thinking_accumulator = "x".into();
    reply.state.thinking_dirty = true;
    FakeSender::script(&sender.edits, Outcome::Err(other));
    reply.edit_tick().await;
    assert!(
        !reply.state().thinking_edit_disabled,
        "transient keeps trying"
    );
}

#[tokio::test]
async fn thinking_send_failure_or_missing_id_stops_thinking_flushes() {
    let (sender, mut reply) = turn("telegram:10");
    FakeSender::script(&sender.sends, Outcome::Err(other));
    reply.on_thinking_delta("x");
    reply.edit_tick().await;
    assert!(reply.state().thinking_edit_disabled);

    let (sender, mut reply) = turn("telegram:11");
    FakeSender::script(&sender.sends, Outcome::Ok(json!({})));
    reply.on_thinking_delta("x");
    reply.edit_tick().await;
    assert!(reply.state().thinking_sent && reply.state().thinking_edit_disabled);
}

#[tokio::test]
async fn uncertain_filler_send_disables_retries_immediately() {
    let (sender, mut reply) = turn("telegram:12");
    FakeSender::script(&sender.sends, Outcome::Err(other));
    FakeSender::script(&sender.sends, Outcome::Err(other));
    reply.filler_tick().await;
    reply.filler_tick().await;
    reply.filler_tick().await;
    assert!(reply.state().filler_disabled);
    assert!(reply.state().filler_delivery_uncertain);
    assert_eq!(sender.calls().len(), 1);
}

#[tokio::test]
async fn filler_sends_are_bounded_independently_of_the_turn_lease() {
    use super::bubbles::send_filler_message;

    let sender = FakeSender::default();
    let mut state = StreamingState::default();
    for _ in 0..12 {
        send_filler_message(&sender, "telegram:bounded-fillers", &mut state).await;
    }
    assert_eq!(sender.calls().len(), 8);
    assert!(state.filler_disabled);
    assert_eq!(state.filler_send_count, 8);
}

#[tokio::test]
async fn typing_indicator_latches_after_repeated_failures_and_recovers_before() {
    let (sender, mut reply) = turn("discord:1");
    FakeSender::script(&sender.typings, Outcome::Err(other));
    reply.typing_tick().await;
    reply.typing_tick().await; // recovers
    FakeSender::script(&sender.typings, Outcome::Err(other));
    FakeSender::script(&sender.typings, Outcome::Err(other));
    reply.typing_tick().await;
    reply.typing_tick().await;
    reply.typing_tick().await; // disabled: not sent
    assert_eq!(sender.calls().len(), 4);
}

#[tokio::test]
async fn delete_failures_are_tolerated_during_cleanup() {
    let (sender, mut reply) = turn("telegram:13");
    reply.state.filler_message_ids = vec!["f1".into(), "f2".into()];
    FakeSender::script(&sender.deletes, Outcome::Err(gone));
    FakeSender::script(&sender.deletes, Outcome::Err(other));
    reply.finalize("ok").await;
    assert_eq!(
        sender.calls(),
        vec![
            Call::Send("ok".into()),
            Call::Delete("f1".into()),
            Call::Delete("f2".into()),
        ]
    );
    assert!(reply.state().filler_message_ids.is_empty());
}

#[tokio::test]
async fn send_reply_is_standalone() {
    let (sender, reply) = turn("slack:1");
    FakeSender::script(&sender.sends, Outcome::Err(unavailable));
    reply.send_reply("sorry").await;
    reply.send_reply("sorry again").await;
    assert_eq!(reply.channel(), "slack:1");
    assert_eq!(sender.calls().len(), 2);
}

#[tokio::test]
async fn missing_edit_route_preserves_owned_bubbles_and_skips_later_requests() {
    use super::bubbles::flush_thinking_message;
    use super::draft::flush_streaming_edit;

    let sender = FakeSender::default();
    let channel = "coverage-no-edit-route:room";
    let mut state = StreamingState {
        content: "draft".into(),
        message_id: Some("draft-id".into()),
        thinking_accumulator: "reasoning".into(),
        thinking_message_id: Some("thinking-id".into()),
        ..Default::default()
    };
    FakeSender::script(&sender.edits, Outcome::Err(unsupported));
    flush_thinking_message(&sender, channel, &mut state, true).await;
    assert!(state.thinking_edit_disabled);
    assert_eq!(state.thinking_message_id.as_deref(), Some("thinking-id"));
    flush_streaming_edit(&sender, channel, &mut state, true).await;
    assert!(state.edit_disabled);
    assert_eq!(state.message_id.as_deref(), Some("draft-id"));
    flush_thinking_message(&sender, channel, &mut state, true).await;
    assert_eq!(
        sender.calls().len(),
        1,
        "known absent routes must not be retried"
    );
}

#[tokio::test]
async fn unavailable_bubble_transport_does_not_spend_failure_budgets_or_claim_delivery() {
    use super::bubbles::{flush_thinking_message, send_filler_message};
    use super::draft::flush_streaming_edit;

    let sender = FakeSender::default();
    let mut state = StreamingState {
        content: "draft".into(),
        thinking_accumulator: "reasoning".into(),
        ..Default::default()
    };
    for _ in 0..3 {
        FakeSender::script(&sender.sends, Outcome::Err(unavailable));
    }
    flush_streaming_edit(&sender, "coverage-unavailable", &mut state, true).await;
    flush_thinking_message(&sender, "coverage-unavailable", &mut state, true).await;
    send_filler_message(&sender, "coverage-unavailable", &mut state).await;
    assert!(!state.draft_sent && !state.thinking_sent);
    assert!(!state.edit_disabled && !state.thinking_edit_disabled && !state.filler_disabled);
    assert_eq!(state.edit_failures, 0);
    assert_eq!(state.filler_failures, 0);
    assert!(state.filler_message_ids.is_empty());
    state.message_id = Some("existing".into());
    FakeSender::script(&sender.edits, Outcome::Err(unavailable));
    flush_streaming_edit(&sender, "coverage-unavailable", &mut state, true).await;
    assert_eq!(state.message_id.as_deref(), Some("existing"));
    assert_eq!(state.edit_failures, 0);
}

#[tokio::test]
async fn transient_thinking_edit_failure_retains_cleanup_id_and_allows_retry() {
    use super::bubbles::flush_thinking_message;

    let sender = FakeSender::default();
    let mut state = StreamingState {
        thinking_accumulator: "reasoning".into(),
        thinking_message_id: Some("thinking-id".into()),
        ..Default::default()
    };
    FakeSender::script(&sender.edits, Outcome::Err(other));
    flush_thinking_message(&sender, "coverage-thinking-retry", &mut state, true).await;
    assert!(!state.thinking_edit_disabled);
    assert_eq!(state.thinking_message_id.as_deref(), Some("thinking-id"));
    flush_thinking_message(&sender, "coverage-thinking-retry", &mut state, true).await;
    assert_eq!(sender.calls().len(), 2);
}

#[tokio::test]
async fn empty_thinking_and_filler_without_an_id_do_not_claim_cleanup_ownership() {
    use super::bubbles::{flush_thinking_message, send_filler_message};

    let sender = FakeSender::default();
    let mut state = StreamingState {
        thinking_accumulator: " \n ".into(),
        thinking_dirty: true,
        ..Default::default()
    };
    flush_thinking_message(&sender, "coverage-empty-thinking", &mut state, true).await;
    assert!(sender.calls().is_empty());
    assert!(!state.thinking_dirty);
    FakeSender::script(&sender.sends, Outcome::Ok(json!({"ok": true})));
    send_filler_message(&sender, "coverage-empty-thinking", &mut state).await;
    assert!(state.filler_message_ids.is_empty());
    assert_eq!(state.filler_failures, 0);
}

#[tokio::test]
async fn draft_uncertain_edit_retains_authority_and_never_posts_a_replacement() {
    use super::draft::flush_streaming_edit;

    let sender = FakeSender::default();
    let mut state = StreamingState {
        content: "updated draft".into(),
        message_id: Some("draft-1".into()),
        message_ids: vec!["draft-1".into(), "draft-2".into()],
        ..Default::default()
    };
    FakeSender::script(
        &sender.edits,
        Outcome::Err(|| {
            ProgressiveSendError::PartialOutcome(Box::new(
                tinychannels_bus::channel::ChannelSendOutcome {
                    receipt: tinychannels_bus::channel::MessageReceipt {
                        platform_message_ids: vec!["draft-1".into(), "draft-2".into()],
                        ..Default::default()
                    },
                    error: Some(tinychannels_bus::channel::ChannelSendError {
                        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                        sent_before_error: true,
                        ..Default::default()
                    }),
                },
            ))
        }),
    );

    flush_streaming_edit(&sender, "telegram:receipt", &mut state, false).await;
    flush_streaming_edit(&sender, "telegram:receipt", &mut state, false).await;

    assert!(state.edit_disabled);
    assert_eq!(state.message_id.as_deref(), Some("draft-1"));
    assert_eq!(state.message_ids, ["draft-1", "draft-2"]);
    assert_eq!(
        sender.calls(),
        [Call::Edit("draft-1".into(), "updated draft".into())]
    );
}

#[tokio::test]
async fn thinking_partial_edit_keeps_bubble_for_cleanup_and_stops_editing() {
    let (sender, mut reply) = turn("telegram:thinking-partial");
    reply.state = StreamingState {
        thinking_accumulator: "current thought".into(),
        thinking_message_id: Some("thinking-1".into()),
        thinking_message_ids: vec!["thinking-1".into(), "thinking-2".into()],
        thinking_dirty: true,
        ..Default::default()
    };
    FakeSender::script(
        &sender.edits,
        Outcome::Err(|| ProgressiveSendError::PartialOutcome(Box::default())),
    );

    reply.edit_tick().await;
    reply.state.thinking_dirty = true;
    reply.edit_tick().await;

    assert!(reply.state.thinking_edit_disabled);
    assert_eq!(
        reply.state.thinking_message_id.as_deref(),
        Some("thinking-1")
    );
    assert_eq!(
        reply.state.thinking_message_ids,
        ["thinking-1", "thinking-2"]
    );
    assert_eq!(sender.calls().len(), 1);
}

#[tokio::test]
async fn typing_failures_disable_only_after_the_configured_failure_budget() {
    use super::bubbles::send_typing_indicator;

    let sender = FakeSender::default();
    let mut state = TypingState::default();
    FakeSender::script(&sender.typings, Outcome::Err(other));
    send_typing_indicator(&sender, "telegram:typing", &mut state).await;
    assert_eq!(state.failures, 1);
    assert!(!state.disabled);
    FakeSender::script(&sender.typings, Outcome::Err(other));
    send_typing_indicator(&sender, "telegram:typing", &mut state).await;
    assert!(state.disabled);
    send_typing_indicator(&sender, "telegram:typing", &mut state).await;
    assert_eq!(sender.calls(), [Call::Typing, Call::Typing]);
}

#[test]
fn unicode_segment_boundaries_match_configured_units() {
    let source = "a😀éz";
    let utf8 =
        super::reply::split_text_by_limit(source, 4, tinychannels_bus::LengthUnit::Utf8Bytes)
            .unwrap();
    assert_eq!(utf8.concat(), source);
    assert!(utf8.iter().all(|segment| segment.len() <= 4));
    let utf16 =
        super::reply::split_text_by_limit(source, 2, tinychannels_bus::LengthUnit::Utf16Units)
            .unwrap();
    assert_eq!(utf16.concat(), source);
    assert!(
        utf16
            .iter()
            .all(|segment| segment.encode_utf16().count() <= 2)
    );
    assert!(
        super::reply::split_text_by_limit("😀", 1, tinychannels_bus::LengthUnit::Utf8Bytes,)
            .is_err()
    );
}

#[test]
fn ordered_text_parts_keep_dialects_and_do_not_duplicate_projection() {
    let first = tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Text {
            text: "abcd".into(),
            markdown_dialect: Some("markdown".into()),
        },
    };
    let second = tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Text {
            text: "ef".into(),
            markdown_dialect: Some("plain".into()),
        },
    };
    let segments = super::reply::segment_text_parts(
        &[first.clone(), second.clone()],
        "abcdef",
        4,
        tinychannels_bus::LengthUnit::Characters,
    )
    .unwrap();
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0], (vec![first], "abcd".into()));
    assert_eq!(segments[1], (vec![second], "ef".into()));

    let projected_parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "a".into(),
                markdown_dialect: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "b".into(),
                markdown_dialect: None,
            },
        },
    ];
    let projected = super::reply::segment_text_parts(
        &projected_parts,
        "ab",
        4,
        tinychannels_bus::LengthUnit::Characters,
    )
    .unwrap();
    assert_eq!(projected, vec![(projected_parts, "ab".into())]);
}

use tinychannels_bus::channel::{ChannelSendOutcome, MessageReceipt, MessageReceiptPart};
use tinychannels_bus::module_progressive::{ChannelOutputEvent as Event, ProgressiveResult};
use tinychannels_bus::{ContentPart, OutboundContentPart};

fn text_part(text: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: text.into(),
            markdown_dialect: Some("commonmark".into()),
        },
    }
}

fn delivered(id: &str) -> ChannelSendOutcome {
    ChannelSendOutcome {
        receipt: MessageReceipt {
            primary_platform_message_id: Some(id.into()),
            platform_message_ids: vec![id.into()],
            edit_token: Some(format!("edit-{id}")),
            delete_token: Some(format!("delete-{id}")),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[derive(Clone, Debug)]
struct TypedCall {
    parts: Vec<OutboundContentPart>,
    text: String,
    turn_id: Option<String>,
    sequence: u64,
    segment: u32,
}

#[derive(Default)]
struct TypedSender {
    legacy: FakeSender,
    sent: Mutex<Vec<TypedCall>>,
    outcomes: Mutex<VecDeque<Result<ChannelSendOutcome, ProgressiveSendError>>>,
}

#[async_trait]
impl ProgressiveSender for TypedSender {
    async fn send(&self, channel: &str, text: &str) -> Result<Value, ProgressiveSendError> {
        self.legacy.send(channel, text).await
    }
    async fn send_parts_with_segment(
        &self,
        _channel: &str,
        parts: &[OutboundContentPart],
        text: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        segment: u32,
    ) -> Result<ChannelSendOutcome, ProgressiveSendError> {
        let mut sent = self.sent.lock().unwrap();
        sent.push(TypedCall {
            parts: parts.to_vec(),
            text: text.into(),
            turn_id: context.map(|context| context.turn_id.clone()),
            sequence,
            segment,
        });
        self.outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(delivered(&format!("typed-{}", sent.len()))))
    }
    async fn edit(&self, channel: &str, id: &str, text: &str) -> Result<(), ProgressiveSendError> {
        self.legacy.edit(channel, id, text).await
    }
    async fn delete(&self, channel: &str, id: &str) -> Result<(), ProgressiveSendError> {
        self.legacy.delete(channel, id).await
    }
    async fn typing(&self, channel: &str) -> Result<(), ProgressiveSendError> {
        self.legacy.typing(channel).await
    }
}

fn typed_turn(
    limit: Option<(u64, tinychannels_bus::LengthUnit)>,
) -> (Arc<TypedSender>, ProgressiveReply) {
    let sender = Arc::new(TypedSender::default());
    let mut capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    if let Some((limit, unit)) = limit {
        capabilities.text.text_limit = Some(limit);
        capabilities.text.length_unit = unit;
    }
    let reply = ProgressiveReply::new_scoped(
        sender.clone(),
        "telegram:typed",
        capabilities,
        ProgressiveOutputContext {
            turn_id: "turn-typed".into(),
            channel: tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: Some("bot".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                id: "chat".into(),
                ..Default::default()
            },
        },
    );
    (sender, reply)
}

#[tokio::test]
async fn typed_final_segments_unicode_with_scoped_sequence_and_retains_every_receipt() {
    for (unit, limit, expected) in [
        (
            tinychannels_bus::LengthUnit::Characters,
            2,
            vec!["a😀", "éz"],
        ),
        (
            tinychannels_bus::LengthUnit::Utf8Bytes,
            4,
            vec!["a", "😀", "éz"],
        ),
        (
            tinychannels_bus::LengthUnit::Utf16Units,
            2,
            vec!["a", "😀", "éz"],
        ),
    ] {
        let (sender, mut reply) = typed_turn(Some((limit, unit)));
        let event = Event::FinalMessage {
            text: "projection must not be sent twice".into(),
            parts: vec![text_part("a😀éz")],
            receipt: None,
        };
        reply.on_output_event(&event).await.unwrap();
        let dispatch = reply.finalize_output_event(&event).await.unwrap();
        assert_eq!(dispatch.outcomes.len(), 1);
        let sent = sender.sent.lock().unwrap();
        assert_eq!(
            sent.iter()
                .map(|call| call.text.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        for (index, call) in sent.iter().enumerate() {
            assert_eq!(call.parts, vec![text_part(expected[index])]);
            assert_eq!(call.sequence, 1);
            assert_eq!(call.segment, index as u32);
            assert_eq!(call.turn_id.as_deref(), Some("turn-typed"));
        }
        let receipt = &dispatch.outcomes[0].receipt;
        assert_eq!(receipt.platform_message_ids.len(), expected.len());
        assert_eq!(
            receipt.primary_platform_message_id.as_deref(),
            Some("typed-1")
        );
        for (index, part) in receipt.parts.iter().enumerate() {
            let id = format!("typed-{}", index + 2);
            assert_eq!(part.platform_message_id, id);
            assert_eq!(part.edit_token, Some(format!("edit-{id}")));
            assert_eq!(part.delete_token, Some(format!("delete-{id}")));
        }
    }
}

#[tokio::test]
async fn segmented_send_stops_after_partial_failure_and_retains_delivered_prefix() {
    for failure in [
        Err(other()),
        Err(ProgressiveSendError::PartialOutcome(Box::new(
            ChannelSendOutcome {
                receipt: delivered("partial").receipt,
                error: Some(tinychannels_bus::channel::ChannelSendError {
                    sent_before_error: true,
                    ..Default::default()
                }),
            },
        ))),
        Ok(ChannelSendOutcome::default()),
    ] {
        let (sender, mut reply) = typed_turn(Some((2, tinychannels_bus::LengthUnit::Characters)));
        sender
            .outcomes
            .lock()
            .unwrap()
            .extend([Ok(delivered("first")), failure]);
        let outcome = reply
            .send_parts(&[text_part("abcdef")], "abcdef")
            .await
            .unwrap();
        assert_eq!(
            sender.sent.lock().unwrap().len(),
            2,
            "never attempt the remaining tail"
        );
        assert_eq!(
            outcome.receipt.primary_platform_message_id.as_deref(),
            Some("first")
        );
        assert!(
            outcome
                .receipt
                .platform_message_ids
                .contains(&"first".into())
        );
        assert!(outcome.error.unwrap().sent_before_error);
    }
}

#[tokio::test]
async fn unsupported_notice_and_media_remain_ordered_when_later_final_succeeds() {
    let (sender, mut reply) = typed_turn(None);
    sender.outcomes.lock().unwrap().extend([
        Err(ProgressiveSendError::UnsupportedContent),
        Err(ProgressiveSendError::UnsupportedContent),
        Ok(delivered("final")),
    ]);
    let mut batch = reply
        .apply_output_event(
            &Event::Clarification {
                prompt: "which?".into(),
            },
            true,
        )
        .await
        .unwrap();
    let media = ContentPart::Text {
        text: "caption".into(),
        markdown_dialect: Some("plain".into()),
    };
    batch.absorb(
        reply
            .apply_output_event(
                &Event::Media {
                    text: Some("intro".into()),
                    parts: vec![media.clone()],
                },
                true,
            )
            .await
            .unwrap(),
    );
    batch.absorb(
        reply
            .finalize_output_event(&Event::FinalMessage {
                text: "done".into(),
                parts: vec![],
                receipt: None,
            })
            .await
            .unwrap(),
    );
    let result = batch.into_result();
    let ProgressiveResult::DeliveryBatch {
        outcomes,
        hosted_accepted,
        unsupported,
        failure,
    } = &result
    else {
        panic!("mixed facts need a batch")
    };
    assert_eq!(failure, &None);
    assert!(!hosted_accepted);
    assert_eq!(outcomes, &vec![delivered("final")]);
    assert_eq!(
        unsupported,
        &vec![
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "which?".into(),
                    markdown_dialect: None
                }
            },
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "intro".into(),
                    markdown_dialect: None
                }
            },
            OutboundContentPart::Content { part: media },
        ]
    );
    let restored = ProgressiveOutputDispatch::from_result(&result);
    assert_eq!(
        serde_json::to_value(restored.into_result()).unwrap(),
        serde_json::to_value(result).unwrap()
    );
}

#[test]
fn public_result_roundtrips_preserve_receipts_unsupported_parts_and_transport_failures() {
    let cases = [
        ProgressiveOutputDispatch::default(),
        ProgressiveOutputDispatch {
            outcomes: vec![delivered("a")],
            ..Default::default()
        },
        ProgressiveOutputDispatch {
            outcomes: vec![delivered("a"), delivered("b")],
            ..Default::default()
        },
        ProgressiveOutputDispatch {
            unsupported: vec![text_part("unsupported")],
            ..Default::default()
        },
    ];
    for dispatch in cases {
        let result = dispatch.into_result();
        assert_eq!(
            serde_json::to_value(ProgressiveOutputDispatch::from_result(&result).into_result())
                .unwrap(),
            serde_json::to_value(result).unwrap()
        );
    }
    let mut batch = ProgressiveOutputDispatch {
        unsupported: vec![text_part("keep")],
        ..Default::default()
    };
    let mut failed = delivered("partial");
    failed.error = Some(tinychannels_bus::channel::ChannelSendError {
        sent_before_error: true,
        ..Default::default()
    });
    batch.push_outcome(failed.clone());
    batch.absorb(ProgressiveOutputDispatch {
        outcomes: vec![delivered("later")],
        ..Default::default()
    });
    let ProgressiveResult::DeliveryBatch {
        outcomes,
        hosted_accepted,
        unsupported,
        failure,
    } = batch.into_result()
    else {
        panic!("expected batch")
    };
    assert_eq!(outcomes, vec![failed, delivered("later")]);
    assert!(!hosted_accepted);
    assert_eq!(unsupported, vec![text_part("keep")]);
    assert_eq!(
        failure,
        Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport)
    );
    assert!(
        ProgressiveOutputDispatch::from_result(&ProgressiveResult::Updated)
            .outcomes
            .is_empty()
    );
}

#[tokio::test]
async fn legacy_sender_receipt_operations_require_primary_id_and_delegate_exactly_once() {
    let sender = FakeSender::default();
    let receipt = delivered("provider-id").receipt;
    sender
        .edit_with_receipt("channel", &receipt, "updated")
        .await
        .unwrap();
    sender
        .delete_with_receipt("channel", &receipt)
        .await
        .unwrap();
    assert!(
        sender
            .edit_with_receipt("channel", &MessageReceipt::default(), "bad")
            .await
            .is_err()
    );
    assert!(
        sender
            .delete_with_receipt("channel", &MessageReceipt::default())
            .await
            .is_err()
    );
    assert!(matches!(
        sender
            .send_parts_with_segment("channel", &[text_part("rich")], "", None, 1, 0)
            .await,
        Err(ProgressiveSendError::UnsupportedContent)
    ));
    assert_eq!(
        sender.calls(),
        vec![
            Call::Edit("provider-id".into(), "updated".into()),
            Call::Delete("provider-id".into())
        ]
    );
}

fn partial_bubble() -> ProgressiveSendError {
    ProgressiveSendError::PartialOutcome(Box::new(ChannelSendOutcome {
        receipt: MessageReceipt {
            primary_platform_message_id: Some("part-1".into()),
            platform_message_ids: vec!["part-1".into()],
            parts: vec![MessageReceiptPart {
                platform_message_id: "part-2".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        error: Some(tinychannels_bus::channel::ChannelSendError {
            sent_before_error: true,
            ..Default::default()
        }),
    }))
}

#[tokio::test]
async fn partial_ephemeral_sends_retain_all_cleanup_ids_and_do_not_fence_final_content() {
    let (sender, mut reply) = turn("telegram:partial-bubbles");
    FakeSender::script(&sender.sends, Outcome::Err(partial_bubble));
    reply.on_thinking_delta("reason");
    reply.edit_tick().await;
    FakeSender::script(&sender.sends, Outcome::Err(partial_bubble));
    reply.filler_tick().await;
    reply.on_thinking_delta(" more");
    reply.edit_tick().await;
    reply.filler_tick().await;
    assert_eq!(
        sender.calls().len(),
        2,
        "uncertain bubbles must not be retried"
    );
    assert!(!reply.state.delivery_uncertain);
    assert!(reply.state.thinking_delivery_uncertain && reply.state.filler_delivery_uncertain);
    assert_eq!(reply.state.thinking_message_ids, ["part-1", "part-2"]);
    assert_eq!(reply.state.filler_message_ids, ["part-1", "part-2"]);
    reply.abort().await;
    assert!(sender.calls().contains(&Call::Delete("part-1".into())));
    assert!(sender.calls().contains(&Call::Delete("part-2".into())));
}

#[tokio::test]
async fn partial_initial_draft_blocks_terminal_replacement_and_returns_the_original_receipt() {
    let (sender, mut reply) = turn("telegram:partial-draft");
    FakeSender::script(&sender.sends, Outcome::Err(partial_bubble));
    reply.on_text_delta("draft");
    reply.edit_tick().await;
    reply.on_text_delta(" more");
    reply.edit_tick().await;
    let error = reply
        .finalize_output_event(&Event::FinalMessage {
            text: "final".into(),
            parts: vec![],
            receipt: None,
        })
        .await
        .unwrap_err();
    let ProgressiveSendError::PartialOutcome(outcome) = error else {
        panic!("must retain partial receipt")
    };
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("part-1")
    );
    assert_eq!(outcome.receipt.parts[0].platform_message_id, "part-2");
    assert!(outcome.error.unwrap().sent_before_error);
    assert_eq!(sender.calls(), vec![Call::Send("draft".into())]);
}

#[tokio::test]
async fn terminal_edit_failures_retain_existing_receipt_and_never_send_replacements() {
    for make in [unavailable, unsupported, gone, other] {
        let (sender, mut reply) = turn("telegram:terminal-edit");
        reply.on_text_delta("draft");
        reply.edit_tick().await;
        FakeSender::script(&sender.edits, Outcome::Err(make));
        let error = reply
            .finalize_output_event(&Event::FinalMessage {
                text: "final".into(),
                parts: vec![],
                receipt: None,
            })
            .await
            .unwrap_err();
        let ProgressiveSendError::PartialOutcome(outcome) = error else {
            panic!("must retain receipt")
        };
        assert_eq!(
            outcome.receipt.primary_platform_message_id.as_deref(),
            Some("m1")
        );
        assert!(outcome.error.is_some());
        assert_eq!(
            sender
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Send(_)))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn final_text_edit_reuses_draft_receipt_and_removes_all_ephemeral_messages() {
    let (sender, mut reply) = turn("telegram:terminal-success");
    reply.on_text_delta("draft");
    reply.edit_tick().await;
    reply.on_thinking_delta("reason");
    reply.edit_tick().await;
    reply.filler_tick().await;
    let dispatch = reply
        .finalize_output_event(&Event::FinalMessage {
            text: "finished".into(),
            parts: vec![],
            receipt: None,
        })
        .await
        .unwrap();
    assert_eq!(
        dispatch.outcomes[0]
            .receipt
            .primary_platform_message_id
            .as_deref(),
        Some("m1")
    );
    assert_eq!(
        sender.calls(),
        vec![
            Call::Send("draft".into()),
            Call::Send("💭 Thinking:\n_reason_".into()),
            Call::Send("💭 _reason…_".into()),
            Call::Edit("m1".into(), "finished".into()),
            Call::Delete("m3".into()),
            Call::Delete("m2".into()),
        ]
    );
}

#[tokio::test]
async fn canonical_progress_events_update_visible_text_and_tool_status_without_sending_notices() {
    let (sender, mut reply) = typed_turn(None);
    let events = [
        (
            Event::ToolCallStarted {
                call_id: "call".into(),
                tool_name: "search".into(),
                arguments: json!({"q": "test"}),
                iteration: 1,
                display_label: Some("Search".into()),
                display_detail: None,
            },
            "🔧 Search…",
        ),
        (
            Event::ToolProgress {
                label: "fallback".into(),
                message: Some("Reading".into()),
            },
            "🔧 Reading…",
        ),
        (
            Event::ToolProgress {
                label: "Searching".into(),
                message: None,
            },
            "🔧 Searching…",
        ),
        (
            Event::ToolCallProgress {
                call_id: "call".into(),
                message: "Fetching".into(),
            },
            "🔧 Fetching…",
        ),
        (
            Event::ToolCallCompleted {
                call_id: "call".into(),
                tool_name: "search".into(),
                success: false,
                output_chars: 0,
                output: String::new(),
                arguments: None,
                elapsed_ms: 2,
                iteration: 1,
                failure: None,
                display_label: Some("Search".into()),
                display_detail: None,
                structured: None,
            },
            "🔧 Search ✗",
        ),
    ];
    for (event, expected) in events {
        let dispatch = reply.apply_output_event(&event, true).await.unwrap();
        assert!(dispatch.outcomes.is_empty() && dispatch.unsupported.is_empty());
        assert_eq!(reply.state.last_tool.as_deref(), Some(expected));
    }
    reply
        .apply_output_event(
            &Event::TextDelta {
                text: "visible".into(),
                iteration: Some(1),
            },
            true,
        )
        .await
        .unwrap();
    reply
        .apply_output_event(
            &Event::ThinkingDelta {
                text: "reason".into(),
                iteration: Some(1),
            },
            true,
        )
        .await
        .unwrap();
    reply
        .apply_output_event(
            &Event::ApprovalRequest {
                id: "approval".into(),
                prompt: "Run?".into(),
                choices: vec![],
            },
            false,
        )
        .await
        .unwrap();
    reply
        .apply_output_event(
            &Event::Clarification {
                prompt: "Which?".into(),
            },
            false,
        )
        .await
        .unwrap();
    assert_eq!(reply.state.content, "visible");
    assert_eq!(reply.state.thinking_accumulator, "reason");
    assert!(sender.sent.lock().unwrap().is_empty());
    let empty = reply
        .finalize_output_event(&Event::Cancellation { reason: None })
        .await
        .unwrap();
    assert!(empty.outcomes.is_empty() && empty.unsupported.is_empty());
}

#[tokio::test]
async fn approvals_and_voice_notes_are_delivered_as_ordered_typed_content() {
    let (sender, mut reply) = typed_turn(None);
    let voice = tinychannels_bus::VoiceNote {
        media: tinychannels_bus::MediaReference {
            kind: tinychannels_bus::MediaKind::Audio,
            url: Some("https://example.test/voice".into()),
            ..Default::default()
        },
        codec: Some("opus".into()),
        duration_ms: Some(100),
        transcript: Some(tinychannels_bus::VoiceTranscript {
            text: "transcript".into(),
            language: None,
            state: tinychannels_bus::VoiceTranscriptState::Complete,
            source: tinychannels_bus::VoiceTranscriptSource::Provider,
        }),
        ..Default::default()
    };
    for event in [
        Event::ApprovalRequest {
            id: "approval".into(),
            prompt: "Run?".into(),
            choices: vec!["yes".into(), "no".into()],
        },
        Event::ApprovalRequest {
            id: "approval-2".into(),
            prompt: "Continue?".into(),
            choices: vec![],
        },
        Event::VoiceNote {
            text: None,
            voice: voice.clone(),
        },
        Event::VoiceNote {
            text: Some("explicit".into()),
            voice: voice.clone(),
        },
    ] {
        let dispatch = reply.apply_output_event(&event, true).await.unwrap();
        assert_eq!(dispatch.outcomes.len(), 1);
        assert!(dispatch.failure.is_none());
    }
    let sent = sender.sent.lock().unwrap();
    assert_eq!(sent[0].text, "Approval requested: Run?\nChoices: yes, no");
    assert_eq!(sent[1].text, "Approval requested: Continue?");
    for (index, expected) in [(2, "transcript"), (3, "explicit")] {
        assert_eq!(sent[index].parts.len(), 2);
        assert_eq!(
            sent[index].parts[0],
            OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: expected.into(),
                    markdown_dialect: None
                }
            }
        );
        assert_eq!(
            sent[index].parts[1],
            OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: voice.media.clone(),
                    codec: voice.codec.clone(),
                    duration_ms: voice.duration_ms,
                },
            }
        );
    }
}

#[tokio::test]
async fn typed_notices_distinguish_unsupported_content_transport_failure_and_missing_receipts() {
    let event = Event::Clarification {
        prompt: "Which?".into(),
    };
    let (sender, mut reply) = typed_turn(None);
    sender.outcomes.lock().unwrap().push_back(Err(other()));
    assert!(matches!(
        reply.apply_output_event(&event, true).await,
        Err(ProgressiveSendError::Other(_))
    ));
    sender
        .outcomes
        .lock()
        .unwrap()
        .push_back(Ok(ChannelSendOutcome::default()));
    let dispatch = reply.apply_output_event(&event, true).await.unwrap();
    assert!(
        dispatch.outcomes[0]
            .error
            .as_ref()
            .unwrap()
            .sent_before_error
    );
    assert_eq!(
        dispatch.failure,
        Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport)
    );
    sender
        .outcomes
        .lock()
        .unwrap()
        .push_back(Err(ProgressiveSendError::UnsupportedContent));
    let voice = Event::VoiceNote {
        text: None,
        voice: tinychannels_bus::VoiceNote::default(),
    };
    let unsupported = reply.apply_output_event(&voice, true).await.unwrap();
    assert_eq!(unsupported.unsupported.len(), 1);
    assert!(unsupported.failure.is_none());
    sender.outcomes.lock().unwrap().push_back(Err(other()));
    assert!(reply.apply_output_event(&voice, true).await.is_err());
    sender.outcomes.lock().unwrap().push_back(Err(other()));
    assert!(
        reply
            .apply_output_event(
                &Event::Media {
                    text: None,
                    parts: vec![]
                },
                true
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn scoped_edit_latch_does_not_poison_an_independent_turn_or_follow_legacy_cache() {
    mark_channel_edits_unsupported("scoped-isolation:legacy");
    let sender = Arc::new(FakeSender::default());
    let make = || {
        ProgressiveReply::new_scoped(
            sender.clone(),
            "scoped-isolation:chat",
            tinychannels_bus::ChannelCapabilities {
                progressive_edits: true,
                ..Default::default()
            },
            ProgressiveOutputContext {
                turn_id: "isolated-turn".into(),
                channel: tinychannels_bus::ChannelRef {
                    id: "scoped-isolation".into(),
                    account_id: None,
                },
                conversation: tinychannels_bus::ConversationRef::default(),
            },
        )
    };
    let mut first = make();
    first.on_text_delta("first");
    first.edit_tick().await;
    FakeSender::script(&sender.edits, Outcome::Err(unsupported));
    first.on_text_delta(" update");
    first.edit_tick().await;
    assert!(first.state.edit_disabled);
    let mut second = make();
    second.on_text_delta("second");
    second.edit_tick().await;
    second.on_text_delta(" update");
    second.edit_tick().await;
    assert!(!second.state.edit_disabled);
    assert_eq!(
        sender.calls().last(),
        Some(&Call::Edit("m2".into(), "second update".into()))
    );
}

#[tokio::test]
async fn impossible_text_limit_and_unsupported_first_segment_never_send_a_replacement() {
    let (sender, mut reply) = typed_turn(Some((1, tinychannels_bus::LengthUnit::Utf8Bytes)));
    assert!(reply.send_parts(&[text_part("😀")], "😀").await.is_err());
    assert!(sender.sent.lock().unwrap().is_empty());
    let (sender, mut reply) = typed_turn(None);
    sender
        .outcomes
        .lock()
        .unwrap()
        .push_back(Err(ProgressiveSendError::UnsupportedContent));
    let dispatch = reply
        .finalize_output_event(&Event::FinalMessage {
            text: "fallback".into(),
            parts: vec![text_part("authoritative")],
            receipt: None,
        })
        .await
        .unwrap();
    assert_eq!(dispatch.unsupported, vec![text_part("authoritative")]);
    assert!(sender.legacy.calls().is_empty());
}

#[tokio::test]
async fn disabled_draft_flush_makes_no_provider_calls() {
    use super::draft::flush_streaming_edit;
    let sender = FakeSender::default();
    let mut state = StreamingState {
        content: "text".into(),
        edit_disabled: true,
        ..Default::default()
    };
    flush_streaming_edit(&sender, "empty", &mut state, false).await;
    assert!(sender.calls().is_empty());
}

#[tokio::test]
async fn ids_only_draft_receipt_edits_in_place_without_replacing_delivered_message() {
    for receipt in [
        MessageReceipt {
            primary_platform_message_id: Some("known-id".into()),
            ..Default::default()
        },
        MessageReceipt {
            platform_message_ids: vec!["known-id".into()],
            ..Default::default()
        },
        MessageReceipt {
            parts: vec![MessageReceiptPart {
                platform_message_id: "known-id".into(),
                edit_token: Some("opaque-edit".into()),
                delete_token: Some("opaque-delete".into()),
                ..Default::default()
            }],
            ..Default::default()
        },
    ] {
        for response in [
            serde_json::to_value(&receipt).unwrap(),
            json!({"receipt": receipt}),
            json!({"data": {"receipt": receipt}}),
        ] {
            let (sender, mut reply) = turn("telegram:ids-only-receipt");
            FakeSender::script(&sender.sends, Outcome::Ok(response));
            reply.on_text_delta("draft");
            reply.edit_tick().await;
            reply.on_text_delta(" update");
            reply.edit_tick().await;
            let dispatch = reply
                .finalize_output_event(&Event::FinalMessage {
                    text: "finished".into(),
                    parts: vec![],
                    receipt: None,
                })
                .await
                .expect("a known provider ID is sufficient to edit a valid draft receipt");
            assert_eq!(
                dispatch.outcomes[0]
                    .receipt
                    .primary_platform_message_id
                    .as_deref(),
                Some("known-id")
            );
            assert_eq!(
                sender.calls(),
                [
                    Call::Send("draft".into()),
                    Call::Edit("known-id".into(), "draft update".into()),
                    Call::Edit("known-id".into(), "finished".into()),
                ]
            );
        }
    }
}

#[tokio::test]
async fn direct_typed_receipts_preserve_opaque_authority_for_drafts_and_ephemeral_cleanup() {
    let (sender, mut reply) = turn("telegram:direct-typed-receipts");
    let receipt = MessageReceipt {
        channel: Some(tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("bot-account".into()),
        }),
        conversation: Some(tinychannels_bus::ConversationRef {
            id: "conversation".into(),
            topic_id: Some("topic".into()),
            ..Default::default()
        }),
        idempotency_key: Some("original-key".into()),
        primary_platform_message_id: Some("provider-id".into()),
        platform_message_ids: vec!["provider-id".into()],
        edit_token: Some("opaque-edit-authority".into()),
        delete_token: Some("opaque-delete-authority".into()),
        parts: vec![MessageReceiptPart {
            platform_message_id: "provider-id".into(),
            edit_token: Some("opaque-part-edit".into()),
            delete_token: Some("opaque-part-delete".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    FakeSender::script(
        &sender.sends,
        Outcome::Ok(serde_json::to_value(&receipt).unwrap()),
    );
    reply.on_text_delta("draft");
    reply.edit_tick().await;
    assert_eq!(
        reply.state.message_receipt.as_ref(),
        Some(&receipt),
        "direct typed Outcome receipts must retain all provider authority"
    );
    FakeSender::script(
        &sender.sends,
        Outcome::Ok(serde_json::to_value(&receipt).unwrap()),
    );
    reply.on_thinking_delta("reasoning");
    reply.edit_tick().await;
    assert_eq!(
        reply.state.thinking_message_receipt.as_ref(),
        Some(&receipt)
    );
    FakeSender::script(
        &sender.sends,
        Outcome::Ok(serde_json::to_value(&receipt).unwrap()),
    );
    reply.filler_tick().await;
    assert_eq!(reply.state.filler_message_receipts, vec![receipt.clone()]);
    let dispatch = reply
        .finalize_output_event(&Event::FinalMessage {
            text: "finished".into(),
            parts: vec![],
            receipt: None,
        })
        .await
        .unwrap();
    assert_eq!(dispatch.outcomes[0].receipt, receipt);
}

#[test]
fn unrelated_legacy_response_objects_do_not_become_normalized_receipts() {
    for response in [
        json!({"success": true}),
        json!({"id": "legacy-id"}),
        json!({"data": {"messageId": 8}}),
    ] {
        assert!(super::state::extract_message_receipt(&response).is_none());
    }
}

#[tokio::test]
async fn segmented_projection_retains_later_receipt_address_tokens_and_raw_provider_facts() {
    let (sender, mut reply) = typed_turn(Some((2, tinychannels_bus::LengthUnit::Characters)));
    let first = ChannelSendOutcome {
        receipt: MessageReceipt {
            parts: vec![MessageReceiptPart {
                platform_message_id: "first".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        ..Default::default()
    };
    let second = ChannelSendOutcome {
        receipt: MessageReceipt {
            primary_platform_message_id: Some("second".into()),
            platform_message_ids: vec!["second".into()],
            channel: Some(tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: Some("bot".into()),
            }),
            conversation: Some(tinychannels_bus::ConversationRef {
                id: "chat".into(),
                ..Default::default()
            }),
            idempotency_key: Some("second-key".into()),
            thread_id: Some("thread".into()),
            reply_to_id: Some("reply".into()),
            edit_token: Some("second-edit".into()),
            delete_token: Some("second-delete".into()),
            sent_at: 9,
            raw: Some(vec![
                tinychannels_bus::channel::MessageReceiptSourceResult {
                    message_id: Some("second".into()),
                    meta: Some(json!({"accepted": true})),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        },
        ..Default::default()
    };
    sender
        .outcomes
        .lock()
        .unwrap()
        .extend([Ok(first.clone()), Ok(second.clone())]);
    let outcome = reply.send_parts(&[], "abcd").await.unwrap();
    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.channel, second.receipt.channel);
    assert_eq!(outcome.receipt.conversation, second.receipt.conversation);
    assert_eq!(
        outcome.receipt.idempotency_key,
        second.receipt.idempotency_key
    );
    assert_eq!(outcome.receipt.thread_id, second.receipt.thread_id);
    assert_eq!(outcome.receipt.reply_to_id, second.receipt.reply_to_id);
    assert_eq!(outcome.receipt.edit_token, second.receipt.edit_token);
    assert_eq!(outcome.receipt.delete_token, second.receipt.delete_token);
    assert_eq!(outcome.receipt.raw, second.receipt.raw);
    assert_eq!(outcome.receipt.sent_at, 9);
    assert_eq!(outcome.receipt.parts[0], first.receipt.parts[0]);
    assert_eq!(outcome.receipt.parts[1].platform_message_id, "second");
    let sent = sender.sent.lock().unwrap();
    assert_eq!(
        sent.iter()
            .map(|call| call.text.as_str())
            .collect::<Vec<_>>(),
        ["ab", "cd"]
    );
    for (call, text) in sent.iter().zip(["ab", "cd"]) {
        assert_eq!(
            call.parts,
            vec![OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: text.into(),
                    markdown_dialect: None
                }
            }]
        );
    }
}

#[tokio::test]
async fn partial_thinking_without_ids_fences_only_thinking_and_does_not_invent_cleanup_authority() {
    let (sender, mut reply) = turn("telegram:thinking-no-ids");
    FakeSender::script(
        &sender.sends,
        Outcome::Err(|| {
            ProgressiveSendError::PartialOutcome(Box::new(ChannelSendOutcome {
                receipt: MessageReceipt {
                    idempotency_key: Some("accepted-but-unidentified".into()),
                    ..Default::default()
                },
                error: Some(tinychannels_bus::channel::ChannelSendError {
                    sent_before_error: true,
                    ..Default::default()
                }),
            }))
        }),
    );
    reply.on_thinking_delta("reasoning");
    reply.edit_tick().await;
    reply.on_thinking_delta(" more");
    reply.edit_tick().await;
    assert!(reply.state.thinking_sent);
    assert!(reply.state.thinking_delivery_uncertain);
    assert!(reply.state.thinking_edit_disabled);
    assert!(reply.state.thinking_message_id.is_none());
    assert!(reply.state.thinking_message_ids.is_empty());
    assert!(reply.state.thinking_message_receipt.is_none());
    assert!(!reply.state.delivery_uncertain);
    reply.on_text_delta("visible");
    reply.edit_tick().await;
    reply.abort().await;
    assert_eq!(
        sender.calls(),
        [
            Call::Send("💭 Thinking:\n_reasoning_".into()),
            Call::Send("visible".into()),
            Call::Delete("m2".into()),
        ]
    );
}

#[tokio::test]
async fn unsupported_draft_edits_keep_owned_message_for_cleanup_without_retrying() {
    let (sender, mut reply) = turn("telegram:draft-unsupported-content");
    reply.on_text_delta("draft");
    reply.edit_tick().await;
    FakeSender::script(
        &sender.edits,
        Outcome::Err(|| ProgressiveSendError::UnsupportedContent),
    );
    reply.on_text_delta(" update");
    reply.edit_tick().await;
    reply.on_text_delta(" again");
    reply.edit_tick().await;
    assert!(reply.state.edit_disabled);
    assert_eq!(reply.state.message_id.as_deref(), Some("m1"));
    reply.abort().await;
    assert_eq!(
        sender.calls(),
        [
            Call::Send("draft".into()),
            Call::Edit("m1".into(), "draft update".into()),
            Call::Delete("m1".into()),
        ]
    );
}

#[derive(Clone, Default)]
struct DeliveryDiagnostics(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for DeliveryDiagnostics {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                write!(&mut self.0, "{}={value:?};", field.name()).unwrap();
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn uncertain_send_diagnostics_report_only_shape_counts_without_message_or_receipt_secrets() {
    let diagnostics = DeliveryDiagnostics::default();
    // tracing's single-dispatch optimization registers shared callsites using
    // the registering thread's subscriber. Another test thread has no local
    // collector and can therefore cache Interest::never for our edit event.
    // Keep two local registrations alive so tracing consults its dispatcher
    // registry; only the scoped subscriber below receives this thread's events.
    let _registration = tracing::Dispatch::new(diagnostics.clone());
    let _guard = tracing::subscriber::set_default(diagnostics.clone());
    // Reproduce first registration on an unrelated, unobserved thread before
    // checking that our scoped collector still sees the same delivery events.
    std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let (_, mut reply) = turn("telegram:unobserved-edit-diagnostics");
                reply.on_text_delta("unobserved");
                reply.edit_tick().await;
                reply.on_text_delta(" update");
                reply.edit_tick().await;
            });
    })
    .join()
    .unwrap();
    let (sender, mut reply) = turn("telegram:redacted-diagnostics");
    let receipt = MessageReceipt {
        idempotency_key: Some("private-idempotency-key".into()),
        edit_token: Some("private-edit-token".into()),
        delete_token: Some("private-delete-token".into()),
        raw: Some(vec![
            tinychannels_bus::channel::MessageReceiptSourceResult {
                meta: Some(json!({"private": "private-provider-payload"})),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    FakeSender::script(
        &sender.sends,
        Outcome::Ok(serde_json::to_value(&receipt).unwrap()),
    );
    reply.on_text_delta("private-user-message");
    reply.edit_tick().await;
    assert!(reply.state.delivery_uncertain && reply.state.edit_disabled);
    assert_eq!(reply.state.message_receipt, Some(receipt.clone()));
    assert!(reply.state.message_id.is_none());
    reply.on_text_delta(" more");
    reply.edit_tick().await;
    assert_eq!(sender.calls().len(), 1);
    let (sender, mut reply) = turn("telegram:redacted-ephemeral");
    FakeSender::script(
        &sender.sends,
        Outcome::Ok(serde_json::to_value(&receipt).unwrap()),
    );
    reply.on_thinking_delta("private-model-reasoning");
    reply.edit_tick().await;
    FakeSender::script(
        &sender.sends,
        Outcome::Ok(serde_json::to_value(receipt).unwrap()),
    );
    reply.filler_tick().await;
    assert!(reply.state.thinking_delivery_uncertain && reply.state.filler_delivery_uncertain);
    let (sender, mut reply) = turn("telegram:redacted-edit");
    reply.on_text_delta("private-edit-message");
    reply.edit_tick().await;
    reply.on_text_delta(" update");
    reply.edit_tick().await;
    assert_eq!(
        sender.calls(),
        [
            Call::Send("private-edit-message".into()),
            Call::Edit("m1".into(), "private-edit-message update".into()),
        ]
    );

    let messages = diagnostics.0.lock().unwrap().join("\n");
    assert!(
        !messages.contains("unobserved"),
        "collector escaped its test thread"
    );
    assert!(messages.contains("initial draft response lacked an id"));
    assert!(messages.contains("receipt_parts=0"));
    assert!(messages.contains("response_fields="));
    assert!(
        messages.contains("edit ok"),
        "captured diagnostics: {messages}; edit calls: {:?}; static maximum: {:?}; runtime maximum: {:?}; debug enabled: {}",
        sender.calls(),
        tracing::level_filters::STATIC_MAX_LEVEL,
        tracing::level_filters::LevelFilter::current(),
        tracing::enabled!(tracing::Level::DEBUG)
    );
    for secret in [
        "private-idempotency-key",
        "private-edit-token",
        "private-delete-token",
        "private-provider-payload",
        "private-user-message",
        "private-model-reasoning",
        "private-edit-message",
    ] {
        assert!(
            !messages.contains(secret),
            "delivery diagnostic exposed {secret}"
        );
    }
}
