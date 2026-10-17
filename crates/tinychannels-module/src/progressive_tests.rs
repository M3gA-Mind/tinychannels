use super::*;
use base64::Engine as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinychannels_bus::module_progressive::SubagentOutput;
use tokio::sync::Notify;

struct ConfiguredChannel(&'static str);
struct ConfiguredChannelWithCapabilities {
    name: &'static str,
    capabilities: tinychannels_bus::ChannelCapabilities,
}

#[async_trait::async_trait]
#[allow(
    deprecated,
    reason = "test fake implements the required v21 compatibility methods"
)]
impl tinychannels::Channel for ConfiguredChannelWithCapabilities {
    fn name(&self) -> &str {
        self.name
    }
    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.capabilities.clone()
    }
    async fn send(&self, _message: &tinychannels::SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[async_trait::async_trait]
#[allow(
    deprecated,
    reason = "test fake implements the required v21 compatibility methods"
)]
impl tinychannels::Channel for ConfiguredChannel {
    fn name(&self) -> &str {
        self.0
    }
    async fn send(&self, _message: &tinychannels::SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[test]
fn legacy_feed_retains_the_v1_event_wire_shape() {
    let command = ProgressiveCommand::Feed {
        handle: "turn-handle".into(),
        events: vec![
            tinychannels_bus::module_progressive::ProgressiveEvent::TextDelta("chunk".into()),
        ],
    };
    assert_eq!(
        serde_json::to_value(command).unwrap(),
        serde_json::json!({
            "operation": "feed",
            "handle": "turn-handle",
            "events": [{"event": "text_delta", "value": "chunk"}]
        })
    );
}

#[test]
fn subagent_output_dto_preserves_authorized_worktree_and_iteration_facts() {
    let event = ChannelOutputEvent::Subagent(Box::new(SubagentOutput {
        agent_id: "agent-2".into(),
        task_id: "task-9".into(),
        state: "completed".into(),
        mode: None,
        dedicated_thread: Some(true),
        parent_call_id: None,
        call_id: None,
        iteration: Some(3),
        max_iterations: Some(5),
        prompt_chars: Some(128),
        message: None,
        display_name: Some("executor".into()),
        tool_name: None,
        arguments: None,
        success: Some(true),
        elapsed_ms: Some(450),
        output_chars: Some(12),
        output: Some("short result".into()),
        structured: None,
        usage: None,
        failure: None,
        display_label: None,
        display_detail: None,
        worker_thread_id: Some("child-thread".into()),
        worktree_path: Some("/workspace/.worktrees/task-9".into()),
        changed_files: vec!["src/main.rs".into()],
        dirty_status: Some(true),
        stop: None,
        extended_policy: Some(true),
    }));
    let encoded = serde_json::to_value(&event).unwrap();
    let decoded: ChannelOutputEvent = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(decoded, event);
    assert_eq!(encoded["worktreePath"], "/workspace/.worktrees/task-9");
    assert_eq!(encoded["extendedPolicy"], true);
}

#[derive(Clone, Default)]
struct Callback {
    sends: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<ProgressiveTransportRequest>>>,
    output_result: Arc<Mutex<Option<ProgressiveTransportResult>>>,
    output_queue: Arc<Mutex<std::collections::VecDeque<ProgressiveTransportResult>>>,
    send_result: Arc<Mutex<Option<ProgressiveTransportResult>>>,
    send_queue: Arc<Mutex<std::collections::VecDeque<ProgressiveTransportResult>>>,
    rpc_error_after_send: Arc<Mutex<bool>>,
    unexpected_typed_response: Arc<Mutex<bool>>,
    block: bool,
    block_synthesis: Arc<AtomicBool>,
    entered: Arc<Notify>,
    released: Arc<Notify>,
    voice_policy: Arc<Mutex<Option<tinychannels_bus::HostVoiceReplyPolicy>>>,
    synthesis_calls: Arc<AtomicUsize>,
    voice_store_calls: Arc<AtomicUsize>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn get_voice_reply_policy(
        &self,
        _request: tinychannels_bus::HostVoiceReplyPolicyRequest,
    ) -> Result<tinychannels_bus::HostVoiceReplyPolicy> {
        Ok(self.voice_policy.lock().await.clone().unwrap_or(
            tinychannels_bus::HostVoiceReplyPolicy {
                mode: tinychannels_bus::HostVoiceReplyMode::Mirror,
                inbound_has_voice: false,
                explicitly_requested: false,
            },
        ))
    }

    async fn synthesize_speech(
        &self,
        _request: tinychannels_bus::HostSpeechSynthesisRequest,
    ) -> Result<tinychannels_bus::HostSpeechSynthesisResult> {
        self.synthesis_calls.fetch_add(1, Ordering::SeqCst);
        if self.block_synthesis.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.released.notified().await;
        }
        Ok(tinychannels_bus::HostSpeechSynthesisResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(progressive_test_wav()),
            mime_type: "audio/wav".into(),
            duration_ms: Some(8_000),
        })
    }

    async fn store_voice_media(
        &self,
        _request: tinychannels_bus::HostStoreVoiceMediaRequest,
    ) -> Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        self.voice_store_calls.fetch_add(1, Ordering::SeqCst);
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("durable-host-voice".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            },
            accessible_url: Some("https://media.example/authorized/voice".into()),
        })
    }

    async fn dispatch_controller_backend(
        &self,
        request: ChannelBackendRequest,
    ) -> Result<ChannelBackendResult> {
        let ChannelBackendRequest::ProgressiveTransport(request) = request else {
            return Err(Error::failed("unexpected callback"));
        };
        let request = *request;
        self.requests.lock().await.push(request.clone());
        let typed_send = matches!(request, ProgressiveTransportRequest::SendTyped { .. });
        match &request {
            ProgressiveTransportRequest::Send { message, .. } => {
                self.sends.lock().await.push(message.clone());
            }
            ProgressiveTransportRequest::SendTyped { text, parts, .. } => {
                self.sends
                    .lock()
                    .await
                    .push(serde_json::json!({"text": text, "parts": parts}));
            }
            _ => {}
        }
        if (typed_send || matches!(request, ProgressiveTransportRequest::Send { .. })) && self.block
        {
            self.entered.notify_one();
            self.released.notified().await;
        }
        let queued = if typed_send {
            self.send_queue.lock().await.pop_front()
        } else if matches!(request, ProgressiveTransportRequest::OutputEvent { .. }) {
            self.output_queue.lock().await.pop_front()
        } else {
            None
        };
        let configured = if queued.is_some() {
            queued
        } else if matches!(request, ProgressiveTransportRequest::OutputEvent { .. }) {
            self.output_result.lock().await.clone()
        } else {
            self.send_result.lock().await.clone()
        };
        let result = configured.unwrap_or_else(|| {
            if typed_send {
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                outcome.receipt.platform_message_ids = vec!["fixture-message".into()];
                ProgressiveTransportResult::Outcome(Box::new(outcome))
            } else if matches!(request, ProgressiveTransportRequest::OutputEvent { .. }) {
                ProgressiveTransportResult::Acknowledged
            } else {
                ProgressiveTransportResult::Success(serde_json::json!({"id": "fixture-message"}))
            }
        });
        if typed_send || matches!(request, ProgressiveTransportRequest::Send { .. }) {
            let fail_after_send = std::mem::take(&mut *self.rpc_error_after_send.lock().await);
            if fail_after_send {
                return Err(Error::failed(
                    "fixture callback failed after accepting send",
                ));
            }
        }
        if typed_send && *self.unexpected_typed_response.lock().await {
            return Ok(ChannelBackendResult::Status(Vec::new()));
        }
        Ok(ChannelBackendResult::ProgressiveTransport(Box::new(result)))
    }
}

async fn fixture(block: bool) -> (Replies, Callback) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    connection
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let callback = Callback {
        block,
        ..Default::default()
    };
    connection
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callback.clone(),
        )
        .await
        .unwrap();
    (
        Replies::new(
            HostChannels::new(connection),
            Arc::new(Mutex::new(HashMap::new())),
        ),
        callback,
    )
}

async fn open(replies: &Replies) -> String {
    match replies
        .execute(ProgressiveCommand::Open {
            channel: "discord:fixture".into(),
        })
        .await
        .unwrap()
    {
        ProgressiveResult::Opened { handle } => handle,
        _ => panic!("expected opened handle"),
    }
}

async fn open_scoped(
    replies: &Replies,
    account: &str,
    turn_id: &str,
    capabilities: tinychannels_bus::ChannelCapabilities,
) -> String {
    open_scoped_provider(replies, "discord", account, turn_id, capabilities).await
}

async fn open_scoped_provider(
    replies: &Replies,
    provider: &'static str,
    account: &str,
    turn_id: &str,
    capabilities: tinychannels_bus::ChannelCapabilities,
) -> String {
    replies.running.lock().await.insert(
        account.to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: provider,
                capabilities,
            }),
            Arc::new(crate::provider::Hooks::default()),
            replies.host.clone(),
        ),
    );
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: provider.into(),
                account_id: Some(account.to_owned()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: turn_id.to_owned(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected scoped output handle")
    };
    handle
}

#[tokio::test]
async fn terminal_retry_reuses_one_module_send_and_rejects_changed_content() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    for _ in 0..2 {
        assert_eq!(
            replies
                .execute(ProgressiveCommand::Finish {
                    handle: handle.clone(),
                    text: "final fixture".into()
                })
                .await
                .unwrap(),
            ProgressiveResult::Closed
        );
    }
    assert!(
        replies
            .execute(ProgressiveCommand::Finish {
                handle: handle.clone(),
                text: "changed fixture".into()
            })
            .await
            .is_err()
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    let sends = callback.sends.lock().await;
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0]["text"], "final fixture");
    assert!(
        sends[0]["idempotencyKey"]
            .as_str()
            .is_some_and(|key| !key.is_empty())
    );
}

#[tokio::test]
async fn cancelling_the_close_caller_preserves_terminal_delivery_and_cleanup() {
    let (replies, callback) = fixture(true).await;
    let replies = Arc::new(replies);
    let handle = open(&replies).await;
    let retained = replies.clone();
    let closing_handle = handle.clone();
    let caller = tokio::spawn(async move {
        retained
            .execute(ProgressiveCommand::Finish {
                handle: closing_handle,
                text: "retained fixture".into(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), callback.entered.notified())
        .await
        .unwrap();
    caller.abort();
    let _ = caller.await;
    callback.released.notify_one();
    replies
        .execute(ProgressiveCommand::Finish {
            handle,
            text: "retained fixture".into(),
        })
        .await
        .unwrap();
    assert_eq!(callback.sends.lock().await.len(), 1);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_terminal_feed_replays_retained_receipt_without_resending() {
    let (replies, callback) = fixture(true).await;
    let handle = open(&replies).await;
    let terminal_events = vec![ChannelOutputEvent::FinalMessage {
        text: "retained terminal".into(),
        parts: vec![],
        receipt: None,
    }];
    let retained = Arc::new(replies);
    let calling = retained.clone();
    let calling_handle = handle.clone();
    let calling_events = terminal_events.clone();
    let caller = tokio::spawn(async move {
        calling
            .execute(ProgressiveCommand::FeedOutput {
                handle: calling_handle,
                events: calling_events,
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), callback.entered.notified())
        .await
        .unwrap();
    caller.abort();
    let _ = caller.await;
    callback.released.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if callback.sends.lock().await.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let replay = retained
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: terminal_events,
        })
        .await
        .unwrap();
    assert_eq!(
        replay,
        ProgressiveResult::DeliveryOutcome {
            outcome: {
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                outcome.receipt.platform_message_ids = vec!["fixture-message".into()];
                Box::new(outcome)
            }
        }
    );
    assert_eq!(callback.sends.lock().await.len(), 1);
    let changed = retained
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "changed".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap_err();
    assert!(changed.to_string().contains("differs"));
    retained.shutdown().await.unwrap();
}

#[tokio::test]
async fn identical_terminal_waiters_share_result_when_one_waiter_cancels() {
    let (replies, callback) = fixture(true).await;
    let handle = open(&replies).await;
    let events = vec![ChannelOutputEvent::FinalMessage {
        text: "shared terminal".into(),
        parts: vec![],
        receipt: None,
    }];
    let retained = Arc::new(replies);
    let first_replies = retained.clone();
    let first_handle = handle.clone();
    let first_events = events.clone();
    let first = tokio::spawn(async move {
        first_replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: first_handle,
                events: first_events,
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), callback.entered.notified())
        .await
        .unwrap();
    let mut waiters = Vec::new();
    for _ in 0..3 {
        let replies = retained.clone();
        let events = events.clone();
        let handle = handle.clone();
        waiters.push(tokio::spawn(async move {
            replies
                .execute(ProgressiveCommand::FeedOutput { handle, events })
                .await
        }));
    }
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    waiters[0].abort();
    callback.released.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(result, ProgressiveResult::DeliveryOutcome { .. }));
    for waiter in waiters.into_iter().skip(1) {
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            ProgressiveResult::DeliveryOutcome { .. }
        ));
    }
    assert_eq!(callback.sends.lock().await.len(), 1);
    retained.shutdown().await.unwrap();
}

#[tokio::test]
async fn handles_cannot_cross_private_runtime_reply_stores() {
    let (first, _) = fixture(false).await;
    let (second, _) = fixture(false).await;
    let handle = open(&first).await;
    assert!(
        second
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![ChannelOutputEvent::TextDelta {
                    text: "fixture".into(),
                    iteration: None,
                }]
            })
            .await
            .is_err()
    );
    first
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_feed_is_rejected_atomically_before_updating_reply_state() {
    let (replies, _) = fixture(false).await;
    let mut reply = ProgressiveReply::new(
        Arc::new(Sender {
            host: replies.host.clone(),
            context: None,
            capabilities: tinychannels_bus::ChannelCapabilities::unknown(),
            channel: "discord:fixture".into(),
            send_sequence: std::sync::atomic::AtomicU64::new(0),
        }),
        "discord:fixture",
        tinychannels_bus::ChannelCapabilities::unknown(),
    );
    apply(
        &mut reply,
        vec![ChannelOutputEvent::TextDelta {
            text: "x".repeat(MAX_CONTENT),
            iteration: None,
        }],
        false,
    )
    .await
    .unwrap();
    assert!(
        apply(
            &mut reply,
            vec![
                ChannelOutputEvent::ToolProgress {
                    label: "fixture-tool".into(),
                    message: None
                },
                ChannelOutputEvent::TextDelta {
                    text: "extra".into(),
                    iteration: None,
                }
            ],
            false,
        )
        .await
        .is_err()
    );
    assert_eq!(reply.state().content.len(), MAX_CONTENT);
    assert!(reply.state().last_tool.is_none());
    let handle = open(&replies).await;
    assert!(
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![ChannelOutputEvent::ThinkingDelta {
                    text: "x".repeat(MAX_REQUEST),
                    iteration: None
                }]
            })
            .await
            .is_err()
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
}

#[tokio::test]
async fn reply_capacity_refuses_seventeenth_and_shutdown_drains_every_worker() {
    let (replies, callback) = fixture(false).await;
    for _ in 0..MAX_ACTIVE {
        open(&replies).await;
    }
    assert!(
        replies
            .execute(ProgressiveCommand::Open {
                channel: "discord:overflow".into()
            })
            .await
            .is_err()
    );
    replies.shutdown().await.unwrap();
    assert!(callback.sends.lock().await.is_empty());
    assert!(replies.state.lock().await.entries.is_empty());
    assert!(
        replies
            .execute(ProgressiveCommand::Open {
                channel: "discord:after-shutdown".into()
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn completed_progressive_handles_are_evicted_at_the_retention_bound() {
    let (replies, _) = fixture(false).await;
    let mut handles = Vec::new();
    for _ in 0..=MAX_RETAINED {
        let handle = open(&replies).await;
        replies
            .execute(ProgressiveCommand::Close {
                handle: handle.clone(),
            })
            .await
            .unwrap();
        handles.push(handle);
    }
    assert_eq!(replies.state.lock().await.entries.len(), MAX_RETAINED);
    let mut evicted = 0;
    for handle in handles.iter().take(MAX_RETAINED) {
        let result = replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![],
            })
            .await;
        if result
            .as_ref()
            .err()
            .is_some_and(|error| error.to_string().contains("unknown progressive handle"))
        {
            evicted += 1;
        }
    }
    assert_eq!(evicted, 1, "one completed handle is evicted at capacity");
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn progressive_commands_validate_event_batches_and_keep_legacy_finish_usable() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;

    let too_many = (0..33)
        .map(|index| ChannelOutputEvent::ThinkingDelta {
            text: format!("thinking-{index}"),
            iteration: None,
        })
        .collect();
    let error = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: too_many,
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("too many progressive events"));

    let error = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![
                ChannelOutputEvent::FinalMessage {
                    text: "terminal".into(),
                    parts: vec![],
                    receipt: None,
                },
                ChannelOutputEvent::Cancellation {
                    reason: Some("cancelled".into()),
                },
            ],
        })
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("terminal progressive event must be unique and last")
    );

    let error = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::Native {
                data: serde_json::json!({"payload": "x".repeat(MAX_REQUEST + 1)}),
            }],
        })
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("progressive request exceeds its limit")
    );
    assert!(callback.requests.lock().await.is_empty());

    let error = replies
        .execute(ProgressiveCommand::Finish {
            handle: handle.clone(),
            text: "x".repeat(MAX_CONTENT + 1),
        })
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("progressive content exceeds its limit")
    );

    let legacy_feed: ProgressiveCommand = serde_json::from_value(serde_json::json!({
        "operation": "feed",
        "handle": handle,
        "events": [
            {"event": "text_delta", "value": "legacy chunk"},
            {"event": "thinking_delta", "value": "legacy thought"},
            {"event": "tool_call", "value": "search"},
            {"event": "tool_result", "value": {"tool_name": "search", "success": true}}
        ]
    }))
    .unwrap();
    assert_eq!(
        replies.execute(legacy_feed).await.unwrap(),
        ProgressiveResult::Updated
    );
    replies
        .execute(ProgressiveCommand::Finish {
            handle,
            text: "legacy final".into(),
        })
        .await
        .unwrap();
    let sends = callback.sends.lock().await;
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0]["text"], "legacy final");
    drop(sends);
    let requests = callback.requests.lock().await;
    assert!(
        requests
            .iter()
            .all(|request| !matches!(request, ProgressiveTransportRequest::OutputEvent { .. }))
    );
    assert!(
        requests
            .iter()
            .all(|request| !matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
    );
    assert!(
        requests
            .iter()
            .any(|request| matches!(request, ProgressiveTransportRequest::Send { .. }))
    );
    drop(requests);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn progressive_open_and_handle_errors_are_closed_after_shutdown() {
    let (replies, _) = fixture(false).await;
    let invalid = replies
        .execute(ProgressiveCommand::Open { channel: "".into() })
        .await
        .unwrap_err();
    assert!(invalid.to_string().contains("invalid progressive target"));

    let missing = replies
        .execute(ProgressiveCommand::Finish {
            handle: "missing".into(),
            text: "final".into(),
        })
        .await
        .unwrap_err();
    assert!(missing.to_string().contains("unknown progressive handle"));

    replies.shutdown().await.unwrap();
    let stopped = replies
        .execute(ProgressiveCommand::Open {
            channel: "discord:after-shutdown".into(),
        })
        .await
        .unwrap_err();
    assert!(
        stopped
            .to_string()
            .contains("progressive replies are shutting down")
    );
}

#[tokio::test]
async fn typed_transport_outcome_error_stops_later_batch_delivery() {
    let (replies, callback) = fixture(false).await;
    let handle = open_scoped(
        &replies,
        "discord:outcome-error",
        "outcome-error-turn",
        tinychannels_bus::ChannelCapabilities::default(),
    )
    .await;
    let mut partial = tinychannels_bus::channel::ChannelSendOutcome::default();
    partial.receipt.platform_message_ids = vec!["accepted-before-error".into()];
    partial.error = Some(tinychannels_bus::channel::ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
        message: "provider outcome is ambiguous".into(),
        sent_before_error: true,
        ..Default::default()
    });
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::Outcome(Box::new(
        partial.clone(),
    )));
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![tinychannels_bus::ContentPart::Image {
                        media: tinychannels_bus::MediaReference {
                            kind: tinychannels_bus::MediaKind::Image,
                            url: Some("https://media.invalid/uncertain.png".into()),
                            ..Default::default()
                        },
                        caption: None,
                        alt_text: None,
                    }],
                },
                ChannelOutputEvent::FinalMessage {
                    text: "must not send after unknown outcome".into(),
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();
    let ProgressiveResult::DeliveryBatch {
        outcomes, failure, ..
    } = result
    else {
        panic!("expected ordered delivery batch");
    };
    assert!(failure.is_some());
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].receipt, partial.receipt);
    assert!(outcomes[0].error.is_some());
    assert_eq!(callback.sends.lock().await.len(), 1);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_aborts_every_open_reply_without_sending_cancellation_text() {
    let (replies, callback) = fixture(false).await;
    let replies = Arc::new(replies);
    open(&replies).await;
    open(&replies).await;
    let retained = replies.clone();
    let shutdown = tokio::spawn(async move { retained.shutdown().await });
    shutdown.await.unwrap().unwrap();
    assert_eq!(replies.state.lock().await.entries.len(), 0);
    assert!(callback.sends.lock().await.is_empty());
    replies.shutdown().await.unwrap();
    assert!(replies.state.lock().await.entries.is_empty());
}

#[tokio::test]
async fn full_reply_within_content_limit_survives_json_expansion() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    // Quotes and backslashes expand under JSON escaping. Admission must
    // permit the same content budget as the progressive state machine.
    let text = "\"\\".repeat(MAX_CONTENT / 2);
    assert_eq!(
        replies
            .execute(ProgressiveCommand::Finish {
                handle: handle.clone(),
                text: text.clone()
            })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    let sends = callback.sends.lock().await;
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0]["text"], text);
}

#[tokio::test]
async fn oversized_terminal_content_is_rejected_without_closing_reply() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    assert!(
        replies
            .execute(ProgressiveCommand::Finish {
                handle: handle.clone(),
                text: "x".repeat(MAX_CONTENT + 1)
            })
            .await
            .is_err()
    );
    assert_eq!(
        replies
            .execute(ProgressiveCommand::Finish {
                handle: handle.clone(),
                text: "valid reply".into()
            })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    assert_eq!(callback.sends.lock().await[0]["text"], "valid reply");
}

#[tokio::test]
async fn feed_updates_text_thinking_and_tool_state_before_terminal_send() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    assert_eq!(
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![
                    ChannelOutputEvent::ThinkingDelta {
                        text: "considering".into(),
                        iteration: None
                    },
                    ChannelOutputEvent::ToolProgress {
                        label: "lookup".into(),
                        message: None
                    },
                    ChannelOutputEvent::ToolCallCompleted {
                        call_id: "call-1".into(),
                        tool_name: "lookup".into(),
                        success: true,
                        output_chars: 0,
                        output: String::new(),
                        arguments: Some(serde_json::json!({"q": "safe test"})),
                        elapsed_ms: 0,
                        iteration: 1,
                        failure: None,
                        display_label: None,
                        display_detail: None,
                        structured: Some(serde_json::json!({"kind": "search", "count": 1})),
                    },
                    ChannelOutputEvent::TextDelta {
                        text: "partial answer".into(),
                        iteration: None,
                    },
                ],
            })
            .await
            .unwrap(),
        ProgressiveResult::Updated
    );
    let forwarded = callback.requests.lock().await.iter().any(|request| {
        matches!(request, ProgressiveTransportRequest::OutputEvent { event, .. }
            if matches!(event.as_ref(), ChannelOutputEvent::ToolCallCompleted { call_id, arguments, structured, .. }
                if call_id == "call-1"
                    && arguments.as_ref().is_some_and(|value| value["q"] == "safe test")
                    && structured.as_ref().is_some_and(|value| value["count"] == 1)))
    });
    assert!(
        forwarded,
        "the bounded worker forwards canonical events to the host bus"
    );
    replies
        .execute(ProgressiveCommand::Finish {
            handle: handle.clone(),
            text: "complete answer".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        callback.sends.lock().await.last().unwrap()["text"],
        "complete answer"
    );
    let error = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "late".into(),
                iteration: None,
            }],
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("closed"));
    replies.shutdown().await.unwrap();
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![],
        })
        .await
        .unwrap_err();
    assert!(result.to_string().contains("shutting down"));
}

#[tokio::test]
async fn invalid_targets_and_excess_event_batches_do_not_consume_reply_capacity() {
    let (replies, _) = fixture(false).await;
    for channel in [String::new(), "x".repeat(4097)] {
        assert!(
            replies
                .execute(ProgressiveCommand::Open { channel })
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid progressive target")
        );
    }
    assert!(replies.state.lock().await.entries.is_empty());
    let handle = open(&replies).await;
    let error = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![
                ChannelOutputEvent::TextDelta {
                    text: "x".into(),
                    iteration: None
                };
                33
            ],
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("too many progressive events"));
    assert_eq!(
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle,
                events: vec![]
            })
            .await
            .unwrap(),
        ProgressiveResult::Updated
    );
    assert_eq!(
        replies
            .execute(ProgressiveCommand::Close {
                handle: "absent".into()
            })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn scoped_open_rejects_missing_identity_invalid_leases_and_unconfigured_accounts() {
    let (replies, _) = fixture(false).await;
    let scoped = |channel_id: &str,
                  account_id: Option<&str>,
                  conversation_id: &str,
                  turn_id: &str,
                  lifetime_ms| ProgressiveCommand::OpenOutput {
        channel: tinychannels_bus::ChannelRef {
            id: channel_id.into(),
            account_id: account_id.map(str::to_owned),
        },
        conversation: tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: conversation_id.into(),
            ..Default::default()
        },
        turn_id: turn_id.into(),
        lifetime_ms,
    };

    for (request, expected) in [
        (
            scoped("", Some("discord:one"), "room", "turn", 1),
            "invalid scoped",
        ),
        (
            scoped("discord", Some("discord:one"), "", "turn", 1),
            "invalid scoped",
        ),
        (
            scoped("discord", Some("discord:one"), "room", "", 1),
            "invalid scoped",
        ),
        (
            scoped("discord", Some("discord:one"), "room", "turn", 0),
            "invalid scoped",
        ),
        (
            scoped("discord", Some("discord:missing"), "room", "turn", 10),
            "not configured",
        ),
    ] {
        let error = replies.execute(request).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
    let max_lease_is_representable = tokio::time::Instant::now()
        .checked_add(tokio::time::Duration::from_millis(u64::MAX))
        .is_some();
    let max_lease_error = replies
        .execute(scoped(
            "discord",
            Some("discord:one"),
            "room",
            "turn",
            u64::MAX,
        ))
        .await
        .unwrap_err();
    let expected = if max_lease_is_representable {
        "not configured"
    } else {
        "out of range"
    };
    assert!(max_lease_error.to_string().contains(expected));
    assert!(replies.state.lock().await.entries.is_empty());
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn thinking_limit_is_atomic_and_independent_of_text_budget() {
    let (replies, _) = fixture(false).await;
    let mut reply = ProgressiveReply::new(
        Arc::new(Sender {
            host: replies.host.clone(),
            context: None,
            capabilities: tinychannels_bus::ChannelCapabilities::unknown(),
            channel: "discord:fixture".into(),
            send_sequence: std::sync::atomic::AtomicU64::new(0),
        }),
        "discord:fixture",
        tinychannels_bus::ChannelCapabilities::unknown(),
    );
    apply(
        &mut reply,
        vec![ChannelOutputEvent::ThinkingDelta {
            text: "x".repeat(MAX_CONTENT),
            iteration: None,
        }],
        false,
    )
    .await
    .unwrap();
    assert!(
        apply(
            &mut reply,
            vec![
                ChannelOutputEvent::TextDelta {
                    text: "not committed".into(),
                    iteration: None,
                },
                ChannelOutputEvent::ThinkingDelta {
                    text: "extra".into(),
                    iteration: None
                }
            ],
            false,
        )
        .await
        .is_err()
    );
    assert!(reply.state().content.is_empty());
    assert_eq!(reply.state().thinking_accumulator.len(), MAX_CONTENT);
}

#[tokio::test]
async fn progressive_edits_and_deletes_preserve_target_and_message_id() {
    let (replies, callback) = fixture(false).await;
    let sender = Sender {
        host: replies.host.clone(),
        context: None,
        capabilities: tinychannels_bus::ChannelCapabilities::unknown(),
        channel: "discord:fixture".into(),
        send_sequence: std::sync::atomic::AtomicU64::new(0),
    };
    sender
        .edit("discord:room", "message-id", "updated")
        .await
        .unwrap();
    sender.delete("discord:room", "message-id").await.unwrap();
    let requests = callback.requests.lock().await;
    assert!(
        matches!(&requests[0], ProgressiveTransportRequest::Edit { channel, message_id, text, .. }
        if channel == "discord:room" && message_id == "message-id" && text == "updated")
    );
    assert!(
        matches!(&requests[1], ProgressiveTransportRequest::Delete { channel, message_id, .. }
        if channel == "discord:room" && message_id == "message-id")
    );
}

#[tokio::test]
async fn transport_failures_preserve_recovery_classification() {
    let (replies, callback) = fixture(false).await;
    let sender = Sender {
        host: replies.host.clone(),
        context: None,
        capabilities: tinychannels_bus::ChannelCapabilities::unknown(),
        channel: "discord:fixture".into(),
        send_sequence: std::sync::atomic::AtomicU64::new(0),
    };
    for result in [
        ProgressiveTransportResult::Unavailable,
        ProgressiveTransportResult::EditUnsupported,
        ProgressiveTransportResult::MessageGone,
        ProgressiveTransportResult::Failed,
    ] {
        *callback.send_result.lock().await = Some(result.clone());
        let error = sender
            .edit("discord:room", "message-id", "updated")
            .await
            .unwrap_err();
        match result {
            ProgressiveTransportResult::Acknowledged
            | ProgressiveTransportResult::UnsupportedContent => unreachable!(),
            ProgressiveTransportResult::Unavailable => {
                assert!(matches!(error, ProgressiveSendError::Unavailable))
            }
            ProgressiveTransportResult::EditUnsupported => {
                assert!(matches!(error, ProgressiveSendError::EditUnsupported))
            }
            ProgressiveTransportResult::MessageGone => {
                assert!(matches!(error, ProgressiveSendError::MessageGone))
            }
            ProgressiveTransportResult::Failed => {
                let ProgressiveSendError::Other(error) = error else {
                    panic!("expected opaque transport failure")
                };
                assert_eq!(error.to_string(), "progressive transport failed");
            }
            ProgressiveTransportResult::Success(_) => unreachable!(),
            ProgressiveTransportResult::Outcome(_) => unreachable!(),
        }
    }
}

#[tokio::test]
async fn output_callback_failure_cleans_up_without_sending_a_replacement() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    *callback.output_result.lock().await = Some(ProgressiveTransportResult::Failed);
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "already attempted".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch {
            outcomes,
            failure: Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ObservationCallback),
            ..
        } if outcomes.is_empty()
    ));
    assert!(callback.sends.lock().await.is_empty());
    assert!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .any(|request| matches!(request, ProgressiveTransportRequest::OutputEvent { .. }))
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_final_and_cancellation_events_end_the_worker() {
    let (replies, callback) = fixture(false).await;
    let final_handle = open(&replies).await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: final_handle.clone(),
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "canonical final".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    let late = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: final_handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "late".into(),
                iteration: Some(2),
            }],
        })
        .await
        .unwrap_err();
    assert!(late.to_string().contains("terminal"));
    let cancellation_handle = open(&replies).await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: cancellation_handle.clone(),
            events: vec![ChannelOutputEvent::Cancellation { reason: None }],
        })
        .await
        .unwrap();
    assert_eq!(callback.sends.lock().await.len(), 1);
    for handle in [final_handle, cancellation_handle] {
        replies
            .execute(ProgressiveCommand::Close { handle })
            .await
            .unwrap();
    }
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_final_delivery_returns_partial_receipt_without_retrying() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
    outcome.receipt.platform_message_ids = vec!["accepted-part-1".into()];
    outcome.error = Some(tinychannels_bus::channel::ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Transient,
        message: "second part failed".into(),
        retryable: true,
        sent_before_error: true,
        continuation_message_ids: vec![],
        ..Default::default()
    });
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::Outcome(Box::new(
        outcome.clone(),
    )));
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "caption".into(),
                parts: vec![tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: "caption".into(),
                        markdown_dialect: None,
                    },
                }],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch { outcomes, failure: Some(_), .. }
            if outcomes == [outcome]
    ));
    let sends = callback.sends.lock().await;
    assert_eq!(sends.len(), 1, "partial sends must not be retried");
    assert_eq!(sends[0]["text"], "caption");
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_edit_channel_sends_final_text_in_configured_segments() {
    let (replies, callback) = fixture(false).await;
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.text.text_limit = Some(4);
    capabilities.text.length_unit = tinychannels_bus::LengthUnit::Utf8Bytes;
    let channel = Arc::new(ConfiguredChannelWithCapabilities {
        name: "discord",
        capabilities,
    });
    let mut configured = HashMap::new();
    configured.insert(
        "discord:segmented".to_owned(),
        Running::start(
            channel,
            Arc::new(crate::provider::Hooks::default()),
            replies.host.clone(),
        ),
    );
    let replies = Replies::new(replies.host.clone(), Arc::new(Mutex::new(configured)));
    let handle = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:segmented".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "segmented-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap();
    let ProgressiveResult::Opened { handle } = handle else {
        panic!("expected opened output")
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "a😀éz".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    let sends = callback.sends.lock().await;
    assert_eq!(
        sends
            .iter()
            .map(|send| send["text"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["a", "😀", "éz"]
    );
    assert!(matches!(result, ProgressiveResult::DeliveryOutcome { .. }));
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn segmented_partial_send_retains_prior_receipts_and_stops_remaining_parts() {
    let (base, callback) = fixture(false).await;
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.text.text_limit = Some(4);
    let mut configured = HashMap::new();
    configured.insert(
        "discord:partial-segments".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities,
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    callback
        .send_queue
        .lock()
        .await
        .push_back(ProgressiveTransportResult::Outcome(Box::new(
            tinychannels_bus::channel::ChannelSendOutcome {
                receipt: tinychannels_bus::channel::MessageReceipt {
                    platform_message_ids: vec!["seg-1".into()],
                    ..Default::default()
                },
                error: None,
            },
        )));
    callback
        .send_queue
        .lock()
        .await
        .push_back(ProgressiveTransportResult::Outcome(Box::new(
            tinychannels_bus::channel::ChannelSendOutcome {
                receipt: tinychannels_bus::channel::MessageReceipt {
                    platform_message_ids: vec!["seg-2".into()],
                    ..Default::default()
                },
                error: Some(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Transient,
                    message: "partial segment".into(),
                    sent_before_error: true,
                    retryable: true,
                    ..Default::default()
                }),
            },
        )));
    let replies = Replies::new(base.host.clone(), Arc::new(Mutex::new(configured)));
    let opened = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:partial-segments".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "partial-segments".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap();
    let ProgressiveResult::Opened { handle } = opened else {
        panic!("expected open")
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "abcdefghij".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    let ProgressiveResult::DeliveryBatch {
        outcomes,
        failure: Some(_),
        ..
    } = result
    else {
        panic!("expected retained delivery outcome")
    };
    let outcome = &outcomes[0];
    assert_eq!(outcome.receipt.platform_message_ids, ["seg-1", "seg-2"]);
    assert!(outcome.error.is_some());
    assert_eq!(
        callback.sends.lock().await.len(),
        2,
        "segment 3 must not be attempted"
    );
    let typed: Vec<_> = callback
        .requests
        .lock()
        .await
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::SendTyped {
                sequence,
                segment_index,
                idempotency_key,
                ..
            } => Some((*sequence, *segment_index, idempotency_key.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(typed.len(), 2);
    assert_eq!(typed[0].1, Some(0));
    assert_eq!(typed[1].1, Some(1));
    assert_ne!(typed[0].2, typed[1].2);
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn terminal_text_edits_existing_draft_without_duplicate_send() {
    let (base, callback) = fixture(false).await;
    *callback.send_result.lock().await =
        Some(ProgressiveTransportResult::Success(serde_json::json!({
            "receipt": {
                "primaryPlatformMessageId": "fixture-message",
                "platformMessageIds": ["fixture-message"],
                "parts": [{
                    "platformMessageId": "fixture-message",
                    "editToken": "opaque-edit-token",
                    "deleteToken": "opaque-delete-token"
                }]
            }
        })));
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let mut configured = HashMap::new();
    configured.insert(
        "discord:final-draft".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities,
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), Arc::new(Mutex::new(configured)));
    let opened = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:final-draft".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "final-draft-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap();
    let ProgressiveResult::Opened { handle } = opened else {
        panic!("expected opened output")
    };
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "partial".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "complete".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    assert!(
        matches!(result, ProgressiveResult::DeliveryOutcome { outcome }
        if outcome.receipt.platform_message_ids == ["fixture-message"])
    );
    let sends = callback.sends.lock().await;
    assert_eq!(
        sends.len(),
        1,
        "the terminal result must edit the visible draft"
    );
    assert!(matches!(callback.requests.lock().await.last().unwrap(),
        ProgressiveTransportRequest::Edit { text, message_id, receipt: Some(receipt), .. }
            if text == "complete" && message_id == "fixture-message"
                && receipt.parts[0].edit_token.as_deref() == Some("opaque-edit-token")
    ));
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn failed_terminal_edit_retains_the_existing_draft_receipt() {
    let (base, callback) = fixture(false).await;
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let mut configured = HashMap::new();
    configured.insert(
        "discord:failed-final-edit".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities,
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), Arc::new(Mutex::new(configured)));
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:failed-final-edit".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "failed-final-edit-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened output")
    };
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "draft".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let edit_error = tinychannels_bus::channel::ChannelSendOutcome {
        error: Some(tinychannels_bus::channel::ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Transient,
            message: "edit failed".into(),
            sent_before_error: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    *callback.send_result.lock().await =
        Some(ProgressiveTransportResult::Outcome(Box::new(edit_error)));
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "final".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch { outcomes, failure: Some(_), .. }
            if outcomes.len() == 1
                && outcomes[0].receipt.platform_message_ids == ["fixture-message"]
                && outcomes[0].error.as_ref().is_some_and(|error| error.sent_before_error)
    ));
    assert_eq!(
        callback.sends.lock().await.len(),
        1,
        "failed edit cannot cause a blind replacement send"
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_media_outcome_is_retained_and_stops_mixed_terminal_batch() {
    let (base, callback) = fixture(false).await;
    let configured = Arc::new(Mutex::new(HashMap::new()));
    configured.lock().await.insert(
        "discord:partial-batch".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities: tinychannels_bus::ChannelCapabilities::default(),
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), configured);
    *callback.send_queue.lock().await = [ProgressiveTransportResult::Outcome(Box::new(
        tinychannels_bus::channel::ChannelSendOutcome {
            receipt: tinychannels_bus::channel::MessageReceipt {
                platform_message_ids: vec!["media-part-1".into()],
                ..Default::default()
            },
            error: Some(tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                message: "media tail uncertain".into(),
                sent_before_error: true,
                ..Default::default()
            }),
        },
    ))]
    .into();
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:partial-batch".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "partial-batch-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened output")
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: Some("caption".into()),
                    parts: vec![],
                },
                ChannelOutputEvent::FinalMessage {
                    text: "must not be sent after uncertainty".into(),
                    parts: vec![tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text: "must not be sent after uncertainty".into(),
                            markdown_dialect: None,
                        },
                    }],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();
    let outcomes = match result {
        ProgressiveResult::DeliveryOutcome { outcome } => vec![*outcome],
        ProgressiveResult::DeliveryOutcomes { outcomes } => outcomes,
        ProgressiveResult::DeliveryBatch { outcomes, .. } => outcomes,
        other => panic!("expected retained media outcome, got {other:?}"),
    };
    assert!(outcomes.iter().any(|outcome| {
        outcome.receipt.platform_message_ids == ["media-part-1"]
            && outcome
                .error
                .as_ref()
                .is_some_and(|error| error.sent_before_error)
    }));
    assert_eq!(callback.sends.lock().await.len(), 1);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn media_without_receipt_is_unknown_and_stops_terminal_batch() {
    let (base, callback) = fixture(false).await;
    *callback.send_queue.lock().await =
        [ProgressiveTransportResult::Outcome(Box::default())].into();
    let configured = Arc::new(Mutex::new(HashMap::new()));
    configured.lock().await.insert(
        "discord:no-receipt".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities: tinychannels_bus::ChannelCapabilities::default(),
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), configured);
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:no-receipt".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "no-receipt-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened output")
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![],
                },
                ChannelOutputEvent::FinalMessage {
                    text: "must not send".into(),
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch { outcomes, failure: Some(_), .. }
            if outcomes.len() == 1 && outcomes[0].error.as_ref().is_some_and(|error| error.sent_before_error)
    ));
    assert_eq!(callback.sends.lock().await.len(), 1);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_retains_prior_media_receipt() {
    let (base, callback) = fixture(false).await;
    let configured = Arc::new(Mutex::new(HashMap::new()));
    configured.lock().await.insert(
        "discord:media-cancel".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities: tinychannels_bus::ChannelCapabilities::default(),
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), configured);
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:media-cancel".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "media-cancel-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened output")
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![],
                },
                ChannelOutputEvent::Cancellation { reason: None },
            ],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryOutcome { outcome }
            if outcome.receipt.platform_message_ids == ["fixture-message"]
    ));
    assert_eq!(callback.sends.lock().await.len(), 1);

    *callback.send_queue.lock().await = [ProgressiveTransportResult::Outcome(Box::new(
        tinychannels_bus::channel::ChannelSendOutcome {
            receipt: tinychannels_bus::channel::MessageReceipt {
                platform_message_ids: vec!["partial-before-cancel".into()],
                ..Default::default()
            },
            error: Some(tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                message: "partial media delivery".into(),
                sent_before_error: true,
                ..Default::default()
            }),
        },
    ))]
    .into();
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:media-cancel".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "partial-media-cancel-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened output")
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![],
                },
                ChannelOutputEvent::Cancellation { reason: None },
            ],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch { outcomes, failure: Some(_), .. }
            if outcomes.len() == 1
                && outcomes[0].receipt.platform_message_ids == ["partial-before-cancel"]
                && outcomes[0].error.as_ref().is_some_and(|error| error.sent_before_error)
    ));
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_callback_error_after_acceptance_is_unknown_and_stops_batch() {
    let (base, callback) = fixture(false).await;
    *callback.rpc_error_after_send.lock().await = true;
    let configured = Arc::new(Mutex::new(HashMap::new()));
    configured.lock().await.insert(
        "discord:typed-unknown".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities: tinychannels_bus::ChannelCapabilities::default(),
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), configured);
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:typed-unknown".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "typed-unknown-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened output")
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![],
                },
                ChannelOutputEvent::FinalMessage {
                    text: "must not send after unknown callback result".into(),
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch { outcomes, failure: Some(_), .. }
            if outcomes.len() == 1 && outcomes[0].error.as_ref().is_some_and(|error| error.sent_before_error)
    ));
    let sends = callback
        .requests
        .lock()
        .await
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
        .count();
    assert_eq!(sends, 1);
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn thinking_multipart_receipt_drives_per_part_token_cleanup() {
    let (replies, callback) = fixture(false).await;
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::Success(
        serde_json::json!({
            "receipt": {
                "primaryPlatformMessageId": "thinking-1",
                "platformMessageIds": ["thinking-1", "thinking-2"],
                "parts": [
                    {"platformMessageId": "thinking-1", "editToken": "edit-1", "deleteToken": "delete-1"},
                    {"platformMessageId": "thinking-2", "editToken": "edit-2", "deleteToken": "delete-2"}
                ]
            }
        }),
    ));
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let handle = open_scoped(
        &replies,
        "discord:thinking-receipt",
        "thinking-receipt-turn",
        capabilities,
    )
    .await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::ThinkingDelta {
                text: "reasoning".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let closed = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::Cancellation { reason: None }],
        })
        .await
        .unwrap();
    assert_eq!(closed, ProgressiveResult::Closed);
    let requests = callback.requests.lock().await;
    let deletes: Vec<_> = requests
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::Delete {
                receipt,
                message_id,
                ..
            } => Some((message_id.as_str(), receipt.as_ref()?)),
            _ => None,
        })
        .collect();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0].0, "thinking-1");
    assert_eq!(
        deletes[0].1.parts[0].delete_token.as_deref(),
        Some("delete-1")
    );
    assert_eq!(deletes[1].0, "thinking-2");
    assert_eq!(
        deletes[1].1.parts[0].delete_token.as_deref(),
        Some("delete-2")
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn filler_callback_error_after_acceptance_is_not_retried() {
    let (replies, callback) = fixture(false).await;
    *callback.rpc_error_after_send.lock().await = true;
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let mut reply = ProgressiveReply::new(
        Arc::new(Sender {
            host: replies.host.clone(),
            context: Some(ProgressiveOutputContext {
                turn_id: "filler-unknown-turn".into(),
                channel: tinychannels_bus::ChannelRef {
                    id: "discord".into(),
                    account_id: Some("discord:filler-unknown".into()),
                },
                conversation: tinychannels_bus::ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Dm,
                    id: "room".into(),
                    ..Default::default()
                },
            }),
            capabilities: tinychannels_bus::ChannelCapabilities::unknown(),
            channel: "discord:filler-unknown".into(),
            send_sequence: std::sync::atomic::AtomicU64::new(0),
        }),
        "discord:filler-unknown",
        capabilities,
    );
    reply.filler_tick().await;
    reply.filler_tick().await;
    reply.filler_tick().await;
    let sends = callback
        .requests
        .lock()
        .await
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::Send { .. }))
        .count();
    assert_eq!(sends, 1, "accepted-then-error filler must not be retried");
    assert!(reply.state().filler_delivery_uncertain);
    assert!(reply.state().filler_disabled);
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn filler_multipart_receipt_drives_per_part_token_cleanup() {
    let (replies, callback) = fixture(false).await;
    *callback.send_result.lock().await =
        Some(ProgressiveTransportResult::Success(serde_json::json!({
            "receipt": {
                "primaryPlatformMessageId": "filler-1",
                "platformMessageIds": ["filler-1", "filler-2"],
                "parts": [
                    {"platformMessageId": "filler-1", "deleteToken": "filler-delete-1"},
                    {"platformMessageId": "filler-2", "deleteToken": "filler-delete-2"}
                ]
            }
        })));
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let handle = open_scoped(
        &replies,
        "discord:filler-receipt",
        "filler-receipt-turn",
        capabilities,
    )
    .await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(13)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let closed = replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    assert_eq!(closed, ProgressiveResult::Closed);
    let requests = callback.requests.lock().await;
    let deletes: Vec<_> = requests
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::Delete {
                receipt,
                message_id,
                ..
            } => Some((message_id.as_str(), receipt.as_ref()?)),
            _ => None,
        })
        .collect();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0].0, "filler-1");
    assert_eq!(
        deletes[0].1.parts[0].delete_token.as_deref(),
        Some("filler-delete-1")
    );
    assert_eq!(deletes[1].0, "filler-2");
    assert_eq!(
        deletes[1].1.parts[0].delete_token.as_deref(),
        Some("filler-delete-2")
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn scoped_output_survives_more_than_two_hundred_seconds_until_host_lease() {
    let (replies, callback) = fixture(false).await;
    let handle = open_scoped(
        &replies,
        "discord:long-turn",
        "long-turn",
        tinychannels_bus::ChannelCapabilities::default(),
    )
    .await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(201)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "long turn complete".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        ProgressiveResult::DeliveryOutcome { outcome }
            if outcome.receipt.platform_message_ids == ["fixture-message"]
    ));
    assert_eq!(callback.sends.lock().await.len(), 1);
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn scoped_voice_finalization_obeys_turn_lease_while_synthesis_is_blocked() {
    let (replies, callback) = fixture(false).await;
    let replies = Arc::new(replies);
    *callback.voice_policy.lock().await = Some(tinychannels_bus::HostVoiceReplyPolicy {
        mode: tinychannels_bus::HostVoiceReplyMode::Always,
        inbound_has_voice: false,
        explicitly_requested: false,
    });
    callback.block_synthesis.store(true, Ordering::SeqCst);
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.voice_out.supported = true;
    capabilities.voice_out.synthesis = true;
    capabilities.voice_out.accepted_codecs = vec!["ogg_opus".into()];
    replies.running.lock().await.insert(
        "discord:voice-lease".into(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities,
            }),
            Arc::new(crate::provider::Hooks::default()),
            replies.host.clone(),
        ),
    );
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:voice-lease".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "voice-lease-room".into(),
                ..Default::default()
            },
            turn_id: "blocked-voice-turn".into(),
            lifetime_ms: 1100,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened scoped output")
    };
    // Keep the host synthesis callback pending so finalization must observe
    // the short output lease itself.
    let feed_replies = replies.clone();
    let replay_handle = handle.clone();
    let terminal_events = vec![ChannelOutputEvent::FinalMessage {
        text: "spoken words".into(),
        parts: vec![],
        receipt: None,
    }];
    let feed_events = terminal_events.clone();
    let feed = tokio::spawn(async move {
        feed_replies
            .execute(ProgressiveCommand::FeedOutput {
                handle,
                events: feed_events,
            })
            .await
    });
    callback.entered.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        feed.is_finished(),
        "expired finalization must stop waiting on host synthesis"
    );
    let result = feed.await.expect("feed task joins");
    assert!(
        result.is_err(),
        "expired voice turn must not report successful delivery"
    );
    assert!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .all(|request| !matches!(request, ProgressiveTransportRequest::SendTyped { .. })),
        "a late synthesis result must not reach provider transport"
    );
    callback.released.notify_one();
    tokio::task::yield_now().await;
    let replay = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: replay_handle,
            events: terminal_events,
        })
        .await;
    assert!(
        replay.is_err(),
        "identical terminal replay remains fenced after expiry"
    );
    assert_eq!(
        callback.synthesis_calls.load(Ordering::SeqCst),
        1,
        "terminal replay must not start another synthesis callback"
    );
    assert!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .all(|request| !matches!(request, ProgressiveTransportRequest::SendTyped { .. })),
        "released late synthesis must not reach provider transport"
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn scoped_output_expiry_cleans_an_existing_draft_without_sending_a_reply() {
    let (replies, callback) = fixture(false).await;
    let account = "discord:short-lease";
    replies.running.lock().await.insert(
        account.into(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities: tinychannels_bus::ChannelCapabilities {
                    progressive_edits: true,
                    ..Default::default()
                },
            }),
            Arc::new(crate::provider::Hooks::default()),
            replies.host.clone(),
        ),
    );
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some(account.into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "lease-room".into(),
                ..Default::default()
            },
            turn_id: "short-lease-turn".into(),
            lifetime_ms: 1100,
        })
        .await
        .unwrap()
    else {
        panic!("expected scoped output handle")
    };
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "visible draft".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    assert!(callback.requests.lock().await.iter().any(|request| matches!(request,
        ProgressiveTransportRequest::Send { message, .. } if message["text"] == "visible draft"
    )));

    tokio::time::advance(Duration::from_millis(150)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let late = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "too late".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap_err();
    assert!(late.to_string().contains("closed"));
    let requests = callback.requests.lock().await;
    assert!(requests.iter().any(|request| matches!(request,
        ProgressiveTransportRequest::Delete { message_id, .. } if message_id == "fixture-message"
    )));
    assert_eq!(
        callback.sends.lock().await.len(),
        1,
        "lease cleanup only deletes the draft and does not send a replacement"
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_approval_and_clarification_events_deliver_prompts() {
    let (base, callback) = fixture(false).await;
    let configured = Arc::new(Mutex::new(HashMap::new()));
    configured.lock().await.insert(
        "discord:prompts".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities: tinychannels_bus::ChannelCapabilities::default(),
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), configured);
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:prompts".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "prompt-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    else {
        panic!("expected opened output")
    };
    for event in [
        ChannelOutputEvent::ApprovalRequest {
            id: "approval-7".into(),
            prompt: "May I run the command?".into(),
            choices: vec!["Allow once".into(), "Deny".into()],
        },
        ChannelOutputEvent::Clarification {
            prompt: "Which folder should I use?".into(),
        },
    ] {
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![event],
            })
            .await
            .unwrap();
    }
    let requests = callback.requests.lock().await;
    let delivered = requests
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::SendTyped { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        delivered
            .iter()
            .any(|text| text.contains("May I run the command?"))
    );
    assert!(
        delivered
            .iter()
            .any(|text| text.contains("Allow once, Deny"))
    );
    assert!(
        delivered
            .iter()
            .any(|text| text.contains("Which folder should I use?"))
    );
    assert!(requests.iter().any(|request| matches!(request,
        ProgressiveTransportRequest::OutputEvent { event, .. }
            if matches!(event.as_ref(), ChannelOutputEvent::ApprovalRequest { id, .. } if id == "approval-7")
    )));
    drop(requests);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn later_provider_rpc_failure_retains_prior_receipt_and_stops_terminal_batch() {
    let (replies, callback) = fixture(false).await;
    let handle = open_scoped(
        &replies,
        "discord:provider-error",
        "provider-error-turn",
        tinychannels_bus::ChannelCapabilities::default(),
    )
    .await;
    let mut accepted = tinychannels_bus::channel::ChannelSendOutcome::default();
    accepted.receipt.platform_message_ids = vec!["first-media".into()];
    *callback.send_queue.lock().await = [
        ProgressiveTransportResult::Outcome(Box::new(accepted)),
        ProgressiveTransportResult::Failed,
    ]
    .into();
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![tinychannels_bus::ContentPart::Image {
                        media: tinychannels_bus::MediaReference {
                            kind: tinychannels_bus::MediaKind::Image,
                            url: Some("https://media.invalid/one.png".into()),
                            ..Default::default()
                        },
                        caption: None,
                        alt_text: None,
                    }],
                },
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![tinychannels_bus::ContentPart::Image {
                        media: tinychannels_bus::MediaReference {
                            kind: tinychannels_bus::MediaKind::Image,
                            url: Some("https://media.invalid/two.png".into()),
                            ..Default::default()
                        },
                        caption: None,
                        alt_text: None,
                    }],
                },
                ChannelOutputEvent::FinalMessage {
                    text: "do not send after transport error".into(),
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch {
            outcomes,
            failure: Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport),
            ..
        } if outcomes.len() == 2
            && outcomes[0].receipt.platform_message_ids == ["first-media"]
            && outcomes[1].error.as_ref().is_some_and(|error| error.sent_before_error)
    ));
    assert_eq!(callback.sends.lock().await.len(), 2);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn later_observation_failure_retains_prior_receipt_and_stops_terminal_batch() {
    let (replies, callback) = fixture(false).await;
    let handle = open_scoped(
        &replies,
        "discord:observation-error",
        "observation-error-turn",
        tinychannels_bus::ChannelCapabilities::default(),
    )
    .await;
    let mut accepted = tinychannels_bus::channel::ChannelSendOutcome::default();
    accepted.receipt.platform_message_ids = vec!["observed-media".into()];
    *callback.send_queue.lock().await =
        [ProgressiveTransportResult::Outcome(Box::new(accepted))].into();
    *callback.output_queue.lock().await = [
        ProgressiveTransportResult::Acknowledged,
        ProgressiveTransportResult::Failed,
    ]
    .into();
    let media = || ChannelOutputEvent::Media {
        text: None,
        parts: vec![tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                kind: tinychannels_bus::MediaKind::Image,
                url: Some("https://media.invalid/one.png".into()),
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        }],
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                media(),
                ChannelOutputEvent::FinalMessage {
                    text: "must not reach observation or provider".into(),
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();
    assert!(matches!(result,
        ProgressiveResult::DeliveryBatch {
            outcomes,
            failure: Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ObservationCallback),
            ..
        } if outcomes.len() == 1 && outcomes[0].receipt.platform_message_ids == ["observed-media"]
    ));
    assert_eq!(callback.sends.lock().await.len(), 1);
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn callback_error_after_accepting_draft_is_not_retried_or_resent() {
    let (replies, callback) = fixture(false).await;
    *callback.rpc_error_after_send.lock().await = true;
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let handle = open_scoped(
        &replies,
        "discord:ambiguous-draft",
        "ambiguous-draft-turn",
        capabilities,
    )
    .await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "draft text".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
    }
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "final text".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    let outcomes = match result {
        ProgressiveResult::DeliveryOutcome { outcome } => vec![*outcome],
        ProgressiveResult::DeliveryOutcomes { outcomes }
        | ProgressiveResult::DeliveryBatch { outcomes, .. } => outcomes,
        other => panic!("expected retained uncertain draft result, got {other:?}"),
    };
    assert!(outcomes.iter().any(|outcome| {
        outcome
            .error
            .as_ref()
            .is_some_and(|error| error.sent_before_error)
    }));
    let requests = callback.requests.lock().await;
    let sends = requests
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::Send {
                idempotency_key, ..
            }
            | ProgressiveTransportRequest::SendTyped {
                idempotency_key, ..
            } => Some(idempotency_key),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(sends.len(), 1);
    assert!(sends[0].as_deref().is_some_and(|key| !key.is_empty()));
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn scoped_draft_keys_differ_for_identical_text_across_turns_and_accounts() {
    let (replies, callback) = fixture(false).await;
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    for (account, turn) in [("discord:key-a", "turn-a"), ("discord:key-b", "turn-b")] {
        let handle = open_scoped(&replies, account, turn, capabilities.clone()).await;
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![ChannelOutputEvent::TextDelta {
                    text: "same draft text".into(),
                    iteration: Some(1),
                }],
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        replies
            .execute(ProgressiveCommand::Close { handle })
            .await
            .unwrap();
    }
    let requests = callback.requests.lock().await;
    let keys = requests
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::Send {
                idempotency_key, ..
            }
            | ProgressiveTransportRequest::SendTyped {
                idempotency_key, ..
            } => idempotency_key.as_deref(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
}

#[tokio::test(start_paused = true)]
async fn configured_progressive_channel_posts_and_edits_one_draft() {
    let (base, callback) = fixture(false).await;
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let mut configured = HashMap::new();
    configured.insert(
        "discord:editable".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities,
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    configured.insert(
        "discord:editable-b".to_owned(),
        Running::start(
            Arc::new(ConfiguredChannelWithCapabilities {
                name: "discord",
                capabilities: tinychannels_bus::ChannelCapabilities {
                    progressive_edits: true,
                    ..Default::default()
                },
            }),
            Arc::new(crate::provider::Hooks::default()),
            base.host.clone(),
        ),
    );
    let replies = Replies::new(base.host.clone(), Arc::new(Mutex::new(configured)));
    let opened = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:editable".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room".into(),
                ..Default::default()
            },
            turn_id: "editable-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap();
    let ProgressiveResult::Opened { handle } = opened else {
        panic!("expected opened output")
    };
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "hello".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let requests = callback.requests.lock().await;
    assert!(requests.iter().any(|request| matches!(request,
        ProgressiveTransportRequest::Send { message, .. }
            if message["text"] == "hello"
    )));
    drop(requests);
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::EditUnsupported);
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "!".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let requests = callback.requests.lock().await;
    assert!(requests.iter().any(|request| matches!(request,
        ProgressiveTransportRequest::Edit { text, message_id, .. }
            if text == "hello!" && message_id == "fixture-message"
    )));
    drop(requests);
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::Cancellation { reason: None }],
        })
        .await
        .unwrap();
    let before = callback
        .requests
        .lock()
        .await
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::Typing { .. }))
        .count();
    tokio::time::advance(Duration::from_secs(12)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let after = callback
        .requests
        .lock()
        .await
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::Typing { .. }))
        .count();
    assert_eq!(after, before, "terminal cancel must stop typing refresh");

    *callback.send_result.lock().await = None;
    let opened_b = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: tinychannels_bus::ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:editable-b".into()),
            },
            conversation: tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: "room-b".into(),
                ..Default::default()
            },
            turn_id: "editable-turn-b".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap();
    let ProgressiveResult::Opened { handle: handle_b } = opened_b else {
        panic!("expected second configured output")
    };
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle_b.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "account b".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle_b.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "!".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    assert!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .any(|request| matches!(request,
                ProgressiveTransportRequest::Edit { turn_id: Some(turn_id), text, .. }
                    if turn_id == "editable-turn-b" && text == "account b!"
            )),
        "account A's unsupported edit must not disable account B"
    );
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle_b,
            events: vec![ChannelOutputEvent::Cancellation { reason: None }],
        })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn module_outcome_receipts_keep_all_draft_edit_and_close_tokens() {
    let (replies, callback) = fixture(false).await;
    let capabilities = tinychannels_bus::ChannelCapabilities {
        progressive_edits: true,
        ..Default::default()
    };
    let receipt = tinychannels_bus::channel::MessageReceipt {
        idempotency_key: Some("draft-op-1".into()),
        channel: Some(tinychannels_bus::ChannelRef {
            id: "discord".into(),
            account_id: Some("discord:receipt-account".into()),
        }),
        conversation: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Thread,
            id: "room".into(),
            thread_id: Some("thread-7".into()),
            ..Default::default()
        }),
        primary_platform_message_id: Some("draft-a".into()),
        platform_message_ids: vec!["draft-a".into()],
        parts: vec![tinychannels_bus::channel::MessageReceiptPart {
            platform_message_id: "draft-a".into(),
            kind: tinychannels_bus::channel::MessageReceiptPartKind::Text,
            index: 0,
            edit_token: Some("edit-a".into()),
            delete_token: Some("delete-a".into()),
            ..Default::default()
        }],
        thread_id: Some("thread-7".into()),
        reply_to_id: Some("reply-1".into()),
        ..Default::default()
    };
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::Outcome(Box::new(
        tinychannels_bus::channel::ChannelSendOutcome {
            receipt: receipt.clone(),
            ..Default::default()
        },
    )));

    let handle = open_scoped(
        &replies,
        "discord:receipt-account",
        "receipt-turn",
        capabilities,
    )
    .await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![
                ChannelOutputEvent::ThinkingDelta {
                    text: "thinking".into(),
                    iteration: Some(1),
                },
                ChannelOutputEvent::TextDelta {
                    text: "draft".into(),
                    iteration: Some(1),
                },
            ],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: " updated".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }

    let edits = callback
        .requests
        .lock()
        .await
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::Edit { receipt, .. } => receipt.as_ref(),
            _ => None,
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(edits.len(), 1, "the editable draft is updated in place");
    assert!(edits.iter().all(|actual| actual == &receipt));

    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    let requests = callback.requests.lock().await;
    let deletes = requests
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::Delete { receipt, .. } => receipt.as_ref(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        deletes.len(),
        2,
        "draft and thinking bubbles are deleted on close"
    );
    assert!(deletes.iter().all(|actual| *actual == &receipt));
    replies.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn module_outcome_multipart_draft_close_deletes_every_provider_part() {
    let (replies, callback) = fixture(false).await;
    let parts = vec![
        tinychannels_bus::channel::MessageReceiptPart {
            platform_message_id: "draft-a".into(),
            kind: tinychannels_bus::channel::MessageReceiptPartKind::Text,
            index: 0,
            edit_token: Some("edit-a".into()),
            delete_token: Some("delete-a".into()),
            ..Default::default()
        },
        tinychannels_bus::channel::MessageReceiptPart {
            platform_message_id: "draft-b".into(),
            kind: tinychannels_bus::channel::MessageReceiptPartKind::Text,
            index: 1,
            edit_token: Some("edit-b".into()),
            delete_token: Some("delete-b".into()),
            ..Default::default()
        },
    ];
    let receipt = tinychannels_bus::channel::MessageReceipt {
        idempotency_key: Some("multipart-draft".into()),
        primary_platform_message_id: Some("draft-a".into()),
        platform_message_ids: vec!["draft-a".into(), "draft-b".into()],
        parts: parts.clone(),
        ..Default::default()
    };
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::Outcome(Box::new(
        tinychannels_bus::channel::ChannelSendOutcome {
            receipt: receipt.clone(),
            ..Default::default()
        },
    )));
    let handle = open_scoped(
        &replies,
        "discord:multipart-account",
        "multipart-turn",
        tinychannels_bus::ChannelCapabilities {
            progressive_edits: true,
            ..Default::default()
        },
    )
    .await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "draft".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();

    let requests = callback.requests.lock().await;
    let deletes = requests
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::Delete { receipt, .. } => receipt.as_ref(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(deletes.len(), 2);
    for part in parts {
        assert!(deletes.iter().any(|actual| {
            actual.platform_message_ids == [part.platform_message_id.as_str()]
                && actual.parts == [part.clone()]
                && actual.idempotency_key.as_deref() == Some("multipart-draft")
        }));
    }
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn open_output_uses_configured_account_and_preserves_full_conversation_scope() {
    use tinychannels_bus::{ChannelRef, ConversationKind, ConversationRef};

    let (replies, callback) = fixture(false).await;
    let mut configured = HashMap::new();
    for account in ["discord:account-a", "discord:account-b"] {
        let channel = Arc::new(ConfiguredChannel("discord"));
        configured.insert(
            account.to_owned(),
            Running::start(
                channel,
                Arc::new(crate::provider::Hooks::default()),
                replies.host.clone(),
            ),
        );
    }
    let replies = Replies::new(replies.host.clone(), Arc::new(Mutex::new(configured)));
    let channel_ref = ChannelRef {
        id: "discord".into(),
        account_id: Some("discord:account-b".into()),
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Thread,
        id: "room-1".into(),
        scope_id: Some("guild-7".into()),
        parent_id: Some("room-1".into()),
        thread_id: Some("thread-8".into()),
        topic_id: Some("topic-9".into()),
    };
    let forged = replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: ChannelRef {
                id: "discord".into(),
                account_id: Some("discord:forged".into()),
            },
            conversation: conversation.clone(),
            turn_id: "forged-turn".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap_err();
    assert!(forged.to_string().contains("not configured"));
    let handle = match replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: channel_ref.clone(),
            conversation: conversation.clone(),
            turn_id: "turn-topic-9".into(),

            lifetime_ms: 210_000,
        })
        .await
        .unwrap()
    {
        ProgressiveResult::Opened { handle } => handle,
        _ => panic!("expected scoped open"),
    };
    let outcome = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "scoped".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();
    assert!(matches!(outcome, ProgressiveResult::DeliveryOutcome { .. }));
    let requests = callback.requests.lock().await;
    let ProgressiveTransportRequest::SendTyped {
        turn_id,
        channel_ref: sent_channel,
        conversation: sent_conversation,
        sequence,
        idempotency_key,
        ..
    } = requests.last().unwrap()
    else {
        panic!("expected typed provider send")
    };
    assert_eq!(turn_id.as_deref(), Some("turn-topic-9"));
    assert_eq!(sent_channel.as_ref(), Some(&channel_ref));
    assert_eq!(sent_conversation.as_ref(), Some(&conversation));
    assert_eq!(*sequence, Some(1));
    assert_eq!(idempotency_key.as_ref().map(String::len), Some(64));
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_rich_final_returns_the_unflattened_parts() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::UnsupportedContent);
    let parts = vec![tinychannels_bus::OutboundContentPart::Voice {
        voice: tinychannels_bus::OutboundVoice::Media {
            media: tinychannels_bus::MediaReference {
                url: Some("https://media.invalid/voice.ogg".into()),
                content_type: Some("audio/ogg".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                duration_ms: Some(1200),
                ..Default::default()
            },
            codec: None,
            duration_ms: Some(1200),
            audio_base64: None,
        },
    }];
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "voice note".into(),
                parts: parts.clone(),
                receipt: None,
            }],
        })
        .await
        .unwrap();
    assert_eq!(
        result,
        ProgressiveResult::DeliveryUnsupported {
            parts: parts.clone()
        }
    );
    assert_eq!(callback.sends.lock().await.len(), 1);
    let requests = callback.requests.lock().await;
    let ProgressiveTransportRequest::SendTyped {
        parts: sent_parts, ..
    } = requests
        .iter()
        .find(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
        .unwrap()
    else {
        panic!("expected typed provider send")
    };
    assert_eq!(sent_parts, &parts);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_media_is_retained_with_later_final_receipt_in_same_batch() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let unsupported = tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                kind: tinychannels_bus::MediaKind::Image,
                url: Some("https://media.invalid/unsupported.png".into()),
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    };
    let mut final_outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
    final_outcome.receipt.platform_message_ids = vec!["final-receipt".into()];
    callback.send_queue.lock().await.extend([
        ProgressiveTransportResult::UnsupportedContent,
        ProgressiveTransportResult::Outcome(Box::new(final_outcome.clone())),
    ]);

    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![match unsupported.clone() {
                        tinychannels_bus::OutboundContentPart::Content { part } => part,
                        _ => unreachable!(),
                    }],
                },
                ChannelOutputEvent::FinalMessage {
                    text: "delivered final".into(),
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();

    assert_eq!(
        result,
        ProgressiveResult::DeliveryBatch {
            outcomes: vec![final_outcome],
            unsupported: vec![unsupported],
            failure: None,
        }
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_and_successful_media_facts_survive_later_provider_failure() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let unsupported_part = tinychannels_bus::ContentPart::Image {
        media: tinychannels_bus::MediaReference {
            kind: tinychannels_bus::MediaKind::Image,
            url: Some("https://media.invalid/first.png".into()),
            ..Default::default()
        },
        caption: None,
        alt_text: None,
    };
    let accepted = tinychannels_bus::channel::ChannelSendOutcome {
        receipt: tinychannels_bus::channel::MessageReceipt {
            platform_message_ids: vec!["accepted-media".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let failed = tinychannels_bus::channel::ChannelSendOutcome {
        error: Some(tinychannels_bus::channel::ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Unknown,
            message: "provider failed after dispatch".into(),
            sent_before_error: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    callback.send_queue.lock().await.extend([
        ProgressiveTransportResult::UnsupportedContent,
        ProgressiveTransportResult::Outcome(Box::new(accepted.clone())),
        ProgressiveTransportResult::Outcome(Box::new(failed.clone())),
    ]);

    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![unsupported_part.clone()],
                },
                ChannelOutputEvent::Media {
                    text: Some("accepted caption".into()),
                    parts: vec![],
                },
                ChannelOutputEvent::Media {
                    text: None,
                    parts: vec![tinychannels_bus::ContentPart::Image {
                        media: tinychannels_bus::MediaReference {
                            kind: tinychannels_bus::MediaKind::Image,
                            url: Some("https://media.invalid/third.png".into()),
                            ..Default::default()
                        },
                        caption: None,
                        alt_text: None,
                    }],
                },
                ChannelOutputEvent::FinalMessage {
                    text: "must not retry after provider failure".into(),
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await
        .unwrap();

    assert_eq!(
        result,
        ProgressiveResult::DeliveryBatch {
            outcomes: vec![accepted, failed],
            unsupported: vec![tinychannels_bus::OutboundContentPart::Content {
                part: unsupported_part,
            }],
            failure: Some(
                tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport,
            ),
        }
    );
    let sent = callback
        .requests
        .lock()
        .await
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
        .count();
    assert_eq!(
        sent, 3,
        "the failed send is not repeated or followed by final"
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn media_and_voice_events_use_ordered_typed_provider_sends() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let voice = tinychannels_bus::VoiceNote {
        media: tinychannels_bus::MediaReference {
            kind: tinychannels_bus::MediaKind::Audio,
            url: Some("https://media.invalid/voice.ogg".into()),
            content_type: Some("audio/ogg".into()),
            ..Default::default()
        },
        codec: Some("opus".into()),
        duration_ms: Some(800),
        transcript: Some(tinychannels_bus::VoiceTranscript {
            text: "spoken words".into(),
            language: None,
            state: tinychannels_bus::VoiceTranscriptState::Complete,
            source: tinychannels_bus::VoiceTranscriptSource::Provider,
        }),
        ..Default::default()
    };
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![
                ChannelOutputEvent::Media {
                    text: Some("image caption".into()),
                    parts: vec![tinychannels_bus::ContentPart::Image {
                        media: tinychannels_bus::MediaReference {
                            kind: tinychannels_bus::MediaKind::Image,
                            url: Some("https://media.invalid/image.png".into()),
                            ..Default::default()
                        },
                        caption: None,
                        alt_text: Some("image".into()),
                    }],
                },
                ChannelOutputEvent::VoiceNote { text: None, voice },
            ],
        })
        .await
        .unwrap();
    let ProgressiveResult::DeliveryOutcomes { outcomes } = result else {
        panic!("expected both provider receipts")
    };
    assert_eq!(outcomes.len(), 2);
    assert_eq!(
        outcomes[0].receipt.platform_message_ids,
        ["fixture-message"]
    );
    let sends = callback.requests.lock().await;
    let typed = sends
        .iter()
        .filter_map(|request| match request {
            ProgressiveTransportRequest::SendTyped { parts, .. } => Some(parts),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(typed.len(), 2);
    assert!(
        matches!(&typed[0][0], tinychannels_bus::OutboundContentPart::Content { part: tinychannels_bus::ContentPart::Text { text, .. } } if text == "image caption")
    );
    assert!(matches!(
        &typed[0][1],
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image { .. }
        }
    ));
    assert!(
        matches!(&typed[1][0], tinychannels_bus::OutboundContentPart::Content { part: tinychannels_bus::ContentPart::Text { text, .. } } if text == "spoken words")
    );
    assert!(
        matches!(&typed[1][1], tinychannels_bus::OutboundContentPart::Voice { voice: tinychannels_bus::OutboundVoice::Media { codec: Some(codec), .. } } if codec == "opus")
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_callback_acknowledgement_and_unavailability_keep_distinct_failure_facts() {
    let (replies, callback) = fixture(false).await;
    let events = vec![ChannelOutputEvent::Media {
        text: None,
        parts: vec![tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                kind: tinychannels_bus::MediaKind::Image,
                url: Some("https://media.invalid/pending.png".into()),
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        }],
    }];

    for (provider_result, sent_before_error) in [
        (ProgressiveTransportResult::Unavailable, false),
        (ProgressiveTransportResult::Acknowledged, true),
        (ProgressiveTransportResult::Failed, true),
    ] {
        let handle = open(&replies).await;
        callback.send_queue.lock().await.push_back(provider_result);
        let result = replies
            .execute(ProgressiveCommand::FeedOutput {
                handle,
                events: events.clone(),
            })
            .await
            .unwrap();
        let ProgressiveResult::DeliveryBatch {
            outcomes,
            unsupported,
            failure:
                Some(tinychannels_bus::module_progressive::ProgressiveOutputFailure::ProviderTransport),
        } = result
        else {
            panic!("typed callback failures must remain visible in the batch result")
        };
        assert!(unsupported.is_empty());
        assert_eq!(outcomes.len(), 1);
        let error = outcomes[0].error.as_ref().expect("visible send failure");
        assert_eq!(error.sent_before_error, sent_before_error);
        assert_eq!(
            error.kind,
            tinychannels_bus::channel::SendErrorKind::Unknown
        );
    }
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn ambiguous_voice_receipt_is_retained_and_identical_final_is_not_retried() {
    let (replies, callback) = fixture(false).await;
    let handle = open_scoped_provider(
        &replies,
        "discord",
        "discord:ambiguous-voice",
        "ambiguous-voice-turn",
        tinychannels_bus::ChannelCapabilities::default(),
    )
    .await;
    callback
        .send_queue
        .lock()
        .await
        .push_back(ProgressiveTransportResult::Outcome(Box::new(
            tinychannels_bus::channel::ChannelSendOutcome {
                receipt: tinychannels_bus::channel::MessageReceipt {
                    platform_message_ids: vec!["accepted-voice-message".into()],
                    ..Default::default()
                },
                error: Some(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                    message: "voice request may have been accepted".into(),
                    sent_before_error: true,
                    ..Default::default()
                }),
            },
        )));
    let events = vec![ChannelOutputEvent::FinalMessage {
        text: "voice transcript".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("durable-host-voice".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1200),
                audio_base64: None,
            },
        }],
        receipt: None,
    }];
    let first = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: events.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(first, ProgressiveResult::DeliveryBatch { outcomes, .. }
            if outcomes.len() == 1
                && outcomes[0].receipt.platform_message_ids == ["accepted-voice-message"]
                && outcomes[0].error.as_ref().is_some_and(|error| error.sent_before_error)
        ),
        "unknown voice delivery must preserve accepted receipt and uncertainty"
    );
    let before = callback
        .requests
        .lock()
        .await
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
        .count();
    assert_eq!(before, 1);
    let replay = replies
        .execute(ProgressiveCommand::FeedOutput { handle, events })
        .await
        .unwrap();
    assert!(
        matches!(replay, ProgressiveResult::DeliveryBatch { outcomes, .. }
            if outcomes.len() == 1
                && outcomes[0].receipt.platform_message_ids == ["accepted-voice-message"]
                && outcomes[0].error.as_ref().is_some_and(|error| error.sent_before_error)
        ),
        "terminal replay returns the retained ambiguous outcome"
    );
    let after = callback
        .requests
        .lock()
        .await
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
        .count();
    assert_eq!(after, before, "unknown voice receipt must not be retried");
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_provider_callback_with_wrong_result_family_is_unknown_not_success() {
    let (replies, callback) = fixture(false).await;
    *callback.unexpected_typed_response.lock().await = true;
    let handle = open(&replies).await;
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            events: vec![ChannelOutputEvent::Media {
                text: None,
                parts: vec![tinychannels_bus::ContentPart::Image {
                    media: tinychannels_bus::MediaReference {
                        kind: tinychannels_bus::MediaKind::Image,
                        url: Some("https://media.invalid/wrong-response.png".into()),
                        ..Default::default()
                    },
                    caption: None,
                    alt_text: None,
                }],
            }],
        })
        .await
        .unwrap();
    let ProgressiveResult::DeliveryBatch { outcomes, .. } = result else {
        panic!("unexpected callback result family must remain a delivery failure")
    };
    assert_eq!(outcomes.len(), 1);
    let error = outcomes[0].error.as_ref().expect("unknown delivery result");
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(error.sent_before_error);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the separately built TinyVoice module; run with TINYVOICE_MODULE_ARTIFACT"]
async fn final_voice_policy_runs_through_progressive_reply_and_loaded_codec() {
    use tinybus::module::ModuleHost;

    let path = std::env::var("TINYVOICE_MODULE_ARTIFACT").expect("TinyVoice artifact path");
    let bytes = std::fs::read(&path).expect("read TinyVoice module");
    let expected = std::env::var("TINYVOICE_MODULE_SHA256").expect("artifact SHA");
    assert_eq!(hex::encode(sha2::Sha256::digest(&bytes)), expected);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let module_host = ModuleHost::new(broker);
    assert_eq!(
        module_host.load_file(&path).unwrap().name,
        "tinyvoice-module"
    );
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    connection
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let callback = Callback::default();
    connection
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callback.clone(),
        )
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let context = ProgressiveOutputContext {
        turn_id: "turn-voice-1".into(),
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram:bot-1".into()),
        },
        conversation: tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "chat-1".into(),
            topic_id: Some("topic-2".into()),
            ..Default::default()
        },
    };
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.voice_out.supported = true;
    capabilities.voice_out.synthesis = true;
    capabilities.progressive_edits = true;
    capabilities.voice_out.accepted_codecs = vec!["ogg_opus".into()];
    capabilities.voice_out.max_size_bytes = Some(8 * 1024 * 1024);
    capabilities.voice_out.max_duration_ms = Some(10_000);
    let cases = [
        (
            tinychannels_bus::HostVoiceReplyMode::Mirror,
            true,
            false,
            true,
        ),
        (
            tinychannels_bus::HostVoiceReplyMode::Mirror,
            false,
            false,
            false,
        ),
        (
            tinychannels_bus::HostVoiceReplyMode::Always,
            false,
            false,
            true,
        ),
        (
            tinychannels_bus::HostVoiceReplyMode::Never,
            true,
            true,
            false,
        ),
        (
            tinychannels_bus::HostVoiceReplyMode::SpeakReply,
            false,
            true,
            true,
        ),
        (
            tinychannels_bus::HostVoiceReplyMode::SpeakReply,
            true,
            false,
            false,
        ),
    ];
    for (index, (mode, inbound, explicit, expected_voice)) in cases.into_iter().enumerate() {
        *callback.voice_policy.lock().await = Some(tinychannels_bus::HostVoiceReplyPolicy {
            mode,
            inbound_has_voice: inbound,
            explicitly_requested: explicit,
        });
        let sender = Arc::new(Sender {
            host: host.clone(),
            context: Some(context.clone()),
            capabilities: capabilities.clone(),
            channel: "telegram:bot-1".into(),
            send_sequence: std::sync::atomic::AtomicU64::new(0),
        });
        let mut reply = tinychannels::delivery::progressive::ProgressiveReply::new_scoped(
            sender,
            "telegram:bot-1",
            capabilities.clone(),
            context.clone(),
        );
        reply
            .send_parts(
                &[tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: "spoken words".into(),
                        markdown_dialect: None,
                    },
                }],
                "spoken words",
            )
            .await
            .expect("progressive final sends successfully");
        let requests = callback.requests.lock().await;
        let request = requests
            .iter()
            .rev()
            .find_map(|request| match request {
                ProgressiveTransportRequest::SendTyped { parts, text, .. } => Some((parts, text)),
                _ => None,
            })
            .expect("final typed provider call");
        let has_media = request.0.iter().any(|part| {
            matches!(part,
                tinychannels_bus::OutboundContentPart::Voice {
                    voice: tinychannels_bus::OutboundVoice::Media { media, .. }
                } if media.opaque_id.as_deref() == Some("durable-host-voice")
            )
        });
        assert_eq!(has_media, expected_voice, "policy case {index}: {mode:?}");
        if expected_voice {
            let encoded = serde_json::to_vec(
                &requests
                    .iter()
                    .rev()
                    .find(|candidate| {
                        matches!(candidate, ProgressiveTransportRequest::SendTyped { parts, .. }
                        if parts.iter().any(|part| matches!(part,
                            tinychannels_bus::OutboundContentPart::Voice {
                                voice: tinychannels_bus::OutboundVoice::Media {
                                    audio_base64: None,
                                    media,
                                    ..
                                }
                            } if media.opaque_id.as_deref() == Some("durable-host-voice")
                        )))
                    })
                    .unwrap(),
            )
            .unwrap();
            assert!(encoded.len() < tinychannels_bus::module_relay::RELAY_FRAME_BYTES);
            assert!(!String::from_utf8_lossy(&encoded).contains("audio_base64"));
        }
        drop(requests);
    }

    // Exercise the real progressive worker sequence that previously bypassed
    // final voice policy: a text delta creates/updates a draft, then the
    // canonical final event has no explicit typed parts. Each mode gets a
    // distinct turn so delivery idempotency keys cannot alias.
    for (turn_id, mode, inbound) in [
        (
            "draft-mirror",
            tinychannels_bus::HostVoiceReplyMode::Mirror,
            true,
        ),
        (
            "draft-always",
            tinychannels_bus::HostVoiceReplyMode::Always,
            false,
        ),
    ] {
        *callback.voice_policy.lock().await = Some(tinychannels_bus::HostVoiceReplyPolicy {
            mode,
            inbound_has_voice: inbound,
            explicitly_requested: false,
        });
        let replies = Replies::new(host.clone(), Arc::new(Mutex::new(HashMap::new())));
        let handle = open_scoped_provider(
            &replies,
            "telegram",
            "telegram:bot-1",
            turn_id,
            capabilities.clone(),
        )
        .await;
        let before = callback.requests.lock().await.len();
        let draft = replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![ChannelOutputEvent::TextDelta {
                    text: "spoken ".into(),
                    iteration: None,
                }],
            })
            .await
            .expect("text delta reaches progressive worker");
        assert!(matches!(draft, ProgressiveResult::Updated));
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if callback
                    .requests
                    .lock()
                    .await
                    .iter()
                    .skip(before)
                    .any(|request| {
                        matches!(request,
                            ProgressiveTransportRequest::Send { turn_id: Some(actual_turn), .. }
                                if actual_turn == turn_id
                        )
                    })
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("text delta must create a provider draft before finalization");
        let final_result = replies
            .execute(ProgressiveCommand::FeedOutput {
                handle,
                events: vec![ChannelOutputEvent::FinalMessage {
                    text: "spoken words".into(),
                    parts: vec![],
                    receipt: None,
                }],
            })
            .await
            .expect("final message completes progressive turn");
        assert!(matches!(
            final_result,
            ProgressiveResult::DeliveryOutcome { .. }
        ));
        let requests = callback.requests.lock().await;
        let terminal = &requests[before..];
        let voice_sends = terminal
            .iter()
            .filter(|request| {
                matches!(request, ProgressiveTransportRequest::SendTyped { parts, .. }
                    if parts.iter().any(|part| matches!(part,
                        tinychannels_bus::OutboundContentPart::Voice {
                            voice: tinychannels_bus::OutboundVoice::Media { media, .. }
                        } if media.opaque_id.as_deref() == Some("durable-host-voice")
                    ))
                )
            })
            .count();
        assert_eq!(
            voice_sends, 1,
            "{mode:?} final must send one native voice part"
        );
        assert!(
            terminal.iter().any(|request| matches!(request,
                ProgressiveTransportRequest::Send { turn_id: Some(actual_turn), .. }
                    if actual_turn == turn_id
            )),
            "the worker must first create/update the progressive text draft"
        );
        let draft_delete = terminal.iter().find_map(|request| match request {
            ProgressiveTransportRequest::Delete {
                turn_id: Some(actual_turn),
                receipt,
                ..
            } if actual_turn == turn_id => receipt.as_ref(),
            _ => None,
        });
        assert!(
            draft_delete.is_some(),
            "the existing progressive draft must be cleaned up after final voice delivery"
        );
        assert_eq!(
            draft_delete.unwrap().platform_message_ids,
            ["fixture-message"],
            "cleanup must use the draft's provider receipt"
        );
        drop(requests);
        replies.shutdown().await.expect("close scoped worker");
    }

    *callback.voice_policy.lock().await = Some(tinychannels_bus::HostVoiceReplyPolicy {
        mode: tinychannels_bus::HostVoiceReplyMode::Always,
        inbound_has_voice: false,
        explicitly_requested: false,
    });
    let audio_turn = ProgressiveOutputContext {
        turn_id: "audio-file-fallback".into(),
        ..context.clone()
    };
    let mut audio_caps = tinychannels_bus::ChannelCapabilities::default();
    audio_caps.voice_out.synthesis = true;
    audio_caps.content.parts = vec!["text".into(), "audio".into()];
    audio_caps.content.audio.outbound = true;
    audio_caps.content.audio.accepted_types = vec!["audio/ogg".into()];
    audio_caps.content.audio.max_size_bytes = Some(8 * 1024 * 1024);
    audio_caps.content.audio.max_duration_ms = Some(10_000);
    let audio_sender = Arc::new(Sender {
        host: host.clone(),
        context: Some(audio_turn.clone()),
        capabilities: audio_caps.clone(),
        channel: "telegram:bot-1".into(),
        send_sequence: std::sync::atomic::AtomicU64::new(0),
    });
    let mut audio_reply = tinychannels::delivery::progressive::ProgressiveReply::new_scoped(
        audio_sender,
        "telegram:bot-1",
        audio_caps,
        audio_turn,
    );
    audio_reply
        .send_parts(
            &[tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: "spoken words".into(),
                    markdown_dialect: None,
                },
            }],
            "spoken words",
        )
        .await
        .expect("audio-file fallback is delivered");
    let requests = callback.requests.lock().await;
    let audio_request = requests
        .iter()
        .rev()
        .find_map(|request| match request {
            ProgressiveTransportRequest::SendTyped {
                turn_id: Some(turn_id),
                parts,
                ..
            } if turn_id == "audio-file-fallback" => Some(parts),
            _ => None,
        })
        .expect("audio-file typed send");
    assert!(audio_request.iter().any(|part| matches!(part,
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Audio { media, codec: Some(codec), .. }
        } if media.opaque_id.as_deref() == Some("durable-host-voice") && codec == "ogg_opus"
    )));
    assert!(
        !audio_request
            .iter()
            .any(|part| matches!(part, tinychannels_bus::OutboundContentPart::Voice { .. }))
    );
    drop(requests);

    let link_turn = ProgressiveOutputContext {
        turn_id: "authorized-link-fallback".into(),
        ..context.clone()
    };
    let mut link_caps = tinychannels_bus::ChannelCapabilities::default();
    link_caps.voice_out.synthesis = true;
    let link_sender = Arc::new(Sender {
        host: host.clone(),
        context: Some(link_turn.clone()),
        capabilities: link_caps.clone(),
        channel: "telegram:bot-1".into(),
        send_sequence: std::sync::atomic::AtomicU64::new(0),
    });
    let mut link_reply = tinychannels::delivery::progressive::ProgressiveReply::new_scoped(
        link_sender,
        "telegram:bot-1",
        link_caps,
        link_turn,
    );
    link_reply
        .send_parts(
            &[
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: "spoken ".into(),
                        markdown_dialect: None,
                    },
                },
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: "words".into(),
                        markdown_dialect: None,
                    },
                },
            ],
            "spoken words",
        )
        .await
        .expect("authorized-link fallback is delivered");
    let requests = callback.requests.lock().await;
    let link_request = requests
        .iter()
        .rev()
        .find_map(|request| match request {
            ProgressiveTransportRequest::SendTyped {
                turn_id: Some(turn_id),
                parts,
                text,
                ..
            } if turn_id == "authorized-link-fallback" => Some((parts, text)),
            _ => None,
        })
        .expect("text plus authorized link send");
    assert!(!link_request.0.iter().any(|part| matches!(
        part,
        tinychannels_bus::OutboundContentPart::Voice { .. }
            | tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Audio { .. }
            }
    )));
    assert!(
        link_request
            .1
            .contains("https://media.example/authorized/voice")
    );
    let canonical_text = link_request
        .0
        .iter()
        .filter_map(|part| match part {
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text { text, .. },
            } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        canonical_text.len(),
        2,
        "fallback must preserve ordered text parts"
    );
    assert_eq!(canonical_text[0], "spoken ");
    assert!(canonical_text[1].contains("words"));
    assert!(
        canonical_text[1].contains("https://media.example/authorized/voice"),
        "authorized link must exist in provider-rendered content.parts"
    );
    drop(requests);

    *callback.voice_policy.lock().await = Some(tinychannels_bus::HostVoiceReplyPolicy {
        mode: tinychannels_bus::HostVoiceReplyMode::Always,
        inbound_has_voice: false,
        explicitly_requested: false,
    });
    capabilities.voice_out.max_size_bytes = Some(1);
    let sender = Arc::new(Sender {
        host: host.clone(),
        context: Some(context.clone()),
        capabilities: capabilities.clone(),
        channel: "telegram:bot-1".into(),
        send_sequence: std::sync::atomic::AtomicU64::new(0),
    });
    let mut too_long = tinychannels::delivery::progressive::ProgressiveReply::new_scoped(
        sender,
        "telegram:bot-1",
        capabilities.clone(),
        context.clone(),
    );
    let stores_before = callback.voice_store_calls.load(Ordering::SeqCst);
    too_long
        .send_parts(
            &[tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: "spoken words".into(),
                    markdown_dialect: None,
                },
            }],
            "spoken words",
        )
        .await
        .expect("oversized voice falls back to the text response");
    assert_eq!(
        callback.voice_store_calls.load(Ordering::SeqCst),
        stores_before
    );
    let requests = callback.requests.lock().await;
    let too_long_request = requests
        .iter()
        .rev()
        .find_map(|request| match request {
            ProgressiveTransportRequest::SendTyped { parts, text, .. } => Some((parts, text)),
            _ => None,
        })
        .unwrap();
    assert!(
        !too_long_request
            .0
            .iter()
            .any(|part| matches!(part, tinychannels_bus::OutboundContentPart::Voice { .. }))
    );
    assert!(
        too_long_request
            .1
            .contains("Audio reply exceeded the channel size limit")
    );
    assert!(
        too_long_request.0.iter().any(|part| matches!(part,
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text { text, .. }
            } if text.contains("Audio reply exceeded the channel size limit")
        )),
        "size fallback note must exist in canonical provider-rendered content.parts"
    );
    drop(requests);

    let duration_context = ProgressiveOutputContext {
        turn_id: "duration-limit-fallback".into(),
        ..context.clone()
    };
    capabilities.voice_out.max_size_bytes = Some(8 * 1024 * 1024);
    capabilities.voice_out.max_duration_ms = Some(1_000);
    let sender = Arc::new(Sender {
        host: host.clone(),
        context: Some(duration_context.clone()),
        capabilities: capabilities.clone(),
        channel: "telegram:bot-1".into(),
        send_sequence: std::sync::atomic::AtomicU64::new(0),
    });
    let mut duration_limited = tinychannels::delivery::progressive::ProgressiveReply::new_scoped(
        sender,
        "telegram:bot-1",
        capabilities.clone(),
        duration_context,
    );
    duration_limited
        .send_parts(
            &[tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: "spoken words".into(),
                    markdown_dialect: None,
                },
            }],
            "spoken words",
        )
        .await
        .expect("too-long audio duration falls back to text");
    let requests = callback.requests.lock().await;
    let duration_request = requests
        .iter()
        .rev()
        .find_map(|request| match request {
            ProgressiveTransportRequest::SendTyped {
                turn_id: Some(turn_id),
                parts,
                ..
            } if turn_id == "duration-limit-fallback" => Some(parts),
            _ => None,
        })
        .expect("duration-limit text send");
    assert!(
        duration_request.iter().any(|part| matches!(part,
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text { text, .. }
            } if text.contains("Audio reply was too long to send")
        )),
        "duration limit note must exist in canonical provider-rendered content.parts"
    );
    drop(requests);

    *callback.voice_policy.lock().await = Some(tinychannels_bus::HostVoiceReplyPolicy {
        mode: tinychannels_bus::HostVoiceReplyMode::Always,
        inbound_has_voice: false,
        explicitly_requested: false,
    });
    let sender = Arc::new(Sender {
        host,
        context: Some(context.clone()),
        capabilities: capabilities.clone(),
        channel: "telegram:bot-1".into(),
        send_sequence: std::sync::atomic::AtomicU64::new(0),
    });
    let prompt = [tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Text {
            text: "approval prompt".into(),
            markdown_dialect: None,
        },
    }];
    let before = callback.synthesis_calls.load(Ordering::SeqCst);
    sender
        .send_parts_with_segment(
            "telegram:bot-1",
            &prompt,
            "approval prompt",
            Some(&context),
            100,
            0,
        )
        .await
        .expect("mid-turn approval delivery");
    assert_eq!(callback.synthesis_calls.load(Ordering::SeqCst), before);
}

fn progressive_test_wav() -> Vec<u8> {
    let rate = 16_000u32;
    let count = rate as usize * 8;
    let data_len = count * 2;
    let mut wav = Vec::with_capacity(44 + data_len);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36u32 + data_len as u32).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&rate.to_le_bytes());
    wav.extend_from_slice(&(rate * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(data_len as u32).to_le_bytes());
    for index in 0..count {
        let sample =
            ((index as f32 * 440.0 * std::f32::consts::TAU / rate as f32).sin() * 12_000.0) as i16;
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    wav
}
