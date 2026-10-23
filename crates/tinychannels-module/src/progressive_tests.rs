use super::*;
use crate::provider::Hooks;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tokio::sync::Notify;

struct ProgressiveChannel;

#[allow(deprecated)]
#[async_trait::async_trait]
impl tinychannels::Channel for ProgressiveChannel {
    fn name(&self) -> &str {
        "discord"
    }

    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        tinychannels_bus::ChannelCapabilities {
            progressive_edits: true,
            ..Default::default()
        }
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

#[derive(Clone, Default)]
struct Callback {
    sends: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<ProgressiveTransportRequest>>>,
    result: Arc<Mutex<Option<ProgressiveTransportResult>>>,
    typed_result: Arc<Mutex<Option<ProgressiveTransportResult>>>,
    failed_output_sequences: Arc<Mutex<Vec<u64>>>,
    block: bool,
    entered: Arc<Notify>,
    released: Arc<Notify>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn dispatch_controller_backend(
        &self,
        request: ChannelBackendRequest,
    ) -> Result<ChannelBackendResult> {
        let ChannelBackendRequest::ProgressiveTransport(request) = request else {
            return Err(Error::failed("unexpected callback"));
        };
        self.requests.lock().await.push((*request).clone());
        if let ProgressiveTransportRequest::OutputEvent { sequence, .. } = &*request
            && self.failed_output_sequences.lock().await.contains(sequence)
        {
            return Ok(ChannelBackendResult::ProgressiveTransport(
                ProgressiveTransportResult::Failed,
            ));
        }
        if let ProgressiveTransportRequest::Send { message, .. } = &*request {
            self.sends.lock().await.push(message.clone());
            if self.block {
                self.entered.notify_one();
                self.released.notified().await;
            }
        }
        let default = match &*request {
            ProgressiveTransportRequest::OutputEvent { .. } => {
                ProgressiveTransportResult::Acknowledged
            }
            ProgressiveTransportRequest::SendTyped { .. } => {
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                outcome.receipt.primary_platform_message_id = Some("fixture-message".into());
                ProgressiveTransportResult::Outcome(Box::new(outcome))
            }
            _ => ProgressiveTransportResult::Success(serde_json::json!({"id": "fixture-message"})),
        };
        let override_result = if matches!(&*request, ProgressiveTransportRequest::SendTyped { .. })
        {
            self.typed_result.lock().await.clone()
        } else {
            self.result.lock().await.clone()
        };
        Ok(ChannelBackendResult::ProgressiveTransport(
            override_result.unwrap_or(default),
        ))
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

#[tokio::test(start_paused = true)]
async fn typed_final_removes_a_flushed_progressive_draft() {
    let (replies, callback) = fixture(true).await;
    let provider = Running::start_outbound(
        Arc::new(ProgressiveChannel),
        Arc::new(Hooks::default()),
        replies.host.clone(),
    );
    replies
        .running
        .lock()
        .await
        .insert("discord".into(), provider.clone());
    let handle = open(&replies).await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 1,
            events: vec![ChannelOutputEvent::TextDelta {
                text: "draft answer".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();

    let draft_send_started = callback.entered.notified();
    tokio::time::advance(tinychannels::delivery::progressive::EDIT_FLUSH_INTERVAL).await;
    draft_send_started.await;
    callback.released.notify_one();

    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 2,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "final answer".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();

    let requests = callback.requests.lock().await;
    let draft = requests
        .iter()
        .position(|request| matches!(request, ProgressiveTransportRequest::Send { .. }))
        .expect("the timer flushed a draft before the final message");
    let final_message = requests
        .iter()
        .position(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
        .expect("the canonical final message was sent");
    let delete = requests
        .iter()
        .position(|request| matches!(request, ProgressiveTransportRequest::Delete { .. }))
        .expect("the stale draft is deleted after the typed final");
    assert!(draft < final_message && final_message < delete);
    drop(requests);

    replies.shutdown().await.unwrap();
    provider.stop().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn accepted_typed_final_keeps_a_flushed_progressive_draft() {
    let (replies, callback) = fixture(true).await;
    *callback.typed_result.lock().await = Some(ProgressiveTransportResult::Accepted);
    let provider = Running::start_outbound(
        Arc::new(ProgressiveChannel),
        Arc::new(Hooks::default()),
        replies.host.clone(),
    );
    replies
        .running
        .lock()
        .await
        .insert("discord".into(), provider.clone());
    let handle = open(&replies).await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 1,
            events: vec![ChannelOutputEvent::TextDelta {
                text: "draft answer".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();

    let draft_send_started = callback.entered.notified();
    tokio::time::advance(tinychannels::delivery::progressive::EDIT_FLUSH_INTERVAL).await;
    draft_send_started.await;
    callback.released.notify_one();

    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 2,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "final answer".into(),
                parts: vec![],
                receipt: None,
            }],
        })
        .await
        .unwrap();

    let requests = callback.requests.lock().await;
    assert!(
        requests
            .iter()
            .any(|request| matches!(request, ProgressiveTransportRequest::Send { .. }))
    );
    assert!(
        requests
            .iter()
            .any(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
    );
    assert!(
        !requests
            .iter()
            .any(|request| matches!(request, ProgressiveTransportRequest::Delete { .. })),
        "accepted means the provider receipt is pending, so keep the visible draft"
    );
    drop(requests);

    replies.shutdown().await.unwrap();
    provider.stop().await.unwrap();
}

#[tokio::test]
async fn first_scoped_output_typing_uses_the_output_batch_scope() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            turn_id: Some("turn-scoped".into()),
            channel_ref: Some(Box::new(tinychannels_bus::ChannelRef {
                id: "channel-ref".into(),
                account_id: Some("account-1".into()),
            })),
            conversation: Some(Box::new(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Thread,
                id: "conversation-1".into(),
                scope_id: None,
                parent_id: Some("parent-1".into()),
                thread_id: Some("thread-1".into()),
                topic_id: None,
            })),
            sequence: 1,
            events: vec![ChannelOutputEvent::TextDelta {
                text: "scoped response".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap();

    let requests = callback.requests.lock().await;
    let first_typing = requests
        .iter()
        .find_map(|request| match request {
            ProgressiveTransportRequest::Typing {
                turn_id,
                channel_ref,
                conversation,
                ..
            } => Some((turn_id, channel_ref, conversation)),
            _ => None,
        })
        .expect("the first scoped batch should start typing");
    assert_eq!(first_typing.0.as_deref(), Some("turn-scoped"));
    assert_eq!(
        first_typing
            .1
            .as_ref()
            .map(|channel| channel.account_id.as_deref()),
        Some(Some("account-1"))
    );
    assert_eq!(
        first_typing
            .2
            .as_ref()
            .and_then(|conversation| conversation.thread_id.as_deref()),
        Some("thread-1")
    );
    drop(requests);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_legacy_feed_keeps_the_previous_progressive_sender_scope() {
    let (replies, callback) = fixture(false).await;
    let sender = Sender::new(replies.host.clone());
    sender
        .set_scope(Some(OutputScope {
            handle: "old-handle".into(),
            channel: "discord:fixture".into(),
            turn_id: Some("old-turn".into()),
            channel_ref: None,
            conversation: None,
        }))
        .await;
    let mut reply = ProgressiveReply::new(
        Arc::new(sender.clone()),
        "discord:fixture",
        Default::default(),
    );
    let result = apply_legacy_feed(
        &mut reply,
        &sender,
        vec![ProgressiveEvent::TextDelta("x".repeat(MAX_CONTENT + 1))],
    )
    .await;
    assert!(result.is_err());

    ProgressiveSender::typing(&sender, "discord:fixture")
        .await
        .unwrap();
    let requests = callback.requests.lock().await;
    let ProgressiveTransportRequest::Typing { turn_id, .. } = requests.last().unwrap() else {
        panic!("expected a typing request")
    };
    assert_eq!(turn_id.as_deref(), Some("old-turn"));
}

#[tokio::test]
async fn repeated_final_text_does_not_double_count_against_the_delta_budget() {
    let (replies, _) = fixture(false).await;
    let handle = open(&replies).await;
    let repeated_text = "x".repeat(250 * 1024);
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle,
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 1,
            events: vec![
                ChannelOutputEvent::TextDelta {
                    text: repeated_text.clone(),
                    iteration: Some(1),
                },
                ChannelOutputEvent::FinalMessage {
                    text: repeated_text,
                    parts: vec![],
                    receipt: None,
                },
            ],
        })
        .await;
    assert!(result.is_ok());
    replies.shutdown().await.unwrap();
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
async fn handles_cannot_cross_private_runtime_reply_stores() {
    let (first, _) = fixture(false).await;
    let (second, _) = fixture(false).await;
    let handle = open(&first).await;
    assert!(
        second
            .execute(ProgressiveCommand::Feed {
                handle: handle.clone(),
                events: vec![ProgressiveEvent::TextDelta("fixture".into())]
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
        Arc::new(Sender::new(replies.host.clone())),
        "discord:fixture",
        tinychannels_bus::ChannelCapabilities::unknown(),
    );
    apply(
        &mut reply,
        vec![ProgressiveEvent::TextDelta("x".repeat(MAX_CONTENT))],
    )
    .unwrap();
    assert!(
        apply(
            &mut reply,
            vec![
                ProgressiveEvent::ToolCall("fixture-tool".into()),
                ProgressiveEvent::TextDelta("extra".into())
            ]
        )
        .is_err()
    );
    assert_eq!(reply.state().content.len(), MAX_CONTENT);
    assert!(reply.state().last_tool.is_none());
    let handle = open(&replies).await;
    assert!(
        replies
            .execute(ProgressiveCommand::Feed {
                handle: handle.clone(),
                events: vec![ProgressiveEvent::ThinkingDelta("x".repeat(MAX_REQUEST))]
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
    assert_eq!(callback.sends.lock().await.len(), MAX_ACTIVE);
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
async fn shutdown_starts_every_close_before_waiting_for_blocked_delivery() {
    let (replies, callback) = fixture(true).await;
    let replies = Arc::new(replies);
    open(&replies).await;
    open(&replies).await;
    let retained = replies.clone();
    let shutdown = tokio::spawn(async move { retained.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let entered = callback.entered.notified();
            if callback.sends.lock().await.len() == 2 {
                break;
            }
            entered.await;
        }
    })
    .await
    .expect("every reply must begin closing even while another delivery is blocked");
    assert!(shutdown.await.unwrap().is_err());
    assert_eq!(replies.state.lock().await.entries.len(), 2);
    callback.released.notify_waiters();
    replies.shutdown().await.unwrap();
    assert!(replies.state.lock().await.entries.is_empty());
    assert_eq!(callback.sends.lock().await.len(), 2);
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
            .execute(ProgressiveCommand::Feed {
                handle: handle.clone(),
                events: vec![
                    ProgressiveEvent::ThinkingDelta("considering".into()),
                    ProgressiveEvent::ToolCall("lookup".into()),
                    ProgressiveEvent::ToolResult {
                        tool_name: "lookup".into(),
                        success: true
                    },
                    ProgressiveEvent::TextDelta("partial answer".into()),
                ],
            })
            .await
            .unwrap(),
        ProgressiveResult::Updated
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
        .execute(ProgressiveCommand::Feed {
            handle: handle.clone(),
            events: vec![ProgressiveEvent::TextDelta("late".into())],
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("closed"));
    replies.shutdown().await.unwrap();
    let error = replies
        .execute(ProgressiveCommand::Feed {
            handle,
            events: vec![],
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("shutting down"));
}

#[tokio::test]
async fn feed_output_forwards_canonical_events_in_order_with_delivery_scope() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let final_parts = vec![tinychannels_bus::channel::OutboundContentPart::Content {
        part: tinychannels_bus::channel::ContentPart::Text {
            text: "complete".into(),
            markdown_dialect: None,
        },
    }];
    let events = vec![
        ChannelOutputEvent::TextDelta {
            text: "partial".into(),
            iteration: Some(1),
        },
        ChannelOutputEvent::FinalMessage {
            text: "complete".into(),
            parts: final_parts.clone(),
            receipt: None,
        },
    ];
    assert_eq!(
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                turn_id: Some("turn-1".into()),
                channel_ref: Some(Box::new(tinychannels_bus::ChannelRef {
                    id: "channel-ref".into(),
                    account_id: Some("account-1".into()),
                })),
                conversation: Some(Box::new(tinychannels_bus::ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Thread,
                    id: "conversation-1".into(),
                    scope_id: Some("scope-1".into()),
                    parent_id: Some("parent-1".into()),
                    thread_id: Some("thread-1".into()),
                    topic_id: None,
                })),
                sequence: 40,
                events: events.clone(),
            })
            .await
            .unwrap(),
        ProgressiveResult::OutputUpdated {
            deliveries: vec![ProgressiveTransportResult::Outcome(Box::new({
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                outcome.receipt.primary_platform_message_id = Some("fixture-message".into());
                outcome
            }))],
            output_event_failures: vec![],
        }
    );
    let requests = callback.requests.lock().await;
    let send_request = requests
        .iter()
        .find(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
        .expect("final output is sent through typed provider transport");
    assert!(matches!(
        send_request,
        ProgressiveTransportRequest::SendTyped {
            turn_id: Some(turn_id),
            channel_ref: Some(channel_ref),
            conversation: Some(conversation),
            sequence: Some(41),
            idempotency_key: Some(key),
            parts,
            text,
            ..
        } if turn_id == "turn-1"
            && key == &format!("progressive:{handle}:41")
            && channel_ref.id == "channel-ref"
            && channel_ref.account_id.as_deref() == Some("account-1")
            && conversation.id == "conversation-1"
            && conversation.thread_id.as_deref() == Some("thread-1")
            && parts == &final_parts
            && text == "complete"
    ));
    let output_requests = requests
        .iter()
        .filter(|request| matches!(request, ProgressiveTransportRequest::OutputEvent { .. }))
        .collect::<Vec<_>>();
    assert_eq!(output_requests.len(), events.len());
    assert!(matches!(
        output_requests[0],
        ProgressiveTransportRequest::OutputEvent {
            turn_id: Some(turn_id),
            channel_ref: Some(channel_ref),
            conversation: Some(conversation),
            sequence: 40,
            event,
            ..
        } if turn_id == "turn-1"
            && channel_ref.id == "channel-ref"
            && conversation.id == "conversation-1"
            && **event == events[0]
    ));
    assert!(matches!(
        output_requests[1],
        ProgressiveTransportRequest::OutputEvent {
            turn_id: Some(turn_id),
            sequence: 41,
            event,
            ..
        } if turn_id == "turn-1"
            && matches!(**event, ChannelOutputEvent::FinalMessage { receipt: Some(_), .. })
    ));
    drop(requests);
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn feed_output_returns_prior_and_current_receipts_when_output_event_callbacks_fail() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    *callback.failed_output_sequences.lock().await = vec![41];
    let events = vec![
        ChannelOutputEvent::TextDelta {
            text: "draft".into(),
            iteration: Some(1),
        },
        ChannelOutputEvent::FinalMessage {
            text: "second".into(),
            parts: vec![],
            receipt: None,
        },
    ];

    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 40,
            events,
        })
        .await
        .expect("callback failures must not erase completed provider deliveries");

    let ProgressiveResult::OutputUpdated {
        deliveries,
        output_event_failures,
    } = result
    else {
        panic!("expected typed partial output result");
    };
    assert_eq!(deliveries.len(), 1);
    for delivery in deliveries {
        let ProgressiveTransportResult::Outcome(outcome) = delivery else {
            panic!("expected provider receipt for each successful send");
        };
        assert_eq!(
            outcome.receipt.primary_platform_message_id.as_deref(),
            Some("fixture-message")
        );
    }
    assert_eq!(output_event_failures, vec![41]);
    assert_eq!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .filter(|request| matches!(request, ProgressiveTransportRequest::OutputEvent { .. }))
            .count(),
        2,
        "a failed observation callback must not prevent later events from being processed"
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn feed_output_retries_failed_callbacks_without_repeating_typed_delivery() {
    let (replies, callback) = fixture(false).await;
    let sender = Sender::new(replies.host.clone());
    let scope = OutputScope {
        handle: "handle-1".into(),
        channel: "discord:fixture".into(),
        turn_id: Some("turn-1".into()),
        channel_ref: None,
        conversation: None,
    };
    let mut reply = ProgressiveReply::new(
        Arc::new(sender.clone()),
        "discord:fixture",
        Default::default(),
    );
    let output_replay = Mutex::new(None);
    let events = vec![ChannelOutputEvent::FinalMessage {
        text: "done".into(),
        parts: vec![],
        receipt: None,
    }];
    *callback.failed_output_sequences.lock().await = vec![41];

    let first = apply_output(&mut reply, &sender, &scope, 41, events.clone(), &output_replay)
        .await
        .unwrap();
    assert_eq!(first.output_event_failures, vec![41]);
    *callback.failed_output_sequences.lock().await = vec![];

    let replay = apply_output(&mut reply, &sender, &scope, 41, events, &output_replay)
        .await
        .unwrap();
    assert!(replay.output_event_failures.is_empty());
    assert_eq!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .filter(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
            .count(),
        1,
        "replaying an accepted batch must not repeat typed delivery"
    );
    assert_eq!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .filter(|request| matches!(request, ProgressiveTransportRequest::OutputEvent { .. }))
            .count(),
        2,
        "replay must retry the failed observation callback"
    );
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn feed_output_retries_reuse_the_previous_result_without_reapplying_deltas() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let event = ChannelOutputEvent::TextDelta {
        text: "once".into(),
        iteration: Some(1),
    };
    let request = || ProgressiveCommand::FeedOutput {
        handle: handle.clone(),
        turn_id: Some("turn-1".into()),
        channel_ref: None,
        conversation: None,
        sequence: 70,
        events: vec![event.clone()],
    };

    assert_eq!(
        replies.execute(request()).await.unwrap(),
        ProgressiveResult::Updated
    );
    assert_eq!(
        replies.execute(request()).await.unwrap(),
        ProgressiveResult::Updated
    );
    assert_eq!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .filter(|request| matches!(request, ProgressiveTransportRequest::OutputEvent { .. }))
            .count(),
        1,
        "an identical retry must not re-append or re-project its delta"
    );
    let mut changed = event;
    if let ChannelOutputEvent::TextDelta { text, .. } = &mut changed {
        text.push_str(" changed");
    }
    assert!(
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                turn_id: Some("turn-1".into()),
                channel_ref: None,
                conversation: None,
                sequence: 70,
                events: vec![changed],
            })
            .await
            .is_err(),
        "reusing a sequence for different content must be rejected"
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn final_output_terminates_without_a_second_legacy_send_and_replays_receipt() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let request = || ProgressiveCommand::FeedOutput {
        handle: handle.clone(),
        turn_id: Some("turn-1".into()),
        channel_ref: None,
        conversation: None,
        sequence: 80,
        events: vec![ChannelOutputEvent::FinalMessage {
            text: "final".into(),
            parts: vec![],
            receipt: None,
        }],
    };
    let first = replies.execute(request()).await.unwrap();
    assert!(matches!(first, ProgressiveResult::OutputUpdated { .. }));
    assert_eq!(replies.execute(request()).await.unwrap(), first);
    tokio::task::yield_now().await;
    replies
        .execute(ProgressiveCommand::Close {
            handle: handle.clone(),
        })
        .await
        .unwrap();
    let requests = callback.requests.lock().await;
    assert_eq!(
        requests
            .iter()
            .filter(|request| matches!(request, ProgressiveTransportRequest::SendTyped { .. }))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| matches!(request, ProgressiveTransportRequest::Send { .. }))
            .count(),
        0,
        "closing a completed turn must not emit a cancellation or second final send"
    );
    drop(requests);
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn feed_output_rejects_accumulated_text_and_thinking_over_the_content_limit() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 90,
            events: vec![
                ChannelOutputEvent::TextDelta {
                    text: "would be applied".into(),
                    iteration: Some(1),
                },
                ChannelOutputEvent::ThinkingDelta {
                    text: "x".repeat(MAX_CONTENT + 1),
                    iteration: Some(1),
                },
            ],
        })
        .await;
    assert!(result.is_err());
    assert_eq!(
        callback
            .requests
            .lock()
            .await
            .iter()
            .filter(|request| matches!(request, ProgressiveTransportRequest::OutputEvent { .. }))
            .count(),
        0,
        "the batch must be rejected before projecting or applying any event"
    );
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn feed_output_rejects_final_message_text_over_the_content_limit() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let result = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: None,
            sequence: 1,
            events: vec![ChannelOutputEvent::FinalMessage {
                text: "x".repeat(MAX_CONTENT + 1),
                parts: vec![],
                receipt: None,
            }],
        })
        .await;
    assert!(result.is_err());
    assert!(callback.requests.lock().await.iter().all(|request| {
        !matches!(
            request,
            ProgressiveTransportRequest::SendTyped { .. }
                | ProgressiveTransportRequest::OutputEvent { .. }
        )
    }));
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_output_keeps_the_previous_progressive_sender_scope() {
    let (replies, callback) = fixture(false).await;
    let sender = Sender::new(replies.host.clone());
    let old_scope = OutputScope {
        handle: "old-handle".into(),
        channel: "discord:fixture".into(),
        turn_id: Some("old-turn".into()),
        channel_ref: None,
        conversation: None,
    };
    sender.set_scope(Some(old_scope)).await;
    let new_scope = OutputScope {
        handle: "new-handle".into(),
        channel: "discord:fixture".into(),
        turn_id: Some("new-turn".into()),
        channel_ref: None,
        conversation: None,
    };
    let mut reply = ProgressiveReply::new(
        Arc::new(sender.clone()),
        "discord:fixture",
        Default::default(),
    );
    let output_replay = Mutex::new(None);
    let result = apply_scoped_output(
        &mut reply,
        &sender,
        &new_scope,
        1,
        vec![
            ChannelOutputEvent::FinalMessage {
                text: "misplaced final".into(),
                parts: vec![],
                receipt: None,
            },
            ChannelOutputEvent::TextDelta {
                text: "late delta".into(),
                iteration: Some(1),
            },
        ],
        &output_replay,
    )
    .await;
    assert!(result.is_err());

    ProgressiveSender::typing(&sender, "discord:fixture")
        .await
        .unwrap();
    let requests = callback.requests.lock().await;
    let ProgressiveTransportRequest::Typing { turn_id, .. } = requests.last().unwrap() else {
        panic!("expected a typing request")
    };
    assert_eq!(turn_id.as_deref(), Some("old-turn"));
}

#[tokio::test]
async fn feed_output_rejects_scope_changes_after_first_accepted_batch() {
    let (replies, callback) = fixture(false).await;
    let handle = open(&replies).await;
    let first_scope = tinychannels_bus::ConversationRef {
        kind: tinychannels_bus::ConversationKind::Thread,
        id: "conversation-1".into(),
        ..Default::default()
    };
    let event = || ChannelOutputEvent::TextDelta {
        text: "draft".into(),
        iteration: Some(1),
    };
    replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-1".into()),
            channel_ref: None,
            conversation: Some(Box::new(first_scope)),
            sequence: 1,
            events: vec![event()],
        })
        .await
        .unwrap();

    let changed = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            turn_id: Some("turn-2".into()),
            channel_ref: Some(Box::new(tinychannels_bus::ChannelRef {
                id: "channel-2".into(),
                account_id: Some("account-2".into()),
            })),
            conversation: Some(Box::new(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Thread,
                id: "conversation-2".into(),
                ..Default::default()
            })),
            sequence: 2,
            events: vec![event()],
        })
        .await;
    assert!(changed.is_err(), "a reply handle must retain its first scope");
    assert!(callback.requests.lock().await.iter().all(|request| match request {
        ProgressiveTransportRequest::OutputEvent {
            turn_id,
            channel_ref,
            conversation,
            ..
        } => {
            turn_id.as_deref() == Some("turn-1")
                && channel_ref.is_none()
                && conversation.as_ref().is_some_and(|value| value.id == "conversation-1")
        }
        _ => true,
    }));
    replies
        .execute(ProgressiveCommand::Close { handle })
        .await
        .unwrap();
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn progressive_draft_transport_preserves_the_output_batch_scope() {
    let (replies, callback) = fixture(false).await;
    let sender = Sender::new(replies.host.clone());
    sender
        .set_scope(Some(OutputScope {
            handle: "handle-1".into(),
            channel: "discord:fixture".into(),
            turn_id: Some("turn-1".into()),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "channel-ref".into(),
                account_id: Some("account-1".into()),
            }),
            conversation: Some(tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Thread,
                id: "conversation-1".into(),
                scope_id: Some("scope-1".into()),
                parent_id: Some("parent-1".into()),
                thread_id: Some("thread-1".into()),
                topic_id: None,
            }),
        }))
        .await;
    ProgressiveSender::send(&sender, "discord:fixture", "draft")
        .await
        .unwrap();
    ProgressiveSender::edit(&sender, "discord:fixture", "message-1", "edited")
        .await
        .unwrap();
    ProgressiveSender::delete(&sender, "discord:fixture", "message-1")
        .await
        .unwrap();
    ProgressiveSender::typing(&sender, "discord:fixture")
        .await
        .unwrap();

    let requests = callback.requests.lock().await;
    assert_eq!(requests.len(), 4);
    for request in requests.iter() {
        let (turn_id, channel_ref, conversation) = match request {
            ProgressiveTransportRequest::Send {
                turn_id,
                channel_ref,
                conversation,
                ..
            }
            | ProgressiveTransportRequest::Edit {
                turn_id,
                channel_ref,
                conversation,
                ..
            }
            | ProgressiveTransportRequest::Delete {
                turn_id,
                channel_ref,
                conversation,
                ..
            }
            | ProgressiveTransportRequest::Typing {
                turn_id,
                channel_ref,
                conversation,
                ..
            } => (turn_id, channel_ref, conversation),
            _ => panic!("expected a scoped progressive draft operation"),
        };
        assert_eq!(turn_id.as_deref(), Some("turn-1"));
        assert_eq!(
            channel_ref.as_ref().map(|value| value.id.as_str()),
            Some("channel-ref")
        );
        assert_eq!(
            conversation
                .as_ref()
                .map(|value| value.thread_id.as_deref()),
            Some(Some("thread-1"))
        );
    }
    drop(requests);
    replies.shutdown().await.unwrap();
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
        .execute(ProgressiveCommand::Feed {
            handle: handle.clone(),
            events: vec![ProgressiveEvent::TextDelta("x".into()); 33],
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("too many progressive events"));
    assert_eq!(
        replies
            .execute(ProgressiveCommand::Feed {
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
async fn thinking_limit_is_atomic_and_independent_of_text_budget() {
    let (replies, _) = fixture(false).await;
    let mut reply = ProgressiveReply::new(
        Arc::new(Sender::new(replies.host.clone())),
        "discord:fixture",
        tinychannels_bus::ChannelCapabilities::unknown(),
    );
    apply(
        &mut reply,
        vec![ProgressiveEvent::ThinkingDelta("x".repeat(MAX_CONTENT))],
    )
    .unwrap();
    assert!(
        apply(
            &mut reply,
            vec![
                ProgressiveEvent::TextDelta("not committed".into()),
                ProgressiveEvent::ThinkingDelta("extra".into())
            ]
        )
        .is_err()
    );
    assert!(reply.state().content.is_empty());
    assert_eq!(reply.state().thinking_accumulator.len(), MAX_CONTENT);
}

#[tokio::test]
async fn progressive_edits_and_deletes_preserve_target_and_message_id() {
    let (replies, callback) = fixture(false).await;
    let sender = Sender::new(replies.host.clone());
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
    let sender = Sender::new(replies.host.clone());
    for result in [
        ProgressiveTransportResult::Unavailable,
        ProgressiveTransportResult::EditUnsupported,
        ProgressiveTransportResult::MessageGone,
        ProgressiveTransportResult::Failed,
    ] {
        *callback.result.lock().await = Some(result.clone());
        let error = sender
            .edit("discord:room", "message-id", "updated")
            .await
            .unwrap_err();
        match result {
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
            ProgressiveTransportResult::Acknowledged
            | ProgressiveTransportResult::Accepted
            | ProgressiveTransportResult::Success(_)
            | ProgressiveTransportResult::Outcome(_)
            | ProgressiveTransportResult::UnsupportedContent => unreachable!(),
        }
    }
}
