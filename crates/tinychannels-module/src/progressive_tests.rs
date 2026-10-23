use super::*;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tokio::sync::Notify;

#[derive(Clone, Default)]
struct Callback {
    sends: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<ProgressiveTransportRequest>>>,
    result: Arc<Mutex<Option<ProgressiveTransportResult>>>,
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
        if let ProgressiveTransportRequest::Send { message, .. } = *request {
            self.sends.lock().await.push(message);
            if self.block {
                self.entered.notify_one();
                self.released.notified().await;
            }
        }
        Ok(ChannelBackendResult::ProgressiveTransport(Box::new(
            self.result.lock().await.clone().unwrap_or_else(|| {
                ProgressiveTransportResult::Success(serde_json::json!({"id": "fixture-message"}))
            }),
        )))
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
        Arc::new(Sender(replies.host.clone())),
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
async fn completed_replies_are_evicted_when_the_retained_handle_limit_is_reached() {
    let (replies, callback) = fixture(false).await;
    let mut first_handle = None;
    for index in 0..MAX_RETAINED {
        let handle = open(&replies).await;
        if index == 0 {
            first_handle = Some(handle.clone());
        }
        replies
            .execute(ProgressiveCommand::Finish {
                handle,
                text: format!("completed reply {index}"),
            })
            .await
            .unwrap();
    }
    assert_eq!(replies.state.lock().await.entries.len(), MAX_RETAINED);

    let newest = open(&replies).await;
    let entries = replies.state.lock().await.entries;
    assert_eq!(entries.len(), MAX_RETAINED);
    assert!(!entries.contains_key(first_handle.as_deref().unwrap()));
    assert!(entries.contains_key(&newest));
    drop(entries);
    replies.shutdown().await.unwrap();
    assert_eq!(callback.sends.lock().await.len(), MAX_RETAINED + 1);
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
        Arc::new(Sender(replies.host.clone())),
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
    let sender = Sender(replies.host.clone());
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
    let sender = Sender(replies.host.clone());
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

#[tokio::test]
async fn scoped_output_preserves_acknowledgement_receipt_and_unsupported_content_results() {
    use tinychannels_bus::module_progressive::{ChannelOutputEvent, ProgressiveOutputFailure};

    let (replies, callback) = fixture(false).await;
    let scope = OutputScope {
        channel: tinychannels_bus::ChannelRef {
            id: "discord".into(),
            account_id: Some("discord:fixture".into()),
        },
        conversation: tinychannels_bus::ConversationRef {
            id: "room-1".into(),
            ..Default::default()
        },
        turn_id: "turn-1".into(),
        lifetime: TURN_LIFETIME,
    };
    let sequence = AtomicU64::new(0);
    let text_delta = || ChannelOutputEvent::TextDelta {
        text: "partial".into(),
        iteration: Some(1),
    };

    *callback.result.lock().await = Some(ProgressiveTransportResult::Acknowledged);
    assert_eq!(
        feed_output(
            &replies.host,
            "discord:fixture",
            Some(&scope),
            &sequence,
            vec![text_delta()]
        )
        .await
        .unwrap(),
        ProgressiveResult::Updated
    );

    *callback.result.lock().await = Some(ProgressiveTransportResult::Accepted);
    assert!(matches!(
        feed_output(
            &replies.host,
            "discord:fixture",
            Some(&scope),
            &sequence,
            vec![text_delta()]
        )
        .await
        .unwrap(),
        ProgressiveResult::DeliveryBatch {
            hosted_accepted: true,
            failure: None,
            ..
        }
    ));

    let outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
    *callback.result.lock().await = Some(ProgressiveTransportResult::Outcome(Box::new(
        outcome.clone(),
    )));
    assert_eq!(
        feed_output(
            &replies.host,
            "discord:fixture",
            Some(&scope),
            &sequence,
            vec![text_delta()]
        )
        .await
        .unwrap(),
        ProgressiveResult::DeliveryOutcome {
            outcome: Box::new(outcome.clone())
        }
    );

    *callback.result.lock().await = Some(ProgressiveTransportResult::Success(
        serde_json::to_value(outcome.clone()).unwrap(),
    ));
    assert_eq!(
        feed_output(
            &replies.host,
            "discord:fixture",
            Some(&scope),
            &sequence,
            vec![text_delta()],
        )
        .await
        .unwrap(),
        ProgressiveResult::DeliveryOutcome {
            outcome: Box::new(outcome)
        }
    );

    *callback.result.lock().await = Some(ProgressiveTransportResult::UnsupportedContent);
    let part = tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: Default::default(),
            caption: None,
            alt_text: Some("chart".into()),
        },
    };
    assert_eq!(
        feed_output(
            &replies.host,
            "discord:fixture",
            Some(&scope),
            &sequence,
            vec![ChannelOutputEvent::FinalMessage {
                text: "summary".into(),
                parts: vec![part.clone()],
                receipt: None,
            }],
        )
        .await
        .unwrap(),
        ProgressiveResult::DeliveryUnsupported { parts: vec![part] }
    );

    assert!(matches!(
        feed_output(
            &replies.host,
            "discord:fixture",
            Some(&scope),
            &sequence,
            vec![text_delta()]
        )
        .await
        .unwrap(),
        ProgressiveResult::DeliveryBatch {
            failure: Some(ProgressiveOutputFailure::ProviderTransport),
            hosted_accepted: false,
            ..
        }
    ));
    replies.shutdown().await.unwrap();
}

#[tokio::test]
async fn hosted_output_lifecycle_carries_turn_scope_and_finishes_through_host_transport() {
    use tinychannels_bus::module_progressive::ChannelOutputEvent;

    let (replies, callback) = fixture(false).await;
    let channel = tinychannels_bus::ChannelRef {
        id: "discord".into(),
        account_id: Some("discord:fixture".into()),
    };
    let conversation = tinychannels_bus::ConversationRef {
        id: "room-1".into(),
        thread_id: Some("thread-2".into()),
        ..Default::default()
    };
    let ProgressiveResult::Opened { handle } = replies
        .execute(ProgressiveCommand::OpenHostedOutput {
            channel: channel.clone(),
            conversation: conversation.clone(),
            turn_id: "turn-9".into(),
            lifetime_ms: 30_000,
        })
        .await
        .unwrap()
    else {
        panic!("hosted output should open a scoped reply");
    };

    *callback.result.lock().await = Some(ProgressiveTransportResult::Acknowledged);
    assert_eq!(
        replies
            .execute(ProgressiveCommand::FeedOutput {
                handle: handle.clone(),
                events: vec![ChannelOutputEvent::TextDelta {
                    text: "partial".into(),
                    iteration: Some(1),
                }],
            })
            .await
            .unwrap(),
        ProgressiveResult::Updated
    );
    *callback.result.lock().await = Some(ProgressiveTransportResult::Success(
        serde_json::json!({"id": "fixture-message"}),
    ));
    assert_eq!(
        replies
            .execute(ProgressiveCommand::Finish {
                handle,
                text: "complete".into(),
            })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );

    let requests = callback.requests.lock().await;
    assert!(requests.iter().any(|request| matches!(
        request,
        ProgressiveTransportRequest::OutputEvent {
            channel: request_channel,
            turn_id: Some(turn_id),
            channel_ref: Some(request_scope),
            conversation: Some(request_conversation),
            sequence: 0,
            ..
        } if request_channel == "discord" && turn_id == "turn-9" && request_scope == &channel && request_conversation == &conversation
    )));
    assert_eq!(callback.sends.lock().await[0]["text"], "complete");
    replies.shutdown().await.unwrap();
}
