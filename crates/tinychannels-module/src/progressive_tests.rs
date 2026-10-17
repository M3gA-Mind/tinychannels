use super::*;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tokio::sync::Notify;

struct ConfiguredChannel(&'static str);

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

#[derive(Clone, Default)]
struct Callback {
    sends: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<ProgressiveTransportRequest>>>,
    output_result: Arc<Mutex<Option<ProgressiveTransportResult>>>,
    send_result: Arc<Mutex<Option<ProgressiveTransportResult>>>,
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
        if typed_send || matches!(request, ProgressiveTransportRequest::Send { .. }) {
            if self.block {
                self.entered.notify_one();
                self.released.notified().await;
            }
        }
        let configured = if matches!(request, ProgressiveTransportRequest::OutputEvent { .. }) {
            self.output_result.lock().await.clone()
        } else {
            self.send_result.lock().await.clone()
        };
        let result = configured.unwrap_or_else(|| {
            if typed_send {
                let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
                outcome.receipt.platform_message_ids = vec!["fixture-message".into()];
                ProgressiveTransportResult::Outcome(outcome)
            } else if matches!(request, ProgressiveTransportRequest::OutputEvent { .. }) {
                ProgressiveTransportResult::Acknowledged
            } else {
                ProgressiveTransportResult::Success(serde_json::json!({"id": "fixture-message"}))
            }
        });
        Ok(ChannelBackendResult::ProgressiveTransport(result))
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
        matches!(request, ProgressiveTransportRequest::OutputEvent {
            event: ChannelOutputEvent::ToolCallCompleted { call_id, arguments, structured, .. },
            ..
        } if call_id == "call-1"
            && arguments.as_ref().is_some_and(|value| value["q"] == "safe test")
            && structured.as_ref().is_some_and(|value| value["count"] == 1))
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
    let error = replies
        .execute(ProgressiveCommand::FeedOutput {
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
async fn thinking_limit_is_atomic_and_independent_of_text_budget() {
    let (replies, _) = fixture(false).await;
    let mut reply = ProgressiveReply::new(
        Arc::new(Sender {
            host: replies.host.clone(),
            context: None,
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
    let error = replies
        .execute(ProgressiveCommand::FeedOutput {
            handle: handle.clone(),
            events: vec![ChannelOutputEvent::TextDelta {
                text: "already attempted".into(),
                iteration: Some(1),
            }],
        })
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("host output event callback failed")
    );
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
    *callback.send_result.lock().await = Some(ProgressiveTransportResult::Outcome(outcome.clone()));
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
    assert_eq!(result, ProgressiveResult::DeliveryOutcome { outcome });
    let sends = callback.sends.lock().await;
    assert_eq!(sends.len(), 1, "partial sends must not be retried");
    assert_eq!(sends[0]["text"], "caption");
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
        })
        .await
        .unwrap_err();
    assert!(forged.to_string().contains("not configured"));
    let handle = match replies
        .execute(ProgressiveCommand::OpenOutput {
            channel: channel_ref.clone(),
            conversation: conversation.clone(),
            turn_id: "turn-topic-9".into(),
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
        ..
    } = requests.last().unwrap()
    else {
        panic!("expected typed provider send")
    };
    assert_eq!(turn_id.as_deref(), Some("turn-topic-9"));
    assert_eq!(sent_channel.as_ref(), Some(&channel_ref));
    assert_eq!(sent_conversation.as_ref(), Some(&conversation));
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
