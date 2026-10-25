use super::*;

#[tokio::test]
async fn text_goes_to_the_recipient_and_returns_the_signal_timestamp() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let outcome = send(&channel, &message(vec![text("hi")])).await.unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("1700000000123")
    );
    let sent = &calls(&server, "send").await[0];
    assert_eq!(sent["recipient"], json!([PEER]));
    assert_eq!(sent["message"], "hi");
    assert_eq!(sent["account"], ACCOUNT);
    let part = &outcome.receipt.parts[0];
    assert!(part.edit_token.is_some() && part.delete_token.is_some());
}

#[tokio::test]
async fn group_conversations_send_by_group_id() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let mut group = message(vec![text("hello team")]);
    group.conversation_ref = Some(conversation("group:abc"));
    send(&channel, &group).await.unwrap();
    assert_eq!(calls(&server, "send").await[0]["groupId"], "abc");
}

#[tokio::test]
async fn long_text_is_chunked_and_each_chunk_gets_a_receipt_part() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let long = "word ".repeat(1000);
    let outcome = send(&channel, &message(vec![text(&long)])).await.unwrap();
    assert!(outcome.receipt.parts.len() >= 3);
    assert_eq!(
        calls(&server, "send").await.len(),
        outcome.receipt.parts.len()
    );
}

#[tokio::test]
async fn replies_quote_the_target_message() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let mut reply = message(vec![text("re")]);
    reply.reply_to_id = Some("1700000000001".into());
    send(&channel, &reply).await.unwrap();
    let sent = &calls(&server, "send").await[0];
    assert_eq!(sent["quoteTimestamp"], 1_700_000_000_001u64);
    assert_eq!(sent["quoteAuthor"], PEER);

    let mut bad = message(vec![text("re")]);
    bad.reply_to_id = Some("not-a-timestamp".into());
    assert!(send(&channel, &bad).await.is_err());
}

#[tokio::test]
async fn threads_are_unsupported_before_any_request() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let mut threaded = message(vec![text("x")]);
    threaded.thread_id = Some("t".into());
    let error = send(&channel, &threaded).await.unwrap_err();
    assert!(error.unsupported);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn images_are_sent_as_data_uri_attachments_with_the_caption() {
    let server = mock_daemon(b"").await;
    let rig = rig(&server);
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    let outcome = send(
        &rig.channel,
        &message(vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: media_ref(MediaKind::Image),
                caption: Some("look; here, now".into()),
                alt_text: None,
            },
        }]),
    )
    .await
    .unwrap();
    let sent = &calls(&server, "send").await[0];
    let uri = sent["attachments"][0].as_str().unwrap();
    assert!(uri.starts_with("data:image/png;filename=report.pdf;base64,"));
    assert_eq!(sent["message"], "look; here, now");
    assert_eq!(outcome.receipt.parts[0].kind, MessageReceiptPartKind::Media);
    assert!(outcome.receipt.parts[0].edit_token.is_none());
    assert_eq!(rig.media.scopes.lock().unwrap()[0].turn_id, "turn-1");
}

#[tokio::test]
async fn voice_clips_are_audio_attachments_captioned_with_the_transcript() {
    let server = mock_daemon(b"").await;
    let rig = rig(&server);
    let outcome = send(
        &rig.channel,
        &message(vec![
            text("the transcript"),
            OutboundContentPart::Voice {
                voice: OutboundVoice::Media {
                    media: media_ref(MediaKind::Audio),
                    codec: None,
                    duration_ms: None,
                },
            },
        ]),
    )
    .await
    .unwrap();
    let sent = &calls(&server, "send").await[0];
    assert!(
        sent["attachments"][0]
            .as_str()
            .unwrap()
            .starts_with("data:audio/mp4;filename=reply.m4a;base64,")
    );
    assert_eq!(sent["message"], "the transcript");
    assert_eq!(outcome.receipt.parts[0].kind, MessageReceiptPartKind::Voice);
}

#[tokio::test]
async fn a_failing_second_chunk_reports_a_partial_send() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::path("/api/v1/rpc"))
        .respond_with(SequencedRpc::default())
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let outcome = send(&channel, &message(vec![text(&"word ".repeat(1000))]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.parts.len(), 1);
    assert!(outcome.error.unwrap().sent_before_error);

    // A failure on the very first request is a plain error.
    let dead = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::path("/api/v1/rpc"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
            "error": {"code": -1, "message": "Rate limit exceeded"}
        })))
        .mount(&dead)
        .await;
    let error = send(&channel_for(&dead), &message(vec![text("x")]))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::RateLimited);
}

/// Answers the first `send` with a timestamp and every later call with an error.
#[derive(Default)]
struct SequencedRpc(std::sync::atomic::AtomicUsize);

impl wiremock::Respond for SequencedRpc {
    fn respond(&self, _: &wiremock::Request) -> wiremock::ResponseTemplate {
        if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            wiremock::ResponseTemplate::new(200)
                .set_body_json(json!({"result": {"timestamp": SENT_TS}}))
        } else {
            wiremock::ResponseTemplate::new(200)
                .set_body_json(json!({"error": {"code": -1, "message": "later failure"}}))
        }
    }
}

#[tokio::test]
async fn a_send_without_a_timestamp_is_an_uncertain_failure() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::path("/api/v1/rpc"))
        .respond_with(wiremock::ResponseTemplate::new(201))
        .mount(&server)
        .await;
    let error = send(&channel_for(&server), &message(vec![text("x")]))
        .await
        .unwrap_err();
    assert!(error.sent_before_error);
}

#[tokio::test]
async fn typing_starts_and_stops() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    ChannelTyping::start_typing(&channel, PEER).await.unwrap();
    ChannelTyping::stop_typing(&channel, "group:abc")
        .await
        .unwrap();
    let typing = calls(&server, "sendTyping").await;
    assert!(typing[0].get("stop").is_none());
    assert_eq!(typing[1]["stop"], true);
    assert_eq!(typing[1]["groupId"], "abc");
}

#[tokio::test]
async fn reactions_name_the_target_author_and_timestamp() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let channel_ref = ChannelRef {
        id: "signal".into(),
        account_id: None,
    };
    let thumbs = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    ReactionPort::send_reaction(
        &channel,
        &channel_ref,
        &conversation(PEER),
        "1700000000001",
        &thumbs,
        false,
    )
    .await
    .unwrap();
    ReactionPort::send_reaction(
        &channel,
        &channel_ref,
        &conversation("group:abc"),
        "+15552222:1700000000002",
        &thumbs,
        true,
    )
    .await
    .unwrap();
    let reactions = calls(&server, "sendReaction").await;
    assert_eq!(reactions[0]["targetAuthor"], PEER);
    assert_eq!(reactions[0]["targetTimestamp"], 1_700_000_000_001u64);
    assert_eq!(reactions[0]["remove"], false);
    assert_eq!(reactions[1]["targetAuthor"], "+15552222");
    assert_eq!(reactions[1]["groupId"], "abc");
    assert_eq!(reactions[1]["remove"], true);

    let blank = tinychannels_bus::channel::ChannelReaction {
        value: " ".into(),
        custom_emoji_id: None,
    };
    assert!(
        channel
            .react(&conversation(PEER), "1", &blank, false)
            .await
            .is_err()
    );
    let other = ChannelRef {
        id: "slack".into(),
        account_id: None,
    };
    let forbidden =
        ReactionPort::send_reaction(&channel, &other, &conversation(PEER), "1", &thumbs, false)
            .await
            .unwrap_err();
    assert_eq!(forbidden.kind, SendErrorKind::Forbidden);
}

#[test]
fn message_targets_resolve_authors_by_conversation() {
    let channel = channel_for_url();
    assert_eq!(
        channel.message_target(PEER, "sig_1700000000001").unwrap(),
        (PEER.to_owned(), 1_700_000_000_001)
    );
    assert_eq!(
        channel
            .message_target("group:abc", "1700000000001")
            .unwrap(),
        (ACCOUNT.to_owned(), 1_700_000_000_001)
    );
    assert_eq!(
        channel
            .message_target("group:abc", "+1555:1700000000001")
            .unwrap(),
        ("+1555".to_owned(), 1_700_000_000_001)
    );
    assert!(channel.message_target(PEER, "abc").is_err());
}

fn channel_for_url() -> SignalChannel {
    SignalChannel::new(
        "http://127.0.0.1:1".into(),
        ACCOUNT.into(),
        None,
        vec!["*".into()],
        false,
        false,
    )
}

#[tokio::test]
async fn edits_and_deletes_target_the_original_timestamp() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let sent = receipt("1700000000123", PEER);
    let edited = ChannelEdit::edit(&channel, &sent, edit_intent("fixed"))
        .await
        .unwrap();
    assert_eq!(edited, sent);
    ChannelDelete::delete(&channel, &sent).await.unwrap();
    let edit = &calls(&server, "send").await[0];
    assert_eq!(edit["editTimestamp"], 1_700_000_000_123u64);
    assert_eq!(edit["message"], "fixed");
    assert_eq!(
        calls(&server, "remoteDelete").await[0]["targetTimestamp"],
        1_700_000_000_123u64
    );

    let bad = receipt("abc", PEER);
    assert!(ChannelDelete::delete(&channel, &bad).await.is_err());
    assert!(
        ChannelEdit::edit(&channel, &bad, edit_intent("x"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unsupported_content_never_reaches_the_daemon() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    for payload in [
        OutboundPayload::Poll {
            question: "q".into(),
            options: vec!["a".into()],
        },
        OutboundPayload::NativeChannelData { data: json!({}) },
    ] {
        let mut intent = edit_intent("x");
        intent.payload = payload;
        let error = ChannelAdapter::send(&channel, intent).await.unwrap_err();
        assert!(error.unsupported);
    }
    assert!(
        crate::channel::ChannelStreamingDraft::upsert_draft(&channel, None, edit_intent("x"))
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_provider_ports_send_and_preserve_typed_errors() {
    let server = mock_daemon(b"").await;
    let channel = channel_for(&server);
    let outcome = ChannelTypedSend::send_typed(&channel, &message(vec![text("typed")]))
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["1700000000123"]);
    let thumbs = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    ReactionPort::send_reaction(
        &channel,
        &ChannelRef {
            id: "signal".into(),
            account_id: None,
        },
        &conversation(PEER),
        "1700000000001",
        &thumbs,
        false,
    )
    .await
    .unwrap();
    let sent = receipt("1700000000123", PEER);
    ChannelEdit::edit(&channel, &sent, edit_intent("e"))
        .await
        .unwrap();
    ChannelDelete::delete(&channel, &sent).await.unwrap();

    let error = ChannelTypedSend::send_typed(
        &channel,
        &message(vec![OutboundContentPart::Content {
            part: ContentPart::Poll {
                question: "q".into(),
                options: vec![],
                multiple_choice: false,
                closes_at_unix_ms: None,
                provider_poll_id: None,
            },
        }]),
    )
    .await
    .unwrap_err();
    assert!(error.unsupported);
    assert!(error.message.contains("content kind"));
}

#[tokio::test]
async fn rpc_failures_map_onto_typed_errors() {
    let dead = channel_for_url();
    let transport = dead.rpc_request("send", json!({})).await.unwrap_err();
    let mapped = rpc_error(&transport, "send");
    assert_eq!(mapped.kind, SendErrorKind::Transient);
    assert!(mapped.sent_before_error);

    let unregistered = anyhow::anyhow!("Signal RPC error -1: Unregistered user");
    assert_eq!(
        rpc_error(&unregistered, "send").kind,
        SendErrorKind::NotFound
    );
    let rejected = anyhow::anyhow!("Signal RPC error -32602: invalid");
    assert_eq!(rpc_error(&rejected, "send").kind, SendErrorKind::Unknown);
    let unreadable = anyhow::anyhow!("expected value at line 1");
    assert!(rpc_error(&unreadable, "send").sent_before_error);
}

#[test]
fn the_descriptor_and_defaults_identify_the_native_adapter() {
    let channel = channel_for_url();
    let descriptor = ChannelAdapter::descriptor(&channel);
    assert_eq!(descriptor.id, "signal");
    assert_eq!(descriptor.metadata["adapterMode"], "native-v2");
    assert_eq!(descriptor.metadata["voiceOut"], "attachment");
    assert_eq!(NativeProvider::proactive_target(&channel), None);
    let grouped = SignalChannel::new(
        "http://127.0.0.1:1".into(),
        ACCOUNT.into(),
        Some("abc".into()),
        vec![],
        false,
        false,
    );
    assert_eq!(
        ChannelAdapter::proactive_target(&grouped).as_deref(),
        Some("group:abc")
    );
    let dm_only = SignalChannel::new(
        "http://127.0.0.1:1".into(),
        ACCOUNT.into(),
        Some("dm".into()),
        vec![],
        false,
        false,
    );
    assert_eq!(NativeProvider::proactive_target(&dm_only), None);
    assert!(channel.declared().reactions);
    assert!(channel.declared().addressing.reply_to_id);
    assert!(!channel.declared().addressing.thread_id);
    assert!(
        channel
            .declared()
            .actions
            .supported
            .iter()
            .any(|action| action == "edit")
    );
}

#[tokio::test]
async fn lifecycle_hooks_are_owned_by_the_module_runtime() {
    struct NoSink;
    #[async_trait::async_trait]
    impl crate::channel::ChannelInboundSink for NoSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _: tinychannels_bus::ChannelIngress,
        ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
            Ok(())
        }
    }
    let channel = Arc::new(channel_for_url());
    let sink = Arc::new(NoSink);
    let listener_channel = channel.clone();
    let listener_sink = sink.clone();
    let listener = tokio::spawn(async move {
        ChannelAdapter::start(listener_channel.as_ref(), listener_sink.as_ref()).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if channel.listener_cancel.lock().unwrap().as_ref().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener should register before stop");
    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), listener)
        .await
        .expect("module stop should cancel the provider listener")
        .unwrap()
        .unwrap();
    assert_eq!(
        ChannelAdapter::status(channel.as_ref())
            .await
            .unwrap()
            .channel_id,
        "signal"
    );
}
