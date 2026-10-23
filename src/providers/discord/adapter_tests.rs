use super::*;
use crate::channel::SendErrorKind;
use crate::providers::discord::support::*;
use parking_lot::Mutex;
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::channel::{ChannelRef, ConversationRef};
use tinychannels_bus::{
    ChannelOutboundIntent, ChannelOutboundMessage, ContentPart, OutboundContentPart,
    OutboundPayload,
};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn intent(text: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "k".into(),
        channel_id: "discord".into(),
        conversation_id: CHANNEL.into(),
        channel_ref: Some(account()),
        conversation_ref: Some(conversation()),
        turn_id: Some("turn-1".into()),
        payload: OutboundPayload::Text { text: text.into() },
        ..Default::default()
    }
}

#[test]
fn capabilities_follow_the_services_the_host_supplied() {
    let bare = DiscordChannel::new("t".into(), None, None, vec![], false, false);
    let caps = bare.v2_capabilities();
    assert!(caps.content.text.outbound && caps.content.buttons.outbound);
    assert!(caps.content.presentation.outbound && caps.content.native.outbound);
    assert!(!caps.content.image.outbound && !caps.content.file.outbound);
    assert!(!caps.voice_in.supported && !caps.voice_out.supported);
    assert!(!caps.actions.supported.iter().any(|a| a == "sendAttachment"));
    assert_eq!(caps.text.text_limit, Some(2000));
    assert_eq!(caps.content.button_presentation.max_rows, Some(5));
    assert!(caps.reactions && caps.progressive_edits && caps.addressing.thread_id);
}

#[tokio::test]
async fn a_fully_wired_account_declares_media_voice_and_waveforms() {
    let server = MockServer::start().await;
    let (channel, ..) = full_channel(&server);
    let caps = channel.v2_capabilities();
    for content in [
        &caps.content.image,
        &caps.content.video,
        &caps.content.audio,
        &caps.content.file,
    ] {
        assert!(content.inbound && content.outbound);
        assert_eq!(content.max_size_bytes, Some(10 * 1024 * 1024));
    }
    assert!(caps.content.parts.iter().any(|p| p == "voice_note"));
    assert!(caps.voice_in.native_waveform && caps.voice_out.native_waveform);
    assert!(caps.voice_out.normalized_waveform && caps.voice_out.synthesis);
    assert_eq!(caps.voice_out.accepted_codecs, vec!["ogg_opus"]);
    assert!(caps.actions.supported.iter().any(|a| a == "sendAttachment"));
    for event in [
        "reaction_added",
        "reaction_removed",
        "message_updated",
        "message_deleted",
        "button_callback",
    ] {
        assert!(caps.events.supported.iter().any(|e| e == event), "{event}");
    }
}

#[tokio::test]
async fn adapter_capabilities_match_the_discord_provider_facts() {
    let server = MockServer::start().await;
    let (channel, ..) = full_channel(&server);
    let capabilities = ChannelAdapter::declared_capabilities(&channel);
    assert_eq!(capabilities, channel.v2_capabilities());
    assert!(capabilities.reactions);
    assert!(
        capabilities
            .actions
            .supported
            .iter()
            .any(|action| action == "edit")
    );
    assert!(capabilities.progressive_edits);
    assert!(
        capabilities
            .actions
            .supported
            .iter()
            .any(|action| action == "draft-update")
    );
    assert!(capabilities.addressing.reply_to_id && capabilities.addressing.thread_id);
}

#[tokio::test]
async fn the_adapter_describes_itself_without_starting_a_gateway() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path("/users/@me"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let channel = DiscordChannel::new("t".into(), None, Some(" 77 ".into()), vec![], false, false)
        .with_api_base(server.uri());

    let descriptor = ChannelAdapter::descriptor(&channel);
    assert_eq!(
        (descriptor.id.as_str(), descriptor.display_name.as_str()),
        ("discord", "Discord")
    );
    assert_eq!(
        descriptor.metadata.get("adapterMode").map(String::as_str),
        Some("native-v2")
    );
    assert_eq!(
        ChannelAdapter::proactive_target(&channel).as_deref(),
        Some("77")
    );
    assert_eq!(
        ChannelAdapter::declared_capabilities(&channel),
        channel.v2_capabilities()
    );

    let status = ChannelAdapter::status(&channel).await.unwrap();
    assert_eq!(status.channel_id, "discord");
    assert_eq!(status.health_state.as_deref(), Some("healthy"));
}

#[tokio::test]
async fn gateway_lookup_failures_are_mapped_and_release_listener_ownership() {
    struct Sink;
    #[async_trait::async_trait]
    impl ChannelInboundSink for Sink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _: tinychannels_bus::ChannelIngress,
        ) -> Result<(), ChannelSendError> {
            Ok(())
        }
    }

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path("/gateway/bot"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .expect(2)
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let sink = Sink;

    for _ in 0..2 {
        let error = ChannelAdapter::start(&channel, &sink)
            .await
            .expect_err("an invalid Gateway lookup response must fail startup");
        assert_eq!(error.kind, SendErrorKind::Unknown);
        assert!(error.retryable);
        assert!(
            error
                .message
                .starts_with("Discord Gateway listener failed:")
        );
        assert!(channel.listener_cancel.lock().is_none());
    }
    server.verify().await;
}

#[tokio::test]
async fn gateway_listener_cleans_up_after_aborted_start_and_restarts_once() {
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_tungstenite::tungstenite::Message;
    use wiremock::matchers::path;

    struct Sink;
    #[async_trait::async_trait]
    impl ChannelInboundSink for Sink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _: tinychannels_bus::ChannelIngress,
        ) -> Result<(), ChannelSendError> {
            Ok(())
        }
    }

    let rest = MockServer::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_url = format!("ws://{}", listener.local_addr().unwrap());
    Mock::given(method("GET"))
        .and(path("/gateway/bot"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"url": websocket_url})))
        .mount(&rest)
        .await;

    let sessions = Arc::new(AtomicUsize::new(0));
    let accepted = sessions.clone();
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(2);
    let gateway = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            accepted.fetch_add(1, Ordering::SeqCst);
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            websocket
                .send(Message::Text(
                    json!({"op": 10, "d": {"heartbeat_interval": 60_000}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let identify =
                tokio::time::timeout(std::time::Duration::from_secs(2), websocket.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&identify.to_string()).unwrap()["op"],
                2
            );
            ready_tx.send(()).await.unwrap();
            while websocket.next().await.is_some() {}
        }
    });

    let channel = Arc::new(channel_for(&rest));
    let sink = Arc::new(Sink);
    let first_channel = channel.clone();
    let first_sink = sink.clone();
    let first = tokio::spawn(async move {
        ChannelAdapter::start(first_channel.as_ref(), first_sink.as_ref()).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), ready_rx.recv())
        .await
        .expect("first Gateway session starts")
        .unwrap();

    let duplicate = ChannelAdapter::start(channel.as_ref(), sink.as_ref()).await;
    assert!(
        duplicate.is_err(),
        "a second Gateway session must be rejected"
    );

    // Aborting the caller drops the typed start future; its ownership guard
    // must release the active-session slot and close the Gateway connection.
    first.abort();
    let _ = first.await;

    let second_channel = channel.clone();
    let second_sink = sink.clone();
    let second = tokio::spawn(async move {
        ChannelAdapter::start(second_channel.as_ref(), second_sink.as_ref()).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), ready_rx.recv())
        .await
        .expect("the same configured channel can restart after caller cancellation")
        .unwrap();
    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), second)
        .await
        .expect("stop cancels and joins the second Gateway session")
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), gateway)
        .await
        .expect("both mock Gateway sessions close")
        .unwrap();
    assert_eq!(sessions.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn typed_gateway_delivers_the_original_rich_ingress_to_its_sink() {
    use futures_util::{SinkExt, StreamExt};
    use tinychannels_bus::{ChannelIngress, ContentPart};
    use tokio_tungstenite::tungstenite::Message;
    use wiremock::matchers::path;

    let rest = MockServer::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_url = format!("ws://{}", listener.local_addr().unwrap());
    Mock::given(method("GET"))
        .and(path("/gateway/bot"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"url": websocket_url})))
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/channels/{}",
            crate::providers::discord::support::THREAD
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": crate::providers::discord::support::THREAD,
            "type": 11,
            "parent_id": crate::providers::discord::support::CHANNEL,
        })))
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(path("/cdn/voice-message.ogg"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            crate::providers::discord::support::OGG_FIXTURE.to_vec(),
            "audio/ogg",
        ))
        .mount(&rest)
        .await;

    let rest_uri = rest.uri();
    let gateway = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.send(Message::Text(
            json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let identify = ws.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&identify.to_string()).unwrap()["op"],
            2
        );
        // The gateway timer emits an immediate heartbeat before dispatches.
        let initial_heartbeat = ws.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&initial_heartbeat.to_string()).unwrap(),
            json!({"op": 1, "d": null})
        );
        ws.send(Message::Text(json!({"op": 1, "s": 11}).to_string().into()))
            .await
            .unwrap();
        let heartbeat = ws.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&heartbeat.to_string()).unwrap(),
            json!({"op": 1, "d": 11})
        );
        ws.send(Message::Text(
            json!({"op": 0, "s": 11, "t": "THREAD_CREATE", "d": {
                "id": crate::providers::discord::support::THREAD,
                "type": 11,
                "parent_id": crate::providers::discord::support::CHANNEL
            }})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({
                "op": 0, "s": 12, "t": "MESSAGE_CREATE", "d": {
                    "id": "1111111111111111111",
                    "channel_id": crate::providers::discord::support::THREAD,
                    "guild_id": "999000333",
                    "content": "",
                    "flags": 8192,
                    "author": {"id": "42", "username": "alice"},
                    "message_reference": {"message_id": "2222222222222222222"},
                    "attachments": [{
                        "id": "5",
                        "url": format!("{rest_uri}/cdn/voice-message.ogg"),
                        "filename": "voice-message.ogg",
                        "content_type": "audio/ogg",
                        "duration_secs": 0.2,
                        "waveform": "AQID"
                    }]
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        while ws.next().await.is_some() {}
    });

    struct IngressSink(tokio::sync::mpsc::Sender<ChannelIngress>);
    #[async_trait::async_trait]
    impl ChannelInboundSink for IngressSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(&self, ingress: ChannelIngress) -> Result<(), ChannelSendError> {
            self.0
                .send(ingress)
                .await
                .map_err(|error| ChannelSendError {
                    kind: SendErrorKind::Unknown,
                    message: error.to_string(),
                    retryable: false,
                    ..Default::default()
                })
        }
    }

    let (ingress_tx, mut ingress_rx) = tokio::sync::mpsc::channel(1);
    let channel = Arc::new(
        DiscordChannel::new(
            crate::providers::discord::support::BOT_TOKEN.into(),
            None,
            None,
            vec![],
            false,
            false,
        )
        .with_api_base(rest.uri())
        .with_voice_media_store(Arc::new(
            crate::providers::discord::support::MemoryVoice::default(),
        ))
        .with_transcriber(Arc::new(
            crate::providers::discord::support::FixedTranscriber(Some("spoken words")),
        )),
    );
    let sink = Arc::new(IngressSink(ingress_tx));
    let channel_task = channel.clone();
    let sink_task = sink.clone();
    let listener_task = tokio::spawn(async move {
        ChannelAdapter::start(channel_task.as_ref(), sink_task.as_ref()).await
    });
    let ingress = tokio::time::timeout(std::time::Duration::from_secs(3), ingress_rx.recv())
        .await
        .expect("typed Gateway delivers ingress")
        .unwrap();
    let ChannelIngress::Inbound(envelope) = ingress else {
        panic!("expected the original inbound envelope");
    };
    assert!(envelope.raw.is_some());
    assert!(envelope.media.iter().any(|media| media.opaque_id.is_some()));
    assert!(
        matches!(envelope.parts.as_slice(), [ContentPart::VoiceNote { voice_note }] if voice_note.media.opaque_id.is_some() && voice_note.transcript.as_ref().is_some_and(|t| t.text == "spoken words"))
    );
    assert_eq!(envelope.reply_to_id.as_deref(), Some("2222222222222222222"));
    assert_eq!(
        envelope.conversation.thread_id.as_deref(),
        Some(crate::providers::discord::support::THREAD)
    );
    assert_eq!(
        envelope.conversation.parent_id.as_deref(),
        Some(crate::providers::discord::support::CHANNEL)
    );

    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), listener_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), gateway)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn blocked_typed_delivery_does_not_stop_gateway_heartbeat_handling() {
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::{Notify, oneshot};
    use tokio_tungstenite::tungstenite::Message;
    use wiremock::matchers::path;

    struct BlockingSink {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Notify,
    }
    #[async_trait::async_trait]
    impl ChannelInboundSink for BlockingSink {
        async fn push(
            &self,
            envelope: tinychannels_bus::ChannelInboundEnvelope,
        ) -> Result<(), ChannelSendError> {
            self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
                .await
        }

        async fn push_ingress(
            &self,
            _: tinychannels_bus::ChannelIngress,
        ) -> Result<(), ChannelSendError> {
            if let Some(entered) = self.entered.lock().take() {
                let _ = entered.send(());
            }
            self.release.notified().await;
            Ok(())
        }
    }

    let rest = MockServer::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_url = format!("ws://{}", listener.local_addr().unwrap());
    Mock::given(method("GET"))
        .and(path("/gateway/bot"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"url": websocket_url})))
        .mount(&rest)
        .await;
    let (heartbeat_tx, heartbeat_rx) = oneshot::channel();
    let (request_tx, request_rx) = oneshot::channel();
    let gateway = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.send(Message::Text(
            json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let _identify = ws.next().await.unwrap().unwrap();
        let _initial_heartbeat = ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            json!({
                "op": 0, "s": 20, "t": "MESSAGE_CREATE", "d": {
                    "id": "1111111111111111111", "channel_id": "555000111", "content": "hello",
                    "author": {"id": "42"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        request_rx.await.unwrap();
        ws.send(Message::Text(json!({"op": 1, "s": 21}).to_string().into()))
            .await
            .unwrap();
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next()).await;
        let responded = received
            .ok()
            .flatten()
            .and_then(Result::ok)
            .is_some_and(|frame| {
                serde_json::from_str::<serde_json::Value>(&frame.to_string())
                    .is_ok_and(|value| value == json!({"op": 1, "d": 21}))
            });
        let _ = heartbeat_tx.send(responded);
        while ws.next().await.is_some() {}
    });

    let channel = Arc::new(crate::providers::discord::support::channel_for(&rest));
    let (started_tx, started_rx) = oneshot::channel();
    let sink = Arc::new(BlockingSink {
        entered: Mutex::new(Some(started_tx)),
        release: Notify::new(),
    });
    let start_channel = channel.clone();
    let start_sink = sink.clone();
    let start = tokio::spawn(async move {
        ChannelAdapter::start(start_channel.as_ref(), start_sink.as_ref()).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), started_rx)
        .await
        .unwrap()
        .unwrap();
    request_tx.send(()).unwrap();
    let responded = tokio::time::timeout(std::time::Duration::from_secs(3), heartbeat_rx)
        .await
        .unwrap()
        .unwrap();
    sink.release.notify_one();
    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), start)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), gateway)
        .await
        .unwrap()
        .unwrap();
    assert!(
        responded,
        "Gateway op 1 heartbeat must be answered while sink delivery is pending"
    );
}

#[tokio::test]
async fn every_extension_trait_reaches_discord_through_the_adapter() {
    let server = MockServer::start().await;
    mount_message_post(&server, 600).await;
    for http in ["PATCH", "DELETE", "PUT", "POST"] {
        if http != "POST" {
            Mock::given(method(http))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "600"})))
                .mount(&server)
                .await;
        }
    }
    Mock::given(method("POST"))
        .and(wiremock::matchers::path(format!(
            "/channels/{CHANNEL}/typing"
        )))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let (channel, ..) = full_channel(&server);

    let sent = ChannelAdapter::send(&channel, intent("hello"))
        .await
        .unwrap();
    let typed = ChannelTypedSend::send_typed(
        &channel,
        &ChannelOutboundMessage::from_intent(&intent("typed")),
    )
    .await
    .unwrap();
    assert_eq!(typed.receipt.platform_message_ids.len(), 1);

    crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &account(),
        &conversation(),
        "600",
        &tinychannels_bus::ChannelReaction {
            value: "👍".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await
    .unwrap();
    ChannelEdit::edit(&channel, &sent, intent("edited"))
        .await
        .unwrap();
    ChannelTyping::start_typing(&channel, CHANNEL)
        .await
        .unwrap();
    ChannelTyping::stop_typing(&channel, CHANNEL).await.unwrap();
    let draft = ChannelStreamingDraft::upsert_draft(&channel, None, intent("draft"))
        .await
        .unwrap();
    ChannelStreamingDraft::upsert_draft(&channel, Some(&draft), intent("more"))
        .await
        .unwrap();
    ChannelDelete::delete(&channel, &sent).await.unwrap();
}

#[tokio::test]
async fn other_providers_cannot_drive_the_discord_extensions() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let slack = ChannelRef {
        id: "slack".into(),
        account_id: None,
    };

    let reaction = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &slack,
        &ConversationRef {
            id: CHANNEL.into(),
            ..Default::default()
        },
        "1",
        &tinychannels_bus::ChannelReaction {
            value: "x".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(reaction.kind, SendErrorKind::Forbidden);

    let mut foreign = intent("x");
    foreign.channel_ref = Some(slack);
    let draft = ChannelStreamingDraft::upsert_draft(&channel, None, foreign)
        .await
        .unwrap_err();
    assert_eq!(draft.kind, SendErrorKind::Forbidden);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_operations_preserve_errors_and_receipts() {
    let server = MockServer::start().await;
    mount_message_post(&server, 800).await;
    Mock::given(method("PATCH"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "800"})))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let message = ChannelOutboundMessage::from_intent(&intent("typed"));

    ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap();
    let outcome = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap();
    let receipt = outcome.receipt;
    let thumbs = tinychannels_bus::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &account(),
        &conversation(),
        "800",
        &thumbs,
        false,
    )
    .await
    .unwrap();
    crate::channel::ChannelEdit::edit(&channel, &receipt, intent("again"))
        .await
        .unwrap();
    crate::channel::ChannelDelete::delete(&channel, &receipt)
        .await
        .unwrap();

    let bad = ChannelOutboundMessage {
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Location {
                latitude: 0.0,
                longitude: 0.0,
                label: None,
                address: None,
                accuracy_meters: None,
                live_until_unix_ms: None,
            },
        }],
        ..message
    };
    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &bad)
        .await
        .unwrap_err();
    assert!(error.unsupported);
    let failed = crate::channel::ChannelEdit::edit(&channel, &receipt, intent(&"x".repeat(2001)))
        .await
        .unwrap_err();
    assert_eq!(failed.kind, SendErrorKind::TooLong);
    assert!(
        crate::channel::ChannelDelete::delete(&channel, &MessageReceipt::default())
            .await
            .is_err()
    );
    let empty = tinychannels_bus::ChannelReaction::default();
    assert!(
        crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &account(),
            &conversation(),
            "x",
            &empty,
            false
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn the_factory_wires_every_host_service_into_the_discord_account() {
    use crate::host::ChannelHostBuilder;
    let events = Arc::new(RecordingEvents::default());
    let host = ChannelHostBuilder::new()
        .events(events)
        .media_repository(Arc::new(MemoryMedia::default()))
        .voice_media_store(Arc::new(MemoryVoice::default()))
        .voice_codec(Arc::new(FixedCodec::new(false)))
        .transcriber(Arc::new(FixedTranscriber(Some("x"))))
        .build();
    let config = tinychannels_bus::ChannelsConfig {
        discord: Some(tinychannels_bus::config::DiscordConfig {
            bot_token: BOT_TOKEN.into(),
            guild_id: None,
            channel_id: Some(CHANNEL.into()),
            allowed_users: vec!["42".into()],
            listen_to_bots: false,
            mention_only: true,
        }),
        ..Default::default()
    };

    let built =
        crate::factory::build_discord_channel(&config, &host, &crate::DefaultHttpClients).unwrap();

    let caps = built.v2_capabilities();
    assert!(caps.content.image.outbound && caps.voice_in.supported);
    assert!(caps.voice_out.normalized_waveform);
    assert!(!caps.voice_out.synthesis, "no synthesizer was supplied");
    assert_eq!(
        ChannelAdapter::proactive_target(built.as_ref()).as_deref(),
        Some(CHANNEL)
    );
    assert!(
        crate::factory::build_discord_channel(
            &Default::default(),
            &host,
            &crate::DefaultHttpClients
        )
        .is_none()
    );
    assert_eq!(
        crate::build_channels(&config, &host, &crate::DefaultHttpClients).len(),
        1
    );
}
