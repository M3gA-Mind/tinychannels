//! Shared conformance checks against the factory-built Yuanbao adapter.

use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction, ChannelStreamingDraft,
    ChannelTyping,
};
use crate::factory::build_yuanbao_channel_with_voice_http_for_test;
use crate::host::ChannelHostBuilder;
use crate::providers::http_test_support::HttpsServer;
use crate::providers::native::fixtures::{StubTranscriber, StubVoice};
use crate::providers::yuanbao::connection::test_support::{
    GatewaySession, MockGateway, gateway_config,
};
use async_trait::async_trait;
use base64::Engine as _;
use parking_lot::Mutex;
use std::sync::Arc;
use tinychannels_bus::channel::{
    ChannelRef, ChannelSendError, ChannelSendOutcome, ConversationKind, ConversationRef,
    MessageReceipt,
};
use tinychannels_bus::{
    ChannelOutboundIntent, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    OutboundPayload, OutboundVoice,
};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature as Feature, ExpectedScope, WireObservation,
    check_all_features,
};

const GROUP: &str = "g:group-42";
const BOT: &str = "bot1";

struct Fixture {
    channel: Arc<dyn crate::providers::long_tail::LongTailNative>,
    _media_server: HttpsServer,
    _gateway: MockGateway,
    session: tokio::sync::Mutex<GatewaySession>,
    listener: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), ChannelSendError>>>>,
    inbound:
        tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<tinychannels_bus::ChannelIngress>>,
    voice_store: Arc<StubVoice>,
    observations: Mutex<Vec<WireObservation>>,
    active_feature: Mutex<Feature>,
}

impl Fixture {
    async fn start() -> Self {
        let audio = include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg").to_vec();
        let response_audio = audio.clone();
        let media_server = HttpsServer::start_with_bytes(
            move |request| {
                assert_eq!(request.method, "GET");
                assert_eq!(request.target, "/voice.ogg");
                (200, response_audio.clone())
            },
            "audio/ogg",
        )
        .await;
        let gateway = MockGateway::bind().await;
        let voice_store = Arc::new(StubVoice::default());
        *voice_store.audio_base64.lock().unwrap() =
            Some(base64::engine::general_purpose::STANDARD.encode(b"voice fixture"));
        let config = tinychannels_bus::ChannelsConfig {
            yuanbao: Some(gateway_config(&gateway.url)),
            ..Default::default()
        };
        let host: Arc<dyn crate::host::ChannelHost> = ChannelHostBuilder::new()
            .voice_media_store(voice_store.clone())
            .transcriber(Arc::new(StubTranscriber::default()))
            .build();
        let channel = build_yuanbao_channel_with_voice_http_for_test(
            &config,
            &host,
            media_server.client.clone(),
        )
        .expect("factory should build configured Yuanbao adapter");
        let (ingress_tx, ingress_rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = IngressSink(ingress_tx);
        let listener_channel = channel.clone();
        let listener =
            tokio::spawn(
                async move { ChannelAdapter::start(listener_channel.as_ref(), &sink).await },
            );
        let session = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            gateway.accept_authed("conformance-connect"),
        )
        .await
        .expect("factory-built adapter should connect to local gateway");
        Self {
            channel,
            _media_server: media_server,
            _gateway: gateway,
            session: tokio::sync::Mutex::new(session),
            listener: tokio::sync::Mutex::new(Some(listener)),
            inbound: tokio::sync::Mutex::new(ingress_rx),
            voice_store,
            observations: Mutex::new(Vec::new()),
            active_feature: Mutex::new(Feature::Text),
        }
    }

    fn intent(&self, feature: Feature, payload: OutboundPayload) -> ChannelOutboundIntent {
        ChannelOutboundIntent {
            idempotency_key: format!("yuanbao-{feature:?}"),
            channel_id: "yuanbao".into(),
            conversation_id: GROUP.into(),
            channel_ref: Some(channel_ref()),
            conversation_ref: Some(group_ref()),
            reply_to_id: (feature == Feature::ReplyAddressing).then(|| "previous-message".into()),
            turn_id: Some("yuanbao-conformance-turn".into()),
            payload,
            ..Default::default()
        }
    }

    async fn send_and_record(
        &self,
        feature: Feature,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        *self.active_feature.lock() = feature;
        let send = {
            let channel = self.channel.clone();
            let intent = self.intent(feature, payload);
            tokio::spawn(async move { ChannelAdapter::send(channel.as_ref(), intent).await })
        };
        let mut session = self.session.lock().await;
        let request = tokio::time::timeout(std::time::Duration::from_secs(3), session.recv_frame())
            .await
            .expect("declared send must reach the mock gateway");
        assert_eq!(
            request.cmd,
            crate::providers::yuanbao::proto_constants::biz_cmd::SEND_GROUP_MESSAGE
        );
        let fields = crate::providers::yuanbao::wire::parse_fields(&request.data)
            .expect("outgoing group request must decode");
        let group_id = crate::providers::yuanbao::wire::get_string(&fields, 2);
        let provider_id = crate::providers::yuanbao::wire::get_string(&fields, 3);
        let reply_to_id = crate::providers::yuanbao::wire::get_string(&fields, 7);
        let elements = crate::providers::yuanbao::wire::get_repeated_bytes(&fields, 6);
        let payload_kinds = elements
            .iter()
            .map(|element| {
                let fields = crate::providers::yuanbao::wire::parse_fields(element).unwrap();
                match crate::providers::yuanbao::wire::get_string(&fields, 1).as_str() {
                    "TIMTextElem" => "text".to_owned(),
                    other => other.to_owned(),
                }
            })
            .collect();
        let feature = *self.active_feature.lock();
        self.observations.lock().push(WireObservation {
            feature,
            operation: "send".into(),
            channel: ChannelRef {
                id: "yuanbao".into(),
                account_id: Some(provider_id.clone()),
            },
            conversation: ConversationRef {
                kind: ConversationKind::Group,
                id: format!("g:{group_id}"),
                ..Default::default()
            },
            reply_to_id: (!reply_to_id.is_empty()).then_some(reply_to_id),
            thread_id: None,
            payload_kinds,
            platform_message_ids: vec![request.msg_id.clone()],
            request: serde_json::json!({
                "command": request.cmd,
                "message_id": request.msg_id,
                "group_code": group_id,
                "from_account": provider_id,
                "reply_to_id": crate::providers::yuanbao::wire::get_string(&fields, 7),
            }),
        });
        session.reply(&request, 0, &[]).await;
        drop(session);
        let outcome = send.await.expect("send task should not panic")?;
        assert_eq!(outcome.receipt.platform_message_ids, vec![request.msg_id]);
        Ok(Some(outcome))
    }

    async fn refuse_send(
        &self,
        feature: Feature,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        *self.active_feature.lock() = feature;
        let result =
            ChannelAdapter::send(self.channel.as_ref(), self.intent(feature, payload)).await;
        assert_no_wire_request(&self.session).await;
        result.map(|_| None)
    }

    async fn voice_in(&self) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        *self.active_feature.lock() = Feature::VoiceIn;
        let mut session = self.session.lock().await;
        let push = crate::providers::yuanbao::connection::test_support::encode_frame(
            crate::providers::yuanbao::proto_constants::cmd_type::PUSH,
            "inbound_message",
            "voice-in-1",
            crate::providers::yuanbao::proto_constants::module::BIZ_PKG,
            true,
            0,
            &serde_json::to_vec(&serde_json::json!({
                "from_account":"alice",
                "msg_id":"voice-in-1",
                "msg_time":1_700_000_000,
                "sender_nickname":"Alice",
                "msg_body":[{"msg_type":"TIMSoundElem","msg_content":{
                    "sound":"sound-id",
                    "url":"https://api.dingtalk.com/voice.ogg",
                    "file_name":"voice.ogg"
                }}]
            }))
            .unwrap(),
        );
        session
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                push.clone().into(),
            ))
            .await;
        let ack = tokio::time::timeout(std::time::Duration::from_secs(3), session.recv_frame())
            .await
            .expect("gateway should receive a push acknowledgement");
        assert_eq!(
            ack.cmd_type,
            crate::providers::yuanbao::proto_constants::cmd_type::PUSH_ACK
        );
        assert_eq!(ack.msg_id, "voice-in-1");
        drop(session);

        let ingress = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            self.inbound.lock().await.recv(),
        )
        .await
        .expect("voice ingress should reach host sink")
        .expect("host sink should stay open");
        let tinychannels_bus::ChannelIngress::Inbound(envelope) = ingress else {
            panic!("voice fixture should emit inbound envelope");
        };
        assert_eq!(envelope.channel.id, "yuanbao");
        assert_eq!(envelope.channel.account_id.as_deref(), Some(BOT));
        assert_eq!(envelope.conversation.kind, ConversationKind::Dm);
        assert_eq!(envelope.conversation.id, "alice");
        let [ContentPart::VoiceNote { voice_note }] = envelope.parts.as_slice() else {
            panic!("voice ingress should remain one typed voice note");
        };
        assert_eq!(voice_note.media.opaque_id.as_deref(), Some("voice-1"));
        assert_eq!(voice_note.media.kind, MediaKind::Audio);
        assert!(voice_note.media.path.is_none());
        assert!(voice_note.media.url.is_none());
        assert!(voice_note.media.local_cache_path.is_none());
        let transcript = voice_note.transcript.as_ref().expect("host transcript");
        assert_eq!(transcript.text, "hello");
        assert_eq!(transcript.language.as_deref(), Some("en"));
        let observed_push: serde_json::Value = serde_json::from_slice(
            &crate::providers::yuanbao::proto::decode_conn_msg(&push)
                .unwrap()
                .data,
        )
        .unwrap();
        self.observations.lock().push(WireObservation {
            feature: Feature::VoiceIn,
            operation: "voice_in".into(),
            channel: envelope.channel,
            conversation: envelope.conversation,
            reply_to_id: envelope.reply_to_id,
            thread_id: None,
            payload_kinds: vec!["audio".into(), "transcript".into()],
            platform_message_ids: vec![],
            request: observed_push,
        });
        let stored = self.voice_store.stored.lock().unwrap();
        assert_eq!(stored.len(), 1);
        Ok(None)
    }
}

struct IngressSink(tokio::sync::mpsc::UnboundedSender<tinychannels_bus::ChannelIngress>);

#[async_trait]
impl crate::channel::ChannelInboundSink for IngressSink {
    async fn push(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), ChannelSendError> {
        self.0.send(ingress).map_err(|_| ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Transient,
            message: "conformance ingress receiver closed".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

fn channel_ref() -> ChannelRef {
    ChannelRef {
        id: "yuanbao".into(),
        account_id: Some(BOT.into()),
    }
}

fn group_ref() -> ConversationRef {
    ConversationRef {
        kind: ConversationKind::Group,
        id: GROUP.into(),
        ..Default::default()
    }
}

fn media(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some(format!("fixture-{kind:?}")),
        kind,
        ..Default::default()
    }
}

fn content(part: ContentPart) -> OutboundPayload {
    OutboundPayload::Parts {
        parts: vec![OutboundContentPart::Content { part }],
    }
}

#[async_trait]
impl AdapterFixture for Fixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        self.channel.as_ref()
    }

    fn expected_scope(&self, feature: Feature) -> ExpectedScope {
        if feature == Feature::VoiceIn {
            ExpectedScope {
                channel: channel_ref(),
                conversation: ConversationRef {
                    kind: ConversationKind::Dm,
                    id: "alice".into(),
                    ..Default::default()
                },
                reply_to_id: None,
                thread_id: None,
            }
        } else {
            ExpectedScope {
                channel: channel_ref(),
                conversation: group_ref(),
                reply_to_id: (feature == Feature::ReplyAddressing)
                    .then(|| "previous-message".into()),
                thread_id: None,
            }
        }
    }

    fn clear_observations(&self) {
        self.observations.lock().clear();
    }

    fn observations(&self) -> Vec<WireObservation> {
        self.observations.lock().clone()
    }

    async fn exercise(
        &self,
        feature: Feature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        match feature {
            Feature::Text => {
                self.send_and_record(
                    feature,
                    OutboundPayload::Text {
                        text: "conformance".into(),
                    },
                )
                .await
            }
            Feature::ReplyAddressing => {
                self.send_and_record(
                    feature,
                    OutboundPayload::Text {
                        text: "reply".into(),
                    },
                )
                .await
            }
            Feature::Typing => {
                ChannelTyping::start_typing(self.channel.as_ref(), GROUP).await?;
                let mut session = self.session.lock().await;
                let request =
                    tokio::time::timeout(std::time::Duration::from_secs(3), session.recv_frame())
                        .await
                        .expect("typing heartbeat should reach the gateway");
                assert_eq!(
                    request.cmd,
                    crate::providers::yuanbao::proto_constants::biz_cmd::SEND_GROUP_HEARTBEAT
                );
                let fields = crate::providers::yuanbao::wire::parse_fields(&request.data).unwrap();
                let group = crate::providers::yuanbao::wire::get_string(&fields, 3);
                assert_eq!(group, "group-42");
                self.observations.lock().push(WireObservation {
                    feature,
                    operation: "typing".into(),
                    channel: channel_ref(),
                    conversation: ConversationRef {
                        kind: ConversationKind::Group,
                        id: format!("g:{group}"),
                        ..Default::default()
                    },
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: vec![],
                    platform_message_ids: vec![],
                    request: serde_json::json!({"command":request.cmd,"group_code":group}),
                });
                ChannelTyping::stop_typing(self.channel.as_ref(), GROUP).await?;
                let _stop = session.recv_frame().await;
                Ok(None)
            }
            Feature::VoiceIn => self.voice_in().await,
            Feature::Image => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Image {
                        media: media(MediaKind::Image),
                        caption: None,
                        alt_text: None,
                    }),
                )
                .await
            }
            Feature::Video => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Video {
                        media: media(MediaKind::Video),
                        caption: None,
                        duration_ms: None,
                        thumbnail: None,
                    }),
                )
                .await
            }
            Feature::Audio => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Audio {
                        media: media(MediaKind::Audio),
                        codec: Some("ogg_opus".into()),
                        duration_ms: None,
                        title: None,
                        transcript: None,
                    }),
                )
                .await
            }
            Feature::File => {
                self.refuse_send(
                    feature,
                    content(ContentPart::File {
                        media: media(MediaKind::Document),
                        file_name: Some("fixture.pdf".into()),
                        caption: None,
                    }),
                )
                .await
            }
            Feature::Sticker => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Sticker {
                        media: media(MediaKind::Unknown),
                        pack_id: None,
                        sticker_id: None,
                        emoji: None,
                        alt_text: None,
                    }),
                )
                .await
            }
            Feature::Location => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Location {
                        latitude: 1.0,
                        longitude: 2.0,
                        label: None,
                        address: None,
                        accuracy_meters: None,
                        live_until_unix_ms: None,
                    }),
                )
                .await
            }
            Feature::Contact => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Contact {
                        display_name: "fixture".into(),
                        phone_numbers: vec![],
                        emails: vec![],
                        organization: None,
                    }),
                )
                .await
            }
            Feature::Buttons => {
                self.refuse_send(feature, content(ContentPart::Buttons { rows: vec![] }))
                    .await
            }
            Feature::Poll => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Poll {
                        question: "fixture".into(),
                        options: vec!["a".into()],
                        multiple_choice: false,
                        closes_at_unix_ms: None,
                        provider_poll_id: None,
                    }),
                )
                .await
            }
            Feature::Presentation => {
                self.refuse_send(
                    feature,
                    content(ContentPart::Presentation {
                        blocks: serde_json::json!({"fixture":true}),
                    }),
                )
                .await
            }
            Feature::NativeData => {
                self.refuse_send(
                    feature,
                    OutboundPayload::NativeChannelData {
                        data: serde_json::json!({"fixture":true}),
                    },
                )
                .await
            }
            Feature::VoiceOut => {
                self.refuse_send(
                    feature,
                    OutboundPayload::Voice {
                        voice: OutboundVoice::Media {
                            media: media(MediaKind::Audio),
                            audio_base64: None,
                            codec: Some("ogg_opus".into()),
                            duration_ms: None,
                            waveform: None,
                        },
                    },
                )
                .await
            }
            Feature::VoiceSynthesis => {
                self.refuse_send(
                    feature,
                    OutboundPayload::Voice {
                        voice: OutboundVoice::Synthesize {
                            text: "fixture".into(),
                            voice: None,
                        },
                    },
                )
                .await
            }
            Feature::ThreadAddressing => {
                let mut intent = self.intent(
                    feature,
                    OutboundPayload::Text {
                        text: "thread".into(),
                    },
                );
                intent.thread_id = Some("thread-root".into());
                let result = ChannelAdapter::send(self.channel.as_ref(), intent).await;
                assert_no_wire_request(&self.session).await;
                result.map(|_| None)
            }
            Feature::Reactions => {
                ChannelReaction::send_reaction(
                    self.channel.as_ref(),
                    &channel_ref(),
                    &group_ref(),
                    "previous-message",
                    &Default::default(),
                    false,
                )
                .await?;
                Ok(None)
            }
            Feature::Edit => ChannelEdit::edit(
                self.channel.as_ref(),
                &MessageReceipt::default(),
                self.intent(
                    feature,
                    OutboundPayload::Text {
                        text: "edit".into(),
                    },
                ),
            )
            .await
            .map(|_| None),
            Feature::Delete => {
                ChannelDelete::delete(self.channel.as_ref(), &MessageReceipt::default())
                    .await
                    .map(|_| None)
            }
            Feature::InboundReactions | Feature::ReadReceipts => Err(ChannelSendError {
                kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
                message: "Yuanbao has no inbound reaction/read-receipt operation".into(),
                unsupported: true,
                ..Default::default()
            }),
            Feature::StreamingDraft => ChannelStreamingDraft::upsert_draft(
                self.channel.as_ref(),
                None,
                self.intent(
                    feature,
                    OutboundPayload::Text {
                        text: "draft".into(),
                    },
                ),
            )
            .await
            .map(|_| None),
        }
    }
}

#[tokio::test]
async fn factory_built_yuanbao_passes_the_full_configured_conformance_matrix() {
    let fixture = Fixture::start().await;
    let results = check_all_features(&fixture).await;
    let failed = results
        .iter()
        .filter_map(|(feature, result)| {
            result
                .as_ref()
                .err()
                .map(|error| format!("{feature:?}: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(
        failed.is_empty(),
        "Yuanbao conformance failures: {failed:#?}"
    );
    ChannelAdapter::stop(fixture.channel.as_ref())
        .await
        .expect("listener should stop cleanly");
    if let Some(listener) = fixture.listener.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(3), listener)
            .await
            .expect("listener should exit after stop")
            .expect("listener task should not panic")
            .expect("listener should stop cleanly");
    }
}

async fn assert_no_wire_request(session: &tokio::sync::Mutex<GatewaySession>) {
    let mut session = session.lock().await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), session.recv_frame())
            .await
            .is_err(),
        "undeclared Yuanbao operation must not reach the gateway"
    );
}
