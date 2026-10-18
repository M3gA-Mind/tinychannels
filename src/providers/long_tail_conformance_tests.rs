//! Runs the shared adapter conformance kit against the long-tail providers.
//!
//! Mattermost is the one long-tail platform whose host is configurable, so its
//! whole matrix runs against a wiremock server: every observation is parsed
//! from the requests the mock received and every id from the responses it
//! returned. The other five providers (Lark, IRC, QQ, DingTalk, Yuanbao) reach
//! fixed hosts, so for them the kit checks capability honesty: every feature
//! the account does not declare must answer an explicit `unsupported` result
//! and never touch the network.

use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction, ChannelStreamingDraft,
    ChannelTyping,
};
use crate::factory::{DefaultHttpClients, build_long_tail_channel};
use crate::LongTailNative;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tinychannels_bus::channel::{
    ChannelRef, ChannelSendError, ChannelSendOutcome, ConversationRef, MessageReceipt,
    SendErrorKind,
};
use tinychannels_bus::config::{MattermostConfig, QQConfig};
use tinychannels_bus::{
    ChannelOutboundIntent, ChannelsConfig, ContentPart, MediaKind, MediaReference,
    OutboundContentPart, OutboundPayload,
};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature as Feature, ExpectedScope, WireObservation,
    check_all_features, check_feature,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const CHANNEL: &str = "chan1";
const ROOT: &str = "root9";

fn account(id: &str) -> ChannelRef {
    ChannelRef {
        id: id.into(),
        account_id: None,
    }
}

fn conversation() -> ConversationRef {
    ConversationRef {
        id: CHANNEL.into(),
        thread_id: Some(ROOT.into()),
        ..Default::default()
    }
}

fn unsupported_result() -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
    Err(ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: "no such operation on this platform".into(),
        unsupported: true,
        ..Default::default()
    })
}

struct LongTailFixture {
    id: &'static str,
    channel: Arc<dyn LongTailNative>,
    server: Option<MockServer>,
    /// Message ids the mock returned, in response order.
    response_ids: Arc<Mutex<Vec<String>>>,
    observed: Mutex<Vec<WireObservation>>,
}

impl LongTailFixture {
    async fn start(id: &'static str) -> Self {
        let server = MockServer::start().await;
        let response_ids = Arc::new(Mutex::new(Vec::new()));
        let counter = Arc::new(AtomicU64::new(7000));
        let ids = response_ids.clone();
        Mock::given(method("POST"))
            .and(path("/api/v4/posts"))
            .respond_with(move |_: &Request| {
                let id = counter.fetch_add(1, Ordering::SeqCst).to_string();
                ids.lock().push(id.clone());
                ResponseTemplate::new(201).set_body_json(json!({"id": id}))
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v4/users/me/typing"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let config = ChannelsConfig {
            mattermost: Some(MattermostConfig {
                url: server.uri(),
                bot_token: "bot-token".into(),
                channel_id: None,
                allowed_users: vec![],
                thread_replies: None,
                mention_only: None,
            }),
            ..Default::default()
        };
        Self {
            id,
            channel: build_long_tail_channel(&config, &DefaultHttpClients, id).unwrap(),
            server: Some(server),
            response_ids,
            observed: Mutex::new(Vec::new()),
        }
    }

    /// A provider with no reachable mock: only undeclared features run.
    fn offline(id: &'static str, config: &ChannelsConfig) -> Self {
        Self {
            id,
            channel: build_long_tail_channel(config, &DefaultHttpClients, id).unwrap(),
            server: None,
            response_ids: Arc::default(),
            observed: Mutex::new(Vec::new()),
        }
    }

    fn intent(&self, feature: Feature, payload: OutboundPayload) -> ChannelOutboundIntent {
        ChannelOutboundIntent {
            idempotency_key: format!("kit-{feature:?}"),
            channel_id: self.id.into(),
            conversation_id: CHANNEL.into(),
            channel_ref: Some(account(self.id)),
            conversation_ref: Some(conversation()),
            thread_id: Some(ROOT.into()),
            payload,
            ..Default::default()
        }
    }

    async fn request_count(&self) -> usize {
        match &self.server {
            Some(server) => server.received_requests().await.unwrap().len(),
            None => 0,
        }
    }

    /// Run `operation` and fail if it reached the network while undeclared.
    async fn refused<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, ChannelSendError>>,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let before = self.request_count().await;
        let result = operation.await;
        assert_eq!(self.request_count().await, before, "{}", self.id);
        result.map(|_| None)
    }

    /// Turn every request the mock received since `before` into observations.
    async fn record(&self, feature: Feature, before: usize) {
        let Some(server) = &self.server else { return };
        let requests = server.received_requests().await.unwrap();
        let mut ids = self.response_ids.lock().clone().into_iter();
        for request in &requests[before..] {
            let url_path = request.url.path();
            let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
            let (operation, message_ids, kinds) = if url_path == "/api/v4/posts" {
                (
                    "send",
                    ids.next().into_iter().collect(),
                    vec!["text".to_owned()],
                )
            } else {
                ("typing", Vec::new(), Vec::new())
            };
            let channel_id = body["channel_id"].as_str().unwrap_or(CHANNEL);
            let thread = body["root_id"]
                .as_str()
                .or_else(|| body["parent_id"].as_str())
                .map(str::to_owned);
            self.observed.lock().push(WireObservation {
                feature,
                operation: operation.into(),
                channel: account(self.id),
                conversation: ConversationRef {
                    id: channel_id.into(),
                    thread_id: thread.clone(),
                    ..Default::default()
                },
                reply_to_id: None,
                thread_id: thread,
                payload_kinds: kinds,
                platform_message_ids: message_ids,
                request: body,
            });
        }
    }

    async fn send_and_record(
        &self,
        feature: Feature,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let before = self.request_count().await;
        let outcome = self.ChannelAdapter::send(channel, self.intent(feature, payload)).await?;
        self.record(feature, before).await;
        Ok(Some(outcome))
    }
}

fn rich(part: ContentPart) -> OutboundPayload {
    OutboundPayload::Parts {
        parts: vec![OutboundContentPart::Content { part }],
    }
}

fn media(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some("media-1".into()),
        kind,
        ..Default::default()
    }
}

#[async_trait]
impl AdapterFixture for LongTailFixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        self.channel.as_ref()
    }

    fn expected_scope(&self, _feature: Feature) -> ExpectedScope {
        ExpectedScope {
            channel: account(self.id),
            conversation: conversation(),
            reply_to_id: None,
            thread_id: Some(ROOT.into()),
        }
    }

    fn clear_observations(&self) {
        self.observed.lock().clear();
    }

    fn observations(&self) -> Vec<WireObservation> {
        self.observed.lock().clone()
    }

    async fn exercise(
        &self,
        feature: Feature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let channel = self.channel.as_ref();
        let target = conversation();
        match feature {
            Feature::Text | Feature::ThreadAddressing => {
                self.send_and_record(
                    feature,
                    OutboundPayload::Text {
                        text: "conformance".into(),
                    },
                )
                .await
            }
            Feature::Typing => {
                let before = self.request_count().await;
                let address = format!("{CHANNEL}:{ROOT}");
                let started = ChannelTyping::start_typing(channel, &address).await;
                if started.is_ok() {
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    ChannelTyping::stop_typing(channel, &address).await?;
                }
                started?;
                self.record(feature, before).await;
                Ok(None)
            }
            Feature::Image => {
                self.refused(
                    ChannelAdapter::send(channel, self.intent(
                        feature,
                        rich(ContentPart::Image {
                            media: media(MediaKind::Image),
                            caption: None,
                            alt_text: None,
                        }),
                    )),
                )
                .await
            }
            Feature::Video => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Video {
                        media: media(MediaKind::Video),
                        caption: None,
                        duration_ms: None,
                        thumbnail: None,
                    }),
                )))
                .await
            }
            Feature::Audio => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Audio {
                        media: media(MediaKind::Audio),
                        codec: None,
                        duration_ms: None,
                        title: None,
                        transcript: None,
                    }),
                )))
                .await
            }
            Feature::File => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::File {
                        media: media(MediaKind::Document),
                        file_name: None,
                        caption: None,
                    }),
                )))
                .await
            }
            Feature::Sticker => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Sticker {
                        media: Default::default(),
                        pack_id: None,
                        sticker_id: None,
                        emoji: None,
                        alt_text: None,
                    }),
                )))
                .await
            }
            Feature::Location => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Location {
                        latitude: 1.0,
                        longitude: 2.0,
                        label: None,
                        address: None,
                        accuracy_meters: None,
                        live_until_unix_ms: None,
                    }),
                )))
                .await
            }
            Feature::Contact => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Contact {
                        display_name: "k".into(),
                        phone_numbers: vec![],
                        emails: vec![],
                        organization: None,
                    }),
                )))
                .await
            }
            Feature::Poll => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Poll {
                        question: "q".into(),
                        options: vec!["a".into()],
                        multiple_choice: false,
                        closes_at_unix_ms: None,
                        provider_poll_id: None,
                    }),
                )))
                .await
            }
            Feature::Buttons => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Buttons {
                        rows: vec![vec![tinychannels_bus::ButtonAction {
                            id: "opaque".into(),
                            label: "Go".into(),
                            style: None,
                            disabled: false,
                        }]],
                    }),
                )))
                .await
            }
            Feature::Presentation => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    rich(ContentPart::Presentation {
                        blocks: json!({"kind": "approval", "id": "kit"}),
                    }),
                )))
                .await
            }
            Feature::NativeData => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    OutboundPayload::NativeChannelData {
                        data: json!({"action": "create_thread"}),
                    },
                )))
                .await
            }
            Feature::VoiceOut | Feature::VoiceSynthesis => {
                self.refused(ChannelAdapter::send(channel, self.intent(
                    feature,
                    OutboundPayload::Voice {
                        voice: tinychannels_bus::OutboundVoice::Media {
                            media: media(MediaKind::Audio),
                            audio_base64: None,
                            codec: Some("ogg_opus".into()),
                            duration_ms: Some(200),
                            waveform: None,
                        },
                    },
                )))
                .await
            }
            Feature::Reactions => {
                self.refused(ChannelReaction::send_reaction(
                    channel,
                    &account(self.id),
                    &target,
                    "m1",
                    &tinychannels_bus::channel::ChannelReaction::default(),
                    false,
                ))
                .await
            }
            Feature::Edit => {
                self.refused(ChannelEdit::edit(
                    channel,
                    &MessageReceipt::default(),
                    self.intent(feature, OutboundPayload::Text { text: "e".into() }),
                ))
                .await
            }
            Feature::Delete => {
                self.refused(ChannelDelete::delete(channel, &MessageReceipt::default()))
                    .await
            }
            Feature::StreamingDraft => {
                self.refused(ChannelStreamingDraft::upsert_draft(
                    channel,
                    None,
                    self.intent(feature, OutboundPayload::Text { text: "d".into() }),
                ))
                .await
            }
            // Reply addressing, inbound voice, inbound reactions and read
            // receipts have no outbound operation on these platforms.
            Feature::ReplyAddressing
            | Feature::VoiceIn
            | Feature::InboundReactions
            | Feature::ReadReceipts => unsupported_result(),
        }
    }
}

fn failures(results: &[(Feature, Result<(), String>)]) -> Vec<String> {
    results
        .iter()
        .filter_map(|(feature, result)| result.as_ref().err().map(|e| format!("{feature:?}: {e}")))
        .collect()
}

#[tokio::test]
async fn the_mattermost_adapter_passes_the_full_conformance_matrix() {
    let fixture = LongTailFixture::start("mattermost").await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), Feature::ALL.len());
    let failed = failures(&results);
    assert!(failed.is_empty(), "conformance failures: {failed:#?}");
}

#[tokio::test]
async fn mattermost_text_receipt_carries_the_post_id_the_server_returned() {
    let fixture = LongTailFixture::start("mattermost").await;
    let outcome = fixture
        .send_and_record(
            Feature::Text,
            OutboundPayload::Text {
                text: "hello".into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["7000"]);
    assert_eq!(outcome.receipt.thread_id.as_deref(), Some(ROOT));
    assert_eq!(outcome.receipt.parts.len(), 1);
}

fn offline_config() -> ChannelsConfig {
    ChannelsConfig {
        qq: Some(QQConfig {
            app_id: "id".into(),
            app_secret: "secret".into(),
            allowed_users: vec![],
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn an_offline_provider_degrades_every_undeclared_feature_gracefully() {
    let fixture = LongTailFixture::offline("qq", &offline_config());
    let declared = fixture.channel.declared_capabilities();
    let mut checked = 0;
    for feature in Feature::ALL {
        // Text is the only feature QQ declares; its wire is a fixed host.
        if feature == Feature::Text {
            assert!(declared.content.text.outbound);
            continue;
        }
        check_feature(&fixture, feature)
            .await
            .unwrap_or_else(|error| panic!("{feature:?}: {error}"));
        checked += 1;
    }
    assert_eq!(checked, Feature::ALL.len() - 1);
}
