//! Runs the shared adapter conformance kit against the long-tail providers.
//!
//! Mattermost is the one long-tail platform whose host is configurable, so its
//! whole matrix runs against a wiremock server: every observation is parsed
//! from the requests the mock received and every id from the responses it
//! returned. The other five providers (Lark, IRC, QQ, DingTalk, Yuanbao) reach
//! fixed hosts, so for them the kit checks capability honesty: every feature
//! the account does not declare must answer an explicit `unsupported` result
//! and never touch the network.

use crate::LongTailNative;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction, ChannelStreamingDraft,
    ChannelTyping,
};
use crate::factory::{
    DefaultHttpClients, HttpClientFactory, build_long_tail_channel,
    build_long_tail_channel_with_host,
};
use crate::host::{
    MediaRepository, MediaScope, MediaStoreRequest, StoredMedia, VoiceMediaRequest, VoiceMediaStore,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tinychannels_bus::channel::{
    ChannelRef, ChannelSendError, ChannelSendOutcome, ConversationRef, MessageReceipt,
    SendErrorKind,
};
#[cfg(feature = "lark")]
use tinychannels_bus::config::LarkConfig;
use tinychannels_bus::config::{
    DingTalkConfig, IrcConfig, MattermostConfig, QQConfig, YuanbaoConfig,
};
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

struct ConformanceMedia;

#[async_trait]
impl MediaRepository for ConformanceMedia {
    async fn store_media(
        &self,
        _request: MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        anyhow::bail!("conformance fixture only reads scoped media")
    }

    async fn read_media(
        &self,
        _scope: MediaScope,
        opaque_id: String,
        _max_bytes: u64,
    ) -> anyhow::Result<StoredMedia> {
        let (kind, content_type, file_name, bytes) = match opaque_id.as_str() {
            "media-Image" => (
                MediaKind::Image,
                "image/png",
                "diagram.png",
                b"conformance-image".as_slice(),
            ),
            "media-Document" => (
                MediaKind::Document,
                "application/pdf",
                "document.pdf",
                b"conformance-file".as_slice(),
            ),
            "media-Video" => (
                MediaKind::Video,
                "video/mp4",
                "clip.mp4",
                b"conformance-video".as_slice(),
            ),
            "media-Audio" => (
                MediaKind::Audio,
                "audio/mp4",
                "audio.m4a",
                b"conformance-audio".as_slice(),
            ),
            _ => anyhow::bail!("unexpected conformance media id"),
        };
        Ok(StoredMedia {
            media: MediaReference {
                opaque_id: Some(opaque_id),
                kind,
                ..Default::default()
            },
            content_type: content_type.into(),
            file_name: Some(file_name.into()),
            bytes: bytes.to_vec(),
        })
    }
}

struct ConformanceVoice;

#[async_trait]
impl VoiceMediaStore for ConformanceVoice {
    async fn store_voice(
        &self,
        _request: VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        anyhow::bail!("conformance fixture only reads scoped voice")
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        use base64::Engine as _;
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(b"conformance-audio"),
            mime_type: "audio/ogg".into(),
            file_name: Some("voice.m4a".into()),
        })
    }
}

struct FixtureHttpClients(reqwest::Client);

impl HttpClientFactory for FixtureHttpClients {
    fn client_for(&self, _channel: &str) -> reqwest::Client {
        self.0.clone()
    }
}

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
    fixed_server: Option<crate::providers::http_test_support::HttpsServer>,
    /// Message ids the mock returned, in response order.
    response_ids: Arc<Mutex<Vec<String>>>,
    observed: Mutex<Vec<WireObservation>>,
}

impl LongTailFixture {
    async fn start(id: &'static str) -> Self {
        if id == "qq" {
            return Self::start_qq().await;
        }
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
            .and(path("/api/v4/files"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "file_infos": [{"id": "uploaded-file"}]
            })))
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
        let host: Arc<dyn crate::host::ChannelHost> = crate::ChannelHostBuilder::new()
            .media_repository(Arc::new(ConformanceMedia))
            .voice_media_store(Arc::new(ConformanceVoice))
            .build();
        Self {
            id,
            channel: build_long_tail_channel_with_host(&config, &host, &DefaultHttpClients, id)
                .unwrap(),
            server: Some(server),
            fixed_server: None,
            response_ids,
            observed: Mutex::new(Vec::new()),
        }
    }

    async fn start_qq() -> Self {
        use crate::providers::http_test_support::HttpsServer;

        let response_ids = Arc::new(Mutex::new(Vec::new()));
        let next_id = Arc::new(AtomicU64::new(7000));
        let issued = response_ids.clone();
        let counter = next_id.clone();
        let server = HttpsServer::start(move |request| {
            if request.target == "/app/getAppAccessToken" {
                (
                    200,
                    r#"{"access_token":"fixture-token","expires_in":"7200"}"#.into(),
                )
            } else if request.target.ends_with("/files") {
                (200, r#"{"file_info":"fixture-file"}"#.into())
            } else if request.target.ends_with("/messages") {
                let id = counter.fetch_add(1, Ordering::SeqCst).to_string();
                issued.lock().push(id.clone());
                (200, format!(r#"{{"id":"{id}"}}"#))
            } else {
                (404, r#"{"error":"unexpected request"}"#.into())
            }
        })
        .await;
        let host: Arc<dyn crate::host::ChannelHost> = crate::ChannelHostBuilder::new()
            .media_repository(Arc::new(ConformanceMedia))
            .voice_media_store(Arc::new(ConformanceVoice))
            .build();
        let config = ChannelsConfig {
            qq: Some(QQConfig {
                app_id: "fixture-app".into(),
                app_secret: "fixture-secret".into(),
                allowed_users: vec!["*".into()],
            }),
            ..Default::default()
        };
        let clients = FixtureHttpClients(server.client.clone());
        let channel = build_long_tail_channel_with_host(&config, &host, &clients, "qq")
            .expect("configured QQ adapter built through provider factory");
        Self {
            id: "qq",
            channel,
            server: None,
            fixed_server: Some(server),
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
            fixed_server: None,
            response_ids: Arc::default(),
            observed: Mutex::new(Vec::new()),
        }
    }

    fn intent(&self, feature: Feature, payload: OutboundPayload) -> ChannelOutboundIntent {
        let conversation = if self.id == "qq" {
            ConversationRef {
                id: format!("user:{CHANNEL}"),
                ..Default::default()
            }
        } else {
            conversation()
        };
        ChannelOutboundIntent {
            idempotency_key: format!("kit-{feature:?}"),
            channel_id: self.id.into(),
            conversation_id: if self.id == "qq" {
                format!("user:{CHANNEL}")
            } else {
                CHANNEL.into()
            },
            channel_ref: Some(account(self.id)),
            conversation_ref: Some(conversation.clone()),
            thread_id: conversation.thread_id.clone(),
            turn_id: Some("conformance-turn".into()),
            payload,
            ..Default::default()
        }
    }

    async fn request_count(&self) -> usize {
        match &self.server {
            Some(server) => server.received_requests().await.unwrap().len(),
            None => self
                .fixed_server
                .as_ref()
                .map_or(0, |server| server.requests.lock().unwrap().len()),
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
        if self.fixed_server.is_some() {
            self.record_fixed(feature, before);
            return;
        }
        let Some(server) = &self.server else { return };
        let requests = server.received_requests().await.unwrap();
        let mut ids = std::mem::take(&mut *self.response_ids.lock()).into_iter();
        let mut uploaded_kind = None;
        for request in &requests[before..] {
            let url_path = request.url.path();
            if url_path == "/api/v4/files" {
                let multipart = String::from_utf8_lossy(&request.body).to_ascii_lowercase();
                uploaded_kind = if multipart.contains("image/png") {
                    Some("image")
                } else if multipart.contains("application/pdf") {
                    Some("file")
                } else if multipart.contains("video/mp4") {
                    Some("video")
                } else if multipart.contains("audio/mp4") {
                    Some(if feature == Feature::VoiceOut {
                        "voice"
                    } else {
                        "audio"
                    })
                } else {
                    None
                };
                continue;
            }
            let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
            let (operation, message_ids, kinds) = if url_path == "/api/v4/posts" {
                let kinds = if body.get("file_ids").is_some() {
                    uploaded_kind
                        .take()
                        .into_iter()
                        .map(str::to_owned)
                        .collect()
                } else {
                    vec!["text".to_owned()]
                };
                ("send", ids.next().into_iter().collect(), kinds)
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

    fn record_fixed(&self, feature: Feature, before: usize) {
        let Some(server) = &self.fixed_server else {
            return;
        };
        let requests = server.requests.lock().unwrap();
        let mut ids = std::mem::take(&mut *self.response_ids.lock()).into_iter();
        let mut uploaded_kind = None;
        for request in &requests[before..] {
            if request.target.ends_with("/files") {
                let file_type = request.body["file_type"].as_u64();
                uploaded_kind = match file_type {
                    Some(1) => Some("image"),
                    Some(2) => Some("video"),
                    Some(3) => Some("voice"),
                    Some(4) => Some("file"),
                    _ => None,
                };
                continue;
            }
            if !request.target.ends_with("/messages") {
                continue;
            }
            let payload_kinds = if request.body["msg_type"].as_u64() == Some(0) {
                vec!["text".to_owned()]
            } else {
                uploaded_kind
                    .take()
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            };
            self.observed.lock().push(WireObservation {
                feature,
                operation: "send".into(),
                channel: account(self.id),
                conversation: self.expected_scope(feature).conversation,
                reply_to_id: None,
                thread_id: None,
                payload_kinds,
                platform_message_ids: ids.next().into_iter().collect(),
                request: request.body.clone(),
            });
        }
    }

    async fn send_and_record(
        &self,
        feature: Feature,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        self.send_intent_and_record(feature, self.intent(feature, payload))
            .await
    }

    async fn send_intent_and_record(
        &self,
        feature: Feature,
        intent: ChannelOutboundIntent,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let before = self.request_count().await;
        let outcome = ChannelAdapter::send(self.channel.as_ref(), intent).await?;
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
        opaque_id: Some(format!("media-{kind:?}")),
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
            conversation: if self.id == "qq" {
                ConversationRef {
                    id: CHANNEL.into(),
                    ..Default::default()
                }
            } else {
                conversation()
            },
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
            // A platform without thread addressing has nothing to send to.
            Feature::ThreadAddressing if !channel.declared_capabilities().addressing.thread_id => {
                let mut intent = self.intent(
                    feature,
                    OutboundPayload::Text {
                        text: "thread conformance".into(),
                    },
                );
                intent.thread_id = Some(ROOT.into());
                self.refused(ChannelAdapter::send(channel, intent)).await
            }
            Feature::Text | Feature::ThreadAddressing => {
                self.send_and_record(
                    feature,
                    OutboundPayload::Text {
                        text: "conformance".into(),
                    },
                )
                .await
            }
            Feature::ReplyAddressing => {
                let mut intent = self.intent(
                    feature,
                    OutboundPayload::Text {
                        text: "reply conformance".into(),
                    },
                );
                intent.reply_to_id = Some(ROOT.into());
                if channel.declared_capabilities().addressing.reply_to_id {
                    self.send_intent_and_record(feature, intent).await
                } else {
                    self.refused(ChannelAdapter::send(channel, intent)).await
                }
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
                self.send_and_record(
                    feature,
                    rich(ContentPart::Image {
                        media: media(MediaKind::Image),
                        caption: None,
                        alt_text: None,
                    }),
                )
                .await
            }
            Feature::Video => {
                self.send_and_record(
                    feature,
                    rich(ContentPart::Video {
                        media: media(MediaKind::Video),
                        caption: None,
                        duration_ms: None,
                        thumbnail: None,
                    }),
                )
                .await
            }
            Feature::Audio => {
                self.send_and_record(
                    feature,
                    rich(ContentPart::Audio {
                        media: media(MediaKind::Audio),
                        codec: None,
                        duration_ms: None,
                        title: None,
                        transcript: None,
                    }),
                )
                .await
            }
            Feature::File => {
                self.send_and_record(
                    feature,
                    rich(ContentPart::File {
                        media: media(MediaKind::Document),
                        file_name: None,
                        caption: None,
                    }),
                )
                .await
            }
            Feature::Sticker => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        rich(ContentPart::Sticker {
                            media: Default::default(),
                            pack_id: None,
                            sticker_id: None,
                            emoji: None,
                            alt_text: None,
                        }),
                    ),
                ))
                .await
            }
            Feature::Location => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        rich(ContentPart::Location {
                            latitude: 1.0,
                            longitude: 2.0,
                            label: None,
                            address: None,
                            accuracy_meters: None,
                            live_until_unix_ms: None,
                        }),
                    ),
                ))
                .await
            }
            Feature::Contact => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        rich(ContentPart::Contact {
                            display_name: "k".into(),
                            phone_numbers: vec![],
                            emails: vec![],
                            organization: None,
                        }),
                    ),
                ))
                .await
            }
            Feature::Poll => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        rich(ContentPart::Poll {
                            question: "q".into(),
                            options: vec!["a".into()],
                            multiple_choice: false,
                            closes_at_unix_ms: None,
                            provider_poll_id: None,
                        }),
                    ),
                ))
                .await
            }
            Feature::Buttons => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        rich(ContentPart::Buttons {
                            rows: vec![vec![tinychannels_bus::ButtonAction {
                                id: "opaque".into(),
                                label: "Go".into(),
                                style: None,
                                disabled: false,
                            }]],
                        }),
                    ),
                ))
                .await
            }
            Feature::Presentation => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        rich(ContentPart::Presentation {
                            blocks: json!({"kind": "approval", "id": "kit"}),
                        }),
                    ),
                ))
                .await
            }
            Feature::NativeData => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        OutboundPayload::NativeChannelData {
                            data: json!({"action": "create_thread"}),
                        },
                    ),
                ))
                .await
            }
            Feature::VoiceOut => {
                self.send_and_record(
                    feature,
                    OutboundPayload::Voice {
                        voice: tinychannels_bus::OutboundVoice::Media {
                            media: media(MediaKind::Audio),
                            audio_base64: None,
                            codec: Some("m4a_aac".into()),
                            duration_ms: Some(1500),
                            waveform: None,
                        },
                    },
                )
                .await
            }
            Feature::VoiceSynthesis => {
                self.refused(ChannelAdapter::send(
                    channel,
                    self.intent(
                        feature,
                        OutboundPayload::Voice {
                            voice: tinychannels_bus::OutboundVoice::Synthesize {
                                text: "synthesize this".into(),
                                voice: None,
                            },
                        },
                    ),
                ))
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
            // Inbound voice, reactions and read receipts have no outbound
            // operation on these platforms.
            Feature::VoiceIn | Feature::InboundReactions | Feature::ReadReceipts => {
                unsupported_result()
            }
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
async fn the_factory_built_qq_adapter_passes_the_full_conformance_matrix() {
    let fixture = LongTailFixture::start("qq").await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), Feature::ALL.len());
    let failed = failures(&results);
    assert!(
        failed.is_empty(),
        "QQ conformance failures: {failed:#?}; observations={:#?}; requests={:#?}",
        fixture.observations(),
        fixture.fixed_server.as_ref().unwrap().requests.lock().unwrap().clone()
    );
    let observations = fixture.observations();
    for feature in [
        Feature::Text,
        Feature::Image,
        Feature::Video,
        Feature::File,
        Feature::VoiceOut,
    ] {
        assert!(
            observations
                .iter()
                .any(|observation| observation.feature == feature),
            "declared QQ feature {feature:?} was not observed on the wire"
        );
    }
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
        irc: Some(IrcConfig {
            server: "irc.example.invalid".into(),
            port: 6697,
            nickname: "bot".into(),
            username: None,
            channels: vec![],
            allowed_users: vec![],
            server_password: None,
            nickserv_password: None,
            sasl_password: None,
            verify_tls: None,
        }),
        dingtalk: Some(DingTalkConfig {
            client_id: "id".into(),
            client_secret: "secret".into(),
            allowed_users: vec![],
        }),
        qq: Some(QQConfig {
            app_id: "id".into(),
            app_secret: "secret".into(),
            allowed_users: vec![],
        }),
        yuanbao: Some(YuanbaoConfig {
            app_key: "ak".into(),
            ws_domain: "wss://example.invalid".into(),
            token: "tok".into(),
            ..Default::default()
        }),
        #[cfg(feature = "lark")]
        lark: Some(LarkConfig {
            app_id: "id".into(),
            app_secret: "secret".into(),
            encrypt_key: None,
            verification_token: None,
            allowed_users: vec![],
            use_feishu: false,
            receive_mode: Default::default(),
            port: None,
        }),
        ..Default::default()
    }
}

#[cfg(feature = "lark")]
const OFFLINE: &[&str] = &["lark", "irc", "qq", "dingtalk", "yuanbao"];
#[cfg(not(feature = "lark"))]
const OFFLINE: &[&str] = &["irc", "qq", "dingtalk", "yuanbao"];

/// These providers reach fixed hosts, so only the features they do not
/// declare can run: each must refuse explicitly and without a request.
#[tokio::test]
async fn offline_providers_degrade_every_undeclared_feature_gracefully() {
    let config = offline_config();
    for id in OFFLINE {
        let fixture = LongTailFixture::offline(id, &config);
        let declared = fixture.channel.declared_capabilities();
        let mut checked = 0;
        for feature in Feature::ALL {
            let thread_declared = declared.addressing.thread_id;
            let skip = match feature {
                Feature::Text => true,
                Feature::Typing => declared.actions.supported.iter().any(|a| a == "typing"),
                Feature::ReplyAddressing => declared.addressing.reply_to_id,
                Feature::ThreadAddressing => thread_declared,
                _ => false,
            };
            if skip {
                continue;
            }
            check_feature(&fixture, feature)
                .await
                .unwrap_or_else(|error| panic!("{id} {feature:?}: {error}"));
            checked += 1;
        }
        assert!(checked >= Feature::ALL.len() - 3, "{id} checked {checked}");
    }
}
