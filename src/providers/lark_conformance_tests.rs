//! Runs the shared channel conformance matrix against a host-aware Lark adapter.

use super::api_tests::{FixtureVoiceCodec, fixture_client, is_token_request, token_ok};
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction, ChannelStreamingDraft,
    ChannelTyping,
};
use crate::factory::{HttpClientFactory, build_long_tail_channel_with_host};
use crate::host::ChannelHostBuilder;
use crate::providers::http_test_support::HttpsServer;
use crate::providers::native::fixtures::{StubMedia, StubVoice, stored_media};
use async_trait::async_trait;
use base64::Engine as _;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_bus::channel::{
    ChannelRef, ChannelSendError, ChannelSendOutcome, ConversationRef, MessageReceipt,
    SendErrorKind,
};
use tinychannels_bus::{
    ChannelOutboundIntent, ContentPart, MediaKind, MediaReference, OutboundContentPart,
    OutboundPayload, OutboundVoice,
};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature as Feature, ExpectedScope, WireObservation,
    check_all_features,
};

const CHANNEL: &str = "oc-conformance-chat";
const ACCOUNT: &str = "lark:conformance";

struct FixtureHttpClients(reqwest::Client);

impl HttpClientFactory for FixtureHttpClients {
    fn client_for(&self, channel: &str) -> reqwest::Client {
        let _ = channel;
        self.0.clone()
    }
}

struct LarkFixture {
    channel: Arc<dyn crate::LongTailNative>,
    server: HttpsServer,
    media: Arc<StubMedia>,
    observations: Mutex<Vec<WireObservation>>,
    issued_message_ids: Arc<Mutex<Vec<String>>>,
}

impl LarkFixture {
    async fn start() -> Self {
        let next_id = Arc::new(AtomicUsize::new(1));
        let issued_message_ids = Arc::new(Mutex::new(Vec::new()));
        let server = HttpsServer::start({
            let next_id = next_id.clone();
            let issued = issued_message_ids.clone();
            move |request| {
                if is_token_request(request) {
                    token_ok("t-conformance")
                } else if request.target == "/open-apis/im/v1/images" {
                    (
                        200,
                        r#"{"code":0,"data":{"image_key":"img-conformance"}}"#.into(),
                    )
                } else if request.target == "/open-apis/im/v1/files" {
                    (
                        200,
                        r#"{"code":0,"data":{"file_key":"file-conformance"}}"#.into(),
                    )
                } else if request.target.starts_with("/open-apis/im/v1/messages?") {
                    let id = format!("om-{}", next_id.fetch_add(1, Ordering::SeqCst));
                    issued.lock().push(id.clone());
                    (
                        200,
                        format!(r#"{{"code":0,"data":{{"message_id":"{id}"}}}}"#),
                    )
                } else {
                    (
                        404,
                        r#"{"code":99991663,"msg":"unexpected request"}"#.into(),
                    )
                }
            }
        })
        .await;
        let media = Arc::new(StubMedia::default());
        let voice = Arc::new(StubVoice::default());
        *voice.audio_base64.lock().unwrap() =
            Some(base64::engine::general_purpose::STANDARD.encode(b"voice-source"));
        let host = ChannelHostBuilder::new()
            .media_repository(media.clone())
            .voice_media_store(voice)
            .voice_codec(Arc::new(FixtureVoiceCodec))
            .build();
        let config = tinychannels_bus::ChannelsConfig {
            lark: Some(tinychannels_bus::config::LarkConfig {
                app_id: "app-id".into(),
                app_secret: "app-secret".into(),
                encrypt_key: None,
                verification_token: None,
                allowed_users: vec!["*".into()],
                use_feishu: true,
                receive_mode: Default::default(),
                port: None,
            }),
            ..Default::default()
        };
        let client = fixture_client(&server);
        let channel =
            build_long_tail_channel_with_host(&config, &host, &FixtureHttpClients(client), "lark")
                .expect("configured Lark adapter");
        Self {
            channel,
            server,
            media,
            observations: Mutex::new(Vec::new()),
            issued_message_ids,
        }
    }

    fn intent(&self, payload: OutboundPayload) -> ChannelOutboundIntent {
        ChannelOutboundIntent {
            idempotency_key: "lark-conformance".into(),
            channel_id: "lark".into(),
            conversation_id: CHANNEL.into(),
            channel_ref: Some(channel_ref()),
            conversation_ref: Some(conversation_ref()),
            turn_id: Some("lark-conformance-turn".into()),
            payload,
            ..Default::default()
        }
    }

    fn set_media(&self, kind: MediaKind, mime: &str, bytes: &[u8]) {
        *self.media.read.lock().unwrap() = Some(stored_media(kind, mime, bytes));
    }

    async fn request_count(&self) -> usize {
        self.server.requests.lock().unwrap().len()
    }

    async fn send_and_record(
        &self,
        feature: Feature,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let before = self.request_count().await;
        let issued_before = self.issued_message_ids.lock().len();
        let outcome = ChannelAdapter::send(self.channel.as_ref(), self.intent(payload)).await?;
        let issued_ids = self.issued_message_ids.lock()[issued_before..].to_vec();
        assert_eq!(
            outcome.receipt.platform_message_ids, issued_ids,
            "adapter receipt must match IDs returned by the mocked Lark API"
        );
        let requests = self.server.requests.lock().unwrap().clone();
        let request = requests[before..]
            .iter()
            .find(|request| request.target.starts_with("/open-apis/im/v1/messages?"))
            .ok_or_else(|| ChannelSendError {
                kind: SendErrorKind::Unknown,
                message: "mock did not receive a message request".into(),
                sent_before_error: true,
                ..Default::default()
            })?;
        let msg_type = request.body["msg_type"].as_str().unwrap_or_default();
        let payload_kind = match msg_type {
            "text" => "text",
            "image" => "image",
            "media" => "video",
            "file" => "file",
            "audio" if feature == Feature::VoiceOut => "voice",
            "audio" => "audio",
            other => other,
        };
        let result = outcome.clone();
        self.observations.lock().push(WireObservation {
            feature,
            operation: "send".into(),
            channel: channel_ref(),
            conversation: ConversationRef {
                id: request.body["receive_id"]
                    .as_str()
                    .unwrap_or_default()
                    .into(),
                kind: tinychannels_bus::ConversationKind::Group,
                ..Default::default()
            },
            reply_to_id: None,
            thread_id: None,
            payload_kinds: vec![payload_kind.into()],
            platform_message_ids: issued_ids,
            request: request.body.clone(),
        });
        Ok(Some(result))
    }

    async fn refused<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, ChannelSendError>>,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let before = self.request_count().await;
        let result = operation.await;
        assert_eq!(
            self.request_count().await,
            before,
            "undeclared Lark feature made a request"
        );
        result.map(|_| None)
    }
}

fn channel_ref() -> ChannelRef {
    ChannelRef {
        id: "lark".into(),
        account_id: Some(ACCOUNT.into()),
    }
}

fn conversation_ref() -> ConversationRef {
    ConversationRef {
        kind: tinychannels_bus::ConversationKind::Group,
        id: CHANNEL.into(),
        ..Default::default()
    }
}

fn media_ref(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some(format!("lark-media-{kind:?}")),
        kind,
        ..Default::default()
    }
}

fn rich(part: ContentPart) -> OutboundPayload {
    OutboundPayload::Parts {
        parts: vec![OutboundContentPart::Content { part }],
    }
}

#[async_trait]
impl AdapterFixture for LarkFixture {
    fn adapter(&self) -> &dyn crate::channel::ChannelAdapter {
        self.channel.as_ref()
    }

    fn expected_scope(&self, _feature: Feature) -> ExpectedScope {
        ExpectedScope {
            channel: channel_ref(),
            conversation: conversation_ref(),
            reply_to_id: None,
            thread_id: None,
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
            Feature::Image => {
                self.set_media(MediaKind::Image, "image/png", b"image-bytes");
                self.send_and_record(
                    feature,
                    rich(ContentPart::Image {
                        media: media_ref(MediaKind::Image),
                        caption: None,
                        alt_text: None,
                    }),
                )
                .await
            }
            Feature::Video => {
                self.set_media(MediaKind::Video, "video/mp4", b"video-bytes");
                self.send_and_record(
                    feature,
                    rich(ContentPart::Video {
                        media: media_ref(MediaKind::Video),
                        caption: None,
                        duration_ms: None,
                        thumbnail: None,
                    }),
                )
                .await
            }
            Feature::Audio => {
                self.set_media(
                    MediaKind::Audio,
                    "audio/ogg",
                    include_bytes!("telegram/fixtures/sine-ogg-opus.ogg"),
                );
                self.send_and_record(
                    feature,
                    rich(ContentPart::Audio {
                        media: media_ref(MediaKind::Audio),
                        codec: Some("ogg_opus".into()),
                        duration_ms: None,
                        title: None,
                        transcript: None,
                    }),
                )
                .await
            }
            Feature::File => {
                self.set_media(MediaKind::Document, "application/pdf", b"pdf-bytes");
                self.send_and_record(
                    feature,
                    rich(ContentPart::File {
                        media: media_ref(MediaKind::Document),
                        file_name: Some("conformance.pdf".into()),
                        caption: None,
                    }),
                )
                .await
            }
            Feature::VoiceOut => {
                self.send_and_record(
                    feature,
                    OutboundPayload::Voice {
                        voice: OutboundVoice::Media {
                            media: media_ref(MediaKind::Audio),
                            audio_base64: None,
                            codec: Some("m4a_aac".into()),
                            duration_ms: Some(1000),
                            waveform: None,
                        },
                    },
                )
                .await
            }
            // Media and action kinds that Lark does not declare still go
            // through the actual adapter planner and must stop before HTTP.
            Feature::Sticker => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Sticker {
                        media: Default::default(),
                        pack_id: None,
                        sticker_id: None,
                        emoji: None,
                        alt_text: None,
                    })),
                ))
                .await
            }
            Feature::Location => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Location {
                        latitude: 1.0,
                        longitude: 2.0,
                        label: None,
                        address: None,
                        accuracy_meters: None,
                        live_until_unix_ms: None,
                    })),
                ))
                .await
            }
            Feature::Contact => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Contact {
                        display_name: "test".into(),
                        phone_numbers: vec![],
                        emails: vec![],
                        organization: None,
                    })),
                ))
                .await
            }
            Feature::Buttons => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Buttons { rows: vec![] })),
                ))
                .await
            }
            Feature::Poll => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Poll {
                        question: "q".into(),
                        options: vec!["a".into()],
                        multiple_choice: false,
                        closes_at_unix_ms: None,
                        provider_poll_id: None,
                    })),
                ))
                .await
            }
            Feature::Presentation => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Presentation {
                        blocks: serde_json::json!({"kind":"test"}),
                    })),
                ))
                .await
            }
            Feature::NativeData => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(OutboundPayload::NativeChannelData {
                        data: serde_json::json!({"kind":"test"}),
                    }),
                ))
                .await
            }
            Feature::VoiceSynthesis => {
                self.refused(ChannelAdapter::send(
                    self.channel.as_ref(),
                    self.intent(OutboundPayload::Voice {
                        voice: OutboundVoice::Synthesize {
                            text: "test".into(),
                            voice: None,
                        },
                    }),
                ))
                .await
            }
            Feature::Typing => {
                self.refused(ChannelTyping::start_typing(self.channel.as_ref(), CHANNEL))
                    .await
            }
            Feature::Reactions => {
                self.refused(ChannelReaction::send_reaction(
                    self.channel.as_ref(),
                    &channel_ref(),
                    &conversation_ref(),
                    "om-1",
                    &tinychannels_bus::channel::ChannelReaction::default(),
                    false,
                ))
                .await
            }
            Feature::Edit => {
                self.refused(ChannelEdit::edit(
                    self.channel.as_ref(),
                    &MessageReceipt::default(),
                    self.intent(OutboundPayload::Text {
                        text: "edit".into(),
                    }),
                ))
                .await
            }
            Feature::Delete => {
                self.refused(ChannelDelete::delete(
                    self.channel.as_ref(),
                    &MessageReceipt::default(),
                ))
                .await
            }
            Feature::StreamingDraft => {
                self.refused(ChannelStreamingDraft::upsert_draft(
                    self.channel.as_ref(),
                    None,
                    self.intent(OutboundPayload::Text {
                        text: "draft".into(),
                    }),
                ))
                .await
            }
            Feature::ReplyAddressing => {
                let mut intent = self.intent(OutboundPayload::Text {
                    text: "reply".into(),
                });
                intent.reply_to_id = Some("om-parent".into());
                self.refused(ChannelAdapter::send(self.channel.as_ref(), intent))
                    .await
            }
            Feature::ThreadAddressing => {
                let mut intent = self.intent(OutboundPayload::Text {
                    text: "thread".into(),
                });
                intent.thread_id = Some("om-root".into());
                self.refused(ChannelAdapter::send(self.channel.as_ref(), intent))
                    .await
            }
            // These inbound-only features have no outbound adapter operation;
            // the actual fixture has no ingress event source installed.
            Feature::VoiceIn | Feature::InboundReactions | Feature::ReadReceipts => {
                Err(ChannelSendError {
                    kind: SendErrorKind::BadFormat,
                    message: "unsupported Lark operation".into(),
                    unsupported: true,
                    ..Default::default()
                })
            }
        }
    }
}

#[tokio::test]
async fn the_host_aware_lark_adapter_passes_the_full_conformance_matrix() {
    let fixture = LarkFixture::start().await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), Feature::ALL.len());
    let failed = results
        .iter()
        .filter_map(|(feature, result)| {
            result
                .as_ref()
                .err()
                .map(|error| format!("{feature:?}: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(failed.is_empty(), "Lark conformance failures: {failed:#?}");
}
