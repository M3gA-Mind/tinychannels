//! Runs the shared channel conformance matrix against a host-aware Lark adapter.

use super::api_tests::{FixtureVoiceCodec, fixture_client, is_token_request, token_ok};
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelInboundSink, ChannelStreamingDraft,
    ChannelTyping,
};
use crate::factory::{HttpClientFactory, build_long_tail_channel_with_host};
use crate::host::ChannelHostBuilder;
use crate::providers::http_test_support::HttpsServer;
use crate::providers::native::fixtures::{
    RecordingEvents, StubMedia, StubTranscriber, StubVoice, stored_media,
};
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
    ChannelIngress, ChannelOutboundIntent, ContentPart, MediaKind, MediaReference,
    OutboundContentPart, OutboundPayload, OutboundVoice,
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
    voice_store: Arc<StubVoice>,
    webhook_port: Option<u16>,
    voice_in_enabled: bool,
}

#[derive(Clone)]
struct LarkFixtureSink(tokio::sync::mpsc::UnboundedSender<ChannelIngress>);

#[async_trait]
impl ChannelInboundSink for LarkFixtureSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(&self, ingress: ChannelIngress) -> Result<(), ChannelSendError> {
        self.0.send(ingress).map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "Lark conformance sink closed".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

impl LarkFixture {
    async fn start() -> Self {
        Self::start_with_voice_ingress(false).await
    }

    async fn start_with_voice_ingress(voice_in_enabled: bool) -> Self {
        let webhook_port = if voice_in_enabled {
            let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = probe.local_addr().unwrap().port();
            drop(probe);
            Some(port)
        } else {
            None
        };
        let next_id = Arc::new(AtomicUsize::new(1));
        let issued_message_ids = Arc::new(Mutex::new(Vec::new()));
        let server = HttpsServer::start_with_bytes(
            {
                let next_id = next_id.clone();
                let issued = issued_message_ids.clone();
                move |request| {
                    if is_token_request(request) {
                        let (status, body) = token_ok("t-conformance");
                        (status, body.into_bytes())
                    } else if request.method == "GET"
                        && request
                            .target
                            .ends_with("/resources/fixture-voice?type=file")
                    {
                        (
                            200,
                            include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg").to_vec(),
                        )
                    } else if request.target == "/open-apis/im/v1/images" {
                        (
                            200,
                            r#"{"code":0,"data":{"image_key":"img-conformance"}}"#
                                .as_bytes()
                                .to_vec(),
                        )
                    } else if request.target == "/open-apis/im/v1/files" {
                        (
                            200,
                            r#"{"code":0,"data":{"file_key":"file-conformance"}}"#
                                .as_bytes()
                                .to_vec(),
                        )
                    } else if request.target.starts_with("/open-apis/im/v1/messages?") {
                        let id = format!("om-{}", next_id.fetch_add(1, Ordering::SeqCst));
                        issued.lock().push(id.clone());
                        (
                            200,
                            format!(r#"{{"code":0,"data":{{"message_id":"{id}"}}}}"#).into_bytes(),
                        )
                    } else {
                        (
                            404,
                            r#"{"code":99991663,"msg":"unexpected request"}"#.as_bytes().to_vec(),
                        )
                    }
                }
            },
            "audio/ogg",
        )
        .await;
        let media = Arc::new(StubMedia::default());
        let voice = Arc::new(StubVoice::default());
        *voice.audio_base64.lock().unwrap() =
            Some(base64::engine::general_purpose::STANDARD.encode(b"voice-source"));
        let host = ChannelHostBuilder::new()
            .media_repository(media.clone())
            .voice_media_store(voice.clone())
            .voice_codec(Arc::new(FixtureVoiceCodec));
        let host = if voice_in_enabled {
            host.transcriber(Arc::new(StubTranscriber::default()))
                .events(Arc::new(RecordingEvents::delivering()))
                .build()
        } else {
            host.build()
        };
        let config = tinychannels_bus::ChannelsConfig {
            lark: Some(tinychannels_bus::config::LarkConfig {
                app_id: "app-id".into(),
                app_secret: "app-secret".into(),
                encrypt_key: None,
                verification_token: Some("verify-token".into()),
                allowed_users: vec!["*".into()],
                use_feishu: true,
                receive_mode: if voice_in_enabled {
                    tinychannels_bus::config::LarkReceiveMode::Webhook
                } else {
                    Default::default()
                },
                port: webhook_port,
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
            voice_store: voice,
            webhook_port,
            voice_in_enabled,
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

    async fn receive_voice_in(&self) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let port = self.webhook_port.ok_or_else(|| ChannelSendError {
            kind: SendErrorKind::BadFormat,
            message: "Lark voice ingress fixture is not configured".into(),
            unsupported: true,
            ..Default::default()
        })?;
        let payload = serde_json::json!({
            "header": {
                "event_type": "im.message.receive_v1",
                "event_id": "event-voice-in-1",
                "token": "verify-token"
            },
            "event": {
                "sender": {
                    "sender_type": "user",
                    "sender_id": {"open_id": "ou-voice-sender"}
                },
                "message": {
                    "message_id": "om-voice-in-1",
                    "chat_id": CHANNEL,
                    "chat_type": "p2p",
                    "message_type": "audio",
                    "content": serde_json::json!({
                        "file_key": "fixture-voice",
                        "duration": 1000
                    }).to_string(),
                    "create_time": "1760900000000"
                }
            }
        });
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let sink = LarkFixtureSink(sender);
        let adapter = self.channel.clone();
        let listener =
            tokio::spawn(async move { ChannelAdapter::start(adapter.as_ref(), &sink).await });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "could not create Lark conformance client".into(),
                retryable: true,
                ..Default::default()
            })?;
        let post_result = async {
            for _ in 0..100 {
                if listener.is_finished() {
                    return Err(ChannelSendError {
                        kind: SendErrorKind::Transient,
                        message: "Lark webhook listener stopped before accepting an event".into(),
                        retryable: true,
                        ..Default::default()
                    });
                }
                match client
                    .post(format!("http://127.0.0.1:{port}/lark"))
                    .json(&payload)
                    .send()
                    .await
                {
                    Ok(response) => return Ok(response),
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                }
            }
            Err(ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "Lark webhook did not become ready".into(),
                retryable: true,
                ..Default::default()
            })
        }
        .await;
        let ingress_result = match post_result {
            Ok(response) if response.status().is_success() => {
                tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
                    .await
                    .ok()
                    .flatten()
                    .ok_or_else(|| ChannelSendError {
                        kind: SendErrorKind::Transient,
                        message: "Lark voice event was not delivered to the typed sink".into(),
                        retryable: true,
                        ..Default::default()
                    })
            }
            Ok(response) => Err(ChannelSendError {
                kind: SendErrorKind::Transient,
                message: format!("Lark webhook returned {}", response.status()),
                retryable: true,
                ..Default::default()
            }),
            Err(error) => Err(error),
        };
        let stop_result = ChannelAdapter::stop(self.channel.as_ref()).await;
        let listener_result = tokio::time::timeout(std::time::Duration::from_secs(2), listener)
            .await
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "Lark typed listener did not stop".into(),
                retryable: true,
                ..Default::default()
            })?
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "Lark typed listener task failed".into(),
                retryable: true,
                ..Default::default()
            })?;
        stop_result?;
        listener_result?;
        let ingress = ingress_result?;
        let ChannelIngress::Inbound(envelope) = ingress else {
            return Err(ChannelSendError {
                kind: SendErrorKind::BadFormat,
                message: "Lark voice fixture received a non-message ingress".into(),
                ..Default::default()
            });
        };
        assert_eq!(
            envelope.channel,
            ChannelRef {
                id: "lark".into(),
                account_id: None,
            }
        );
        assert_eq!(
            envelope.conversation.kind,
            tinychannels_bus::ConversationKind::Dm
        );
        assert_eq!(envelope.conversation.id, CHANNEL);
        assert!(envelope.conversation.thread_id.is_none());
        assert_eq!(envelope.message_id, "om-voice-in-1");
        let voice_notes = envelope
            .parts
            .iter()
            .filter_map(|part| match part {
                ContentPart::VoiceNote { voice_note } => Some(voice_note),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(voice_notes.len(), 1);
        let voice_note = voice_notes[0];
        assert_eq!(voice_note.media.opaque_id.as_deref(), Some("voice-1"));
        assert_eq!(voice_note.media.kind, MediaKind::Audio);
        assert!(voice_note.media.path.is_none());
        assert!(voice_note.media.url.is_none());
        assert!(voice_note.media.local_cache_path.is_none());
        let transcript = voice_note
            .transcript
            .as_ref()
            .expect("configured host transcriber should enrich the voice note");
        assert_eq!(transcript.text, "hello");
        assert_eq!(transcript.language.as_deref(), Some("en"));
        assert_eq!(
            transcript.state,
            tinychannels_bus::VoiceTranscriptState::Complete
        );
        assert_eq!(
            transcript.source,
            tinychannels_bus::VoiceTranscriptSource::HostStt
        );
        let mut payload_kinds = Vec::new();
        for part in &envelope.parts {
            if let ContentPart::VoiceNote { voice_note } = part {
                payload_kinds.push("audio".into());
                if voice_note.transcript.is_some() {
                    payload_kinds.push("transcript".into());
                }
            }
        }
        self.observations.lock().push(WireObservation {
            feature: Feature::VoiceIn,
            operation: "voice_in".into(),
            channel: envelope.channel.clone(),
            conversation: envelope.conversation.clone(),
            reply_to_id: envelope.reply_to_id.clone(),
            thread_id: envelope.conversation.thread_id.clone(),
            payload_kinds,
            platform_message_ids: Vec::new(),
            request: payload,
        });
        Ok(None)
    }

    async fn send_and_record(
        &self,
        feature: Feature,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let before = self.request_count().await;
        let issued_before = self.issued_message_ids.lock().len();
        let outcome =
            ChannelAdapter::send_outcome(self.channel.as_ref(), self.intent(payload)).await?;
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
        if _feature == Feature::VoiceIn {
            ExpectedScope {
                channel: ChannelRef {
                    id: "lark".into(),
                    account_id: None,
                },
                conversation: ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Dm,
                    id: CHANNEL.into(),
                    ..Default::default()
                },
                reply_to_id: None,
                thread_id: None,
            }
        } else {
            ExpectedScope {
                channel: channel_ref(),
                conversation: conversation_ref(),
                reply_to_id: None,
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
                    include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg"),
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
                            codec: Some("m4a_aac".into()),
                            duration_ms: Some(1000),
                        },
                    },
                )
                .await
            }
            // Media and action kinds that Lark does not declare still go
            // through the actual adapter planner and must stop before HTTP.
            Feature::Sticker => {
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Buttons { rows: vec![] })),
                ))
                .await
            }
            Feature::Poll => {
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
                    self.channel.as_ref(),
                    self.intent(rich(ContentPart::Presentation {
                        blocks: serde_json::json!({"kind":"test"}),
                    })),
                ))
                .await
            }
            Feature::NativeData => {
                self.refused(ChannelAdapter::send_outcome(
                    self.channel.as_ref(),
                    self.intent(OutboundPayload::NativeChannelData {
                        data: serde_json::json!({"kind":"test"}),
                    }),
                ))
                .await
            }
            Feature::VoiceSynthesis => {
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(crate::channel::LegacyChannelReaction::send_reaction(
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
                self.refused(ChannelAdapter::send_outcome(self.channel.as_ref(), intent))
                    .await
            }
            Feature::ThreadAddressing => {
                let mut intent = self.intent(OutboundPayload::Text {
                    text: "thread".into(),
                });
                intent.thread_id = Some("om-root".into());
                self.refused(ChannelAdapter::send_outcome(self.channel.as_ref(), intent))
                    .await
            }
            // These inbound-only features have no outbound adapter operation;
            // the actual fixture has no ingress event source installed.
            Feature::VoiceIn if self.voice_in_enabled => self.receive_voice_in().await,
            Feature::VoiceIn | Feature::InboundReactions | Feature::ReadReceipts => {
                Err(ChannelSendError {
                    kind: SendErrorKind::BadFormat,
                    message: "unsupported Lark operation".into(),
                    unsupported: true,
                    ..Default::default()
                })
            }
            Feature::ManagedReaction
            | Feature::ManagedThreadCreate
            | Feature::ManagedThreadUpdate
            | Feature::ManagedThreadList => Err(ChannelSendError::default()),
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

#[tokio::test]
async fn configured_lark_voice_in_is_observed_through_its_typed_webhook_listener() {
    use tinychannels_conformance::check_feature;

    let fixture = LarkFixture::start_with_voice_ingress(true).await;
    let capabilities = fixture.channel.declared_capabilities();
    assert!(capabilities.voice_in.supported);

    check_feature(&fixture, Feature::VoiceIn)
        .await
        .unwrap_or_else(|error| panic!("Lark voice ingress conformance failed: {error}"));

    let requests = fixture.server.requests.lock().unwrap();
    let download = requests
        .iter()
        .find(|request| {
            request.method == "GET"
                && request
                    .target
                    .ends_with("/resources/fixture-voice?type=file")
        })
        .expect("the typed listener must download the actual voice resource");
    assert!(
        download
            .headers
            .to_ascii_lowercase()
            .contains("authorization: bearer t-conformance"),
        "voice download must use the token issued to the configured app"
    );
    let stored = fixture.voice_store.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].direction,
        tinychannels_bus::HostVoiceMediaDirection::Inbound
    );
    assert_eq!(stored[0].mime_type, "audio/ogg");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&stored[0].audio_base64)
            .unwrap(),
        include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg")
    );
}
