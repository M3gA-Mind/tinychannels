//! Runs the shared adapter conformance kit against the long-tail providers.
//!
//! Mattermost, QQ and DingTalk run their shared matrices against local wire
//! fixtures: observations come from requests the configured adapter actually
//! sent, and receipts come from fixture responses. Other fixed-host providers
//! retain capability-honesty checks until their transports have equivalent
//! local fixtures.

use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelStreamingDraft, ChannelTyping,
};
use crate::factory::{
    DefaultHttpClients, HttpClientFactory, build_long_tail_channel,
    build_long_tail_channel_with_host,
};
use crate::host::{
    MediaRepository, MediaScope, MediaStoreRequest, StoredMedia, VoiceMediaRequest, VoiceMediaStore,
};
use crate::providers::long_tail::LongTailNative;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::collections::HashMap;
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
use wiremock::matchers::{method, path, path_regex};
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
            mime_type: "audio/mp4".into(),
            file_name: Some("voice.m4a".into()),
        })
    }
}

struct QQConformanceVoice;

#[async_trait]
impl VoiceMediaStore for QQConformanceVoice {
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
            file_name: Some("voice.ogg".into()),
        })
    }
}

struct DingTalkConformanceVoice;

#[async_trait]
impl VoiceMediaStore for DingTalkConformanceVoice {
    async fn store_voice(
        &self,
        request: VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD.decode(&request.audio_base64)?;
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("dingtalk-conformance-voice".into()),
                content_type: Some(request.mime_type),
                codec: request.codec,
                file_name: request.file_name,
                size_bytes: Some(bytes.len() as u64),
                duration_ms: request.duration_ms,
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            },
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        use base64::Engine as _;
        const AMR: &[u8] = include_bytes!("../../tests/fixtures/dingtalk/amrnb_mode0.amr");
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(AMR),
            mime_type: "audio/amr".into(),
            file_name: Some("voice.amr".into()),
        })
    }
}

struct ConformanceInboundSink(tokio::sync::mpsc::UnboundedSender<tinychannels_bus::ChannelIngress>);

#[async_trait]
impl crate::channel::ChannelInboundSink for ConformanceInboundSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), ChannelSendError> {
        self.0.send(ingress).map_err(|_| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "conformance sink closed".into(),
            retryable: true,
            ..Default::default()
        })
    }
}

struct DingTalkStream {
    listener: tokio::task::JoinHandle<Result<(), ChannelSendError>>,
    stream: tokio::task::JoinHandle<()>,
    voice_tx: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
    ingress_rx:
        tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<tinychannels_bus::ChannelIngress>>,
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

type PostScope = (String, Option<String>);

struct LongTailFixture {
    id: &'static str,
    channel: Arc<dyn LongTailNative>,
    server: Option<MockServer>,
    fixed_server: Option<crate::providers::http_test_support::HttpsServer>,
    dingtalk_stream: Option<DingTalkStream>,
    /// Message ids the mock returned, in response order.
    response_ids: Arc<Mutex<Vec<String>>>,
    /// Conversation facts associated with post IDs created by the mock wire.
    post_scopes: Arc<Mutex<HashMap<String, PostScope>>>,
    observed: Mutex<Vec<WireObservation>>,
}

impl LongTailFixture {
    async fn start(id: &'static str) -> Self {
        if id == "qq" {
            return Self::start_qq().await;
        }
        if id == "dingtalk" {
            return Self::start_dingtalk().await;
        }
        let server = MockServer::start().await;
        let response_ids = Arc::new(Mutex::new(Vec::new()));
        let post_scopes = Arc::new(Mutex::new(HashMap::new()));
        let counter = Arc::new(AtomicU64::new(7000));
        let ids = response_ids.clone();
        let scopes = post_scopes.clone();
        Mock::given(method("POST"))
            .and(path("/api/v4/posts"))
            .respond_with(move |request: &Request| {
                let id = counter.fetch_add(1, Ordering::SeqCst).to_string();
                ids.lock().push(id.clone());
                let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
                scopes.lock().insert(
                    id.clone(),
                    (
                        body["channel_id"].as_str().unwrap_or_default().to_owned(),
                        body["root_id"].as_str().map(str::to_owned),
                    ),
                );
                ResponseTemplate::new(201).set_body_json(json!({"id": id}))
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v4/users/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "conformance-bot",
                "username": "fixture-bot"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v4/reactions"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "reaction-1"})))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path_regex(
                r"^/api/v4/users/conformance-bot/posts/[0-9]+/reactions/thumbsup$",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "OK"})))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path_regex(r"^/api/v4/posts/[0-9]+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "updated"})))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path_regex(r"^/api/v4/posts/[0-9]+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "OK"})))
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
            dingtalk_stream: None,
            response_ids,
            post_scopes,
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
            .voice_media_store(Arc::new(QQConformanceVoice))
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
            dingtalk_stream: None,
            response_ids,
            post_scopes: Arc::default(),
            observed: Mutex::new(Vec::new()),
        }
    }

    async fn start_dingtalk() -> Self {
        use crate::providers::http_test_support::HttpsServer;
        use tokio_tungstenite::{accept_async, tungstenite::Message};

        let stream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local DingTalk Stream fixture");
        let stream_address = stream_listener.local_addr().unwrap();
        let response_ids = Arc::new(Mutex::new(Vec::new()));
        let next_id = Arc::new(AtomicU64::new(9000));
        let response_ids_for_server = response_ids.clone();
        let counter = next_id.clone();
        let server = HttpsServer::start_with_bytes(
            move |request| match (request.method.as_str(), request.target.as_str()) {
                ("POST", "/v1.0/gateway/connections/open") => (
                    200,
                    json!({"endpoint":format!("ws://{stream_address}/"),"ticket":"conformance-ticket"})
                        .to_string()
                        .into_bytes(),
                ),
                ("POST", "/v1.0/oauth2/accessToken") => {
                    (200, r#"{"accessToken":"fixture-token"}"#.as_bytes().to_vec())
                }
                ("POST", "/v1.0/robot/messageFiles/download") => (
                    200,
                    json!({"downloadUrl":"https://api.dingtalk.com/media/voice"})
                        .to_string()
                        .into_bytes(),
                ),
                ("GET", "/media/voice") => (
                    200,
                    include_bytes!("../../tests/fixtures/dingtalk/amrnb_mode0.amr").to_vec(),
                ),
                ("POST", target) if target.starts_with("/media/upload?") => {
                    (200, r#"{"media_id":"dingtalk-uploaded-media"}"#.as_bytes().to_vec())
                }
                ("POST", "/v1.0/robot/groupMessages/send") => {
                    let id = counter.fetch_add(1, Ordering::SeqCst).to_string();
                    response_ids_for_server.lock().push(id.clone());
                    (200, json!({"processQueryKey":id}).to_string().into_bytes())
                }
                _ => (404, r#"{"message":"unexpected request"}"#.as_bytes().to_vec()),
            },
            "audio/amr",
        )
        .await;

        let (callback_ready_tx, callback_ready_rx) = tokio::sync::oneshot::channel();
        let (voice_tx, mut voice_rx) =
            tokio::sync::mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<()>>();
        let stream = tokio::spawn(async move {
            let (socket, _) =
                tokio::time::timeout(std::time::Duration::from_secs(8), stream_listener.accept())
                    .await
                    .expect("DingTalk adapter should connect to the local Stream fixture")
                    .unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            websocket
                .send(Message::Text(
                    json!({
                        "type":"SYSTEM",
                        "headers":{"messageId":"conformance-ping","topic":"ping"}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let _pong = tokio::time::timeout(std::time::Duration::from_secs(8), websocket.next())
                .await
                .expect("DingTalk Stream ping should be acknowledged")
                .unwrap()
                .unwrap();
            websocket
                .send(Message::Text(
                    json!({
                        "type":"CALLBACK",
                        "headers":{"messageId":"conformance-callback","topic":"/v1.0/im/bot/messages/get"},
                        "data":json!({
                            "msgId":"provider-ingress-1",
                            "msgtype":"text",
                            "robotCode":"conformance-robot",
                            "senderStaffId":"conformance-user",
                            "senderId":"$:conformance-user",
                            "conversationType":"2",
                            "conversationId":"chan1",
                            "text":{"content":"establish context"}
                        }).to_string()
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let _ack = tokio::time::timeout(std::time::Duration::from_secs(8), websocket.next())
                .await
                .expect("DingTalk callback should be acknowledged after ingress")
                .unwrap()
                .unwrap();
            let _ = callback_ready_tx.send(());
            while let Some(acknowledged) = voice_rx.recv().await {
                websocket
                    .send(Message::Text(
                        json!({
                            "type":"CALLBACK",
                            "headers":{"messageId":"voice-frame-1","topic":"/v1.0/im/bot/messages/get"},
                            "data":json!({
                                "msgId":"voice-message-1",
                                "msgtype":"audio",
                                "robotCode":"conformance-robot",
                                "senderStaffId":"conformance-user",
                                "senderId":"$:conformance-user",
                                "conversationType":"2",
                                "conversationId":"chan1",
                                "content":{"downloadCode":"voice-download-1","recognition":"turn left"}
                            }).to_string()
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
                let ack = tokio::time::timeout(std::time::Duration::from_secs(8), websocket.next())
                    .await
                    .expect("DingTalk voice callback should be acknowledged after typed ingress")
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&ack.to_string()).unwrap()["code"],
                    200
                );
                let _ = acknowledged.send(());
            }
        });

        let host: Arc<dyn crate::host::ChannelHost> = crate::ChannelHostBuilder::new()
            .media_repository(Arc::new(ConformanceMedia))
            .voice_media_store(Arc::new(DingTalkConformanceVoice))
            .build();
        let config = ChannelsConfig {
            dingtalk: Some(DingTalkConfig {
                client_id: "fixture-app-key".into(),
                client_secret: "fixture-app-secret".into(),
                allowed_users: vec!["conformance-user".into()],
            }),
            ..Default::default()
        };
        let clients = FixtureHttpClients(server.client.clone());
        let channel = build_long_tail_channel_with_host(&config, &host, &clients, "dingtalk")
            .expect("configured DingTalk adapter built through provider factory");
        let (sink_tx, mut sink_rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = Arc::new(ConformanceInboundSink(sink_tx));
        let listener_channel = channel.clone();
        let listener = tokio::spawn(async move {
            ChannelAdapter::start(listener_channel.as_ref(), sink.as_ref()).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(8), callback_ready_rx)
            .await
            .expect("DingTalk callback should establish native conversation context")
            .expect("DingTalk Stream fixture should complete callback");
        let initial_ingress =
            tokio::time::timeout(std::time::Duration::from_secs(5), sink_rx.recv())
                .await
                .expect("DingTalk context callback should reach the configured sink")
                .expect("DingTalk context callback should be an ingress event");
        assert!(matches!(
            initial_ingress,
            tinychannels_bus::ChannelIngress::Inbound(ref envelope)
                if envelope.message_id == "provider-ingress-1"
        ));
        Self {
            id: "dingtalk",
            channel,
            server: None,
            fixed_server: Some(server),
            dingtalk_stream: Some(DingTalkStream {
                listener,
                stream,
                voice_tx,
                ingress_rx: tokio::sync::Mutex::new(sink_rx),
            }),
            response_ids,
            post_scopes: Arc::default(),
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
            dingtalk_stream: None,
            response_ids: Arc::default(),
            post_scopes: Arc::default(),
            observed: Mutex::new(Vec::new()),
        }
    }

    fn intent(&self, feature: Feature, payload: OutboundPayload) -> ChannelOutboundIntent {
        let conversation = if self.id == "qq" {
            ConversationRef {
                id: format!("user:{CHANNEL}"),
                ..Default::default()
            }
        } else if self.id == "dingtalk" {
            ConversationRef {
                kind: tinychannels_bus::ConversationKind::Group,
                id: CHANNEL.into(),
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
            if url_path == "/api/v4/users/me" {
                continue;
            }
            let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
            let (operation, message_ids, kinds, post_id, request_body) =
                if url_path == "/api/v4/posts" {
                    let kinds = if body.get("file_ids").is_some() {
                        uploaded_kind
                            .take()
                            .into_iter()
                            .map(str::to_owned)
                            .collect()
                    } else {
                        vec!["text".to_owned()]
                    };
                    let response_id = ids.next().unwrap_or_default();
                    ("send", vec![response_id.clone()], kinds, response_id, body)
                } else if url_path == "/api/v4/reactions" {
                    let post_id = body["post_id"].as_str().unwrap_or_default().to_owned();
                    ("reaction_send", Vec::new(), Vec::new(), post_id, body)
                } else if url_path.starts_with("/api/v4/users/")
                    && url_path.contains("/posts/")
                    && url_path.contains("/reactions/")
                {
                    let post_id = url_path
                        .split("/posts/")
                        .nth(1)
                        .and_then(|suffix| suffix.split('/').next())
                        .unwrap_or_default()
                        .to_owned();
                    (
                        "reaction_send",
                        Vec::new(),
                        Vec::new(),
                        post_id,
                        json!({"path": url_path}),
                    )
                } else if matches!(request.method.as_str(), "PUT" | "DELETE")
                    && url_path.starts_with("/api/v4/posts/")
                {
                    let post_id = url_path.rsplit('/').next().unwrap_or_default().to_owned();
                    let operation = if request.method.as_str() == "PUT" {
                        "edit"
                    } else {
                        "delete"
                    };
                    let kinds = if operation == "edit" {
                        vec!["text".to_owned()]
                    } else {
                        Vec::new()
                    };
                    let observation = if operation == "delete" {
                        json!({"path": url_path})
                    } else {
                        body
                    };
                    (
                        operation,
                        vec![post_id.clone()],
                        kinds,
                        post_id,
                        observation,
                    )
                } else {
                    ("typing", Vec::new(), Vec::new(), String::new(), body)
                };
            let scope = self.post_scopes.lock().get(&post_id).cloned();
            let (channel_id, thread) = match scope {
                Some(scope) => scope,
                None if operation == "send" => (
                    request_body["channel_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    request_body["root_id"].as_str().map(str::to_owned),
                ),
                None if operation == "typing" => (
                    request_body["channel_id"]
                        .as_str()
                        .unwrap_or(CHANNEL)
                        .to_owned(),
                    request_body["root_id"]
                        .as_str()
                        .or_else(|| request_body["parent_id"].as_str())
                        .map(str::to_owned),
                ),
                None => (String::new(), None),
            };
            self.observed.lock().push(WireObservation {
                feature,
                operation: operation.into(),
                channel: account(self.id),
                conversation: ConversationRef {
                    id: channel_id,
                    thread_id: thread.clone(),
                    ..Default::default()
                },
                reply_to_id: None,
                thread_id: thread,
                payload_kinds: kinds,
                platform_message_ids: message_ids,
                request: request_body,
            });
        }
    }

    fn record_fixed(&self, feature: Feature, before: usize) {
        let Some(server) = &self.fixed_server else {
            return;
        };
        let requests = server.requests.lock().unwrap();
        if self.id == "dingtalk" {
            self.record_dingtalk(feature, before, &requests);
            return;
        }
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
                conversation: qq_conversation_from_request(request),
                reply_to_id: None,
                thread_id: None,
                payload_kinds,
                platform_message_ids: ids.next().into_iter().collect(),
                request: request.body.clone(),
            });
        }
    }

    fn record_dingtalk(
        &self,
        feature: Feature,
        before: usize,
        requests: &[crate::providers::http_test_support::Request],
    ) {
        let mut ids = std::mem::take(&mut *self.response_ids.lock()).into_iter();
        let mut uploaded_kind = None;
        for request in &requests[before..] {
            if request.target.starts_with("/media/upload?") {
                let body = String::from_utf8_lossy(&request.raw_body).to_ascii_lowercase();
                uploaded_kind = if body.contains("image/png") {
                    Some("image")
                } else if body.contains("application/pdf") {
                    Some("file")
                } else if body.contains("audio/amr") {
                    Some("voice")
                } else {
                    None
                };
                continue;
            }
            if request.target != "/v1.0/robot/groupMessages/send" {
                continue;
            }
            let msg_key = request.body["msgKey"].as_str().unwrap_or_default();
            let payload_kinds = match msg_key {
                "sampleText" => vec!["text".to_owned()],
                "sampleFile" => uploaded_kind
                    .take()
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                "sampleAudio" => uploaded_kind
                    .take()
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                _ => Vec::new(),
            };
            self.observed.lock().push(WireObservation {
                feature,
                operation: "send".into(),
                channel: account(self.id),
                conversation: ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Group,
                    id: request.body["openConversationId"]
                        .as_str()
                        .unwrap_or_default()
                        .into(),
                    ..Default::default()
                },
                reply_to_id: None,
                thread_id: None,
                payload_kinds,
                platform_message_ids: ids.next().into_iter().collect(),
                request: request.body.clone(),
            });
        }
    }

    async fn stop_dingtalk(&self) {
        if let Some(stream) = &self.dingtalk_stream {
            let _ = ChannelAdapter::stop(self.channel.as_ref()).await;
            stream.listener.abort();
            stream.stream.abort();
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
        let outcome = ChannelAdapter::send_outcome(self.channel.as_ref(), intent).await;
        self.record(feature, before).await;
        outcome.map(Some)
    }
}

fn qq_conversation_from_request(
    request: &crate::providers::http_test_support::Request,
) -> ConversationRef {
    let mut segments = request.target.split('/');
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some(""), Some("v2"), Some("users"), Some(id), Some("messages")) => ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: id.into(),
            ..Default::default()
        },
        (Some(""), Some("v2"), Some("groups"), Some(id), Some("messages")) => ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: id.into(),
            ..Default::default()
        },
        _ => panic!("unexpected QQ message path: {}", request.target),
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

impl LongTailFixture {
    async fn receive_dingtalk_voice_in(
        &self,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let stream = self
            .dingtalk_stream
            .as_ref()
            .ok_or_else(|| ChannelSendError {
                kind: SendErrorKind::BadFormat,
                message: "DingTalk voice ingress fixture is not configured".into(),
                unsupported: true,
                ..Default::default()
            })?;
        let (acknowledged_tx, acknowledged_rx) = tokio::sync::oneshot::channel();
        stream
            .voice_tx
            .send(acknowledged_tx)
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "DingTalk voice fixture stream stopped".into(),
                retryable: true,
                ..Default::default()
            })?;
        let ingress = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.ingress_rx.lock().await.recv(),
        )
        .await
        .ok()
        .flatten()
        .ok_or_else(|| ChannelSendError {
            kind: SendErrorKind::Transient,
            message: "DingTalk voice callback was not delivered to the configured sink".into(),
            retryable: true,
            ..Default::default()
        })?;
        tokio::time::timeout(std::time::Duration::from_secs(5), acknowledged_rx)
            .await
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "DingTalk voice callback was not acknowledged".into(),
                retryable: true,
                ..Default::default()
            })?
            .map_err(|_| ChannelSendError {
                kind: SendErrorKind::Transient,
                message: "DingTalk voice fixture acknowledgement was dropped".into(),
                retryable: true,
                ..Default::default()
            })?;
        let tinychannels_bus::ChannelIngress::Inbound(envelope) = ingress else {
            return Err(ChannelSendError {
                kind: SendErrorKind::BadFormat,
                message: "DingTalk voice fixture received a non-message ingress".into(),
                ..Default::default()
            });
        };
        assert_eq!(envelope.channel.id, "dingtalk");
        assert_eq!(
            envelope.channel.account_id.as_deref(),
            Some("fixture-app-key")
        );
        assert_eq!(envelope.conversation.id, "chan1");
        assert_eq!(envelope.message_id, "voice-message-1");
        let note = envelope
            .parts
            .iter()
            .find_map(|part| match part {
                tinychannels_bus::ContentPart::VoiceNote { voice_note } => Some(voice_note),
                _ => None,
            })
            .expect("configured DingTalk voice should be stored as a typed voice note");
        assert_eq!(
            note.media.opaque_id.as_deref(),
            Some("dingtalk-conformance-voice")
        );
        assert_eq!(note.media.kind, tinychannels_bus::MediaKind::Audio);
        assert_eq!(
            note.transcript
                .as_ref()
                .map(|transcript| transcript.text.as_str()),
            Some("turn left")
        );
        let server = self.fixed_server.as_ref().expect("DingTalk HTTPS fixture");
        let requests = server.requests.lock().unwrap();
        let download = requests
            .iter()
            .find(|request| request.target == "/v1.0/robot/messageFiles/download")
            .expect("the configured adapter must fetch the incoming voice bytes");
        assert_eq!(download.body["downloadCode"], "voice-download-1");
        self.observed.lock().push(WireObservation {
            feature: Feature::VoiceIn,
            operation: "voice_in".into(),
            channel: envelope.channel.clone(),
            conversation: envelope.conversation.clone(),
            reply_to_id: envelope.reply_to_id.clone(),
            thread_id: envelope.conversation.thread_id.clone(),
            payload_kinds: vec!["audio".into(), "transcript".into()],
            platform_message_ids: vec![envelope.message_id.clone()],
            request: json!({
                "type":"CALLBACK",
                "headers":{"messageId":"voice-frame-1","topic":"/v1.0/im/bot/messages/get"},
                "data":{
                    "msgId":"voice-message-1",
                    "msgtype":"audio",
                    "robotCode":"conformance-robot",
                    "senderStaffId":"conformance-user",
                    "senderId":"$:conformance-user",
                    "conversationType":"2",
                    "conversationId":"chan1",
                    "content":{"downloadCode":"voice-download-1","recognition":"turn left"}
                }
            }),
        });
        Ok(None)
    }
}

#[async_trait]
impl AdapterFixture for LongTailFixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        self.channel.as_ref()
    }

    fn expected_scope(&self, feature: Feature) -> ExpectedScope {
        ExpectedScope {
            channel: if self.id == "dingtalk" && feature == Feature::VoiceIn {
                ChannelRef {
                    id: "dingtalk".into(),
                    account_id: Some("fixture-app-key".into()),
                }
            } else {
                account(self.id)
            },
            conversation: if self.id == "qq" {
                ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Dm,
                    id: CHANNEL.into(),
                    ..Default::default()
                }
            } else if self.id == "dingtalk" {
                ConversationRef {
                    kind: tinychannels_bus::ConversationKind::Group,
                    id: CHANNEL.into(),
                    ..Default::default()
                }
            } else {
                conversation()
            },
            reply_to_id: None,
            thread_id: (self.id != "qq" && self.id != "dingtalk").then(|| ROOT.into()),
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
            Feature::VoiceIn if self.id == "dingtalk" => {
                LongTailFixture::receive_dingtalk_voice_in(self).await
            }
            // A platform without thread addressing has nothing to send to.
            Feature::ThreadAddressing if !channel.declared_capabilities().addressing.thread_id => {
                let mut intent = self.intent(
                    feature,
                    OutboundPayload::Text {
                        text: "thread conformance".into(),
                    },
                );
                intent.thread_id = Some(ROOT.into());
                self.refused(ChannelAdapter::send_outcome(channel, intent))
                    .await
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
                    self.refused(ChannelAdapter::send_outcome(channel, intent))
                        .await
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                self.refused(ChannelAdapter::send_outcome(
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
                            codec: Some(if self.id == "dingtalk" {
                                "amr_nb".into()
                            } else {
                                "m4a_aac".into()
                            }),
                            duration_ms: Some(1500),
                        },
                    },
                )
                .await
            }
            Feature::VoiceSynthesis => {
                self.refused(ChannelAdapter::send_outcome(
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
                if !channel.declared_capabilities().reactions {
                    return self
                        .refused(crate::channel::LegacyChannelReaction::send_reaction(
                            channel,
                            &account(self.id),
                            &target,
                            "m1",
                            &tinychannels_bus::channel::ChannelReaction::default(),
                            false,
                        ))
                        .await;
                }
                let setup = ChannelAdapter::send_outcome(
                    channel,
                    self.intent(
                        feature,
                        OutboundPayload::Text {
                            text: "reaction target".into(),
                        },
                    ),
                )
                .await?;
                let message_id = setup
                    .receipt
                    .primary_platform_message_id
                    .as_deref()
                    .ok_or_else(|| crate::providers::native::pre_dispatch("missing post ID"))?;
                let before = self.request_count().await;
                crate::channel::LegacyChannelReaction::send_reaction(
                    channel,
                    &account(self.id),
                    &target,
                    message_id,
                    &tinychannels_bus::channel::ChannelReaction {
                        value: "thumbsup".into(),
                        custom_emoji_id: None,
                    },
                    false,
                )
                .await?;
                self.record(feature, before).await;
                Ok(None)
            }
            Feature::Edit => {
                if !channel
                    .declared_capabilities()
                    .actions
                    .supported
                    .iter()
                    .any(|action| action == "edit")
                {
                    return self
                        .refused(ChannelEdit::edit(
                            channel,
                            &MessageReceipt::default(),
                            self.intent(feature, OutboundPayload::Text { text: "e".into() }),
                        ))
                        .await;
                }
                let setup = ChannelAdapter::send_outcome(
                    channel,
                    self.intent(
                        feature,
                        OutboundPayload::Text {
                            text: "edit target".into(),
                        },
                    ),
                )
                .await?;
                let before = self.request_count().await;
                let receipt = ChannelEdit::edit(
                    channel,
                    &setup.receipt,
                    self.intent(
                        feature,
                        OutboundPayload::Text {
                            text: "edited text".into(),
                        },
                    ),
                )
                .await?;
                self.record(feature, before).await;
                Ok(Some(ChannelSendOutcome {
                    receipt,
                    error: None,
                }))
            }
            Feature::Delete => {
                if !channel
                    .declared_capabilities()
                    .actions
                    .supported
                    .iter()
                    .any(|action| action == "delete")
                {
                    return self
                        .refused(ChannelDelete::delete(channel, &MessageReceipt::default()))
                        .await;
                }
                let setup = ChannelAdapter::send_outcome(
                    channel,
                    self.intent(
                        feature,
                        OutboundPayload::Text {
                            text: "delete target".into(),
                        },
                    ),
                )
                .await?;
                let before = self.request_count().await;
                ChannelDelete::delete(channel, &setup.receipt).await?;
                self.record(feature, before).await;
                Ok(None)
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
            Feature::ManagedReaction
            | Feature::ManagedThreadCreate
            | Feature::ManagedThreadUpdate
            | Feature::ManagedThreadList => Err(ChannelSendError::default()),
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
    check_feature(&fixture, Feature::Reactions)
        .await
        .expect("Mattermost reaction fixture should pass when replayed alone");
    let observations = fixture.observations();
    let reaction = observations
        .iter()
        .find(|observation| observation.feature == Feature::Reactions)
        .expect("Mattermost reaction must have an actual wire observation");
    assert_eq!(reaction.operation, "reaction_send");
    assert!(
        reaction.request["post_id"]
            .as_str()
            .is_some_and(|id| id.parse::<u64>().is_ok())
    );
    assert_eq!(reaction.request["emoji_name"], "thumbsup");
    check_feature(&fixture, Feature::Edit)
        .await
        .expect("Mattermost edit fixture should pass when replayed alone");
    let observations = fixture.observations();
    let edit = observations
        .iter()
        .find(|observation| observation.feature == Feature::Edit)
        .expect("Mattermost edit must have an actual wire observation");
    assert_eq!(edit.operation, "edit");
    assert_eq!(edit.request["message"], "edited text");
    assert_eq!(edit.request["id"], edit.platform_message_ids[0]);
    check_feature(&fixture, Feature::Delete)
        .await
        .expect("Mattermost delete fixture should pass when replayed alone");
    let observations = fixture.observations();
    let delete = observations
        .iter()
        .find(|observation| observation.feature == Feature::Delete)
        .expect("Mattermost delete must have an actual wire observation");
    assert_eq!(delete.operation, "delete");
    assert_eq!(
        delete.request["path"],
        format!("/api/v4/posts/{}", delete.platform_message_ids[0])
    );
}

#[tokio::test]
async fn the_factory_built_qq_adapter_passes_the_full_conformance_matrix() {
    let fixture = LongTailFixture::start("qq").await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), Feature::ALL.len());
    let failed = failures(&results);
    assert!(failed.is_empty(), "QQ conformance failures: {failed:#?}");
    let requests = fixture
        .fixed_server
        .as_ref()
        .unwrap()
        .requests
        .lock()
        .unwrap();
    let message_sends = requests
        .iter()
        .filter(|request| request.target.ends_with("/messages"))
        .collect::<Vec<_>>();
    assert_eq!(
        message_sends.len(),
        5,
        "declared features must send once each"
    );
    assert!(
        requests.iter().any(|request| {
            request.target == "/app/getAppAccessToken"
                && request.body == json!({"appId": "fixture-app", "clientSecret": "fixture-secret"})
        }),
        "configured credentials must be used in the actual OAuth request: {requests:#?}"
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.target.starts_with("/v2/"))
            .all(|request| request
                .headers
                .to_ascii_lowercase()
                .contains("authorization: qqbot fixture-token")),
        "every QQ upload and send must carry the issued token: {requests:#?}"
    );
    assert!(
        message_sends
            .iter()
            .all(|request| request.target == "/v2/users/chan1/messages"),
        "the actual wire recipient must match the configured DM: {message_sends:#?}"
    );
    assert_eq!(message_sends[0].body["msg_type"], 0);
    assert!(
        message_sends[1..]
            .iter()
            .all(|request| request.body["msg_type"] == 7)
    );
    let uploads = requests
        .iter()
        .filter(|request| request.target.ends_with("/files"))
        .collect::<Vec<_>>();
    assert_eq!(
        uploads.len(),
        4,
        "image, video, file, and voice each upload"
    );
    assert_eq!(
        uploads
            .iter()
            .map(|request| request.body["file_type"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [1, 2, 4, 3],
        "each declared media feature must use its matching QQ upload type"
    );
    assert!(
        uploads
            .iter()
            .all(|request| request.target == "/v2/users/chan1/files")
    );
    assert!(
        message_sends[1..]
            .iter()
            .all(|request| request.body["media"]["file_info"] == "fixture-file"),
        "messages must use the file_info returned by the upload endpoint"
    );
}

#[tokio::test]
async fn the_factory_built_dingtalk_adapter_passes_the_full_conformance_matrix() {
    let fixture = LongTailFixture::start("dingtalk").await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), Feature::ALL.len());
    let failed = failures(&results);
    assert!(
        failed.is_empty(),
        "DingTalk conformance failures: {failed:#?}"
    );

    {
        let requests = fixture
            .fixed_server
            .as_ref()
            .unwrap()
            .requests
            .lock()
            .unwrap();
        assert!(
            requests.iter().any(|request| {
                request.target == "/v1.0/gateway/connections/open"
                    && request.body["clientId"] == "fixture-app-key"
                    && request.body["clientSecret"] == "fixture-app-secret"
            }),
            "configured credentials must be used in the actual Stream registration: {requests:#?}"
        );
        assert!(
            requests.iter().any(|request| {
                request.target == "/v1.0/oauth2/accessToken"
                    && request.body
                        == json!({"appKey":"fixture-app-key","appSecret":"fixture-app-secret"})
            }),
            "configured credentials must be used in the actual token request: {requests:#?}"
        );
        let sends = requests
            .iter()
            .filter(|request| request.target == "/v1.0/robot/groupMessages/send")
            .collect::<Vec<_>>();
        assert_eq!(
            sends.len(),
            3,
            "text, file, and voice are declared send features"
        );
        assert!(
            sends.iter().all(|request| {
                request.body["robotCode"] == "conformance-robot"
                    && request.body["openConversationId"] == CHANNEL
                    && request
                        .headers
                        .to_ascii_lowercase()
                        .contains("x-acs-dingtalk-access-token: fixture-token")
            }),
            "sends must use callback-established robot context, configured group, and issued token: {sends:#?}"
        );
        let uploads = requests
            .iter()
            .filter(|request| request.target.starts_with("/media/upload?"))
            .collect::<Vec<_>>();
        assert_eq!(uploads.len(), 2, "file and voice each upload");
        assert!(
            sends.iter().any(|request| {
                request.body["msgKey"] == "sampleFile"
                    && request.body["msgParam"]
                        .as_str()
                        .is_some_and(|params| params.contains("dingtalk-uploaded-media"))
            }),
            "the file response media_id must be used by the subsequent send: {sends:#?}"
        );
        assert!(
            sends.iter().any(|request| {
                request.body["msgKey"] == "sampleAudio"
                    && request.body["msgParam"]
                        .as_str()
                        .and_then(|params| serde_json::from_str::<serde_json::Value>(params).ok())
                        .is_some_and(|params| params["mediaId"] == "dingtalk-uploaded-media")
            }),
            "the voice response media_id must be used by the subsequent send: {sends:#?}"
        );
        assert!(
            uploads
                .iter()
                .all(|request| request.target.contains("access_token=fixture-token")),
            "both media uploads must use the token issued by the OAuth endpoint: {uploads:#?}"
        );
    }
    fixture.stop_dingtalk().await;
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
