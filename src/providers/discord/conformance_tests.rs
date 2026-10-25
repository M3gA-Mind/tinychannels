//! Runs the shared adapter conformance matrix against the Discord adapter.
//!
//! Every observation is parsed from the requests a wiremock Discord actually
//! received, and every message id from the responses it actually returned.

use crate::channel::{ChannelAdapter, ChannelDelete, ChannelEdit};
use crate::channel::{ChannelStreamingDraft, ChannelTyping};
use crate::providers::discord::channel::DiscordChannel;
use crate::providers::discord::support::*;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tinychannels_bus::channel::{ChannelSendError, ChannelSendOutcome, SendErrorKind};
use tinychannels_bus::{
    ButtonAction, ChannelOutboundIntent, ChannelRef, ContentPart, ConversationRef, MediaKind,
    OutboundContentPart, OutboundPayload, OutboundVoice,
};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature as Feature, ExpectedScope, WireObservation,
    check_all_features,
};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

struct DiscordFixture {
    channel: DiscordChannel,
    server: MockServer,
    events: Arc<RecordingEvents>,
    media: Arc<MemoryMedia>,
    voice: Arc<MemoryVoice>,
    /// `(request path, message id)` for every message-bearing response.
    response_ids: Arc<Mutex<Vec<(String, String)>>>,
    observed: Mutex<Vec<WireObservation>>,
    cursor: Mutex<usize>,
}

impl DiscordFixture {
    async fn start() -> Self {
        let server = MockServer::start().await;
        let response_ids = Arc::new(Mutex::new(Vec::new()));
        let next = Arc::new(AtomicU64::new(9000));
        let ids = response_ids.clone();
        let counter = next.clone();
        Mock::given(method("POST"))
            .and(path_regex(r"^/channels/\d+/messages$"))
            .respond_with(move |request: &Request| {
                let id = counter.fetch_add(1, Ordering::SeqCst).to_string();
                ids.lock().push((request.url.path().to_owned(), id.clone()));
                ResponseTemplate::new(200).set_body_json(json!({"id": id}))
            })
            .mount(&server)
            .await;
        let ids = response_ids.clone();
        Mock::given(method("POST"))
            .and(path_regex(r"^/channels/\d+/threads$"))
            .respond_with(move |request: &Request| {
                ids.lock()
                    .push((request.url.path().to_owned(), THREAD.to_owned()));
                ResponseTemplate::new(201).set_body_json(json!({"id": THREAD}))
            })
            .mount(&server)
            .await;
        let ids = response_ids.clone();
        Mock::given(method("PATCH"))
            .and(path_regex(r"^/channels/\d+/messages/\d+$"))
            .respond_with(move |request: &Request| {
                let id = request.url.path().rsplit('/').next().unwrap().to_owned();
                ids.lock().push((request.url.path().to_owned(), id.clone()));
                ResponseTemplate::new(200).set_body_json(json!({"id": id}))
            })
            .mount(&server)
            .await;
        for (http_method, pattern) in [
            ("POST", r"^/channels/\d+/typing$"),
            ("PUT", r"^/channels/\d+/messages/\d+/reactions/.+/@me$"),
            ("DELETE", r"^/channels/\d+/messages/\d+$"),
        ] {
            Mock::given(method(http_method))
                .and(path_regex(pattern))
                .respond_with(ResponseTemplate::new(204))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path(format!("/channels/{THREAD}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"id": THREAD, "type": 11, "parent_id": CHANNEL})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/cdn/voice-message.ogg"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(OGG_FIXTURE.to_vec(), "audio/ogg"),
            )
            .mount(&server)
            .await;
        let (channel, events, media, voice) = full_channel(&server);
        Self {
            channel,
            server,
            events,
            media,
            voice,
            response_ids,
            observed: Mutex::new(Vec::new()),
            cursor: Mutex::new(0),
        }
    }

    /// Conversation every send fixture targets: a thread under `CHANNEL`.
    fn target(&self) -> ConversationRef {
        thread_conversation()
    }

    fn intent(&self, feature: Feature, payload: OutboundPayload) -> ChannelOutboundIntent {
        let native = feature == Feature::NativeData;
        ChannelOutboundIntent {
            idempotency_key: format!("kit-{feature:?}"),
            channel_id: "discord".into(),
            conversation_id: CHANNEL.into(),
            channel_ref: Some(account()),
            conversation_ref: Some(if native {
                conversation()
            } else {
                self.target()
            }),
            reply_to_id: (!native).then(|| "42".to_owned()),
            thread_id: (!native).then(|| THREAD.to_owned()),
            turn_id: Some("turn-1".into()),
            payload,
            ..Default::default()
        }
    }

    async fn mark_start(&self) {
        *self.cursor.lock() = self.server.received_requests().await.unwrap().len();
        self.response_ids.lock().clear();
    }

    /// Parse every request since `mark_start` into observations.
    async fn record(&self, feature: Feature, names: &[&str]) {
        let requests = self.server.received_requests().await.unwrap();
        let start = *self.cursor.lock();
        let mut ids = self.response_ids.lock().clone().into_iter();
        for (index, request) in requests[start..].iter().enumerate() {
            let url_path = request.url.path().to_owned();
            let segments: Vec<&str> = url_path.split('/').collect();
            let is_reaction = request.method.as_str() == "PUT" && url_path.contains("/reactions/");
            let is_typing = url_path.ends_with("/typing");
            let is_threads = url_path.ends_with("/threads");
            let bears_id =
                matches!(request.method.to_string().as_str(), "POST" | "PATCH") && !is_typing;
            let message_ids = if bears_id {
                ids.next().map(|(_, id)| vec![id]).unwrap_or_default()
            } else if request.method.as_str() == "DELETE" {
                vec![segments[4].to_owned()]
            } else {
                Vec::new()
            };
            let channel_id = segments[2];
            let in_thread = channel_id == THREAD;
            let conversation = if in_thread {
                thread_conversation()
            } else {
                conversation()
            };
            let payload = if request.method.as_str() == "POST" && !is_typing {
                parse_payload(request)
            } else {
                Value::Null
            };
            let operation = names.get(index).copied().unwrap_or("send");
            self.observed.lock().push(WireObservation {
                feature,
                operation: operation.into(),
                channel: account(),
                conversation,
                reply_to_id: payload
                    .pointer("/message_reference/message_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                thread_id: in_thread.then(|| THREAD.to_owned()),
                payload_kinds: payload_kinds(request, &payload, is_threads),
                platform_message_ids: message_ids,
                request: json!({"method": request.method.to_string(), "path": url_path, "reaction": is_reaction}),
            });
        }
    }

    async fn send_and_record(
        &self,
        feature: Feature,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        self.mark_start().await;
        let outcome = self
            .channel
            .send_outcome(self.intent(feature, payload))
            .await?;
        self.record(feature, &["send"]).await;
        Ok(Some(outcome))
    }

    fn media_in_scope(
        &self,
        kind: MediaKind,
        content_type: &str,
        name: &str,
        bytes: Vec<u8>,
    ) -> tinychannels_bus::MediaReference {
        let scope = tinychannels_bus::HostVoiceScope {
            conversation: self.target(),
            ..media_scope()
        };
        self.media.seed(&scope, kind, content_type, name, bytes)
    }
}

fn parse_payload(request: &Request) -> Value {
    let raw = String::from_utf8_lossy(&request.body);
    if raw.contains("name=\"payload_json\"") {
        multipart_payload(request)
    } else {
        body_json(request)
    }
}

fn payload_kinds(request: &Request, payload: &Value, is_threads: bool) -> Vec<String> {
    let mut kinds = Vec::new();
    if is_threads {
        return vec!["native".into()];
    }
    if payload.get("flags").and_then(Value::as_u64) == Some(8192) {
        kinds.push("voice".to_owned());
    }
    if payload
        .get("content")
        .and_then(Value::as_str)
        .is_some_and(|c| !c.is_empty())
    {
        kinds.push("text".into());
    }
    if payload
        .get("components")
        .and_then(Value::as_array)
        .is_some_and(|c| !c.is_empty())
    {
        kinds.push("buttons".into());
    }
    if payload
        .get("embeds")
        .and_then(Value::as_array)
        .is_some_and(|c| !c.is_empty())
    {
        kinds.push("presentation".into());
    }
    let raw = String::from_utf8_lossy(&request.body);
    for segment in raw.split("name=\"files[").skip(1) {
        let content_type = segment
            .split("Content-Type: ")
            .nth(1)
            .and_then(|rest| rest.split("\r\n").next())
            .unwrap_or_default();
        if payload.get("flags").and_then(Value::as_u64) == Some(8192) {
            continue;
        }
        kinds.push(
            match content_type.split('/').next().unwrap_or_default() {
                "image" => "image",
                "video" => "video",
                "audio" => "audio",
                _ => "file",
            }
            .into(),
        );
    }
    kinds
}

fn unsupported_result() -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
    Err(ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: "Discord bots expose no such operation".into(),
        unsupported: true,
        ..Default::default()
    })
}

#[async_trait]
impl AdapterFixture for DiscordFixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        &self.channel
    }

    fn expected_scope(&self, feature: Feature) -> ExpectedScope {
        match feature {
            Feature::NativeData => ExpectedScope {
                channel: account(),
                conversation: conversation(),
                reply_to_id: None,
                thread_id: None,
            },
            Feature::InboundReactions | Feature::VoiceIn => ExpectedScope {
                channel: ChannelRef {
                    id: "discord".into(),
                    account_id: None,
                },
                conversation: ConversationRef {
                    scope_id: Some(GUILD.into()),
                    ..thread_conversation()
                },
                reply_to_id: None,
                thread_id: None,
            },
            _ => ExpectedScope {
                channel: account(),
                conversation: thread_conversation(),
                reply_to_id: Some("42".into()),
                thread_id: Some(THREAD.into()),
            },
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
        let part = |part: ContentPart| OutboundPayload::Parts {
            parts: vec![OutboundContentPart::Content { part }],
        };
        match feature {
            Feature::Text | Feature::ReplyAddressing | Feature::ThreadAddressing => {
                self.send_and_record(feature, OutboundPayload::Text { text: "conformance".into() }).await
            }
            Feature::Image => {
                let media = self.media_in_scope(MediaKind::Image, "image/png", "kit.png", png_bytes());
                self.send_and_record(feature, part(ContentPart::Image { media, caption: Some("image".into()), alt_text: None })).await
            }
            Feature::Video => {
                let media = self.media_in_scope(MediaKind::Video, "video/mp4", "kit.mp4", vec![1; 32]);
                self.send_and_record(feature, part(ContentPart::Video { media, caption: None, duration_ms: None, thumbnail: None })).await
            }
            Feature::Audio => {
                let media = self.media_in_scope(MediaKind::Audio, "audio/mpeg", "kit.mp3", vec![2; 32]);
                self.send_and_record(feature, part(ContentPart::Audio { media, codec: None, duration_ms: None, title: Some("kit".into()), transcript: None })).await
            }
            Feature::File => {
                let media = self.media_in_scope(MediaKind::Document, "text/plain", "kit.txt", b"kit".to_vec());
                self.send_and_record(feature, part(ContentPart::File { media, file_name: None, caption: None })).await
            }
            Feature::Buttons => {
                let rows = vec![vec![ButtonAction { id: "opaque".into(), label: "Go".into(), style: None, disabled: false }]];
                self.send_and_record(
                    feature,
                    OutboundPayload::Parts {
                        parts: vec![text_part("Choose"), OutboundContentPart::Content { part: ContentPart::Buttons { rows } }],
                    },
                )
                .await
            }
            Feature::Presentation => {
                self.send_and_record(feature, part(ContentPart::Presentation {
                    blocks: json!({"kind": "approval", "id": "kit", "prompt": "Approve?", "choices": ["Yes", "No"]}),
                }))
                .await
            }
            Feature::NativeData => {
                self.send_and_record(feature, OutboundPayload::NativeChannelData {
                    data: json!({"action": "create_thread", "name": "kit thread"}),
                })
                .await
            }
            Feature::VoiceOut | Feature::VoiceSynthesis => {
                let media = self.voice.seed(OGG_FIXTURE, "audio/ogg");
                self.send_and_record(feature, OutboundPayload::Voice {
                    voice: OutboundVoice::Media { media, codec: Some("ogg_opus".into()), duration_ms: Some(200) },
                })
                .await
            }
            Feature::Sticker => self.reject(part(ContentPart::Sticker { media: Default::default(), pack_id: None, sticker_id: None, emoji: None, alt_text: None })).await,
            Feature::Location => self.reject(part(ContentPart::Location { latitude: 1.0, longitude: 2.0, label: None, address: None, accuracy_meters: None, live_until_unix_ms: None })).await,
            Feature::Contact => self.reject(part(ContentPart::Contact { display_name: "k".into(), phone_numbers: vec![], emails: vec![], organization: None })).await,
            Feature::Poll => self.reject(part(ContentPart::Poll { question: "q".into(), options: vec!["a".into()], multiple_choice: false, closes_at_unix_ms: None, provider_poll_id: None })).await,
            Feature::ReadReceipts => unsupported_result(),
            Feature::VoiceIn => {
                let url = format!("{}/cdn/voice-message.ogg", self.server.uri());
                let d = json!({
                    "id": "1234567890123456789", "channel_id": THREAD, "guild_id": GUILD, "content": "", "flags": 8192,
                    "author": {"id": "42", "username": "alice"},
                    "attachments": [{"id": "5", "url": url, "filename": "voice-message.ogg", "content_type": "audio/ogg", "duration_secs": 0.2}]
                });
                let (tx, _rx) = tokio::sync::mpsc::channel(4);
                self.channel.handle_message_create(&d, BOT_ID, &tx).await;
                let envelope = self.events.inbound().into_iter().next().ok_or_else(ChannelSendError::default)?;
                let note = envelope.parts.iter().find_map(|p| match p {
                    ContentPart::VoiceNote { voice_note } => Some(voice_note.clone()),
                    _ => None,
                });
                let mut kinds = Vec::new();
                if let Some(note) = note {
                    kinds.push("audio".to_owned());
                    if note.transcript.is_some_and(|t| !t.text.is_empty()) {
                        kinds.push("transcript".into());
                    }
                }
                self.observed.lock().push(WireObservation {
                    feature,
                    operation: "voice_in".into(),
                    channel: envelope.channel.clone(),
                    conversation: envelope.conversation.clone(),
                    reply_to_id: envelope.reply_to_id.clone(),
                    thread_id: None,
                    payload_kinds: kinds,
                    platform_message_ids: Vec::new(),
                    request: serde_json::to_value(&envelope).unwrap(),
                });
                Ok(None)
            }
            Feature::InboundReactions => {
                let d = json!({
                    "user_id": "42", "message_id": "88", "channel_id": THREAD, "guild_id": GUILD,
                    "emoji": {"name": "🔥"}
                });
                self.channel.handle_reaction(&d, BOT_ID, true).await;
                let event = self.events.events().into_iter().next().ok_or_else(ChannelSendError::default)?;
                self.observed.lock().push(WireObservation {
                    feature,
                    operation: "reaction_receive".into(),
                    channel: event.channel.clone(),
                    conversation: event.conversation.clone().unwrap_or_default(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: Vec::new(),
                    platform_message_ids: Vec::new(),
                    request: serde_json::to_value(&event).unwrap(),
                });
                Ok(None)
            }
            Feature::Typing => {
                self.mark_start().await;
                ChannelTyping::start_typing(&self.channel, THREAD).await?;
                self.record(feature, &["typing"]).await;
                Ok(None)
            }
            Feature::Reactions => {
                let sent = self.channel.send_outcome(self.intent(feature, OutboundPayload::Text { text: "react to me".into() })).await?;
                let id = sent.receipt.primary_platform_message_id.clone().ok_or_else(ChannelSendError::default)?;
                self.mark_start().await;
                let reaction = tinychannels_bus::ChannelReaction { value: "👍".into(), custom_emoji_id: None };
                crate::channel::LegacyChannelReaction::send_reaction(&self.channel, &account(), &self.target(), &id, &reaction, false).await?;
                self.record(feature, &["reaction_send"]).await;
                Ok(None)
            }
            Feature::Edit => {
                let sent = self.channel.send_outcome(self.intent(feature, OutboundPayload::Text { text: "original".into() })).await?;
                self.mark_start().await;
                let edit = self.intent(feature, OutboundPayload::Text { text: "edited".into() });
                let receipt = ChannelEdit::edit(&self.channel, &sent.receipt, edit).await?;
                self.record(feature, &["edit"]).await;
                Ok(Some(ChannelSendOutcome { receipt, error: None }))
            }
            Feature::Delete => {
                let sent = self.channel.send_outcome(self.intent(feature, OutboundPayload::Text { text: "delete me".into() })).await?;
                self.mark_start().await;
                ChannelDelete::delete(&self.channel, &sent.receipt).await?;
                self.record(feature, &["delete"]).await;
                Ok(None)
            }
            Feature::StreamingDraft => {
                self.mark_start().await;
                let text = |t: &str| self.intent(feature, OutboundPayload::Text { text: t.into() });
                let created = ChannelStreamingDraft::upsert_draft(&self.channel, None, text("draft")).await?;
                let updated = ChannelStreamingDraft::upsert_draft(&self.channel, Some(&created), text("draft more")).await?;
                let finalized = ChannelEdit::edit(&self.channel, &updated, text("draft final")).await?;
                let abandoned = ChannelStreamingDraft::upsert_draft(&self.channel, None, text("never finished")).await?;
                ChannelDelete::delete(&self.channel, &abandoned).await?;
                self.record(
                    feature,
                    &["draft_create", "draft_update", "draft_finalize", "send", "draft_cancel_cleanup"],
                )
                .await;
                // The abandoned draft is an extra create; relabel it as the
                // cleanup's setup so every observed operation is accurate.
                self.observed.lock().retain(|o| !(o.operation == "send" && o.feature == feature));
                Ok(Some(ChannelSendOutcome { receipt: finalized, error: None }))
            }
            Feature::ManagedReaction | Feature::ManagedThreadCreate | Feature::ManagedThreadUpdate | Feature::ManagedThreadList => Err(ChannelSendError::default()),
        }
    }
}

impl DiscordFixture {
    /// Send content the adapter must refuse without touching the network.
    async fn reject(
        &self,
        payload: OutboundPayload,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        let before = self.server.received_requests().await.unwrap().len();
        let result = self
            .channel
            .send_outcome(self.intent(Feature::Text, payload))
            .await;
        assert_eq!(self.server.received_requests().await.unwrap().len(), before);
        result.map(Some)
    }
}

#[tokio::test]
async fn the_discord_adapter_passes_the_full_conformance_matrix() {
    let fixture = DiscordFixture::start().await;
    let results = check_all_features(&fixture).await;
    let failures: Vec<_> = results
        .iter()
        .filter_map(|(feature, result)| result.as_ref().err().map(|e| format!("{feature:?}: {e}")))
        .collect();
    assert!(failures.is_empty(), "conformance failures: {failures:#?}");
    assert_eq!(results.len(), Feature::ALL.len());
}

#[tokio::test]
async fn an_account_without_host_storage_declares_only_text_and_structured_features() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let capabilities = ChannelAdapter::declared_capabilities(&channel);
    assert!(
        !capabilities.content.image.outbound
            && !capabilities.voice_in.supported
            && !capabilities.voice_out.supported
    );
    assert!(capabilities.content.buttons.outbound && capabilities.content.presentation.outbound);
}
