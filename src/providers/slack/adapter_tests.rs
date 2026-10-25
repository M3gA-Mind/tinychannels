use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelStreamingDraft, ChannelTypedSend,
    ChannelTyping,
};
use crate::providers::slack::fixtures::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tinychannels_bus::channel::{MessageReceiptPart, SendErrorKind};
use tinychannels_bus::{
    ContactMethod, ContentPart, ConversationKind, MediaKind, MediaReference, OutboundContentPart,
    OutboundPayload, OutboundVoice,
};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature, ExpectedScope, WireObservation, check_all_features,
};
use wiremock::MockServer;

const MOCK_TS: &str = "5.5";
const SHARE_TS: &str = "7.7";
const CLIP_TS: &str = "1700000000.000100";

fn channel_ref() -> tinychannels_bus::ChannelRef {
    tinychannels_bus::ChannelRef {
        id: "slack".into(),
        account_id: None,
    }
}

fn conversation(thread: Option<&str>) -> ConversationRef {
    ConversationRef {
        kind: ConversationKind::Channel,
        id: "C1".into(),
        thread_id: thread.map(str::to_owned),
        ..Default::default()
    }
}

fn intent(payload: OutboundPayload) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "kit-op".into(),
        channel_id: "slack".into(),
        conversation_id: "C1".into(),
        channel_ref: Some(channel_ref()),
        conversation_ref: Some(conversation(None)),
        turn_id: Some("turn-1".into()),
        payload,
        ..Default::default()
    }
}

fn text_intent(text: &str) -> ChannelOutboundIntent {
    intent(OutboundPayload::Text { text: text.into() })
}

fn content(part: ContentPart) -> OutboundContentPart {
    OutboundContentPart::Content { part }
}

fn parts(parts: Vec<OutboundContentPart>) -> OutboundPayload {
    OutboundPayload::Parts { parts }
}

fn media_ref(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some("media-1".into()),
        kind,
        ..Default::default()
    }
}

fn receipt(id: &str) -> MessageReceipt {
    MessageReceipt {
        channel: Some(channel_ref()),
        conversation: Some(conversation(None)),
        primary_platform_message_id: Some(id.into()),
        platform_message_ids: vec![id.into()],
        parts: vec![MessageReceiptPart {
            platform_message_id: id.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The real adapter against a mock Slack platform, observed through wire traffic.
struct SlackWireFixture {
    slack: Arc<SlackChannel>,
    server: MockServer,
    events: Arc<RecordingEvents>,
    media: Arc<StubMedia>,
    clip_url: String,
    observations: Mutex<Vec<WireObservation>>,
    cursor: Mutex<usize>,
}

impl SlackWireFixture {
    async fn start() -> Self {
        use base64::Engine as _;
        let server = mock_api().await;
        mount_auth(&server, "UBOT").await;
        mount(&server, "chat.postMessage", ok(json!({"ts": MOCK_TS}))).await;
        for method in [
            "chat.update",
            "chat.delete",
            "reactions.add",
            "assistant.threads.setStatus",
        ] {
            mount(&server, method, ok(json!({}))).await;
        }
        mount(
            &server,
            "files.getUploadURLExternal",
            ok(json!({"upload_url": format!("{}/upload/F1", server.uri()), "file_id": "F1"})),
        )
        .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/upload/F1"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;
        mount(
            &server,
            "files.completeUploadExternal",
            ok(json!({"files": [{"id": "F1", "shares": {"public": {"C1": [{"ts": SHARE_TS}]}}}]})),
        )
        .await;
        let clip_url = mount_file(&server, "/f/clip.m4a", "audio/mp4", b"audio").await;
        let media = Arc::new(StubMedia::default());
        let voice = Arc::new(StubVoice::default());
        *voice.audio_base64.lock().unwrap() =
            Some(base64::engine::general_purpose::STANDARD.encode(b"voice"));
        let events = Arc::new(RecordingEvents {
            deliver: true,
            ..Default::default()
        });
        let slack = Arc::new(
            channel_for(&server, &["*"])
                .with_media_repository(media.clone())
                .with_voice_media_store(voice)
                .with_events(events.clone()),
        );
        Self {
            slack,
            server,
            events,
            media,
            clip_url,
            observations: Mutex::default(),
            cursor: Mutex::default(),
        }
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        ChannelAdapter::send_outcome(self.slack.as_ref(), intent)
            .await
            .map(Some)
    }

    /// Observations for the requests sent since the previous harvest.
    async fn harvest(&self, feature: ConformanceFeature) {
        let requests = self.server.received_requests().await.unwrap_or_default();
        let mut cursor = self.cursor.lock().unwrap();
        let fresh: Vec<_> = requests[*cursor..].to_vec();
        *cursor = requests.len();
        let mut updates = 0;
        let mut observed = self.observations.lock().unwrap();
        for request in fresh {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let draft = feature == ConformanceFeature::StreamingDraft;
            let (operation, ids, kinds): (&str, Vec<String>, Vec<String>) = match request.url.path()
            {
                "/api/chat.postMessage" => {
                    let has_actions = body["blocks"]
                        .as_array()
                        .is_some_and(|b| b.iter().any(|b| b["type"] == "actions"));
                    let kind = match (has_actions, feature) {
                        (true, ConformanceFeature::Presentation) => "presentation",
                        (true, _) => "buttons",
                        (false, _) => "text",
                    };
                    (
                        if draft { "draft_create" } else { "send" },
                        vec![MOCK_TS.into()],
                        vec![kind.into()],
                    )
                }
                "/api/files.completeUploadExternal" => {
                    let kind = match feature {
                        ConformanceFeature::Image => "image",
                        ConformanceFeature::Video => "video",
                        ConformanceFeature::Audio => "audio",
                        ConformanceFeature::File => "file",
                        _ => "voice",
                    };
                    ("send", vec![SHARE_TS.into()], vec![kind.into()])
                }
                "/api/chat.update" => {
                    updates += 1;
                    let operation = match (draft, updates) {
                        (false, _) => "edit",
                        (true, 1) => "draft_update",
                        (true, _) => "draft_finalize",
                    };
                    (
                        operation,
                        vec![body["ts"].as_str().unwrap().to_owned()],
                        vec!["text".into()],
                    )
                }
                "/api/chat.delete" => (
                    if draft {
                        "draft_cancel_cleanup"
                    } else {
                        "delete"
                    },
                    vec![body["ts"].as_str().unwrap().to_owned()],
                    vec![],
                ),
                "/api/reactions.add" => ("reaction_send", vec![], vec![]),
                "/api/assistant.threads.setStatus" => ("typing", vec![], vec![]),
                _ => continue,
            };
            let channel_id = body["channel"]
                .as_str()
                .or(body["channel_id"].as_str())
                .unwrap_or_default();
            let thread = body["thread_ts"].as_str().map(str::to_owned);
            let thread_in_scope = feature == ConformanceFeature::ThreadAddressing;
            observed.push(WireObservation {
                feature,
                operation: operation.into(),
                channel: channel_ref(),
                conversation: conversation(thread.clone().filter(|_| thread_in_scope).as_deref()),
                reply_to_id: thread.clone(),
                thread_id: thread,
                payload_kinds: kinds,
                platform_message_ids: ids,
                request: json!({"channel": channel_id, "body": body}),
            });
        }
    }
}

#[async_trait::async_trait]
impl AdapterFixture for SlackWireFixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        self.slack.as_ref()
    }

    fn expected_scope(&self, feature: ConformanceFeature) -> ExpectedScope {
        ExpectedScope {
            channel: channel_ref(),
            conversation: match feature {
                ConformanceFeature::ThreadAddressing => conversation(Some("1.0")),
                ConformanceFeature::VoiceIn => conversation(Some(CLIP_TS)),
                _ => conversation(None),
            },
            reply_to_id: (feature == ConformanceFeature::ReplyAddressing).then(|| "2.0".into()),
            thread_id: (feature == ConformanceFeature::ThreadAddressing).then(|| "1.0".into()),
        }
    }

    fn clear_observations(&self) {
        self.observations.lock().unwrap().clear();
    }

    fn observations(&self) -> Vec<WireObservation> {
        self.observations.lock().unwrap().clone()
    }

    async fn exercise(
        &self,
        feature: ConformanceFeature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        use ConformanceFeature as F;
        let adapter = self.slack.as_ref();
        let stored = |kind, mime: &str| Some(stored_media(kind, mime, b"bytes"));
        let result = match feature {
            F::Text => self.send(text_intent("hello-kit")).await,
            F::Image | F::Video | F::Audio | F::File => {
                let (kind, mime, part) = match feature {
                    F::Image => (
                        MediaKind::Image,
                        "image/png",
                        ContentPart::Image {
                            media: media_ref(MediaKind::Image),
                            caption: None,
                            alt_text: None,
                        },
                    ),
                    F::Video => (
                        MediaKind::Video,
                        "video/mp4",
                        ContentPart::Video {
                            media: media_ref(MediaKind::Video),
                            caption: None,
                            duration_ms: None,
                            thumbnail: None,
                        },
                    ),
                    F::Audio => (
                        MediaKind::Audio,
                        "audio/mpeg",
                        ContentPart::Audio {
                            media: media_ref(MediaKind::Audio),
                            codec: None,
                            duration_ms: None,
                            title: None,
                            transcript: None,
                        },
                    ),
                    _ => (
                        MediaKind::Document,
                        "application/pdf",
                        ContentPart::File {
                            media: media_ref(MediaKind::Document),
                            file_name: None,
                            caption: None,
                        },
                    ),
                };
                *self.media.read.lock().unwrap() = stored(kind, mime);
                self.send(intent(parts(vec![content(part)]))).await
            }
            F::Buttons => {
                let button = tinychannels_bus::channel::ButtonAction {
                    id: "go".into(),
                    label: "Go".into(),
                    style: None,
                    disabled: false,
                };
                self.send(intent(parts(vec![
                    content(ContentPart::Text {
                        text: "Pick".into(),
                        markdown_dialect: None,
                    }),
                    content(ContentPart::Buttons {
                        rows: vec![vec![button]],
                    }),
                ])))
                .await
            }
            F::Presentation => {
                let approval = json!({"kind": "approval", "id": "r1", "prompt": "Run it?", "choices": ["Yes", "No"]});
                self.send(intent(OutboundPayload::PresentationBlocks {
                    blocks: approval,
                }))
                .await
            }
            F::VoiceOut => {
                self.send(intent(OutboundPayload::Voice {
                    voice: OutboundVoice::Media {
                        media: media_ref(MediaKind::Audio),
                        codec: Some("m4a_aac".into()),
                        duration_ms: None,
                    },
                }))
                .await
            }
            F::VoiceSynthesis => {
                self.send(intent(OutboundPayload::Voice {
                    voice: OutboundVoice::Synthesize {
                        text: "speak".into(),
                        voice: None,
                    },
                }))
                .await
            }
            F::Sticker => {
                self.send(intent(parts(vec![content(ContentPart::Sticker {
                    media: media_ref(MediaKind::Image),
                    pack_id: None,
                    sticker_id: None,
                    emoji: None,
                    alt_text: None,
                })])))
                .await
            }
            F::Location => {
                self.send(intent(parts(vec![content(ContentPart::Location {
                    latitude: 1.0,
                    longitude: 2.0,
                    label: None,
                    address: None,
                    accuracy_meters: None,
                    live_until_unix_ms: None,
                })])))
                .await
            }
            F::Contact => {
                self.send(intent(parts(vec![content(ContentPart::Contact {
                    display_name: "Ada".into(),
                    phone_numbers: vec![ContactMethod {
                        value: "1".into(),
                        label: None,
                    }],
                    emails: vec![],
                    organization: None,
                })])))
                .await
            }
            F::Poll => {
                self.send(intent(OutboundPayload::Poll {
                    question: "q".into(),
                    options: vec!["a".into(), "b".into()],
                }))
                .await
            }
            F::NativeData => {
                self.send(intent(OutboundPayload::NativeChannelData {
                    data: json!({"k": 1}),
                }))
                .await
            }
            F::ReplyAddressing => {
                let mut reply = text_intent("reply");
                reply.reply_to_id = Some("2.0".into());
                self.send(reply).await
            }
            F::ThreadAddressing => {
                let mut threaded = text_intent("threaded");
                threaded.conversation_ref = Some(conversation(Some("1.0")));
                threaded.thread_id = Some("1.0".into());
                self.send(threaded).await
            }
            F::Typing => ChannelTyping::start_typing(adapter, "C1:1.0")
                .await
                .map(|()| None),
            F::Reactions => {
                let reaction = tinychannels_bus::channel::ChannelReaction {
                    value: "👍".into(),
                    custom_emoji_id: None,
                };
                crate::channel::LegacyChannelReaction::send_reaction(
                    adapter,
                    &channel_ref(),
                    &conversation(None),
                    "1.1",
                    &reaction,
                    false,
                )
                .await
                .map(|()| None)
            }
            F::Edit => ChannelEdit::edit(adapter, &receipt("1.1"), text_intent("edited"))
                .await
                .map(|receipt| {
                    Some(ChannelSendOutcome {
                        receipt,
                        error: None,
                    })
                }),
            F::Delete => ChannelDelete::delete(adapter, &receipt("1.1"))
                .await
                .map(|()| None),
            F::StreamingDraft => {
                let created =
                    ChannelStreamingDraft::upsert_draft(adapter, None, text_intent("d")).await?;
                ChannelStreamingDraft::upsert_draft(adapter, Some(&created), text_intent("draft"))
                    .await?;
                ChannelEdit::edit(adapter, &created, text_intent("final")).await?;
                ChannelDelete::delete(adapter, &created).await?;
                Ok(Some(ChannelSendOutcome {
                    receipt: created,
                    error: None,
                }))
            }
            F::InboundReactions => {
                let event = json!({"type": "reaction_added", "user": "U1", "reaction": "tada", "event_ts": "9.9", "item": {"type": "message", "channel": "C1", "ts": "1.1"}});
                self.slack.handle_event(&event, None).await;
                let recorded = self.events.events().pop().expect("a reaction event");
                self.observations.lock().unwrap().push(WireObservation {
                    feature,
                    operation: "reaction_receive".into(),
                    channel: recorded.channel.clone(),
                    conversation: recorded.conversation.clone().unwrap(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: vec![],
                    platform_message_ids: vec![],
                    request: event,
                });
                Ok(None)
            }
            F::VoiceIn => {
                let event = json!({"type": "message", "channel": "C1", "ts": CLIP_TS, "user": "U1", "channel_type": "channel", "files": [{
                    "id": "F9", "mimetype": "audio/mp4", "subtype": "slack_audio", "size": 5,
                    "url_private_download": self.clip_url,
                    "transcription": {"status": "complete", "preview": {"content": "hello"}},
                }]});
                self.slack.handle_event(&event, None).await;
                let envelope = self.events.inbound().pop().expect("a voice envelope");
                let mut kinds = vec![];
                if let Some(ContentPart::VoiceNote { voice_note }) = envelope.parts.first() {
                    kinds.push("audio".to_owned());
                    if voice_note.transcript.is_some() {
                        kinds.push("transcript".into());
                    }
                }
                self.observations.lock().unwrap().push(WireObservation {
                    feature,
                    operation: "voice_in".into(),
                    channel: envelope.channel.clone(),
                    conversation: envelope.conversation.clone(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: kinds,
                    platform_message_ids: vec![],
                    request: event,
                });
                Ok(None)
            }
            F::ReadReceipts => Err(crate::providers::slack::api::unsupported(
                "Slack exposes no read receipts to bots",
            )),
            F::ManagedReaction
            | F::ManagedThreadCreate
            | F::ManagedThreadUpdate
            | F::ManagedThreadList => Err(ChannelSendError::default()),
        };
        self.harvest(feature).await;
        result
    }
}

#[tokio::test]
async fn slack_adapter_passes_the_full_conformance_matrix_against_the_mock_platform() {
    let fixture = SlackWireFixture::start().await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), ConformanceFeature::ALL.len());
    let failures: Vec<_> = results
        .iter()
        .filter_map(|(feature, result)| result.as_ref().err().map(|e| format!("{feature:?}: {e}")))
        .collect();
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test]
async fn the_conformance_run_observes_real_wire_requests() {
    let fixture = SlackWireFixture::start().await;
    tinychannels_conformance::check_feature(&fixture, ConformanceFeature::Text)
        .await
        .unwrap();
    let observation = fixture.observations().pop().unwrap();
    assert_eq!(observation.request["channel"], "C1");
    assert_eq!(observation.request["body"]["text"], "hello-kit");
    assert_eq!(observation.platform_message_ids, [MOCK_TS]);
    tinychannels_conformance::check_feature(&fixture, ConformanceFeature::Sticker)
        .await
        .unwrap();
    assert!(
        fixture.observations().is_empty(),
        "undeclared stickers must not reach Slack"
    );
}

#[test]
fn the_descriptor_status_and_capabilities_identify_the_native_adapter() {
    let channel = SlackChannel::new("t".into(), Some("C1".into()), vec![]);
    let descriptor = ChannelAdapter::descriptor(&channel);
    assert_eq!(
        (descriptor.id.as_str(), descriptor.display_name.as_str()),
        ("slack", "Slack")
    );
    assert_eq!(descriptor.metadata["adapterMode"], "native-v2");
    assert_eq!(descriptor.metadata["voiceOut"], "file");
    assert_eq!(
        ChannelAdapter::declared_capabilities(&channel),
        channel.declared()
    );
    assert_eq!(
        ChannelAdapter::proactive_target(&channel).as_deref(),
        Some("C1")
    );
    assert_eq!(descriptor.id, "slack");
    let capabilities = ChannelAdapter::declared_capabilities(&channel);
    assert!(capabilities.remote_control && capabilities.chat_approvals);
    assert!(
        capabilities.reactions
            && capabilities
                .actions
                .supported
                .iter()
                .any(|action| action == "edit")
            && capabilities
                .actions
                .supported
                .iter()
                .any(|action| action == "draft-update")
    );
    assert!(capabilities.addressing.reply_to_id && capabilities.addressing.thread_id);
}

#[tokio::test]
async fn lifecycle_hooks_are_owned_by_the_module_runtime() {
    struct NoSink;
    #[async_trait::async_trait]
    impl crate::channel::ChannelInboundSink for NoSink {
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
    let channel = Arc::new(SlackChannel::new("t".into(), Some("C1".into()), vec![]));
    let listener_channel = channel.clone();
    let listener =
        tokio::spawn(
            async move { ChannelAdapter::start(listener_channel.as_ref(), &NoSink).await },
        );
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !channel
            .listener_running
            .load(std::sync::atomic::Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener start registers before it polls");
    ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), listener)
        .await
        .expect("listener stop cancels polling")
        .expect("listener task joins")
        .unwrap();
    let status = ChannelAdapter::status(channel.as_ref()).await.unwrap();
    assert_eq!(status.channel_id, "slack");
    assert_eq!(status.states.len(), 2);
}

#[tokio::test]
async fn typed_sends_post_text_and_keep_provider_errors_recoverable() {
    let server = mock_api().await;
    mount(&server, "chat.postMessage", ok(json!({"ts": "1.1"}))).await;
    let channel = channel_for(&server, &[]);
    ChannelAdapter::send_outcome(&channel, text_intent("hi"))
        .await
        .unwrap();
    let mut threaded = text_intent("again");
    threaded.thread_id = Some("1.0".into());
    threaded.conversation_ref = Some(conversation(Some("1.0")));
    ChannelAdapter::send_outcome(&channel, threaded)
        .await
        .unwrap();
    let posts = bodies(&server, "chat.postMessage").await;
    assert_eq!(
        (posts[0]["channel"].as_str(), posts[0]["text"].as_str()),
        (Some("C1"), Some("hi"))
    );
    assert_eq!(posts[1]["thread_ts"], "1.0");

    let denied = mock_api().await;
    mount(
        &denied,
        "chat.postMessage",
        json!({"ok": false, "error": "channel_not_found"}),
    )
    .await;
    let error = ChannelAdapter::send_outcome(&channel_for(&denied, &[]), text_intent("x"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::NotFound);

    let outcome = crate::channel::ChannelTypedSend::send_typed(
        &channel,
        &ChannelOutboundMessage::from_intent(&text_intent("typed")),
    )
    .await
    .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["1.1"]);
    let outcome = ChannelTypedSend::send_typed(
        &channel,
        &ChannelOutboundMessage::from_intent(&text_intent("typed")),
    )
    .await
    .unwrap();
    assert!(outcome.error.is_none());
    let bad = ChannelOutboundMessage::from_intent(&intent(OutboundPayload::Poll {
        question: "q".into(),
        options: vec![],
    }));
    assert!(
        crate::channel::ChannelTypedSend::send_typed(&channel, &bad)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typed_drafts_post_then_edit_in_place() {
    let server = mock_api().await;
    mount(&server, "chat.postMessage", ok(json!({"ts": "3.3"}))).await;
    mount(&server, "chat.update", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    let id = ChannelStreamingDraft::start_draft(&channel, text_intent("draft"))
        .await
        .unwrap();
    assert_eq!(id.as_deref(), Some("3.3"));
    ChannelStreamingDraft::update_draft(&channel, "3.3", text_intent("draft more"))
        .await
        .unwrap();
    ChannelStreamingDraft::finalize_draft(&channel, "3.3", text_intent("final"))
        .await
        .unwrap();
    let updates = bodies(&server, "chat.update").await;
    assert_eq!(
        (updates[0]["text"].as_str(), updates[1]["text"].as_str()),
        (Some("draft more"), Some("final"))
    );
    assert!(
        ChannelStreamingDraft::start_draft(&channel, text_intent(" "))
            .await
            .is_err()
    );
    assert!(
        ChannelStreamingDraft::update_draft(&channel, "3.3", text_intent(" "))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typed_draft_lifecycle_preserves_slack_create_update_and_finalize() {
    let server = mock_api().await;
    mount(&server, "chat.postMessage", ok(json!({"ts": "4.4"}))).await;
    mount(&server, "chat.update", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);

    let id = ChannelStreamingDraft::start_draft(&channel, text_intent("draft"))
        .await
        .unwrap();
    assert_eq!(id.as_deref(), Some("4.4"));
    ChannelStreamingDraft::update_draft(&channel, "4.4", text_intent("draft more"))
        .await
        .unwrap();
    ChannelStreamingDraft::finalize_draft(&channel, "4.4", text_intent("final"))
        .await
        .unwrap();

    let posted = bodies(&server, "chat.postMessage").await;
    let updates = bodies(&server, "chat.update").await;
    assert_eq!(posted.len(), 1);
    assert_eq!(posted[0]["text"], "draft");
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0]["text"], "draft more");
    assert_eq!(updates[1]["text"], "final");
}

#[tokio::test]
async fn typed_interaction_operations_preserve_provider_validation() {
    let server = mock_api().await;
    for method in [
        "reactions.add",
        "chat.update",
        "chat.delete",
        "assistant.threads.setStatus",
    ] {
        mount(&server, method, ok(json!({}))).await;
    }
    let channel = channel_for(&server, &[]);
    let reaction = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &channel_ref(),
        &conversation(None),
        "1.1",
        &reaction,
        false,
    )
    .await
    .unwrap();
    ChannelEdit::edit(&channel, &receipt("1.1"), text_intent("new"))
        .await
        .unwrap();
    ChannelDelete::delete(&channel, &receipt("1.1"))
        .await
        .unwrap();
    ChannelTyping::start_typing(&channel, "C1:1.0")
        .await
        .unwrap();
    ChannelTyping::stop_typing(&channel, "C1:1.0")
        .await
        .unwrap();
    assert_eq!(bodies(&server, "reactions.add").await.len(), 1);
    assert_eq!(bodies(&server, "chat.update").await.len(), 1);
    assert_eq!(bodies(&server, "chat.delete").await.len(), 1);
    let statuses = bodies(&server, "assistant.threads.setStatus").await;
    assert_eq!(statuses.len(), 2);
    assert_eq!(statuses[1]["status"], "");
    assert!(ChannelTyping::start_typing(&channel, "C1").await.is_err());
    assert!(ChannelTyping::stop_typing(&channel, "C1").await.is_err());
    assert!(
        ChannelEdit::edit(&channel, &receipt("1.1"), text_intent(" "))
            .await
            .is_err()
    );
    assert!(
        ChannelDelete::delete(&channel, &MessageReceipt::default())
            .await
            .is_err()
    );
    let unknown = tinychannels_bus::channel::ChannelReaction {
        value: "??".into(),
        custom_emoji_id: None,
    };
    assert!(
        crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &channel_ref(),
            &conversation(None),
            "1.1",
            &unknown,
            false,
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn reactions_for_another_channel_are_forbidden() {
    let server = mock_api().await;
    mount(&server, "reactions.add", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    let reaction = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    let other = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: None,
    };
    let error = crate::channel::LegacyChannelReaction::send_reaction(
        &channel,
        &other,
        &conversation(None),
        "1.1",
        &reaction,
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Forbidden);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn health_checks_follow_the_auth_test_status() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let channel = channel_for(&server, &[]);
    assert_eq!(
        ChannelAdapter::status(&channel)
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("healthy"),
        "adapter status must report the actual auth.test result"
    );
    let dead = SlackChannel::new("t".into(), None, vec![]).with_api_base("http://127.0.0.1:1/api");
    assert_eq!(
        ChannelAdapter::status(&dead)
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("unhealthy")
    );
    let empty = mock_api().await;
    assert_eq!(
        ChannelAdapter::status(&channel_for(&empty, &[]))
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("unhealthy")
    );
}

#[tokio::test]
async fn listening_needs_a_channel_or_an_app_token_and_picks_the_transport() {
    struct NoSink;
    #[async_trait::async_trait]
    impl crate::channel::ChannelInboundSink for NoSink {
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
    let bare = SlackChannel::new("t".into(), None, vec![]);
    let error = ChannelAdapter::start(&bare, &NoSink).await.unwrap_err();
    assert!(
        error
            .message
            .contains("needs a channel_id or Socket Mode app token")
    );
    let dead = |channel: Option<&str>, app: Option<&str>| {
        SlackChannel::new("t".into(), channel.map(str::to_owned), vec!["*".into()])
            .with_app_token(app.map(str::to_owned))
            .with_api_base("http://127.0.0.1:1/api")
            .with_timing(
                std::time::Duration::from_millis(5),
                std::time::Duration::from_millis(5),
            )
    };
    for channel in [
        dead(Some("C1"), None),
        dead(None, Some("app-token")),
        dead(Some("C1"), Some("  ")),
    ] {
        let waited = tokio::time::timeout(
            std::time::Duration::from_millis(80),
            ChannelAdapter::start(&channel, &NoSink),
        )
        .await;
        assert!(waited.is_err(), "the transport loops until cancelled");
    }
}
