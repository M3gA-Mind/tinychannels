//! The shared adapter conformance matrix against a mock signal-cli daemon.

use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelTyping,
    LegacyChannelReaction as ReactionPort,
};
use crate::providers::signal::fixtures::*;
use serde_json::{Value, json};
use std::sync::Mutex;
use tinychannels_bus::channel::{
    ChannelSendError, ChannelSendOutcome, ConversationRef, MessageReceipt, MessageReceiptPart,
    SendErrorKind,
};
use tinychannels_bus::{
    ChannelOutboundIntent, ChannelRef, ContactMethod, ContentPart, ConversationKind, MediaKind,
    MediaReference, OutboundContentPart, OutboundPayload, OutboundVoice,
};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature, ExpectedScope, WireObservation, check_all_features,
};
use tokio::sync::mpsc;
use wiremock::MockServer;

const REPLY_TS: &str = "1700000000002";

fn channel_ref() -> ChannelRef {
    ChannelRef {
        id: "signal".into(),
        account_id: None,
    }
}

fn conversation() -> ConversationRef {
    ConversationRef {
        kind: ConversationKind::Dm,
        id: PEER.into(),
        ..Default::default()
    }
}

fn intent(payload: OutboundPayload) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "kit-op".into(),
        channel_id: "signal".into(),
        conversation_id: PEER.into(),
        channel_ref: Some(channel_ref()),
        conversation_ref: Some(conversation()),
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
        conversation: Some(conversation()),
        primary_platform_message_id: Some(id.into()),
        platform_message_ids: vec![id.into()],
        parts: vec![MessageReceiptPart {
            platform_message_id: id.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

struct SignalWireFixture {
    rig: Rig,
    server: MockServer,
    observations: Mutex<Vec<WireObservation>>,
    cursor: Mutex<usize>,
}

impl SignalWireFixture {
    async fn start() -> Self {
        let server = mock_daemon(b"clip").await;
        let rig = rig(&server);
        Self {
            rig,
            server,
            observations: Mutex::default(),
            cursor: Mutex::default(),
        }
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        ChannelAdapter::send_outcome(&self.rig.channel, intent)
            .await
            .map(Some)
    }

    fn observe(
        &self,
        feature: ConformanceFeature,
        operation: &str,
        params: &Value,
        ids: Vec<String>,
        kinds: Vec<String>,
    ) {
        let reply = params["quoteTimestamp"].as_u64().map(|ts| ts.to_string());
        self.observations.lock().unwrap().push(WireObservation {
            feature,
            operation: operation.into(),
            channel: channel_ref(),
            conversation: conversation(),
            reply_to_id: reply,
            thread_id: None,
            payload_kinds: kinds,
            platform_message_ids: ids,
            request: params.clone(),
        });
    }

    /// Observations for the daemon calls made since the previous harvest.
    async fn harvest(&self, feature: ConformanceFeature) {
        let requests = self.server.received_requests().await.unwrap_or_default();
        let mut cursor = self.cursor.lock().unwrap();
        let fresh: Vec<_> = requests[*cursor..].to_vec();
        *cursor = requests.len();
        drop(cursor);
        for request in fresh {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let params = &body["params"];
            match body["method"].as_str().unwrap_or_default() {
                "send" if params.get("editTimestamp").is_some() => self.observe(
                    feature,
                    "edit",
                    params,
                    vec![params["editTimestamp"].to_string()],
                    vec!["text".into()],
                ),
                "send" => {
                    let kind = match (feature, params.get("attachments").is_some()) {
                        (ConformanceFeature::Image, _) => "image",
                        (ConformanceFeature::Video, _) => "video",
                        (ConformanceFeature::Audio, _) => "audio",
                        (ConformanceFeature::File, _) => "file",
                        (_, true) => "voice",
                        _ => "text",
                    };
                    self.observe(
                        feature,
                        "send",
                        params,
                        vec![SENT_TS.to_string()],
                        vec![kind.into()],
                    );
                }
                "sendTyping" => self.observe(feature, "typing", params, vec![], vec![]),
                "sendReaction" => self.observe(feature, "reaction_send", params, vec![], vec![]),
                "remoteDelete" => self.observe(
                    feature,
                    "delete",
                    params,
                    vec![params["targetTimestamp"].to_string()],
                    vec![],
                ),
                _ => {}
            }
        }
    }

    async fn feed(&self, data: Value) {
        let (tx, _rx) = mpsc::channel(4);
        let envelope = data_envelope(data);
        self.rig.channel.dispatch_envelope(&envelope, &tx).await;
    }
}

#[async_trait::async_trait]
impl AdapterFixture for SignalWireFixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        &self.rig.channel
    }

    fn expected_scope(&self, feature: ConformanceFeature) -> ExpectedScope {
        ExpectedScope {
            channel: channel_ref(),
            conversation: conversation(),
            reply_to_id: (feature == ConformanceFeature::ReplyAddressing)
                .then(|| REPLY_TS.to_owned()),
            thread_id: None,
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
        let adapter = &self.rig.channel;
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
                *self.rig.media.read.lock().unwrap() = Some(
                    crate::providers::native::fixtures::stored_media(kind, mime, b"bytes"),
                );
                self.send(intent(parts(vec![content(part)]))).await
            }
            F::Buttons => {
                let button = tinychannels_bus::channel::ButtonAction {
                    id: "go".into(),
                    label: "Go".into(),
                    style: None,
                    disabled: false,
                };
                self.send(intent(parts(vec![content(ContentPart::Buttons {
                    rows: vec![vec![button]],
                })])))
                .await
            }
            F::Presentation => {
                self.send(intent(OutboundPayload::PresentationBlocks {
                    blocks: json!({"kind": "approval", "id": "r1", "prompt": "Run?", "choices": ["Yes"]}),
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
                reply.reply_to_id = Some(REPLY_TS.into());
                self.send(reply).await
            }
            F::ThreadAddressing => {
                let mut threaded = text_intent("threaded");
                threaded.thread_id = Some("1.0".into());
                self.send(threaded).await
            }
            F::Typing => ChannelTyping::start_typing(adapter, PEER)
                .await
                .map(|()| None),
            F::Reactions => {
                let reaction = tinychannels_bus::channel::ChannelReaction {
                    value: "👍".into(),
                    custom_emoji_id: None,
                };
                ReactionPort::send_reaction(
                    adapter,
                    &channel_ref(),
                    &conversation(),
                    "1700000000001",
                    &reaction,
                    false,
                )
                .await
                .map(|()| None)
            }
            F::Edit => ChannelEdit::edit(adapter, &receipt("1700000000123"), text_intent("edited"))
                .await
                .map(|receipt| {
                    Some(ChannelSendOutcome {
                        receipt,
                        error: None,
                    })
                }),
            F::Delete => ChannelDelete::delete(adapter, &receipt("1700000000123"))
                .await
                .map(|()| None),
            F::StreamingDraft => {
                crate::channel::ChannelStreamingDraft::upsert_draft(
                    adapter,
                    None,
                    text_intent("draft"),
                )
                .await
                .map(|_| None)
            }
            F::InboundReactions => {
                let data = json!({
                    "timestamp": 1_700_000_000_900u64,
                    "reaction": {"emoji": "🎉", "targetSentTimestamp": 1_700_000_000_123u64},
                });
                self.feed(data.clone()).await;
                let recorded = self.rig.events.events().pop().expect("a reaction event");
                self.observations.lock().unwrap().push(WireObservation {
                    feature,
                    operation: "reaction_receive".into(),
                    channel: recorded.channel.clone(),
                    conversation: recorded.conversation.clone().unwrap(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: vec![],
                    platform_message_ids: vec![],
                    request: data,
                });
                Ok(None)
            }
            F::VoiceIn => {
                let data = json!({
                    "timestamp": 1_700_000_001_000u64,
                    "attachments": [{"id": "clip", "contentType": "audio/mp4", "size": 4, "voiceNote": true}],
                });
                self.feed(data.clone()).await;
                let envelope = self.rig.events.inbound().pop().expect("a voice envelope");
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
                    request: data,
                });
                Ok(None)
            }
            F::ReadReceipts => {
                let (tx, _rx) = mpsc::channel(4);
                let envelope: Envelope = serde_json::from_value(json!({
                    "sourceNumber": PEER,
                    "receiptMessage": {"isRead": true, "when": 1u64, "timestamps": [1_700_000_000_123u64]},
                }))
                .unwrap();
                self.rig.channel.dispatch_envelope(&envelope, &tx).await;
                let recorded = self.rig.events.events().pop().expect("a receipt event");
                self.observations.lock().unwrap().push(WireObservation {
                    feature,
                    operation: "read_receipt".into(),
                    channel: recorded.channel.clone(),
                    conversation: recorded.conversation.clone().unwrap(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: vec![],
                    platform_message_ids: vec![],
                    request: json!({}),
                });
                Ok(None)
            }
            F::ManagedReaction | F::ManagedThreadCreate | F::ManagedThreadUpdate | F::ManagedThreadList => Err(ChannelSendError::default()),
        };
        self.harvest(feature).await;
        result
    }
}

#[tokio::test]
async fn signal_adapter_passes_the_full_conformance_matrix_against_the_mock_daemon() {
    let fixture = SignalWireFixture::start().await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), ConformanceFeature::ALL.len());
    let failures: Vec<_> = results
        .iter()
        .filter_map(|(feature, result)| result.as_ref().err().map(|e| format!("{feature:?}: {e}")))
        .collect();
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test]
async fn undeclared_features_never_reach_the_daemon() {
    let fixture = SignalWireFixture::start().await;
    tinychannels_conformance::check_feature(&fixture, ConformanceFeature::Sticker)
        .await
        .unwrap();
    tinychannels_conformance::check_feature(&fixture, ConformanceFeature::ThreadAddressing)
        .await
        .unwrap();
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
    let error = fixture
        .send(intent(OutboundPayload::Poll {
            question: "q".into(),
            options: vec![],
        }))
        .await
        .unwrap_err();
    assert!(error.unsupported && error.kind == SendErrorKind::BadFormat);
}
