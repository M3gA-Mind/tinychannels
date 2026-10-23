//! The shared conformance kit run against the real Cloud adapter, with every
//! observation read from the requests a mock Graph server actually received.

use super::*;
use crate::channel::{ChannelAdapter, ChannelEdit as _, ChannelStreamingDraft as _};
use crate::providers::whatsapp::api::GRAPH_VERSION;
use crate::providers::whatsapp_shared::fakes::{
    CollectingSink, MemoryMedia, MemoryVoices, ogg_bytes, png_bytes,
};
use hmac::{Hmac, KeyInit, Mac};
use parking_lot::Mutex;
use serde_json::{Value, json};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_bus::channel::{ChannelSendError, ChannelSendOutcome};
use tinychannels_bus::{
    ChannelIngress, ChannelOutboundIntent, ChannelRef, ContentPart, ConversationKind,
    ConversationRef, MediaKind, MediaReference, OutboundContentPart, OutboundPayload,
    OutboundVoice,
};
use tinychannels_conformance::{
    AdapterFixture, ConformanceFeature as Feature, ExpectedScope, WireObservation,
    check_all_features,
};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const SECRET: &str = "kit-secret";

struct Logged {
    path: String,
    body: Vec<u8>,
    id: Option<String>,
}

struct Recording {
    log: Arc<Mutex<Vec<Logged>>>,
    counter: AtomicUsize,
}

impl Respond for Recording {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let path = request.url.path().to_owned();
        if path.ends_with("/media") {
            self.log.lock().push(Logged {
                path,
                body: request.body.clone(),
                id: None,
            });
            return ResponseTemplate::new(200).set_body_json(json!({"id": format!("media-{n}")}));
        }
        let id = format!("wamid.OUT{n}");
        self.log.lock().push(Logged {
            path,
            body: request.body.clone(),
            id: Some(id.clone()),
        });
        ResponseTemplate::new(200).set_body_json(json!({"messages": [{"id": id}]}))
    }
}

struct Transcribe;

#[async_trait::async_trait]
impl crate::host::Transcriber for Transcribe {
    fn name(&self) -> &str {
        "kit"
    }

    async fn transcribe(
        &self,
        _request: crate::host::TranscriptionRequest,
    ) -> anyhow::Result<crate::host::TranscriptionResult> {
        Ok(crate::host::TranscriptionResult {
            text: "spoken words".into(),
            language: Some("en".into()),
            duration_secs: Some(1.0),
        })
    }
}

struct CloudKit {
    channel: Arc<WhatsAppChannel>,
    server: MockServer,
    sink: Arc<CollectingSink>,
    log: Arc<Mutex<Vec<Logged>>>,
    cursor: Mutex<usize>,
    observed: Mutex<Vec<WireObservation>>,
    media: HashMap<Feature, String>,
    scope: ExpectedScope,
}

fn scope() -> ExpectedScope {
    ExpectedScope {
        channel: ChannelRef {
            id: "whatsapp".into(),
            account_id: Some("whatsapp".into()),
        },
        conversation: ConversationRef {
            kind: ConversationKind::Dm,
            id: "+15550001111".into(),
            ..Default::default()
        },
        reply_to_id: None,
        thread_id: None,
    }
}

async fn kit() -> CloudKit {
    let server = MockServer::start().await;
    let log = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("POST"))
        .respond_with(Recording {
            log: log.clone(),
            counter: AtomicUsize::new(0),
        })
        .mount(&server)
        .await;
    let sink = Arc::new(CollectingSink::new());
    let repository = Arc::new(MemoryMedia::default());
    let voices = Arc::new(MemoryVoices::default());
    voices.seed("voice-kit", "audio/ogg", &ogg_bytes());
    let mut media = HashMap::new();
    media.insert(
        Feature::Image,
        repository.seed(MediaKind::Image, "image/png", Some("kit.png"), &png_bytes()),
    );
    media.insert(
        Feature::Video,
        repository.seed(
            MediaKind::Video,
            "video/mp4",
            Some("kit.mp4"),
            b"0000ftypmp42",
        ),
    );
    media.insert(
        Feature::Audio,
        repository.seed(MediaKind::Audio, "audio/mpeg", Some("kit.mp3"), b"ID3-----"),
    );
    media.insert(
        Feature::File,
        repository.seed(
            MediaKind::Document,
            "application/pdf",
            Some("kit.pdf"),
            b"%PDF-1.4 x",
        ),
    );
    media.insert(
        Feature::Sticker,
        repository.seed(MediaKind::Image, "image/webp", None, b"RIFF----WEBPxxxx"),
    );
    let channel = Arc::new(
        WhatsAppChannel::new(
            "tok".into(),
            "123".into(),
            "verify".into(),
            vec!["+15550001111".into()],
        )
        .with_api_base_for_test(&server.uri())
        .with_app_secret(Some(SECRET.into()))
        .with_events(sink.clone())
        .with_media_repository(repository)
        .with_voice_media_store(voices),
    );
    CloudKit {
        channel,
        server,
        sink,
        log,
        cursor: Mutex::new(0),
        observed: Mutex::new(Vec::new()),
        media,
        scope: scope(),
    }
}

impl CloudKit {
    async fn post_webhook(&self, value: Value) {
        let payload = json!({
            "object": "whatsapp_business_account",
            "entry": [{"changes": [{"field": "messages", "value": value}]}]
        });
        let body = serde_json::to_vec(&payload).unwrap();
        let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
        mac.update(&body);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        ChannelAdapter::deliver_webhook(self.channel.as_ref(), Some(&signature), &body)
            .await
            .expect("authenticated delivery is ingested");
    }

    fn base_value(&self, extra: Value) -> Value {
        let mut value = json!({"metadata": {"phone_number_id": "123"}});
        for (key, field) in extra.as_object().cloned().unwrap_or_default() {
            value[key] = field;
        }
        value
    }

    fn intent(&self, feature: Feature, payload: OutboundPayload) -> ChannelOutboundIntent {
        ChannelOutboundIntent {
            idempotency_key: "kit-op".into(),
            channel_id: "whatsapp".into(),
            conversation_id: self.scope.conversation.id.clone(),
            channel_ref: Some(self.scope.channel.clone()),
            conversation_ref: Some(self.scope.conversation.clone()),
            reply_to_id: (feature == Feature::ReplyAddressing).then(|| "wamid.quoted".into()),
            thread_id: (feature == Feature::ThreadAddressing).then(|| "thread-1".into()),
            turn_id: Some("turn-kit".into()),
            payload,
            ..Default::default()
        }
    }

    fn media_ref(&self, feature: Feature) -> MediaReference {
        MediaReference {
            opaque_id: self.media.get(&feature).cloned(),
            ..Default::default()
        }
    }

    fn observe_wire(&self, feature: Feature) {
        let log = self.log.lock();
        let mut cursor = self.cursor.lock();
        for (index, entry) in log.iter().enumerate().skip(*cursor) {
            if !entry.path.ends_with("/messages") {
                continue;
            }
            let body: Value = serde_json::from_slice(&entry.body).unwrap_or(Value::Null);
            let kind = body["type"].as_str().unwrap_or_default();
            let (operation, payload) = if body["status"] == "read" {
                ("typing", None)
            } else if kind == "reaction" {
                ("reaction_send", None)
            } else {
                let payload = match kind {
                    "text" => "text",
                    "image" => "image",
                    "video" => "video",
                    "document" => "file",
                    "sticker" => "sticker",
                    "location" => "location",
                    "contacts" => "contact",
                    "audio" => {
                        // The preceding upload tells a voice note from audio.
                        let voice = index
                            .checked_sub(1)
                            .and_then(|previous| log.get(previous))
                            .is_some_and(|upload| {
                                String::from_utf8_lossy(&upload.body).contains("codecs=opus")
                            });
                        if voice { "voice" } else { "audio" }
                    }
                    _ => "unknown",
                };
                ("send", Some(payload))
            };
            let to = body["to"].as_str().map(|digits| format!("+{digits}"));
            self.observed.lock().push(WireObservation {
                feature,
                operation: operation.into(),
                channel: self.scope.channel.clone(),
                conversation: ConversationRef {
                    kind: ConversationKind::Dm,
                    id: to.unwrap_or_else(|| self.scope.conversation.id.clone()),
                    ..Default::default()
                },
                reply_to_id: body["context"]["message_id"].as_str().map(str::to_owned),
                thread_id: None,
                payload_kinds: payload.into_iter().map(str::to_owned).collect(),
                platform_message_ids: if operation == "send" {
                    entry.id.clone().into_iter().collect()
                } else {
                    Vec::new()
                },
                request: body,
            });
        }
        *cursor = log.len();
    }
}

#[async_trait::async_trait]
impl AdapterFixture for CloudKit {
    fn adapter(&self) -> &dyn crate::channel::ChannelAdapter {
        self.channel.as_ref()
    }

    fn expected_scope(&self, feature: Feature) -> ExpectedScope {
        let mut scope = self.scope.clone();
        if feature == Feature::ReplyAddressing {
            scope.reply_to_id = Some("wamid.quoted".into());
        }
        scope
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
        let content = |part| OutboundPayload::Parts {
            parts: vec![OutboundContentPart::Content { part }],
        };
        let payload = match feature {
            Feature::Text | Feature::ReplyAddressing | Feature::ThreadAddressing => {
                OutboundPayload::Text {
                    text: "hello kit".into(),
                }
            }
            Feature::Image => content(ContentPart::Image {
                media: self.media_ref(feature),
                caption: Some("image".into()),
                alt_text: None,
            }),
            Feature::Video => content(ContentPart::Video {
                media: self.media_ref(feature),
                caption: Some("video".into()),
                duration_ms: Some(1000),
                thumbnail: None,
            }),
            Feature::Audio => content(ContentPart::Audio {
                media: self.media_ref(feature),
                codec: None,
                duration_ms: Some(1000),
                title: None,
                transcript: None,
            }),
            Feature::File => content(ContentPart::File {
                media: self.media_ref(feature),
                file_name: None,
                caption: Some("file".into()),
            }),
            Feature::Sticker => content(ContentPart::Sticker {
                media: self.media_ref(feature),
                pack_id: None,
                sticker_id: None,
                emoji: None,
                alt_text: None,
            }),
            Feature::Location => content(ContentPart::Location {
                latitude: 29.37,
                longitude: 47.97,
                label: Some("Kit".into()),
                address: None,
                accuracy_meters: None,
                live_until_unix_ms: None,
            }),
            Feature::Contact => content(ContentPart::Contact {
                display_name: "Kit Contact".into(),
                phone_numbers: vec![tinychannels_bus::ContactMethod {
                    value: "+15550123".into(),
                    label: Some("CELL".into()),
                }],
                emails: vec![],
                organization: None,
            }),
            Feature::Buttons => content(ContentPart::Buttons {
                rows: vec![vec![tinychannels_bus::ButtonAction {
                    id: "a".into(),
                    label: "A".into(),
                    style: None,
                    disabled: false,
                }]],
            }),
            Feature::Poll => content(ContentPart::Poll {
                question: "q".into(),
                options: vec!["a".into(), "b".into()],
                multiple_choice: false,
                closes_at_unix_ms: None,
                provider_poll_id: None,
            }),
            Feature::Presentation => content(ContentPart::Presentation {
                blocks: json!([{"type": "text", "text": "x"}]),
            }),
            Feature::NativeData => OutboundPayload::NativeChannelData {
                data: json!({"provider": "whatsapp"}),
            },
            Feature::VoiceOut => OutboundPayload::Voice {
                voice: OutboundVoice::Media {
                    media: MediaReference {
                        opaque_id: Some("voice-kit".into()),
                        ..Default::default()
                    },
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1000),
                },
            },
            Feature::VoiceSynthesis => OutboundPayload::Voice {
                voice: OutboundVoice::Synthesize {
                    text: "speak this".into(),
                    voice: None,
                },
            },
            Feature::Typing
            | Feature::Reactions
            | Feature::InboundReactions
            | Feature::VoiceIn
            | Feature::ReadReceipts
            | Feature::Edit
            | Feature::Delete
            | Feature::StreamingDraft => OutboundPayload::Text { text: "op".into() },
            Feature::ManagedReaction
            | Feature::ManagedThreadCreate
            | Feature::ManagedThreadUpdate
            | Feature::ManagedThreadList => OutboundPayload::Text {
                text: String::new(),
            },
        };
        let intent = self.intent(feature, payload);
        let result = match feature {
            Feature::Edit => {
                let receipt = tinychannels_bus::channel::MessageReceipt::default();
                self.channel.edit(&receipt, intent).await.map(|_| None)
            }
            Feature::Delete => Err(crate::channel::ChannelDelete::delete(
                self.channel.as_ref(),
                &tinychannels_bus::channel::MessageReceipt::default(),
            )
            .await
            .expect_err("the Cloud API cannot unsend")),
            Feature::StreamingDraft => self.channel.upsert_draft(None, intent).await.map(|_| None),
            Feature::Typing => {
                self.post_webhook(self.base_value(json!({
                    "contacts": [{"wa_id": "15550001111", "profile": {"name": "Kit"}}],
                    "messages": [{"from": "15550001111", "id": "wamid.IN1", "timestamp": "1", "type": "text", "text": {"body": "hi"}}]
                })))
                .await;
                crate::channel::ChannelTyping::start_typing(self.channel.as_ref(), "+15550001111")
                    .await
                    .map(|()| None)
            }
            Feature::Reactions => crate::channel::LegacyChannelReaction::send_reaction(
                self.channel.as_ref(),
                &self.scope.channel,
                &self.scope.conversation,
                "wamid.IN1",
                &tinychannels_bus::ChannelReaction {
                    value: "👍".into(),
                    custom_emoji_id: None,
                },
                false,
            )
            .await
            .map(|()| None),
            Feature::InboundReactions => {
                self.sink.ingress.lock().clear();
                self.post_webhook(self.base_value(json!({
                    "messages": [{"from": "15550001111", "id": "wamid.IN2", "timestamp": "2", "type": "reaction",
                                  "reaction": {"message_id": "wamid.OUT1", "emoji": "🔥"}}]
                })))
                .await;
                let event = self
                    .sink
                    .ingress
                    .lock()
                    .iter()
                    .find_map(|ingress| match ingress {
                        ChannelIngress::Event(event) => Some(event.clone()),
                        ChannelIngress::Inbound(_) => None,
                    });
                let event = event.expect("a reaction event was published");
                self.observed.lock().push(WireObservation {
                    feature,
                    operation: "reaction_receive".into(),
                    channel: self.scope.channel.clone(),
                    conversation: event.conversation.clone().unwrap_or_default(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: Vec::new(),
                    platform_message_ids: Vec::new(),
                    request: serde_json::to_value(&event).unwrap(),
                });
                Ok(None)
            }
            Feature::ReadReceipts => {
                self.sink.ingress.lock().clear();
                self.post_webhook(self.base_value(json!({
                    "statuses": [{"id": "wamid.OUT1", "status": "read", "recipient_id": "15550001111", "timestamp": "3"}]
                })))
                .await;
                let event = self
                    .sink
                    .ingress
                    .lock()
                    .iter()
                    .find_map(|ingress| match ingress {
                        ChannelIngress::Event(event) => Some(event.clone()),
                        ChannelIngress::Inbound(_) => None,
                    });
                let event = event.expect("a receipt event was published");
                self.observed.lock().push(WireObservation {
                    feature,
                    operation: "read_receipt".into(),
                    channel: self.scope.channel.clone(),
                    conversation: event.conversation.clone().unwrap_or_default(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: Vec::new(),
                    platform_message_ids: Vec::new(),
                    request: serde_json::to_value(&event).unwrap(),
                });
                Ok(None)
            }
            Feature::VoiceIn => {
                self.sink.ingress.lock().clear();
                Mock::given(wiremock::matchers::method("GET"))
                    .and(wiremock::matchers::path(format!("/{GRAPH_VERSION}/ptt-1")))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "url": format!("{}/cdn/ptt-1", self.server.uri()),
                        "mime_type": "audio/ogg; codecs=opus"
                    })))
                    .mount(&self.server)
                    .await;
                Mock::given(wiremock::matchers::method("GET"))
                    .and(wiremock::matchers::path("/cdn/ptt-1"))
                    .respond_with(ResponseTemplate::new(200).set_body_bytes(ogg_bytes()))
                    .mount(&self.server)
                    .await;
                self.post_webhook(self.base_value(json!({
                    "messages": [{"from": "15550001111", "id": "wamid.IN3", "timestamp": "4", "type": "audio",
                                  "audio": {"id": "ptt-1", "voice": true, "mime_type": "audio/ogg; codecs=opus"}}]
                })))
                .await;
                let envelope = self
                    .sink
                    .ingress
                    .lock()
                    .iter()
                    .find_map(|ingress| match ingress {
                        ChannelIngress::Inbound(envelope) => Some(envelope.clone()),
                        ChannelIngress::Event(_) => None,
                    })
                    .expect("the voice note was published");
                let ContentPart::VoiceNote { voice_note } = &envelope.parts[0] else {
                    panic!("expected a voice note")
                };
                // The module enriches the stored note with the same shared
                // function; run it here so the transcript is evidence too.
                let mut note = voice_note.clone();
                crate::host::enrich_voice_note(
                    &mut note,
                    tinychannels_bus::HostVoiceScope::default(),
                    Some(crate::host::TranscriptionRequest {
                        audio_base64: String::new(),
                        mime_type: Some("audio/ogg".into()),
                        file_name: None,
                        language: None,
                    }),
                    Some(Arc::new(Transcribe)),
                )
                .await;
                let mut kinds = vec!["audio".to_owned()];
                if note.transcript.as_ref().is_some_and(|t| !t.text.is_empty()) {
                    kinds.push("transcript".into());
                }
                self.observed.lock().push(WireObservation {
                    feature,
                    operation: "voice_in".into(),
                    channel: self.scope.channel.clone(),
                    conversation: envelope.conversation.clone(),
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: kinds,
                    platform_message_ids: Vec::new(),
                    request: serde_json::to_value(&envelope).unwrap(),
                });
                Ok(None)
            }
            _ => crate::channel::ChannelAdapter::send_outcome(self.channel.as_ref(), intent)
                .await
                .map(Some),
        };
        self.observe_wire(feature);
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error)
                if matches!(
                    feature,
                    Feature::Edit | Feature::Delete | Feature::StreamingDraft
                ) =>
            {
                debug_assert!(error.unsupported);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
}

#[tokio::test]
async fn the_cloud_adapter_passes_the_whole_conformance_matrix_against_its_wire() {
    let kit = kit().await;
    let results = check_all_features(&kit).await;
    assert_eq!(results.len(), Feature::ALL.len());
    for (feature, result) in &results {
        assert!(
            result.is_ok(),
            "{feature:?}: {}",
            result.as_ref().unwrap_err()
        );
    }
}

#[tokio::test]
async fn the_cloud_adapter_declares_exactly_what_the_protocol_and_its_ports_allow() {
    let kit = kit().await;
    let facts = crate::channel::ChannelAdapter::declared_capabilities(kit.channel.as_ref());
    let declared = |feature: Feature| match feature {
        Feature::Text => facts.content.text.outbound,
        Feature::Image => facts.content.image.outbound,
        Feature::Video => facts.content.video.outbound,
        Feature::Audio => facts.content.audio.outbound,
        Feature::File => facts.content.file.outbound,
        Feature::Sticker => facts.content.sticker.outbound,
        Feature::Location => facts.content.location.outbound,
        Feature::Contact => facts.content.contact.outbound,
        Feature::Buttons => facts.content.buttons.outbound,
        Feature::Poll => facts.content.poll.outbound,
        Feature::Presentation => facts.content.presentation.outbound,
        Feature::NativeData => facts.content.native.outbound,
        Feature::VoiceIn => facts.voice_in.supported,
        Feature::VoiceOut => facts.voice_out.supported,
        Feature::VoiceSynthesis => facts.voice_out.synthesis,
        Feature::ReplyAddressing => facts.addressing.reply_to_id,
        Feature::ThreadAddressing => facts.addressing.thread_id,
        Feature::Typing => facts.actions.supported.iter().any(|a| a == "typing"),
        Feature::Reactions => facts.reactions,
        Feature::InboundReactions => facts.events.supported.iter().any(|e| e == "reaction_added"),
        Feature::Edit => facts.actions.supported.iter().any(|a| a == "edit"),
        Feature::Delete => facts.actions.supported.iter().any(|a| a == "delete"),
        Feature::ReadReceipts => facts.events.supported.iter().any(|e| e == "read_receipt"),
        Feature::StreamingDraft => facts.progressive_edits,
        Feature::ManagedReaction
        | Feature::ManagedThreadCreate
        | Feature::ManagedThreadUpdate
        | Feature::ManagedThreadList => false,
    };
    let supported = [
        Feature::Text,
        Feature::Image,
        Feature::Video,
        Feature::Audio,
        Feature::File,
        Feature::Sticker,
        Feature::Location,
        Feature::Contact,
        Feature::VoiceIn,
        Feature::VoiceOut,
        Feature::ReplyAddressing,
        Feature::Typing,
        Feature::Reactions,
        Feature::InboundReactions,
        Feature::ReadReceipts,
    ];
    for feature in Feature::ALL {
        assert_eq!(
            declared(feature),
            supported.contains(&feature),
            "{feature:?}"
        );
    }
}
