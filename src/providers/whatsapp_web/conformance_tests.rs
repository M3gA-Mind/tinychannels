//! The shared conformance kit run against the real Web adapter, with every
//! observation read from what the linked-session double actually received.

use super::*;
use crate::providers::whatsapp_shared::fakes::{
    CollectingSink, MemoryMedia, MemoryVoices, ogg_bytes, png_bytes,
};
use crate::providers::whatsapp_web::fakes::{Call, RecordingClient};
use std::collections::HashMap;
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
use wacore::types::events::{BatchOrigin, Event, InboundMessage, MessageBatch, Receipt};
use wacore::types::message::{MessageInfo, MessageSource};
use wacore::types::presence::ReceiptType;
use whatsapp_rust::buffa::MessageField;
use whatsapp_rust::waproto::whatsapp as wa;

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

struct WebKit {
    channel: Arc<WhatsAppWebChannel>,
    client: Arc<RecordingClient>,
    sink: Arc<CollectingSink>,
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

fn kit() -> WebKit {
    let client = Arc::new(RecordingClient::linked("15559998888@s.whatsapp.net"));
    client.downloads.lock().insert("/v/ptt".into(), ogg_bytes());
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
        WhatsAppWebChannel::new(
            "/tmp/kit.db".into(),
            None,
            None,
            vec!["+15550001111".into()],
        )
        .with_events(sink.clone())
        .with_media_repository(repository)
        .with_voice_media_store(voices),
    );
    *channel.client.lock() = Some(client.clone() as Arc<dyn OutboundClient>);
    WebKit {
        channel,
        client,
        sink,
        cursor: Mutex::new(0),
        observed: Mutex::new(Vec::new()),
        media,
        scope: scope(),
    }
}

fn payload_kind(message: &wa::Message) -> &'static str {
    if message.conversation.is_some() || message.extended_text_message.text.is_some() {
        "text"
    } else if message.image_message.as_option().is_some() {
        "image"
    } else if message.video_message.as_option().is_some() {
        "video"
    } else if let Some(audio) = message.audio_message.as_option() {
        if audio.ptt == Some(true) {
            "voice"
        } else {
            "audio"
        }
    } else if message.document_message.as_option().is_some() {
        "file"
    } else if message.sticker_message.as_option().is_some() {
        "sticker"
    } else if message.location_message.as_option().is_some() {
        "location"
    } else if message.contact_message.as_option().is_some() {
        "contact"
    } else {
        "unknown"
    }
}

fn reply_of(message: &wa::Message) -> Option<String> {
    [
        &message.extended_text_message.context_info,
        &message.image_message.context_info,
        &message.video_message.context_info,
        &message.audio_message.context_info,
        &message.document_message.context_info,
        &message.sticker_message.context_info,
        &message.location_message.context_info,
        &message.contact_message.context_info,
    ]
    .into_iter()
    .find_map(|context| context.stanza_id.clone())
}

fn phone(jid: &str) -> String {
    format!("+{}", jid.split('@').next().unwrap_or_default())
}

impl WebKit {
    fn observation(&self, feature: Feature, operation: &str, jid: &str) -> WireObservation {
        WireObservation {
            feature,
            operation: operation.into(),
            channel: self.scope.channel.clone(),
            conversation: ConversationRef {
                kind: ConversationKind::Dm,
                id: phone(jid),
                ..Default::default()
            },
            reply_to_id: None,
            thread_id: None,
            payload_kinds: Vec::new(),
            platform_message_ids: Vec::new(),
            request: serde_json::Value::Null,
        }
    }

    fn observe_calls(&self, feature: Feature) {
        let calls = self.client.calls();
        let mut cursor = self.cursor.lock();
        for call in calls.iter().skip(*cursor) {
            let observation = match call {
                Call::Send { to, message, id } => {
                    let mut observation = self.observation(feature, "send", to);
                    observation.reply_to_id = reply_of(message);
                    observation.payload_kinds = vec![payload_kind(message).into()];
                    observation.platform_message_ids = vec![id.clone()];
                    observation
                }
                Call::Composing(to) => self.observation(feature, "typing", to),
                Call::Reaction { chat, .. } => self.observation(feature, "reaction_send", chat),
                Call::Edit {
                    chat, original_id, ..
                } => {
                    let mut observation = self.observation(feature, "edit", chat);
                    observation.platform_message_ids = vec![original_id.clone()];
                    observation
                }
                Call::Revoke { chat, id } => {
                    let mut observation = self.observation(feature, "delete", chat);
                    observation.platform_message_ids = vec![id.clone()];
                    observation
                }
                Call::Media { .. } | Call::Read { .. } | Call::Paused(_) => continue,
            };
            self.observed.lock().push(observation);
        }
        *cursor = calls.len();
    }

    fn intent(&self, feature: Feature, payload: OutboundPayload) -> ChannelOutboundIntent {
        ChannelOutboundIntent {
            idempotency_key: "kit-op".into(),
            channel_id: "whatsapp".into(),
            conversation_id: self.scope.conversation.id.clone(),
            channel_ref: Some(self.scope.channel.clone()),
            conversation_ref: Some(self.scope.conversation.clone()),
            reply_to_id: (feature == Feature::ReplyAddressing).then(|| "3EB0QUOTED".into()),
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

    async fn inbound(&self, id: &str, message: wa::Message) {
        let info = MessageInfo {
            source: MessageSource {
                chat: "15550001111@s.whatsapp.net".parse().unwrap(),
                sender: "15550001111@s.whatsapp.net".parse().unwrap(),
                ..Default::default()
            },
            id: id.to_owned(),
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            ..Default::default()
        };
        let inbound = InboundMessage::builder()
            .message(Arc::new(message))
            .info(Arc::new(info))
            .build();
        let batch = Event::Messages(
            MessageBatch::builder()
                .messages(vec![inbound].into())
                .origin(BatchOrigin::Live)
                .build(),
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(4);
        self.channel
            .inbound_state(tx, None)
            .handle_event(&batch)
            .await;
    }

    fn published(&self) -> Vec<ChannelIngress> {
        std::mem::take(&mut *self.sink.ingress.lock())
    }
}

#[async_trait::async_trait]
impl AdapterFixture for WebKit {
    fn adapter(&self) -> &dyn crate::channel::ChannelAdapter {
        self.channel.as_ref()
    }

    fn expected_scope(&self, feature: Feature) -> ExpectedScope {
        let mut scope = self.scope.clone();
        if feature == Feature::ReplyAddressing {
            scope.reply_to_id = Some("3EB0QUOTED".into());
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
        let text = |value: &str| OutboundPayload::Text { text: value.into() };
        let payload = match feature {
            Feature::Image => content(ContentPart::Image {
                media: self.media_ref(feature),
                caption: Some("image".into()),
                alt_text: None,
            }),
            Feature::Video => content(ContentPart::Video {
                media: self.media_ref(feature),
                caption: None,
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
                blocks: serde_json::json!([{"type": "text", "text": "x"}]),
            }),
            Feature::NativeData => OutboundPayload::NativeChannelData {
                data: serde_json::json!({"provider": "whatsapp"}),
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
            _ => text("hello kit"),
        };
        let intent = self.intent(feature, payload);
        let result = match feature {
            Feature::Typing => {
                crate::channel::ChannelTyping::start_typing(self.channel.as_ref(), "+15550001111")
                    .await
                    .map(|()| None)
            }
            Feature::Reactions => crate::channel::LegacyChannelReaction::send_reaction(
                self.channel.as_ref(),
                &self.scope.channel,
                &self.scope.conversation,
                "3EB0TARGET",
                &tinychannels_bus::ChannelReaction {
                    value: "👍".into(),
                    custom_emoji_id: None,
                },
                false,
            )
            .await
            .map(|()| None),
            Feature::Edit => {
                let sent =
                    crate::channel::ChannelAdapter::send(self.channel.as_ref(), intent.clone())
                        .await?;
                let edited = crate::channel::ChannelEdit::edit(
                    self.channel.as_ref(),
                    &sent,
                    ChannelOutboundIntent {
                        payload: text("edited kit"),
                        ..intent
                    },
                )
                .await?;
                Ok(Some(ChannelSendOutcome {
                    receipt: edited,
                    error: None,
                }))
            }
            Feature::Delete => {
                let sent =
                    crate::channel::ChannelAdapter::send(self.channel.as_ref(), intent).await?;
                crate::channel::ChannelDelete::delete(self.channel.as_ref(), &sent)
                    .await
                    .map(|()| None)
            }
            Feature::StreamingDraft => crate::channel::ChannelStreamingDraft::upsert_draft(
                self.channel.as_ref(),
                None,
                intent,
            )
            .await
            .map(|_| None),
            Feature::InboundReactions => {
                self.published();
                self.inbound(
                    "3EB0IN1",
                    wa::Message {
                        reaction_message: MessageField::some(wa::message::ReactionMessage {
                            key: MessageField::some(wa::MessageKey {
                                id: Some("3EB0OUT1".into()),
                                ..Default::default()
                            }),
                            text: Some("🔥".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await;
                let event = self
                    .published()
                    .into_iter()
                    .find_map(|ingress| match ingress {
                        ChannelIngress::Event(event) => Some(event),
                        ChannelIngress::Inbound(_) => None,
                    })
                    .expect("a reaction event was published");
                let mut observation =
                    self.observation(feature, "reaction_receive", "15550001111@s.whatsapp.net");
                observation.conversation = event.conversation.clone().unwrap_or_default();
                observation.request = serde_json::to_value(&event).unwrap();
                self.observed.lock().push(observation);
                Ok(None)
            }
            Feature::ReadReceipts => {
                self.published();
                let (tx, _rx) = tokio::sync::mpsc::channel(4);
                let receipt = Receipt::builder()
                    .source(MessageSource {
                        chat: "15550001111@s.whatsapp.net".parse().unwrap(),
                        sender: "15550001111@s.whatsapp.net".parse().unwrap(),
                        ..Default::default()
                    })
                    .message_ids(vec!["3EB0OUT1".to_owned()])
                    .timestamp(chrono::DateTime::from_timestamp(1_700_000_003, 0).unwrap())
                    .r#type(ReceiptType::Read)
                    .offline(false)
                    .build();
                self.channel
                    .inbound_state(tx, None)
                    .handle_event(&Event::Receipt(receipt))
                    .await;
                let event = self
                    .published()
                    .into_iter()
                    .find_map(|ingress| match ingress {
                        ChannelIngress::Event(event) => Some(event),
                        ChannelIngress::Inbound(_) => None,
                    })
                    .expect("a receipt event was published");
                let mut observation =
                    self.observation(feature, "read_receipt", "15550001111@s.whatsapp.net");
                observation.conversation = event.conversation.clone().unwrap_or_default();
                observation.request = serde_json::to_value(&event).unwrap();
                self.observed.lock().push(observation);
                Ok(None)
            }
            Feature::VoiceIn => {
                self.published();
                self.inbound(
                    "3EB0IN2",
                    wa::Message {
                        audio_message: MessageField::some(wa::message::AudioMessage {
                            direct_path: Some("/v/ptt".into()),
                            mimetype: Some("audio/ogg; codecs=opus".into()),
                            seconds: Some(1),
                            ptt: Some(true),
                            waveform: Some(vec![10, 40, 80]),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await;
                let envelope = self
                    .published()
                    .into_iter()
                    .find_map(|ingress| match ingress {
                        ChannelIngress::Inbound(envelope) => Some(envelope),
                        ChannelIngress::Event(_) => None,
                    })
                    .expect("the voice note was published");
                let ContentPart::VoiceNote { voice_note } = &envelope.parts[0] else {
                    panic!("expected a voice note")
                };
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
                let mut observation =
                    self.observation(feature, "voice_in", "15550001111@s.whatsapp.net");
                observation.payload_kinds = vec!["audio".into()];
                if note.transcript.as_ref().is_some_and(|t| !t.text.is_empty()) {
                    observation.payload_kinds.push("transcript".into());
                }
                observation.conversation = envelope.conversation.clone();
                observation.request = serde_json::to_value(&envelope).unwrap();
                self.observed.lock().push(observation);
                Ok(None)
            }
            _ => crate::channel::ChannelAdapter::send_outcome(self.channel.as_ref(), intent)
                .await
                .map(Some),
        };
        self.observe_calls(feature);
        result
    }
}

#[tokio::test]
async fn the_web_adapter_passes_the_whole_conformance_matrix_against_its_session() {
    let kit = kit();
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
async fn the_web_adapter_declares_exactly_what_the_protocol_and_its_ports_allow() {
    let kit = kit();
    let facts = crate::channel::ChannelAdapter::declared_capabilities(kit.channel.as_ref());
    assert!(
        facts.voice_out.native_waveform,
        "voice notes carry WhatsApp's waveform"
    );
    let has = |name: &str| facts.actions.supported.iter().any(|action| action == name);
    for supported in [
        "send",
        "typing",
        "mark-read",
        "react",
        "reply",
        "edit",
        "delete",
        "unsend",
    ] {
        assert!(has(supported), "{supported}");
    }
    assert!(!has("draft-update") && !facts.progressive_edits);
    assert!(!facts.addressing.thread_id && facts.addressing.reply_to_id);
    assert!(!facts.content.buttons.outbound && !facts.content.poll.outbound);
    assert!(!facts.voice_out.synthesis);
}
