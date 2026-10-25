//! The shared adapter conformance matrix against a capturing SMTP relay.

use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelTyping,
    LegacyChannelReaction as ReactionPort,
};
use crate::providers::email_channel::fixtures::*;
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

const REPLY_ID: &str = "reply@x";
const THREAD_ID: &str = "thread@x";

fn channel_ref() -> ChannelRef {
    ChannelRef {
        id: "email".into(),
        account_id: None,
    }
}

/// The conversation a feature addresses: a thread send is scoped to its thread.
fn conversation_for(feature: ConformanceFeature) -> ConversationRef {
    ConversationRef {
        thread_id: (feature == ConformanceFeature::ThreadAddressing).then(|| THREAD_ID.to_owned()),
        ..conversation()
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
        channel_id: "email".into(),
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

struct EmailFixture {
    rig: Rig,
    smtp: Smtp,
    observations: Mutex<Vec<WireObservation>>,
    cursor: Mutex<usize>,
}

impl EmailFixture {
    async fn start() -> Self {
        let smtp = smtp(false).await;
        let rig = rig(&smtp);
        Self {
            rig,
            smtp,
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

    fn push(
        &self,
        feature: ConformanceFeature,
        operation: &str,
        ids: Vec<String>,
        kinds: Vec<String>,
        request: Value,
    ) {
        self.observations.lock().unwrap().push(WireObservation {
            feature,
            operation: operation.into(),
            channel: channel_ref(),
            conversation: conversation_for(feature),
            reply_to_id: None,
            thread_id: None,
            payload_kinds: kinds,
            platform_message_ids: ids,
            request,
        });
    }

    /// One observation per message the relay accepted since the last harvest,
    /// parsed from the captured DATA.
    async fn harvest(&self, feature: ConformanceFeature) {
        let fresh: Vec<String> = {
            let messages = self.smtp.messages.lock().unwrap();
            let mut cursor = self.cursor.lock().unwrap();
            let fresh = messages[*cursor..].to_vec();
            *cursor = messages.len();
            fresh
        };
        for message in fresh {
            let lower = message.to_ascii_lowercase();
            let kind = match feature {
                ConformanceFeature::Image => "image",
                ConformanceFeature::Video => "video",
                ConformanceFeature::Audio => "audio",
                ConformanceFeature::File => "file",
                ConformanceFeature::VoiceOut | ConformanceFeature::VoiceSynthesis => "voice",
                _ => "text",
            };
            let id = header(&message, "Message-ID")
                .map(|id| id.trim_matches(['<', '>']).to_owned())
                .unwrap_or_default();
            let reply =
                header(&message, "In-Reply-To").map(|id| id.trim_matches(['<', '>']).to_owned());
            let thread = header(&message, "References").and_then(|refs| {
                refs.split_whitespace()
                    .next()
                    .map(|id| id.trim_matches(['<', '>']).to_owned())
            });
            let attached = lower.contains("content-disposition: attachment");
            let mut kinds = vec![kind.to_owned()];
            if attached {
                kinds.push("attachment".into());
            }
            self.observations.lock().unwrap().push(WireObservation {
                feature,
                operation: "send".into(),
                channel: channel_ref(),
                conversation: conversation_for(feature),
                reply_to_id: reply,
                thread_id: thread,
                payload_kinds: kinds,
                platform_message_ids: vec![id],
                request: json!({"to": header(&message, "To")}),
            });
        }
    }
}

#[async_trait::async_trait]
impl AdapterFixture for EmailFixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        &self.rig.channel
    }

    fn expected_scope(&self, feature: ConformanceFeature) -> ExpectedScope {
        ExpectedScope {
            channel: channel_ref(),
            conversation: conversation_for(feature),
            reply_to_id: (feature == ConformanceFeature::ReplyAddressing)
                .then(|| REPLY_ID.to_owned()),
            thread_id: (feature == ConformanceFeature::ThreadAddressing)
                .then(|| THREAD_ID.to_owned()),
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
                reply.reply_to_id = Some(REPLY_ID.into());
                self.send(reply).await
            }
            F::ThreadAddressing => {
                let mut threaded = text_intent("threaded");
                threaded.thread_id = Some(THREAD_ID.into());
                threaded.conversation_ref = Some(conversation_for(F::ThreadAddressing));
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
            F::InboundReactions | F::ReadReceipts => Err(unsupported_here()),
            F::VoiceIn => {
                let raw = raw_with_attachment(
                    "Subject: Voice\r\n",
                    "",
                    "audio/mp4",
                    "memo.m4a",
                    b"\0\0\0\x20ftypM4A voice",
                );
                dispatch(&self.rig.channel, "voice-1@x", raw).await;
                let envelope = self.rig.events.inbound().pop().expect("a voice envelope");
                let mut kinds = vec![];
                if let Some(ContentPart::VoiceNote { voice_note }) = envelope.parts.get(1) {
                    kinds.push("audio".to_owned());
                    if voice_note.transcript.is_some() {
                        kinds.push("transcript".into());
                    }
                }
                self.push(feature, "voice_in", vec![], kinds, json!({}));
                Ok(None)
            }
            F::ManagedReaction | F::ManagedThreadCreate | F::ManagedThreadUpdate | F::ManagedThreadList => Err(ChannelSendError::default()),
        };
        self.harvest(feature).await;
        result
    }
}

#[tokio::test]
async fn email_adapter_passes_the_full_conformance_matrix_against_a_capturing_relay() {
    let fixture = EmailFixture::start().await;
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), ConformanceFeature::ALL.len());
    let failures: Vec<_> = results
        .iter()
        .filter_map(|(feature, result)| result.as_ref().err().map(|e| format!("{feature:?}: {e}")))
        .collect();
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test]
async fn undeclared_features_never_reach_the_relay() {
    let fixture = EmailFixture::start().await;
    for feature in [
        ConformanceFeature::Sticker,
        ConformanceFeature::Edit,
        ConformanceFeature::Delete,
        ConformanceFeature::Reactions,
    ] {
        tinychannels_conformance::check_feature(&fixture, feature)
            .await
            .unwrap();
    }
    assert!(fixture.smtp.messages.lock().unwrap().is_empty());
    let error = fixture
        .send(intent(OutboundPayload::Poll {
            question: "q".into(),
            options: vec![],
        }))
        .await
        .unwrap_err();
    assert!(error.unsupported && error.kind == SendErrorKind::BadFormat);
}

fn unsupported_here() -> ChannelSendError {
    ChannelSendError {
        kind: SendErrorKind::BadFormat,
        message: "Email has no inbound events".into(),
        unsupported: true,
        ..Default::default()
    }
}
