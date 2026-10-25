//! The shared adapter conformance matrix against a fake Messages folder:
//! a scratch `chat.db`, a recording AppleScript runner and a stand-in that
//! records every sent message the way Messages does.

use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelTyping,
    LegacyChannelReaction as ReactionPort,
};
use crate::providers::imessage::fixtures::*;
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

const REPLY_ID: &str = "SENT0";

fn channel_ref() -> ChannelRef {
    ChannelRef {
        id: "imessage".into(),
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
        channel_id: "imessage".into(),
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

struct IMessageFixture {
    rig: Rig,
    messages: tokio::task::JoinHandle<()>,
    observations: Mutex<Vec<WireObservation>>,
    seen: Mutex<usize>,
}

impl Drop for IMessageFixture {
    fn drop(&mut self) {
        self.messages.abort();
    }
}

impl IMessageFixture {
    fn start() -> Self {
        let rig = rig();
        let messages = rig.spawn_messages();
        Self {
            rig,
            messages,
            observations: Mutex::default(),
            seen: Mutex::default(),
        }
    }

    async fn send(
        &self,
        intent: ChannelOutboundIntent,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        ChannelAdapter::send(&self.rig.channel, intent)
            .await
            .map(|receipt| {
                Some(ChannelSendOutcome {
                    receipt,
                    error: None,
                })
            })
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
            conversation: conversation(),
            reply_to_id: None,
            thread_id: None,
            payload_kinds: kinds,
            platform_message_ids: ids,
            request,
        });
    }

    /// One observation per AppleScript the runner ran since the last harvest,
    /// with the guid Messages recorded for it.
    fn harvest(&self, feature: ConformanceFeature) {
        let scripts = self.rig.scripts();
        let mut seen = self.seen.lock().unwrap();
        for (index, script) in scripts.iter().enumerate().skip(*seen) {
            let kind = match feature {
                ConformanceFeature::Image => "image",
                ConformanceFeature::Video => "video",
                ConformanceFeature::Audio => "audio",
                ConformanceFeature::File => "file",
                ConformanceFeature::VoiceOut | ConformanceFeature::VoiceSynthesis => "voice",
                _ => "text",
            };
            self.push(
                feature,
                "send",
                vec![format!("SENT{}", index + 1)],
                vec![kind.into()],
                json!({"script": script}),
            );
        }
        *seen = scripts.len();
    }
}

#[async_trait::async_trait]
impl AdapterFixture for IMessageFixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        &self.rig.channel
    }

    fn expected_scope(&self, _feature: ConformanceFeature) -> ExpectedScope {
        ExpectedScope {
            channel: channel_ref(),
            conversation: conversation(),
            reply_to_id: None,
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
                reply.reply_to_id = Some(REPLY_ID.into());
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
                let (tx, _rx) = mpsc::channel(4);
                let mut row = self.rig.inbound(40, None, false, &[]);
                row.associated_type = 2001;
                row.associated_guid = Some("p:0/SENT1".into());
                self.rig.channel.dispatch_row(row, &tx).await;
                let recorded = self.rig.events.events().pop().expect("a reaction event");
                self.push(
                    feature,
                    "reaction_receive",
                    vec![],
                    vec![],
                    json!({"conversation": recorded.conversation.as_ref().map(|c| c.id.clone())}),
                );
                Ok(None)
            }
            F::VoiceIn => {
                let (tx, _rx) = mpsc::channel(4);
                let caf = self.rig.attachment("Audio Message.caf", b"caf-bytes");
                let row = self.rig.inbound(41, None, true, &[(&caf, "audio/x-caf")]);
                self.rig.channel.dispatch_row(row, &tx).await;
                let envelope = self.rig.events.inbound().pop().expect("a voice envelope");
                let mut kinds = vec![];
                if let Some(ContentPart::VoiceNote { voice_note }) = envelope.parts.first() {
                    kinds.push("audio".to_owned());
                    if voice_note.transcript.is_some() {
                        kinds.push("transcript".into());
                    }
                }
                self.push(feature, "voice_in", vec![], kinds, json!({}));
                Ok(None)
            }
            F::ReadReceipts => {
                let conn = self.rig.conn();
                conn.execute(
                    "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date, date_read) \
                     VALUES (50, 'MINE', 1, 'sent', 1, 700000000000000000, 700000000000000001)",
                    [],
                )
                .unwrap();
                let reads = db::read_since(&conn, 0).unwrap();
                self.rig.channel.dispatch_read(reads.into_iter().next().unwrap()).await;
                let recorded = self.rig.events.events().pop().expect("a receipt event");
                self.push(
                    feature,
                    "read_receipt",
                    vec![],
                    vec![],
                    json!({"conversation": recorded.conversation.as_ref().map(|c| c.id.clone())}),
                );
                Ok(None)
            }
            F::ManagedReaction | F::ManagedThreadCreate | F::ManagedThreadUpdate | F::ManagedThreadList => Err(ChannelSendError::default()),
        };
        self.harvest(feature);
        result
    }
}

#[tokio::test]
async fn imessage_adapter_passes_the_full_conformance_matrix_against_a_fake_messages_folder() {
    let fixture = IMessageFixture::start();
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), ConformanceFeature::ALL.len());
    let failures: Vec<_> = results
        .iter()
        .filter_map(|(feature, result)| result.as_ref().err().map(|e| format!("{feature:?}: {e}")))
        .collect();
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test]
async fn undeclared_features_never_run_a_script() {
    let fixture = IMessageFixture::start();
    for feature in [
        ConformanceFeature::Sticker,
        ConformanceFeature::ReplyAddressing,
        ConformanceFeature::ThreadAddressing,
        ConformanceFeature::Edit,
        ConformanceFeature::Delete,
    ] {
        tinychannels_conformance::check_feature(&fixture, feature)
            .await
            .unwrap();
    }
    assert!(fixture.rig.scripts().is_empty());
    let error = fixture
        .send(intent(OutboundPayload::Poll {
            question: "q".into(),
            options: vec![],
        }))
        .await
        .unwrap_err();
    assert!(error.unsupported && error.kind == SendErrorKind::BadFormat);
}
