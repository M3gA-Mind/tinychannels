use super::*;
use base64::Engine as _;
use tinychannels_bus::ChannelsConfig;
use tinychannels_bus::config::WhatsAppConfig;

/// Mirrors the predicate in `start_channel`.
///
/// Extracted rather than duplicated in the test so the two cannot disagree:
/// the point of these cases is the *classification*, and a copy of the rule
/// would keep passing after the real one changed.
use super::is_webhook_backed as webhook_backed;

fn whatsapp(cloud: bool) -> ChannelsConfig {
    ChannelsConfig {
        whatsapp: Some(WhatsAppConfig {
            access_token: None,
            phone_number_id: cloud.then(|| "1".to_owned()),
            verify_token: None,
            app_secret: None,
            session_path: (!cloud).then(|| "/tmp/session".to_owned()),
            pair_phone: None,
            pair_code: None,
            allowed_numbers: Vec::new(),
        }),
        ..ChannelsConfig::default()
    }
}

#[test]
fn linq_is_refused_because_its_inbound_path_is_a_webhook() {
    assert!(webhook_backed("linq", &ChannelsConfig::default()));
}

#[test]
fn whatsapp_cloud_is_refused_but_whatsapp_web_is_not() {
    // Both report `name() == "whatsapp"`, so only the config shape separates
    // them. Getting this backwards would either refuse a working provider or
    // silently accept a dead one.
    assert!(webhook_backed("whatsapp", &whatsapp(true)));
    assert!(!webhook_backed("whatsapp", &whatsapp(false)));
}

#[test]
fn providers_with_a_real_listen_loop_are_unaffected() {
    for name in ["telegram", "discord", "slack", "irc", "signal"] {
        assert!(
            !webhook_backed(name, &ChannelsConfig::default()),
            "{name} should not be refused"
        );
    }
}

struct CaptureChannel(tokio::sync::mpsc::Sender<SendMessage>);

#[async_trait::async_trait]
impl tinychannels::Channel for CaptureChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    fn supports_remote_control(&self) -> bool {
        true
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.0.send(message.clone()).await?;
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

struct CaptureTypedChannel(tokio::sync::mpsc::Sender<tinychannels_bus::ChannelOutboundMessage>);

#[async_trait::async_trait]
impl tinychannels::Channel for CaptureTypedChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        let outbound = tinychannels_bus::ContentCapability {
            outbound: true,
            ..Default::default()
        };
        tinychannels_bus::ChannelCapabilities {
            content: tinychannels_bus::ContentCapabilities {
                text: outbound.clone(),
                image: outbound.clone(),
                video: outbound.clone(),
                audio: outbound.clone(),
                file: outbound.clone(),
                sticker: outbound.clone(),
                location: outbound.clone(),
                contact: outbound.clone(),
                buttons: outbound.clone(),
                poll: outbound.clone(),
                presentation: outbound.clone(),
                native: outbound,
                ..Default::default()
            },
            voice_out: tinychannels_bus::VoiceCapabilities {
                supported: true,
                synthesis: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        anyhow::bail!("typed test provider expects typed sends")
    }

    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<()> {
        self.0.send(message.clone()).await?;
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

struct CaptureVoiceReceiptChannel(
    tokio::sync::mpsc::Sender<tinychannels_bus::ChannelOutboundMessage>,
);

#[async_trait::async_trait]
impl tinychannels::Channel for CaptureVoiceReceiptChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    fn capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        tinychannels_bus::ChannelCapabilities {
            voice_out: tinychannels_bus::VoiceCapabilities {
                supported: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        anyhow::bail!("voice intents must stay typed")
    }

    async fn send_typed(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<()> {
        self.0.send(message.clone()).await?;
        Ok(())
    }

    async fn send_typed_with_receipt(
        &self,
        message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<tinychannels_bus::channel::ChannelSendOutcome> {
        self.0.send(message.clone()).await?;
        let mut outcome = tinychannels_bus::channel::ChannelSendOutcome::default();
        outcome.receipt.primary_platform_message_id = Some("voice-message".into());
        outcome
            .receipt
            .platform_message_ids
            .push("voice-message".into());
        Ok(outcome)
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn typed_intent_preserves_durable_key_and_thread_in_legacy_send() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection.clone());
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let channel = Arc::new(CaptureChannel(sent));
    let hooks = Arc::new(Hooks::default());
    let mut running = HashMap::new();
    running.insert(
        "fixture".to_owned(),
        Running::start(channel, hooks, host.clone()),
    );
    let channels = Channels {
        progressive: Arc::new(crate::progressive::Replies::new(
            host.clone(),
            Arc::new(Mutex::new(running.clone())),
        )),
        host: host.clone(),
        email: crate::email::EmailSender::new(),
        media_uploads: Arc::new(crate::media::MediaUploads::default()),
        media_reads: Arc::new(crate::media::MediaReads::default()),
        running: Arc::new(Mutex::new(running)),
        provider_activations: Mutex::new(HashMap::new()),
        relays: Arc::new(Relays::new(host.clone())),
        terminal: AtomicBool::new(false),
        runtimes: None,
        shutdown_task: Mutex::new(None),
        delivery_admission: Arc::new(Mutex::new(())),
    };
    connection
        .serve_at(tinychannels_bus::OBJECT_PATH.try_into().unwrap(), channels)
        .await
        .unwrap();
    connection
        .request_name(tinychannels_bus::BUS_NAME)
        .await
        .unwrap();
    let client = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let intent = ChannelOutboundIntent {
        idempotency_key: "delivery-42".to_owned(),
        channel_id: "fixture".to_owned(),
        conversation_id: "room-7".to_owned(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: Some("message-3".to_owned()),
        thread_id: Some("thread-9".to_owned()),
        turn_id: None,
        durability: tinychannels::DeliveryDurability::BestEffort,
        payload: tinychannels::OutboundPayload::Text {
            text: "hello".to_owned(),
        },
    };
    let result = client
        .call::<tinychannels_bus::controllers::ChannelSendMessageResult>(
            tinychannels_bus::BUS_NAME.try_into().unwrap(),
            tinychannels_bus::OBJECT_PATH.try_into().unwrap(),
            tinychannels_bus::BUS_NAME.try_into().unwrap(),
            tinychannels_bus::methods::SEND_CHANNEL_INTENT
                .try_into()
                .unwrap(),
            ("fixture", intent),
        )
        .await
        .unwrap();
    assert!(result.message_id.is_none());
    assert!(result.receipt.is_none());
    assert!(result.raw.is_none());
    let sent = received.recv().await.unwrap();
    assert_eq!(sent.content, "hello");
    assert_eq!(sent.recipient, "room-7");
    assert_eq!(sent.thread_ts.as_deref(), Some("thread-9"));
    assert_eq!(sent.idempotency_key.as_deref(), Some("delivery-42"));
    client
        .call::<()>(
            tinychannels_bus::BUS_NAME.try_into().unwrap(),
            tinychannels_bus::OBJECT_PATH.try_into().unwrap(),
            tinychannels_bus::BUS_NAME.try_into().unwrap(),
            tinychannels_bus::methods::SHUTDOWN.try_into().unwrap(),
            (),
        )
        .await
        .unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_intent_keeps_media_typed_and_preserves_reply_target() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection.clone());
    let (sent, mut received) = tokio::sync::mpsc::channel(8);
    let channel = Arc::new(CaptureTypedChannel(sent));
    let hooks = Arc::new(Hooks::default());
    let _provider_host = hooks.host(host.clone(), "account-a".into(), None);
    let mut running = HashMap::new();
    running.insert(
        "fixture".to_owned(),
        Running::start(channel, hooks, host.clone()),
    );
    let channels = Channels {
        progressive: Arc::new(crate::progressive::Replies::new(
            host.clone(),
            Arc::new(Mutex::new(running.clone())),
        )),
        host: host.clone(),
        email: crate::email::EmailSender::new(),
        media_uploads: Arc::new(crate::media::MediaUploads::default()),
        media_reads: Arc::new(crate::media::MediaReads::default()),
        running: Arc::new(Mutex::new(running)),
        provider_activations: Mutex::new(HashMap::new()),
        relays: Arc::new(Relays::new(host.clone())),
        terminal: AtomicBool::new(false),
        runtimes: None,
        shutdown_task: Mutex::new(None),
        delivery_admission: Arc::new(Mutex::new(())),
    };
    connection
        .serve_at(tinychannels_bus::OBJECT_PATH.try_into().unwrap(), channels)
        .await
        .unwrap();
    connection
        .request_name(tinychannels_bus::BUS_NAME)
        .await
        .unwrap();
    let client = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let rich_parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "ordered text".into(),
                markdown_dialect: Some("telegram_markdown_v2".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    url: Some("https://example.test/image.png".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("image caption".into()),
                alt_text: Some("image alt".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Video {
                media: tinychannels_bus::MediaReference {
                    url: Some("https://example.test/video.mp4".into()),
                    kind: tinychannels_bus::MediaKind::Video,
                    ..Default::default()
                },
                caption: Some("video caption".into()),
                duration_ms: Some(950),
                thumbnail: Some(tinychannels_bus::MediaReference {
                    url: Some("https://example.test/thumb.jpg".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                }),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Audio {
                media: tinychannels_bus::MediaReference {
                    url: Some("https://example.test/audio.mp3".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                codec: Some("audio/mpeg".into()),
                duration_ms: Some(3300),
                title: Some("audio title".into()),
                transcript: Some("spoken words".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::File {
                media: tinychannels_bus::MediaReference {
                    url: Some("https://example.test/file.pdf".into()),
                    kind: tinychannels_bus::MediaKind::Document,
                    file_name: Some("file.pdf".into()),
                    size_bytes: Some(42),
                    ..Default::default()
                },
                file_name: Some("file.pdf".into()),
                caption: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Sticker {
                media: tinychannels_bus::MediaReference {
                    url: Some("https://example.test/sticker.webp".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                pack_id: Some("pack-opaque".into()),
                sticker_id: Some("sticker-opaque".into()),
                emoji: Some("✨".into()),
                alt_text: Some("sparkles".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Location {
                latitude: 25.2,
                longitude: 55.3,
                label: Some("place".into()),
                address: Some("address".into()),
                accuracy_meters: Some(3.5),
                live_until_unix_ms: Some(5000),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Contact {
                display_name: "Ada".into(),
                phone_numbers: vec![tinychannels_bus::ContactMethod {
                    value: "+1".into(),
                    label: Some("mobile".into()),
                }],
                emails: vec![tinychannels_bus::ContactMethod {
                    value: "ada@example.test".into(),
                    label: None,
                }],
                organization: Some("Analytical Engines".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Buttons {
                rows: vec![vec![tinychannels_bus::ButtonAction {
                    id: "opaque-action".into(),
                    label: "Continue".into(),
                    style: Some(tinychannels_bus::ButtonStyle::Primary),
                    disabled: false,
                }]],
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Poll {
                question: "Q?".into(),
                options: vec!["A".into(), "B".into()],
                multiple_choice: true,
                closes_at_unix_ms: Some(8000),
                provider_poll_id: Some("poll-opaque".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Presentation {
                blocks: serde_json::json!([{"kind":"text","text":"block"}]),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Native {
                provider: "fixture".into(),
                data: serde_json::json!({"opaque":"native"}),
            },
        },
        tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    url: Some("https://example.test/voice.ogg".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                codec: Some("audio/ogg".into()),
                duration_ms: Some(700),
                audio_base64: None,
                waveform: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Synthesize {
                text: "speak this".into(),
                voice: Some("voice-opaque".into()),
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::VoiceNote {
                voice_note: tinychannels_bus::VoiceNote {
                    media: tinychannels_bus::MediaReference {
                        url: Some("https://example.test/note.ogg".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        ..Default::default()
                    },
                    codec: Some("audio/ogg".into()),
                    duration_ms: Some(2000),
                    waveform: Some(vec![0.0, 0.4, 1.0]),
                    push_to_talk: true,
                    transcript: Some(tinychannels_bus::VoiceTranscript {
                        text: "note transcript".into(),
                        language: Some("en".into()),
                        state: tinychannels_bus::VoiceTranscriptState::Complete,
                        source: tinychannels_bus::VoiceTranscriptSource::HostStt,
                    }),
                },
            },
        },
    ];
    let payloads = vec![
        tinychannels_bus::OutboundPayload::Text {
            text: "hello".into(),
        },
        tinychannels_bus::OutboundPayload::Media {
            text: Some("caption".into()),
            media_urls: vec!["https://example.test/image.png".into()],
        },
        tinychannels_bus::OutboundPayload::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    url: Some("https://example.test/audio.ogg".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                codec: Some("audio/ogg".into()),
                duration_ms: Some(1200),
                audio_base64: None,
                waveform: None,
            },
        },
        tinychannels_bus::OutboundPayload::Voice {
            voice: tinychannels_bus::OutboundVoice::Synthesize {
                text: "hello spoken aloud".into(),
                voice: Some("voice-a".into()),
            },
        },
        tinychannels_bus::OutboundPayload::Files {
            file_urls: vec!["https://example.test/file.pdf".into()],
        },
        tinychannels_bus::OutboundPayload::Poll {
            question: "Q?".into(),
            options: vec!["A".into(), "B".into()],
        },
        tinychannels_bus::OutboundPayload::PresentationBlocks {
            blocks: serde_json::json!([{"type":"text"}]),
        },
        tinychannels_bus::OutboundPayload::NativeChannelData {
            data: serde_json::json!({"opaque":true}),
        },
        tinychannels_bus::OutboundPayload::Parts {
            parts: rich_parts.clone(),
        },
    ];
    for (index, payload) in payloads.into_iter().enumerate() {
        client
            .call::<tinychannels_bus::controllers::ChannelSendMessageResult>(
                tinychannels_bus::BUS_NAME.try_into().unwrap(),
                tinychannels_bus::OBJECT_PATH.try_into().unwrap(),
                tinychannels_bus::BUS_NAME.try_into().unwrap(),
                tinychannels_bus::methods::SEND_CHANNEL_INTENT
                    .try_into()
                    .unwrap(),
                (
                    "fixture",
                    ChannelOutboundIntent {
                        idempotency_key: format!("key-{index}"),
                        channel_id: "fixture".into(),
                        conversation_id: "room".into(),
                        channel_ref: Some(tinychannels_bus::channel::ChannelRef {
                            id: "fixture".into(),
                            account_id: None,
                        }),
                        conversation_ref: Some(tinychannels_bus::channel::ConversationRef {
                            kind: tinychannels_bus::channel::ConversationKind::Thread,
                            id: "room".into(),
                            scope_id: Some("workspace-a".into()),
                            parent_id: Some("parent-room".into()),
                            thread_id: Some("thread".into()),
                            topic_id: Some("topic".into()),
                        }),
                        reply_to_id: Some("source-message".into()),
                        thread_id: Some("thread".into()),
                        turn_id: Some(format!("source-turn-{index}")),
                        durability: tinychannels_bus::DeliveryDurability::BestEffort,
                        payload,
                    },
                ),
            )
            .await
            .unwrap();
        let typed = received.recv().await.unwrap();
        assert_eq!(
            typed.turn_id.as_deref(),
            Some(format!("source-turn-{index}").as_str())
        );
        assert_eq!(typed.reply_to_id.as_deref(), Some("source-message"));
        assert_eq!(typed.thread_id.as_deref(), Some("thread"));
        assert_eq!(
            typed
                .channel_ref
                .as_ref()
                .and_then(|channel| channel.account_id.as_deref()),
            Some("account-a")
        );
        let address = typed.conversation_ref.as_ref().unwrap();
        assert_eq!(address.scope_id.as_deref(), Some("workspace-a"));
        assert_eq!(address.parent_id.as_deref(), Some("parent-room"));
        assert_eq!(address.thread_id.as_deref(), Some("thread"));
        assert_eq!(address.topic_id.as_deref(), Some("topic"));
        assert!(!typed.parts.is_empty(), "payload {index} must stay typed");
        assert!(serde_json::to_string(&typed).unwrap().contains("\"kind\":"));
        match index {
            0 => {
                let tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text { text, .. },
                } = &typed.parts[0]
                else {
                    panic!("text intent must produce a typed text part")
                };
                assert_eq!(text, "hello");
            }
            1 => {
                assert_eq!(typed.parts.len(), 2, "caption precedes media");
                let tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text { text, .. },
                } = &typed.parts[0]
                else {
                    panic!("media caption must remain first")
                };
                assert_eq!(text, "caption");
                let tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Image { media, .. },
                } = &typed.parts[1]
                else {
                    panic!("media URL must remain a typed image part")
                };
                assert_eq!(media.url.as_deref(), Some("https://example.test/image.png"));
            }
            2 => {
                let tinychannels_bus::OutboundContentPart::Voice {
                    voice:
                        tinychannels_bus::OutboundVoice::Media {
                            media,
                            codec,
                            duration_ms,
                            ..
                        },
                } = &typed.parts[0]
                else {
                    panic!("voice media intent must remain typed")
                };
                assert_eq!(media.url.as_deref(), Some("https://example.test/audio.ogg"));
                assert_eq!(codec.as_deref(), Some("audio/ogg"));
                assert_eq!(*duration_ms, Some(1200));
            }
            3 => {
                let tinychannels_bus::OutboundContentPart::Voice {
                    voice: tinychannels_bus::OutboundVoice::Synthesize { text, voice },
                } = &typed.parts[0]
                else {
                    panic!("voice synthesis intent must remain typed")
                };
                assert_eq!(text, "hello spoken aloud");
                assert_eq!(voice.as_deref(), Some("voice-a"));
            }
            4 => {
                let tinychannels_bus::OutboundContentPart::Content {
                    part:
                        tinychannels_bus::ContentPart::File {
                            media, file_name, ..
                        },
                } = &typed.parts[0]
                else {
                    panic!("file URL must remain a typed file part")
                };
                assert_eq!(media.url.as_deref(), Some("https://example.test/file.pdf"));
                assert!(file_name.is_none(), "legacy file URL has no supplied name");
            }
            5 => {
                let tinychannels_bus::OutboundContentPart::Content {
                    part:
                        tinychannels_bus::ContentPart::Poll {
                            question, options, ..
                        },
                } = &typed.parts[0]
                else {
                    panic!("poll must remain a typed poll part")
                };
                assert_eq!(question, "Q?");
                assert_eq!(options, &["A", "B"]);
            }
            6 => {
                let tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Presentation { blocks },
                } = &typed.parts[0]
                else {
                    panic!("presentation must remain a typed part")
                };
                assert_eq!(blocks, &serde_json::json!([{"type":"text"}]));
            }
            7 => {
                let tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Native { provider, data },
                } = &typed.parts[0]
                else {
                    panic!("native payload must remain typed and provider-scoped")
                };
                assert_eq!(provider, "fixture");
                assert_eq!(data, &serde_json::json!({"opaque":true}));
            }
            8 => assert_eq!(typed.parts, rich_parts),
            _ => unreachable!(),
        }
    }
    for (key, payload) in [
        (
            "empty-parts",
            tinychannels_bus::OutboundPayload::Parts { parts: Vec::new() },
        ),
        (
            "empty-media",
            tinychannels_bus::OutboundPayload::Media {
                text: None,
                media_urls: Vec::new(),
            },
        ),
        (
            "empty-files",
            tinychannels_bus::OutboundPayload::Files {
                file_urls: Vec::new(),
            },
        ),
    ] {
        let result = client
            .call::<tinychannels_bus::controllers::ChannelSendMessageResult>(
                tinychannels_bus::BUS_NAME.try_into().unwrap(),
                tinychannels_bus::OBJECT_PATH.try_into().unwrap(),
                tinychannels_bus::BUS_NAME.try_into().unwrap(),
                tinychannels_bus::methods::SEND_CHANNEL_INTENT
                    .try_into()
                    .unwrap(),
                (
                    "fixture",
                    ChannelOutboundIntent {
                        idempotency_key: key.into(),
                        channel_id: "fixture".into(),
                        conversation_id: "room".into(),
                        turn_id: None,
                        durability: tinychannels_bus::DeliveryDurability::BestEffort,
                        payload,
                        ..Default::default()
                    },
                ),
            )
            .await;
        assert!(result.is_err(), "{key} normalizes to invalid empty parts");
    }
    client
        .call::<()>(
            tinychannels_bus::BUS_NAME.try_into().unwrap(),
            tinychannels_bus::OBJECT_PATH.try_into().unwrap(),
            tinychannels_bus::BUS_NAME.try_into().unwrap(),
            tinychannels_bus::methods::SHUTDOWN.try_into().unwrap(),
            (),
        )
        .await
        .unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn required_durability_is_refused_before_provider_send_without_host_queue() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let channel = Arc::new(CaptureChannel(sent));
    let hooks = Arc::new(Hooks::default());
    let mut running = HashMap::new();
    running.insert(
        "fixture".to_owned(),
        Running::start(channel, hooks, host.clone()),
    );
    let channels = Channels {
        progressive: Arc::new(crate::progressive::Replies::new(
            host.clone(),
            Arc::new(Mutex::new(running.clone())),
        )),
        host: host.clone(),
        email: crate::email::EmailSender::new(),
        media_uploads: Arc::new(crate::media::MediaUploads::default()),
        media_reads: Arc::new(crate::media::MediaReads::default()),
        running: Arc::new(Mutex::new(running)),
        provider_activations: Mutex::new(HashMap::new()),
        relays: Arc::new(Relays::new(host.clone())),
        terminal: AtomicBool::new(false),
        runtimes: None,
        shutdown_task: Mutex::new(None),
        delivery_admission: Arc::new(Mutex::new(())),
    };
    let result = channels
        .send_channel_intent(
            "fixture".to_owned(),
            ChannelOutboundIntent {
                channel_id: "fixture".to_owned(),
                conversation_id: "room-7".to_owned(),
                channel_ref: None,
                conversation_ref: None,
                idempotency_key: "delivery-required".to_owned(),
                turn_id: None,
                durability: tinychannels::DeliveryDurability::Required,
                payload: tinychannels::OutboundPayload::Text {
                    text: "must remain durable".to_owned(),
                },
                ..Default::default()
            },
        )
        .await;
    assert!(
        result.is_err(),
        "Required must not degrade to a direct send"
    );
    assert!(
        received.try_recv().is_err(),
        "provider must not see refused intent"
    );
    channels.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
}

#[derive(Clone)]
struct DurableHost {
    storage: Arc<crate::storage_fixture::StorageHost>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl DurableHost {
    async fn deliver_inbound(
        &self,
        _channel: String,
        _message: serde_json::Value,
    ) -> tinybus::Result<()> {
        Ok(())
    }

    async fn report_status(
        &self,
        _channel: String,
        _state: String,
        _detail: Option<String>,
    ) -> tinybus::Result<()> {
        Ok(())
    }

    async fn persist_allowed_identity(
        &self,
        _channel: String,
        _identity: String,
    ) -> tinybus::Result<()> {
        Ok(())
    }
}

struct FailingChannel(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl tinychannels::Channel for FailingChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!("fixture provider failure"))
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

struct EndedChannel(Arc<std::sync::atomic::AtomicUsize>);

struct PartialReceiptChannel(Arc<std::sync::atomic::AtomicUsize>);

struct SuccessfulReceiptChannel(Arc<std::sync::atomic::AtomicUsize>);

struct AmbiguousNoIdChannel(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl tinychannels::Channel for AmbiguousNoIdChannel {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn send_typed_with_receipt(
        &self,
        _message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<tinychannels_bus::channel::ChannelSendOutcome> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(tinychannels_bus::channel::ChannelSendOutcome {
            receipt: Default::default(),
            error: Some(tinychannels_bus::channel::ChannelSendError {
                message: "accepted but response had no usable ID".into(),
                sent_before_error: true,
                retryable: false,
                ..Default::default()
            }),
        })
    }
    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn required_durable_ambiguous_send_without_ids_is_not_retried() {
    let sends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channels, callbacks, _server) =
        channels_with_store(Arc::new(AmbiguousNoIdChannel(sends.clone()))).await;
    let intent = required_intent("ambiguous-no-id", "body");
    assert!(
        channels
            .send_channel_intent("fixture".into(), intent.clone())
            .await
            .is_err()
    );
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    let pending = callbacks.storage.snapshot().await;
    let entry = pending
        .get("scope-a")
        .unwrap()
        .pending
        .get("ambiguous-no-id")
        .unwrap();
    assert_eq!(
        entry.recovery_state,
        Some(tinychannels_bus::DeliveryRecoveryState::UnknownAfterSend)
    );
    assert_eq!(
        entry
            .partial_receipt
            .as_ref()
            .unwrap()
            .platform_message_ids
            .len(),
        0
    );
    drop(pending);
    assert!(
        channels
            .send_channel_intent("fixture".into(), intent)
            .await
            .is_err()
    );
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    channels.shutdown().await.unwrap();
}

#[async_trait::async_trait]
impl tinychannels::Channel for SuccessfulReceiptChannel {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn send_typed_with_receipt(
        &self,
        _message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<tinychannels_bus::channel::ChannelSendOutcome> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(tinychannels_bus::channel::ChannelSendOutcome {
            receipt: tinychannels_bus::channel::MessageReceipt {
                primary_platform_message_id: Some("actual-id-9".into()),
                platform_message_ids: vec!["actual-id-9".into()],
                edit_token: Some("telegram:edit:9".into()),
                delete_token: Some("telegram:delete:9".into()),
                ..Default::default()
            },
            error: None,
        })
    }
    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn required_success_and_replay_return_same_actual_scoped_receipt_without_resending() {
    let sends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channels, _callbacks, _server) =
        channels_with_store(Arc::new(SuccessfulReceiptChannel(sends.clone()))).await;
    let intent = required_intent("receipt-replay", "receipt body");
    let first = channels
        .send_channel_intent("fixture".into(), intent.clone())
        .await
        .unwrap();
    let second = channels
        .send_channel_intent("fixture".into(), intent)
        .await
        .unwrap();
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    assert_eq!(first.message_id, second.message_id);
    let receipt = first.receipt.unwrap();
    assert_eq!(Some(receipt.clone()), second.receipt);
    assert_eq!(receipt.platform_message_ids, ["actual-id-9"]);
    assert_eq!(receipt.idempotency_key.as_deref(), Some("receipt-replay"));
    assert_eq!(receipt.edit_token.as_deref(), Some("telegram:edit:9"));
    assert_eq!(receipt.delete_token.as_deref(), Some("telegram:delete:9"));
    assert_eq!(receipt.channel.as_ref().unwrap().id, "fixture");
    assert_eq!(
        receipt.channel.as_ref().unwrap().account_id.as_deref(),
        Some("fixture")
    );
    assert_eq!(receipt.conversation.as_ref().unwrap().id, "room-1");
    channels.shutdown().await.unwrap();
}

#[async_trait::async_trait]
impl tinychannels::Channel for PartialReceiptChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn send_typed_with_receipt(
        &self,
        _message: &tinychannels_bus::ChannelOutboundMessage,
    ) -> anyhow::Result<tinychannels_bus::channel::ChannelSendOutcome> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(tinychannels_bus::channel::ChannelSendOutcome {
            receipt: tinychannels_bus::channel::MessageReceipt {
                platform_message_ids: vec![
                    "actual-provider-id-1".into(),
                    "actual-provider-id-2".into(),
                ],
                ..Default::default()
            },
            error: Some(tinychannels_bus::channel::ChannelSendError {
                message: "third part rejected".into(),
                retryable: true,
                ..Default::default()
            }),
        })
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn required_durable_partial_provider_send_persists_receipt_and_is_not_retried_blindly() {
    let sends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channels, callbacks, _server) =
        channels_with_store(Arc::new(PartialReceiptChannel(sends.clone()))).await;
    let intent = required_intent("partial-required", "multipart body");
    assert!(
        channels
            .send_channel_intent("fixture".into(), intent.clone())
            .await
            .is_err()
    );
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    let scopes = callbacks.storage.snapshot().await;
    let queue = scopes.get("scope-a").unwrap();
    let pending = queue
        .pending
        .get("partial-required")
        .expect("partial send remains pending for reconciliation");
    assert_eq!(
        pending.recovery_state,
        Some(tinychannels_bus::DeliveryRecoveryState::UnknownAfterSend)
    );
    let receipt = pending
        .partial_receipt
        .as_ref()
        .expect("actual partial provider receipt is durable");
    assert_eq!(
        receipt.platform_message_ids,
        ["actual-provider-id-1", "actual-provider-id-2"]
    );
    drop(scopes);

    assert!(
        channels
            .send_channel_intent("fixture".into(), intent)
            .await
            .is_err()
    );
    assert_eq!(
        sends.load(Ordering::SeqCst),
        1,
        "unknown partial sends must never be replayed as untouched sends"
    );
    channels.shutdown().await.unwrap();
}

#[async_trait::async_trait]
impl tinychannels::Channel for EndedChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn channels_with_store(
    channel: Arc<dyn tinychannels::Channel>,
) -> (Channels, DurableHost, tinybus::Connection) {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let callbacks = DurableHost {
        storage: Arc::new(crate::storage_fixture::StorageHost::serve(&server, "scope-a").await),
    };
    server
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callbacks.clone(),
        )
        .await
        .unwrap();
    server
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let client = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    let hooks = Arc::new(Hooks::default());
    let running = HashMap::from([(
        "fixture".to_owned(),
        Running::start(channel, hooks, host.clone()),
    )]);
    let relays = Arc::new(Relays::new(host.clone()));
    let channels = Channels {
        progressive: Arc::new(crate::progressive::Replies::new(
            host.clone(),
            Arc::new(Mutex::new(running.clone())),
        )),
        host,
        email: crate::email::EmailSender::new(),
        media_uploads: Arc::new(crate::media::MediaUploads::default()),
        media_reads: Arc::new(crate::media::MediaReads::default()),
        running: Arc::new(Mutex::new(running)),
        provider_activations: Mutex::new(HashMap::new()),
        relays,
        terminal: AtomicBool::new(false),
        runtimes: None,
        shutdown_task: Mutex::new(None),
        delivery_admission: Arc::new(Mutex::new(())),
    };
    (channels, callbacks, server)
}

fn required_intent(id: &str, text: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: id.into(),
        channel_id: "fixture".into(),
        conversation_id: "room-1".into(),
        channel_ref: None,
        conversation_ref: None,
        turn_id: None,
        durability: tinychannels::DeliveryDurability::Required,
        payload: tinychannels::OutboundPayload::Text { text: text.into() },
        ..Default::default()
    }
}

#[tokio::test]
async fn required_send_persists_before_delivery_and_replays_lost_final_reply() {
    let (sent, mut received) = tokio::sync::mpsc::channel(4);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks
        .storage
        .faults
        .lose_result_reply
        .store(true, Ordering::SeqCst);
    let intent = required_intent("delivery-replay", "confidential body");

    channels
        .send_channel_intent("fixture".into(), intent.clone())
        .await
        .unwrap();
    assert_eq!(received.recv().await.unwrap().content, "confidential body");
    channels
        .send_channel_intent("fixture".into(), intent)
        .await
        .unwrap();
    assert!(received.try_recv().is_err(), "retry must not resend");
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("delivery-replay", "changed payload"),
            )
            .await
            .is_err()
    );

    let scopes = callbacks.storage.snapshot().await;
    let scope = scopes.get("scope-a").unwrap();
    assert!(scope.pending.is_empty());
    assert!(scope.failed.is_empty());
    assert_eq!(scope.results.len(), 1);
}

#[tokio::test]
async fn pending_persistence_failure_refuses_before_provider_send() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks
        .storage
        .faults
        .fail_pending_write
        .store(true, Ordering::SeqCst);
    assert!(
        channels
            .send_channel_intent("fixture".into(), required_intent("persist-fail", "body"))
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    assert!(
        channels.delivery_admission.try_lock().is_ok(),
        "admission guard leaked"
    );
    let scopes = callbacks.storage.snapshot().await;
    assert!(scopes.get("scope-a").is_none());
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn required_send_refuses_invalid_and_oversized_intents_before_persistence() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;

    let mut invalid_key = required_intent("invalid-key", "body");
    invalid_key.idempotency_key.clear();
    assert!(
        channels
            .send_channel_intent("fixture".into(), invalid_key)
            .await
            .is_err()
    );

    let mut invalid_channel = required_intent("invalid-channel", "body");
    invalid_channel.channel_id = " fixture".into();
    assert!(
        channels
            .send_channel_intent(" fixture".into(), invalid_channel)
            .await
            .is_err()
    );

    let oversized = required_intent(
        "oversized",
        &"x".repeat(tinychannels_bus::module_delivery::DELIVERY_RECORD_BYTES),
    );
    assert!(
        channels
            .send_channel_intent("fixture".into(), oversized)
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    assert!(callbacks.storage.snapshot().await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn large_voice_bytes_use_compact_scoped_reference_in_best_effort_and_required_intents() {
    let (sent, mut received) = tokio::sync::mpsc::channel(4);
    let (channels, callbacks, _server) =
        channels_with_store(Arc::new(CaptureVoiceReceiptChannel(sent))).await;
    let voice_intent = |id: &str, durability, audio_base64| ChannelOutboundIntent {
        idempotency_key: id.into(),
        channel_id: "fixture".into(),
        conversation_id: "room-1".into(),
        turn_id: Some(format!("{id}-origin-turn")),
        durability,
        payload: tinychannels_bus::OutboundPayload::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("host-media-opaque".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    content_type: Some("audio/ogg".into()),
                    ..Default::default()
                },
                audio_base64,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1_200),
                waveform: None,
            },
        },
        ..Default::default()
    };

    let raw_audio = base64::engine::general_purpose::STANDARD.encode(vec![7_u8; 128 * 1024 + 1]);
    let oversized = voice_intent(
        "large-inline-voice",
        tinychannels::DeliveryDurability::BestEffort,
        Some(raw_audio),
    );
    assert!(
        channels
            .send_channel_intent("fixture".into(), oversized)
            .await
            .is_err(),
        "raw audio above the relay envelope limit must be rejected before sending"
    );
    assert!(received.try_recv().is_err());

    for durability in [
        tinychannels::DeliveryDurability::BestEffort,
        tinychannels::DeliveryDurability::Required,
    ] {
        let id = match durability {
            tinychannels::DeliveryDurability::BestEffort => "compact-best-effort",
            tinychannels::DeliveryDurability::Required => "compact-required",
            tinychannels::DeliveryDurability::Disabled => unreachable!(),
        };
        let intent = voice_intent(id, durability, None);
        let encoded = serde_json::to_vec(&intent).unwrap();
        assert!(encoded.len() < tinychannels_bus::module_relay::RELAY_FRAME_BYTES);
        assert!(encoded.len() < tinychannels_bus::module_delivery::DELIVERY_RECORD_BYTES);
        assert!(!String::from_utf8_lossy(&encoded).contains("audioBase64"));

        let result = channels
            .send_channel_intent("fixture".into(), intent)
            .await
            .expect("compact durable reference fits normal and required paths");
        if durability == tinychannels::DeliveryDurability::Required {
            assert_eq!(result.message_id.as_deref(), Some("voice-message"));
        }
        let message = received.recv().await.expect("typed provider send");
        assert_eq!(
            message.turn_id.as_deref(),
            Some(format!("{id}-origin-turn").as_str()),
            "the real service dispatch retains the source turn separately from the idempotency key"
        );
        assert_eq!(message.idempotency_key, id);
        assert!(matches!(
            &message.parts[0],
            tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media { media, audio_base64: None, .. }
            } if media.opaque_id.as_deref() == Some("host-media-opaque")
        ));
    }
    assert!(!callbacks.storage.snapshot().await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_required_delivery_target_keeps_the_intent_pending() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    channels.running.lock().await.remove("fixture");

    assert!(
        channels
            .send_channel_intent("fixture".into(), required_intent("no-target", "body"))
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    let scopes = callbacks.storage.snapshot().await;
    assert_eq!(scopes.get("scope-a").unwrap().pending.len(), 1);
    assert!(scopes.get("scope-a").unwrap().failed.is_empty());
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn ended_provider_does_not_mark_unstarted_delivery_as_unknown_send() {
    let send_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channels, callbacks, _server) =
        channels_with_store(Arc::new(EndedChannel(send_calls.clone()))).await;
    let provider = channels
        .running
        .lock()
        .await
        .get("fixture")
        .unwrap()
        .clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !provider.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(
        channels
            .send_channel_intent("fixture".into(), required_intent("ended-provider", "body"))
            .await
            .is_err()
    );
    assert_eq!(send_calls.load(Ordering::SeqCst), 0);
    let scopes = callbacks.storage.snapshot().await;
    let queue = scopes.get("scope-a").unwrap();
    assert!(queue.failed.is_empty());
    let pending = queue.pending.get("ended-provider").unwrap();
    assert_eq!(pending.recovery_state, None);
    assert!(pending.platform_send_started_at_unix_ms.is_none());
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn replayed_terminal_result_acknowledges_a_remaining_pending_record() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = required_intent("terminal-cleanup", "body");
    let fingerprint = intent_fingerprint(&intent).unwrap();
    let store = crate::delivery::HostDeliveryStore::from_host(&channels.host);
    store
        .save_pending(QueuedDelivery::new(
            "terminal-cleanup",
            intent.clone(),
            now_unix_ms(),
        ))
        .await
        .unwrap();
    store
        .save_result(tinychannels_bus::DeliveryTerminalResult {
            id: "terminal-cleanup".into(),
            intent_fingerprint: fingerprint,
            completed_at_unix_ms: now_unix_ms(),
            receipt: None,
        })
        .await
        .unwrap();

    channels
        .send_channel_intent("fixture".into(), intent)
        .await
        .unwrap();
    assert!(received.try_recv().is_err());
    assert!(callbacks.storage.pending("scope-a").await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_recovery_acks_a_matching_terminal_result_without_resending() {
    use tinychannels::delivery::{DeliveryQueueStore as _, DeliveryRecoveryState, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = required_intent("recovery-terminal", "already sent");
    let store = crate::delivery::HostDeliveryStore::from_host(&channels.host);
    let mut entry = QueuedDelivery::new("recovery-terminal", intent.clone(), now_unix_ms());
    entry.recovery_state = Some(DeliveryRecoveryState::SendAttemptStarted);
    entry.platform_send_started_at_unix_ms = Some(now_unix_ms());
    store.save_pending(entry).await.unwrap();
    store
        .save_result(tinychannels_bus::DeliveryTerminalResult {
            id: "recovery-terminal".into(),
            intent_fingerprint: intent_fingerprint(&intent).unwrap(),
            completed_at_unix_ms: now_unix_ms(),
            receipt: None,
        })
        .await
        .unwrap();

    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();

    let scopes = callbacks.storage.snapshot().await;
    let scope = scopes.get("scope-a").unwrap();
    assert!(scope.pending.is_empty());
    assert!(scope.failed.is_empty());
    drop(scopes);
    assert!(
        received.try_recv().is_err(),
        "recovery must not resend success"
    );
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_recovery_rejects_a_conflicting_terminal_result() {
    use tinychannels::delivery::{DeliveryQueueStore as _, DeliveryRecoveryState, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = required_intent("recovery-conflict", "pending intent");
    let store = crate::delivery::HostDeliveryStore::from_host(&channels.host);
    let mut entry = QueuedDelivery::new("recovery-conflict", intent, now_unix_ms());
    entry.recovery_state = Some(DeliveryRecoveryState::SendAttemptStarted);
    entry.platform_send_started_at_unix_ms = Some(now_unix_ms());
    store.save_pending(entry).await.unwrap();
    store
        .save_result(tinychannels_bus::DeliveryTerminalResult {
            id: "recovery-conflict".into(),
            intent_fingerprint: "f".repeat(64),
            completed_at_unix_ms: now_unix_ms(),
            receipt: None,
        })
        .await
        .unwrap();

    assert!(
        recover_delivery_channel(&channels.host, &channels.running, "fixture")
            .await
            .is_err(),
        "a terminal result for a different intent is a key conflict"
    );
    let scopes = callbacks.storage.snapshot().await;
    let scope = scopes.get("scope-a").unwrap();
    assert_eq!(scope.pending.len(), 1, "conflicts stay retryable");
    assert!(scope.failed.is_empty());
    drop(scopes);
    assert!(received.try_recv().is_err());
    channels.shutdown().await.unwrap_err();
}

#[tokio::test]
async fn failed_and_pending_keys_refuse_reuse_with_different_intents() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let failed = required_intent("failed-conflict", "original");
    let pending = required_intent("pending-conflict", "original");
    let store = crate::delivery::HostDeliveryStore::from_host(&channels.host);
    store
        .save_failed(QueuedDelivery::new("failed-conflict", failed, 1))
        .await
        .unwrap();
    store
        .save_pending(QueuedDelivery::new("pending-conflict", pending, 1))
        .await
        .unwrap();

    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("failed-conflict", "original"),
            )
            .await
            .is_err()
    );
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("failed-conflict", "changed"),
            )
            .await
            .is_err()
    );
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("pending-conflict", "changed"),
            )
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    let scopes = callbacks.storage.snapshot().await;
    let queue = scopes.get("scope-a").unwrap();
    assert_eq!(queue.failed.len(), 1);
    assert_eq!(queue.pending.len(), 1);
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn changed_intent_cannot_reuse_a_terminal_result_key() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks
        .storage
        .seed_result(
            "scope-a",
            tinychannels_bus::DeliveryTerminalResult {
                id: "result-conflict".into(),
                intent_fingerprint: "a".repeat(64),
                completed_at_unix_ms: now_unix_ms(),
                receipt: None,
            },
        )
        .await;

    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("result-conflict", "different intent"),
            )
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn stop_channel_keeps_provider_running_when_queue_recovery_fails() {
    let (sent, _received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    callbacks
        .storage
        .faults
        .fail_queue_read
        .store(true, Ordering::SeqCst);

    assert!(channels.stop_channel("fixture".into()).await.is_err());
    assert!(channels.running.lock().await.contains_key("fixture"));

    callbacks
        .storage
        .faults
        .fail_queue_read
        .store(false, Ordering::SeqCst);
    channels.stop_channel("fixture".into()).await.unwrap();
    assert!(!channels.running.lock().await.contains_key("fixture"));
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn best_effort_rich_payload_is_rejected_by_the_legacy_text_only_provider() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, _callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let intent = ChannelOutboundIntent {
        idempotency_key: "structured-1".into(),
        channel_id: "fixture".into(),
        turn_id: None,
        durability: tinychannels::DeliveryDurability::BestEffort,
        payload: tinychannels::OutboundPayload::NativeChannelData {
            data: serde_json::json!({"kind": "card", "safe": true}),
        },
        ..Default::default()
    };

    let error = channels
        .send_channel_intent("fixture".into(), intent)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BusError::MethodFailed { name, .. } if name == SEND_FAILED_ERROR
    ));
    assert!(
        received.try_recv().is_err(),
        "rich content never reaches SendMessage"
    );
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_failure_stays_pending_then_recovery_fails_closed_once() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channels, callbacks, _server) =
        channels_with_store(Arc::new(FailingChannel(calls.clone()))).await;
    let intent = required_intent("provider-fail", "body");
    assert!(
        channels
            .send_channel_intent("fixture".into(), intent.clone())
            .await
            .is_err()
    );
    {
        let scopes = callbacks.storage.snapshot().await;
        let scope = scopes.get("scope-a").unwrap();
        assert_eq!(scope.pending.len(), 1);
        assert!(scope.failed.is_empty());
    }

    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let scopes = callbacks.storage.snapshot().await;
    let scope = scopes.get("scope-a").unwrap();
    assert!(scope.pending.is_empty());
    assert_eq!(scope.failed.len(), 1);
    drop(scopes);
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_recovery_is_ordered_and_respects_host_user_scope() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(4);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let store = crate::delivery::HostDeliveryStore::from_host(&channels.host);
    let later = required_intent("recovery-later", "later");
    let earlier = required_intent("recovery-earlier", "earlier");
    store
        .save_pending(QueuedDelivery::new("recovery-later", later, 20))
        .await
        .unwrap();
    store
        .save_pending(QueuedDelivery::new("recovery-earlier", earlier, 10))
        .await
        .unwrap();
    callbacks.storage.set_scope("scope-b");
    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();
    assert!(
        received.try_recv().is_err(),
        "another user scope stays isolated"
    );

    callbacks.storage.set_scope("scope-a");
    recover_delivery_channel(&channels.host, &channels.running, "fixture")
        .await
        .unwrap();
    assert_eq!(received.recv().await.unwrap().content, "earlier");
    assert_eq!(received.recv().await.unwrap().content, "later");
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_retryable_queue_before_stopping_provider() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, _callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let store = crate::delivery::HostDeliveryStore::from_host(&channels.host);
    let intent = required_intent("shutdown-drain", "drain before stop");
    store
        .save_pending(QueuedDelivery::new("shutdown-drain", intent, 1))
        .await
        .unwrap();

    channels.shutdown().await.unwrap();
    assert_eq!(received.recv().await.unwrap().content, "drain before stop");
}

#[tokio::test]
async fn shutdown_store_failure_still_joins_provider_and_can_retry_persistence() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (sent, _received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let provider = channels
        .running
        .lock()
        .await
        .get("fixture")
        .unwrap()
        .clone();
    let store = crate::delivery::HostDeliveryStore::from_host(&channels.host);
    let intent = required_intent("shutdown-store-outage", "retain for restart");
    store
        .save_pending(QueuedDelivery::new(
            "shutdown-store-outage",
            intent,
            now_unix_ms(),
        ))
        .await
        .unwrap();
    channels.relays.reserve().await.unwrap();
    callbacks
        .storage
        .faults
        .fail_queue_read
        .store(true, Ordering::SeqCst);

    assert!(channels.shutdown().await.is_err());
    assert!(
        provider.is_finished(),
        "native provider must be joined on error"
    );
    assert!(channels.running.lock().await.is_empty());
    assert!(
        channels.relays.is_empty().await,
        "relay owner must be stopped"
    );
    assert_eq!(
        callbacks.storage.pending("scope-a").await.len(),
        1,
        "queue data remains host-owned and retryable"
    );
    assert!(
        channels.host.flush().await.is_err(),
        "host callback owner must be closed after shutdown cleanup"
    );

    callbacks
        .storage
        .faults
        .fail_queue_read
        .store(false, Ordering::SeqCst);
    channels.shutdown().await.unwrap();
    assert_eq!(
        callbacks.storage.pending("scope-a").await.len(),
        1,
        "a stopped provider leaves the durable entry for the next start"
    );
}

#[tokio::test]
async fn nested_runtime_shutdown_failure_still_drains_root_owners_and_can_retry() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let module = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host_server = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let root_callbacks = DurableHost {
        storage: Arc::new(
            crate::storage_fixture::StorageHost::serve(&host_server, "scope-a").await,
        ),
    };
    host_server
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            root_callbacks.clone(),
        )
        .await
        .unwrap();
    host_server
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();

    let root_host = HostChannels::new(module.clone());
    let runtimes = Arc::new(crate::runtime::Runtimes::new(module.clone()));
    let channels = Channels::new(root_host.clone(), Some(runtimes.clone()));
    let (sent, _received) = tokio::sync::mpsc::channel(1);
    let provider = Running::start(
        Arc::new(CaptureChannel(sent)),
        Arc::new(Hooks::default()),
        root_host.clone(),
    );
    channels
        .running
        .lock()
        .await
        .insert("fixture".into(), provider.clone());
    channels.relays.reserve().await.unwrap();

    let reservation = runtimes.reserve().await.unwrap();
    // The nested runtime shares the module's storage connection, so one
    // outage on the pending queue fails both the nested and the root cleanup.
    root_callbacks
        .storage
        .faults
        .fail_queue_read
        .store(true, Ordering::SeqCst);
    runtimes.start(reservation.handle.clone()).await.unwrap();

    assert!(channels.shutdown().await.is_err());
    assert!(
        runtimes.stop(reservation.handle.clone()).await.is_err(),
        "the failed nested runtime owner must remain retryable"
    );
    assert!(
        provider.is_finished(),
        "root providers must be joined despite a nested runtime stop error"
    );
    assert!(channels.running.lock().await.is_empty());
    assert!(channels.relays.is_empty().await);
    assert!(
        root_callbacks
            .storage
            .faults
            .pending_reads
            .load(Ordering::SeqCst)
            > 0
    );
    assert!(
        channels
            .email
            .send(tinychannels_bus::module_email::EmailSendRequest {
                config: tinychannels_bus::config::EmailConfig::default(),
                recipient: "recipient@example.test".into(),
                subject: "fixture".into(),
                body: "fixture".into(),
                attachments: Vec::new(),
            })
            .await
            .is_err(),
        "email admission must be closed even when nested runtime cleanup fails"
    );
    assert!(root_host.flush().await.is_err());
    root_callbacks
        .storage
        .faults
        .fail_queue_read
        .store(false, Ordering::SeqCst);
    channels.shutdown().await.unwrap();
    assert!(runtimes.start(reservation.handle).await.is_err());
}

#[tokio::test]
async fn start_waiting_for_delivery_admission_is_rejected_after_shutdown_freezes() {
    let (sent, _received) = tokio::sync::mpsc::channel(1);
    let (channels, _callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    let provider = channels
        .running
        .lock()
        .await
        .get("fixture")
        .unwrap()
        .clone();
    let admission = channels.delivery_admission.clone();
    let guard = admission.lock().await;
    let admission_future = finish_start_admission(
        &channels.host,
        &channels.running,
        &admission,
        &channels.terminal,
        "fixture",
    );
    tokio::pin!(admission_future);
    tokio::select! {
        result = &mut admission_future => panic!("admission should wait for its lock: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }

    channels.terminal.store(true, Ordering::SeqCst);
    drop(guard);

    assert!(admission_future.await.is_err());
    assert!(provider.is_finished());
    assert!(channels.running.lock().await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn full_durable_queue_applies_backpressure_before_provider_send() {
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let (channels, callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    for index in 0..tinychannels_bus::module_delivery::DELIVERY_PENDING_CAPACITY {
        let id = format!("queued-{index}");
        callbacks
            .storage
            .seed_pending(
                "scope-a",
                tinychannels_bus::DeliveryQueueEntry::new(
                    id.clone(),
                    required_intent(&id, "queued"),
                    index as u64,
                ),
            )
            .await;
    }
    assert!(
        channels
            .send_channel_intent(
                "fixture".into(),
                required_intent("queue-overflow", "must stay queued"),
            )
            .await
            .is_err()
    );
    assert!(received.try_recv().is_err());
    assert_eq!(
        callbacks.storage.pending("scope-a").await.len(),
        tinychannels_bus::module_delivery::DELIVERY_PENDING_CAPACITY,
        "the refused intent was not queued"
    );
    callbacks.storage.clear_pending("scope-a").await;
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_catalog_reports_webhook_mode_without_starting_a_listener() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let channels = Channels::new(host.clone(), None);
    let config = ChannelsConfig {
        linq: Some(tinychannels_bus::config::LinqConfig {
            api_token: "fixture-token".into(),
            from_phone: "+15550000001".into(),
            allowed_senders: vec![],
        }),
        ..Default::default()
    };
    let result = tinybus::Interface::call(
        &channels,
        &tinybus::MemberName::try_from("DescribeConfiguredChannels").unwrap(),
        serde_json::json!([config, HttpProxyPolicy::default()]),
    )
    .await
    .unwrap();
    assert_eq!(result.as_array().unwrap().len(), 1);
    assert_eq!(result[0]["name"], "linq");
    assert_eq!(result[0]["ingress"], "external_webhook");
    assert!(channels.running.lock().await.is_empty());
    channels.shutdown().await.unwrap();
}

#[tokio::test]
async fn preparation_preserves_explicit_keys_and_typed_thread_before_provider_io() {
    use tinychannels_bus::module_runtime::ChannelMessagePreparation;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let channels = Channels::new(host.clone(), None);
    let legacy =
        serde_json::json!({"text":"fixture", "idempotencyKey":"original-key", "threadId":"topic"});
    let prepared = channels
        .prepare_outbound_intent(
            "telegram".into(),
            ChannelMessagePreparation::Legacy(legacy.clone()),
        )
        .await
        .unwrap();
    assert_eq!(prepared.intent.idempotency_key, "original-key");
    assert_eq!(prepared.legacy_message, legacy);
    let typed = SendMessage::new("fixture", "recipient").in_thread(Some("topic".into()));
    let expected = tinychannels::outbound_intent_from_send_message("telegram", &typed);
    let prepared = channels
        .prepare_outbound_intent("telegram".into(), ChannelMessagePreparation::Typed(typed))
        .await
        .unwrap();
    assert_eq!(prepared.intent.idempotency_key, expected.idempotency_key);
    assert_eq!(prepared.intent.thread_id.as_deref(), Some("topic"));
    assert_eq!(
        prepared.legacy_message["idempotencyKey"],
        prepared.intent.idempotency_key
    );
    assert!(
        channels
            .prepare_outbound_intent(
                "telegram".into(),
                ChannelMessagePreparation::Legacy(serde_json::json!({"text":"a".repeat(tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES)}))
            )
            .await
            .is_err()
    );
    assert!(
        channels
            .prepare_outbound_intent(
                "telegram".into(),
                ChannelMessagePreparation::Legacy(
                    serde_json::json!({"text": "a".repeat(256*1024)})
                ),
            )
            .await
            .is_ok(),
        "large legacy sends must not acquire a smaller callback-queue limit"
    );
    let original = tinychannels_bus::ChannelOutboundIntent {
        channel_id: "telegram".into(),
        idempotency_key: "existing-required-key".into(),
        turn_id: None,
        durability: tinychannels_bus::DeliveryDurability::Required,
        payload: tinychannels_bus::OutboundPayload::Text {
            text: "fixture".into(),
        },
        ..Default::default()
    };
    let rendered = channels
        .prepare_outbound_intent(
            "telegram".into(),
            ChannelMessagePreparation::Intent(original.clone()),
        )
        .await
        .unwrap();
    assert_eq!(rendered.intent.idempotency_key, original.idempotency_key);
    assert_eq!(rendered.intent.durability, original.durability);
    assert_eq!(rendered.legacy_message["text"], "fixture");
    assert_eq!(
        rendered.legacy_message["idempotencyKey"],
        "existing-required-key"
    );
    assert!(
        channels
            .prepare_outbound_intent(
                "discord".into(),
                ChannelMessagePreparation::Intent(original)
            )
            .await
            .is_err()
    );
    assert!(channels.running.lock().await.is_empty());
    channels.shutdown().await.unwrap();
}

struct OutboundOnlyFixture(tokio::sync::mpsc::Sender<SendMessage>);

#[async_trait::async_trait]
impl tinychannels::Channel for OutboundOnlyFixture {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.0.send(message.clone()).await?;
        Ok(())
    }
    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        panic!("external webhook providers must not start a native listener")
    }
}

#[tokio::test]
async fn outbound_only_worker_sends_and_joins_without_polling_webhook_listener() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let running = Running::start_outbound(
        Arc::new(OutboundOnlyFixture(sent)),
        Arc::new(Hooks::default()),
        host.clone(),
    );
    running
        .send(SendMessage::new("fixture", "recipient"))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(received.recv().await.unwrap().content, "fixture");
    assert!(!running.is_finished());
    running.stop().await.unwrap();
    assert!(running.is_finished());
    assert!(
        running
            .send(SendMessage::new("closed", "recipient"))
            .await
            .is_err()
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_preparation_preserves_escaped_persisted_keys_and_provider_thread_rules() {
    use tinychannels_bus::module_inbound::{ChannelInboundSource, PrepareChannelInboundRequest};
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = Channels::new(HostChannels::new(connection), None);
    let message = tinychannels_bus::ChannelMessage {
        id: "message-1".into(),
        channel: "discord".into(),
        sender: "sender/a".into(),
        reply_target: "room#b".into(),
        content: "fixture content".into(),
        timestamp: 42,
        topic_id: None,
        reply_to_id: None,
        thread_ts: Some("thread/c".into()),
        sender_name: Some("Display".into()),
    };
    let prepared = service
        .prepare_channel_inbound(PrepareChannelInboundRequest {
            source: ChannelInboundSource::Legacy {
                message: message.clone(),
                envelope: None,
            },
            namespace: "openhuman".into(),
            session_policy: Default::default(),
        })
        .await
        .unwrap();
    assert_eq!(
        prepared.thread_id,
        "channel:discord/sender%2Fa/room%23b#thread:thread%2Fc"
    );
    assert_eq!(prepared.client_id, "inbound:discord:sender/a");
    assert_eq!(
        prepared.history_key,
        "discord_sender/a_room#b_thread:thread/c"
    );
    assert_eq!(prepared.message.timestamp, 42);
    assert_eq!(prepared.message.sender_name.as_deref(), Some("Display"));
    assert_eq!(prepared.envelope.text, "fixture content");
    assert_eq!(
        prepared.session_key,
        "openhuman:discord:default:unknown:room#b:thread/c"
    );
    service.shutdown().await.unwrap();
}

async fn inbound_preparation_service() -> Channels {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    Channels::new(host, None)
}

async fn enable_remote_providers(channels: &Channels) {
    let host = channels.host.clone();
    for configured_name in ["telegram", "discord"] {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        channels.running.lock().await.insert(
            configured_name.to_owned(),
            Running::start(
                Arc::new(CaptureChannel(sender)),
                Arc::new(Hooks::default()),
                host.clone(),
            ),
        );
    }
}

#[tokio::test]
async fn runtime_command_preparation_covers_supported_commands_and_limits() {
    use tinychannels_bus::context::ChannelRouteSelection;
    use tinychannels_bus::module_routes::{
        ModelCatalog, ModelCatalogEntry, PrepareRuntimeCommandRequest, PreparedRuntimeCommand,
        ProviderDescriptor,
    };

    let service = inbound_preparation_service().await;
    enable_remote_providers(&service).await;
    let request = |content: &str| PrepareRuntimeCommandRequest {
        channel: "telegram".into(),
        content: content.into(),
        current: ChannelRouteSelection {
            provider: "anthropic".into(),
            model: "claude-fixture".into(),
        },
        providers: vec![ProviderDescriptor {
            name: "anthropic".into(),
            aliases: vec!["claude".into()],
        }],
        model_catalog: ModelCatalog {
            entries: vec![ModelCatalogEntry {
                provider: "anthropic".into(),
                models: vec!["first-model".into(), "second-model".into()],
            }],
        },
    };

    assert_eq!(
        service
            .prepare_runtime_command(request("/models claude"))
            .await
            .unwrap(),
        Some(PreparedRuntimeCommand::SetProvider {
            requested: "claude".into(),
            provider: Some("anthropic".into()),
        })
    );
    assert_eq!(
        service
            .prepare_runtime_command(request("/models unknown"))
            .await
            .unwrap(),
        Some(PreparedRuntimeCommand::SetProvider {
            requested: "unknown".into(),
            provider: None,
        })
    );
    assert_eq!(
        service
            .prepare_runtime_command(request("/model custom model"))
            .await
            .unwrap(),
        Some(PreparedRuntimeCommand::SetModel("custom model".into()))
    );
    assert_eq!(
        service
            .prepare_runtime_command(request("/models"))
            .await
            .unwrap(),
        Some(PreparedRuntimeCommand::ShowProviders(
            "Current provider: `anthropic`\nCurrent model: `claude-fixture`\n\nSwitch provider with `/models <provider>`.\nSwitch model with `/model <model-id>`.\n\nAvailable providers:\n- anthropic (aliases: claude)\n".into()
        ))
    );
    assert_eq!(
        service
            .prepare_runtime_command(request("/model"))
            .await
            .unwrap(),
        Some(PreparedRuntimeCommand::ShowModel(
            "Current provider: `anthropic`\nCurrent model: `claude-fixture`\n\nSwitch model with `/model <model-id>`.\n\nCached model IDs (top 2):\n- `first-model`\n- `second-model`\n".into()
        ))
    );
    assert_eq!(
        service
            .prepare_runtime_command(request("/unknown"))
            .await
            .unwrap(),
        None
    );

    let mut unsupported_channel = request("/models");
    unsupported_channel.channel = "email".into();
    assert_eq!(
        service
            .prepare_runtime_command(unsupported_channel)
            .await
            .unwrap(),
        None
    );

    let mut oversized = request("/models");
    oversized.content = "x".repeat(tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES);
    assert!(service.prepare_runtime_command(oversized).await.is_err());

    service.shutdown().await.unwrap();
    assert!(
        service
            .prepare_runtime_command(request("/models"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn hosted_identity_preserves_absent_sender_and_telegram_topic_rules() {
    use tinychannels_bus::module_inbound::PrepareChannelIdentityRequest;
    let service = inbound_preparation_service().await;
    let prepared = service
        .prepare_channel_identity(PrepareChannelIdentityRequest {
            channel: "telegram:bot".into(),
            sender: None,
            reply_target: Some("chat".into()),
            thread_ts: Some("topic".into()),
        })
        .await
        .unwrap();
    assert_eq!(prepared.thread_id, "channel:telegram:bot/chat");
    assert_eq!(prepared.client_id, "inbound");
    let prepared = service
        .prepare_channel_identity(PrepareChannelIdentityRequest {
            channel: "discord:bot".into(),
            sender: Some("user:one".into()),
            reply_target: Some("chat".into()),
            thread_ts: Some("topic".into()),
        })
        .await
        .unwrap();
    assert_eq!(
        prepared.thread_id,
        "channel:discord:bot/user:one/chat#thread:topic"
    );
    assert_eq!(prepared.client_id, "inbound:discord%3Abot:user%3Aone");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_envelope_preserves_media_account_and_group_isolation_policy() {
    use tinychannels_bus::module_inbound::{ChannelInboundSource, PrepareChannelInboundRequest};
    let service = inbound_preparation_service().await;
    let mut envelope = tinychannels_bus::ChannelInboundEnvelope::default();
    envelope.channel.id = "discord".into();
    envelope.channel.account_id = Some("account".into());
    envelope.message_id = "msg".into();
    envelope.sender.id = "user".into();
    envelope.sender.name = Some("Display".into());
    envelope.conversation.id = "room".into();
    envelope.conversation.kind = tinychannels_bus::channel::ConversationKind::Group;
    envelope.conversation.thread_id = Some("thread".into());
    envelope.text = "content".into();
    envelope
        .media
        .push(tinychannels_bus::channel::MediaReference {
            url: Some("https://fixture.invalid/media".into()),
            kind: tinychannels_bus::channel::MediaKind::Image,
            ..Default::default()
        });
    let request = PrepareChannelInboundRequest {
        source: ChannelInboundSource::Envelope {
            envelope: envelope.clone(),
            timestamp: 123,
        },
        namespace: "tenant".into(),
        session_policy: Default::default(),
    };
    let prepared = service
        .prepare_channel_inbound(request.clone())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(prepared.envelope).unwrap(),
        serde_json::to_value(envelope).unwrap()
    );
    assert_eq!(prepared.message.timestamp, 123);
    assert_eq!(prepared.message.sender_name.as_deref(), Some("Display"));
    assert_eq!(
        prepared.session_key,
        "tenant:discord:account:group:room:thread"
    );
    let mut per_user = request;
    per_user.session_policy.thread_sessions_per_user = true;
    let prepared = service.prepare_channel_inbound(per_user).await.unwrap();
    assert_eq!(
        prepared.session_key,
        "tenant:discord:account:group:room:thread:user"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_preparation_refuses_mismatched_identity_oversized_payload_and_shutdown() {
    use tinychannels_bus::module_inbound::{
        ChannelInboundSource, PrepareChannelIdentityRequest, PrepareChannelInboundRequest,
    };
    let service = inbound_preparation_service().await;
    let message = tinychannels_bus::ChannelMessage {
        channel: "discord".into(),
        id: "msg".into(),
        sender: "user".into(),
        reply_target: "room".into(),
        ..Default::default()
    };
    let mut envelope = tinychannels::inbound_envelope_from_legacy_message(&message);
    envelope.sender.id = "other".into();
    let request = PrepareChannelInboundRequest {
        source: ChannelInboundSource::Legacy {
            message: message.clone(),
            envelope: Some(envelope),
        },
        namespace: "tenant".into(),
        session_policy: Default::default(),
    };
    let error = service
        .prepare_channel_inbound(request)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("identity does not match"));
    let mut huge = message;
    huge.content = "x".repeat(tinychannels_bus::module_runtime::PREPARATION_MESSAGE_BYTES);
    let request = PrepareChannelInboundRequest {
        source: ChannelInboundSource::Legacy {
            message: huge,
            envelope: None,
        },
        namespace: "tenant".into(),
        session_policy: Default::default(),
    };
    assert!(
        service
            .prepare_channel_inbound(request.clone())
            .await
            .is_err()
    );
    let identity = PrepareChannelIdentityRequest {
        channel: "discord".into(),
        sender: Some("x".repeat(4096)),
        reply_target: None,
        thread_ts: None,
    };
    assert!(
        service
            .prepare_channel_identity(identity.clone())
            .await
            .is_err()
    );
    service.shutdown().await.unwrap();
    assert!(service.prepare_channel_inbound(request).await.is_err());
    assert!(service.prepare_channel_identity(identity).await.is_err());
}

#[cfg(feature = "static-link")]
#[test]
fn exported_manifest_preserves_the_complete_contract_member_order() {
    let linked = exports::linked_module().unwrap();
    let provided = linked.manifest.provides.first().unwrap();
    let actual: Vec<_> = provided
        .methods
        .iter()
        .map(|method| method.as_str())
        .collect();
    assert_eq!(actual, tinychannels_bus::METHODS);
}

#[tokio::test]
async fn progressive_reply_is_opened_owned_and_closed_inside_the_module() {
    use tinychannels_bus::module_progressive::{ProgressiveCommand, ProgressiveResult};
    let service = inbound_preparation_service().await;
    let opened = service
        .execute_progressive(ProgressiveCommand::Open {
            channel: "telegram:fixture".into(),
        })
        .await
        .unwrap();
    let ProgressiveResult::Opened { handle } = opened else {
        panic!("missing progressive handle")
    };
    assert_eq!(
        service
            .execute_progressive(ProgressiveCommand::Close {
                handle: handle.clone()
            })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );
    assert_eq!(
        service
            .execute_progressive(ProgressiveCommand::Close { handle })
            .await
            .unwrap(),
        ProgressiveResult::Closed
    );
    service.shutdown().await.unwrap();
    assert!(
        service
            .execute_progressive(ProgressiveCommand::Open {
                channel: "telegram:fixture".into()
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn remote_help_is_parsed_and_rendered_inside_the_module() {
    use tinychannels_bus::module_remote::{RemoteOperation, RemoteRoute};
    let service = inbound_preparation_service().await;
    enable_remote_providers(&service).await;
    let message = serde_json::from_value(serde_json::json!({
        "id":"fixture", "sender":"sender", "reply_target":"chat",
        "content":"/HELP@FixtureBot", "channel":"tg:fixture", "timestamp":1
    }))
    .unwrap();
    let result = service
        .execute_remote(RemoteOperation::Execute {
            workspace_dir: ".".into(),
            message,
            route: RemoteRoute {
                provider: "fixture".into(),
                model: "fixture".into(),
            },
            history_len: 0,
        })
        .await
        .unwrap();
    assert_eq!(
        result.reply.as_deref(),
        Some(tinychannels::remote::build_remote_help_response().as_str())
    );
    let route = service
        .prepare_channel_control(
            tinychannels_bus::module_control::ChannelControlRequest::Route {
                workspace_dir: ".".into(),
                message: serde_json::from_value(serde_json::json!({
                    "id":"fixture", "sender":"sender", "reply_target":"chat",
                    "content":"/help", "channel":"telegram:fixture", "timestamp":1
                }))
                .unwrap(),
                route: RemoteRoute {
                    provider: "fixture".into(),
                    model: "fixture".into(),
                },
                history_len: 0,
                providers: vec![],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        route,
        tinychannels_bus::module_control::ChannelControlResult::Route {
            reply: Some(reply),
            ..
        } if reply == tinychannels::remote::build_remote_help_response()
    ));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_control_facts_preserve_module_timeout_floor_and_overflow_classification() {
    use tinychannels_bus::module_control::{
        ChannelControlRequest as Request, ChannelControlResult as Response,
    };
    let service = inbound_preparation_service().await;
    enable_remote_providers(&service).await;
    assert_eq!(
        service
            .prepare_channel_control(Request::RuntimeLimits {
                timeout_secs: 1,
                listener_count: 100
            })
            .await
            .unwrap(),
        Response::RuntimeLimits {
            timeout_secs: 30,
            max_in_flight: 64
        }
    );
    assert_eq!(
        service
            .prepare_channel_control(Request::Overflow {
                message: "maximum context length exceeded".into()
            })
            .await
            .unwrap(),
        Response::Overflow(true)
    );
    assert_eq!(
        service
            .prepare_channel_control(Request::Overflow {
                message: "ordinary failure".into()
            })
            .await
            .unwrap(),
        Response::Overflow(false)
    );
    for (mode, expected) in [
        (tinychannels_bus::ChannelAuthMode::BotToken, true),
        (tinychannels_bus::ChannelAuthMode::ManagedDm, false),
    ] {
        assert_eq!(
            service
                .prepare_channel_control(Request::Connected {
                    configured: vec!["telegram".into()],
                    channel: "telegram".into(),
                    mode
                })
                .await
                .unwrap(),
            Response::Connected(expected)
        );
    }
    assert_eq!(
        service
            .prepare_channel_control(Request::Capabilities {
                channel: "tg:fixture".into()
            })
            .await
            .unwrap(),
        Response::Capabilities(tinychannels_bus::LegacyChannelCapabilities {
            remote_control: true,
            ..Default::default()
        })
    );
    let Response::CapabilitiesV2(capabilities) = service
        .prepare_channel_control(Request::CapabilitiesV2 {
            channel: "tg:fixture".into(),
        })
        .await
        .unwrap()
    else {
        panic!("expected v2 capabilities")
    };
    assert!(capabilities.remote_control);
    assert_eq!(capabilities.account.configured, Some(true));
    service.shutdown().await.unwrap();
    assert!(
        service
            .prepare_channel_control(Request::Approval {
                tool_name: "shell".into(),
                action_summary: "echo fixture".into()
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn route_preparation_preserves_aliases_history_resets_and_ordinary_messages() {
    use tinychannels_bus::module_control::{
        ChannelControlRequest as Request, ChannelControlResult as Response, ProviderDescriptor,
    };
    use tinychannels_bus::module_remote::RemoteRoute;
    let service = inbound_preparation_service().await;
    enable_remote_providers(&service).await;
    let request = |content: &str, channel: &str| {
        Request::Route {
        workspace_dir: ".".into(),
        message: serde_json::from_value(serde_json::json!({"id":"fixture","sender":"sender","reply_target":"chat","content":content,"channel":channel,"timestamp":1})).unwrap(),
        route: RemoteRoute { provider: "old".into(), model: "model".into() }, history_len: 3,
        providers: vec![ProviderDescriptor { name: "approved".into(), aliases: vec!["alias".into()] }],
    }
    };
    assert_eq!(service.prepare_channel_control(request("/models ALIAS", "telegram")).await.unwrap(), Response::Route {
        reply: Some("Provider switched to `approved` for this sender session. Current model is `model`.\nUse `/model <model-id>` to set a provider-compatible model.".into()),
        next_route: Some(RemoteRoute { provider: "approved".into(), model: "model".into() }), clear_history: true,
    });
    assert_eq!(
        service
            .prepare_channel_control(request("/model `next`", "discord"))
            .await
            .unwrap(),
        Response::Route {
            reply: Some(
                "Model switched to `next` for provider `old` in this sender session.".into()
            ),
            next_route: Some(RemoteRoute {
                provider: "old".into(),
                model: "next".into()
            }),
            clear_history: true,
        }
    );
    for (content, channel) in [("ordinary message", "telegram"), ("/model next", "email")] {
        assert_eq!(
            service
                .prepare_channel_control(request(content, channel))
                .await
                .unwrap(),
            Response::Route {
                reply: None,
                next_route: None,
                clear_history: false
            }
        );
    }
    let Response::Route {
        reply: Some(help),
        next_route: None,
        clear_history: false,
    } = service
        .prepare_channel_control(request("/HELP@FixtureBot", "telegram"))
        .await
        .unwrap()
    else {
        panic!("missing remote help")
    };
    assert_eq!(help, tinychannels::remote::build_remote_help_response());
    assert_eq!(
        service
            .prepare_channel_control(Request::Approval {
                tool_name: "shell".into(),
                action_summary: "echo fixture".into()
            })
            .await
            .unwrap(),
        Response::Approval(tinychannels::approvals::format_approval_prompt(
            "shell",
            "echo fixture"
        ))
    );
    assert_eq!(
        service
            .prepare_channel_control(Request::Acknowledgment {
                content: "hello fixture".into()
            })
            .await
            .unwrap(),
        Response::Acknowledgment(
            tinychannels::runtime::select_acknowledgment_reaction("hello fixture").to_owned()
        )
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn webhook_provider_requires_outbound_start_and_rejects_duplicate_ownership() {
    let service = inbound_preparation_service().await;
    let config = ChannelsConfig {
        linq: Some(tinychannels_bus::config::LinqConfig {
            api_token: "fixture-token".into(),
            from_phone: "+15551234567".into(),
            allowed_senders: vec![],
        }),
        ..Default::default()
    };
    let error = service
        .start_channel("linq".into(), config.clone())
        .await
        .map(|_| ())
        .expect_err("request must fail");
    assert!(matches!(error, BusError::MethodFailed { name, .. } if name == NO_INGRESS_ERROR));
    assert!(service.list_channels().await.unwrap().is_empty());
    service
        .start_outbound_channel("linq".into(), config.clone(), Default::default())
        .await
        .unwrap();
    assert_eq!(service.list_channels().await.unwrap(), ["linq"]);
    let error = service
        .start_outbound_channel("linq".into(), config.clone(), Default::default())
        .await
        .map(|_| ())
        .expect_err("request must fail");
    assert!(matches!(error, BusError::MethodFailed { name, .. } if name == ALREADY_RUNNING_ERROR));
    service.stop_channel("linq".into()).await.unwrap();
    assert_eq!(
        service.channel_status("linq".into()).await.unwrap(),
        "stopped"
    );
    service
        .start_outbound_channel("linq".into(), config.clone(), Default::default())
        .await
        .unwrap();
    service.shutdown().await.unwrap();
    assert!(
        service
            .start_outbound_channel("linq".into(), config, Default::default())
            .await
            .map(|_| ())
            .expect_err("request must fail")
            .to_string()
            .contains("shutting down")
    );
}

#[tokio::test]
async fn channel_start_variants_refuse_missing_configuration_and_excessive_backoff() {
    let service = inbound_preparation_service().await;
    let config = ChannelsConfig::default();
    let error = service
        .start_channel_configured("unknown".into(), config.clone(), Default::default())
        .await
        .map(|_| ())
        .expect_err("request must fail");
    assert!(matches!(error, BusError::MethodFailed { name, .. } if name == NOT_CONFIGURED_ERROR));
    let error = service
        .start_channel_supervised(
            "unknown".into(),
            config.clone(),
            Default::default(),
            Default::default(),
        )
        .await
        .map(|_| ())
        .expect_err("request must fail");
    assert!(matches!(error, BusError::MethodFailed { name, .. } if name == NOT_CONFIGURED_ERROR));
    for policy in [
        tinychannels_bus::ChannelSupervisionPolicy {
            initial_backoff_secs: 86_401,
            max_backoff_secs: 1,
        },
        tinychannels_bus::ChannelSupervisionPolicy {
            initial_backoff_secs: 1,
            max_backoff_secs: 86_401,
        },
    ] {
        assert!(
            service
                .start_channel_supervised(
                    "unknown".into(),
                    config.clone(),
                    Default::default(),
                    policy
                )
                .await
                .map(|_| ())
                .expect_err("request must fail")
                .to_string()
                .contains("exceeds one day")
        );
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn service_media_upload_binds_activation_and_scoped_storage_lifecycle() {
    use base64::Engine as _;
    use tinychannels_bus::{
        ChannelRef, ConversationKind, ConversationRef, HostVoiceScope, MediaKind, MediaReadBegin,
        MediaReadChunk, MediaReadFinish, MediaUploadBegin, MediaUploadChunk, MediaUploadFinish,
    };

    let (service, _callbacks, _server) =
        channels_with_store(Arc::new(CaptureChannel(tokio::sync::mpsc::channel(1).0))).await;
    service.provider_activations.lock().await.insert(
        "fixture".into(),
        super::ProviderActivation {
            generation: "activation-a".into(),
            media_account_scope: crate::provider::MediaScope::fixed(Some(
                "fixture:media-v1:test".into(),
            )),
        },
    );
    let bytes = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/f9sAAAAASUVORK5CYII=")
        .unwrap();
    let begin = MediaUploadBegin {
        instance: "fixture".into(),
        scope: HostVoiceScope {
            configured_instance: "forged-instance".into(),
            channel: ChannelRef {
                id: "forged-channel".into(),
                account_id: Some("forged-account".into()),
            },
            conversation: ConversationRef {
                kind: ConversationKind::Thread,
                id: "conversation".into(),
                scope_id: Some("scope".into()),
                parent_id: Some("parent".into()),
                thread_id: Some("thread".into()),
                topic_id: Some("topic".into()),
            },
            turn_id: "turn".into(),
        },
        kind: MediaKind::Image,
        content_type: "image/png".into(),
        file_name: Some("image.png".into()),
        duration_ms: None,
        codec: None,
        byte_count: bytes.len(),
    };
    assert!(
        service
            .begin_media_upload(MediaUploadBegin {
                instance: "missing".into(),
                ..begin.clone()
            })
            .await
            .is_err()
    );
    let handle = service.begin_media_upload(begin.clone()).await.unwrap();
    assert_eq!(
        service
            .append_media_upload_chunk(MediaUploadChunk {
                handle: handle.clone(),
                sequence: 0,
                bytes: bytes.clone(),
            })
            .await
            .unwrap(),
        1
    );
    let finish = MediaUploadFinish(handle);
    let result = service.finish_media_upload(finish.clone()).await.unwrap();
    assert!(result.opaque_id.is_some());
    assert_eq!(result.content_type.as_deref(), Some("image/png"));
    assert_eq!(
        service.finish_media_upload(finish.clone()).await.unwrap(),
        result
    );
    assert!(service.abort_media_upload(finish.clone()).await.is_err());

    let mut read_scope = begin.scope.clone();
    read_scope.configured_instance = "fixture".into();
    read_scope.channel.id = "fixture".into();
    read_scope.channel.account_id = Some("fixture:media-v1:test".into());
    let read_request = MediaReadBegin {
        instance: "fixture".into(),
        scope: read_scope.clone(),
        media: result.clone(),
        max_bytes: bytes_len(&result),
    };
    assert!(
        service
            .begin_media_read(MediaReadBegin {
                instance: "missing".into(),
                ..read_request.clone()
            })
            .await
            .expect_err("inactive provider cannot resolve media")
            .to_string()
            .contains("not running")
    );
    service.provider_activations.lock().await.insert(
        "fixture-no-identity".into(),
        super::ProviderActivation {
            generation: "activation-no-identity".into(),
            media_account_scope: crate::provider::MediaScope::default(),
        },
    );
    let running_fixture = service.running.lock().await["fixture"].clone();
    service
        .running
        .lock()
        .await
        .insert("fixture-no-identity".into(), running_fixture);
    assert!(
        service
            .begin_media_read(MediaReadBegin {
                instance: "fixture-no-identity".into(),
                ..read_request.clone()
            })
            .await
            .expect_err("provider without stable identity fails closed")
            .to_string()
            .contains("no stable media identity")
    );
    assert!(
        service
            .begin_media_read(MediaReadBegin {
                max_bytes: 0,
                ..read_request.clone()
            })
            .await
            .expect_err("zero-byte bound is invalid")
            .to_string()
            .contains("request is invalid")
    );
    let read = service
        .begin_media_read(read_request.clone())
        .await
        .unwrap();
    assert_eq!(read.byte_count, bytes_len(&result) as usize);
    assert_eq!(read.media.opaque_id, result.opaque_id);
    assert_eq!(read.content_type, "image/png");
    assert_eq!(read.file_name.as_deref(), Some("image.png"));
    let first = service
        .read_media_chunk(MediaReadChunk {
            handle: read.handle.clone(),
            sequence: 0,
        })
        .await
        .unwrap();
    assert!(first.done);
    assert_eq!(first.bytes, bytes);
    assert_eq!(
        service
            .read_media_chunk(MediaReadChunk {
                handle: read.handle.clone(),
                sequence: 0,
            })
            .await
            .unwrap(),
        first
    );
    let aborted_handle = read.handle.clone();
    service
        .abort_media_read(MediaReadFinish(read.handle))
        .await
        .unwrap();
    assert!(service.media_reads.binding(&aborted_handle).await.is_err());
    assert!(
        service
            .read_media_chunk(MediaReadChunk {
                handle: aborted_handle,
                sequence: 0,
            })
            .await
            .is_err()
    );

    let replaced_read = service
        .begin_media_read(MediaReadBegin {
            instance: "fixture".into(),
            scope: read_scope.clone(),
            media: result.clone(),
            max_bytes: bytes_len(&result),
        })
        .await
        .unwrap();

    service.provider_activations.lock().await.insert(
        "fixture".into(),
        super::ProviderActivation {
            generation: "activation-b".into(),
            media_account_scope: crate::provider::MediaScope::fixed(Some(
                "fixture:media-v1:test".into(),
            )),
        },
    );
    assert!(
        service
            .read_media_chunk(MediaReadChunk {
                handle: replaced_read.handle.clone(),
                sequence: 0,
            })
            .await
            .is_err()
    );
    assert!(
        service
            .abort_media_read(MediaReadFinish(replaced_read.handle.clone()))
            .await
            .is_err()
    );
    assert!(service.finish_media_upload(finish).await.is_err());
    let terminal_read = service
        .begin_media_read(MediaReadBegin {
            instance: "fixture".into(),
            scope: read_scope,
            media: result,
            max_bytes: 1024,
        })
        .await
        .unwrap();
    service.terminal.store(true, Ordering::SeqCst);
    assert!(
        service
            .begin_media_upload(MediaUploadBegin {
                instance: "fixture".into(),
                ..MediaUploadBegin {
                    byte_count: 1,
                    ..request_media_fixture()
                }
            })
            .await
            .is_err()
    );
    assert!(
        service
            .begin_media_read(read_request)
            .await
            .expect_err("terminal module refuses begin")
            .to_string()
            .contains("shutting down")
    );
    assert!(
        service
            .read_media_chunk(MediaReadChunk {
                handle: terminal_read.handle.clone(),
                sequence: 0,
            })
            .await
            .expect_err("terminal module refuses chunks")
            .to_string()
            .contains("shutting down")
    );
    assert!(
        service
            .abort_media_read(MediaReadFinish(terminal_read.handle))
            .await
            .expect_err("terminal module refuses abort")
            .to_string()
            .contains("shutting down")
    );
    service.shutdown().await.unwrap();
}

fn bytes_len(reference: &tinychannels_bus::MediaReference) -> u64 {
    reference
        .size_bytes
        .expect("stored media includes byte count")
}

fn request_media_fixture() -> tinychannels_bus::MediaUploadBegin {
    tinychannels_bus::MediaUploadBegin {
        instance: "fixture".into(),
        scope: tinychannels_bus::HostVoiceScope {
            configured_instance: "fixture".into(),
            channel: tinychannels_bus::ChannelRef {
                id: "fixture".into(),
                account_id: None,
            },
            conversation: tinychannels_bus::ConversationRef {
                id: "conversation".into(),
                ..Default::default()
            },
            turn_id: "turn".into(),
        },
        kind: tinychannels_bus::MediaKind::Unknown,
        content_type: "application/octet-stream".into(),
        file_name: None,
        duration_ms: None,
        codec: None,
        byte_count: 1,
    }
}

#[tokio::test]
async fn identity_scoped_services_refuse_nested_runtime_management() {
    let service = inbound_preparation_service().await;
    assert!(
        service
            .reserve_runtime()
            .await
            .map(|_| ())
            .expect_err("request must fail")
            .to_string()
            .contains("unavailable")
    );
    assert!(
        service
            .start_runtime("handle".into())
            .await
            .map(|_| ())
            .expect_err("request must fail")
            .to_string()
            .contains("unavailable")
    );
    assert!(
        service
            .start_runtime_with_dispatch(
                "handle".into(),
                tinychannels_bus::ChannelRuntimeDispatchPolicy { listener_count: 1 }
            )
            .await
            .map(|_| ())
            .expect_err("request must fail")
            .to_string()
            .contains("unavailable")
    );
    assert!(
        service
            .stop_runtime("handle".into())
            .await
            .map(|_| ())
            .expect_err("request must fail")
            .to_string()
            .contains("unavailable")
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn health_and_controller_operations_refuse_requests_after_shutdown() {
    let service = inbound_preparation_service().await;
    assert!(
        service
            .check_channel_health(Default::default(), Default::default(), 100)
            .await
            .unwrap()
            .is_empty()
    );
    let result = service
        .channel_controller(tinychannels_bus::ChannelControllerRequest::Status {
            config: Default::default(),
            channel: None,
        })
        .await;
    assert!(result.is_err(), "no host backend is served");
    service.shutdown().await.unwrap();
    assert!(
        service
            .check_channel_health(Default::default(), Default::default(), 100)
            .await
            .map(|_| ())
            .expect_err("request must fail")
            .to_string()
            .contains("shutting down")
    );
    assert!(
        service
            .channel_controller(tinychannels_bus::ChannelControllerRequest::Status {
                config: Default::default(),
                channel: None,
            })
            .await
            .map(|_| ())
            .expect_err("request must fail")
            .to_string()
            .contains("shutting down")
    );
}

#[tokio::test]
async fn legacy_send_and_interaction_route_to_live_provider_and_reject_unknown_channels() {
    let (sent, mut received) = tokio::sync::mpsc::channel(2);
    let (service, _callbacks, _server) = channels_with_store(Arc::new(CaptureChannel(sent))).await;
    service
        .send_message("fixture".into(), SendMessage::new("legacy", "room"))
        .await
        .unwrap();
    assert_eq!(received.recv().await.unwrap().content, "legacy");
    let result = service
        .channel_interaction(tinychannels_bus::ChannelInteractionRequest {
            channel: "fixture".into(),
            interaction: tinychannels_bus::ChannelInteraction::Describe,
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        tinychannels_bus::ChannelInteractionResult::Info(_)
    ));
    assert_eq!(
        service.channel_status("fixture".into()).await.unwrap(),
        "connected"
    );
    assert!(
        service
            .send_message("missing".into(), SendMessage::new("hello", "room"))
            .await
            .is_err()
    );
    assert!(
        service
            .channel_interaction(tinychannels_bus::ChannelInteractionRequest {
                channel: "missing".into(),
                interaction: tinychannels_bus::ChannelInteraction::Describe,
            })
            .await
            .is_err()
    );
    let mut intent = required_intent("best-effort", "hello");
    intent.durability = tinychannels::DeliveryDurability::BestEffort;
    assert!(
        service
            .send_channel_intent("wrong".into(), intent.clone())
            .await
            .is_err()
    );
    intent.channel_id = "missing".into();
    assert!(
        service
            .send_channel_intent("missing".into(), intent.clone())
            .await
            .is_err()
    );
    intent.channel_id = "fixture".into();
    intent.payload = tinychannels::OutboundPayload::Text {
        text: "x".repeat(tinychannels_bus::module_relay::RELAY_FRAME_BYTES),
    };
    assert!(
        service
            .send_channel_intent("fixture".into(), intent)
            .await
            .is_err()
    );
    service.shutdown().await.unwrap();
    assert!(
        service
            .send_message("fixture".into(), SendMessage::new("hello", "room"))
            .await
            .is_err()
    );
    assert!(
        service
            .channel_interaction(tinychannels_bus::ChannelInteractionRequest {
                channel: "fixture".into(),
                interaction: tinychannels_bus::ChannelInteraction::Describe,
            })
            .await
            .is_err()
    );
    assert!(
        service
            .send_channel_intent("fixture".into(), required_intent("closed", "hello"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn service_send_failures_preserve_named_error_and_durable_queue_busy_refuses_admission() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (service, _callbacks, _server) =
        channels_with_store(Arc::new(FailingChannel(calls.clone()))).await;
    for error in [
        service
            .send_message("fixture".into(), SendMessage::new("hello", "room"))
            .await
            .expect_err("provider fails"),
        service
            .send_channel_intent(
                "fixture".into(),
                ChannelOutboundIntent {
                    turn_id: None,
                    durability: tinychannels::DeliveryDurability::BestEffort,
                    ..required_intent("best-effort", "hello")
                },
            )
            .await
            .map(|_| ())
            .expect_err("provider fails"),
    ] {
        assert!(matches!(error, BusError::MethodFailed { name, .. } if name == SEND_FAILED_ERROR));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let admission = service.delivery_admission.lock().await;
    let error = service
        .send_channel_intent("fixture".into(), required_intent("busy", "hello"))
        .await
        .map(|_| ())
        .expect_err("queue busy");
    assert!(error.to_string().contains("queue is busy"));
    drop(admission);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn service_email_upload_routes_chunks_abort_and_validation_and_freezes_on_shutdown() {
    let service = inbound_preparation_service().await;
    let begin = EmailUploadBegin {
        config: tinychannels_bus::config::EmailConfig {
            from_address: "from@example.test".into(),
            ..Default::default()
        },
        recipient: "to@example.test".into(),
        subject: "test".into(),
        body: "body".into(),
        attachments: vec![tinychannels_bus::EmailAttachmentUpload {
            filename: "file.txt".into(),
            content_type: "text/plain".into(),
            byte_count: 3,
        }],
    };
    let request = EmailSendRequest {
        config: begin.config.clone(),
        recipient: "invalid mailbox".into(),
        subject: begin.subject.clone(),
        body: begin.body.clone(),
        attachments: vec![],
    };
    assert!(service.send_email(request.clone()).await.is_err());
    let handle = service.begin_email_upload(begin.clone()).await.unwrap();
    let chunk = EmailUploadChunk {
        handle: handle.clone(),
        attachment_index: 0,
        sequence: 0,
        bytes: b"abc".to_vec(),
    };
    assert_eq!(
        service
            .append_email_upload_chunk(chunk.clone())
            .await
            .unwrap(),
        1
    );
    service.abort_email_upload(handle.clone()).await.unwrap();
    assert!(service.send_email_upload(handle.clone()).await.is_err());
    service.shutdown().await.unwrap();
    assert!(service.send_email(request).await.is_err());
    assert!(service.begin_email_upload(begin).await.is_err());
    assert!(service.append_email_upload_chunk(chunk).await.is_err());
    assert!(service.send_email_upload(handle).await.is_err());
}

#[tokio::test]
async fn service_relay_reservations_are_single_use_and_shutdown_prevents_new_owners() {
    let service = inbound_preparation_service().await;
    let id = service.reserve_relay().await.unwrap();
    assert_eq!(
        service.relay_status(id.clone()).await.unwrap().state,
        tinychannels_bus::module_relay::RelayPhase::Reserved
    );
    assert!(service.reserve_relay().await.is_err());
    assert!(
        service
            .send_relay_intent(id.clone(), required_intent("relay", "hello"))
            .await
            .is_err()
    );
    service.stop_relay(id.clone()).await.unwrap();
    assert!(service.relay_status(id.clone()).await.is_err());
    assert!(service.start_relay(id, Default::default()).await.is_err());
    service.shutdown().await.unwrap();
    assert!(service.reserve_relay().await.is_err());
}

#[tokio::test]
async fn service_runtime_wrappers_activate_stop_and_freeze_identity_owners() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = Channels::new(
        HostChannels::new(connection.clone()),
        Some(Arc::new(crate::runtime::Runtimes::new(connection))),
    );
    let legacy = service.reserve_runtime().await.unwrap();
    service.start_runtime(legacy.handle.clone()).await.unwrap();
    service.stop_runtime(legacy.handle).await.unwrap();
    let dispatch = service.reserve_runtime().await.unwrap();
    service
        .start_runtime_with_dispatch(
            dispatch.handle.clone(),
            tinychannels_bus::ChannelRuntimeDispatchPolicy { listener_count: 2 },
        )
        .await
        .unwrap();
    service.stop_runtime(dispatch.handle).await.unwrap();
    service.shutdown().await.unwrap();
    assert!(service.reserve_runtime().await.is_err());
}

#[tokio::test]
async fn shutdown_refuses_preparation_and_execution_before_host_work() {
    let service = inbound_preparation_service().await;
    service.shutdown().await.unwrap();
    assert!(
        service
            .describe_configured_channels(Default::default(), Default::default())
            .await
            .is_err()
    );
    assert!(
        service
            .prepare_channel_control(
                tinychannels_bus::module_control::ChannelControlRequest::Acknowledgment {
                    content: "hello".into()
                }
            )
            .await
            .is_err()
    );
    assert!(
        service
            .execute_remote(tinychannels_bus::module_remote::RemoteOperation::MarkTurn {
                workspace_dir: ".".into(),
                channel: "fixture".into(),
                reply_target: "room".into(),
                busy: true,
            })
            .await
            .is_err()
    );
    assert!(
        service
            .execute_progressive(
                tinychannels_bus::module_progressive::ProgressiveCommand::Open {
                    channel: "fixture".into(),
                }
            )
            .await
            .is_err()
    );
    assert!(
        service
            .prepare_channel_identity(
                tinychannels_bus::module_inbound::PrepareChannelIdentityRequest {
                    channel: "fixture".into(),
                    sender: None,
                    reply_target: None,
                    thread_ts: None,
                }
            )
            .await
            .is_err()
    );
    assert!(
        service
            .prepare_outbound_intent(
                "fixture".into(),
                tinychannels_bus::module_runtime::ChannelMessagePreparation::Legacy(
                    serde_json::json!({"content": "hello", "recipient": "room"})
                )
            )
            .await
            .is_err()
    );
}

#[test]
fn only_providers_without_any_module_ingress_are_refused_a_normal_start() {
    // WhatsApp Cloud is webhook-backed, but the module authenticates and
    // ingests the raw POSTs the host forwards, so it is not refused.
    assert!(super::lacks_module_ingress("linq"));
    for name in ["whatsapp", "telegram", "discord"] {
        assert!(!super::lacks_module_ingress(name), "{name}");
    }
}

#[tokio::test]
async fn whatsapp_cloud_starts_normally_and_serves_webhook_ingress_over_the_bus() {
    let service = inbound_preparation_service().await;
    let config = ChannelsConfig {
        whatsapp: Some(tinychannels_bus::config::WhatsAppConfig {
            access_token: Some("access-token".into()),
            phone_number_id: Some("123".into()),
            verify_token: Some("verify-me".into()),
            app_secret: Some("secret".into()),
            session_path: None,
            pair_phone: None,
            pair_code: None,
            allowed_numbers: vec!["*".into()],
        }),
        ..Default::default()
    };
    service
        .start_channel("whatsapp".into(), config.clone())
        .await
        .expect("the module serves the Cloud webhook ingress");
    // The subscription handshake is answered through the interaction method.
    let challenge = service
        .channel_interaction(tinychannels_bus::ChannelInteractionRequest {
            channel: "whatsapp".into(),
            interaction: tinychannels_bus::ChannelInteraction::VerifyWebhook {
                mode: "subscribe".into(),
                verify_token: "verify-me".into(),
                challenge: "challenge-1".into(),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        challenge,
        tinychannels_bus::ChannelInteractionResult::WebhookChallenge(value) if value == "challenge-1"
    ));
    // An unsigned delivery is refused with the client-error name.
    let refused = service
        .channel_interaction(tinychannels_bus::ChannelInteractionRequest {
            channel: "whatsapp".into(),
            interaction: tinychannels_bus::ChannelInteraction::DeliverWebhook {
                signature: None,
                body_base64: "e30=".into(),
            },
        })
        .await
        .map(|_| ())
        .expect_err("an unsigned delivery must be refused");
    assert!(matches!(
        refused,
        BusError::MethodFailed { name, .. } if name == crate::interaction::WEBHOOK_REJECTED_ERROR
    ));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn whatsapp_selection_follows_the_stanza_shape_and_web_binds_its_media_authority() {
    use tinychannels::host::ChannelHost;
    let host: Arc<dyn ChannelHost> = tinychannels::NoopHost::arc();
    let select = |name: &str, config: &ChannelsConfig| {
        super::WhatsAppNative::select(name, config, &host, &tinychannels::DefaultHttpClients)
    };
    let cloud_config = ChannelsConfig {
        whatsapp: Some(tinychannels_bus::config::WhatsAppConfig {
            access_token: Some("token".into()),
            phone_number_id: Some("123".into()),
            verify_token: Some("verify".into()),
            app_secret: None,
            session_path: None,
            pair_phone: None,
            pair_code: None,
            allowed_numbers: Vec::new(),
        }),
        ..Default::default()
    };
    let web_config = whatsapp(false);
    assert!(select("telegram", &cloud_config).is_none());
    assert!(select("whatsapp", &ChannelsConfig::default()).is_none());
    let unusable = ChannelsConfig {
        whatsapp: Some(tinychannels_bus::config::WhatsAppConfig {
            access_token: None,
            phone_number_id: None,
            verify_token: None,
            app_secret: None,
            session_path: None,
            pair_phone: None,
            pair_code: None,
            allowed_numbers: Vec::new(),
        }),
        ..Default::default()
    };
    assert!(select("whatsapp", &unusable).is_none());

    let cloud =
        select("whatsapp", &cloud_config).expect("a Cloud stanza selects the Cloud adapter");
    let web = select("whatsapp", &web_config).expect("a session stanza selects the Web adapter");
    assert!(matches!(cloud, super::WhatsAppNative::Cloud(_)));
    assert_eq!(cloud.channel().name(), "whatsapp");
    assert_eq!(web.channel().name(), "whatsapp");

    // Only Web learns its media authority from the session; before the device
    // is linked that authority is unknown, and Cloud's is its configuration.
    let scope = crate::provider::MediaScope::fixed(None);
    cloud.bind_media_authority(&scope);
    assert_eq!(scope.current(), None);
    web.bind_media_authority(&scope);
    assert_eq!(
        scope.current(),
        None,
        "an unlinked device has no account yet"
    );

    // An outbound-only start is network-free and still yields the native adapter.
    let service = inbound_preparation_service().await;
    let running = web.start(Arc::new(Hooks::default()), service.host.clone(), None, true);
    assert_eq!(
        running
            .adapter_handle()
            .descriptor()
            .metadata
            .get("adapterMode")
            .map(String::as_str),
        Some("native-v2")
    );
    running.stop().await.unwrap();
}
