use super::*;

type ProviderMediaScopeCase = (
    &'static str,
    tinychannels_bus::ChannelsConfig,
    fn(&mut tinychannels_bus::ChannelsConfig),
);

#[test]
fn media_account_scope_hashes_credentials_but_ignores_mutable_policy() {
    let mut config = tinychannels_bus::ChannelsConfig {
        telegram: Some(tinychannels_bus::config::TelegramConfig {
            bot_token: "123:secret-one".into(),
            chat_id: Some("room-a".into()),
            allowed_users: vec!["one".into()],
            stream_mode: Default::default(),
            draft_update_interval_ms: 1000,
            silent_streaming: true,
            mention_only: false,
        }),
        ..Default::default()
    };
    let original = configured_media_account_scope("telegram", &config).unwrap();

    config.telegram.as_mut().unwrap().allowed_users = vec!["two".into()];
    config.telegram.as_mut().unwrap().chat_id = Some("room-b".into());
    assert_eq!(
        configured_media_account_scope("telegram", &config).as_deref(),
        Some(original.as_str())
    );

    config.telegram.as_mut().unwrap().bot_token = "123:secret-two".into();
    assert_ne!(
        configured_media_account_scope("telegram", &config).as_deref(),
        Some(original.as_str()),
        "a matching unverified token prefix cannot cross configured accounts"
    );
}

#[test]
fn configured_media_scope_requires_each_supported_providers_authentication_material() {
    use tinychannels_bus::config::*;

    let cases: Vec<ProviderMediaScopeCase> = vec![
        (
            "telegram",
            tinychannels_bus::ChannelsConfig {
                telegram: Some(TelegramConfig {
                    bot_token: "bot:secret".into(),
                    chat_id: None,
                    allowed_users: vec![],
                    stream_mode: Default::default(),
                    draft_update_interval_ms: 1000,
                    silent_streaming: true,
                    mention_only: false,
                }),
                ..Default::default()
            },
            |c| c.telegram.as_mut().unwrap().bot_token.clear(),
        ),
        (
            "discord",
            tinychannels_bus::ChannelsConfig {
                discord: Some(DiscordConfig {
                    bot_token: "secret".into(),
                    guild_id: None,
                    channel_id: None,
                    allowed_users: vec![],
                    listen_to_bots: false,
                    mention_only: false,
                }),
                ..Default::default()
            },
            |c| c.discord.as_mut().unwrap().bot_token.clear(),
        ),
        (
            "slack",
            tinychannels_bus::ChannelsConfig {
                slack: Some(SlackConfig {
                    bot_token: "bot".into(),
                    app_token: None,
                    channel_id: None,
                    allowed_users: vec![],
                }),
                ..Default::default()
            },
            |c| c.slack.as_mut().unwrap().bot_token.clear(),
        ),
        (
            "mattermost",
            tinychannels_bus::ChannelsConfig {
                mattermost: Some(MattermostConfig {
                    url: "https://chat.example".into(),
                    bot_token: "secret".into(),
                    channel_id: None,
                    allowed_users: vec![],
                    thread_replies: None,
                    mention_only: None,
                }),
                ..Default::default()
            },
            |c| c.mattermost.as_mut().unwrap().bot_token.clear(),
        ),
        (
            "webhook",
            tinychannels_bus::ChannelsConfig {
                webhook: Some(WebhookConfig {
                    port: 8080,
                    secret: Some("secret".into()),
                }),
                ..Default::default()
            },
            |c| c.webhook.as_mut().unwrap().secret = None,
        ),
        (
            "matrix",
            tinychannels_bus::ChannelsConfig {
                matrix: Some(MatrixConfig {
                    homeserver: "https://matrix.example".into(),
                    access_token: "secret".into(),
                    user_id: Some("@bot:example".into()),
                    device_id: None,
                    room_id: "!room:example".into(),
                    allowed_users: vec![],
                }),
                ..Default::default()
            },
            |c| c.matrix.as_mut().unwrap().access_token.clear(),
        ),
        (
            "email",
            tinychannels_bus::ChannelsConfig {
                email: Some(EmailConfig {
                    imap_host: "imap.example".into(),
                    imap_port: 993,
                    imap_folder: "INBOX".into(),
                    smtp_host: "smtp.example".into(),
                    smtp_port: 465,
                    smtp_tls: true,
                    username: "user".into(),
                    password: "secret".into(),
                    from_address: "user@example".into(),
                    idle_timeout_secs: 300,
                    allowed_senders: vec![],
                }),
                ..Default::default()
            },
            |c| c.email.as_mut().unwrap().password.clear(),
        ),
        (
            "whatsapp",
            tinychannels_bus::ChannelsConfig {
                whatsapp: Some(WhatsAppConfig {
                    access_token: Some("secret".into()),
                    phone_number_id: Some("phone-1".into()),
                    verify_token: None,
                    app_secret: Some("app".into()),
                    session_path: None,
                    pair_phone: None,
                    pair_code: None,
                    allowed_numbers: vec![],
                }),
                ..Default::default()
            },
            |c| c.whatsapp.as_mut().unwrap().access_token = None,
        ),
        (
            "linq",
            tinychannels_bus::ChannelsConfig {
                linq: Some(LinqConfig {
                    api_token: "secret".into(),
                    from_phone: "+15550001".into(),
                    allowed_senders: vec![],
                }),
                ..Default::default()
            },
            |c| c.linq.as_mut().unwrap().api_token.clear(),
        ),
        (
            "irc",
            tinychannels_bus::ChannelsConfig {
                irc: Some(IrcConfig {
                    server: "irc.example".into(),
                    port: 6697,
                    nickname: "bot".into(),
                    username: Some("auth-user".into()),
                    channels: vec![],
                    allowed_users: vec![],
                    server_password: None,
                    nickserv_password: None,
                    sasl_password: Some("secret".into()),
                    verify_tls: Some(true),
                }),
                ..Default::default()
            },
            |c| c.irc.as_mut().unwrap().sasl_password = None,
        ),
        (
            "lark",
            tinychannels_bus::ChannelsConfig {
                lark: Some(LarkConfig {
                    app_id: "app".into(),
                    app_secret: "secret".into(),
                    encrypt_key: None,
                    verification_token: None,
                    allowed_users: vec![],
                    use_feishu: false,
                    receive_mode: Default::default(),
                    port: None,
                }),
                ..Default::default()
            },
            |c| c.lark.as_mut().unwrap().app_secret.clear(),
        ),
        (
            "dingtalk",
            tinychannels_bus::ChannelsConfig {
                dingtalk: Some(DingTalkConfig {
                    client_id: "client".into(),
                    client_secret: "secret".into(),
                    allowed_users: vec![],
                }),
                ..Default::default()
            },
            |c| c.dingtalk.as_mut().unwrap().client_secret.clear(),
        ),
        (
            "qq",
            tinychannels_bus::ChannelsConfig {
                qq: Some(QQConfig {
                    app_id: "app".into(),
                    app_secret: "secret".into(),
                    allowed_users: vec![],
                }),
                ..Default::default()
            },
            |c| c.qq.as_mut().unwrap().app_secret.clear(),
        ),
        (
            "yuanbao",
            tinychannels_bus::ChannelsConfig {
                yuanbao: Some(YuanbaoConfig {
                    app_key: "key".into(),
                    app_secret: "secret".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            |c| c.yuanbao.as_mut().unwrap().app_secret.clear(),
        ),
    ];
    for (provider, config, clear_auth) in cases {
        assert!(
            configured_media_account_scope(provider, &config).is_some(),
            "{provider} has the auth material required for media scope"
        );
        let mut unauthenticated = config;
        clear_auth(&mut unauthenticated);
        assert!(
            configured_media_account_scope(provider, &unauthenticated).is_none(),
            "{provider} cannot fall back to a public endpoint or display field"
        );
    }

    // Signal has no secret: the daemon and the account it serves are the
    // identity, so each pair gets its own scope and a blank one gets none.
    let signal = |url: &str, account: &str| tinychannels_bus::ChannelsConfig {
        signal: Some(SignalConfig {
            http_url: url.into(),
            account: account.into(),
            group_id: None,
            allowed_from: vec![],
            ignore_attachments: false,
            ignore_stories: false,
        }),
        ..Default::default()
    };
    let first =
        configured_media_account_scope("signal", &signal("http://signal.example", "+15550001"))
            .unwrap();
    assert_eq!(
        configured_media_account_scope("signal", &signal("http://signal.example", "+15550001"))
            .as_deref(),
        Some(first.as_str())
    );
    for other in [
        signal("http://signal.example", "+15550002"),
        signal("http://other.example", "+15550001"),
    ] {
        assert_ne!(
            configured_media_account_scope("signal", &other).as_deref(),
            Some(first.as_str())
        );
    }
    assert!(configured_media_account_scope("signal", &signal(" ", "+15550001")).is_none());
    assert!(configured_media_account_scope("signal", &signal("http://s", " ")).is_none());
    assert!(
        configured_media_account_scope("signal", &tinychannels_bus::ChannelsConfig::default())
            .is_none()
    );
    let session_only_whatsapp = tinychannels_bus::ChannelsConfig {
        whatsapp: Some(WhatsAppConfig {
            access_token: None,
            phone_number_id: None,
            verify_token: None,
            app_secret: None,
            session_path: Some("/private/session".into()),
            pair_phone: None,
            pair_code: None,
            allowed_numbers: vec![],
        }),
        ..Default::default()
    };
    assert!(configured_media_account_scope("whatsapp", &session_only_whatsapp).is_none());

    let email = tinychannels_bus::ChannelsConfig {
        email: Some(EmailConfig {
            imap_host: "imap.example".into(),
            imap_port: 993,
            username: "user".into(),
            password: "secret".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut alternate_imap = email.clone();
    alternate_imap.email.as_mut().unwrap().imap_port = 1993;
    assert_ne!(
        configured_media_account_scope("email", &email),
        configured_media_account_scope("email", &alternate_imap),
        "endpoint authority participates in the account binding"
    );

    let mut missing_authority = email.clone();
    missing_authority.email.as_mut().unwrap().imap_host.clear();
    assert!(configured_media_account_scope("email", &missing_authority).is_none());

    let mut missing_authority = tinychannels_bus::ChannelsConfig {
        mattermost: Some(MattermostConfig {
            url: "https://chat.example".into(),
            bot_token: "secret".into(),
            channel_id: None,
            allowed_users: vec![],
            thread_replies: None,
            mention_only: None,
        }),
        ..Default::default()
    };
    assert!(configured_media_account_scope("mattermost", &missing_authority).is_some());
    missing_authority.mattermost.as_mut().unwrap().url.clear();
    assert!(configured_media_account_scope("mattermost", &missing_authority).is_none());

    let mut missing_authority = tinychannels_bus::ChannelsConfig {
        matrix: Some(MatrixConfig {
            homeserver: "https://matrix.example".into(),
            access_token: "secret".into(),
            user_id: Some("@bot:example".into()),
            device_id: None,
            room_id: "!room:example".into(),
            allowed_users: vec![],
        }),
        ..Default::default()
    };
    assert!(configured_media_account_scope("matrix", &missing_authority).is_some());
    missing_authority
        .matrix
        .as_mut()
        .unwrap()
        .homeserver
        .clear();
    assert!(configured_media_account_scope("matrix", &missing_authority).is_none());

    let mut missing_authority = tinychannels_bus::ChannelsConfig {
        irc: Some(IrcConfig {
            server: "irc.example".into(),
            port: 6697,
            nickname: "bot".into(),
            username: Some("auth-user".into()),
            channels: vec![],
            allowed_users: vec![],
            server_password: None,
            nickserv_password: None,
            sasl_password: Some("secret".into()),
            verify_tls: Some(true),
        }),
        ..Default::default()
    };
    assert!(configured_media_account_scope("irc", &missing_authority).is_some());
    missing_authority.irc.as_mut().unwrap().server.clear();
    assert!(configured_media_account_scope("irc", &missing_authority).is_none());
}

#[test]
fn slack_optional_socket_mode_token_does_not_change_media_account_scope() {
    use tinychannels_bus::config::SlackConfig;

    let mut config = tinychannels_bus::ChannelsConfig {
        slack: Some(SlackConfig {
            bot_token: "slack-bot-account-secret".into(),
            app_token: None,
            channel_id: None,
            allowed_users: vec![],
        }),
        ..Default::default()
    };
    let bot_only_scope = configured_media_account_scope("slack", &config).unwrap();
    config.slack.as_mut().unwrap().app_token = Some("xapp-first".into());
    assert_eq!(
        configured_media_account_scope("slack", &config).as_deref(),
        Some(bot_only_scope.as_str())
    );
    config.slack.as_mut().unwrap().app_token = Some("xapp-rotated".into());
    assert_eq!(
        configured_media_account_scope("slack", &config).as_deref(),
        Some(bot_only_scope.as_str()),
        "optional transport auth rotation preserves the bot account binding"
    );
}
use base64::Engine as _;
use sha2::Digest as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinychannels::host::EventSink as _;
use tinychannels_bus::{ChannelInboundEnvelope, ContentPart, ConversationKind, ConversationRef};
use tinychannels_conformance::AdapterFixture as _;

type TelegramMessageContextMap =
    Arc<std::sync::Mutex<std::collections::HashMap<String, (ConversationRef, Option<String>)>>>;

struct RunningTelegramConformance {
    adapter: ConfiguredAdapterHandle,
    running: Arc<Running>,
    server: wiremock::MockServer,
    scope: tinychannels_conformance::ExpectedScope,
    media: std::collections::HashMap<
        tinychannels_conformance::ConformanceFeature,
        tinychannels_bus::MediaReference,
    >,
    callbacks: IngressVoiceCallback,
    provider_host: Arc<dyn tinychannels::host::ChannelHost>,
    observed: std::sync::Mutex<Vec<tinychannels_conformance::WireObservation>>,
    request_cursor: std::sync::Mutex<usize>,
    response_cursor: std::sync::Mutex<usize>,
    response_ids: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    message_contexts: TelegramMessageContextMap,
}

#[async_trait::async_trait]
impl tinychannels_conformance::AdapterFixture for RunningTelegramConformance {
    fn adapter(&self) -> &dyn tinychannels::channel::ChannelAdapter {
        self.adapter.adapter()
    }

    fn expected_scope(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
    ) -> tinychannels_conformance::ExpectedScope {
        let mut scope = self.scope.clone();
        if feature == tinychannels_conformance::ConformanceFeature::InboundReactions {
            // MessageReactionUpdated does not carry a topic/thread field. The
            // outbound mutation cases below resolve their actual target ID
            // through the mock's recorded message context.
            scope.conversation.thread_id = None;
            scope.conversation.topic_id = None;
            scope.thread_id = None;
            scope.reply_to_id = None;
        }
        if feature == tinychannels_conformance::ConformanceFeature::VoiceIn {
            // Telegram Message carries message_thread_id as the normalized
            // topic_id; the shared envelope does not duplicate it in thread_id.
            scope.conversation.thread_id = None;
            scope.thread_id = None;
            scope.reply_to_id = None;
        }
        scope
    }

    fn clear_observations(&self) {
        self.observed.lock().unwrap().clear();
    }

    fn observations(&self) -> Vec<tinychannels_conformance::WireObservation> {
        self.observed.lock().unwrap().clone()
    }

    async fn exercise(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
    ) -> std::result::Result<
        Option<tinychannels_bus::channel::ChannelSendOutcome>,
        tinychannels_bus::channel::ChannelSendError,
    > {
        use tinychannels_bus::{ContentPart as Part, OutboundContentPart as OutPart};
        use tinychannels_conformance::ConformanceFeature as Feature;
        let media = || {
            self.media.get(&feature).cloned().unwrap_or_else(|| {
                let (kind, content_type, file_name) = match feature {
                    Feature::Image => (
                        tinychannels_bus::MediaKind::Image,
                        "image/png",
                        "fixture.png",
                    ),
                    Feature::Video => (
                        tinychannels_bus::MediaKind::Video,
                        "video/mp4",
                        "fixture.mp4",
                    ),
                    Feature::Audio => (
                        tinychannels_bus::MediaKind::Audio,
                        "audio/mpeg",
                        "fixture.mp3",
                    ),
                    Feature::VoiceOut => {
                        (tinychannels_bus::MediaKind::Audio, "audio/ogg", "voice.ogg")
                    }
                    Feature::Sticker => (
                        tinychannels_bus::MediaKind::Image,
                        "image/webp",
                        "sticker-512.webp",
                    ),
                    _ => (
                        tinychannels_bus::MediaKind::Document,
                        "text/plain",
                        "fixture.txt",
                    ),
                };
                tinychannels_bus::MediaReference {
                    url: Some(format!("https://fixtures.invalid/{file_name}")),
                    content_type: Some(content_type.into()),
                    file_name: Some(file_name.into()),
                    kind,
                    ..Default::default()
                }
            })
        };
        let content = |part| tinychannels_bus::OutboundPayload::Parts {
            parts: vec![OutPart::Content { part }],
        };
        let payload = match feature {
            Feature::Text | Feature::ThreadAddressing | Feature::ReplyAddressing => {
                tinychannels_bus::OutboundPayload::Text {
                    text: "module-kit".into(),
                }
            }
            Feature::Image => content(Part::Image {
                media: media(),
                caption: Some("image case".into()),
                alt_text: None,
            }),
            Feature::Video => content(Part::Video {
                media: media(),
                caption: Some("video case".into()),
                duration_ms: Some(1000),
                thumbnail: None,
            }),
            Feature::Audio => content(Part::Audio {
                media: media(),
                codec: Some("mp3".into()),
                duration_ms: Some(1000),
                title: Some("audio case".into()),
                transcript: None,
            }),
            Feature::File => content(Part::File {
                media: media(),
                file_name: Some("fixture.pdf".into()),
                caption: Some("file case".into()),
            }),
            Feature::Sticker => content(Part::Sticker {
                media: media(),
                pack_id: None,
                sticker_id: None,
                emoji: Some("🙂".into()),
                alt_text: None,
            }),
            Feature::Location => content(Part::Location {
                latitude: 29.3759,
                longitude: 47.9774,
                label: None,
                address: None,
                accuracy_meters: Some(10.0),
                live_until_unix_ms: None,
            }),
            Feature::Contact => content(Part::Contact {
                display_name: "Telegram Fixture".into(),
                phone_numbers: vec![tinychannels_bus::ContactMethod {
                    value: "+15550123".into(),
                    label: Some("CELL".into()),
                }],
                emails: Vec::new(),
                organization: None,
            }),
            Feature::Buttons => tinychannels_bus::OutboundPayload::Parts {
                parts: vec![
                    OutPart::Content {
                        part: Part::Text {
                            text: "Choose an action".into(),
                            markdown_dialect: None,
                        },
                    },
                    OutPart::Content {
                        part: Part::Buttons {
                            rows: vec![vec![tinychannels_bus::ButtonAction {
                                id: "opaque-action".into(),
                                label: "Continue".into(),
                                style: None,
                                disabled: false,
                            }]],
                        },
                    },
                ],
            },
            Feature::Poll => content(Part::Poll {
                question: "Choose one".into(),
                options: vec!["First".into(), "Second".into()],
                multiple_choice: false,
                closes_at_unix_ms: None,
                provider_poll_id: None,
            }),
            Feature::Presentation => content(Part::Presentation {
                blocks: serde_json::json!({"kind":"approval","id":"fixture-request","prompt":"Approve this action?","choices":["Approve","Deny"]}),
            }),
            Feature::NativeData => tinychannels_bus::OutboundPayload::NativeChannelData {
                data: serde_json::json!({"kind":"provider_specific"}),
            },
            Feature::VoiceOut => tinychannels_bus::OutboundPayload::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: media(),
                    audio_base64: None,
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1000),
                    waveform: None,
                },
            },
            Feature::VoiceSynthesis => {
                let scope = tinychannels_bus::HostVoiceScope {
                    configured_instance: self.adapter.configured_instance.clone(),
                    channel: self.scope.channel.clone(),
                    conversation: self.scope.conversation.clone(),
                    turn_id: "turn-kit".into(),
                };
                let synthesizer = self
                    .provider_host
                    .synthesizer()
                    .ok_or_else(tinychannels_bus::channel::ChannelSendError::default)?;
                let synthesized = synthesizer
                    .synthesize_scoped(
                        scope.clone(),
                        tinychannels::host::SpeechRequest {
                            text: "speak this".into(),
                            voice: None,
                            format: Some("ogg_opus".into()),
                            thread_id: scope
                                .conversation
                                .thread_id
                                .clone()
                                .or_else(|| scope.conversation.topic_id.clone()),
                        },
                    )
                    .await
                    .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let audio = base64::engine::general_purpose::STANDARD
                    .decode(&synthesized.audio_base64)
                    .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let repository = self
                    .provider_host
                    .media_repository()
                    .ok_or_else(tinychannels_bus::channel::ChannelSendError::default)?;
                let stored = repository
                    .store_media(tinychannels::host::MediaStoreRequest {
                        metadata: tinychannels::host::MediaMetadata {
                            scope: scope.clone(),
                            kind: tinychannels_bus::MediaKind::Audio,
                            content_type: synthesized.mime_type.clone(),
                            file_name: Some("synthesized.ogg".into()),
                            duration_ms: synthesized.duration_ms,
                            codec: Some("ogg_opus".into()),
                            voice_direction: Some(
                                tinychannels_bus::HostVoiceMediaDirection::Outbound,
                            ),
                            dedupe_key: None,
                            max_bytes: 8 * 1024 * 1024,
                        },
                        bytes: audio,
                    })
                    .await
                    .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                tinychannels_bus::OutboundPayload::Voice {
                    voice: tinychannels_bus::OutboundVoice::Media {
                        media: stored,
                        audio_base64: None,
                        codec: Some("ogg_opus".into()),
                        duration_ms: synthesized.duration_ms,
                        waveform: None,
                    },
                }
            }
            Feature::VoiceIn | Feature::InboundReactions => {
                tinychannels_bus::OutboundPayload::Text {
                    text: "ingress observation".into(),
                }
            }
            Feature::Typing
            | Feature::Reactions
            | Feature::Edit
            | Feature::Delete
            | Feature::StreamingDraft => tinychannels_bus::OutboundPayload::Text {
                text: "operation fixture".into(),
            },
            Feature::ReadReceipts => {
                return Err(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Forbidden,
                    message: "Telegram Bot API has no read-receipt operation".into(),
                    unsupported: true,
                    ..Default::default()
                });
            }
        };
        let intent = tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "module-kit-send".into(),
            channel_id: "telegram:account-a".into(),
            conversation_id: self.scope.conversation.id.clone(),
            channel_ref: Some(self.scope.channel.clone()),
            conversation_ref: Some(self.scope.conversation.clone()),
            reply_to_id: self.scope.reply_to_id.clone(),
            thread_id: self.scope.thread_id.clone(),
            turn_id: Some("turn-kit".into()),
            payload,
            ..Default::default()
        };
        let result: std::result::Result<
            Option<tinychannels_bus::channel::ChannelSendOutcome>,
            tinychannels_bus::channel::ChannelSendError,
        > = match feature {
            Feature::VoiceIn => {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        if !self.callbacks.inbound.lock().await.is_empty() {
                            break;
                        }
                        self.callbacks.ingress_changed.notified().await;
                    }
                })
                .await
                .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let envelope = self
                    .callbacks
                    .inbound
                    .lock()
                    .await
                    .first()
                    .map(|(_, value)| value.clone())
                    .ok_or_else(tinychannels_bus::channel::ChannelSendError::default)?;
                let channel =
                    serde_json::from_value(envelope.get("channel").cloned().unwrap_or_default())
                        .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let conversation: ConversationRef = serde_json::from_value(
                    envelope.get("conversation").cloned().unwrap_or_default(),
                )
                .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let parts = envelope
                    .get("parts")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let voice = parts
                    .iter()
                    .find(|part| {
                        part.get("kind").and_then(serde_json::Value::as_str) == Some("voice_note")
                    })
                    .and_then(|part| part.get("voiceNote"));
                let mut payload_kinds = Vec::new();
                if voice.and_then(|v| v.get("media")).is_some() {
                    payload_kinds.push("audio".into());
                }
                if voice
                    .and_then(|v| v.get("transcript"))
                    .and_then(|t| t.get("text"))
                    .and_then(serde_json::Value::as_str)
                    .is_some()
                {
                    payload_kinds.push("transcript".into());
                }
                let thread_id = conversation
                    .thread_id
                    .clone()
                    .or_else(|| conversation.topic_id.clone());
                self.observed
                    .lock()
                    .unwrap()
                    .push(tinychannels_conformance::WireObservation {
                        feature,
                        operation: "voice_in".into(),
                        channel,
                        conversation,
                        reply_to_id: None,
                        thread_id,
                        payload_kinds,
                        platform_message_ids: Vec::new(),
                        request: envelope,
                    });
                return Ok(None);
            }
            Feature::InboundReactions => {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        if self.callbacks.events.lock().await.iter().any(|event| {
                            matches!(
                                event.event,
                                tinychannels_bus::ChannelEventKind::ReactionAdded { .. }
                                    | tinychannels_bus::ChannelEventKind::ReactionRemoved { .. }
                            )
                        }) {
                            break;
                        }
                        self.callbacks.ingress_changed.notified().await;
                    }
                })
                .await
                .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                let events = self.callbacks.events.lock().await;
                let event = events
                    .iter()
                    .find(|event| {
                        matches!(
                            event.event,
                            tinychannels_bus::ChannelEventKind::ReactionAdded { .. }
                                | tinychannels_bus::ChannelEventKind::ReactionRemoved { .. }
                        )
                    })
                    .expect("reaction update observed");
                self.observed
                    .lock()
                    .unwrap()
                    .push(tinychannels_conformance::WireObservation {
                        feature,
                        operation: "reaction_receive".into(),
                        channel: event.channel.clone(),
                        conversation: event.conversation.clone().unwrap_or_default(),
                        reply_to_id: None,
                        thread_id: None,
                        payload_kinds: Vec::new(),
                        platform_message_ids: Vec::new(),
                        request: serde_json::to_value(event).unwrap(),
                    });
                return Ok(None);
            }
            Feature::Typing => {
                self.running
                    .interact(tinychannels_bus::ChannelInteraction::StartTyping {
                        recipient: format!(
                            "{}:{}",
                            self.scope.conversation.id,
                            self.scope.thread_id.as_deref().unwrap_or_default()
                        ),
                    })
                    .await
                    .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                Ok(None)
            }
            Feature::Reactions => {
                let sent = self.adapter.send(intent.clone()).await?;
                let provider_message_id = sent
                    .receipt
                    .primary_platform_message_id
                    .clone()
                    .ok_or_else(tinychannels_bus::channel::ChannelSendError::default)?;
                *self.request_cursor.lock().unwrap() = self
                    .server
                    .received_requests()
                    .await
                    .unwrap_or_default()
                    .len();
                *self.response_cursor.lock().unwrap() = self.response_ids.lock().unwrap().len();
                self.running
                    .interact(tinychannels_bus::ChannelInteraction::SendReaction {
                        channel: self.scope.channel.clone(),
                        conversation: self.scope.conversation.clone(),
                        provider_message_id,
                        reaction: tinychannels_bus::channel::ChannelReaction {
                            value: "👍".into(),
                            custom_emoji_id: None,
                        },
                        remove: false,
                    })
                    .await
                    .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                Ok(None)
            }
            Feature::Edit | Feature::Delete | Feature::StreamingDraft => {
                let sent = if feature == Feature::StreamingDraft {
                    None
                } else {
                    Some(self.adapter.send(intent.clone()).await?)
                };
                let receipt = sent.as_ref().map(|outcome| outcome.receipt.clone());
                *self.request_cursor.lock().unwrap() = self
                    .server
                    .received_requests()
                    .await
                    .unwrap_or_default()
                    .len();
                *self.response_cursor.lock().unwrap() = self.response_ids.lock().unwrap().len();
                match feature {
                    Feature::Edit => {
                        let receipt = receipt.expect("edit fixture primed a provider message");
                        let edit = tinychannels_bus::ChannelOutboundIntent {
                            payload: tinychannels_bus::OutboundPayload::Text {
                                text: "edited kit".into(),
                            },
                            ..intent.clone()
                        };
                        let result = self
                            .running
                            .interact(tinychannels_bus::ChannelInteraction::EditMessage {
                                receipt,
                                intent: edit,
                            })
                            .await
                            .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                        let tinychannels_bus::ChannelInteractionResult::Sent(outcome) = result
                        else {
                            return Err(tinychannels_bus::channel::ChannelSendError::default());
                        };
                        Ok(Some(*outcome))
                    }
                    Feature::Delete => {
                        self.running
                            .interact(tinychannels_bus::ChannelInteraction::DeleteMessage {
                                receipt: receipt.expect("delete fixture primed a provider message"),
                            })
                            .await
                            .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                        Ok(None)
                    }
                    Feature::StreamingDraft => {
                        let created = self
                            .running
                            .interact(tinychannels_bus::ChannelInteraction::UpsertDraft {
                                previous: None,
                                intent: intent.clone(),
                            })
                            .await
                            .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                        let tinychannels_bus::ChannelInteractionResult::Sent(created) = created
                        else {
                            return Err(tinychannels_bus::channel::ChannelSendError::default());
                        };
                        let updated_intent = tinychannels_bus::ChannelOutboundIntent {
                            payload: tinychannels_bus::OutboundPayload::Text {
                                text: "draft update".into(),
                            },
                            ..intent.clone()
                        };
                        let updated = self
                            .running
                            .interact(tinychannels_bus::ChannelInteraction::UpsertDraft {
                                previous: Some(created.receipt),
                                intent: updated_intent.clone(),
                            })
                            .await
                            .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                        let tinychannels_bus::ChannelInteractionResult::Sent(updated) = updated
                        else {
                            return Err(tinychannels_bus::channel::ChannelSendError::default());
                        };
                        let finalized = self
                            .running
                            .interact(tinychannels_bus::ChannelInteraction::EditMessage {
                                receipt: updated.receipt.clone(),
                                intent: tinychannels_bus::ChannelOutboundIntent {
                                    payload: tinychannels_bus::OutboundPayload::Text {
                                        text: "draft final".into(),
                                    },
                                    ..updated_intent
                                },
                            })
                            .await
                            .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                        let canceled = self
                            .running
                            .interact(tinychannels_bus::ChannelInteraction::UpsertDraft {
                                previous: None,
                                intent: tinychannels_bus::ChannelOutboundIntent {
                                    payload: tinychannels_bus::OutboundPayload::Text {
                                        text: "draft cancel pending".into(),
                                    },
                                    ..intent.clone()
                                },
                            })
                            .await
                            .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                        let tinychannels_bus::ChannelInteractionResult::Sent(canceled) = canceled
                        else {
                            return Err(tinychannels_bus::channel::ChannelSendError::default());
                        };
                        self.running
                            .interact(tinychannels_bus::ChannelInteraction::DeleteMessage {
                                receipt: canceled.receipt,
                            })
                            .await
                            .map_err(|_| tinychannels_bus::channel::ChannelSendError::default())?;
                        let tinychannels_bus::ChannelInteractionResult::Sent(outcome) = finalized
                        else {
                            return Err(tinychannels_bus::channel::ChannelSendError::default());
                        };
                        Ok(Some(*outcome))
                    }
                    _ => unreachable!(),
                }
            }
            _ => self.adapter.send(intent).await.map(Some),
        };
        let requests = self.server.received_requests().await.unwrap_or_default();
        let mut cursor = self.request_cursor.lock().unwrap();
        for request in requests.iter().skip(*cursor) {
            if !request.method.to_string().eq_ignore_ascii_case("POST") {
                continue;
            }
            let endpoint = request.url.path().rsplit('/').next().unwrap_or_default();
            let raw_body = String::from_utf8_lossy(&request.body);
            let body: serde_json::Value = serde_json::from_slice(&request.body)
                .unwrap_or_else(|_| serde_json::json!({"raw_multipart": raw_body}));
            let field = |name: &str| -> Option<String> {
                body.get(name)
                    .and_then(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .or_else(|| v.as_i64().map(|n| n.to_string()))
                    })
                    .or_else(|| {
                        let marker = format!("name=\"{name}\"\r\n\r\n");
                        raw_body
                            .split_once(&marker)?
                            .1
                            .split("\r\n")
                            .next()
                            .map(str::to_owned)
                    })
            };
            let chat = field("chat_id").unwrap_or_default();
            let message_id = field("message_id");
            let mapped_context = message_id
                .as_ref()
                .and_then(|id| self.message_contexts.lock().unwrap().get(id).cloned());
            let mut conversation = mapped_context
                .as_ref()
                .map(|(conversation, _)| conversation.clone())
                .unwrap_or_else(|| self.scope.conversation.clone());
            if !chat.is_empty() {
                conversation.id = chat.clone();
            }
            let thread_id = mapped_context
                .as_ref()
                .and_then(|(conversation, _)| {
                    conversation
                        .thread_id
                        .clone()
                        .or_else(|| conversation.topic_id.clone())
                })
                .or_else(|| field("message_thread_id"));
            conversation.thread_id = thread_id.clone();
            conversation.topic_id = thread_id.clone();
            let reply_to_id = mapped_context
                .as_ref()
                .and_then(|(_, reply)| reply.clone())
                .or_else(|| field("reply_to_message_id"));
            let (mut operation, endpoint_kind) = match endpoint {
                "sendMessage" => ("send", "text"),
                "sendPhoto" => ("send", "image"),
                "sendVideo" => ("send", "video"),
                "sendAudio" => ("send", "audio"),
                "sendVoice" => ("send", "voice"),
                "sendDocument" => ("send", "file"),
                "sendSticker" => ("send", "sticker"),
                "sendLocation" | "sendVenue" => ("send", "location"),
                "sendContact" => ("send", "contact"),
                "sendPoll" => ("send", "poll"),
                "editMessageText" => ("edit", "text"),
                "deleteMessage" => ("delete", ""),
                "setMessageReaction" => ("reaction_send", ""),
                "sendChatAction" => ("typing", ""),
                _ => continue,
            };
            if feature == Feature::StreamingDraft {
                operation = match endpoint {
                    "sendMessage" if field("text").as_deref() == Some("operation fixture") => {
                        "draft_create"
                    }
                    "sendMessage" => "draft_cancel_create",
                    "editMessageText" if field("text").as_deref() == Some("draft update") => {
                        "draft_update"
                    }
                    "editMessageText" => "draft_finalize",
                    "deleteMessage" => "draft_cancel_cleanup",
                    _ => operation,
                };
            }
            let keyboard = body
                .get("reply_markup")
                .and_then(|markup| markup.get("inline_keyboard"))
                .and_then(serde_json::Value::as_array);
            let has_action_button = keyboard.is_some_and(|rows| {
                rows.iter().any(|row| {
                    row.as_array().is_some_and(|buttons| {
                        buttons.iter().any(|button| {
                            button
                                .get("callback_data")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|data| !data.is_empty())
                        })
                    })
                })
            });
            let payload_kind = if feature == Feature::Buttons && has_action_button {
                "buttons"
            } else if feature == Feature::Presentation
                && has_action_button
                && body
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|text| !text.is_empty())
            {
                "presentation"
            } else {
                endpoint_kind
            };
            let multipart_fields = [
                "photo",
                "video",
                "audio",
                "voice",
                "document",
                "sticker",
                "latitude",
                "phone_number",
                "question",
            ]
            .into_iter()
            .filter(|name| raw_body.contains(&format!("name=\"{name}\"")))
            .collect::<Vec<_>>();
            let request_summary = serde_json::json!({
                "endpoint": endpoint,
                "chat_id": chat,
                "message_thread_id": field("message_thread_id"),
                "reply_to_message_id": field("reply_to_message_id"),
                "reply_parameters": body.get("reply_parameters"),
                "message_id": field("message_id"),
                "text": body.get("text"),
                "reply_markup": body.get("reply_markup"),
                "multipart_fields": multipart_fields,
            });
            let ids = self.response_ids.lock().unwrap();
            let mut response_cursor = self.response_cursor.lock().unwrap();
            let matched_response = ids
                .iter()
                .enumerate()
                .skip(*response_cursor)
                .find(|(_, (path, _))| path == endpoint);
            let platform_message_ids = if let Some((index, (_, id))) = matched_response {
                *response_cursor = index + 1;
                vec![id.clone()]
            } else {
                message_id.clone().into_iter().collect()
            };
            drop(response_cursor);
            drop(ids);
            self.observed
                .lock()
                .unwrap()
                .push(tinychannels_conformance::WireObservation {
                    feature,
                    operation: operation.into(),
                    channel: self.scope.channel.clone(),
                    conversation,
                    reply_to_id,
                    thread_id,
                    payload_kinds: if payload_kind.is_empty() {
                        vec![]
                    } else {
                        vec![payload_kind.into()]
                    },
                    platform_message_ids,
                    request: request_summary,
                });
        }
        *cursor = requests.len();
        result
    }
}

#[derive(Clone, Default)]
struct IngressVoiceCallback {
    read_requests: Arc<Mutex<Vec<tinychannels_bus::HostReadVoiceMediaRequest>>>,
    stt_requests: Arc<Mutex<Vec<tinychannels_bus::HostVoiceTranscriptionRequest>>>,
    inbound: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    events: Arc<Mutex<Vec<tinychannels_bus::ChannelEvent>>>,
    ingress_changed: Arc<tokio::sync::Notify>,
    transcription_result: Arc<Mutex<Option<String>>>,
    stt_calls: Arc<AtomicUsize>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl IngressVoiceCallback {
    async fn read_voice_media(
        &self,
        request: tinychannels_bus::HostReadVoiceMediaRequest,
    ) -> Result<tinychannels_bus::HostReadVoiceMediaResult> {
        self.read_requests.lock().await.push(request);
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(include_bytes!(
                "../../../src/providers/telegram/fixtures/sine-ogg-opus.ogg"
            )),
            mime_type: "audio/ogg".into(),
            file_name: Some("voice.oga".into()),
        })
    }

    async fn transcribe_voice(
        &self,
        request: tinychannels_bus::HostVoiceTranscriptionRequest,
    ) -> Result<tinychannels_bus::HostTranscriptionResult> {
        self.stt_calls.fetch_add(1, Ordering::SeqCst);
        self.stt_requests.lock().await.push(request);
        if let Some(text) = self.transcription_result.lock().await.clone() {
            Ok(tinychannels_bus::HostTranscriptionResult {
                text,
                language: Some("en".into()),
                duration_secs: Some(1.0),
            })
        } else {
            Err(tinybus::Error::failed("fixture STT unavailable"))
        }
    }

    async fn synthesize_speech(
        &self,
        _request: tinychannels_bus::HostSpeechSynthesisRequest,
    ) -> Result<tinychannels_bus::HostSpeechSynthesisResult> {
        Ok(tinychannels_bus::HostSpeechSynthesisResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode(include_bytes!(
                "../../../src/providers/telegram/fixtures/sine-ogg-opus.ogg"
            )),
            mime_type: "audio/ogg".into(),
            duration_ms: Some(1000),
        })
    }

    async fn store_voice_media(
        &self,
        request: tinychannels_bus::HostStoreVoiceMediaRequest,
    ) -> Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        let media = request
            .media_reference
            .unwrap_or(tinychannels_bus::MediaReference {
                opaque_id: Some("fixture-voice-media".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                content_type: Some(request.mime_type),
                file_name: request.file_name,
                ..Default::default()
            });
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media,
            accessible_url: None,
        })
    }

    async fn deliver_inbound(&self, channel: String, message: serde_json::Value) -> Result<()> {
        self.inbound.lock().await.push((channel, message));
        self.ingress_changed.notify_one();
        Ok(())
    }

    async fn deliver_event(&self, event: tinychannels_bus::ChannelEvent) -> Result<()> {
        self.events.lock().await.push(event);
        self.ingress_changed.notify_one();
        Ok(())
    }
}

async fn ingress_fixture() -> (
    HostChannels,
    IngressVoiceCallback,
    tinybus::Connection,
    crate::storage_fixture::StorageHost,
) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let server = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let storage = crate::storage_fixture::StorageHost::serve(&server, "scope-a").await;
    let callback = IngressVoiceCallback::default();
    server
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callback.clone(),
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
    (HostChannels::new(client), callback, server, storage)
}

fn inbound_voice(transcript: Option<tinychannels_bus::VoiceTranscript>) -> ChannelInboundEnvelope {
    ChannelInboundEnvelope {
        channel: tinychannels_bus::ChannelRef {
            id: "forged-provider".into(),
            account_id: Some("forged-account".into()),
        },
        message_id: "message-voice-13".into(),
        conversation: ConversationRef {
            kind: ConversationKind::Thread,
            id: "chat-43".into(),
            scope_id: Some("community-7".into()),
            parent_id: Some("chat-parent".into()),
            thread_id: Some("thread-8".into()),
            topic_id: Some("topic-9".into()),
        },
        parts: vec![ContentPart::VoiceNote {
            voice_note: tinychannels_bus::VoiceNote {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("opaque-voice".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    content_type: Some("audio/ogg".into()),
                    ..Default::default()
                },
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1400),
                waveform: Some(vec![0.1, 0.6, 1.0]),
                transcript,
                ..Default::default()
            },
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn ingress_retains_voice_and_full_scope_when_stt_fails() {
    let (host, callback, _server, _storage) = ingress_fixture().await;
    let events = ModuleEvents(
        host.clone(),
        "telegram:bot-a".into(),
        crate::provider::MediaScope::default(),
    );
    assert!(
        events
            .publish_ingress(tinychannels_bus::ChannelIngress::Inbound(inbound_voice(
                None
            )))
            .await
            .unwrap()
    );
    host.flush().await.unwrap();

    let read = callback.read_requests.lock().await;
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].max_bytes, 8 * 1024 * 1024);
    assert_eq!(read[0].opaque_id, "opaque-voice");
    let stt = callback.stt_requests.lock().await;
    assert_eq!(stt.len(), 1);
    assert_eq!(stt[0].scope.configured_instance, "telegram:bot-a");
    assert_eq!(stt[0].scope.channel.id, "telegram");
    assert!(!stt[0].scope.configured_instance.contains("media-v1"));
    assert_eq!(
        stt[0].scope.channel.account_id.as_deref(),
        Some("telegram:bot-a")
    );
    assert!(
        !stt[0]
            .scope
            .channel
            .account_id
            .as_deref()
            .unwrap()
            .contains("media-v1")
    );
    assert_eq!(
        stt[0].scope.conversation,
        ConversationRef {
            kind: ConversationKind::Thread,
            id: "chat-43".into(),
            scope_id: Some("community-7".into()),
            parent_id: Some("chat-parent".into()),
            thread_id: Some("thread-8".into()),
            topic_id: Some("topic-9".into()),
        }
    );
    assert_eq!(stt[0].scope.turn_id, "message-voice-13");
    drop(stt);
    drop(read);

    let inbound = callback.inbound.lock().await;
    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].0, "telegram:bot-a");
    let note = &inbound[0].1["parts"][0]["voiceNote"];
    assert_eq!(note["media"]["opaqueId"], "opaque-voice");
    assert_eq!(note["codec"], "ogg_opus");
    assert_eq!(note["durationMs"], 1400);
    let waveform = note["waveform"].as_array().unwrap();
    assert_eq!(waveform.len(), 3);
    for (actual, expected) in waveform.iter().zip([0.1_f64, 0.6, 1.0]) {
        assert!((actual.as_f64().unwrap() - expected).abs() < 0.000_001);
    }
    assert_eq!(note["transcript"]["state"], "unavailable");
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_provider_transcript_skips_media_read_and_host_stt() {
    let (host, callback, _server, _storage) = ingress_fixture().await;
    let events = ModuleEvents(
        host.clone(),
        "telegram:bot-a".into(),
        crate::provider::MediaScope::default(),
    );
    let provider_transcript = tinychannels_bus::VoiceTranscript {
        text: "provider words".into(),
        language: Some("en".into()),
        state: tinychannels_bus::VoiceTranscriptState::Complete,
        source: tinychannels_bus::VoiceTranscriptSource::Provider,
    };
    events
        .publish_ingress(tinychannels_bus::ChannelIngress::Inbound(inbound_voice(
            Some(provider_transcript),
        )))
        .await
        .unwrap();
    host.flush().await.unwrap();
    assert!(callback.read_requests.lock().await.is_empty());
    assert_eq!(callback.stt_calls.load(Ordering::SeqCst), 0);
    let inbound = callback.inbound.lock().await;
    assert_eq!(
        inbound[0].1["parts"][0]["voiceNote"]["transcript"]["text"],
        "provider words"
    );
    assert_eq!(
        inbound[0].1["parts"][0]["voiceNote"]["media"]["opaqueId"],
        "opaque-voice"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the separately built TinyVoice module; run with TINYVOICE_MODULE_ARTIFACT"]
async fn codec_adapter_calls_loaded_tinyvoice_module() {
    use tinybus::broker::Broker;
    use tinybus::module::ModuleHost;
    use tinychannels::host::VoiceCodec as _;
    use tinyvoice_bus::codec::AudioCodec;

    let path = std::env::var("TINYVOICE_MODULE_ARTIFACT").expect("module artifact env");
    let bytes = std::fs::read(&path).expect("read TinyVoice artifact");
    let expected_sha = std::env::var("TINYVOICE_MODULE_SHA256").expect("expected artifact SHA env");
    assert_eq!(hex::encode(sha2::Sha256::digest(&bytes)), expected_sha);
    let bus = tinybus::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let info = host
        .load_file(&path)
        .expect("load compiled TinyVoice module");
    assert_eq!(info.name, "tinyvoice-module");
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = ModuleVoiceCodec(HostChannels::new(connection.clone()));
    let wav = sine_wave_wav();
    let wav_b64 = base64::engine::general_purpose::STANDARD.encode(&wav);
    let probe_proxy = connection
        .proxy(
            tinyvoice_bus::BUS_NAME,
            tinyvoice_bus::OBJECT_PATH,
            tinyvoice_bus::BUS_NAME,
        )
        .unwrap();
    let info: tinyvoice_bus::codec::CodecInfo = probe_proxy
        .call(
            tinyvoice_bus::names::methods::CODEC_PROBE,
            (tinyvoice_bus::codec::CodecInput {
                audio_base64: wav_b64.clone(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(info.codec, AudioCodec::WavPcm16);
    assert!(info.duration_ms > 0);
    let opus = client
        .transcode(tinychannels::host::VoiceCodecRequest {
            audio_base64: wav_b64,
            from: "wav_pcm16".into(),
            target: "ogg_opus".into(),
        })
        .await
        .expect("compiled TinyVoice WAV→Opus transcode");
    let round_trip = client
        .transcode(tinychannels::host::VoiceCodecRequest {
            audio_base64: opus.audio_base64,
            from: "ogg_opus".into(),
            target: "wav_pcm16".into(),
        })
        .await
        .expect("compiled TinyVoice Opus→WAV transcode");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(round_trip.audio_base64)
        .unwrap();
    assert!(decoded.starts_with(b"RIFF"));
    assert!(decoded.iter().any(|byte| *byte != 0));
}

fn sine_wave_wav() -> Vec<u8> {
    let sample_rate = 16_000u32;
    let sample_count = sample_rate as usize;
    let data_len = sample_count * 2;
    let mut wav = Vec::with_capacity(44 + data_len);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36u32 + data_len as u32).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&(sample_rate * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(data_len as u32).to_le_bytes());
    for index in 0..sample_count {
        let phase = (index as f32 * 440.0 * std::f32::consts::TAU) / sample_rate as f32;
        let sample = (phase.sin() * 12_000.0) as i16;
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    wav
}

struct SlowChannel {
    started: StdMutex<Option<oneshot::Sender<()>>>,
    release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl Channel for SlowChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        if let Some(release) = self.release.lock().await.take() {
            let _ = release.await;
        }
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn canceled_send_waiter_does_not_cancel_owned_send_or_shutdown_join() {
    let (started, started_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let channel = Arc::new(SlowChannel {
        started: StdMutex::new(Some(started)),
        release: tokio::sync::Mutex::new(Some(release_rx)),
    });
    let connection = tinybus::Connection::connect({
        let bus = tinybus::transport::memory::MemoryBus::new();
        tinybus::broker::Broker::new().spawn(bus.clone());
        bus.connect().await.unwrap()
    })
    .await
    .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start(channel, Arc::new(Hooks::default()), host);
    let send_waiter = tokio::spawn({
        let running = running.clone();
        async move { running.send(SendMessage::new("body", "recipient")).await }
    });
    started_rx.await.unwrap();
    send_waiter.abort();
    let _ = send_waiter.await;

    let stop = tokio::spawn({
        let running = running.clone();
        async move { running.stop().await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), async {
            while !stop.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err()
    );
    release.send(()).unwrap();
    stop.await.unwrap().unwrap();
}

#[derive(Default)]
struct InteractionChannel(StdMutex<Vec<String>>);
#[async_trait::async_trait]
impl Channel for InteractionChannel {
    fn name(&self) -> &str {
        "interaction-fixture"
    }
    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }
    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("send:{}", message.content));
        Ok(())
    }
    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("typing:{recipient}"));
        Ok(())
    }
    async fn send_draft(&self, message: &SendMessage) -> anyhow::Result<Option<String>> {
        self.0
            .lock()
            .unwrap()
            .push(format!("draft:{}", message.content));
        Ok(Some("module-draft".into()))
    }
    async fn stop_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("stop:{recipient}"));
        Ok(())
    }
}

#[tokio::test]
async fn provider_actor_orders_interactions_with_sends_and_rejects_after_stop() {
    use tinychannels_bus::{ChannelInteraction as Op, ChannelInteractionResult};
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel.clone(),
        Arc::new(Hooks::default()),
        HostChannels::new(connection),
    );
    running
        .interact(Op::StartTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    running
        .send(SendMessage::new("ordinary", "room"))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let draft = running
        .interact(Op::SendDraft {
            message: SendMessage::new("partial", "room"),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(matches!(draft, ChannelInteractionResult::Draft(Some(id)) if id == "module-draft"));
    running
        .interact(Op::StopTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    running.stop().await.unwrap();
    assert_eq!(
        *channel.0.lock().unwrap(),
        ["typing:room", "send:ordinary", "draft:partial", "stop:room"]
    );
    assert!(matches!(
        running.interact(Op::Describe).await,
        Err(RunningSendError::NotDispatched(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_is_bounded_and_joined_by_provider_owner() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel.clone(),
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );

    running
        .interact(Op::StartTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap();
    let first_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "room".into(),
            interval_secs: 4,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh {
        handle: first_handle,
    } = first_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    let second_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "room".into(),
            interval_secs: 4,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh {
        handle: second_handle,
    } = second_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    assert_ne!(first_handle, second_handle);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(4)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        channel.0.lock().unwrap().len(),
        3,
        "both refresh handles remain independently active"
    );

    assert!(
        running
            .interact(Op::StartTypingRefresh {
                recipient: "other".into(),
                interval_secs: 0,
            })
            .await
            .is_err()
    );
    running
        .interact(Op::StopTypingRefresh {
            handle: first_handle,
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(channel.0.lock().unwrap().len(), 4);
    running
        .interact(Op::StopTypingRefresh {
            handle: second_handle.clone(),
        })
        .await
        .unwrap();
    running
        .interact(Op::StopTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap();
    running.stop().await.unwrap();
    assert!(matches!(
        running
            .interact(Op::StopTypingRefresh {
                handle: second_handle,
            })
            .await,
        Err(RunningSendError::NotDispatched(_))
    ));
    assert_eq!(
        *channel.0.lock().unwrap(),
        [
            "typing:room",
            "typing:room",
            "typing:room",
            "typing:room",
            "stop:room"
        ]
    );
    connection.close().await.unwrap();
}

struct PendingTypingChannel {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl Channel for PendingTypingChannel {
    fn name(&self) -> &str {
        "pending-typing"
    }

    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }

    async fn start_typing(&self, _: &str) -> anyhow::Result<()> {
        struct DropMark(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for DropMark {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let _drop_mark = DropMark(Arc::clone(&self.dropped));
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_stop_and_shutdown_cancel_inflight_provider_calls() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let channel = Arc::new(PendingTypingChannel {
        entered: Arc::clone(&entered),
        dropped: Arc::clone(&dropped),
    });
    let running = Running::start(
        channel,
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );

    let stop_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "stop-case".into(),
            interval_secs: 1,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh {
        handle: stop_handle,
    } = stop_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    entered.notified().await;
    running
        .interact(Op::StopTypingRefresh {
            handle: stop_handle,
        })
        .await
        .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let shutdown_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "shutdown-case".into(),
            interval_secs: 1,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh { handle: _ } = shutdown_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    entered.notified().await;
    running.stop().await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    connection.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_capacity_counts_handles_not_recipients() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel,
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );
    let mut handles = Vec::new();
    for _ in 0..MAX_TYPING_REFRESH_RECIPIENTS {
        let result = running
            .interact(Op::StartTypingRefresh {
                recipient: "same-room".into(),
                interval_secs: MAX_TYPING_REFRESH_INTERVAL_SECS,
            })
            .await
            .unwrap();
        let tinychannels_bus::ChannelInteractionResult::TypingRefresh { handle } = result else {
            panic!("provider did not return a refresh handle")
        };
        handles.push(handle);
    }
    assert!(
        running
            .interact(Op::StartTypingRefresh {
                recipient: "same-room".into(),
                interval_secs: MAX_TYPING_REFRESH_INTERVAL_SECS,
            })
            .await
            .is_err()
    );
    for handle in handles {
        running
            .interact(Op::StopTypingRefresh { handle })
            .await
            .unwrap();
    }
    running.stop().await.unwrap();
    connection.close().await.unwrap();
}

struct ReconnectingChannel {
    attempts: StdMutex<Vec<oneshot::Sender<()>>>,
}
#[async_trait::async_trait]
impl Channel for ReconnectingChannel {
    fn name(&self) -> &str {
        "reconnect-fixture"
    }
    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        let last = {
            let mut attempts = self.attempts.lock().unwrap();
            let receipt = attempts.remove(0);
            let _ = receipt.send(());
            attempts.is_empty()
        };
        if last {
            std::future::pending::<()>().await;
        }
        anyhow::bail!("fixture disconnected")
    }
}

#[tokio::test(start_paused = true)]
async fn disconnected_module_provider_reconnects_and_stop_joins_the_new_listener() {
    let (first, first_rx) = oneshot::channel();
    let (second, mut second_rx) = oneshot::channel();
    let channel = Arc::new(ReconnectingChannel {
        attempts: StdMutex::new(vec![first, second]),
    });
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start_supervised(
        channel,
        Arc::new(Hooks::default()),
        host.clone(),
        ChannelSupervisionPolicy::default(),
    );
    first_rx.await.unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        second_rx.try_recv().is_ok(),
        "the module must own provider reconnects"
    );
    assert!(!running.is_finished());
    running.stop().await.unwrap();
    assert!(running.is_finished());
    host.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn stopping_during_selected_backoff_prevents_another_provider_attempt() {
    let (first, first_rx) = oneshot::channel();
    let (second, mut second_rx) = oneshot::channel();
    let channel = Arc::new(ReconnectingChannel {
        attempts: StdMutex::new(vec![first, second]),
    });
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start_supervised(
        channel.clone(),
        Arc::new(Hooks::default()),
        host.clone(),
        ChannelSupervisionPolicy {
            initial_backoff_secs: 30,
            max_backoff_secs: 60,
        },
    );
    first_rx.await.unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        second_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    running.stop().await.unwrap();
    tokio::time::advance(Duration::from_secs(120)).await;
    assert_eq!(channel.attempts.lock().unwrap().len(), 1);
    assert!(running.is_finished());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn lifecycle_hooks_are_drained_once_in_registration_order() {
    let hooks = Hooks::default();
    let calls = Arc::new(StdMutex::new(Vec::new()));
    for index in 0..3 {
        let calls = calls.clone();
        hooks.register_shutdown(
            "fixture",
            Box::new(move || {
                Box::pin(async move {
                    calls.lock().unwrap().push(index);
                })
            }),
        );
    }
    hooks.drain().await;
    hooks.drain().await;
    assert_eq!(*calls.lock().unwrap(), [0, 1, 2]);
}

#[tokio::test]
async fn host_adapters_propagate_callback_failures() {
    use tinychannels::host::{AllowlistStore, EventSink, Transcriber};
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    // Once host callbacks stop, adapters must surface admission failures.
    host.shutdown().await.unwrap();
    assert!(
        ModuleAllowlist(host.clone())
            .persist_allowed_identity("fixture", "member")
            .await
            .is_err()
    );
    assert!(
        ModuleEvents(
            host.clone(),
            "fixture".into(),
            crate::provider::MediaScope::default()
        )
        .publish("fixture", "event", serde_json::json!({}))
        .await
        .is_err()
    );
    let transcriber = ModuleTranscriber(host.clone(), "fixture".into());
    assert_eq!(transcriber.name(), "module-host");
    assert!(
        transcriber
            .transcribe(tinychannels::host::TranscriptionRequest {
                audio_base64: "YQ==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
            .is_err()
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_send_worker_reply_distinguishes_dispatch_from_queueing() {
    for dispatched in [false, true] {
        let (send, mut commands) = mpsc::channel::<SendCommand>(1);
        let (cancel, _) = watch::channel(false);
        let running = Arc::new(Running {
            media_account_scope: crate::provider::MediaScope::default(),
            channel: Arc::new(InteractionChannel::default()),
            configured_instance: "interaction-fixture:account-a".into(),
            send,
            cancel,
            worker: Mutex::new(None),
            finished: Arc::new(AtomicBool::new(false)),
            native_ports: None,
        });
        let waiter = tokio::spawn({
            let running = running.clone();
            async move { running.send(SendMessage::new("body", "room")).await }
        });
        let command = commands.recv().await.unwrap();
        if dispatched {
            command.started.send(()).unwrap();
        } else {
            drop(command.started);
        }
        drop(command.reply);
        let error = waiter.await.unwrap().unwrap_err();
        if dispatched {
            assert!(matches!(error, RunningSendError::AfterDispatch(_)));
            assert_eq!(
                error.to_string(),
                "provider send worker stopped after dispatch"
            );
        } else {
            assert!(matches!(error, RunningSendError::NotDispatched(_)));
            assert_eq!(
                error.to_string(),
                "provider send worker stopped before dispatch"
            );
        }
    }
}

#[tokio::test]
async fn configured_adapter_rejects_account_scope_mismatch_before_dispatch() {
    let (send, mut commands) = mpsc::channel::<SendCommand>(1);
    let (cancel, _) = watch::channel(false);
    let running = Arc::new(Running {
        media_account_scope: crate::provider::MediaScope::default(),
        channel: Arc::new(InteractionChannel::default()),
        configured_instance: "interaction-fixture:account-a".into(),
        send,
        cancel,
        worker: Mutex::new(None),
        finished: Arc::new(AtomicBool::new(false)),
        native_ports: None,
    });
    let adapter = running.adapter_handle();
    let error = adapter
        .send(tinychannels_bus::ChannelOutboundIntent {
            channel_id: "interaction-fixture".into(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "interaction-fixture".into(),
                account_id: Some("interaction-fixture:account-b".into()),
            }),
            conversation_id: "chat-1".into(),
            payload: tinychannels_bus::OutboundPayload::Text {
                text: "hello".into(),
            },
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, tinychannels::channel::SendErrorKind::Forbidden);
    assert!(commands.try_recv().is_err());
}

#[tokio::test]
async fn configured_adapter_preserves_partial_receipt_and_disables_retry() {
    let (send, mut commands) = mpsc::channel::<SendCommand>(1);
    let (cancel, _) = watch::channel(false);
    let running = Arc::new(Running {
        media_account_scope: crate::provider::MediaScope::default(),
        channel: Arc::new(InteractionChannel::default()),
        configured_instance: "interaction-fixture:account-a".into(),
        send,
        cancel,
        worker: Mutex::new(None),
        finished: Arc::new(AtomicBool::new(false)),
        native_ports: None,
    });
    let adapter = running.adapter_handle();
    let task = tokio::spawn(async move {
        adapter
            .send(tinychannels_bus::ChannelOutboundIntent {
                channel_id: "interaction-fixture".into(),
                channel_ref: Some(tinychannels_bus::ChannelRef {
                    id: "interaction-fixture".into(),
                    account_id: Some("interaction-fixture:account-a".into()),
                }),
                conversation_id: "chat-1".into(),
                conversation_ref: Some(tinychannels_bus::ConversationRef {
                    id: "chat-1".into(),
                    thread_id: Some("thread-2".into()),
                    ..Default::default()
                }),
                thread_id: Some("thread-2".into()),
                payload: tinychannels_bus::OutboundPayload::Text {
                    text: "hello".into(),
                },
                ..Default::default()
            })
            .await
    });
    let command = commands.recv().await.expect("adapter dispatch");
    let outcome = Box::new(tinychannels_bus::channel::ChannelSendOutcome {
        receipt: tinychannels_bus::channel::MessageReceipt {
            primary_platform_message_id: Some("msg-1".into()),
            platform_message_ids: vec!["msg-1".into()],
            ..Default::default()
        },
        error: Some(tinychannels_bus::channel::ChannelSendError {
            message: "second chunk failed".into(),
            retryable: true,
            ..Default::default()
        }),
    });
    command.started.send(()).unwrap();
    command
        .reply
        .send(Ok(
            tinychannels_bus::module_interaction::ChannelInteractionResult::Sent(outcome),
        ))
        .unwrap();
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("msg-1")
    );
    let error = outcome.error.expect("partial send error");
    assert!(error.sent_before_error);
    assert!(!error.retryable);
}

async fn send_configured_outcome(
    outcome: tinychannels_bus::channel::ChannelSendOutcome,
) -> tinychannels_bus::channel::ChannelSendOutcome {
    let (send, mut commands) = mpsc::channel::<SendCommand>(1);
    let (cancel, _) = watch::channel(false);
    let running = Arc::new(Running {
        media_account_scope: crate::provider::MediaScope::default(),
        channel: Arc::new(InteractionChannel::default()),
        configured_instance: "interaction-fixture:account-a".into(),
        send,
        cancel,
        worker: Mutex::new(None),
        finished: Arc::new(AtomicBool::new(false)),
        native_ports: None,
    });
    let adapter = running.adapter_handle();
    let task = tokio::spawn(async move {
        adapter
            .send(tinychannels_bus::ChannelOutboundIntent {
                channel_id: "interaction-fixture".into(),
                channel_ref: Some(tinychannels_bus::ChannelRef {
                    id: "interaction-fixture".into(),
                    account_id: Some("interaction-fixture:account-a".into()),
                }),
                conversation_id: "chat-1".into(),
                conversation_ref: Some(tinychannels_bus::ConversationRef {
                    id: "chat-1".into(),
                    ..Default::default()
                }),
                payload: tinychannels_bus::OutboundPayload::Text {
                    text: "hello".into(),
                },
                ..Default::default()
            })
            .await
    });
    let command = commands.recv().await.expect("adapter dispatch");
    command.started.send(()).unwrap();
    command
        .reply
        .send(Ok(
            tinychannels_bus::module_interaction::ChannelInteractionResult::Sent(Box::new(outcome)),
        ))
        .unwrap();
    task.await.unwrap().unwrap()
}

#[tokio::test]
async fn configured_adapter_disables_retry_for_continuation_only_and_unknown_outcomes() {
    let continuation_only =
        send_configured_outcome(tinychannels_bus::channel::ChannelSendOutcome {
            error: Some(tinychannels_bus::channel::ChannelSendError {
                continuation_message_ids: vec!["continuation-2".into()],
                retryable: true,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    let error = continuation_only.error.unwrap();
    assert!(error.sent_before_error);
    assert!(!error.retryable);

    let unknown_without_id =
        send_configured_outcome(tinychannels_bus::channel::ChannelSendOutcome {
            error: Some(tinychannels_bus::channel::ChannelSendError {
                kind: tinychannels::channel::SendErrorKind::Unknown,
                message: "platform outcome unknown".into(),
                retryable: true,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    let error = unknown_without_id.error.unwrap();
    assert!(!error.retryable);
}

#[test]
fn raw_voice_synthesis_requires_the_configured_native_synthesis_capability() {
    let mut capabilities = tinychannels_bus::ChannelCapabilities::default();
    capabilities.voice_out.supported = true;
    capabilities.voice_out.synthesis = false;
    let payload = tinychannels_bus::OutboundPayload::Voice {
        voice: tinychannels_bus::OutboundVoice::Synthesize {
            text: "spoken text".into(),
            voice: None,
        },
    };
    assert!(!adapter_payload_supported(&payload, &capabilities));
    capabilities.voice_out.synthesis = true;
    assert!(adapter_payload_supported(&payload, &capabilities));
}

#[tokio::test]
async fn configured_running_telegram_adapter_passes_real_mock_wire_conformance() {
    use tinychannels_conformance::{ConformanceFeature, ExpectedScope, check_feature};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let response_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let response_ids_for_mock = response_ids.clone();
    let message_contexts = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let message_contexts_for_mock = message_contexts.clone();
    let next_id = Arc::new(AtomicUsize::new(320));
    let next_id_for_mock = next_id.clone();
    Mock::given(method("POST"))
        .respond_with(move |request: &wiremock::Request| {
            let endpoint = request.url.path().rsplit('/').next().unwrap_or_default();
            let has_message = matches!(endpoint,
                "sendMessage" | "sendPhoto" | "sendVideo" | "sendAudio" | "sendVoice"
                | "sendDocument" | "sendSticker" | "sendLocation" | "sendVenue"
                | "sendContact" | "sendPoll" | "editMessageText");
            let mut result = serde_json::json!(true);
            if endpoint == "getFile" {
                result = serde_json::json!({"file_id":"voice-file","file_unique_id":"voice-unique","file_size":2575,"file_path":"voice.oga"});
            } else if has_message {
                let requested_edit_id = if endpoint == "editMessageText" {
                    serde_json::from_slice::<serde_json::Value>(&request.body).ok()
                        .and_then(|body| body.get("message_id").and_then(|value| value.as_i64()))
                        .map(|value| value.to_string())
                } else { None };
                let id = requested_edit_id.unwrap_or_else(|| next_id_for_mock.fetch_add(1, Ordering::SeqCst).to_string());
                response_ids_for_mock.lock().unwrap().push((endpoint.to_owned(), id.clone()));
                if endpoint != "editMessageText"
                    && let Ok(body) = serde_json::from_slice::<serde_json::Value>(&request.body)
                {
                        let field = |name: &str| body.get(name).and_then(|value| value.as_i64().map(|n| n.to_string()).or_else(|| value.as_str().map(str::to_owned)));
                        let chat = field("chat_id").unwrap_or_default();
                        let thread = field("message_thread_id");
                        let reply = field("reply_to_message_id").or_else(|| body.get("reply_parameters")
                            .and_then(|value| value.get("message_id"))
                            .and_then(|value| value.as_i64().map(|n| n.to_string())));
                        let context = ConversationRef { kind: ConversationKind::Group, id: chat, thread_id: thread.clone(), topic_id: thread, ..Default::default() };
                        message_contexts_for_mock.lock().unwrap().insert(id.clone(), (context, reply));
                }
                result = serde_json::json!({});
                result["message_id"] = serde_json::Value::Number(id.parse::<u64>().unwrap().into());
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true, "result": result}))
        })
        .mount(&server)
        .await;
    let updates_sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let updates_sent_once = updates_sent.clone();
    Mock::given(method("POST"))
        .and(path("/bottest-token/getUpdates"))
        .respond_with(move |_request: &wiremock::Request| {
            let result = if updates_sent_once.swap(true, Ordering::SeqCst) {
                serde_json::json!([])
            } else {
                serde_json::json!([
                    {"update_id":701,"message":{"message_id":91,"message_thread_id":77,"date":1720000000,"chat":{"id":-100,"type":"supergroup"},"from":{"id":42,"username":"alice"},"voice":{"file_id":"voice-file","file_unique_id":"voice-unique","duration":1,"mime_type":"audio/ogg","file_size":2575}}},
                    {"update_id":702,"message_reaction":{"chat":{"id":-100,"type":"supergroup"},"message_id":92,"date":1720000001,"user":{"id":42,"username":"alice"},"old_reaction":[],"new_reaction":[{"type":"emoji","emoji":"🔥"}]}}
                ])
            };
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok":true,"result":result}))
        })
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file/bottest-token/voice.oga"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(
            include_bytes!("../../../src/providers/telegram/fixtures/sine-ogg-opus.ogg").to_vec(),
        ))
        .mount(&server)
        .await;
    let instance = "telegram:account-a".to_owned();
    let hooks = Arc::new(Hooks::default());
    let (host, callbacks, _server_connection, _storage) = ingress_fixture().await;
    *callbacks.transcription_result.lock().await = Some("fixture spoken words".into());
    let provider_host = hooks.host(
        host.clone(),
        instance.clone(),
        Some("telegram:media-v1:test".into()),
    );
    let config = tinychannels_bus::ChannelsConfig {
        telegram: Some(tinychannels_bus::config::TelegramConfig {
            bot_token: "test-token".into(),
            chat_id: None,
            allowed_users: vec!["*".into()],
            stream_mode: tinychannels_bus::config::StreamMode::Partial,
            draft_update_interval_ms: 1000,
            silent_streaming: true,
            mention_only: false,
        }),
        ..Default::default()
    };
    let built = tinychannels::build_telegram_channel(
        &config,
        &provider_host,
        &tinychannels::DefaultHttpClients,
    )
    .expect("configured Telegram adapter exists");
    let channel = Arc::new(
        Arc::try_unwrap(built)
            .ok()
            .expect("factory channel has no outside owners")
            .with_api_base_for_test(server.uri()),
    );
    let running = Running::start_telegram(channel, hooks, host, None, false);
    let handle = running.adapter_handle();
    let repository = provider_host
        .media_repository()
        .expect("configured Telegram host media repository");
    let media_scope = tinychannels_bus::HostVoiceScope {
        configured_instance: "telegram:account-a".into(),
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram:account-a".into()),
        },
        conversation: ConversationRef {
            kind: ConversationKind::Group,
            id: "-100".into(),
            thread_id: Some("77".into()),
            topic_id: Some("77".into()),
            ..Default::default()
        },
        turn_id: "turn-kit".into(),
    };
    let png = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=").unwrap();
    let media_inputs: [(
        ConformanceFeature,
        tinychannels_bus::MediaKind,
        &str,
        &str,
        &[u8],
    ); 6] = [
        (
            ConformanceFeature::Image,
            tinychannels_bus::MediaKind::Image,
            "image/png",
            "fixture.png",
            &png,
        ),
        (
            ConformanceFeature::Video,
            tinychannels_bus::MediaKind::Video,
            "video/mp4",
            "fixture.mp4",
            include_bytes!("../../../src/providers/telegram/fixtures/cc0-flower.mp4"),
        ),
        (
            ConformanceFeature::Audio,
            tinychannels_bus::MediaKind::Audio,
            "audio/mpeg",
            "fixture.mp3",
            include_bytes!("../../../src/providers/telegram/fixtures/cc0-mp3-test.mp3"),
        ),
        (
            ConformanceFeature::VoiceOut,
            tinychannels_bus::MediaKind::Audio,
            "audio/ogg",
            "voice.ogg",
            include_bytes!("../../../src/providers/telegram/fixtures/sine-ogg-opus.ogg"),
        ),
        (
            ConformanceFeature::File,
            tinychannels_bus::MediaKind::Document,
            "text/plain",
            "fixture.txt",
            b"Telegram module conformance document\n",
        ),
        (
            ConformanceFeature::Sticker,
            tinychannels_bus::MediaKind::Image,
            "image/webp",
            "sticker-512.webp",
            include_bytes!("../../../src/providers/telegram/fixtures/sticker-512.webp"),
        ),
    ];
    let mut media = std::collections::HashMap::new();
    for (feature, kind, content_type, file_name, bytes) in media_inputs {
        let reference = repository
            .store_media(tinychannels::host::MediaStoreRequest {
                metadata: tinychannels::host::MediaMetadata {
                    scope: media_scope.clone(),
                    kind,
                    content_type: content_type.into(),
                    file_name: Some(file_name.into()),
                    duration_ms: Some(200),
                    codec: Some(if content_type == "audio/ogg" {
                        "ogg_opus".into()
                    } else {
                        String::new()
                    }),
                    voice_direction: None,
                    dedupe_key: None,
                    max_bytes: 20 * 1024 * 1024,
                },
                bytes: bytes.to_vec(),
            })
            .await
            .unwrap_or_else(|error| {
                panic!("store scoped {feature:?} conformance fixture: {error}")
            });
        media.insert(feature, reference);
    }
    let fixture = RunningTelegramConformance {
        adapter: handle,
        running: running.clone(),
        server,
        scope: ExpectedScope {
            channel: tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: Some(instance),
            },
            conversation: ConversationRef {
                kind: ConversationKind::Group,
                id: "-100".into(),
                thread_id: Some("77".into()),
                topic_id: Some("77".into()),
                ..Default::default()
            },
            reply_to_id: Some("5".into()),
            thread_id: Some("77".into()),
        },
        observed: std::sync::Mutex::new(Vec::new()),
        request_cursor: std::sync::Mutex::new(0),
        response_ids,
        media,
        callbacks,
        provider_host,
        response_cursor: std::sync::Mutex::new(0),
        message_contexts,
    };
    let descriptor = fixture.adapter.descriptor();
    assert_eq!(descriptor.id, "telegram");
    assert_eq!(descriptor.account_id.as_deref(), Some("telegram:account-a"));
    assert_eq!(
        descriptor.metadata.get("adapterMode").map(String::as_str),
        Some("native-v2")
    );
    assert_eq!(fixture.adapter.mode, AdapterMode::TelegramNativeV2);
    let mut results = Vec::new();
    for feature in ConformanceFeature::ALL {
        let result = check_feature(&fixture, feature).await;
        println!(
            "{feature:?}: {result:?}; observations={:?}",
            fixture.observations()
        );
        results.push((feature, result));
    }
    for (feature, result) in &results {
        assert!(
            result.is_ok(),
            "{feature:?}: {}",
            result.as_ref().unwrap_err()
        );
    }
    assert_eq!(results.len(), ConformanceFeature::ALL.len());
    running.stop().await.unwrap();
}

#[tokio::test]
async fn configured_running_telegram_adapter_uploads_media_from_host_storage() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendPhoto"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true,
            "result": { "message_id": 908 }
        })))
        .mount(&server)
        .await;
    let (host, _callbacks, _bus_server, _storage) = ingress_fixture().await;
    let instance = "telegram:media-account".to_owned();
    let hooks = Arc::new(Hooks::default());
    let provider_host = hooks.host(
        host.clone(),
        instance.clone(),
        Some("telegram:media-v1:test".into()),
    );
    let repository = provider_host.media_repository().unwrap();
    let conversation = ConversationRef {
        kind: ConversationKind::Group,
        id: "-10099".into(),
        scope_id: Some("scope-99".into()),
        parent_id: Some("parent-99".into()),
        thread_id: Some("31".into()),
        topic_id: Some("topic-31".into()),
    };
    let scope = tinychannels_bus::HostVoiceScope {
        configured_instance: instance.clone(),
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some(instance.clone()),
        },
        conversation: conversation.clone(),
        turn_id: "turn-908".into(),
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
        .unwrap();
    let media = repository
        .store_media(tinychannels::host::MediaStoreRequest {
            metadata: tinychannels::host::MediaMetadata {
                scope,
                kind: tinychannels_bus::MediaKind::Image,
                content_type: "image/png".into(),
                file_name: Some("host-owned.png".into()),
                duration_ms: None,
                codec: None,
                voice_direction: None,
                dedupe_key: None,
                max_bytes: 20 * 1024 * 1024,
            },
            bytes: bytes.clone(),
        })
        .await
        .unwrap();
    let channel = Arc::new(
        tinychannels::TelegramChannel::new("test-token".into(), vec!["*".into()], false)
            .with_api_base_for_test(server.uri())
            .with_media_repository(repository.clone()),
    );
    let running = Running::start_outbound(channel, hooks, host);
    let adapter = running.adapter_handle();
    let result = adapter
        .send(tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "media-upload-908".into(),
            channel_id: "telegram".into(),
            conversation_id: conversation.id.clone(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: Some(instance.clone()),
            }),
            conversation_ref: Some(conversation.clone()),
            reply_to_id: Some("19".into()),
            thread_id: Some("31".into()),
            turn_id: Some("turn-908".into()),
            payload: tinychannels_bus::OutboundPayload::Parts {
                parts: vec![tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Image {
                        media: media.clone(),
                        caption: Some("module path".into()),
                        alt_text: None,
                    },
                }],
            },
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.receipt.platform_message_ids, ["908"]);
    assert!(result.error.is_none());
    let stored = repository
        .read_media(
            tinychannels_bus::HostVoiceScope {
                configured_instance: instance,
                channel: tinychannels_bus::ChannelRef {
                    id: "telegram".into(),
                    account_id: Some("telegram:media-account".into()),
                },
                conversation,
                turn_id: "turn-908".into(),
            },
            media.opaque_id.unwrap(),
            20 * 1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(stored.bytes, bytes);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let body = String::from_utf8_lossy(&requests[0].body);
    assert!(body.contains("name=\"chat_id\"\r\n\r\n-10099"));
    assert!(body.contains("name=\"message_thread_id\"\r\n\r\n31"));
    assert!(body.contains("name=\"reply_parameters\"\r\n\r\n{\"message_id\":19}"));
    assert!(body.contains("host-owned.png"));
    assert!(
        requests[0]
            .body
            .windows(bytes.len())
            .any(|window| window == bytes)
    );
    running.stop().await.unwrap();
}

#[tokio::test]
async fn configured_adapter_marks_first_unknown_media_upload_non_retryable() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendPhoto"))
        .respond_with(ResponseTemplate::new(503).set_body_string("uncertain provider response"))
        .mount(&server)
        .await;
    let (host, _callbacks, _bus_server, _storage) = ingress_fixture().await;
    let instance = "telegram:media-account".to_owned();
    let hooks = Arc::new(Hooks::default());
    let provider_host = hooks.host(
        host.clone(),
        instance.clone(),
        Some("telegram:media-v1:test".into()),
    );
    let repository = provider_host.media_repository().unwrap();
    let conversation = ConversationRef {
        kind: ConversationKind::Group,
        id: "-10099".into(),
        thread_id: Some("31".into()),
        ..Default::default()
    };
    let media = repository
        .store_media(tinychannels::host::MediaStoreRequest {
            metadata: tinychannels::host::MediaMetadata {
                scope: tinychannels_bus::HostVoiceScope {
                    configured_instance: instance.clone(),
                    channel: tinychannels_bus::ChannelRef {
                        id: "telegram".into(),
                        account_id: Some(instance.clone()),
                    },
                    conversation: conversation.clone(),
                    turn_id: "turn-503".into(),
                },
                kind: tinychannels_bus::MediaKind::Image,
                content_type: "image/png".into(),
                file_name: Some("uncertain.png".into()),
                duration_ms: None,
                codec: None,
                voice_direction: None,
                dedupe_key: None,
                max_bytes: 20 * 1024 * 1024,
            },
            bytes: base64::engine::general_purpose::STANDARD
                .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
                .unwrap(),
        })
        .await
        .unwrap();
    let channel = Arc::new(
        tinychannels::TelegramChannel::new("test-token".into(), vec!["*".into()], false)
            .with_api_base_for_test(server.uri())
            .with_media_repository(repository),
    );
    let running = Running::start_outbound(channel, hooks, host);
    let adapter = running.adapter_handle();
    let error = adapter
        .send(tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "uncertain-media-send".into(),
            channel_id: "telegram".into(),
            conversation_id: conversation.id.clone(),
            channel_ref: Some(tinychannels_bus::ChannelRef {
                id: "telegram".into(),
                account_id: Some(instance),
            }),
            conversation_ref: Some(conversation),
            turn_id: Some("turn-503".into()),
            payload: tinychannels_bus::OutboundPayload::Parts {
                parts: vec![tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Image {
                        media,
                        caption: None,
                        alt_text: None,
                    },
                }],
            },
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    assert!(!error.unsupported);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    running.stop().await.unwrap();
}

#[tokio::test]
async fn configured_telegram_native_optional_ports_share_ordered_queue_and_reject_scope_mismatch() {
    use tinychannels_bus::{ChannelInteraction, ChannelOutboundIntent, OutboundPayload};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    for endpoint in [
        "sendMessage",
        "editMessageText",
        "deleteMessage",
        "setMessageReaction",
    ] {
        let result = if endpoint == "deleteMessage" || endpoint == "setMessageReaction" {
            serde_json::json!(true)
        } else {
            serde_json::json!({ "message_id": 321, "chat": {"id": -100} })
        };
        Mock::given(method("POST"))
            .and(path(format!("/bottest-token/{endpoint}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ok":true,"result":result})),
            )
            .mount(&server)
            .await;
    }
    let instance = "telegram:account-a".to_owned();
    let channel = Arc::new(
        tinychannels::TelegramChannel::new("test-token".into(), vec!["*".into()], false)
            .with_api_base_for_test(server.uri()),
    );
    let hooks = Arc::new(Hooks::default());
    let (host, _callbacks, _server_connection, _storage) = ingress_fixture().await;
    let _provider_host = hooks.host(host.clone(), instance.clone(), None);
    let running = Running::start_telegram(channel, hooks, host, None, true);
    let handle = running.adapter_handle();
    assert_eq!(handle.mode, AdapterMode::TelegramNativeV2);

    let channel_ref = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some(instance.clone()),
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Group,
        id: "-100".into(),
        topic_id: Some("77".into()),
        ..Default::default()
    };
    let intent = ChannelOutboundIntent {
        idempotency_key: "native-op".into(),
        channel_id: "telegram".into(),
        conversation_id: conversation.id.clone(),
        channel_ref: Some(channel_ref.clone()),
        conversation_ref: Some(conversation.clone()),
        payload: OutboundPayload::Text {
            text: "native send".into(),
        },
        ..Default::default()
    };
    let sent = handle.send(intent.clone()).await.unwrap();
    assert_eq!(
        sent.receipt.primary_platform_message_id.as_deref(),
        Some("321")
    );
    assert_eq!(sent.receipt.conversation.as_ref(), Some(&conversation));

    handle
        .reactions()
        .unwrap()
        .send_reaction(
            &channel_ref,
            &conversation,
            "321",
            &tinychannels_bus::ChannelReaction {
                value: "👍".into(),
                ..Default::default()
            },
            false,
        )
        .await
        .unwrap();
    let receipt = sent.receipt.clone();
    let edited = handle
        .edits()
        .unwrap()
        .edit(&receipt, intent.clone())
        .await
        .unwrap();
    assert_eq!(edited.conversation.as_ref(), Some(&conversation));
    handle.deletes().unwrap().delete(&receipt).await.unwrap();

    let before = server.received_requests().await.unwrap().len();
    let mut wrong_scope = receipt.clone();
    wrong_scope.conversation.as_mut().unwrap().topic_id = Some("other-topic".into());
    let invalid = handle
        .edits()
        .unwrap()
        .edit(&wrong_scope, intent.clone())
        .await;
    assert!(invalid.is_err());
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        before,
        "mismatched receipt must be rejected before provider I/O"
    );

    let draft = running
        .interact(ChannelInteraction::UpsertDraft {
            previous: None,
            intent: intent.clone(),
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::Sent(draft) = draft else {
        panic!("native draft returned no receipt");
    };
    assert_eq!(
        draft.receipt.primary_platform_message_id.as_deref(),
        Some("321")
    );
    let before_bad_draft = server.received_requests().await.unwrap().len();
    let bad = running
        .interact(ChannelInteraction::UpsertDraft {
            previous: Some(wrong_scope),
            intent,
        })
        .await;
    assert!(bad.is_err());
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        before_bad_draft,
        "mismatched prior draft scope must be rejected before provider I/O"
    );
    running.stop().await.unwrap();
}

#[tokio::test]
async fn configured_telegram_callback_ingress_enforces_allowlist_and_preserves_scope() {
    use tinychannels_bus::ChannelEventKind;
    use tinychannels_bus::channel::parse_approval_action_id;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let updates = serde_json::json!([
        {
            "update_id": 501,
            "callback_query": {
                "id": "spoofed-display-name", "data": "opaque-spoof",
                "from": {"id": 99, "first_name": "alice"},
                "message": {"message_id": 80, "message_thread_id": 77,
                    "chat": {"id": -100, "type": "supergroup"}}
            }
        },
        {
            "update_id": 502,
            "callback_query": {
                "id": "allowed-id-query", "data": "approval-v1:request-42:0",
                "from": {"id": 42, "username": "outsider"},
                "message": {"message_id": 81, "message_thread_id": 77,
                    "chat": {"id": -100, "type": "supergroup"}}
            }
        },
        {
            "update_id": 503,
            "callback_query": {
                "id": "allowed-username-query", "data": "approval-v1:request-42:1",
                "from": {"id": 43, "username": "alice"},
                "message": {"message_id": 82,
                    "chat": {"id": 100, "type": "private"}}
            }
        },
        {
            "update_id": 504,
            "message": {"message_id": 83, "date": 1700000000,
                "chat": {"id": -100, "type": "supergroup"},
                "from": {"id": 42, "username": "outsider"}, "text": "ingress frontier"}
        }
    ]);
    Mock::given(method("POST"))
        .and(path("/bottest-token/getUpdates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true, "result": updates
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/getUpdates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true, "result": []
        })))
        .with_priority(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/answerCallbackQuery"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true, "result": true
        })))
        .mount(&server)
        .await;

    let instance = "telegram:configured-callback-account".to_owned();
    let hooks = Arc::new(Hooks::default());
    let (host, callback, _server_connection, _storage) = ingress_fixture().await;
    let provider_host = hooks.host(host.clone(), instance.clone(), None);
    let config = tinychannels_bus::ChannelsConfig {
        telegram: Some(tinychannels_bus::config::TelegramConfig {
            bot_token: "test-token".into(),
            chat_id: None,
            allowed_users: vec!["alice".into(), "42".into()],
            stream_mode: Default::default(),
            draft_update_interval_ms: 1000,
            silent_streaming: true,
            mention_only: false,
        }),
        ..Default::default()
    };
    let built = tinychannels::build_telegram_channel(
        &config,
        &provider_host,
        &tinychannels::DefaultHttpClients,
    )
    .expect("factory builds configured Telegram channel");
    let channel = Arc::new(
        Arc::try_unwrap(built)
            .ok()
            .expect("factory channel has no outside owners")
            .with_api_base_for_test(server.uri()),
    );
    let running = Running::start_telegram(channel, hooks, host, None, false);

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let frontier_seen = callback
                .inbound
                .lock()
                .await
                .iter()
                .any(|(_, message)| message.to_string().contains("ingress frontier"));
            if frontier_seen {
                break;
            }
            callback.ingress_changed.notified().await;
        }
    })
    .await
    .expect("ordered message after callback batch establishes ingress frontier");

    let events = callback.events.lock().await.clone();
    let callbacks = events
        .iter()
        .filter_map(|event| match &event.event {
            ChannelEventKind::ButtonCallback {
                query_id,
                action_id,
                provider_message_id,
                actor,
            } => Some((event, query_id, action_id, provider_message_id, actor)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        callbacks.len(),
        2,
        "spoofed first_name must not authorize ingress"
    );
    let (by_id, query_id, action_id, provider_message_id, actor) = callbacks[0];
    assert_eq!(query_id, "allowed-id-query");
    assert_eq!(action_id, "approval-v1:request-42:0");
    assert_eq!(parse_approval_action_id(action_id), Some(("request-42", 0)));
    assert_eq!(provider_message_id.as_deref(), Some("81"));
    assert_eq!(actor.id, "42");
    assert_eq!(actor.name.as_deref(), Some("outsider"));
    assert_eq!(by_id.channel.account_id.as_deref(), Some(instance.as_str()));
    assert_eq!(by_id.channel.id, "telegram");
    assert_eq!(by_id.conversation.as_ref().unwrap().id, "-100");
    assert_eq!(
        by_id.conversation.as_ref().unwrap().topic_id.as_deref(),
        Some("77")
    );
    let (by_username, query_id, action_id, provider_message_id, actor) = callbacks[1];
    assert_eq!(query_id, "allowed-username-query");
    assert_eq!(action_id, "approval-v1:request-42:1");
    assert_eq!(parse_approval_action_id(action_id), Some(("request-42", 1)));
    assert_eq!(provider_message_id.as_deref(), Some("82"));
    assert_eq!(actor.id, "43");
    assert_eq!(actor.name.as_deref(), Some("alice"));
    assert_eq!(by_username.channel.id, "telegram");
    assert_eq!(
        by_username.channel.account_id.as_deref(),
        Some(instance.as_str())
    );
    assert_eq!(by_username.conversation.as_ref().unwrap().id, "100");
    assert_eq!(by_username.conversation.as_ref().unwrap().topic_id, None);

    let acknowledgments = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path() == "/bottest-token/answerCallbackQuery")
        .map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).unwrap())
        .map(|body| body["callback_query_id"].as_str().unwrap().to_owned())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        acknowledgments,
        [
            "spoofed-display-name".to_owned(),
            "allowed-id-query".to_owned(),
            "allowed-username-query".to_owned(),
        ]
        .into_iter()
        .collect()
    );
    running.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// WhatsApp: media authority and the configured native adapters
// ---------------------------------------------------------------------------

struct IdentityChannel(std::sync::Mutex<Option<String>>);

#[async_trait::async_trait]
impl Channel for IdentityChannel {
    fn name(&self) -> &str {
        "whatsapp"
    }
    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }
    fn media_account_identity(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
}

#[test]
fn a_fixed_media_scope_is_returned_as_configured_and_never_printed() {
    assert_eq!(MediaScope::default().current(), None);
    let fixed = MediaScope::fixed(Some("telegram:media-v1:abc".into()));
    assert_eq!(fixed.current().as_deref(), Some("telegram:media-v1:abc"));
    assert_eq!(MediaScope::from(None).current(), None);
    assert_eq!(
        MediaScope::from(Some("x".into())).current().as_deref(),
        Some("x")
    );
    assert_eq!(format!("{fixed:?}"), "MediaScope(..)");
}

#[test]
fn a_linked_session_binds_the_media_scope_only_once_it_knows_its_account() {
    let channel = Arc::new(IdentityChannel(std::sync::Mutex::new(None)));
    let scope = MediaScope::fixed(None);
    scope.bind_source(channel.clone());
    // Linking has not happened: media fails closed.
    assert_eq!(scope.current(), None);
    *channel.0.lock().unwrap() = Some("15550001111@s.whatsapp.net".into());
    let first = scope.current().expect("scope after linking");
    assert!(first.starts_with("whatsapp:media-v1:"));
    // The scope is derived from the account, so another account is another scope,
    // and the same account always gets the same one.
    assert_eq!(scope.current().as_deref(), Some(first.as_str()));
    let other = MediaScope::fixed(None);
    other.bind_source(Arc::new(IdentityChannel(std::sync::Mutex::new(Some(
        "15550002222@s.whatsapp.net".into(),
    )))));
    assert_ne!(other.current().unwrap(), first);
    // If the provider withdraws the identity (a re-link to another account),
    // every later use fails closed again.
    *channel.0.lock().unwrap() = None;
    assert_eq!(scope.current(), None);
}

#[test]
fn a_configured_scope_wins_over_any_bound_source() {
    let scope = MediaScope::fixed(Some("fixed".into()));
    scope.bind_source(Arc::new(IdentityChannel(std::sync::Mutex::new(Some(
        "someone".into(),
    )))));
    assert_eq!(scope.current().as_deref(), Some("fixed"));
}

mod whatsapp_native {
    use super::*;
    use hmac::{Hmac, KeyInit, Mac};
    use tinychannels_bus::{ChannelInteraction, WebhookDeliveryOutcome};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const SECRET: &str = "module-secret";

    fn cloud_config() -> tinychannels_bus::ChannelsConfig {
        tinychannels_bus::ChannelsConfig {
            whatsapp: Some(tinychannels_bus::config::WhatsAppConfig {
                access_token: Some("access-token".into()),
                phone_number_id: Some("123".into()),
                verify_token: Some("verify-me".into()),
                app_secret: Some(SECRET.into()),
                session_path: None,
                pair_phone: None,
                pair_code: None,
                allowed_numbers: vec!["+15550001111".into()],
            }),
            ..Default::default()
        }
    }

    fn signed(body: &[u8]) -> String {
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn delivery(messages: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "object": "whatsapp_business_account",
            "entry": [{"changes": [{"field": "messages", "value": {
                "metadata": {"phone_number_id": "123"},
                "messages": messages,
            }}]}]
        }))
        .unwrap()
    }

    struct Rig {
        running: Arc<Running>,
        callbacks: IngressVoiceCallback,
        server: MockServer,
        _storage: crate::storage_fixture::StorageHost,
        _connection: tinybus::Connection,
    }

    async fn rig() -> Rig {
        let server = MockServer::start().await;
        let instance = "whatsapp:account-a".to_owned();
        let hooks = Arc::new(Hooks::default());
        let (host, callbacks, connection, storage) = ingress_fixture().await;
        *callbacks.transcription_result.lock().await = Some("fixture spoken words".into());
        let provider_host = hooks.host(
            host.clone(),
            instance,
            Some("whatsapp:media-v1:test".into()),
        );
        let built = tinychannels::build_whatsapp_cloud_channel(
            &cloud_config(),
            &provider_host,
            &tinychannels::DefaultHttpClients,
        )
        .expect("a complete Cloud config builds");
        let channel = Arc::new(
            Arc::try_unwrap(built)
                .ok()
                .expect("factory channel has no outside owners")
                .with_api_base_for_test(&server.uri()),
        );
        let running = Running::start_native("WhatsApp", channel, hooks, host, None, true);
        Rig {
            running,
            callbacks,
            server,
            _storage: storage,
            _connection: connection,
        }
    }

    /// Host callbacks are delivered asynchronously; wait for the first one.
    async fn first_inbound(rig: &Rig) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some((_, envelope)) = rig.callbacks.inbound.lock().await.first() {
                    return envelope.clone();
                }
                rig.callbacks.ingress_changed.notified().await;
            }
        })
        .await
        .expect("the host received the inbound message")
    }

    async fn webhook(
        rig: &Rig,
        signature: Option<String>,
        body: &[u8],
    ) -> tinybus::Result<tinychannels_bus::ChannelInteractionResult> {
        use base64::Engine as _;
        rig.running
            .webhook(ChannelInteraction::DeliverWebhook {
                signature,
                body_base64: base64::engine::general_purpose::STANDARD.encode(body),
            })
            .await
    }

    #[tokio::test]
    async fn the_configured_adapter_is_native_and_scoped_to_its_instance() {
        let rig = rig().await;
        let handle = rig.running.adapter_handle();
        assert_eq!(handle.mode, AdapterMode::WhatsAppNativeV2);
        let descriptor = handle.descriptor();
        assert_eq!(descriptor.id, "whatsapp");
        assert_eq!(descriptor.account_id.as_deref(), Some("whatsapp:account-a"));
        assert_eq!(
            descriptor.metadata.get("adapterMode").map(String::as_str),
            Some("native-v2")
        );
        let facts = handle.declared_capabilities();
        assert!(facts.reactions && facts.content.image.outbound);
        assert!(
            handle.reactions().is_some() && handle.edits().is_some() && handle.deletes().is_some()
        );
        rig.running.stop().await.unwrap();
    }

    #[tokio::test]
    async fn a_signed_delivery_reaches_the_host_bound_to_the_configured_account() {
        let rig = rig().await;
        let body = delivery(serde_json::json!([
            {"from": "15550001111", "id": "wamid.1", "timestamp": "1700000000", "type": "text", "text": {"body": "hello module"}}
        ]));
        let result = webhook(&rig, Some(signed(&body)), &body).await.unwrap();
        assert!(matches!(
            result,
            tinychannels_bus::ChannelInteractionResult::WebhookAccepted(WebhookDeliveryOutcome {
                messages: 1,
                ..
            })
        ));
        let envelope = first_inbound(&rig).await;
        assert_eq!(rig.callbacks.inbound.lock().await.len(), 1);
        // The module, not the payload, decides which account owns the message.
        assert_eq!(envelope["channel"]["id"], "whatsapp");
        assert_eq!(envelope["channel"]["accountId"], "whatsapp:account-a");
        assert_eq!(envelope["messageId"], "wamid.1");
        assert_eq!(envelope["text"], "hello module");
        rig.running.stop().await.unwrap();
    }

    #[tokio::test]
    async fn unsigned_forged_and_misaddressed_deliveries_never_reach_the_host() {
        let rig = rig().await;
        let body = delivery(serde_json::json!([
            {"from": "15550001111", "id": "wamid.9", "type": "text", "text": {"body": "forged"}}
        ]));
        for signature in [
            None,
            Some("sha256=00".to_owned()),
            Some("verify-me".to_owned()),
        ] {
            let error = webhook(&rig, signature, &body).await.unwrap_err();
            assert!(matches!(
                error,
                tinybus::Error::MethodFailed { name, .. } if name == crate::interaction::WEBHOOK_REJECTED_ERROR
            ));
        }
        let other_number = serde_json::to_vec(&serde_json::json!({
            "object": "whatsapp_business_account",
            "entry": [{"changes": [{"field": "messages", "value": {
                "metadata": {"phone_number_id": "999"},
                "messages": [{"from": "15550001111", "id": "wamid.8", "type": "text", "text": {"body": "x"}}]
            }}]}]
        }))
        .unwrap();
        assert!(
            webhook(&rig, Some(signed(&other_number)), &other_number)
                .await
                .is_err()
        );
        assert!(rig.callbacks.inbound.lock().await.is_empty());
        rig.running.stop().await.unwrap();
    }

    #[tokio::test]
    async fn the_subscription_handshake_is_answered_by_the_module() {
        let rig = rig().await;
        let verify = |token: &str| {
            rig.running.webhook(ChannelInteraction::VerifyWebhook {
                mode: "subscribe".into(),
                verify_token: token.into(),
                challenge: "c-77".into(),
            })
        };
        assert!(matches!(
            verify("verify-me").await.unwrap(),
            tinychannels_bus::ChannelInteractionResult::WebhookChallenge(challenge) if challenge == "c-77"
        ));
        assert!(verify("guess").await.is_err());
        rig.running.stop().await.unwrap();
    }

    #[tokio::test]
    async fn an_inbound_voice_note_is_stored_then_transcribed_by_the_module() {
        let rig = rig().await;
        Mock::given(method("GET"))
            .and(path("/v23.0/ptt-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "url": format!("{}/cdn/ptt-1", rig.server.uri()),
                "mime_type": "audio/ogg; codecs=opus"
            })))
            .mount(&rig.server)
            .await;
        Mock::given(method("GET"))
            .and(path("/cdn/ptt-1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(
                    include_bytes!("../../../src/providers/telegram/fixtures/sine-ogg-opus.ogg")
                        .to_vec(),
                ),
            )
            .mount(&rig.server)
            .await;
        let body = delivery(serde_json::json!([
            {"from": "15550001111", "id": "wamid.voice", "timestamp": "1700000001", "type": "audio",
             "audio": {"id": "ptt-1", "voice": true, "mime_type": "audio/ogg; codecs=opus"}}
        ]));
        webhook(&rig, Some(signed(&body)), &body).await.unwrap();
        let envelope = first_inbound(&rig).await;
        let note = envelope["parts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|part| part["kind"] == "voice_note")
            .expect("a voice note part");
        assert_eq!(note["voiceNote"]["pushToTalk"], true);
        assert_eq!(
            note["voiceNote"]["transcript"]["text"],
            "fixture spoken words"
        );
        assert_eq!(note["voiceNote"]["transcript"]["source"], "host_stt");
        assert!(
            note["voiceNote"]["media"]
                .get("url")
                .is_none_or(serde_json::Value::is_null)
        );
        assert_eq!(rig.callbacks.stt_calls.load(Ordering::SeqCst), 1);
        rig.running.stop().await.unwrap();
    }

    #[tokio::test]
    async fn reactions_read_receipts_and_typing_share_the_ordered_queue_and_scope() {
        let rig = rig().await;
        Mock::given(method("POST"))
            .and(path("/v23.0/123/messages"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"messages": [{"id": "wamid.ok"}]})),
            )
            .mount(&rig.server)
            .await;
        let channel = tinychannels_bus::ChannelRef {
            id: "whatsapp".into(),
            account_id: Some("whatsapp:account-a".into()),
        };
        let conversation = ConversationRef {
            id: "+15550001111".into(),
            kind: ConversationKind::Dm,
            ..Default::default()
        };
        rig.running
            .interact(ChannelInteraction::SendReaction {
                channel: channel.clone(),
                conversation: conversation.clone(),
                provider_message_id: "wamid.in".into(),
                reaction: tinychannels_bus::ChannelReaction {
                    value: "👍".into(),
                    custom_emoji_id: None,
                },
                remove: false,
            })
            .await
            .unwrap();
        rig.running
            .interact(ChannelInteraction::MarkRead {
                channel: channel.clone(),
                conversation: conversation.clone(),
                provider_message_id: "wamid.in".into(),
            })
            .await
            .unwrap();
        // A receipt for another account's scope is refused before dispatch.
        let foreign = rig
            .running
            .interact(ChannelInteraction::MarkRead {
                channel: tinychannels_bus::ChannelRef {
                    id: "whatsapp".into(),
                    account_id: Some("whatsapp:other".into()),
                },
                conversation,
                provider_message_id: "wamid.in".into(),
            })
            .await
            .unwrap_err();
        assert!(matches!(foreign, RunningSendError::NotDispatched(_)));
        let bodies: Vec<serde_json::Value> = rig
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| serde_json::from_slice(&request.body).unwrap())
            .collect();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0]["type"], "reaction");
        assert_eq!(bodies[1]["status"], "read");
        rig.running.stop().await.unwrap();
    }
}
#[test]
fn media_scope_rejects_blank_secrets_and_distinguishes_credential_modes() {
    use tinychannels_bus::config::*;
    let webhook = |secret: &str| tinychannels_bus::ChannelsConfig {
        webhook: Some(WebhookConfig {
            port: 8080,
            secret: Some(secret.into()),
        }),
        ..Default::default()
    };
    assert!(configured_media_account_scope("webhook", &webhook("  ")).is_none());
    assert!(configured_media_account_scope("webhook", &webhook("s")).is_some());

    let whatsapp = |phone: &str, token: &str| tinychannels_bus::ChannelsConfig {
        whatsapp: Some(WhatsAppConfig {
            access_token: Some(token.into()),
            phone_number_id: Some(phone.into()),
            verify_token: None,
            app_secret: None,
            session_path: None,
            pair_phone: None,
            pair_code: None,
            allowed_numbers: vec![],
        }),
        ..Default::default()
    };
    assert!(configured_media_account_scope("whatsapp", &whatsapp(" ", "t")).is_none());
    assert!(configured_media_account_scope("whatsapp", &whatsapp("p", " ")).is_none());

    let irc = |sasl: Option<&str>, nickserv: Option<&str>| tinychannels_bus::ChannelsConfig {
        irc: Some(IrcConfig {
            server: "irc.example".into(),
            port: 6697,
            nickname: "bot".into(),
            username: Some("auth-user".into()),
            channels: vec![],
            allowed_users: vec![],
            server_password: None,
            nickserv_password: nickserv.map(str::to_owned),
            sasl_password: sasl.map(str::to_owned),
            verify_tls: Some(true),
        }),
        ..Default::default()
    };
    let sasl = configured_media_account_scope("irc", &irc(Some("pw"), None)).unwrap();
    let nickserv = configured_media_account_scope("irc", &irc(None, Some("pw"))).unwrap();
    assert_ne!(
        sasl, nickserv,
        "principal differs between SASL and NickServ"
    );
    assert!(configured_media_account_scope("irc", &irc(Some(" "), None)).is_none());

    let lark = |feishu: bool| tinychannels_bus::ChannelsConfig {
        lark: Some(LarkConfig {
            app_id: "app".into(),
            app_secret: "secret".into(),
            encrypt_key: None,
            verification_token: None,
            allowed_users: vec![],
            use_feishu: feishu,
            receive_mode: Default::default(),
            port: None,
        }),
        ..Default::default()
    };
    assert_ne!(
        configured_media_account_scope("lark", &lark(true)),
        configured_media_account_scope("lark", &lark(false)),
        "Feishu and Lark are distinct account authorities"
    );
}

#[tokio::test]
async fn legacy_running_adapter_reports_status_and_refuses_second_start() {
    let (send, _commands) = mpsc::channel::<SendCommand>(1);
    let (cancel, _) = watch::channel(false);
    let running = Arc::new(Running {
        media_account_scope: MediaScope::default(),
        channel: Arc::new(InteractionChannel::default()),
        configured_instance: "interaction-fixture:account-a".into(),
        send,
        cancel,
        worker: Mutex::new(None),
        finished: Arc::new(AtomicBool::new(false)),
        native_ports: None,
    });
    struct NullSink;
    #[async_trait::async_trait]
    impl tinychannels::channel::ChannelInboundSink for NullSink {
        async fn push(
            &self,
            _event: tinychannels_bus::ChannelIngress,
        ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
            Ok(())
        }
    }
    let adapter: &dyn tinychannels::channel::ChannelAdapter = running.as_ref();
    assert_eq!(adapter.proactive_target(), None);
    let error = adapter.start(&NullSink).await.unwrap_err();
    assert_eq!(error.kind, tinychannels::channel::SendErrorKind::Forbidden);
    let status = adapter.status().await.unwrap();
    assert_eq!(
        status.account_id.as_deref(),
        Some("interaction-fixture:account-a")
    );
    adapter.stop().await.unwrap();
}

#[tokio::test]
async fn configured_telegram_native_adapter_owns_polling_typing_drafts_and_scope_checks() {
    use tinychannels::channel::{ChannelAdapter, ChannelStreamingDraft, ChannelTyping};
    use tinychannels_bus::ChannelInteraction;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    for (endpoint, result) in [
        (
            "sendMessage",
            serde_json::json!({"message_id": 9, "chat": {"id": -100}}),
        ),
        ("sendChatAction", serde_json::json!(true)),
    ] {
        Mock::given(method("POST"))
            .and(path(format!("/bottest-token/{endpoint}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ok":true,"result":result})),
            )
            .mount(&server)
            .await;
    }
    let instance = "telegram:account-b".to_owned();
    let channel = Arc::new(
        tinychannels::TelegramChannel::new("test-token".into(), vec!["*".into()], false)
            .with_api_base_for_test(server.uri()),
    );
    let hooks = Arc::new(Hooks::default());
    let (host, _callbacks, _server_connection, _storage) = ingress_fixture().await;
    let _provider_host = hooks.host(host.clone(), instance.clone(), None);
    let running = Running::start_telegram(channel, hooks, host, None, true);
    let handle = running.adapter_handle();
    let native = handle.native.as_ref().unwrap().clone();

    assert_eq!(native.proactive_target(), None);
    assert_eq!(
        native.receive_ack_policy(),
        tinychannels::channel::ChannelReceiveAckPolicy::AfterReceiveRecord
    );
    struct NullSink;
    #[async_trait::async_trait]
    impl tinychannels::channel::ChannelInboundSink for NullSink {
        async fn push(
            &self,
            _event: tinychannels_bus::ChannelIngress,
        ) -> std::result::Result<(), tinychannels::channel::ChannelSendError> {
            Ok(())
        }
    }
    let error = native.start(&NullSink).await.unwrap_err();
    assert_eq!(error.kind, tinychannels::channel::SendErrorKind::Forbidden);
    let status = native.status().await.unwrap();
    assert_eq!(status.account_id.as_deref(), Some(instance.as_str()));

    native.start_typing("-100").await.unwrap();
    native.stop_typing("-100").await.unwrap();
    assert!(matches!(
        running
            .interact(ChannelInteraction::StopTyping {
                recipient: "-100".into()
            })
            .await
            .unwrap(),
        tinychannels_bus::ChannelInteractionResult::Completed
    ));

    let channel_ref = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some(instance.clone()),
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Group,
        id: "-100".into(),
        ..Default::default()
    };
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "draft-scope".into(),
        channel_id: "telegram".into(),
        conversation_id: "-100".into(),
        channel_ref: Some(channel_ref.clone()),
        conversation_ref: Some(conversation.clone()),
        payload: tinychannels_bus::OutboundPayload::Text {
            text: "draft".into(),
        },
        ..Default::default()
    };
    let draft = native.upsert_draft(None, intent.clone()).await.unwrap();
    assert_eq!(draft.primary_platform_message_id.as_deref(), Some("9"));

    let before = server.received_requests().await.unwrap().len();
    let foreign = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram:other".into()),
    };
    let reaction = running
        .interact(ChannelInteraction::SendReaction {
            channel: foreign.clone(),
            conversation: conversation.clone(),
            provider_message_id: "9".into(),
            reaction: tinychannels_bus::ChannelReaction {
                value: "👍".into(),
                ..Default::default()
            },
            remove: false,
        })
        .await
        .unwrap_err();
    assert!(matches!(reaction, RunningSendError::NotDispatched(_)));
    let mut foreign_receipt = draft.clone();
    foreign_receipt.channel = Some(foreign);
    let delete = running
        .interact(ChannelInteraction::DeleteMessage {
            receipt: foreign_receipt,
        })
        .await
        .unwrap_err();
    assert!(matches!(delete, RunningSendError::NotDispatched(_)));
    assert_eq!(server.received_requests().await.unwrap().len(), before);

    native.stop().await.unwrap();
    let after_stop = running
        .interact(ChannelInteraction::StartTyping {
            recipient: "-100".into(),
        })
        .await;
    assert!(after_stop.is_err(), "a stopped runtime accepts no work");
}

#[tokio::test]
async fn legacy_running_routes_reaction_edit_delete_and_refuses_native_drafts() {
    use tinychannels_bus::ChannelInteraction;
    let connection = tinybus::Connection::connect({
        let bus = tinybus::transport::memory::MemoryBus::new();
        tinybus::broker::Broker::new().spawn(bus.clone());
        bus.connect().await.unwrap()
    })
    .await
    .unwrap();
    let running = Running::start(
        Arc::new(InteractionChannel::default()),
        Arc::new(Hooks::default()),
        HostChannels::new(connection),
    );
    let channel_ref = tinychannels_bus::ChannelRef {
        id: tinychannels_bus::provider_id("interaction-fixture").to_owned(),
        account_id: Some(running.configured_instance.clone()),
    };
    let conversation = ConversationRef {
        id: "room".into(),
        ..Default::default()
    };
    let receipt = tinychannels_bus::channel::MessageReceipt {
        channel: Some(channel_ref.clone()),
        conversation: Some(conversation.clone()),
        primary_platform_message_id: Some("m1".into()),
        parts: vec![tinychannels_bus::channel::MessageReceiptPart {
            platform_message_id: "m1".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "legacy-op".into(),
        channel_id: channel_ref.id.clone(),
        conversation_id: "room".into(),
        channel_ref: Some(channel_ref.clone()),
        conversation_ref: Some(conversation.clone()),
        payload: tinychannels_bus::OutboundPayload::Text { text: "x".into() },
        ..Default::default()
    };
    // The fixture implements none of these optional operations; each request
    // must still travel through the ordered worker and come back as a typed
    // provider error rather than being silently dropped.
    let reaction = running
        .interact(ChannelInteraction::SendReaction {
            channel: channel_ref.clone(),
            conversation,
            provider_message_id: "m1".into(),
            reaction: tinychannels_bus::ChannelReaction {
                value: "👍".into(),
                ..Default::default()
            },
            remove: false,
        })
        .await;
    assert!(matches!(reaction, Err(RunningSendError::AfterDispatch(_))));
    let edit = running
        .interact(ChannelInteraction::EditMessage {
            receipt: receipt.clone(),
            intent: intent.clone(),
        })
        .await;
    assert!(matches!(edit, Err(RunningSendError::AfterDispatch(_))));
    let delete = running
        .interact(ChannelInteraction::DeleteMessage {
            receipt: receipt.clone(),
        })
        .await;
    assert!(matches!(delete, Err(RunningSendError::AfterDispatch(_))));

    let draft = running
        .interact(ChannelInteraction::UpsertDraft {
            previous: None,
            intent,
        })
        .await;
    assert!(matches!(draft, Err(RunningSendError::NotDispatched(_))));
    running.stop().await.unwrap();
}

#[tokio::test]
async fn voice_codec_maps_known_formats_and_surfaces_missing_module_errors() {
    use tinychannels::host::{VoiceCodec, VoiceCodecRequest};
    let connection = tinybus::Connection::connect({
        let bus = tinybus::transport::memory::MemoryBus::new();
        tinybus::broker::Broker::new().spawn(bus.clone());
        bus.connect().await.unwrap()
    })
    .await
    .unwrap();
    let codec = ModuleVoiceCodec(HostChannels::new(connection));
    let request = |from: &str, target: &str| VoiceCodecRequest {
        audio_base64: "AAAA".into(),
        from: from.into(),
        target: target.into(),
    };
    let unsupported = codec.transcode(request("flac", "mp3")).await.unwrap_err();
    assert!(
        unsupported
            .to_string()
            .contains("unsupported TinyVoice codec")
    );
    let unsupported = codec
        .transcode(request("wav_pcm16", "opus-raw"))
        .await
        .unwrap_err();
    assert!(
        unsupported
            .to_string()
            .contains("unsupported TinyVoice codec")
    );
    for (from, target) in [
        ("audio/mp4", "audio/mpeg"),
        ("m4a_aac", "mp3"),
        ("audio/ogg", "audio/wav"),
    ] {
        let error = codec.transcode(request(from, target)).await.unwrap_err();
        assert!(
            !error.to_string().contains("unsupported TinyVoice codec"),
            "{from} -> {target} parses and reaches the TinyVoice module call"
        );
    }
}

#[tokio::test]
async fn module_voice_media_reports_each_precondition_and_missing_host_storage() {
    use base64::Engine;
    use tinychannels::host::{VoiceMediaRequest, VoiceMediaStore};
    let connection = tinybus::Connection::connect({
        let bus = tinybus::transport::memory::MemoryBus::new();
        tinybus::broker::Broker::new().spawn(bus.clone());
        bus.connect().await.unwrap()
    })
    .await
    .unwrap();
    let host = HostChannels::new(connection);
    let instance = "telegram:account-a".to_owned();
    let store = ModuleVoiceMediaStore(
        host.clone(),
        instance.clone(),
        MediaScope::fixed(Some("telegram:media-v1:abc".into())),
    );
    let scope = tinychannels_bus::HostVoiceScope {
        conversation: ConversationRef {
            id: "room".into(),
            ..Default::default()
        },
        turn_id: "turn-1".into(),
        ..Default::default()
    };
    let request = |audio: &str, scope: tinychannels_bus::HostVoiceScope| VoiceMediaRequest {
        audio_base64: audio.into(),
        mime_type: "audio/ogg".into(),
        codec: None,
        duration_ms: None,
        file_name: None,
        direction: tinychannels_bus::HostVoiceMediaDirection::Inbound,
        scope,
    };
    let missing_identity = store
        .store_voice(request("AAAA", Default::default()))
        .await
        .unwrap_err();
    assert!(
        missing_identity
            .to_string()
            .contains("conversation and turn")
    );
    let invalid = store
        .store_voice(request("***", scope.clone()))
        .await
        .unwrap_err();
    assert!(invalid.to_string().contains("payload is invalid"));
    let payload = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);
    let no_storage = store
        .store_voice(request(&payload, scope.clone()))
        .await
        .unwrap_err();
    assert!(
        no_storage
            .to_string()
            .contains("scoped storage is unavailable")
    );

    let read = store
        .read_voice(scope.clone(), "tinychannels_media_v1_missing".into())
        .await
        .unwrap_err();
    assert!(read.to_string().contains("scoped storage is unavailable"));

    let repository = ModuleMediaRepository(
        host,
        instance,
        MediaScope::fixed(Some("telegram:media-v1:abc".into())),
    );
    let error = repository
        .scoped_store()
        .await
        .err()
        .expect("no storage module");
    assert!(error.to_string().contains("scoped storage is unavailable"));
}

#[tokio::test]
async fn configured_slack_native_adapter_routes_every_port_through_the_ordered_queue() {
    use tinychannels_bus::{ChannelInteraction, ChannelOutboundIntent, OutboundPayload};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    for endpoint in [
        "chat.postMessage",
        "chat.update",
        "chat.delete",
        "reactions.add",
        "assistant.threads.setStatus",
    ] {
        Mock::given(method("POST"))
            .and(path(format!("/api/{endpoint}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ok": true, "ts": "321.5"})),
            )
            .mount(&server)
            .await;
    }
    let instance = "slack:account-a".to_owned();
    let channel = Arc::new(
        tinychannels::SlackChannel::new("test-token".into(), None, vec!["*".into()])
            .with_api_base(format!("{}/api", server.uri())),
    );
    let hooks = Arc::new(Hooks::default());
    let (host, _callbacks, _server_connection, _storage) = ingress_fixture().await;
    let _provider_host = hooks.host(host.clone(), instance.clone(), None);
    let running = Running::start_slack(channel, hooks, host, None, true);
    let handle = running.adapter_handle();
    assert_eq!(handle.mode, AdapterMode::SlackNativeV2);
    let descriptor = handle.descriptor();
    assert_eq!(descriptor.id, "slack");
    assert_eq!(descriptor.account_id.as_deref(), Some("slack:account-a"));
    assert_eq!(
        descriptor.metadata.get("adapterMode").map(String::as_str),
        Some("native-v2")
    );

    let channel_ref = tinychannels_bus::ChannelRef {
        id: "slack".into(),
        account_id: Some(instance.clone()),
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Channel,
        id: "C1".into(),
        thread_id: Some("100.1".into()),
        ..Default::default()
    };
    let intent = ChannelOutboundIntent {
        idempotency_key: "slack-native-op".into(),
        channel_id: "slack".into(),
        conversation_id: conversation.id.clone(),
        channel_ref: Some(channel_ref.clone()),
        conversation_ref: Some(conversation.clone()),
        payload: OutboundPayload::Text {
            text: "native send".into(),
        },
        ..Default::default()
    };
    let sent = handle.send(intent.clone()).await.unwrap();
    assert_eq!(
        sent.receipt.primary_platform_message_id.as_deref(),
        Some("321.5")
    );
    assert_eq!(sent.receipt.conversation.as_ref(), Some(&conversation));

    handle
        .reactions()
        .unwrap()
        .send_reaction(
            &channel_ref,
            &conversation,
            "321.5",
            &tinychannels_bus::ChannelReaction {
                value: "👍".into(),
                ..Default::default()
            },
            false,
        )
        .await
        .unwrap();
    let receipt = sent.receipt.clone();
    let edited = handle
        .edits()
        .unwrap()
        .edit(&receipt, intent.clone())
        .await
        .unwrap();
    assert_eq!(edited.conversation.as_ref(), Some(&conversation));
    handle.deletes().unwrap().delete(&receipt).await.unwrap();
    running
        .interact(ChannelInteraction::StartTyping {
            recipient: "C1:100.1".into(),
        })
        .await
        .unwrap();
    let draft = running
        .interact(ChannelInteraction::UpsertDraft {
            previous: None,
            intent: intent.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        draft,
        tinychannels_bus::ChannelInteractionResult::Sent(_)
    ));

    let before = server.received_requests().await.unwrap().len();
    let mut wrong_scope = receipt.clone();
    wrong_scope.conversation.as_mut().unwrap().id = "C-other".into();
    assert!(
        handle
            .edits()
            .unwrap()
            .edit(&wrong_scope, intent)
            .await
            .is_err()
    );
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        before,
        "a mismatched receipt must be rejected before provider I/O"
    );
    let paths: Vec<_> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| request.url.path().to_owned())
        .collect();
    for expected in [
        "/api/chat.postMessage",
        "/api/reactions.add",
        "/api/chat.update",
        "/api/chat.delete",
        "/api/assistant.threads.setStatus",
    ] {
        assert!(
            paths.iter().any(|path| path == expected),
            "{expected}: {paths:?}"
        );
    }
    running.stop().await.unwrap();
}

#[tokio::test]
async fn configured_signal_native_adapter_routes_every_port_through_the_ordered_queue() {
    use tinychannels_bus::{ChannelInteraction, ChannelOutboundIntent, OutboundPayload};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"jsonrpc": "2.0", "result": {"timestamp": 1_700_000_000_123u64}, "id": "x"}),
        ))
        .mount(&server)
        .await;
    let instance = "signal:account-a".to_owned();
    let channel = Arc::new(tinychannels::SignalChannel::new(
        server.uri(),
        "+15550000".into(),
        None,
        vec!["*".into()],
        false,
        false,
    ));
    let hooks = Arc::new(Hooks::default());
    let (host, _callbacks, _server_connection, _storage) = ingress_fixture().await;
    let _provider_host = hooks.host(host.clone(), instance.clone(), None);
    let running = Running::start_native("Signal", channel, hooks, host, None, true);
    let handle = running.adapter_handle();
    assert_eq!(handle.mode, AdapterMode::SignalNativeV2);
    let descriptor = handle.descriptor();
    assert_eq!(descriptor.id, "signal");
    assert_eq!(descriptor.account_id.as_deref(), Some("signal:account-a"));
    assert_eq!(
        descriptor.metadata.get("adapterMode").map(String::as_str),
        Some("native-v2")
    );

    let channel_ref = tinychannels_bus::ChannelRef {
        id: "signal".into(),
        account_id: Some(instance.clone()),
    };
    let conversation = ConversationRef {
        kind: ConversationKind::Dm,
        id: "+15551111".into(),
        ..Default::default()
    };
    let intent = ChannelOutboundIntent {
        idempotency_key: "signal-native-op".into(),
        channel_id: "signal".into(),
        conversation_id: conversation.id.clone(),
        channel_ref: Some(channel_ref.clone()),
        conversation_ref: Some(conversation.clone()),
        payload: OutboundPayload::Text {
            text: "native send".into(),
        },
        ..Default::default()
    };
    let sent = handle.send(intent.clone()).await.unwrap();
    assert_eq!(
        sent.receipt.primary_platform_message_id.as_deref(),
        Some("1700000000123")
    );
    assert_eq!(sent.receipt.conversation.as_ref(), Some(&conversation));

    handle
        .reactions()
        .unwrap()
        .send_reaction(
            &channel_ref,
            &conversation,
            "1700000000001",
            &tinychannels_bus::ChannelReaction {
                value: "👍".into(),
                ..Default::default()
            },
            false,
        )
        .await
        .unwrap();
    let receipt = sent.receipt.clone();
    handle
        .edits()
        .unwrap()
        .edit(&receipt, intent.clone())
        .await
        .unwrap();
    handle.deletes().unwrap().delete(&receipt).await.unwrap();
    running
        .interact(ChannelInteraction::StartTyping {
            recipient: conversation.id.clone(),
        })
        .await
        .unwrap();
    // Signal declares no streaming drafts.
    assert!(
        running
            .interact(ChannelInteraction::UpsertDraft {
                previous: None,
                intent: intent.clone(),
            })
            .await
            .is_err()
    );

    let before = server.received_requests().await.unwrap().len();
    let mut wrong_scope = receipt.clone();
    wrong_scope.conversation.as_mut().unwrap().id = "+15559999".into();
    assert!(
        handle
            .edits()
            .unwrap()
            .edit(&wrong_scope, intent)
            .await
            .is_err()
    );
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        before,
        "a mismatched receipt must be rejected before provider I/O"
    );
    let methods: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter_map(|request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).ok()?;
            body["method"].as_str().map(str::to_owned)
        })
        .collect();
    for expected in ["send", "sendReaction", "remoteDelete", "sendTyping"] {
        assert!(
            methods.iter().any(|m| m == expected),
            "{expected}: {methods:?}"
        );
    }
    running.stop().await.unwrap();
}

#[tokio::test]
async fn a_built_native_launch_starts_on_the_ordered_queue() {
    use tinychannels::factory::DefaultHttpClients;
    use tinychannels_bus::config::SignalConfig;
    let config = tinychannels_bus::ChannelsConfig {
        signal: Some(SignalConfig {
            http_url: "http://127.0.0.1:1".into(),
            account: "+15550000".into(),
            group_id: None,
            allowed_from: vec!["*".into()],
            ignore_attachments: false,
            ignore_stories: false,
        }),
        ..Default::default()
    };
    let noop: Arc<dyn tinychannels::host::ChannelHost> = tinychannels::host::NoopHost::arc();
    let launch = crate::native::build_native_v2("signal", &config, &noop, &DefaultHttpClients)
        .expect("signal is configured");
    let hooks = Arc::new(Hooks::default());
    let (host, _callbacks, _server_connection, _storage) = ingress_fixture().await;
    let _provider_host = hooks.host(host.clone(), "signal".to_owned(), None);
    let running = launch.start(hooks, host, None, true);
    assert_eq!(running.adapter_handle().mode, AdapterMode::SignalNativeV2);
    running.stop().await.unwrap();
}

#[tokio::test]
async fn a_built_imessage_launch_starts_as_the_native_imessage_adapter() {
    use tinychannels::factory::DefaultHttpClients;
    let config = tinychannels_bus::ChannelsConfig {
        imessage: Some(tinychannels_bus::config::IMessageConfig {
            allowed_contacts: vec!["*".into()],
        }),
        ..Default::default()
    };
    let noop: Arc<dyn tinychannels::host::ChannelHost> = tinychannels::host::NoopHost::arc();
    let launch = crate::native::build_native_v2("imessage", &config, &noop, &DefaultHttpClients)
        .expect("imessage is configured");
    let hooks = Arc::new(Hooks::default());
    let (host, _callbacks, _server_connection, _storage) = ingress_fixture().await;
    let _provider_host = hooks.host(host.clone(), "imessage".to_owned(), None);
    let running = launch.start(hooks, host, None, true);
    let handle = running.adapter_handle();
    assert_eq!(handle.mode, AdapterMode::IMessageNativeV2);
    assert_eq!(handle.descriptor().id, "imessage");
    running.stop().await.unwrap();
}
