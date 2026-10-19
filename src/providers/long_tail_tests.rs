use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction, ChannelStreamingDraft,
    ChannelTypedSend, ChannelTyping,
};
use crate::factory::{DefaultHttpClients, build_long_tail_channel};
use std::sync::Arc;
use std::sync::Mutex;
use tinychannels_bus::channel::{ChannelRef, ConversationRef, MessageReceipt};
use tinychannels_bus::config::*;
use tinychannels_bus::{
    ChannelsConfig, ContentPart, MediaKind, MediaReference, OutboundContentPart, OutboundPayload,
};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct TypedTextFixture {
    send_error: bool,
    sent: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl crate::traits::Channel for TypedTextFixture {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, message: &crate::traits::SendMessage) -> anyhow::Result<()> {
        self.sent.lock().unwrap().push(message.content.clone());
        if self.send_error {
            anyhow::bail!("private provider detail")
        }
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<crate::traits::ChannelMessage>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl super::LongTail for TypedTextFixture {
    const ID: &'static str = "fixture";
    const DISPLAY: &'static str = "Fixture";

    fn declared(&self) -> tinychannels_bus::ChannelCapabilities {
        super::text_capabilities(super::TextSpec::default())
    }
}

fn typed_fixture_message(
    parts: Vec<OutboundContentPart>,
) -> tinychannels_bus::ChannelOutboundMessage {
    let intent = text_intent("fixture", "room-1", None);
    let mut message = tinychannels_bus::ChannelOutboundMessage::from_intent(&intent);
    message.parts = parts;
    message
}

#[test]
fn typed_receipt_uses_reported_ids_and_falls_back_to_message_routing() {
    let mut message = typed_fixture_message(Vec::new());
    message.channel_ref = None;
    message.conversation_ref = None;
    message.thread_id = Some("thread-1".into());
    message.reply_to_id = Some("reply-1".into());

    let receipt = super::receipt::<TypedTextFixture>(
        &message,
        vec!["platform-1".into(), "platform-2".into()],
    );

    assert_eq!(receipt.idempotency_key.as_deref(), Some("k"));
    assert_eq!(
        receipt.channel.as_ref().map(|channel| channel.id.as_str()),
        Some("fixture")
    );
    assert_eq!(
        receipt
            .conversation
            .as_ref()
            .map(|conversation| conversation.id.as_str()),
        Some("room-1")
    );
    assert_eq!(
        receipt.primary_platform_message_id.as_deref(),
        Some("platform-1")
    );
    assert_eq!(receipt.platform_message_ids, ["platform-1", "platform-2"]);
    assert_eq!(receipt.parts.len(), 2);
    assert_eq!(receipt.parts[1].index, 1);
    assert_eq!(receipt.parts[1].thread_id.as_deref(), Some("thread-1"));
    assert_eq!(receipt.parts[1].reply_to_id.as_deref(), Some("reply-1"));
}

#[tokio::test]
async fn text_only_typed_adapter_sends_a_legacy_text_projection() {
    let provider = TypedTextFixture {
        send_error: false,
        sent: Mutex::new(Vec::new()),
    };
    let message = typed_fixture_message(vec![OutboundContentPart::Content {
        part: ContentPart::Text {
            text: "plain typed text".into(),
            markdown_dialect: None,
        },
    }]);

    let outcome = super::send_typed(&provider, &message).await.unwrap();
    assert!(outcome.error.is_none());
    assert!(
        outcome.receipt.parts.is_empty(),
        "no unknown provider id is fabricated"
    );
    assert_eq!(
        provider.sent.lock().unwrap().as_slice(),
        ["plain typed text"]
    );
}

#[tokio::test]
async fn text_only_typed_adapter_rejects_wrong_channels_and_rich_content_before_send() {
    let provider = TypedTextFixture {
        send_error: false,
        sent: Mutex::new(Vec::new()),
    };
    let mut wrong_channel = typed_fixture_message(Vec::new());
    wrong_channel.channel_id = "another".into();
    let error = super::send_typed(&provider, &wrong_channel)
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Forbidden);

    let rich_content = typed_fixture_message(vec![OutboundContentPart::Content {
        part: ContentPart::Image {
            media: MediaReference {
                opaque_id: Some("opaque-image".into()),
                kind: MediaKind::Image,
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }]);
    let error = super::send_typed(&provider, &rich_content)
        .await
        .unwrap_err();
    assert!(error.unsupported);
    assert!(provider.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn text_only_typed_adapter_classifies_legacy_send_errors_as_uncertain() {
    let provider = TypedTextFixture {
        send_error: true,
        sent: Mutex::new(Vec::new()),
    };
    let message = typed_fixture_message(vec![OutboundContentPart::Content {
        part: ContentPart::Text {
            text: "may have reached the provider".into(),
            markdown_dialect: None,
        },
    }]);

    let error = super::send_typed(&provider, &message).await.unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Unknown);
    assert!(error.sent_before_error);
    assert!(!error.message.contains("private provider detail"));
    assert_eq!(provider.sent.lock().unwrap().len(), 1);
}

/// Lark is an optional feature; every other provider is always built in.
#[cfg(feature = "lark")]
const PROVIDERS: [(&str, &str); 6] = [
    ("lark", "Lark"),
    ("mattermost", "Mattermost"),
    ("irc", "IRC"),
    ("qq", "QQ"),
    ("dingtalk", "DingTalk"),
    ("yuanbao", "Yuanbao"),
];

#[cfg(not(feature = "lark"))]
const PROVIDERS: [(&str, &str); 5] = [
    ("mattermost", "Mattermost"),
    ("irc", "IRC"),
    ("qq", "QQ"),
    ("dingtalk", "DingTalk"),
    ("yuanbao", "Yuanbao"),
];

fn config(mattermost_url: &str) -> ChannelsConfig {
    let yuanbao = YuanbaoConfig {
        app_key: "ak".into(),
        ws_domain: "wss://example.invalid".into(),
        token: "tok".into(),
        ..Default::default()
    };
    ChannelsConfig {
        mattermost: Some(MattermostConfig {
            url: mattermost_url.into(),
            bot_token: "bot-token".into(),
            channel_id: None,
            allowed_users: vec![],
            thread_replies: None,
            mention_only: None,
        }),
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
        yuanbao: Some(yuanbao),
        ..Default::default()
    }
}

fn build(config: &ChannelsConfig, id: &str) -> Arc<dyn crate::LongTailNative> {
    build_long_tail_channel(config, &DefaultHttpClients, id).expect("provider is built in")
}

fn text_intent(id: &str, conversation: &str, thread: Option<&str>) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "k".into(),
        channel_id: id.into(),
        conversation_id: conversation.into(),
        channel_ref: Some(ChannelRef {
            id: id.into(),
            account_id: None,
        }),
        conversation_ref: Some(ConversationRef {
            id: conversation.into(),
            thread_id: thread.map(str::to_owned),
            ..Default::default()
        }),
        thread_id: thread.map(str::to_owned),
        payload: OutboundPayload::Text { text: "hi".into() },
        ..Default::default()
    }
}

#[test]
fn every_provider_describes_itself_and_matches_its_legacy_capabilities() {
    let config = config("http://mm.invalid");
    for (id, display) in PROVIDERS {
        let channel = build(&config, id);
        let descriptor = channel.descriptor();
        assert_eq!(descriptor.id, id);
        assert_eq!(descriptor.display_name, display);
        assert_eq!(descriptor.metadata["adapterMode"], "native-v2");
        let declared = channel.declared_capabilities();
        assert_eq!(declared, Channel::capabilities(channel.as_ref()), "{id}");
        assert!(declared.remote_control && declared.chat_approvals, "{id}");
        assert_eq!(declared.content.parts, ["text"], "{id}");
        assert!(declared.content.text.outbound && declared.content.text.inbound);
        // Nothing this module cannot do is advertised.
        assert!(!declared.reactions && !declared.progressive_edits, "{id}");
        for action in ["edit", "delete", "react", "draft-update", "voice"] {
            assert!(
                !declared.actions.supported.iter().any(|a| a == action),
                "{id} must not claim {action}"
            );
        }
        assert!(!declared.voice_in.supported && !declared.voice_out.supported);
        assert!(!declared.content.image.outbound && !declared.content.file.outbound);
        assert_eq!(ChannelAdapter::proactive_target(channel.as_ref()), None);
    }
}

#[test]
fn threading_and_typing_are_declared_only_where_implemented() {
    let config = config("http://mm.invalid");
    for (id, _) in PROVIDERS {
        let caps = build(&config, id).declared_capabilities();
        let typing = caps.actions.supported.iter().any(|a| a == "typing");
        let thread = caps.actions.supported.iter().any(|a| a == "thread-reply");
        assert_eq!(typing, matches!(id, "mattermost" | "yuanbao"), "{id}");
        assert_eq!(thread, id == "mattermost", "{id}");
        assert_eq!(caps.addressing.thread_id, thread, "{id}");
        assert_eq!(caps.addressing.reply_to_id, id == "yuanbao", "{id}");
    }
}

#[test]
fn yuanbao_declares_streaming_pipeline_without_progressive_edits() {
    let caps = build(&config("http://mm.invalid"), "yuanbao").declared_capabilities();
    // The reply is delivered once at finalize; nothing is edited in place.
    assert!(caps.streaming_reply_pipeline);
    assert!(!caps.progressive_edits);
    assert!(!caps.actions.supported.iter().any(|a| a == "draft-update"));
}

#[test]
fn unconfigured_or_unknown_providers_build_nothing() {
    let empty = ChannelsConfig::default();
    for (id, _) in PROVIDERS {
        assert!(build_long_tail_channel(&empty, &DefaultHttpClients, id).is_none());
    }
    assert!(
        build_long_tail_channel(&config("http://x.invalid"), &DefaultHttpClients, "slack")
            .is_none()
    );
    let mut bad = config("http://x.invalid");
    bad.yuanbao.as_mut().unwrap().ws_domain.clear();
    bad.yuanbao.as_mut().unwrap().token.clear();
    assert!(build_long_tail_channel(&bad, &DefaultHttpClients, "yuanbao").is_none());
}

#[tokio::test]
async fn configured_accounts_report_enabled_status() {
    let config = config("http://mm.invalid");
    for (id, _) in PROVIDERS {
        let channel = build(&config, id);
        let status = ChannelAdapter::status(channel.as_ref()).await.unwrap();
        assert_eq!(status.channel_id, id);
        assert!(status.states.contains(&ChannelAccountState::Enabled));
    }
}

#[tokio::test]
async fn legacy_noop_lifecycle_hooks_remain_compatible() {
    let config = config("http://mm.invalid");
    // IRC and Mattermost own real listeners now, with their lifecycle pinned
    // by provider-specific local-server tests. Keep this list explicit as the
    // remaining adapters migrate; the non-empty assertion prevents this
    // compatibility check from silently becoming vacuous.
    #[cfg(feature = "lark")]
    const LEGACY_NOOP_PROVIDERS: [&str; 4] = ["lark", "qq", "dingtalk", "yuanbao"];
    #[cfg(not(feature = "lark"))]
    const LEGACY_NOOP_PROVIDERS: [&str; 3] = ["qq", "dingtalk", "yuanbao"];
    assert!(!LEGACY_NOOP_PROVIDERS.is_empty());

    for id in LEGACY_NOOP_PROVIDERS {
        assert!(
            PROVIDERS.iter().any(|(provider, _)| *provider == id),
            "legacy lifecycle provider {id} must remain in the configured provider set"
        );
        let channel = build(&config, id);
        struct Sink;
        #[async_trait::async_trait]
        impl crate::channel::ChannelInboundSink for Sink {
            async fn push(
                &self,
                _: tinychannels_bus::ChannelIngress,
            ) -> Result<(), ChannelSendError> {
                Ok(())
            }
        }
        ChannelAdapter::start(channel.as_ref(), &Sink)
            .await
            .unwrap();
        ChannelAdapter::stop(channel.as_ref()).await.unwrap();
    }
}

#[tokio::test]
async fn unimplemented_operations_are_explicitly_unsupported() {
    let config = config("http://mm.invalid");
    let receipt = MessageReceipt::default();
    let reaction = tinychannels_bus::channel::ChannelReaction::default();
    for (id, display) in PROVIDERS {
        let channel = build(&config, id);
        let errors = [
            ChannelReaction::send_reaction(
                channel.as_ref(),
                &ChannelRef {
                    id: id.into(),
                    account_id: None,
                },
                &ConversationRef::default(),
                "m",
                &reaction,
                false,
            )
            .await
            .unwrap_err(),
            ChannelEdit::edit(channel.as_ref(), &receipt, text_intent(id, "c", None))
                .await
                .map(|_| ())
                .unwrap_err(),
            ChannelDelete::delete(channel.as_ref(), &receipt)
                .await
                .unwrap_err(),
            ChannelStreamingDraft::upsert_draft(channel.as_ref(), None, text_intent(id, "c", None))
                .await
                .map(|_| ())
                .unwrap_err(),
        ];
        for error in errors {
            assert!(error.unsupported, "{id}");
            assert!(!error.retryable && !error.sent_before_error, "{id}");
            assert!(error.message.contains(display), "{id}: {}", error.message);
        }
    }
}

#[tokio::test]
async fn providers_without_a_typing_indicator_refuse_it() {
    let config = config("http://mm.invalid");
    for (id, _) in PROVIDERS
        .iter()
        .filter(|(id, _)| matches!(*id, "lark" | "irc" | "qq" | "dingtalk"))
    {
        let channel = build(&config, id);
        for start in [true, false] {
            let error = if start {
                ChannelTyping::start_typing(channel.as_ref(), "c").await
            } else {
                ChannelTyping::stop_typing(channel.as_ref(), "c").await
            }
            .unwrap_err();
            assert!(error.unsupported, "{id}");
        }
    }
}

#[tokio::test]
async fn a_failing_typing_indicator_is_a_retryable_transient_error() {
    // Yuanbao has no live connection here, so the heartbeat cannot be sent.
    let channel = build(&config("http://mm.invalid"), "yuanbao");
    let error = ChannelTyping::start_typing(channel.as_ref(), "user")
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Transient);
    assert!(error.retryable && !error.unsupported);
}

#[tokio::test]
async fn mattermost_typed_text_is_posted_with_the_thread_root_packed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .and(header("authorization", "Bearer bot-token"))
        .and(body_partial_json(serde_json::json!({
            "channel_id": "chan1", "root_id": "root9", "message": "hi"
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({"id": "p1"})))
        .expect(2)
        .mount(&server)
        .await;
    let channel = build(&config(&server.uri()), "mattermost");

    let outcome = ChannelAdapter::send(
        channel.as_ref(),
        text_intent("mattermost", "chan1", Some("root9")),
    )
    .await
    .unwrap();
    assert!(outcome.error.is_none());

    // An already packed recipient is left alone.
    let mut message =
        ChannelOutboundMessage::from_intent(&text_intent("mattermost", "chan1:root9", None));
    message.thread_id = Some("other".into());
    ChannelTypedSend::send_typed(channel.as_ref(), &message)
        .await
        .unwrap();
}

#[tokio::test]
async fn mattermost_typing_posts_the_indicator_until_stopped() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v4/users/me/typing"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let channel = build(&config(&server.uri()), "mattermost");
    ChannelTyping::start_typing(channel.as_ref(), "chan1:root9")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    ChannelTyping::stop_typing(channel.as_ref(), "chan1:root9")
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    assert!(!requests.is_empty());
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["channel_id"], "chan1");
    assert_eq!(body["parent_id"], "root9");
}

#[tokio::test]
async fn a_rejected_post_is_reported_as_possibly_delivered() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let channel = build(&config(&server.uri()), "mattermost");
    let error = ChannelAdapter::send(channel.as_ref(), text_intent("mattermost", "c", None))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Unknown);
    assert!(error.sent_before_error && !error.retryable);
    assert!(
        !error.message.contains("500"),
        "provider detail must not leak"
    );
}

#[tokio::test]
async fn sends_for_another_channel_or_without_a_conversation_never_reach_the_provider() {
    let server = MockServer::start().await;
    let channel = build(&config(&server.uri()), "mattermost");

    let wrong_id = ChannelAdapter::send(channel.as_ref(), text_intent("irc", "c", None))
        .await
        .unwrap_err();
    assert_eq!(wrong_id.kind, SendErrorKind::Forbidden);

    let mut wrong_ref = text_intent("mattermost", "c", None);
    wrong_ref.channel_ref = Some(ChannelRef {
        id: "slack".into(),
        account_id: None,
    });
    assert_eq!(
        ChannelAdapter::send(channel.as_ref(), wrong_ref)
            .await
            .unwrap_err()
            .kind,
        SendErrorKind::Forbidden
    );

    let empty = ChannelAdapter::send(channel.as_ref(), text_intent("mattermost", "  ", None))
        .await
        .unwrap_err();
    assert_eq!(empty.kind, SendErrorKind::BadFormat);
    assert!(!empty.unsupported);

    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn non_text_content_is_refused_before_any_request() {
    let server = MockServer::start().await;
    let channel = build(&config(&server.uri()), "mattermost");
    let mut intent = text_intent("mattermost", "c", None);
    intent.payload = OutboundPayload::Parts {
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("media-1".into()),
                    kind: MediaKind::Image,
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        }],
    };
    let error = ChannelAdapter::send(channel.as_ref(), intent)
        .await
        .unwrap_err();
    assert!(error.unsupported);
    assert!(server.received_requests().await.unwrap().is_empty());
}
