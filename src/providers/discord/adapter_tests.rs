use super::*;
use crate::channel::SendErrorKind;
use crate::providers::discord::http::DiscordError;
use crate::providers::discord::support::*;
use crate::traits::Channel;
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::channel::{ChannelRef, ConversationRef};
use tinychannels_bus::{
    ChannelOutboundIntent, ChannelOutboundMessage, ContentPart, OutboundContentPart,
    OutboundPayload,
};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn intent(text: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "k".into(),
        channel_id: "discord".into(),
        conversation_id: CHANNEL.into(),
        channel_ref: Some(account()),
        conversation_ref: Some(conversation()),
        turn_id: Some("turn-1".into()),
        payload: OutboundPayload::Text { text: text.into() },
        ..Default::default()
    }
}

#[test]
fn capabilities_follow_the_services_the_host_supplied() {
    let bare = DiscordChannel::new("t".into(), None, None, vec![], false, false);
    let caps = bare.v2_capabilities();
    assert!(caps.content.text.outbound && caps.content.buttons.outbound);
    assert!(caps.content.presentation.outbound && caps.content.native.outbound);
    assert!(!caps.content.image.outbound && !caps.content.file.outbound);
    assert!(!caps.voice_in.supported && !caps.voice_out.supported);
    assert!(!caps.actions.supported.iter().any(|a| a == "sendAttachment"));
    assert_eq!(caps.text.text_limit, Some(2000));
    assert_eq!(caps.content.button_presentation.max_rows, Some(5));
    assert!(caps.reactions && caps.progressive_edits && caps.addressing.thread_id);
}

#[tokio::test]
async fn a_fully_wired_account_declares_media_voice_and_waveforms() {
    let server = MockServer::start().await;
    let (channel, ..) = full_channel(&server);
    let caps = channel.v2_capabilities();
    for content in [
        &caps.content.image,
        &caps.content.video,
        &caps.content.audio,
        &caps.content.file,
    ] {
        assert!(content.inbound && content.outbound);
        assert_eq!(content.max_size_bytes, Some(10 * 1024 * 1024));
    }
    assert!(caps.content.parts.iter().any(|p| p == "voice_note"));
    assert!(caps.voice_in.native_waveform && caps.voice_out.native_waveform);
    assert!(caps.voice_out.normalized_waveform && caps.voice_out.synthesis);
    assert_eq!(caps.voice_out.accepted_codecs, vec!["ogg_opus"]);
    assert!(caps.actions.supported.iter().any(|a| a == "sendAttachment"));
    for event in [
        "reaction_added",
        "reaction_removed",
        "message_updated",
        "message_deleted",
        "button_callback",
    ] {
        assert!(caps.events.supported.iter().any(|e| e == event), "{event}");
    }
}

#[tokio::test]
async fn the_legacy_channel_reports_the_same_facts() {
    let server = MockServer::start().await;
    let (channel, ..) = full_channel(&server);
    assert_eq!(Channel::capabilities(&channel), channel.v2_capabilities());
    assert!(channel.supports_reactions() && channel.supports_edit());
    assert!(channel.supports_draft_updates() && channel.supports_progressive_edits());
    assert!(channel.supports_reply_to_id() && channel.supports_thread_id());
}

#[tokio::test]
async fn the_adapter_describes_itself_and_manages_no_lifecycle_of_its_own() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path("/users/@me"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let channel = DiscordChannel::new("t".into(), None, Some(" 77 ".into()), vec![], false, false)
        .with_api_base(server.uri());

    let descriptor = ChannelAdapter::descriptor(&channel);
    assert_eq!(
        (descriptor.id.as_str(), descriptor.display_name.as_str()),
        ("discord", "Discord")
    );
    assert_eq!(
        descriptor.metadata.get("adapterMode").map(String::as_str),
        Some("native-v2")
    );
    assert_eq!(
        ChannelAdapter::proactive_target(&channel).as_deref(),
        Some("77")
    );
    assert_eq!(
        ChannelAdapter::declared_capabilities(&channel),
        channel.v2_capabilities()
    );

    struct Sink;
    #[async_trait::async_trait]
    impl ChannelInboundSink for Sink {
        async fn push(&self, _: tinychannels_bus::ChannelIngress) -> Result<(), ChannelSendError> {
            Ok(())
        }
    }
    ChannelAdapter::start(&channel, &Sink).await.unwrap();
    ChannelAdapter::stop(&channel).await.unwrap();
    let status = ChannelAdapter::status(&channel).await.unwrap();
    assert_eq!(status.channel_id, "discord");
    assert_eq!(status.health_state.as_deref(), Some("healthy"));
}

#[tokio::test]
async fn every_extension_trait_reaches_discord_through_the_adapter() {
    let server = MockServer::start().await;
    mount_message_post(&server, 600).await;
    for http in ["PATCH", "DELETE", "PUT", "POST"] {
        if http != "POST" {
            Mock::given(method(http))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "600"})))
                .mount(&server)
                .await;
        }
    }
    Mock::given(method("POST"))
        .and(wiremock::matchers::path(format!(
            "/channels/{CHANNEL}/typing"
        )))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let (channel, ..) = full_channel(&server);

    let sent = ChannelAdapter::send(&channel, intent("hello"))
        .await
        .unwrap();
    let typed = ChannelTypedSend::send_typed(
        &channel,
        &ChannelOutboundMessage::from_intent(&intent("typed")),
    )
    .await
    .unwrap();
    assert_eq!(typed.receipt.platform_message_ids.len(), 1);

    ChannelReaction::send_reaction(
        &channel,
        &account(),
        &conversation(),
        "600",
        &tinychannels_bus::ChannelReaction {
            value: "👍".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await
    .unwrap();
    ChannelEdit::edit(&channel, &sent.receipt, intent("edited"))
        .await
        .unwrap();
    ChannelTyping::start_typing(&channel, CHANNEL)
        .await
        .unwrap();
    ChannelTyping::stop_typing(&channel, CHANNEL).await.unwrap();
    let draft = ChannelStreamingDraft::upsert_draft(&channel, None, intent("draft"))
        .await
        .unwrap();
    ChannelStreamingDraft::upsert_draft(&channel, Some(&draft), intent("more"))
        .await
        .unwrap();
    ChannelDelete::delete(&channel, &sent.receipt)
        .await
        .unwrap();
}

#[tokio::test]
async fn other_providers_cannot_drive_the_discord_extensions() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let slack = ChannelRef {
        id: "slack".into(),
        account_id: None,
    };

    let reaction = ChannelReaction::send_reaction(
        &channel,
        &slack,
        &ConversationRef {
            id: CHANNEL.into(),
            ..Default::default()
        },
        "1",
        &tinychannels_bus::ChannelReaction {
            value: "x".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(reaction.kind, SendErrorKind::Forbidden);

    let mut foreign = intent("x");
    foreign.channel_ref = Some(slack);
    let draft = ChannelStreamingDraft::upsert_draft(&channel, None, foreign)
        .await
        .unwrap_err();
    assert_eq!(draft.kind, SendErrorKind::Forbidden);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn the_legacy_trait_methods_delegate_and_keep_the_typed_error() {
    let server = MockServer::start().await;
    mount_message_post(&server, 800).await;
    Mock::given(method("PATCH"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "800"})))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let message = ChannelOutboundMessage::from_intent(&intent("legacy"));

    Channel::send_typed(&channel, &message).await.unwrap();
    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    let receipt = outcome.receipt;
    let thumbs = tinychannels_bus::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    Channel::send_reaction(&channel, &conversation(), "800", &thumbs, false)
        .await
        .unwrap();
    channel
        .edit_message(&receipt, &intent("again"))
        .await
        .unwrap();
    channel.delete_message(&receipt).await.unwrap();

    let bad = ChannelOutboundMessage {
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Location {
                latitude: 0.0,
                longitude: 0.0,
                label: None,
                address: None,
                accuracy_meters: None,
                live_until_unix_ms: None,
            },
        }],
        ..message
    };
    let error = channel.send_typed_with_receipt(&bad).await.unwrap_err();
    assert!(error.downcast_ref::<DiscordError>().unwrap().0.unsupported);
    let failed = channel
        .edit_message(&receipt, &intent(&"x".repeat(2001)))
        .await
        .unwrap_err();
    assert_eq!(
        failed.downcast_ref::<DiscordError>().unwrap().0.kind,
        SendErrorKind::TooLong
    );
    assert!(
        channel
            .delete_message(&MessageReceipt::default())
            .await
            .is_err()
    );
    let empty = tinychannels_bus::ChannelReaction::default();
    assert!(
        Channel::send_reaction(&channel, &conversation(), "x", &empty, false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn the_factory_wires_every_host_service_into_the_discord_account() {
    use crate::host::ChannelHostBuilder;
    let events = Arc::new(RecordingEvents::default());
    let host = ChannelHostBuilder::new()
        .events(events)
        .media_repository(Arc::new(MemoryMedia::default()))
        .voice_media_store(Arc::new(MemoryVoice::default()))
        .voice_codec(Arc::new(FixedCodec::new(false)))
        .transcriber(Arc::new(FixedTranscriber(Some("x"))))
        .build();
    let config = tinychannels_bus::ChannelsConfig {
        discord: Some(tinychannels_bus::config::DiscordConfig {
            bot_token: BOT_TOKEN.into(),
            guild_id: None,
            channel_id: Some(CHANNEL.into()),
            allowed_users: vec!["42".into()],
            listen_to_bots: false,
            mention_only: true,
        }),
        ..Default::default()
    };

    let built = crate::build_discord_channel(&config, &host, &crate::DefaultHttpClients).unwrap();

    let caps = built.v2_capabilities();
    assert!(caps.content.image.outbound && caps.voice_in.supported);
    assert!(caps.voice_out.normalized_waveform);
    assert!(!caps.voice_out.synthesis, "no synthesizer was supplied");
    assert_eq!(
        Channel::proactive_target(built.as_ref()).as_deref(),
        Some(CHANNEL)
    );
    assert!(
        crate::build_discord_channel(&Default::default(), &host, &crate::DefaultHttpClients)
            .is_none()
    );
    assert_eq!(
        crate::build_channels(&config, &host, &crate::DefaultHttpClients).len(),
        1
    );
}
