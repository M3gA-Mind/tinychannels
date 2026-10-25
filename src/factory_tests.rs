//! Tests for the provider factory.
//!
//! These matter more than their size suggests: this function is the single
//! config-to-provider mapping shared by the OpenHuman core and the
//! `tinychannels-module` cdylib. Before it existed the mapping was inline in
//! the host, so "does a config stanza produce a provider" was only ever
//! answered by starting the real thing.

use super::*;
use crate::NoopHost;
use crate::host::ChannelHost;
use std::sync::Arc;
use tinychannels_bus::ChannelsConfig;
use tinychannels_bus::HttpProxyPolicy;
use tinychannels_bus::config::{DiscordConfig, TelegramConfig, WhatsAppConfig};

fn host() -> Arc<dyn ChannelHost> {
    NoopHost::arc()
}

fn names(config: &ChannelsConfig) -> Vec<String> {
    build_channels(config, &host(), &DefaultHttpClients)
        .iter()
        .map(|channel| channel.name().to_owned())
        .collect()
}

fn telegram() -> TelegramConfig {
    TelegramConfig {
        bot_token: "token".to_owned(),
        chat_id: None,
        allowed_users: Vec::new(),
        stream_mode: crate::config::StreamMode::default(),
        draft_update_interval_ms: 1_000,
        silent_streaming: false,
        mention_only: false,
    }
}

#[tokio::test]
async fn configured_proxy_client_routes_channel_requests_through_the_host_proxy() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = proxy.accept().await.unwrap();
        let mut request = vec![0; 4096];
        let read = stream.read(&mut request).await.unwrap();
        request.truncate(read);
        let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
    });

    let clients = ConfiguredHttpClients::new(HttpProxyPolicy {
        all_proxy: Some(format!("http://{proxy_address}")),
        service_selectors: vec!["channel.*".into()],
        ..HttpProxyPolicy::default()
    })
    .unwrap();
    let response = clients
        .client_for("channel.telegram")
        .get("http://fixture.invalid/path")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let request = request_rx.await.unwrap();
    assert!(request.starts_with("GET http://fixture.invalid/path HTTP/1.1"));
}

#[tokio::test]
async fn configured_proxy_policy_leaves_unselected_channel_direct() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn serve_once(listener: tokio::net::TcpListener, body: &'static str) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 4096];
        let _ = stream.read(&mut request).await.unwrap();
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }

    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let proxy_task = tokio::spawn(serve_once(proxy, "proxy"));
    let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination_address = destination.local_addr().unwrap();
    let destination_task = tokio::spawn(serve_once(destination, "direct"));

    let clients = ConfiguredHttpClients::new(HttpProxyPolicy {
        all_proxy: Some(format!("http://{proxy_address}")),
        service_selectors: vec!["channel.telegram".into()],
        ..HttpProxyPolicy::default()
    })
    .unwrap();
    let response = clients
        .client_for("channel.discord")
        .get(format!("http://{destination_address}/path"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "direct");
    destination_task.await.unwrap();
    proxy_task.abort();
}

#[test]
fn yuanbao_factory_capabilities_include_only_installed_host_services() {
    use crate::host::ChannelHostBuilder;
    use crate::providers::native::fixtures::StubVoice;
    use tinychannels_bus::config::YuanbaoConfig;

    let yuanbao = YuanbaoConfig {
        app_key: "factory-app".into(),
        token: "factory-token".into(),
        bot_id: "factory-bot".into(),
        ws_domain: "ws://127.0.0.1:1".into(),
        ..Default::default()
    };
    let config = ChannelsConfig {
        yuanbao: Some(yuanbao),
        ..Default::default()
    };
    let host: Arc<dyn ChannelHost> = ChannelHostBuilder::new()
        .voice_media_store(Arc::new(StubVoice::default()))
        .transcriber(Arc::new(
            crate::providers::native::fixtures::StubTranscriber::default(),
        ))
        .build();

    let adapter =
        build_yuanbao_channel_with_voice_http_for_test(&config, &host, reqwest::Client::new())
            .expect("the configured Yuanbao stanza builds through the factory");
    let descriptor = adapter.descriptor();
    let capabilities = adapter.declared_capabilities();

    assert_eq!(descriptor.id, "yuanbao");
    assert!(capabilities.content.text.outbound);
    assert!(capabilities.addressing.reply_to_id);
    assert!(!capabilities.addressing.thread_id);
    assert!(
        capabilities
            .actions
            .supported
            .iter()
            .any(|action| action == "typing")
    );
    assert!(capabilities.voice_in.supported);
    assert!(!capabilities.content.image.outbound);
    assert!(!capabilities.content.file.outbound);
    assert!(!capabilities.voice_out.supported);
}

#[test]
fn configured_proxy_client_rejects_invalid_urls_without_echoing_credentials() {
    let secret = "http://user:password@not a valid proxy";
    let error = ConfiguredHttpClients::new(HttpProxyPolicy {
        all_proxy: Some(secret.into()),
        service_selectors: vec!["channel.*".into()],
        ..HttpProxyPolicy::default()
    })
    .err()
    .expect("invalid proxy policy must be rejected")
    .to_string();
    assert!(!error.contains("user"));
    assert!(!error.contains("password"));
}

#[test]
fn a_default_config_builds_no_channels() {
    assert!(names(&ChannelsConfig::default()).is_empty());
}

#[test]
fn a_configured_provider_is_built() {
    let config = ChannelsConfig {
        telegram: Some(telegram()),
        ..ChannelsConfig::default()
    };
    assert_eq!(names(&config), vec!["telegram".to_owned()]);
}

/// Order is part of the contract: a caller that indexes the result, or logs it,
/// should see the same sequence across runs and across the two hosts.
#[test]
fn providers_are_returned_in_declaration_order() {
    // Discord is written first here on purpose: the assertion below proves the
    // factory's declaration order wins, not the order these fields were set.
    let config = ChannelsConfig {
        discord: Some(DiscordConfig {
            bot_token: "token".to_owned(),
            guild_id: None,
            channel_id: None,
            allowed_users: Vec::new(),
            listen_to_bots: false,
            mention_only: false,
        }),
        telegram: Some(telegram()),
        ..ChannelsConfig::default()
    };

    assert_eq!(
        names(&config),
        vec!["telegram".to_owned(), "discord".to_owned()]
    );
}

/// A WhatsApp stanza that is neither a Cloud nor a Web shape is skipped.
///
/// The important half is that it does **not** panic and does **not** take the
/// other providers down with it: one bad stanza must not cost a user every
/// other channel.
#[test]
fn an_unusable_whatsapp_stanza_is_skipped_without_disturbing_others() {
    let config = ChannelsConfig {
        telegram: Some(telegram()),
        // Neither `phone_number_id` (Cloud) nor `session_path` (Web) is set, so
        // `backend_type()` reports neither shape.
        whatsapp: Some(WhatsAppConfig {
            access_token: None,
            phone_number_id: None,
            verify_token: None,
            app_secret: None,
            session_path: None,
            pair_phone: None,
            pair_code: None,
            allowed_numbers: Vec::new(),
        }),
        ..ChannelsConfig::default()
    };

    assert_eq!(names(&config), vec!["telegram".to_owned()]);
}

#[tokio::test]
async fn configured_stanzas_build_every_supported_provider_in_order() {
    let config: ChannelsConfig = serde_json::from_value(serde_json::json!({
        "telegram": {"bot_token": "token", "allowed_users": []},
        "discord": {"bot_token": "token"},
        "slack": {"bot_token": "token"},
        "mattermost": {"url": "https://mattermost.test", "bot_token": "token"},
        "imessage": {"allowed_contacts": ["alice"]},
        "matrix": {"homeserver": "https://matrix.test", "access_token": "token", "room_id": "room", "allowed_users": []},
        "signal": {"http_url": "http://localhost:8080", "account": "alice"},
        "whatsapp": {"phone_number_id": "phone", "access_token": "token", "verify_token": "verify"},
        "linq": {"api_token": "token", "from_phone": "+15551234567"},
        "email": {"imap_host": "imap.test", "smtp_host": "smtp.test", "username": "user", "password": "pass", "from_address": "user@example.test"},
        "irc": {"server": "irc.test", "nickname": "bot"},
        "lark": {"app_id": "app", "app_secret": "secret"},
        "dingtalk": {"client_id": "app", "client_secret": "secret"},
        "qq": {"app_id": "app", "app_secret": "secret"},
        "yuanbao": {"app_key": "app", "app_secret": "secret"}
    })).unwrap();
    let mut expected = vec![
        "telegram",
        "discord",
        "slack",
        "mattermost",
        "signal",
        "whatsapp",
        "linq",
    ];
    if cfg!(feature = "imessage") {
        expected.insert(4, "imessage");
    }
    if cfg!(feature = "email") {
        expected.push("email");
    }
    expected.push("irc");
    if cfg!(feature = "lark") {
        expected.push("lark");
    }
    expected.extend(["dingtalk", "qq", "yuanbao"]);
    assert_eq!(names(&config), expected);
}

#[tokio::test]
async fn incomplete_cloud_and_invalid_yuanbao_stanzas_preserve_other_providers() {
    let config: ChannelsConfig = serde_json::from_value(serde_json::json!({
        "telegram": {"bot_token": "token", "allowed_users": []},
        "whatsapp": {"phone_number_id": "phone"},
        "yuanbao": {"app_key": "", "app_secret": ""}
    }))
    .unwrap();
    assert_eq!(names(&config), ["telegram"]);
}

#[tokio::test]
async fn whatsapp_web_configuration_respects_the_feature_gate() {
    let directory = tempfile::tempdir().unwrap();
    let config: ChannelsConfig = serde_json::from_value(serde_json::json!({
        "whatsapp": {"session_path": directory.path(), "allowed_numbers": ["alice"]}
    }))
    .unwrap();
    let expected: Vec<&str> = if cfg!(feature = "whatsapp-web") {
        vec!["whatsapp"]
    } else {
        vec![]
    };
    assert_eq!(names(&config), expected);
}

#[test]
fn proxy_policy_limits_reject_oversized_and_empty_entries() {
    let policies = [
        HttpProxyPolicy {
            all_proxy: Some("x".repeat(4097)),
            ..Default::default()
        },
        HttpProxyPolicy {
            no_proxy: vec!["localhost".into(); 129],
            ..Default::default()
        },
        HttpProxyPolicy {
            no_proxy: vec!["x".repeat(1025)],
            ..Default::default()
        },
        HttpProxyPolicy {
            service_selectors: vec!["channel.*".into(); 65],
            ..Default::default()
        },
        HttpProxyPolicy {
            service_selectors: vec!["x".repeat(129)],
            ..Default::default()
        },
        HttpProxyPolicy {
            service_selectors: vec!["  ".into()],
            ..Default::default()
        },
    ];
    for policy in policies {
        assert!(ConfiguredHttpClients::new(policy).is_err());
    }
}

#[test]
fn proxy_selectors_match_exact_services_and_prefixes() {
    let clients = ConfiguredHttpClients::new(HttpProxyPolicy {
        http_proxy: Some(" http://127.0.0.1:1234 ".into()),
        https_proxy: Some("http://127.0.0.1:1234".into()),
        no_proxy: vec![" localhost ".into(), " ".into()],
        service_selectors: vec!["channel.telegram".into(), "channel.discord.*".into()],
        ..Default::default()
    })
    .unwrap();
    assert!(clients.applies_to("channel.telegram"));
    assert!(clients.applies_to("channel.discord.media"));
    assert!(!clients.applies_to("channel.telegram.media"));
    assert!(!clients.applies_to("channel.signal"));
    let _ = clients.signal_client();
    let proxied_signal = ConfiguredHttpClients::new(HttpProxyPolicy {
        service_selectors: vec!["channel.signal".into()],
        ..Default::default()
    })
    .unwrap();
    let _ = proxied_signal.signal_client();
}

// ---------------------------------------------------------------------------
// WhatsApp native builders
// ---------------------------------------------------------------------------

fn cloud_config(app_secret: Option<&str>) -> ChannelsConfig {
    ChannelsConfig {
        whatsapp: Some(WhatsAppConfig {
            access_token: Some("token".into()),
            phone_number_id: Some("123".into()),
            verify_token: Some("verify".into()),
            app_secret: app_secret.map(str::to_owned),
            allowed_numbers: vec!["*".into()],
            ..whatsapp_blank()
        }),
        ..Default::default()
    }
}

fn whatsapp_blank() -> WhatsAppConfig {
    WhatsAppConfig {
        access_token: None,
        phone_number_id: None,
        verify_token: None,
        app_secret: None,
        session_path: None,
        pair_phone: None,
        pair_code: None,
        allowed_numbers: Vec::new(),
    }
}

fn signed(secret: &str, body: &[u8]) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// The Meta app secret in the config must reach the Cloud runtime, or no
/// webhook POST could ever be authenticated.
#[tokio::test]
async fn the_cloud_builder_carries_the_app_secret_into_the_webhook_authenticator() {
    use crate::providers::whatsapp_shared::fakes::CollectingSink;
    let sink = Arc::new(CollectingSink::new());
    let host: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .events(sink.clone())
        .build();
    let body = serde_json::to_vec(&serde_json::json!({
        "object": "whatsapp_business_account",
        "entry": [{"changes": [{"field": "messages", "value": {
            "metadata": {"phone_number_id": "123"},
            "messages": [{"from": "15550001111", "id": "wamid.1", "timestamp": "1", "type": "text", "text": {"body": "hi"}}]
        }}]}]
    }))
    .unwrap();

    let with_secret =
        build_whatsapp_cloud_channel(&cloud_config(Some("s3cret")), &host, &DefaultHttpClients)
            .expect("a complete Cloud config builds");
    let outcome = crate::channel::ChannelAdapter::deliver_webhook(
        with_secret.as_ref(),
        Some(&signed("s3cret", &body)),
        &body,
    )
    .await
    .unwrap();
    assert_eq!(outcome.messages, 1);

    let without_secret =
        build_whatsapp_cloud_channel(&cloud_config(None), &host, &DefaultHttpClients).unwrap();
    assert!(
        crate::channel::ChannelAdapter::deliver_webhook(
            without_secret.as_ref(),
            Some(&signed("s3cret", &body)),
            &body
        )
        .await
        .is_err(),
        "a verify token is not a POST signature, and no secret means no ingress"
    );
}

#[test]
fn the_cloud_builder_wires_the_host_ports_into_the_declared_capabilities() {
    use crate::providers::whatsapp_shared::fakes::{MemoryMedia, MemoryVoices};
    let bare =
        build_whatsapp_cloud_channel(&cloud_config(None), &host(), &DefaultHttpClients).unwrap();
    assert!(
        !bare.declared_capabilities().content.image.outbound
            && !bare.declared_capabilities().voice_out.supported
    );
    let wired_host: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .media_repository(Arc::new(MemoryMedia::default()))
        .voice_media_store(Arc::new(MemoryVoices::default()))
        .build();
    let wired = build_whatsapp_cloud_channel(&cloud_config(None), &wired_host, &DefaultHttpClients)
        .unwrap();
    assert!(wired.declared_capabilities().content.image.outbound);
    assert!(wired.declared_capabilities().voice_out.supported);
    // Anything but a complete Cloud stanza is not a Cloud channel.
    let incomplete = ChannelsConfig {
        whatsapp: Some(WhatsAppConfig {
            phone_number_id: Some("123".into()),
            ..whatsapp_blank()
        }),
        ..Default::default()
    };
    assert!(build_whatsapp_cloud_channel(&incomplete, &host(), &DefaultHttpClients).is_none());
    assert!(
        build_whatsapp_cloud_channel(&ChannelsConfig::default(), &host(), &DefaultHttpClients)
            .is_none()
    );
}

#[cfg(feature = "whatsapp-web")]
#[test]
fn the_web_builder_wires_ports_and_refuses_cloud_shaped_stanzas() {
    use crate::providers::whatsapp_shared::fakes::{MemoryMedia, MemoryVoices};
    let web = |phone_number_id: Option<&str>, session: Option<&str>| ChannelsConfig {
        whatsapp: Some(WhatsAppConfig {
            session_path: session.map(str::to_owned),
            phone_number_id: phone_number_id.map(str::to_owned),
            ..whatsapp_blank()
        }),
        ..Default::default()
    };
    let wired_host: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .media_repository(Arc::new(MemoryMedia::default()))
        .voice_media_store(Arc::new(MemoryVoices::default()))
        .build();
    let channel = build_whatsapp_web_channel(&web(None, Some("label")), &wired_host).unwrap();
    assert!(
        tinychannels_runtime::channel::ChannelAdapter::declared_capabilities(channel.as_ref())
            .content
            .image
            .outbound
    );
    assert!(build_whatsapp_web_channel(&web(None, None), &wired_host).is_none());
    // A stanza that names a phone number id is a Cloud stanza, never a session.
    assert!(build_whatsapp_web_channel(&web(Some("123"), Some("label")), &wired_host).is_none());
    assert!(build_whatsapp_web_channel(&ChannelsConfig::default(), &wired_host).is_none());
}

#[test]
fn the_slack_stanza_builds_a_socket_mode_provider_only_when_configured() {
    let none = ChannelsConfig::default();
    assert!(build_slack_channel(&none, &host(), &DefaultHttpClients).is_none());
    let config: ChannelsConfig = serde_json::from_value(serde_json::json!({
        "slack": {"bot_token": "token", "app_token": "app", "channel_id": "C1", "allowed_users": ["*"]}
    }))
    .unwrap();
    let slack = build_slack_channel(&config, &host(), &DefaultHttpClients).unwrap();
    assert_eq!(slack.descriptor().id, "slack");
    assert_eq!(slack.proactive_target().as_deref(), Some("C1"));
    let capabilities = slack.declared_capabilities();
    assert!(capabilities.addressing.thread_id && capabilities.reactions);
    assert!(
        !capabilities.content.image.outbound,
        "no media port on the no-op host"
    );
}

#[test]
fn dingtalk_factory_wires_host_voice_store_and_ordered_ingress() {
    use crate::providers::native::fixtures::{RecordingEvents, StubVoice};
    let config = ChannelsConfig {
        dingtalk: Some(tinychannels_bus::config::DingTalkConfig {
            client_id: "app-key".into(),
            client_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
        }),
        ..Default::default()
    };
    let bare = build_long_tail_channel_with_host(&config, &host(), &DefaultHttpClients, "dingtalk")
        .expect("configured App Bot should build");
    assert!(!bare.declared_capabilities().voice_in.supported);

    let wired_host: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .voice_media_store(Arc::new(StubVoice::default()))
        .events(Arc::new(RecordingEvents::delivering()))
        .build();
    let wired =
        build_long_tail_channel_with_host(&config, &wired_host, &DefaultHttpClients, "dingtalk")
            .expect("configured App Bot should build with host ports");
    let capabilities = wired.declared_capabilities();
    assert!(capabilities.voice_in.supported);
    assert_eq!(capabilities.voice_in.max_size_bytes, Some(10 * 1024 * 1024));
    assert!(capabilities.voice_out.supported);
    assert!(!capabilities.voice_out.synthesis);

    let with_codec: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .voice_media_store(Arc::new(StubVoice::default()))
        .voice_codec(Arc::new(FactoryVoiceCodec))
        .build();
    let codec_only =
        build_long_tail_channel_with_host(&config, &with_codec, &DefaultHttpClients, "dingtalk")
            .unwrap();
    assert!(codec_only.declared_capabilities().voice_out.supported);
    assert!(!codec_only.declared_capabilities().voice_out.synthesis);

    let with_synthesizer: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .voice_media_store(Arc::new(StubVoice::default()))
        .synthesizer(Arc::new(FactorySpeechSynthesizer))
        .build();
    let synthesizer_only = build_long_tail_channel_with_host(
        &config,
        &with_synthesizer,
        &DefaultHttpClients,
        "dingtalk",
    )
    .unwrap();
    assert!(synthesizer_only.declared_capabilities().voice_out.supported);
    assert!(!synthesizer_only.declared_capabilities().voice_out.synthesis);

    let complete_voice_host: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .voice_media_store(Arc::new(StubVoice::default()))
        .synthesizer(Arc::new(FactorySpeechSynthesizer))
        .voice_codec(Arc::new(FactoryVoiceCodec))
        .build();
    let voice_ready = build_long_tail_channel_with_host(
        &config,
        &complete_voice_host,
        &DefaultHttpClients,
        "dingtalk",
    )
    .unwrap();
    assert!(voice_ready.declared_capabilities().voice_out.supported);
    assert!(voice_ready.declared_capabilities().voice_out.synthesis);

    let without_store: Arc<dyn ChannelHost> = crate::ChannelHostBuilder::new()
        .synthesizer(Arc::new(FactorySpeechSynthesizer))
        .voice_codec(Arc::new(FactoryVoiceCodec))
        .build();
    let no_storage =
        build_long_tail_channel_with_host(&config, &without_store, &DefaultHttpClients, "dingtalk")
            .unwrap();
    assert!(!no_storage.declared_capabilities().voice_out.supported);
    assert!(!no_storage.declared_capabilities().voice_out.synthesis);
}

struct FactorySpeechSynthesizer;

#[async_trait::async_trait]
impl crate::host::SpeechSynthesizer for FactorySpeechSynthesizer {
    async fn synthesize(
        &self,
        _request: crate::host::SpeechRequest,
    ) -> anyhow::Result<crate::host::SpeechResult> {
        anyhow::bail!("factory capability test does not synthesize audio")
    }
}

struct FactoryVoiceCodec;

#[async_trait::async_trait]
impl crate::host::VoiceCodec for FactoryVoiceCodec {
    async fn transcode(
        &self,
        _request: crate::host::VoiceCodecRequest,
    ) -> anyhow::Result<crate::host::VoiceCodecResult> {
        anyhow::bail!("factory capability test does not transcode audio")
    }
}
