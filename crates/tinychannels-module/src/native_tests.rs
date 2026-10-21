use super::*;
use tinychannels::factory::DefaultHttpClients;
use tinychannels::host::NoopHost;
use tinychannels_bus::config::{EmailConfig, IMessageConfig, LinqConfig, SignalConfig};

#[tokio::test]
async fn a_configured_discord_account_starts_without_retaining_legacy_channel() {
    let host = NoopHost::arc();
    let config = ChannelsConfig {
        discord: Some(tinychannels_bus::config::DiscordConfig {
            bot_token: "token".into(),
            guild_id: Some("guild".into()),
            channel_id: Some("channel".into()),
            allowed_users: vec!["*".into()],
            listen_to_bots: false,
            mention_only: false,
        }),
        ..Default::default()
    };

    let launch = build_native_v2("discord:work", &config, &host, &DefaultHttpClients)
        .expect("a configured Discord provider must start through its native adapter");
    assert_eq!(launch.provider_id, "discord");

    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let _server = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let running = launch.start(
        Arc::new(crate::provider::Hooks::default()),
        crate::host::HostChannels::new(client),
        None,
        true,
    );
    assert_eq!(running.provider_name(), "discord");
    assert_eq!(
        running.adapter_handle().mode,
        crate::provider::AdapterMode::DiscordNativeV2
    );
    running.stop().await.unwrap();
}

fn signal_config() -> ChannelsConfig {
    ChannelsConfig {
        signal: Some(SignalConfig {
            http_url: "http://127.0.0.1:8686".into(),
            account: "+15550000".into(),
            group_id: None,
            allowed_from: vec!["*".into()],
            ignore_attachments: false,
            ignore_stories: false,
        }),
        ..Default::default()
    }
}

#[test]
fn a_configured_signal_account_builds_a_native_launch() {
    let host = NoopHost::arc();
    let launch = build_native_v2("signal", &signal_config(), &host, &DefaultHttpClients).unwrap();
    assert_eq!(launch.provider_id, "signal");
    // An aliased instance name resolves to the same provider.
    assert!(build_native_v2("signal:work", &signal_config(), &host, &DefaultHttpClients).is_some());
}

#[test]
fn unconfigured_and_non_native_providers_build_nothing() {
    let host = NoopHost::arc();
    let empty = ChannelsConfig::default();
    assert!(build_native_v2("signal", &empty, &host, &DefaultHttpClients).is_none());
    assert!(build_native_v2("irc", &signal_config(), &host, &DefaultHttpClients).is_none());
}

#[test]
fn long_tail_providers_have_a_display_name_and_nothing_else_does() {
    for (id, display) in [
        ("lark", "Lark"),
        ("mattermost", "Mattermost"),
        ("irc", "IRC"),
        ("qq", "QQ"),
        ("dingtalk", "DingTalk"),
        ("yuanbao", "Yuanbao"),
    ] {
        assert_eq!(super::long_tail_display(id), Some(display));
    }
    for id in [
        "telegram", "slack", "whatsapp", "discord", "email", "unknown",
    ] {
        assert_eq!(super::long_tail_display(id), None, "{id}");
    }
}

#[test]
fn long_tail_providers_build_as_native_launches_only_when_configured() {
    let host: Arc<dyn ChannelHost> = NoopHost::arc();
    let config = ChannelsConfig {
        dingtalk: Some(tinychannels_bus::config::DingTalkConfig {
            client_id: "id".into(),
            client_secret: "secret".into(),
            allowed_users: vec![],
        }),
        ..Default::default()
    };
    let launch = build_native_v2("dingtalk", &config, &host, &DefaultHttpClients)
        .expect("a configured long-tail provider is a native launch");
    assert_eq!(launch.provider_id, "dingtalk");
    assert!(build_native_v2("qq", &config, &host, &DefaultHttpClients).is_none());
    assert!(build_native_v2("slack", &config, &host, &DefaultHttpClients).is_none());
}

#[test]
fn native_launch_accepts_typed_adapters_without_the_legacy_channel_trait() {
    let host: Arc<dyn ChannelHost> = NoopHost::arc();
    let config = ChannelsConfig {
        dingtalk: Some(tinychannels_bus::config::DingTalkConfig {
            client_id: "id".into(),
            client_secret: "secret".into(),
            allowed_users: vec![],
        }),
        ..Default::default()
    };
    let channel = tinychannels::factory::build_long_tail_channel_with_host(
        &config,
        &host,
        &DefaultHttpClients,
        "dingtalk",
    )
    .expect("DingTalk is a configured typed adapter");

    // LongTailNative intentionally does not implement legacy Channel. Passing
    // its trait object here proves typed long-tail launches keep their boundary.
    let launch = NativeLaunch::long_tail("DingTalk", channel, LongTailListenerMode::Adapter);
    assert_eq!(launch.provider_id, "dingtalk");
}

#[test]
fn a_configured_imessage_account_builds_a_native_launch() {
    let host = NoopHost::arc();
    let config = ChannelsConfig {
        imessage: Some(IMessageConfig {
            allowed_contacts: vec!["*".into()],
        }),
        ..Default::default()
    };
    let launch = build_native_v2("imessage", &config, &host, &DefaultHttpClients).unwrap();
    assert_eq!(launch.provider_id, "imessage");
    assert!(
        build_native_v2(
            "imessage",
            &ChannelsConfig::default(),
            &host,
            &DefaultHttpClients
        )
        .is_none()
    );
}

#[test]
fn a_configured_linq_account_builds_a_native_launch() {
    let host = NoopHost::arc();
    let config = ChannelsConfig {
        linq: Some(LinqConfig {
            api_token: "token".into(),
            from_phone: "+15551234567".into(),
            allowed_senders: vec!["*".into()],
            webhook_secret: Some("secret".into()),
        }),
        ..Default::default()
    };
    let launch = build_native_v2("linq", &config, &host, &DefaultHttpClients).unwrap();
    assert_eq!(launch.provider_id, "linq");
    assert!(
        build_native_v2(
            "linq",
            &ChannelsConfig::default(),
            &host,
            &DefaultHttpClients
        )
        .is_none()
    );
}

#[test]
fn a_configured_email_account_builds_a_native_launch() {
    let host = NoopHost::arc();
    let config = ChannelsConfig {
        email: Some(EmailConfig {
            from_address: "bot@example.com".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let launch = build_native_v2("email", &config, &host, &DefaultHttpClients).unwrap();
    assert_eq!(launch.provider_id, "email");
    assert!(
        build_native_v2(
            "email",
            &ChannelsConfig::default(),
            &host,
            &DefaultHttpClients
        )
        .is_none()
    );
}
