use super::*;
use tinychannels::factory::DefaultHttpClients;
use tinychannels::host::NoopHost;
use tinychannels_bus::config::{IMessageConfig, SignalConfig};

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
    assert_eq!(launch.channel.name(), "signal");
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
fn a_configured_imessage_account_builds_a_native_launch() {
    let host = NoopHost::arc();
    let config = ChannelsConfig {
        imessage: Some(IMessageConfig {
            allowed_contacts: vec!["*".into()],
        }),
        ..Default::default()
    };
    let launch = build_native_v2("imessage", &config, &host, &DefaultHttpClients).unwrap();
    assert_eq!(launch.channel.name(), "imessage");
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
