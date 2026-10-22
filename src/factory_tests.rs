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
