//! Tests for the provider factory.
//!
//! These matter more than their size suggests: this function is the single
//! config-to-provider mapping shared by the OpenHuman core and the
//! `tinychannels-module` cdylib. Before it existed the mapping was inline in
//! the host, so "does a config stanza produce a provider" was only ever
//! answered by starting the real thing.

use crate::NoopHost;
use crate::factory::{
    ConfiguredHttpClients, DefaultHttpClients, HttpClientFactory, build_channels,
};
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
