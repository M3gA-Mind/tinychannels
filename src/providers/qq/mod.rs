mod config;
mod inbound;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const QQ_API_BASE: &str = "https://api.sgroup.qq.com";
const QQ_AUTH_URL: &str = "https://bots.qq.com/app/getAppAccessToken";

#[cfg(test)]
fn ensure_https(url: &str) -> anyhow::Result<()> {
    if !url.starts_with("https://") {
        anyhow::bail!(
            "Refusing to transmit sensitive data over non-HTTPS URL: URL scheme must be https"
        );
    }
    Ok(())
}

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("building the default QQ attachment HTTP client should succeed")
}

/// Deduplication set capacity — evict half of entries when full.
const DEDUP_CAPACITY: usize = 10_000;

/// QQ Official Bot channel — uses Tencent's official QQ Bot API with
/// OAuth2 authentication and a Discord-like WebSocket gateway protocol.
pub struct QQChannel {
    app_id: String,
    app_secret: String,
    allowed_users: Vec<String>,
    /// Cached access token + expiry timestamp.
    token_cache: Arc<RwLock<Option<(String, u64)>>>,
    /// Message deduplication set.
    dedup: Arc<RwLock<HashSet<String>>>,
    http_client: reqwest::Client,
    /// Attachment downloads never follow redirects; QQ attachment URLs are
    /// signed platform URLs and must not be usable as arbitrary fetch proxies.
    attachment_http_client: reqwest::Client,
    /// Host-scoped media and voice stores used by native typed sends.
    native: crate::providers::native::HostPorts,
    /// Single-owner cancellation for the public typed gateway listener.
    listener: std::sync::Mutex<Option<(Uuid, tokio::sync::watch::Sender<bool>)>>,
}

struct ListenerRegistration<'a> {
    channel: &'a QQChannel,
    id: Uuid,
}

impl Drop for ListenerRegistration<'_> {
    fn drop(&mut self) {
        let mut listener = self
            .channel
            .listener
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if listener.as_ref().is_some_and(|(id, _)| *id == self.id) {
            *listener = None;
        }
    }
}

async fn listener_cancelled(cancellation: &mut Option<tokio::sync::watch::Receiver<bool>>) {
    match cancellation {
        Some(receiver) => {
            let _ = receiver.changed().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn until_listener_cancelled<F: Future>(
    cancellation: &mut Option<tokio::sync::watch::Receiver<bool>>,
    future: F,
) -> Option<F::Output> {
    tokio::select! {
        biased;
        () = listener_cancelled(cancellation) => None,
        output = future => Some(output),
    }
}

mod capabilities;
mod media;
mod outbound;

#[path = "native.rs"]
mod native;

crate::providers::native::native_v2_adapter!(QQChannel);

#[cfg(test)]
#[path = "qq_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "native_tests.rs"]
mod native_tests;
