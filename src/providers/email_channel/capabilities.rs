//! Capability facts for a configured email account.

use super::EmailChannel;
use super::native::{EMAIL_MAX_FILE_BYTES, EMAIL_MAX_VOICE_BYTES};
use super::{HEALTH_CHECK_TIMEOUT, ImapStream};
use crate::providers::native::capabilities::{media_capabilities, voice_capabilities};
use anyhow::Result;
use async_imap::Session;
use std::time::Duration;
use tinychannels_bus::{
    ActionCapabilities, AddressingCapabilities, ChannelCapabilities, ContentCapabilities,
    ContentCapability,
};
use tokio::time::{Instant, timeout_at};
use tracing::debug;

impl EmailChannel {
    #[cfg(feature = "email")]
    pub(crate) async fn health_probe(&self) -> bool {
        self.health_check_with(self.connect_imap(), HEALTH_CHECK_TIMEOUT)
            .await
    }

    /// Translate the bounded IMAP probe into the status vocabulary used by
    /// native adapters, keeping Email's real health semantics on the typed
    /// path as well as the legacy compatibility path.
    pub(super) async fn typed_health_state<S: ImapStream>(
        &self,
        connect: impl std::future::Future<Output = Result<Session<S>>>,
        limit: Duration,
    ) -> &'static str {
        if self.health_check_with(connect, limit).await {
            "healthy"
        } else {
            "unhealthy"
        }
    }

    /// Bounded IMAP login probe over any session source.
    async fn health_check_with<S: ImapStream>(
        &self,
        connect: impl std::future::Future<Output = Result<Session<S>>>,
        limit: Duration,
    ) -> bool {
        let deadline = Instant::now() + limit;
        match timeout_at(deadline, connect).await {
            Ok(Ok(mut session)) => {
                let _ = timeout_at(deadline, session.logout()).await;
                true
            }
            Ok(Err(error)) => {
                debug!("Health check failed: {error}");
                false
            }
            Err(_) => {
                debug!("Health check timed out");
                false
            }
        }
    }

    pub(crate) fn declared(&self) -> ChannelCapabilities {
        let media_enabled = self.native.media_repository.is_some();
        let voice_enabled = self.native.voice_media_store.is_some();
        let mut parts: Vec<String> = vec!["text".into()];
        let mut content = ContentCapabilities {
            text: ContentCapability {
                inbound: true,
                outbound: true,
                ..Default::default()
            },
            ..Default::default()
        };
        if media_enabled {
            parts.extend(["image", "video", "audio", "file"].map(String::from));
            media_capabilities(&mut content, EMAIL_MAX_FILE_BYTES);
        }
        if voice_enabled {
            parts.push("voice_note".into());
        }
        content.parts = parts;
        let voice = |synthesis| {
            voice_capabilities(
                voice_enabled,
                synthesis,
                EMAIL_MAX_VOICE_BYTES,
                &["m4a_aac", "mp3", "ogg_opus"],
            )
        };
        ChannelCapabilities {
            remote_control: true,
            chat_approvals: true,
            // `In-Reply-To` carries the reply target and `References` the thread.
            addressing: AddressingCapabilities {
                reply_to_id: true,
                thread_id: true,
                ..Default::default()
            },
            content,
            actions: ActionCapabilities {
                supported: ["send", "reply"].map(String::from).into(),
                ..Default::default()
            },
            voice_in: voice(false),
            voice_out: voice(self.native.voice_synthesis),
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
