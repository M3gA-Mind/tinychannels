//! Live verification of email (IMAP) credentials before they are persisted.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use super::email_channel::{EmailChannel, EmailConfig};

/// Outer bound on the IMAP login probe. The channel's own health check has an
/// inner budget; this cap also lets the caller tell a timeout from a failed
/// login.
pub const EMAIL_VERIFY_TIMEOUT: Duration = Duration::from_secs(20);

/// Live-verify IMAP credentials by attempting a login, so a wrong host or
/// password fails at connect time instead of wedging the listener on the next
/// restart. Returns a user-facing error message on failure.
pub async fn verify_email_credentials(cfg: &EmailConfig) -> Result<(), String> {
    let probe = EmailChannel::new(cfg.clone());
    verify_with_probe(
        Box::pin(probe.health_probe()),
        &cfg.imap_host,
        EMAIL_VERIFY_TIMEOUT,
    )
    .await
}

/// Maps a login probe's outcome, bounded by `limit`, to the user-facing result.
async fn verify_with_probe(
    probe: Pin<Box<dyn Future<Output = bool> + Send + '_>>,
    imap_host: &str,
    limit: Duration,
) -> Result<(), String> {
    match tokio::time::timeout(limit, probe).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(
            "IMAP connection failed — check the host, port, email address, and app password"
                .to_string(),
        ),
        Err(_) => Err(format!(
            "IMAP connection to {imap_host} timed out — check the host and port"
        )),
    }
}

#[cfg(test)]
#[path = "email_verify_tests.rs"]
mod tests;
