use std::sync::Arc;
use std::time::Duration;

/// Validate a per-provider budget without allowing an unbounded native call.
pub(crate) fn health_budget(milliseconds: u64) -> tinybus::Result<Duration> {
    if !(1..=60_000).contains(&milliseconds) {
        return Err(tinybus::Error::failed(
            "health budget must be between 1 and 60000 milliseconds",
        ));
    }
    Ok(Duration::from_millis(milliseconds))
}

/// Execute provider checks without starting any provider listener.
pub(crate) async fn check_providers(
    channels: &[Arc<dyn tinychannels::Channel>],
    budget: Duration,
) -> Vec<tinychannels_bus::ChannelHealthReport> {
    tinychannels::runtime::check_channels_health(channels, budget)
        .await
        .into_iter()
        .map(|(channel, state)| tinychannels_bus::ChannelHealthReport { channel, state })
        .collect()
}

#[cfg(test)]
#[path = "health_tests.rs"]
mod tests;
