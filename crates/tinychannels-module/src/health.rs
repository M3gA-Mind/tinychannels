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
    channels: &[tinychannels::factory::BuiltChannel],
    budget: Duration,
) -> Vec<tinychannels_bus::ChannelHealthReport> {
    let mut reports = Vec::with_capacity(channels.len());
    for channel in channels {
        let name = channel.name();
        let state = match tokio::time::timeout(budget, channel.adapter().status()).await {
            Err(_) => tinychannels_bus::ChannelHealthState::Timeout,
            Ok(Ok(snapshot)) if snapshot.health_state.as_deref() == Some("healthy") => {
                tinychannels_bus::ChannelHealthState::Healthy
            }
            _ => tinychannels_bus::ChannelHealthState::Unhealthy,
        };
        reports.push(tinychannels_bus::ChannelHealthReport {
            channel: name,
            state,
        });
    }
    reports
}

#[cfg(test)]
#[path = "health_tests.rs"]
#[allow(
    deprecated,
    reason = "provider test doubles still implement the v21 trait seam"
)]
mod tests;
