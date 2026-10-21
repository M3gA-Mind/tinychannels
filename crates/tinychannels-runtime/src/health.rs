//! Health-state classification shared by typed provider adapters.

pub use tinychannels_bus::ChannelHealthState;

/// Classify a provider health probe under a timeout.
pub fn classify_health_result(
    result: &Result<bool, tokio::time::error::Elapsed>,
) -> ChannelHealthState {
    match result {
        Ok(true) => ChannelHealthState::Healthy,
        Ok(false) => ChannelHealthState::Unhealthy,
        Err(_) => ChannelHealthState::Timeout,
    }
}
