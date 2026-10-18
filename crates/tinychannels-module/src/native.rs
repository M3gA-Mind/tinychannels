//! Providers that run as native v2 adapters (Signal, iMessage, Linq, Email):
//! the module builds them through the root crate's factories and runs them on
//! the same ordered operation queue as Telegram and Slack.

use crate::provider::NativePorts;
use std::sync::Arc;
use tinychannels::Channel;
use tinychannels::factory::HttpClientFactory;
use tinychannels::host::ChannelHost;
use tinychannels_bus::ChannelsConfig;

/// A built native provider and the typed ports the running actor routes to.
pub(crate) struct NativeLaunch {
    pub(crate) channel: Arc<dyn Channel>,
    pub(crate) ports: NativePorts,
}

/// Build the native v2 provider `name` names, when it is one and configured.
pub(crate) fn build_native_v2(
    name: &str,
    config: &ChannelsConfig,
    host: &Arc<dyn ChannelHost>,
    http: &dyn HttpClientFactory,
) -> Option<NativeLaunch> {
    match tinychannels_bus::provider_id(name) {
        "signal" => {
            let channel = tinychannels::factory::build_signal_channel(config, host, http)?;
            Some(NativeLaunch {
                channel: channel.clone(),
                ports: NativePorts::native(channel, "Signal"),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
