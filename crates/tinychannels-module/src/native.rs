//! Providers that run as native v2 adapters (Signal, iMessage, Linq, Email):
//! the module builds them through the root crate's factories and runs them on
//! the same ordered operation queue as Telegram and Slack.

use crate::host::HostChannels;
use crate::provider::{Hooks, Running};
use std::sync::Arc;
use tinychannels::Channel;
use tinychannels::factory::HttpClientFactory;
use tinychannels::host::ChannelHost;
use tinychannels_bus::ChannelsConfig;

/// Starts a built native provider on its ordered operation queue.
type Start = Box<
    dyn FnOnce(
            Arc<Hooks>,
            HostChannels,
            Option<tinychannels_bus::ChannelSupervisionPolicy>,
            bool,
        ) -> Arc<Running>
        + Send,
>;

/// A built native provider and the way to start it.
pub(crate) struct NativeLaunch {
    pub(crate) channel: Arc<dyn Channel>,
    start: Start,
}

impl NativeLaunch {
    fn new<T>(provider: &'static str, channel: Arc<T>) -> Self
    where
        T: Channel
            + tinychannels::channel::ChannelAdapter
            + tinychannels::channel::ChannelTypedSend
            + tinychannels::channel::ChannelTyping
            + tinychannels::channel::ChannelReaction
            + tinychannels::channel::ChannelEdit
            + tinychannels::channel::ChannelDelete
            + tinychannels::channel::ChannelStreamingDraft
            + 'static,
    {
        let dynamic: Arc<dyn Channel> = channel.clone();
        Self {
            channel: dynamic,
            start: Box::new(move |hooks, host, supervision, outbound_only| {
                Running::start_native(provider, channel, hooks, host, supervision, outbound_only)
            }),
        }
    }

    pub(crate) fn start(
        self,
        hooks: Arc<Hooks>,
        host: HostChannels,
        supervision: Option<tinychannels_bus::ChannelSupervisionPolicy>,
        outbound_only: bool,
    ) -> Arc<Running> {
        (self.start)(hooks, host, supervision, outbound_only)
    }
}

/// Build the native v2 provider `name` names, when it is one and configured.
pub(crate) fn build_native_v2(
    name: &str,
    config: &ChannelsConfig,
    host: &Arc<dyn ChannelHost>,
    http: &dyn HttpClientFactory,
) -> Option<NativeLaunch> {
    match tinychannels_bus::provider_id(name) {
        "signal" => tinychannels::factory::build_signal_channel(config, host, http)
            .map(|channel| NativeLaunch::new("Signal", channel)),
        "imessage" => tinychannels::factory::build_imessage_channel(config, host)
            .map(|channel| NativeLaunch::new("iMessage", channel)),
        _ => None,
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
