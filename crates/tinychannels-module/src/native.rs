//! Providers that run as native v2 adapters (Signal, iMessage, Linq, Email):
//! the module builds them through the root crate's factories and runs them on
//! the same ordered operation queue as Telegram and Slack.

use crate::host::HostChannels;
use crate::provider::{Hooks, LongTailListenerMode, Running};
use std::sync::Arc;
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
    pub(crate) provider_id: String,
    start: Start,
}

impl NativeLaunch {
    /// A long-tail text provider held as its trait object.
    fn long_tail(
        provider: &'static str,
        channel: Arc<dyn tinychannels::LongTailNative>,
        listener_mode: LongTailListenerMode,
    ) -> Self {
        Self {
            provider_id: channel.descriptor().id,
            start: Box::new(move |hooks, host, supervision, outbound_only| {
                Running::start_long_tail(
                    provider,
                    channel,
                    hooks,
                    host,
                    supervision,
                    outbound_only,
                    listener_mode,
                )
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

/// Display name of a long-tail text provider that serves the v2 adapter
/// traits itself, or `None` for every other provider.
pub(crate) fn long_tail_display(provider: &str) -> Option<&'static str> {
    match provider {
        "lark" => Some("Lark"),
        "mattermost" => Some("Mattermost"),
        "irc" => Some("IRC"),
        "qq" => Some("QQ"),
        "dingtalk" => Some("DingTalk"),
        "yuanbao" => Some("Yuanbao"),
        _ => None,
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
        "discord" => {
            tinychannels::factory::build_discord_channel(config, host, http).map(|channel| {
                let adapter: Arc<dyn tinychannels::LongTailNative> = channel;
                NativeLaunch::long_tail("Discord", adapter, LongTailListenerMode::Adapter)
            })
        }
        "signal" => {
            tinychannels::factory::build_signal_channel(config, host, http).map(|channel| {
                let adapter: Arc<dyn tinychannels::LongTailNative> = channel;
                NativeLaunch::long_tail("Signal", adapter, LongTailListenerMode::Adapter)
            })
        }
        "imessage" => tinychannels::factory::build_imessage_channel(config, host).map(|channel| {
            let adapter: Arc<dyn tinychannels::LongTailNative> = channel;
            NativeLaunch::long_tail("iMessage", adapter, LongTailListenerMode::Adapter)
        }),
        "linq" => tinychannels::factory::build_linq_channel(config, host, http).map(|channel| {
            let adapter: Arc<dyn tinychannels::LongTailNative> = channel;
            NativeLaunch::long_tail("Linq", adapter, LongTailListenerMode::Webhook)
        }),
        "email" => tinychannels::factory::build_email_channel(config, host).map(|channel| {
            NativeLaunch::long_tail("Email", channel, LongTailListenerMode::Adapter)
        }),
        id => {
            let display = long_tail_display(id)?;
            let listener_mode = match id {
                "irc" | "lark" | "mattermost" | "qq" | "dingtalk" | "yuanbao" => {
                    LongTailListenerMode::Adapter
                }
                _ => LongTailListenerMode::Adapter,
            };
            tinychannels::factory::build_long_tail_channel_with_host(config, host, http, id)
                .map(|channel| NativeLaunch::long_tail(display, channel, listener_mode))
        }
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
