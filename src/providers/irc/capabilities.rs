use super::IrcChannel;

impl IrcChannel {
    pub(super) fn capabilities() -> tinychannels_bus::ChannelCapabilities {
        use crate::providers::long_tail::{TextSpec, text_capabilities};
        text_capabilities(TextSpec::default())
    }
}
