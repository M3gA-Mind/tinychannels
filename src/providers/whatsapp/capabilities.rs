//! Capability facts derived from the configured WhatsApp Cloud account.

use super::channel::WhatsAppChannel;
use crate::providers::whatsapp_shared::capabilities::{CapabilitySpec, capabilities};
use tinychannels_bus::ChannelCapabilities;

pub(super) fn declared(channel: &WhatsAppChannel) -> ChannelCapabilities {
    capabilities(&CapabilitySpec {
        media: channel.media_repository.is_some(),
        voice_store: channel.voice_media_store.is_some(),
        voice_synthesis: channel.voice_synthesis && channel.voice_media_store.is_some(),
        native_waveform: false,
        edit_delete: false,
        limits: WhatsAppChannel::media_limits(),
        text_limit: super::outbound::TEXT_LIMIT as u64,
    })
}
