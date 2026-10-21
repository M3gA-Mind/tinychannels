//! Capability facts for a configured WhatsApp Web session.

use super::WhatsAppWebChannel;
use crate::providers::whatsapp_shared::capabilities::{CapabilitySpec, capabilities};
use tinychannels_bus::ChannelCapabilities;

pub(super) fn declared(channel: &WhatsAppWebChannel) -> ChannelCapabilities {
    capabilities(&CapabilitySpec {
        media: channel.media_repository.is_some(),
        voice_store: channel.voice_media_store.is_some(),
        voice_synthesis: channel.voice_synthesis && channel.voice_media_store.is_some(),
        native_waveform: true,
        edit_delete: true,
        limits: super::outbound::MEDIA_LIMITS,
        text_limit: super::outbound::TEXT_LIMIT as u64,
    })
}
