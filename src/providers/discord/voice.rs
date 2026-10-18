//! Outbound Discord voice messages: OGG/Opus bytes, duration and waveform.

use super::channel::DiscordChannel;
use super::http::{pre_dispatch, unsupported};
use super::inbound::MAX_VOICE_BYTES;
use super::outbound::Target;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use tinychannels_bus::channel::ChannelSendError;
use tinychannels_bus::{ChannelOutboundMessage, MediaReference};

/// Discord stores at most 256 waveform samples per voice message.
const MAX_WAVEFORM_SAMPLES: usize = 256;
/// Used only when no codec can measure the audio.
const FALLBACK_WAVEFORM_SAMPLES: usize = 64;

/// A voice message ready to upload.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct VoiceUpload {
    pub(super) bytes: Vec<u8>,
    pub(super) duration_secs: f64,
    pub(super) waveform_base64: String,
}

/// Encode 0.0..=1.0 samples as Discord's base64 byte waveform.
pub(super) fn encode_waveform(samples: &[f32]) -> String {
    let step = samples.len().div_ceil(MAX_WAVEFORM_SAMPLES).max(1);
    let bytes: Vec<u8> = samples
        .chunks(step)
        .map(|chunk| {
            let peak = chunk.iter().copied().fold(0.0_f32, f32::max);
            (peak.clamp(0.0, 1.0) * 255.0).round() as u8
        })
        .collect();
    BASE64.encode(bytes)
}

/// Duration of an Ogg Opus stream from its last page's granule position.
pub(super) fn ogg_opus_duration_ms(bytes: &[u8]) -> Option<u64> {
    if !bytes.starts_with(b"OggS") {
        return None;
    }
    let head = bytes.windows(8).position(|window| window == b"OpusHead")?;
    let pre_skip = u64::from(u16::from_le_bytes([
        *bytes.get(head + 10)?,
        *bytes.get(head + 11)?,
    ]));
    let last_page = bytes.windows(4).rposition(|window| window == b"OggS")?;
    let granule: [u8; 8] = bytes.get(last_page + 6..last_page + 14)?.try_into().ok()?;
    let granule = u64::from_le_bytes(granule);
    if granule == u64::MAX || granule <= pre_skip {
        return None;
    }
    Some((granule - pre_skip) * 1000 / 48_000)
}

fn codec_for_mime(mime: &str) -> Option<&'static str> {
    match mime {
        "audio/ogg" => Some("ogg_opus"),
        "audio/mp4" => Some("m4a_aac"),
        "audio/mpeg" => Some("mp3"),
        "audio/wav" | "audio/wave" => Some("wav_pcm16"),
        _ => None,
    }
}

impl DiscordChannel {
    /// Resolve, validate and measure the audio for one voice message.
    #[cfg(test)]
    pub(super) async fn prepare_voice(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
        media: &MediaReference,
        inline_audio: Option<&str>,
        codec: Option<&str>,
        duration_ms: Option<u64>,
    ) -> Result<VoiceUpload, ChannelSendError> {
        self.prepare_voice_with_waveform(
            message,
            target,
            media,
            inline_audio,
            codec,
            duration_ms,
            None,
        )
        .await
    }

    /// As [`Self::prepare_voice`], preferring a waveform the caller measured.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare_voice_with_waveform(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
        media: &MediaReference,
        inline_audio: Option<&str>,
        codec: Option<&str>,
        duration_ms: Option<u64>,
        supplied_waveform: Option<&[f32]>,
    ) -> Result<VoiceUpload, ChannelSendError> {
        let store = self
            .voice_media_store
            .as_ref()
            .ok_or_else(|| unsupported("Discord voice messages require host voice storage"))?;
        let scope = self.media_scope(message, target)?;
        let (audio_base64, mime) = match inline_audio {
            Some(audio) => (
                audio.to_owned(),
                media
                    .content_type
                    .clone()
                    .unwrap_or_else(|| "audio/ogg".into()),
            ),
            None => {
                let opaque_id = media
                    .opaque_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| pre_dispatch("voice media has no host-owned reference"))?;
                let resolved = store
                    .read_voice(scope, opaque_id.to_owned())
                    .await
                    .map_err(|error| {
                        tracing::debug!(%error, "[discord] voice reference could not be resolved");
                        pre_dispatch("Discord voice reference could not be resolved")
                    })?;
                (resolved.audio_base64, resolved.mime_type)
            }
        };
        let mut bytes = BASE64
            .decode(audio_base64.as_bytes())
            .map_err(|_| pre_dispatch("Discord voice audio is not valid base64"))?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_VOICE_BYTES {
            return Err(pre_dispatch("Discord voice audio exceeds its size limit"));
        }

        let is_ogg = codec == Some("ogg_opus") || mime == "audio/ogg" || bytes.starts_with(b"OggS");
        let mut duration = duration_ms.or(media.duration_ms);
        let mut waveform: Option<Vec<f32>> = supplied_waveform.map(<[f32]>::to_vec);
        if let Some(host_codec) = self.voice_codec.as_ref() {
            let from = if is_ogg {
                Some("ogg_opus")
            } else {
                codec_for_mime(&mime)
            };
            if let Some(from) = from {
                // Re-encoding is only used to measure; Ogg input is uploaded as is.
                match host_codec
                    .transcode(crate::host::VoiceCodecRequest {
                        audio_base64: BASE64.encode(&bytes),
                        from: from.into(),
                        target: "ogg_opus".into(),
                    })
                    .await
                {
                    Ok(result) => {
                        if !is_ogg {
                            bytes = BASE64
                                .decode(result.audio_base64.as_bytes())
                                .map_err(|_| pre_dispatch("Discord voice transcode was invalid"))?;
                        }
                        duration = result.duration_ms.or(duration);
                        waveform = waveform.or(result.waveform);
                    }
                    Err(error) => {
                        tracing::debug!(%error, "[discord] voice codec failed");
                        if !is_ogg {
                            return Err(pre_dispatch(
                                "Discord voice messages must be OGG/Opus and conversion failed",
                            ));
                        }
                    }
                }
            }
        }
        if !bytes.starts_with(b"OggS") {
            return Err(pre_dispatch("Discord voice messages must be OGG/Opus"));
        }
        if bytes.len() as u64 > MAX_VOICE_BYTES {
            return Err(pre_dispatch("Discord voice audio exceeds its size limit"));
        }
        let duration_ms = duration
            .or_else(|| ogg_opus_duration_ms(&bytes))
            .filter(|ms| *ms > 0)
            .ok_or_else(|| pre_dispatch("Discord voice duration could not be determined"))?;
        let samples = waveform
            .filter(|samples| !samples.is_empty())
            .unwrap_or_else(|| vec![0.5; FALLBACK_WAVEFORM_SAMPLES]);
        Ok(VoiceUpload {
            bytes,
            duration_secs: duration_ms as f64 / 1000.0,
            waveform_base64: encode_waveform(&samples),
        })
    }
}

#[cfg(test)]
#[path = "voice_tests.rs"]
mod tests;
