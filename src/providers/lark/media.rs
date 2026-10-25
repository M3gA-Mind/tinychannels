//! Lark upload, media conversion, and provider-specific payload preparation.

use super::outbound::known_unsent;
use super::*;
use crate::providers::native::FileStep;
use base64::Engine as _;
use tinychannels_bus::channel::ChannelSendError;

impl LarkChannel {
    pub(super) async fn upload_image(&self, file: &FileStep) -> Result<String, ChannelSendError> {
        let image = reqwest::multipart::Part::bytes(file.bytes.clone())
            .file_name(file.name.clone())
            .mime_str(&file.mime)
            .map_err(|_| known_unsent("Lark image MIME type is invalid"))?;
        let form = reqwest::multipart::Form::new()
            .text("image_type", "message")
            .part("image", image);
        let token = self.upload_token().await?;
        let response = self
            .http_client()
            .post(format!("{}/im/v1/images", self.api_base()))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .map_err(|_| known_unsent("Lark image upload failed before message delivery"))?;
        if !response.status().is_success() {
            return Err(known_unsent("Lark image upload was rejected"));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| known_unsent("Lark image upload returned an invalid response"))?;
        if body.get("code").and_then(serde_json::Value::as_i64) != Some(0) {
            return Err(known_unsent("Lark image upload was rejected"));
        }
        body.pointer("/data/image_key")
            .and_then(serde_json::Value::as_str)
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| known_unsent("Lark image upload returned no image key"))
    }

    pub(super) async fn upload_file(
        &self,
        bytes: &[u8],
        name: &str,
        file_type: &str,
        duration_ms: Option<u64>,
    ) -> Result<String, ChannelSendError> {
        if bytes.is_empty() || bytes.len() as u64 > FILE_LIMIT {
            return Err(known_unsent(
                "Lark file is empty or exceeds the 30 MiB limit",
            ));
        }
        let file = reqwest::multipart::Part::bytes(bytes.to_vec()).file_name(name.to_owned());
        let mut form = reqwest::multipart::Form::new()
            .text("file_type", file_type.to_owned())
            .text("file_name", name.to_owned())
            .part("file", file);
        if let Some(duration) = duration_ms {
            form = form.text("duration", duration.to_string());
        }
        let token = self.upload_token().await?;
        let response = self
            .http_client()
            .post(format!("{}/im/v1/files", self.api_base()))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .map_err(|_| known_unsent("Lark file upload failed before message delivery"))?;
        if !response.status().is_success() {
            return Err(known_unsent("Lark file upload was rejected"));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| known_unsent("Lark file upload returned an invalid response"))?;
        if body.get("code").and_then(serde_json::Value::as_i64) != Some(0) {
            return Err(known_unsent("Lark file upload was rejected"));
        }
        body.pointer("/data/file_key")
            .and_then(serde_json::Value::as_str)
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| known_unsent("Lark file upload returned no file key"))
    }

    pub(super) async fn upload_token(&self) -> Result<String, ChannelSendError> {
        self.get_tenant_access_token()
            .await
            .map_err(|_| known_unsent("Lark authentication failed before upload"))
    }

    pub(super) async fn opus_audio(
        &self,
        file: &FileStep,
    ) -> Result<(Vec<u8>, String), ChannelSendError> {
        let from = match file.mime.as_str() {
            "audio/ogg" | "audio/opus" => "ogg_opus",
            "audio/mp4" | "audio/m4a" | "audio/x-m4a" => "m4a_aac",
            "audio/mpeg" | "audio/mp3" => "mp3",
            "audio/wav" | "audio/wave" => "wav_pcm16",
            _ => return Err(crate::providers::native::unsupported("Lark audio codec")),
        };
        let bytes = if from == "ogg_opus" {
            if !is_ogg_opus(&file.bytes) {
                return Err(crate::providers::native::pre_dispatch(
                    "Lark audio must contain an Ogg Opus stream",
                ));
            }
            file.bytes.clone()
        } else {
            let codec = self.voice_codec.as_ref().ok_or_else(|| {
                crate::providers::native::pre_dispatch(
                    "Lark requires the host voice codec to convert audio to OPUS",
                )
            })?;
            let encoded = codec
                .transcode(crate::host::VoiceCodecRequest {
                    audio_base64: base64::engine::general_purpose::STANDARD.encode(&file.bytes),
                    from: from.into(),
                    target: "ogg_opus".into(),
                })
                .await
                .map_err(|_| {
                    crate::providers::native::pre_dispatch("Lark audio conversion failed")
                })?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded.audio_base64)
                .ok()
                .filter(|bytes| !bytes.is_empty() && bytes.len() as u64 <= FILE_LIMIT)
                .ok_or_else(|| {
                    crate::providers::native::pre_dispatch(
                        "Lark audio conversion returned invalid or oversized data",
                    )
                })?;
            if !is_ogg_opus(&bytes) {
                return Err(crate::providers::native::pre_dispatch(
                    "Lark audio conversion did not return Ogg Opus",
                ));
            }
            bytes
        };
        if bytes.is_empty() || bytes.len() as u64 > FILE_LIMIT {
            return Err(known_unsent(
                "Lark audio is empty or exceeds the 30 MiB limit",
            ));
        }
        let name = if file.name.to_ascii_lowercase().ends_with(".opus") {
            file.name.clone()
        } else {
            format!(
                "{}.opus",
                file.name
                    .rsplit_once('.')
                    .map_or(file.name.as_str(), |(stem, _)| stem)
            )
        };
        Ok((bytes, name))
    }
}

/// Check the Opus identification header in the first Ogg packet. This proves
/// the declared codec, not that every later audio packet decodes successfully.
pub(super) fn is_ogg_opus(bytes: &[u8]) -> bool {
    if bytes.len() < 27 || &bytes[..4] != b"OggS" || bytes[4] != 0 {
        return false;
    }
    let segment_count = bytes[26] as usize;
    let Some(lacing) = bytes.get(27..27 + segment_count) else {
        return false;
    };
    let mut packet_len = 0usize;
    for segment in lacing {
        packet_len += *segment as usize;
        if *segment < 255 {
            let packet = &bytes[27 + segment_count..];
            let header = &packet[..packet_len.min(packet.len())];
            if packet.len() < packet_len || header.len() < 19 || &header[..8] != b"OpusHead" {
                return false;
            }
            let channels = header[9];
            let mapping_family = header[18];
            return header[8] <= 15 && channels > 0 && (mapping_family != 0 || channels <= 2);
        }
    }
    false
}

pub(super) fn file_type_for_mime(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "pdf",
        "application/msword"
        | "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "doc",
        "application/vnd.ms-excel"
        | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xls",
        "application/vnd.ms-powerpoint"
        | "application/vnd.openxmlformats-officedocument.presentationml.presentation" => "ppt",
        _ => "stream",
    }
}

impl LarkChannel {
    pub(super) async fn prepare_native_steps(
        &self,
        _target: &crate::providers::native::Target,
        steps: &mut [crate::providers::native::Step],
    ) -> Result<(), ChannelSendError> {
        for step in steps {
            let is_voice = matches!(step, crate::providers::native::Step::Voice(_));
            let file = match step {
                crate::providers::native::Step::Text(_) => continue,
                crate::providers::native::Step::File(file)
                    if file.kind == tinychannels_bus::MediaKind::Image =>
                {
                    if file.bytes.len() as u64 > super::IMAGE_LIMIT {
                        return Err(crate::providers::native::pre_dispatch(
                            "Lark image exceeds the 10 MiB limit",
                        ));
                    }
                    if !matches!(
                        file.mime.as_str(),
                        "image/jpeg" | "image/png" | "image/gif" | "image/webp"
                    ) {
                        return Err(crate::providers::native::pre_dispatch(
                            "Lark image type is not supported",
                        ));
                    }
                    continue;
                }
                crate::providers::native::Step::File(file)
                | crate::providers::native::Step::Voice(file) => file,
            };
            if file
                .caption
                .as_deref()
                .is_some_and(|caption| !caption.trim().is_empty())
            {
                return Err(crate::providers::native::unsupported(
                    "Lark file/audio captions",
                ));
            }
            if file.bytes.len() as u64 > super::FILE_LIMIT {
                return Err(crate::providers::native::pre_dispatch(
                    "Lark file exceeds the 30 MiB limit",
                ));
            }
            if file.kind == tinychannels_bus::MediaKind::Video && file.mime != "video/mp4" {
                return Err(crate::providers::native::unsupported("Lark video type"));
            }
            if is_voice || file.kind == tinychannels_bus::MediaKind::Audio {
                let (bytes, name) = self.opus_audio(file).await?;
                file.bytes = bytes;
                file.name = name;
                file.mime = "audio/ogg".into();
            }
        }
        Ok(())
    }
}
