//! Media message construction for the WhatsApp Web adapter.
//!
//! The SDK's own builders take an upload result it does not let callers
//! construct, so the adapter copies the upload's CDN fields into
//! [`UploadedMedia`] and builds the protocol message here, field for field as
//! the SDK's `media::*_message` builders do.

use whatsapp_rust::buffa::MessageField;
use whatsapp_rust::waproto::whatsapp as wa;

/// What to build around an uploaded attachment.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum MediaBuild {
    Image {
        caption: Option<String>,
        mimetype: String,
    },
    Video {
        caption: Option<String>,
        duration_seconds: Option<u32>,
    },
    Audio {
        mimetype: String,
        duration_seconds: Option<u32>,
    },
    /// A push-to-talk voice note.
    Voice {
        duration_seconds: Option<u32>,
        waveform: Option<Vec<u8>>,
    },
    Document {
        mimetype: String,
        file_name: Option<String>,
        caption: Option<String>,
    },
    Sticker,
}

/// The CDN fields of one finished upload.
#[derive(Debug, Clone)]
pub(super) struct UploadedMedia {
    pub url: String,
    pub direct_path: String,
    pub media_key: Vec<u8>,
    pub file_sha256: Vec<u8>,
    pub file_enc_sha256: Vec<u8>,
    pub file_length: u64,
    pub media_key_timestamp: i64,
    pub streaming_sidecar: Option<Vec<u8>>,
}

fn context(context: Option<wa::ContextInfo>) -> MessageField<wa::ContextInfo> {
    context.map(MessageField::some).unwrap_or_default()
}

/// Build the protocol message that sends `upload` as described by `build`.
pub(super) fn build(
    upload: UploadedMedia,
    build: MediaBuild,
    context_info: Option<wa::ContextInfo>,
) -> wa::Message {
    let UploadedMedia {
        url,
        direct_path,
        media_key,
        file_sha256,
        file_enc_sha256,
        file_length,
        media_key_timestamp,
        streaming_sidecar,
    } = upload;
    let context_info = context(context_info);
    match build {
        MediaBuild::Image { caption, mimetype } => wa::Message {
            image_message: MessageField::some(wa::message::ImageMessage {
                url: Some(url),
                direct_path: Some(direct_path),
                media_key: Some(media_key),
                file_sha256: Some(file_sha256),
                file_enc_sha256: Some(file_enc_sha256),
                file_length: Some(file_length),
                media_key_timestamp: Some(media_key_timestamp),
                mimetype: Some(mimetype),
                caption,
                context_info,
                ..Default::default()
            }),
            ..Default::default()
        },
        MediaBuild::Video {
            caption,
            duration_seconds,
        } => wa::Message {
            video_message: MessageField::some(wa::message::VideoMessage {
                url: Some(url),
                direct_path: Some(direct_path),
                media_key: Some(media_key),
                file_sha256: Some(file_sha256),
                file_enc_sha256: Some(file_enc_sha256),
                file_length: Some(file_length),
                media_key_timestamp: Some(media_key_timestamp),
                streaming_sidecar,
                mimetype: Some("video/mp4".into()),
                caption,
                seconds: duration_seconds,
                context_info,
                ..Default::default()
            }),
            ..Default::default()
        },
        MediaBuild::Audio {
            mimetype,
            duration_seconds,
        } => audio(
            url,
            direct_path,
            media_key,
            file_sha256,
            file_enc_sha256,
            file_length,
            media_key_timestamp,
            streaming_sidecar,
            mimetype,
            duration_seconds,
            None,
            None,
            context_info,
        ),
        MediaBuild::Voice {
            duration_seconds,
            waveform,
        } => audio(
            url,
            direct_path,
            media_key,
            file_sha256,
            file_enc_sha256,
            file_length,
            media_key_timestamp,
            streaming_sidecar,
            "audio/ogg; codecs=opus".into(),
            duration_seconds,
            Some(true),
            waveform,
            context_info,
        ),
        MediaBuild::Document {
            mimetype,
            file_name,
            caption,
        } => wa::Message {
            document_message: MessageField::some(wa::message::DocumentMessage {
                url: Some(url),
                direct_path: Some(direct_path),
                media_key: Some(media_key),
                file_sha256: Some(file_sha256),
                file_enc_sha256: Some(file_enc_sha256),
                file_length: Some(file_length),
                media_key_timestamp: Some(media_key_timestamp),
                mimetype: Some(mimetype),
                file_name,
                caption,
                context_info,
                ..Default::default()
            }),
            ..Default::default()
        },
        MediaBuild::Sticker => wa::Message {
            sticker_message: MessageField::some(wa::message::StickerMessage {
                url: Some(url),
                direct_path: Some(direct_path),
                media_key: Some(media_key),
                file_sha256: Some(file_sha256),
                file_enc_sha256: Some(file_enc_sha256),
                file_length: Some(file_length),
                media_key_timestamp: Some(media_key_timestamp),
                mimetype: Some("image/webp".into()),
                context_info,
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the audio message mirrors the protocol message's flat field list"
)]
fn audio(
    url: String,
    direct_path: String,
    media_key: Vec<u8>,
    file_sha256: Vec<u8>,
    file_enc_sha256: Vec<u8>,
    file_length: u64,
    media_key_timestamp: i64,
    streaming_sidecar: Option<Vec<u8>>,
    mimetype: String,
    seconds: Option<u32>,
    ptt: Option<bool>,
    waveform: Option<Vec<u8>>,
    context_info: MessageField<wa::ContextInfo>,
) -> wa::Message {
    wa::Message {
        audio_message: MessageField::some(wa::message::AudioMessage {
            url: Some(url),
            direct_path: Some(direct_path),
            media_key: Some(media_key),
            file_sha256: Some(file_sha256),
            file_enc_sha256: Some(file_enc_sha256),
            file_length: Some(file_length),
            media_key_timestamp: Some(media_key_timestamp),
            streaming_sidecar,
            mimetype: Some(mimetype),
            seconds,
            ptt,
            waveform,
            context_info,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "media_build_tests.rs"]
mod tests;
