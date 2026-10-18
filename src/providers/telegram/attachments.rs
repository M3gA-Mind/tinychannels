//! MIME inference for Telegram's downloaded media files.

use std::path::Path;

pub(crate) fn infer_attachment_mime(target: &str) -> Option<&'static str> {
    let normalized = target
        .split('?')
        .next()
        .unwrap_or(target)
        .split('#')
        .next()
        .unwrap_or(target);
    match Path::new(normalized)
        .extension()
        .and_then(|ext| ext.to_str())?
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "mp4" => Some("video/mp4"),
        "mov" => Some("video/quicktime"),
        "webm" => Some("video/webm"),
        "mp3" => Some("audio/mpeg"),
        "m4a" => Some("audio/mp4"),
        "wav" => Some("audio/wav"),
        "ogg" | "oga" | "opus" => Some("audio/ogg"),
        "pdf" => Some("application/pdf"),
        "txt" | "md" => Some("text/plain"),
        "csv" => Some("text/csv"),
        "json" => Some("application/json"),
        "zip" => Some("application/zip"),
        "docx" => Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "xlsx" => Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        _ => None,
    }
}
