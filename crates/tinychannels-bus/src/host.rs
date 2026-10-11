//! Portable payloads for the host callback object.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Request speech recognition through the host's configured voice provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct HostTranscriptionRequest {
    /// Base64-encoded audio bytes.
    pub audio_base64: String,
    /// MIME hint such as `audio/ogg` or `audio/mp4`.
    pub mime_type: Option<String>,
    /// Original filename hint.
    pub file_name: Option<String>,
    /// BCP-47 language hint; `None` requests automatic detection.
    pub language: Option<String>,
}

impl Default for HostTranscriptionRequest {
    fn default() -> Self {
        Self {
            audio_base64: String::new(),
            mime_type: None,
            file_name: None,
            language: None,
        }
    }
}

/// Speech recognition result returned by the host.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct HostTranscriptionResult {
    /// Recognized text.
    pub text: String,
    /// Detected or requested BCP-47 language.
    pub language: Option<String>,
    /// Audio duration, when known.
    pub duration_secs: Option<f64>,
}

/// An event published by a provider through the host's event sink.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HostEventPublication {
    /// Event domain, for example `channel` or `web`.
    pub domain: String,
    /// Event kind within the domain.
    pub kind: String,
    /// Domain-specific JSON payload.
    pub payload: serde_json::Value,
}
