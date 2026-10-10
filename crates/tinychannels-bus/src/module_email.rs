//! Pure send-only email vocabulary for the loadable channel module.
use crate::config::EmailConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maximum aggregate attachment bytes accepted by one send request.
pub const EMAIL_ATTACHMENT_BYTES: usize = 25 * 1024 * 1024;
/// Maximum number of attachments accepted by one send request.
pub const EMAIL_ATTACHMENT_COUNT: usize = 32;
/// Maximum confidential attachment upload chunk accepted by the module.
pub const EMAIL_UPLOAD_CHUNK_BYTES: usize = 512 * 1024;
/// Maximum number of chunks accepted for one attachment.
pub const EMAIL_UPLOAD_MAX_CHUNKS: usize = 128;

/// One MIME attachment supplied by the host.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EmailAttachment {
    /// Filename presented in the MIME part.
    pub filename: String,
    /// MIME content type, such as `application/pdf`.
    pub content_type: String,
    /// Raw attachment bytes. TinyBus confidential delivery protects the payload.
    pub bytes: Vec<u8>,
}

/// Host-hydrated SMTP send request. Credentials are confidential bus data.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EmailSendRequest {
    /// Account configuration prepared by the host's credential resolver.
    pub config: EmailConfig,
    /// Recipient mailbox address.
    pub recipient: String,
    /// Message subject.
    pub subject: String,
    /// Plain-text message body.
    pub body: String,
    /// MIME attachments, in presentation order.
    #[serde(default)]
    pub attachments: Vec<EmailAttachment>,
}

/// Metadata for one attachment uploaded in bounded chunks.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EmailAttachmentUpload {
    /// Filename presented in the MIME part.
    pub filename: String,
    /// MIME content type, such as `application/pdf`.
    pub content_type: String,
    /// Exact raw byte count that must be uploaded.
    pub byte_count: usize,
}

/// Begin a confidential, bounded upload for one SMTP message.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EmailUploadBegin {
    /// Account and message metadata; credentials travel confidentially.
    pub config: EmailConfig,
    /// Recipient mailbox address.
    pub recipient: String,
    /// Message subject.
    pub subject: String,
    /// Plain-text message body.
    pub body: String,
    /// Attachments in MIME presentation order.
    pub attachments: Vec<EmailAttachmentUpload>,
}

/// Opaque bounded handle for an in-progress or completed email upload.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct EmailUploadHandle(pub String);

/// One idempotent chunk write to an email attachment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EmailUploadChunk {
    /// Upload handle returned by `BeginEmailUpload`.
    pub handle: EmailUploadHandle,
    /// Zero-based attachment index from the begin request.
    pub attachment_index: u32,
    /// Zero-based chunk sequence within that attachment.
    pub sequence: u32,
    /// Attachment bytes; bounded by [`EMAIL_UPLOAD_CHUNK_BYTES`].
    pub bytes: Vec<u8>,
}

#[cfg(test)]
#[path = "module_email_tests.rs"]
mod tests;
