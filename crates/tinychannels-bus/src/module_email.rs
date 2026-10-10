//! Pure send-only email vocabulary for the loadable channel module.
use crate::config::EmailConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maximum aggregate attachment bytes accepted by one send request.
pub const EMAIL_ATTACHMENT_BYTES: usize = 25 * 1024 * 1024;
/// Maximum number of attachments accepted by one send request.
pub const EMAIL_ATTACHMENT_COUNT: usize = 32;

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

#[cfg(test)]
#[path = "module_email_tests.rs"]
mod tests;
