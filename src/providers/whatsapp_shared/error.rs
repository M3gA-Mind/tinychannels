//! The outcome classes a WhatsApp operation can fail with.

use crate::channel::{ChannelSendError, SendErrorKind};

/// Why one WhatsApp operation did not complete.
///
/// The class is the retry contract: rejected, malformed and unsupported
/// operations are known to have left the account untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaFailure {
    /// Refused before any provider call: bad scope, missing port, invalid input.
    Rejected(String),
    /// The input cannot be represented by this provider's address format.
    BadFormat(String),
    /// The adapter or protocol cannot carry this content or operation.
    Unsupported(String),
    /// A provider call was attempted and its outcome is not known.
    Uncertain(String),
}

impl std::fmt::Display for WaFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (Self::Rejected(message)
        | Self::BadFormat(message)
        | Self::Unsupported(message)
        | Self::Uncertain(message)) = self;
        formatter.write_str(message)
    }
}

impl std::error::Error for WaFailure {}

impl WaFailure {
    pub(crate) fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected(message.into())
    }

    pub(crate) fn bad_format(message: impl Into<String>) -> Self {
        Self::BadFormat(message.into())
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }

    pub(crate) fn uncertain(message: impl Into<String>) -> Self {
        Self::Uncertain(message.into())
    }

    /// The structured send error. Unknown outcomes are never marked retryable:
    /// a second attempt could deliver the message twice.
    pub(crate) fn into_send_error(self) -> ChannelSendError {
        match self {
            Self::Rejected(message) => ChannelSendError {
                kind: SendErrorKind::Forbidden,
                message,
                retryable: false,
                sent_before_error: false,
                ..Default::default()
            },
            Self::BadFormat(message) => ChannelSendError {
                kind: SendErrorKind::BadFormat,
                message,
                retryable: false,
                sent_before_error: false,
                ..Default::default()
            },
            Self::Unsupported(message) => ChannelSendError {
                kind: SendErrorKind::BadFormat,
                message,
                unsupported: true,
                retryable: false,
                sent_before_error: false,
                ..Default::default()
            },
            Self::Uncertain(message) => ChannelSendError {
                kind: SendErrorKind::Unknown,
                message,
                retryable: false,
                sent_before_error: true,
                ..Default::default()
            },
        }
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
