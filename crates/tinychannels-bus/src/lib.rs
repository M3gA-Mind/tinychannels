//! Pure serialized vocabulary, schema declarations and errors for TinyChannels.
//! Provider seams and algorithms live in `tinychannels-runtime`; the root
//! implementation crate preserves legacy module paths through re-exports.
pub mod capabilities;
pub mod channel;
pub mod config;
pub mod context;
pub mod controllers;
pub mod error;
pub mod health;
pub mod host;
pub mod http;
pub mod module_controller;
pub mod module_delivery;
pub mod module_email;
pub mod module_inbound;
pub mod module_interaction;
pub mod module_progressive;
pub mod module_relay;
pub mod module_runtime;
pub mod module_supervision;
pub mod names;
pub mod relay;
pub mod traits;
pub mod version;
pub use capabilities::{ChannelCapabilities, capabilities_for, provider_id};
pub use channel::{
    ChannelInboundEnvelope, ChannelOutboundIntent, DeliveryDurability, OutboundPayload,
};
pub use config::ChannelsConfig;
pub use controllers::{ChannelAuthMode, ChannelDefinition};
pub use error::{Result, TinyChannelsError};
pub use health::{ChannelHealthReport, ChannelHealthState};
pub use host::{HostEventPublication, HostTranscriptionRequest, HostTranscriptionResult};
pub use http::HttpProxyPolicy;
pub use module_controller::{
    ChannelBackendRequest, ChannelBackendResult, ChannelControllerRequest, ChannelControllerResult,
};
pub use module_delivery::{DeliveryQueueEntry, DeliveryRecoveryState, DeliveryTerminalResult};
pub use module_email::{
    EmailAttachment, EmailAttachmentUpload, EmailSendRequest, EmailUploadBegin, EmailUploadChunk,
    EmailUploadHandle,
};
pub use names::{BUS_NAME, HOST_BUS_NAME, HOST_OBJECT_PATH, METHODS, OBJECT_PATH, methods};
pub use traits::{ChannelMessage, SendMessage};
pub use version::{CONTRACT_VERSION, is_compatible};

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;

pub use module_interaction::{
    ChannelInteraction, ChannelInteractionInfo, ChannelInteractionRequest, ChannelInteractionResult,
};

pub use module_supervision::ChannelSupervisionPolicy;

pub use module_runtime::ChannelRuntimeReservation;
