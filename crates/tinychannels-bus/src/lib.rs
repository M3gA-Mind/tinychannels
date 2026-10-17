//! Pure serialized vocabulary, schema declarations and errors for TinyChannels.
//! Provider seams and algorithms live in `tinychannels-runtime`; the root
//! implementation crate preserves legacy module paths through re-exports.
pub mod capabilities;
#[allow(
    deprecated,
    reason = "v21 conversion implementations remain for one release"
)]
pub mod channel;
pub mod config;
pub mod context;
pub mod controllers;
pub mod error;
pub mod health;
pub mod host;
pub mod http;
#[allow(deprecated, reason = "v21 module DTOs remain for one release")]
pub mod module_control;
pub mod module_controller;
pub mod module_delivery;
pub mod module_email;
#[allow(deprecated, reason = "v21 module DTOs remain for one release")]
pub mod module_inbound;
#[allow(deprecated, reason = "v21 module DTOs remain for one release")]
pub mod module_interaction;
pub mod module_progressive;
pub mod module_relay;
#[allow(deprecated, reason = "v21 module DTOs remain for one release")]
pub mod module_remote;
pub mod module_routes;
#[allow(deprecated, reason = "v21 module DTOs remain for one release")]
pub mod module_runtime;
pub mod module_supervision;
pub mod names;
pub mod relay;
#[allow(
    deprecated,
    reason = "v21 DTO definitions and adapters remain for one release"
)]
pub mod traits;
pub mod version;
pub mod web;
pub use capabilities::{
    AccountCapabilities, ActionCapabilities, AddressingCapabilities,
    ButtonPresentationCapabilities, ChannelCapabilities, ContentCapabilities, ContentCapability,
    DeliveryCapabilities, EventCapabilities, LegacyChannelCapabilities, LengthUnit,
    MarkdownDialect, TextCapabilities, VoiceCapabilities, provider_id,
};
pub use channel::{
    ButtonAction, ButtonStyle, ChannelEvent, ChannelEventKind, ChannelInboundEnvelope,
    ChannelOutboundIntent, ChannelOutboundMessage, ChannelReaction, ChannelRef, ContactMethod,
    ContentPart, ConversationKind, ConversationRef, DeliveryDurability, MediaKind, MediaReference,
    OutboundContentPart, OutboundPayload, OutboundVoice, SenderRef, VoiceNote, VoiceTranscript,
    VoiceTranscriptSource, VoiceTranscriptState,
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
#[allow(
    deprecated,
    reason = "re-export the one-release v21 compatibility DTOs"
)]
pub use traits::{Channel, ChannelIngress, ChannelMessage, ChannelSendExt, SendMessage};
pub use version::{CONTRACT_VERSION, is_compatible};
pub use web::{
    ChatRequestMetadata, QueueMode, WebChannelEvent, WebEventContext, WebEventPayload,
    WebGuardrailPayload, WebGuardrailReason, WebQueueItemPayload, WebSubagentProgressDetail,
    WebSubagentUsagePayload, WebTurnTimingPayload, WebTurnUsagePayload, legacy_event_alias,
    web_event_schema_json,
};

#[cfg(test)]
#[path = "wire_tests.rs"]
#[allow(
    deprecated,
    reason = "wire tests pin the v21 conversion shape during migration"
)]
mod tests;

pub use module_interaction::{
    ChannelInteraction, ChannelInteractionInfo, ChannelInteractionInfoV2,
    ChannelInteractionRequest, ChannelInteractionResult,
};

pub use module_supervision::ChannelSupervisionPolicy;

pub use module_runtime::{ChannelRuntimeDispatchPolicy, ChannelRuntimeReservation};
