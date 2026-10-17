//! Reusable behavioral checks for configured TinyChannels adapters.
//!
//! Provider tests implement [`AdapterFixture`] by invoking the concrete
//! adapter operation and reading the request and response from that
//! provider's mocked platform. The kit compares those observations with the
//! adapter's declared capabilities and configured scope. It never substitutes
//! a synthetic transport for the provider fixture.

use async_trait::async_trait;
use tinychannels_bus::{
    ChannelCapabilities, ChannelOutboundIntent, ChannelRef, ChannelSendError, SendErrorKind,
    channel::{ChannelSendOutcome, ConversationRef},
};
use tinychannels_runtime::channel::ChannelAdapter;

/// Behavioral cases shared by provider conformance suites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConformanceFeature {
    Text,
    Image,
    Video,
    Audio,
    File,
    VoiceIn,
    VoiceOut,
    ReplyAddressing,
    ThreadAddressing,
    Typing,
    Reactions,
    Edit,
    Delete,
    ReadReceipts,
    StreamingDraft,
}

impl ConformanceFeature {
    pub const ALL: [Self; 15] = [
        Self::Text,
        Self::Image,
        Self::Video,
        Self::Audio,
        Self::File,
        Self::VoiceIn,
        Self::VoiceOut,
        Self::ReplyAddressing,
        Self::ThreadAddressing,
        Self::Typing,
        Self::Reactions,
        Self::Edit,
        Self::Delete,
        Self::ReadReceipts,
        Self::StreamingDraft,
    ];

    fn declared(self, capabilities: &ChannelCapabilities) -> bool {
        let action = |name: &str| {
            capabilities
                .actions
                .supported
                .iter()
                .any(|value| value == name)
        };
        match self {
            Self::Text => capabilities.content.text.outbound,
            Self::Image => capabilities.content.image.outbound,
            Self::Video => capabilities.content.video.outbound,
            Self::Audio => capabilities.content.audio.outbound,
            Self::File => capabilities.content.file.outbound,
            Self::VoiceIn => capabilities.voice_in.supported,
            Self::VoiceOut => capabilities.voice_out.supported,
            Self::ReplyAddressing => capabilities.addressing.reply_to_id,
            Self::ThreadAddressing => capabilities.addressing.thread_id,
            Self::Typing => action("typing"),
            Self::Reactions => capabilities.reactions,
            Self::Edit => action("edit"),
            Self::Delete => action("delete") || action("unsend"),
            Self::ReadReceipts => capabilities
                .events
                .supported
                .iter()
                .any(|event| event == "read_receipt"),
            Self::StreamingDraft => capabilities.progressive_edits && action("draft-update"),
        }
    }
}

/// Address and operation observed from the mock platform's actual wire
/// request and response.
#[derive(Debug, Clone, PartialEq)]
pub struct WireObservation {
    pub feature: ConformanceFeature,
    pub channel: ChannelRef,
    pub conversation: ConversationRef,
    pub payload_kinds: Vec<String>,
    pub platform_message_ids: Vec<String>,
    pub request: serde_json::Value,
}

/// Scope expected by one fixture operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedScope {
    pub channel: ChannelRef,
    pub conversation: ConversationRef,
}

/// Provider-specific bridge to a real configured adapter and its mock server.
///
/// `exercise` must invoke the adapter or one of its optional extension traits.
/// `observations` must be parsed from mock-server traffic, not synthesized from
/// the invocation arguments.
#[async_trait]
pub trait AdapterFixture: Send + Sync {
    fn adapter(&self) -> &dyn ChannelAdapter;
    fn expected_scope(&self, feature: ConformanceFeature) -> ExpectedScope;
    fn clear_observations(&self);
    fn observations(&self) -> Vec<WireObservation>;
    async fn exercise(
        &self,
        feature: ConformanceFeature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError>;
}

/// Run one honesty and wire-behavior check.
pub async fn check_feature(
    fixture: &dyn AdapterFixture,
    feature: ConformanceFeature,
) -> Result<(), String> {
    fixture.clear_observations();
    let declared = feature.declared(&fixture.adapter().declared_capabilities());
    let result = fixture.exercise(feature).await;
    let observations = fixture.observations();

    if !declared {
        let explicitly_unsupported = match &result {
            Err(error) => error.kind == SendErrorKind::BadFormat && !error.retryable,
            Ok(Some(outcome)) => outcome
                .error
                .as_ref()
                .is_some_and(|error| error.kind == SendErrorKind::BadFormat && !error.retryable),
            Ok(None) => false,
        };
        if !explicitly_unsupported {
            return Err(format!(
                "undeclared {feature:?} did not return an explicit unsupported result"
            ));
        }
        if !observations.is_empty() {
            return Err(format!("undeclared {feature:?} reached the mock platform"));
        }
        return Ok(());
    }

    let outcome =
        result.map_err(|error| format!("declared {feature:?} failed: {}", error.message))?;
    if outcome
        .as_ref()
        .is_some_and(|result| result.error.is_some())
    {
        return Err(format!("declared {feature:?} returned a provider error"));
    }
    let expected = fixture.expected_scope(feature);
    let matching = observations.iter().find(|observation| {
        observation.feature == feature
            && observation.channel == expected.channel
            && observation.conversation == expected.conversation
    });
    let Some(observation) = matching else {
        return Err(format!(
            "declared {feature:?} produced no matching wire observation"
        ));
    };
    let Some(outcome) = outcome else {
        return Err(format!("declared {feature:?} returned no receipt"));
    };
    let receipt = &outcome.receipt;
    if receipt.channel.as_ref() != Some(&expected.channel)
        || receipt.conversation.as_ref() != Some(&expected.conversation)
        || receipt.platform_message_ids.is_empty()
        || receipt.parts.is_empty()
    {
        return Err(format!(
            "declared {feature:?} returned an incomplete or mis-scoped receipt"
        ));
    }
    let expected_payload = feature.payload_kind();
    if let Some(expected_payload) = expected_payload
        && !observation
            .payload_kinds
            .iter()
            .any(|kind| kind == expected_payload)
    {
        return Err(format!(
            "{feature:?} wire payload omitted {expected_payload}"
        ));
    }
    if observation.platform_message_ids != receipt.platform_message_ids {
        return Err(format!(
            "{feature:?} receipt IDs differ from the mock response"
        ));
    }
    Ok(())
}

impl ConformanceFeature {
    fn payload_kind(self) -> Option<&'static str> {
        match self {
            Self::Text => Some("text"),
            Self::Image => Some("image"),
            Self::Video => Some("video"),
            Self::Audio => Some("audio"),
            Self::File => Some("file"),
            Self::VoiceOut => Some("voice"),
            _ => None,
        }
    }
}

/// Run the entire shared feature matrix against a provider fixture.
pub async fn check_all_features(
    fixture: &dyn AdapterFixture,
) -> Vec<(ConformanceFeature, Result<(), String>)> {
    let mut results = Vec::with_capacity(ConformanceFeature::ALL.len());
    for feature in ConformanceFeature::ALL {
        results.push((feature, check_feature(fixture, feature).await));
    }
    results
}

/// Send payload cases may share one full addressing fixture.
pub fn intent_scope(intent: &ChannelOutboundIntent) -> Option<ExpectedScope> {
    Some(ExpectedScope {
        channel: intent.channel_ref.clone()?,
        conversation: intent.conversation_ref.clone()?,
    })
}
