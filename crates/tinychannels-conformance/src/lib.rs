//! Reusable behavioral checks for configured TinyChannels adapters.
//!
//! Provider tests implement [`AdapterFixture`] by invoking the concrete
//! adapter operation and reading the request and response from that
//! provider's mocked platform. The kit compares those observations with the
//! adapter's declared capabilities and configured scope. It never substitutes
//! a synthetic transport for the provider fixture.

use async_trait::async_trait;
use tinychannels_bus::{
    ChannelCapabilities, ChannelOutboundIntent, ChannelRef,
    channel::{ChannelSendError, ChannelSendOutcome, ConversationRef},
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
    Sticker,
    Location,
    Contact,
    Buttons,
    Poll,
    Presentation,
    NativeData,
    VoiceIn,
    VoiceOut,
    VoiceSynthesis,
    ReplyAddressing,
    ThreadAddressing,
    Typing,
    Reactions,
    InboundReactions,
    Edit,
    Delete,
    ReadReceipts,
    StreamingDraft,
    /// Managed-host reaction controller operation.
    ManagedReaction,
    /// Managed-host thread creation operation.
    ManagedThreadCreate,
    /// Managed-host thread update operation.
    ManagedThreadUpdate,
    /// Managed-host thread listing operation.
    ManagedThreadList,
}

/// Operations routed through the module's native provider controller path.
/// These are separate from [`ConformanceFeature::ManagedReaction`] and the
/// managed thread features: bot-token controller calls must reach a provider
/// API and must never be counted as host-managed callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NativeControllerFeature {
    Reaction,
    ThreadCreate,
    ThreadUpdate,
    ThreadList,
}

impl NativeControllerFeature {
    /// Whether the configured provider declares the operation used by the
    /// production native controller route.
    pub fn declared(self, capabilities: &ChannelCapabilities) -> bool {
        let action = |name: &str| {
            capabilities
                .actions
                .supported
                .iter()
                .any(|value| value == name)
        };
        match self {
            Self::Reaction => capabilities.reactions,
            Self::ThreadCreate => action("thread-create"),
            Self::ThreadUpdate => action("thread-update"),
            Self::ThreadList => action("thread-list"),
        }
    }
}

/// One controller operation observed from the configured provider's mock
/// transport and the production controller's returned value.
#[derive(Debug, Clone, PartialEq)]
pub struct NativeControllerObservation {
    pub feature: NativeControllerFeature,
    pub provider: String,
    pub operation: String,
    pub request: serde_json::Value,
    pub response: serde_json::Value,
    /// IDs returned by the actual provider/controller response, not generated
    /// from the request or fixture expectation.
    pub returned_ids: Vec<String>,
}

/// Fixture for native bot-token controller operations. Implementations must
/// invoke the production controller route and derive `capabilities` from the
/// configured provider instance. `observations` must come from real mock
/// transport traffic and its actual response.
#[async_trait]
pub trait NativeControllerFixture: Send + Sync {
    fn capabilities(&self) -> ChannelCapabilities;
    fn provider(&self) -> &str;
    async fn managed_callback_count(&self) -> usize;
    fn clear_observations(&self);
    fn observations(&self) -> Vec<NativeControllerObservation>;
    async fn exercise(&self, feature: NativeControllerFeature)
    -> Result<(), NativeControllerError>;
}

/// Result classification from a native controller fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeControllerError {
    pub unsupported: bool,
    pub message: String,
}

/// Verify one native controller operation against its configured provider
/// capability, actual mock-wire observation, and managed-callback counter.
pub async fn check_native_controller_feature<F: NativeControllerFixture + ?Sized>(
    fixture: &F,
    feature: NativeControllerFeature,
) -> Result<(), String> {
    fixture.clear_observations();
    let initial_callbacks = fixture.managed_callback_count().await;
    let declared = feature.declared(&fixture.capabilities());
    let result = fixture.exercise(feature).await;
    let observations = fixture.observations();
    let callbacks = fixture.managed_callback_count().await;

    if callbacks != initial_callbacks {
        return Err(format!(
            "native {feature:?} invoked the managed host callback"
        ));
    }
    if !declared {
        match result {
            Err(error) if error.unsupported => {}
            Err(error) => {
                return Err(format!(
                    "undeclared {feature:?} failed without an unsupported result: {}",
                    error.message
                ));
            }
            Ok(()) => return Err(format!("undeclared {feature:?} unexpectedly succeeded")),
        }
        if !observations.is_empty() {
            return Err(format!("undeclared {feature:?} reached provider transport"));
        }
        return Ok(());
    }

    result.map_err(|error| format!("declared {feature:?} failed: {}", error.message))?;
    let observation = observations
        .iter()
        .find(|observation| {
            observation.feature == feature && observation.provider == fixture.provider()
        })
        .ok_or_else(|| format!("declared {feature:?} produced no provider wire observation"))?;
    if observation.operation.trim().is_empty()
        || observation.request.is_null()
        || observation.response.is_null()
    {
        return Err(format!(
            "declared {feature:?} omitted its actual request or response"
        ));
    }
    match feature {
        NativeControllerFeature::ThreadCreate | NativeControllerFeature::ThreadList
            if observation.returned_ids.is_empty() =>
        {
            Err(format!(
                "declared {feature:?} returned no provider identifiers"
            ))
        }
        _ => Ok(()),
    }
}

impl ConformanceFeature {
    pub const ALL: [Self; 24] = [
        Self::Text,
        Self::Image,
        Self::Video,
        Self::Audio,
        Self::File,
        Self::Sticker,
        Self::Location,
        Self::Contact,
        Self::Buttons,
        Self::Poll,
        Self::Presentation,
        Self::NativeData,
        Self::VoiceIn,
        Self::VoiceOut,
        Self::VoiceSynthesis,
        Self::ReplyAddressing,
        Self::ThreadAddressing,
        Self::Typing,
        Self::Reactions,
        Self::InboundReactions,
        Self::Edit,
        Self::Delete,
        Self::ReadReceipts,
        Self::StreamingDraft,
    ];

    /// Managed controller operations supported by the host callback contract.
    pub const MANAGED: [Self; 5] = [
        Self::ManagedReaction,
        Self::ManagedThreadCreate,
        Self::ManagedThreadUpdate,
        Self::ManagedThreadList,
        Self::Edit,
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
            Self::Sticker => capabilities.content.sticker.outbound,
            Self::Location => capabilities.content.location.outbound,
            Self::Contact => capabilities.content.contact.outbound,
            Self::Buttons => capabilities.content.buttons.outbound,
            Self::Poll => capabilities.content.poll.outbound,
            Self::Presentation => capabilities.content.presentation.outbound,
            Self::NativeData => capabilities.content.native.outbound,
            Self::VoiceIn => capabilities.voice_in.supported,
            Self::VoiceOut => capabilities.voice_out.supported,
            Self::VoiceSynthesis => capabilities.voice_out.synthesis,
            Self::ReplyAddressing => capabilities.addressing.reply_to_id,
            Self::ThreadAddressing => capabilities.addressing.thread_id,
            Self::Typing => action("typing"),
            Self::Reactions => capabilities.reactions,
            Self::InboundReactions => capabilities
                .events
                .supported
                .iter()
                .any(|event| matches!(event.as_str(), "reaction_added" | "reaction_removed")),
            Self::Edit => action("edit"),
            Self::Delete => action("delete") || action("unsend"),
            Self::ReadReceipts => capabilities
                .events
                .supported
                .iter()
                .any(|event| event == "read_receipt"),
            Self::StreamingDraft => capabilities.progressive_edits && action("draft-update"),
            Self::ManagedReaction => capabilities.reactions,
            Self::ManagedThreadCreate => action("thread-create"),
            Self::ManagedThreadUpdate => action("thread-update"),
            Self::ManagedThreadList => action("thread-list"),
        }
    }
}

/// Address and operation observed from the mock platform's actual wire
/// request and response.
#[derive(Debug, Clone, PartialEq)]
pub struct WireObservation {
    pub feature: ConformanceFeature,
    pub operation: String,
    pub channel: ChannelRef,
    pub conversation: ConversationRef,
    pub reply_to_id: Option<String>,
    pub thread_id: Option<String>,
    pub payload_kinds: Vec<String>,
    pub platform_message_ids: Vec<String>,
    pub request: serde_json::Value,
}

/// Scope expected by one fixture operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedScope {
    pub channel: ChannelRef,
    pub conversation: ConversationRef,
    pub reply_to_id: Option<String>,
    pub thread_id: Option<String>,
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
    /// Whether the platform returns a message identifier for this operation.
    /// A receiptless platform must still provide a wire observation proving
    /// the operation reached the mock platform.
    fn receipt_required(&self, _feature: ConformanceFeature) -> bool {
        true
    }
    fn clear_observations(&self);
    fn observations(&self) -> Vec<WireObservation>;
    async fn exercise(
        &self,
        feature: ConformanceFeature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError>;
}

/// Common conformance surface for provider adapters and managed host callbacks.
///
/// Adapter fixtures should implement [`AdapterFixture`] so capabilities are
/// always read from the concrete configured adapter. A managed fixture uses
/// this trait directly because managed controller capabilities are supplied by
/// the host contract rather than a provider adapter.
#[async_trait]
pub trait ConformanceFixture: Send + Sync {
    fn capabilities(&self) -> ChannelCapabilities;
    /// Managed controller features can only be exercised by a fixture wired
    /// to the production host callback; provider adapter fixtures must leave
    /// this false.
    fn uses_managed_host_callbacks(&self) -> bool {
        false
    }
    fn expected_scope(&self, feature: ConformanceFeature) -> ExpectedScope;
    fn receipt_required(&self, _feature: ConformanceFeature) -> bool {
        true
    }
    fn clear_observations(&self);
    fn observations(&self) -> Vec<WireObservation>;
    async fn exercise(
        &self,
        feature: ConformanceFeature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError>;
}

#[async_trait]
impl<T: AdapterFixture + ?Sized> ConformanceFixture for T {
    fn capabilities(&self) -> ChannelCapabilities {
        self.adapter().declared_capabilities()
    }

    fn expected_scope(&self, feature: ConformanceFeature) -> ExpectedScope {
        AdapterFixture::expected_scope(self, feature)
    }

    fn receipt_required(&self, feature: ConformanceFeature) -> bool {
        AdapterFixture::receipt_required(self, feature)
    }

    fn clear_observations(&self) {
        AdapterFixture::clear_observations(self)
    }

    fn observations(&self) -> Vec<WireObservation> {
        AdapterFixture::observations(self)
    }

    async fn exercise(
        &self,
        feature: ConformanceFeature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        AdapterFixture::exercise(self, feature).await
    }
}

/// Run one honesty and wire-behavior check.
pub async fn check_feature<F: ConformanceFixture + ?Sized>(
    fixture: &F,
    feature: ConformanceFeature,
) -> Result<(), String> {
    if matches!(
        feature,
        ConformanceFeature::ManagedReaction
            | ConformanceFeature::ManagedThreadCreate
            | ConformanceFeature::ManagedThreadUpdate
            | ConformanceFeature::ManagedThreadList
    ) && !fixture.uses_managed_host_callbacks()
    {
        return Err(format!(
            "{feature:?} requires a managed host callback fixture"
        ));
    }
    fixture.clear_observations();
    let declared = feature.declared(&fixture.capabilities());
    let result = fixture.exercise(feature).await;
    let observations = fixture.observations();

    if !declared {
        let explicitly_unsupported = match &result {
            Err(error) => {
                error.unsupported
                    && !error.retryable
                    && !error.sent_before_error
                    && error.continuation_message_ids.is_empty()
                    && error.partial_overflow.is_none()
            }
            Ok(Some(outcome)) => outcome.error.as_ref().is_some_and(|error| {
                error.unsupported
                    && !error.retryable
                    && !error.sent_before_error
                    && error.continuation_message_ids.is_empty()
                    && error.partial_overflow.is_none()
                    && outcome.receipt.platform_message_ids.is_empty()
                    && outcome.receipt.primary_platform_message_id.is_none()
                    && outcome.receipt.parts.is_empty()
            }),
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
    let requires_receipt = matches!(
        feature,
        ConformanceFeature::Text
            | ConformanceFeature::Image
            | ConformanceFeature::Video
            | ConformanceFeature::Audio
            | ConformanceFeature::File
            | ConformanceFeature::Sticker
            | ConformanceFeature::Location
            | ConformanceFeature::Contact
            | ConformanceFeature::Buttons
            | ConformanceFeature::Poll
            | ConformanceFeature::Presentation
            | ConformanceFeature::NativeData
            | ConformanceFeature::VoiceOut
            | ConformanceFeature::VoiceSynthesis
            | ConformanceFeature::ReplyAddressing
            | ConformanceFeature::ThreadAddressing
            | ConformanceFeature::Edit
            | ConformanceFeature::StreamingDraft
    );
    let receipt = outcome.as_ref().map(|outcome| &outcome.receipt);
    let managed_thread_operation = matches!(
        feature,
        ConformanceFeature::ManagedThreadCreate
            | ConformanceFeature::ManagedThreadUpdate
            | ConformanceFeature::ManagedThreadList
    );
    if managed_thread_operation && observation.platform_message_ids.is_empty() {
        return Err(format!(
            "declared {feature:?} returned no thread identifiers"
        ));
    }
    if requires_receipt && fixture.receipt_required(feature) {
        let Some(receipt) = receipt else {
            return Err(format!("declared {feature:?} returned no receipt"));
        };
        if receipt.channel.as_ref() != Some(&expected.channel)
            || receipt.conversation.as_ref() != Some(&expected.conversation)
            || receipt.platform_message_ids.is_empty()
            || receipt.parts.is_empty()
        {
            return Err(format!(
                "declared {feature:?} returned an incomplete or mis-scoped receipt: channel {:?} (expected {:?}), conversation {:?} (expected {:?}), {} IDs, {} parts",
                receipt.channel,
                expected.channel,
                receipt.conversation,
                expected.conversation,
                receipt.platform_message_ids.len(),
                receipt.parts.len(),
            ));
        }
    }
    if requires_receipt
        && !fixture.receipt_required(feature)
        && receipt.is_some_and(|receipt| {
            let has_receipt_data = receipt.channel.is_some()
                || receipt.conversation.is_some()
                || receipt.primary_platform_message_id.is_some()
                || !receipt.platform_message_ids.is_empty()
                || !receipt.parts.is_empty();
            has_receipt_data
                && (receipt.channel.as_ref() != Some(&expected.channel)
                    || receipt.conversation.as_ref() != Some(&expected.conversation)
                    || receipt.platform_message_ids.is_empty()
                    || receipt.parts.is_empty())
        })
    {
        return Err(format!(
            "declared {feature:?} returned a mis-scoped or incomplete optional receipt"
        ));
    }
    for required in feature.required_operations() {
        if !observations.iter().any(|observation| {
            observation.feature == feature
                && observation.channel == expected.channel
                && observation.conversation == expected.conversation
                && observation.operation == *required
        }) {
            return Err(format!("declared {feature:?} omitted operation {required}"));
        }
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
    if feature == ConformanceFeature::ReplyAddressing
        && observation.reply_to_id != expected.reply_to_id
    {
        return Err("reply addressing differs from the configured target".into());
    }
    if feature == ConformanceFeature::ThreadAddressing
        && observation.thread_id != expected.thread_id
    {
        return Err("thread addressing differs from the configured target".into());
    }
    if feature == ConformanceFeature::VoiceIn
        && !(observation.payload_kinds.iter().any(|kind| kind == "audio")
            && observation
                .payload_kinds
                .iter()
                .any(|kind| kind == "transcript"))
    {
        return Err("VoiceIn ingress omitted audio or transcript evidence".into());
    }
    if let Some(receipt) = receipt
        && observation.platform_message_ids != receipt.platform_message_ids
    {
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
            Self::Sticker => Some("sticker"),
            Self::Location => Some("location"),
            Self::Contact => Some("contact"),
            Self::Buttons => Some("buttons"),
            Self::Poll => Some("poll"),
            Self::Presentation => Some("presentation"),
            Self::NativeData => Some("native"),
            Self::VoiceOut => Some("voice"),
            Self::VoiceSynthesis => Some("voice"),
            Self::VoiceIn => None,
            _ => None,
        }
    }

    fn required_operations(self) -> &'static [&'static str] {
        match self {
            Self::VoiceIn => &["voice_in"],
            Self::Typing => &["typing"],
            Self::Reactions => &["reaction_send"],
            Self::InboundReactions => &["reaction_receive"],
            Self::Edit => &["edit"],
            Self::Delete => &["delete"],
            Self::ReadReceipts => &["read_receipt"],
            Self::StreamingDraft => &[
                "draft_create",
                "draft_update",
                "draft_finalize",
                "draft_cancel_cleanup",
            ],
            Self::ManagedReaction => &["reaction_send"],
            Self::ManagedThreadCreate => &["thread_create"],
            Self::ManagedThreadUpdate => &["thread_update"],
            Self::ManagedThreadList => &["thread_list"],
            _ => &["send"],
        }
    }
}

/// Run the entire shared feature matrix against a provider fixture.
pub async fn check_all_features<F: ConformanceFixture + ?Sized>(
    fixture: &F,
) -> Vec<(ConformanceFeature, Result<(), String>)> {
    let mut results = Vec::with_capacity(ConformanceFeature::ALL.len());
    for feature in ConformanceFeature::ALL {
        results.push((feature, check_feature(fixture, feature).await));
    }
    results
}

/// Run a managed-controller matrix against a configured host callback fixture.
pub async fn check_managed_features<F: ConformanceFixture + ?Sized>(
    fixture: &F,
) -> Vec<(ConformanceFeature, Result<(), String>)> {
    let mut results = Vec::with_capacity(ConformanceFeature::MANAGED.len());
    for feature in ConformanceFeature::MANAGED {
        results.push((feature, check_feature(fixture, feature).await));
    }
    results
}

/// Send payload cases may share one full addressing fixture.
pub fn intent_scope(intent: &ChannelOutboundIntent) -> Option<ExpectedScope> {
    Some(ExpectedScope {
        channel: intent.channel_ref.clone()?,
        conversation: intent.conversation_ref.clone()?,
        reply_to_id: intent.reply_to_id.clone(),
        thread_id: intent.thread_id.clone().or_else(|| {
            intent.conversation_ref.as_ref().and_then(|conversation| {
                conversation
                    .thread_id
                    .clone()
                    .or_else(|| conversation.topic_id.clone())
            })
        }),
    })
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
