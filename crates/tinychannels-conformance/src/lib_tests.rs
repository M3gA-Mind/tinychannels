use super::*;
use tinychannels_bus::channel::{
    ChannelDescriptor, ChannelSendError, MessageReceipt, MessageReceiptPart,
};
use tinychannels_runtime::controllers::ChannelAccountSnapshot;

struct FixtureAdapter(ChannelCapabilities);

#[async_trait]
impl ChannelAdapter for FixtureAdapter {
    fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor {
            id: "telegram".into(),
            account_id: Some("telegram:account-a".into()),
            ..Default::default()
        }
    }

    fn declared_capabilities(&self) -> ChannelCapabilities {
        self.0.clone()
    }

    async fn start(
        &self,
        _: &(dyn tinychannels_runtime::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), ChannelSendError> {
        Ok(())
    }

    async fn send(&self, _: ChannelOutboundIntent) -> Result<ChannelSendOutcome, ChannelSendError> {
        Ok(ChannelSendOutcome::default())
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        Ok(ChannelAccountSnapshot::default())
    }
}

struct Fixture {
    adapter: FixtureAdapter,
    scope: ExpectedScope,
    result: Result<Option<ChannelSendOutcome>, ChannelSendError>,
    observations: Vec<WireObservation>,
}

#[async_trait]
impl AdapterFixture for Fixture {
    fn adapter(&self) -> &dyn ChannelAdapter {
        &self.adapter
    }

    fn expected_scope(&self, _: ConformanceFeature) -> ExpectedScope {
        self.scope.clone()
    }

    fn clear_observations(&self) {}

    fn observations(&self) -> Vec<WireObservation> {
        self.observations.clone()
    }

    async fn exercise(
        &self,
        _: ConformanceFeature,
    ) -> Result<Option<ChannelSendOutcome>, ChannelSendError> {
        self.result.clone()
    }
}

fn fixture(feature: ConformanceFeature) -> Fixture {
    let channel = ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram:account-a".into()),
    };
    let conversation = ConversationRef {
        kind: tinychannels_bus::ConversationKind::Dm,
        id: "100".into(),
        ..Default::default()
    };
    let mut capabilities = ChannelCapabilities::default();
    capabilities.content.text.outbound = feature == ConformanceFeature::Text;
    capabilities.content.image.outbound = feature == ConformanceFeature::Image;
    capabilities.actions.supported.push("typing".into());
    capabilities.events.supported.push("read_receipt".into());
    capabilities.voice_in.supported = true;
    capabilities
        .voice_in
        .transcript_sources
        .push(tinychannels_bus::channel::VoiceTranscriptSource::Provider);
    let mut receipt = MessageReceipt {
        channel: Some(channel.clone()),
        conversation: Some(conversation.clone()),
        primary_platform_message_id: Some("321".into()),
        platform_message_ids: vec!["321".into()],
        parts: vec![MessageReceiptPart {
            platform_message_id: "321".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    receipt.idempotency_key = Some("fixture-op".into());
    let observation = WireObservation {
        feature,
        operation: feature.required_operations()[0].into(),
        channel: channel.clone(),
        conversation: conversation.clone(),
        reply_to_id: None,
        thread_id: None,
        payload_kinds: feature
            .payload_kind()
            .map(|kind| vec![kind.into()])
            .unwrap_or_default(),
        platform_message_ids: vec!["321".into()],
        request: serde_json::json!({"chat_id": "100", "text": "fixture"}),
    };
    Fixture {
        adapter: FixtureAdapter(capabilities),
        scope: ExpectedScope {
            channel,
            conversation,
            reply_to_id: None,
            thread_id: None,
        },
        result: Ok(Some(ChannelSendOutcome {
            receipt,
            error: None,
        })),
        observations: vec![observation],
    }
}

#[tokio::test]
async fn check_feature_rejects_auth_failure_as_unsupported() {
    let mut fixture = fixture(ConformanceFeature::Image);
    fixture.adapter.0.content.image.outbound = false;
    fixture.result = Err(ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::Forbidden,
        message: "bad token".into(),
        retryable: false,
        ..Default::default()
    });
    fixture.observations.clear();
    assert!(
        check_feature(&fixture, ConformanceFeature::Image)
            .await
            .unwrap_err()
            .contains("explicit unsupported")
    );
}

#[tokio::test]
async fn check_feature_rejects_wrong_receipt_scope_and_platform_ids() {
    let mut wrong_scope = fixture(ConformanceFeature::Text);
    wrong_scope
        .result
        .as_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .receipt
        .channel
        .as_mut()
        .unwrap()
        .account_id = Some("telegram:account-b".into());
    assert!(
        check_feature(&wrong_scope, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("incomplete or mis-scoped")
    );

    let mut wrong_id = fixture(ConformanceFeature::Text);
    wrong_id.observations[0].platform_message_ids = vec!["999".into()];
    assert!(
        check_feature(&wrong_id, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("receipt IDs differ")
    );
}

#[tokio::test]
async fn check_feature_rejects_unknown_partial_effect_as_unsupported() {
    let mut fixture = fixture(ConformanceFeature::Image);
    fixture.adapter.0.content.image.outbound = false;
    fixture.result = Ok(Some(ChannelSendOutcome {
        receipt: MessageReceipt {
            primary_platform_message_id: Some("maybe-sent".into()),
            ..Default::default()
        },
        error: Some(ChannelSendError {
            unsupported: true,
            sent_before_error: true,
            ..Default::default()
        }),
    }));
    fixture.observations.clear();
    assert!(
        check_feature(&fixture, ConformanceFeature::Image)
            .await
            .unwrap_err()
            .contains("explicit unsupported")
    );
}

#[tokio::test]
async fn check_feature_rejects_missing_declared_send_receipt() {
    let mut fixture = fixture(ConformanceFeature::Text);
    fixture.result = Ok(Some(ChannelSendOutcome::default()));
    assert!(
        check_feature(&fixture, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("incomplete or mis-scoped receipt")
    );
}

#[tokio::test]
async fn check_feature_rejects_wrong_payload_even_with_matching_scope() {
    let mut fixture = fixture(ConformanceFeature::Image);
    fixture.observations[0].payload_kinds = vec!["text".into()];
    assert!(
        check_feature(&fixture, ConformanceFeature::Image)
            .await
            .unwrap_err()
            .contains("omitted image")
    );
}

#[tokio::test]
async fn check_feature_allows_declared_events_without_message_receipts() {
    let mut fixture = fixture(ConformanceFeature::Typing);
    fixture.result = Ok(None);
    fixture.observations[0].operation = "typing".into();
    fixture.observations[0].platform_message_ids.clear();
    assert!(
        check_feature(&fixture, ConformanceFeature::Typing)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn voice_in_requires_audio_and_transcript_on_the_observed_ingress() {
    let mut fixture = fixture(ConformanceFeature::VoiceIn);
    fixture.result = Ok(None);
    fixture.observations[0].operation = "voice_in".into();
    fixture.observations[0].payload_kinds = vec!["audio".into()];
    assert!(
        check_feature(&fixture, ConformanceFeature::VoiceIn)
            .await
            .unwrap_err()
            .contains("audio or transcript")
    );
}
