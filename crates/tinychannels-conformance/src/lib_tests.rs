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

    async fn send(&self, _: ChannelOutboundIntent) -> Result<MessageReceipt, ChannelSendError> {
        Ok(MessageReceipt::default())
    }

    async fn status(&self) -> Result<ChannelAccountSnapshot, ChannelSendError> {
        Ok(ChannelAccountSnapshot::default())
    }
}

struct Fixture {
    adapter: FixtureAdapter,
    scope: ExpectedScope,
    requires_receipt: bool,
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

    fn receipt_required(&self, _: ConformanceFeature) -> bool {
        self.requires_receipt
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
        requires_receipt: true,
        result: Ok(Some(ChannelSendOutcome {
            receipt,
            error: None,
        })),
        observations: vec![observation],
    }
}

#[tokio::test]
async fn conformance_entrypoints_accept_existing_adapter_trait_objects() {
    let fixture = fixture(ConformanceFeature::Text);
    let adapter: &dyn AdapterFixture = &fixture;

    assert!(
        check_feature(adapter, ConformanceFeature::Text)
            .await
            .is_ok()
    );
    assert_eq!(
        check_all_features(adapter).await.len(),
        ConformanceFeature::ALL.len()
    );
}

#[tokio::test]
async fn receiptless_platform_send_requires_real_wire_evidence_without_fabricated_ids() {
    let mut fixture = fixture(ConformanceFeature::Text);
    fixture.requires_receipt = false;
    fixture.result = Ok(Some(ChannelSendOutcome::default()));
    fixture.observations[0].platform_message_ids.clear();

    assert!(
        check_feature(&fixture, ConformanceFeature::Text)
            .await
            .is_ok()
    );

    fixture.observations.clear();
    assert!(
        check_feature(&fixture, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("no matching wire observation")
    );
}

#[tokio::test]
async fn receiptless_platform_rejects_mis_scoped_optional_receipts() {
    let mut fixture = fixture(ConformanceFeature::Text);
    fixture.requires_receipt = false;
    fixture.result = Ok(Some(ChannelSendOutcome {
        receipt: MessageReceipt {
            channel: Some(ChannelRef {
                id: "another-channel".into(),
                account_id: Some("telegram:account-a".into()),
            }),
            conversation: Some(fixture.scope.conversation.clone()),
            platform_message_ids: vec!["321".into()],
            parts: vec![MessageReceiptPart {
                platform_message_id: "321".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        error: None,
    }));

    assert!(
        check_feature(&fixture, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("mis-scoped or incomplete optional receipt")
    );
}

fn declare_feature(fixture: &mut Fixture, feature: ConformanceFeature) {
    let capabilities = &mut fixture.adapter.0;
    match feature {
        ConformanceFeature::Text => capabilities.content.text.outbound = true,
        ConformanceFeature::Image => capabilities.content.image.outbound = true,
        ConformanceFeature::Video => capabilities.content.video.outbound = true,
        ConformanceFeature::Audio => capabilities.content.audio.outbound = true,
        ConformanceFeature::File => capabilities.content.file.outbound = true,
        ConformanceFeature::Sticker => capabilities.content.sticker.outbound = true,
        ConformanceFeature::Location => capabilities.content.location.outbound = true,
        ConformanceFeature::Contact => capabilities.content.contact.outbound = true,
        ConformanceFeature::Buttons => capabilities.content.buttons.outbound = true,
        ConformanceFeature::Poll => capabilities.content.poll.outbound = true,
        ConformanceFeature::Presentation => capabilities.content.presentation.outbound = true,
        ConformanceFeature::NativeData => capabilities.content.native.outbound = true,
        ConformanceFeature::VoiceIn => {
            capabilities.voice_in.supported = true;
            fixture.observations[0].payload_kinds = vec!["audio".into(), "transcript".into()];
        }
        ConformanceFeature::VoiceOut => capabilities.voice_out.supported = true,
        ConformanceFeature::VoiceSynthesis => capabilities.voice_out.synthesis = true,
        ConformanceFeature::ReplyAddressing => {
            capabilities.addressing.reply_to_id = true;
            fixture.scope.reply_to_id = Some("reply-5".into());
            fixture.observations[0].reply_to_id = Some("reply-5".into());
        }
        ConformanceFeature::ThreadAddressing => {
            capabilities.addressing.thread_id = true;
            fixture.scope.thread_id = Some("thread-7".into());
            fixture.observations[0].thread_id = Some("thread-7".into());
        }
        ConformanceFeature::Typing => capabilities.actions.supported.push("typing".into()),
        ConformanceFeature::Reactions => capabilities.reactions = true,
        ConformanceFeature::InboundReactions => {
            capabilities
                .events
                .supported
                .extend(["reaction_added".into(), "reaction_removed".into()]);
        }
        ConformanceFeature::Edit => capabilities.actions.supported.push("edit".into()),
        ConformanceFeature::Delete => capabilities.actions.supported.push("delete".into()),
        ConformanceFeature::ReadReceipts => {
            capabilities.events.supported.push("read_receipt".into());
        }
        ConformanceFeature::StreamingDraft => {
            capabilities.progressive_edits = true;
            capabilities.actions.supported.push("draft-update".into());
            fixture.observations.clear();
            for operation in feature.required_operations() {
                let mut observation = WireObservation {
                    feature,
                    operation: (*operation).into(),
                    channel: fixture.scope.channel.clone(),
                    conversation: fixture.scope.conversation.clone(),
                    reply_to_id: fixture.scope.reply_to_id.clone(),
                    thread_id: fixture.scope.thread_id.clone(),
                    payload_kinds: vec!["text".into()],
                    platform_message_ids: vec!["321".into()],
                    request: serde_json::json!({"operation": operation}),
                };
                observation.feature = feature;
                fixture.observations.push(observation);
            }
            fixture.result = Ok(Some(ChannelSendOutcome {
                receipt: MessageReceipt {
                    channel: Some(fixture.scope.channel.clone()),
                    conversation: Some(fixture.scope.conversation.clone()),
                    primary_platform_message_id: Some("321".into()),
                    platform_message_ids: vec!["321".into()],
                    parts: vec![MessageReceiptPart {
                        platform_message_id: "321".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                error: None,
            }));
        }
        ConformanceFeature::ManagedReaction => capabilities.reactions = true,
        ConformanceFeature::ManagedThreadCreate => {
            capabilities.actions.supported.push("thread-create".into())
        }
        ConformanceFeature::ManagedThreadUpdate => {
            capabilities.actions.supported.push("thread-update".into())
        }
        ConformanceFeature::ManagedThreadList => {
            capabilities.actions.supported.push("thread-list".into())
        }
    }
    if fixture.observations.is_empty() {
        fixture.observations.push(WireObservation {
            feature,
            operation: feature.required_operations()[0].into(),
            channel: fixture.scope.channel.clone(),
            conversation: fixture.scope.conversation.clone(),
            reply_to_id: fixture.scope.reply_to_id.clone(),
            thread_id: fixture.scope.thread_id.clone(),
            payload_kinds: feature
                .payload_kind()
                .map(|kind| vec![kind.into()])
                .unwrap_or_default(),
            platform_message_ids: vec!["321".into()],
            request: serde_json::json!({"chat_id": "100", "text": "fixture"}),
        });
    } else {
        for observation in &mut fixture.observations {
            observation.feature = feature;
            observation.channel = fixture.scope.channel.clone();
            observation.conversation = fixture.scope.conversation.clone();
            observation.reply_to_id = fixture.scope.reply_to_id.clone();
            observation.thread_id = fixture.scope.thread_id.clone();
        }
    }
}

#[tokio::test]
async fn shared_matrix_exercises_every_declared_feature_contract() {
    for feature in ConformanceFeature::ALL {
        let mut fixture = fixture(feature);
        declare_feature(&mut fixture, feature);
        if matches!(
            feature,
            ConformanceFeature::Typing
                | ConformanceFeature::Reactions
                | ConformanceFeature::InboundReactions
                | ConformanceFeature::Delete
                | ConformanceFeature::ReadReceipts
                | ConformanceFeature::VoiceIn
        ) {
            fixture.result = Ok(None);
            for observation in &mut fixture.observations {
                observation.platform_message_ids.clear();
            }
        }
        let result = check_feature(&fixture, feature).await;
        assert!(
            result.is_ok(),
            "{feature:?} should pass its declared case: {result:?}"
        );
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

#[tokio::test]
async fn address_features_require_the_expected_wire_addresses() {
    let mut fixture = fixture(ConformanceFeature::ReplyAddressing);
    fixture.adapter.0.addressing.reply_to_id = true;
    fixture.scope.reply_to_id = Some("reply-5".into());
    fixture.observations[0].reply_to_id = None;
    assert!(
        check_feature(&fixture, ConformanceFeature::ReplyAddressing)
            .await
            .unwrap_err()
            .contains("reply addressing")
    );
}

#[tokio::test]
async fn streaming_draft_requires_the_complete_observed_lifecycle() {
    let mut fixture = fixture(ConformanceFeature::StreamingDraft);
    fixture.adapter.0.progressive_edits = true;
    fixture
        .adapter
        .0
        .actions
        .supported
        .push("draft-update".into());
    fixture.observations[0].operation = "draft_create".into();
    assert!(
        check_feature(&fixture, ConformanceFeature::StreamingDraft)
            .await
            .unwrap_err()
            .contains("omitted operation draft_update")
    );
}

#[tokio::test]
async fn delete_operation_can_conform_without_a_message_receipt() {
    let mut fixture = fixture(ConformanceFeature::Delete);
    fixture.adapter.0.actions.supported.push("delete".into());
    fixture.result = Ok(None);
    fixture.observations[0].operation = "delete".into();
    fixture.observations[0].platform_message_ids.clear();
    assert!(
        check_feature(&fixture, ConformanceFeature::Delete)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn undeclared_features_accept_only_clean_explicit_unsupported_results() {
    let mut error_result = fixture(ConformanceFeature::Image);
    error_result.adapter.0.content.image.outbound = false;
    error_result.observations.clear();
    error_result.result = Err(ChannelSendError {
        unsupported: true,
        retryable: false,
        sent_before_error: false,
        ..Default::default()
    });
    assert!(
        check_feature(&error_result, ConformanceFeature::Image)
            .await
            .is_ok()
    );

    let mut outcome_result = fixture(ConformanceFeature::Image);
    outcome_result.adapter.0.content.image.outbound = false;
    outcome_result.observations.clear();
    outcome_result.result = Ok(Some(ChannelSendOutcome {
        receipt: MessageReceipt::default(),
        error: Some(ChannelSendError {
            unsupported: true,
            retryable: false,
            sent_before_error: false,
            ..Default::default()
        }),
    }));
    assert!(
        check_feature(&outcome_result, ConformanceFeature::Image)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn undeclared_feature_rejects_side_effects_and_nonexplicit_unsupported() {
    let mut wire_side_effect = fixture(ConformanceFeature::Image);
    wire_side_effect.adapter.0.content.image.outbound = false;
    wire_side_effect.result = Err(ChannelSendError {
        unsupported: true,
        ..Default::default()
    });
    assert!(
        check_feature(&wire_side_effect, ConformanceFeature::Image)
            .await
            .unwrap_err()
            .contains("reached the mock platform")
    );

    let mut retryable = fixture(ConformanceFeature::Image);
    retryable.adapter.0.content.image.outbound = false;
    retryable.observations.clear();
    retryable.result = Err(ChannelSendError {
        unsupported: true,
        retryable: true,
        ..Default::default()
    });
    assert!(
        check_feature(&retryable, ConformanceFeature::Image)
            .await
            .unwrap_err()
            .contains("explicit unsupported")
    );

    let mut receipt_side_effect = fixture(ConformanceFeature::Image);
    receipt_side_effect.adapter.0.content.image.outbound = false;
    receipt_side_effect.observations.clear();
    receipt_side_effect.result = Ok(Some(ChannelSendOutcome {
        receipt: MessageReceipt {
            platform_message_ids: vec!["already-sent".into()],
            ..Default::default()
        },
        error: Some(ChannelSendError {
            unsupported: true,
            ..Default::default()
        }),
    }));
    assert!(
        check_feature(&receipt_side_effect, ConformanceFeature::Image)
            .await
            .unwrap_err()
            .contains("explicit unsupported")
    );
}

#[tokio::test]
async fn declared_operations_must_have_a_matching_wire_scope() {
    let mut missing = fixture(ConformanceFeature::Text);
    missing.observations.clear();
    assert!(
        check_feature(&missing, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("no matching wire observation")
    );

    let mut wrong_conversation = fixture(ConformanceFeature::Text);
    wrong_conversation.observations[0].conversation.id = "other-chat".into();
    assert!(
        check_feature(&wrong_conversation, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("no matching wire observation")
    );

    let mut missing_send = fixture(ConformanceFeature::Text);
    missing_send.observations[0].operation = "typing".into();
    assert!(
        check_feature(&missing_send, ConformanceFeature::Text)
            .await
            .unwrap_err()
            .contains("omitted operation send")
    );
}

#[tokio::test]
async fn thread_addressing_and_voice_ingress_require_observed_evidence() {
    let mut thread = fixture(ConformanceFeature::ThreadAddressing);
    thread.adapter.0.addressing.thread_id = true;
    thread.scope.thread_id = Some("topic-9".into());
    thread.observations[0].thread_id = Some("topic-8".into());
    assert!(
        check_feature(&thread, ConformanceFeature::ThreadAddressing)
            .await
            .unwrap_err()
            .contains("thread addressing")
    );

    let mut voice = fixture(ConformanceFeature::VoiceIn);
    voice.result = Ok(None);
    voice.observations[0].payload_kinds = vec!["transcript".into()];
    assert!(
        check_feature(&voice, ConformanceFeature::VoiceIn)
            .await
            .unwrap_err()
            .contains("audio or transcript")
    );
}

#[tokio::test]
async fn streaming_draft_passes_with_all_four_lifecycle_operations() {
    let mut draft = fixture(ConformanceFeature::StreamingDraft);
    declare_feature(&mut draft, ConformanceFeature::StreamingDraft);
    assert!(
        check_feature(&draft, ConformanceFeature::StreamingDraft)
            .await
            .is_ok()
    );
}

#[test]
fn intent_scope_uses_channel_conversation_and_address_fallbacks() {
    let mut intent = ChannelOutboundIntent::default();
    assert!(intent_scope(&intent).is_none());

    intent.channel_ref = Some(ChannelRef {
        id: "matrix".into(),
        account_id: Some("matrix:one".into()),
    });
    intent.conversation_ref = Some(ConversationRef {
        kind: tinychannels_bus::ConversationKind::Group,
        id: "room".into(),
        thread_id: Some("conversation-thread".into()),
        ..Default::default()
    });
    intent.reply_to_id = Some("parent".into());
    let scope = intent_scope(&intent).unwrap();
    assert_eq!(scope.channel.id, "matrix");
    assert_eq!(scope.conversation.id, "room");
    assert_eq!(scope.reply_to_id.as_deref(), Some("parent"));
    assert_eq!(scope.thread_id.as_deref(), Some("conversation-thread"));

    intent.thread_id = Some("explicit-thread".into());
    assert_eq!(
        intent_scope(&intent).unwrap().thread_id.as_deref(),
        Some("explicit-thread")
    );
    intent.thread_id = None;
    intent.conversation_ref.as_mut().unwrap().thread_id = None;
    intent.conversation_ref.as_mut().unwrap().topic_id = Some("topic".into());
    assert_eq!(
        intent_scope(&intent).unwrap().thread_id.as_deref(),
        Some("topic")
    );
}

#[test]
fn feature_matrix_contains_each_canonical_media_and_event_case_once() {
    let expected = [
        ConformanceFeature::Text,
        ConformanceFeature::Image,
        ConformanceFeature::Video,
        ConformanceFeature::Audio,
        ConformanceFeature::File,
        ConformanceFeature::Sticker,
        ConformanceFeature::Location,
        ConformanceFeature::Contact,
        ConformanceFeature::Buttons,
        ConformanceFeature::Poll,
        ConformanceFeature::Presentation,
        ConformanceFeature::NativeData,
        ConformanceFeature::VoiceIn,
        ConformanceFeature::VoiceOut,
        ConformanceFeature::VoiceSynthesis,
        ConformanceFeature::ReplyAddressing,
        ConformanceFeature::ThreadAddressing,
        ConformanceFeature::Typing,
        ConformanceFeature::Reactions,
        ConformanceFeature::InboundReactions,
        ConformanceFeature::Edit,
        ConformanceFeature::Delete,
        ConformanceFeature::ReadReceipts,
        ConformanceFeature::StreamingDraft,
    ];
    assert_eq!(ConformanceFeature::ALL, expected);
}

#[test]
fn each_canonical_feature_uses_its_contract_capability() {
    for feature in ConformanceFeature::ALL {
        let mut capabilities = ChannelCapabilities::default();
        match feature {
            ConformanceFeature::Text => capabilities.content.text.outbound = true,
            ConformanceFeature::Image => capabilities.content.image.outbound = true,
            ConformanceFeature::Video => capabilities.content.video.outbound = true,
            ConformanceFeature::Audio => capabilities.content.audio.outbound = true,
            ConformanceFeature::File => capabilities.content.file.outbound = true,
            ConformanceFeature::Sticker => capabilities.content.sticker.outbound = true,
            ConformanceFeature::Location => capabilities.content.location.outbound = true,
            ConformanceFeature::Contact => capabilities.content.contact.outbound = true,
            ConformanceFeature::Buttons => capabilities.content.buttons.outbound = true,
            ConformanceFeature::Poll => capabilities.content.poll.outbound = true,
            ConformanceFeature::Presentation => capabilities.content.presentation.outbound = true,
            ConformanceFeature::NativeData => capabilities.content.native.outbound = true,
            ConformanceFeature::VoiceIn => capabilities.voice_in.supported = true,
            ConformanceFeature::VoiceOut => capabilities.voice_out.supported = true,
            ConformanceFeature::VoiceSynthesis => capabilities.voice_out.synthesis = true,
            ConformanceFeature::ReplyAddressing => capabilities.addressing.reply_to_id = true,
            ConformanceFeature::ThreadAddressing => capabilities.addressing.thread_id = true,
            ConformanceFeature::Typing => capabilities.actions.supported.push("typing".into()),
            ConformanceFeature::Reactions => capabilities.reactions = true,
            ConformanceFeature::InboundReactions => capabilities
                .events
                .supported
                .extend(["reaction_added".into(), "reaction_removed".into()]),
            ConformanceFeature::Edit => capabilities.actions.supported.push("edit".into()),
            ConformanceFeature::Delete => capabilities.actions.supported.push("unsend".into()),
            ConformanceFeature::ReadReceipts => {
                capabilities.events.supported.push("read_receipt".into())
            }
            ConformanceFeature::StreamingDraft => {
                capabilities.progressive_edits = true;
                capabilities.actions.supported.push("draft-update".into());
            }
            ConformanceFeature::ManagedReaction
            | ConformanceFeature::ManagedThreadCreate
            | ConformanceFeature::ManagedThreadUpdate
            | ConformanceFeature::ManagedThreadList => {
                unreachable!("managed features are not part of the provider matrix")
            }
        }
        assert!(
            feature.declared(&capabilities),
            "{feature:?} contract capability"
        );
    }

    let mut delete_capabilities = ChannelCapabilities::default();
    delete_capabilities.actions.supported.push("delete".into());
    assert!(ConformanceFeature::Delete.declared(&delete_capabilities));

    let mut incomplete_draft = ChannelCapabilities::default();
    incomplete_draft
        .actions
        .supported
        .push("draft-update".into());
    assert!(!ConformanceFeature::StreamingDraft.declared(&incomplete_draft));
}

#[tokio::test]
async fn managed_features_require_a_host_callback_fixture() {
    let adapter_fixture = fixture(ConformanceFeature::Text);
    let observations = ConformanceFixture::observations(&adapter_fixture);
    let result = check_feature(&adapter_fixture, ConformanceFeature::ManagedReaction)
        .await
        .unwrap_err();
    assert!(result.contains("requires a managed host callback fixture"));
    assert_eq!(
        ConformanceFixture::observations(&adapter_fixture),
        observations
    );
}

struct NativeControllerFixtureCase {
    capabilities: ChannelCapabilities,
    callbacks: std::sync::atomic::AtomicUsize,
    observations: std::sync::Mutex<Vec<NativeControllerObservation>>,
    unsupported: bool,
    invoke_managed: bool,
}

#[async_trait]
impl NativeControllerFixture for NativeControllerFixtureCase {
    fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities.clone()
    }

    fn provider(&self) -> &str {
        "fixture-provider"
    }

    async fn managed_callback_count(&self) -> usize {
        self.callbacks.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn clear_observations(&self) {
        self.observations.lock().unwrap().clear();
    }

    fn observations(&self) -> Vec<NativeControllerObservation> {
        self.observations.lock().unwrap().clone()
    }

    async fn exercise(
        &self,
        feature: NativeControllerFeature,
    ) -> Result<(), NativeControllerError> {
        if self.invoke_managed {
            self.callbacks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if self.unsupported {
            return Err(NativeControllerError {
                unsupported: true,
                message: "provider does not declare this operation".into(),
            });
        }
        self.observations
            .lock()
            .unwrap()
            .push(NativeControllerObservation {
                feature,
                provider: self.provider().to_owned(),
                operation: "mocked_provider_operation".into(),
                request: serde_json::json!({"actual": "request"}),
                response: serde_json::json!({"actual": "response"}),
                returned_ids: vec!["provider-id".into()],
            });
        Ok(())
    }
}

#[tokio::test]
async fn native_controller_matrix_requires_declared_wire_and_no_managed_callback() {
    let capabilities = ChannelCapabilities {
        reactions: true,
        ..Default::default()
    };
    let fixture = NativeControllerFixtureCase {
        capabilities,
        callbacks: std::sync::atomic::AtomicUsize::new(0),
        observations: std::sync::Mutex::new(Vec::new()),
        unsupported: false,
        invoke_managed: false,
    };
    assert!(
        check_native_controller_feature(&fixture, NativeControllerFeature::Reaction)
            .await
            .is_ok()
    );

    let unsupported = NativeControllerFixtureCase {
        capabilities: ChannelCapabilities::default(),
        callbacks: std::sync::atomic::AtomicUsize::new(0),
        observations: std::sync::Mutex::new(Vec::new()),
        unsupported: true,
        invoke_managed: false,
    };
    assert!(
        check_native_controller_feature(&unsupported, NativeControllerFeature::ThreadList)
            .await
            .is_ok()
    );

    let fallback = NativeControllerFixtureCase {
        capabilities: ChannelCapabilities::default(),
        callbacks: std::sync::atomic::AtomicUsize::new(0),
        observations: std::sync::Mutex::new(Vec::new()),
        unsupported: true,
        invoke_managed: true,
    };
    assert!(
        check_native_controller_feature(&fallback, NativeControllerFeature::ThreadList)
            .await
            .unwrap_err()
            .contains("managed host callback")
    );
}
