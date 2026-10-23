use super::*;
use crate::providers::native::fixtures::FakeProvider;
use tinychannels_bus::ChannelOutboundIntent;
use tinychannels_bus::channel::{MessageReceipt, MessageReceiptPart};
use tinychannels_bus::{ChannelRef, OutboundPayload};

fn conversation(id: &str) -> ConversationRef {
    ConversationRef {
        id: id.into(),
        ..Default::default()
    }
}

fn receipt(id: &str, conversation_id: &str) -> MessageReceipt {
    MessageReceipt {
        channel: Some(ChannelRef {
            id: "fake".into(),
            account_id: None,
        }),
        conversation: Some(conversation(conversation_id)),
        primary_platform_message_id: Some(id.into()),
        platform_message_ids: vec![id.into()],
        parts: vec![MessageReceiptPart {
            platform_message_id: id.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn text_intent(body: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "i".into(),
        channel_id: "fake".into(),
        conversation_id: "c1".into(),
        payload: OutboundPayload::Text { text: body.into() },
        ..Default::default()
    }
}

#[test]
fn seen_reservations_only_suppress_committed_keys() {
    let ports = HostPorts::default();
    let reservation = match ports.reserve_seen("event-1".into()) {
        SeenReservationResult::Reserved(reservation) => reservation,
        _ => panic!("first dispatch should own a new key"),
    };
    assert!(!ports.has_seen("event-1"));
    assert!(matches!(
        ports.reserve_seen("event-1".into()),
        SeenReservationResult::Pending
    ));
    assert!(!ports.mark_seen("event-1".into()));

    drop(reservation);
    assert!(!ports.has_seen("event-1"));
    let reservation = match ports.reserve_seen("event-1".into()) {
        SeenReservationResult::Reserved(reservation) => reservation,
        _ => panic!("dropping a reservation should release the key"),
    };
    reservation.commit();
    assert!(ports.has_seen("event-1"));
    assert!(matches!(
        ports.reserve_seen("event-1".into()),
        SeenReservationResult::Accepted
    ));
}

#[test]
fn the_error_helpers_distinguish_unsupported_from_rejected() {
    let unsupported = unsupported("no");
    assert!(unsupported.unsupported && !unsupported.retryable);
    let rejected = pre_dispatch("bad");
    assert!(!rejected.unsupported && rejected.kind == SendErrorKind::BadFormat);
    let failure = NativeFailure(rejected);
    assert_eq!(failure.to_string(), "bad");
}

/// A provider with only the required methods answers `unsupported` to the rest.
struct Minimal;

#[async_trait]
impl NativeProvider for Minimal {
    fn id(&self) -> &'static str {
        "min"
    }
    fn display(&self) -> &'static str {
        "Min"
    }
    fn limits(&self) -> Limits {
        Limits {
            text_chars: 10,
            file_bytes: 10,
            voice_bytes: 10,
        }
    }
    fn media_repository(&self) -> Option<&Arc<dyn MediaRepository>> {
        None
    }
    fn voice_media_store(&self) -> Option<&Arc<dyn VoiceMediaStore>> {
        None
    }
    fn voice_synthesis(&self) -> bool {
        false
    }
    fn declared_capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities::default()
    }
    async fn deliver(&self, _: &Target, _: &Step) -> Result<Vec<Sent>, ChannelSendError> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn optional_operations_default_to_explicitly_unsupported() {
    let minimal = Minimal;
    assert!(minimal.voice_caption_supported());
    assert_eq!(minimal.health_state().await, "unknown");
    assert!(!minimal.supports_webhook_delivery());
    assert!(
        minimal
            .deliver_webhook(None, b"payload")
            .await
            .unwrap_err()
            .to_string()
            .contains("no typed webhook ingress")
    );
    minimal
        .start_listener(&NoopInboundSink)
        .await
        .expect("the default listener is a no-op");
    minimal.stop_listener().await.unwrap();
    assert!(!minimal.bundles_steps());
    assert!(
        minimal
            .deliver_bundle(
                &Target {
                    channel: ChannelRef::default(),
                    conversation: ConversationRef::default(),
                    reply_to: None,
                    thread: None,
                },
                &[],
            )
            .await
            .err()
            .expect("bundle delivery is unsupported")
            .unsupported
    );
    assert!(
        minimal
            .start_draft(ChannelOutboundIntent::default())
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(
        minimal
            .update_draft("draft-1", ChannelOutboundIntent::default())
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(
        minimal
            .finalize_draft("draft-1", ChannelOutboundIntent::default())
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(minimal.set_typing("c", true).await.unwrap_err().unsupported);
    let reaction = ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    assert!(
        minimal
            .react(&conversation("c"), "1", &reaction, false)
            .await
            .unwrap_err()
            .unsupported
    );
    let address = Address {
        conversation: conversation("c"),
        message_id: "1".into(),
    };
    assert!(
        minimal
            .edit_text(&address, "x")
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(
        minimal
            .delete_message(&address)
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(!minimal.can_edit() && !minimal.can_draft());
    assert!(minimal.descriptor_extra().is_empty());
    assert!(minimal.proactive_target().is_none());
    assert!(
        edit(&minimal, &receipt("1", "c"), &text_intent("x"))
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(
        upsert_draft(&minimal, None, text_intent("x"))
            .await
            .unwrap_err()
            .unsupported
    );
}

struct NoopInboundSink;

#[async_trait]
impl crate::channel::ChannelInboundSink for NoopInboundSink {
    async fn push(
        &self,
        _envelope: crate::channel::ChannelInboundEnvelope,
    ) -> Result<(), ChannelSendError> {
        Ok(())
    }
}

#[test]
fn seen_window_evicts_the_oldest_accepted_key() {
    let ports = HostPorts::default();
    for index in 0..SEEN_WINDOW {
        assert!(ports.mark_seen(format!("event-{index}")));
    }
    assert!(!ports.mark_seen("event-0".into()));
    assert!(ports.mark_seen("event-new".into()));
    assert!(!ports.has_seen("event-0"));
    assert!(ports.has_seen("event-new"));
}

#[tokio::test]
async fn edits_validate_the_receipt_and_the_intent_before_any_io() {
    let provider = FakeProvider::default();
    let sent = receipt("m1", "c1");
    assert_eq!(
        edit(&provider, &sent, &text_intent("new")).await.unwrap(),
        sent
    );
    assert_eq!(
        provider.edited.lock().unwrap()[0],
        ("m1".into(), "new".into())
    );

    // A receipt from another channel or conversation, with no id, or an intent
    // that names a different conversation never reaches the platform.
    let mut foreign = sent.clone();
    foreign.channel = Some(ChannelRef {
        id: "other".into(),
        account_id: None,
    });
    let mut blank = sent.clone();
    blank.conversation = Some(conversation(" "));
    let mut idless = sent.clone();
    idless.primary_platform_message_id = None;
    idless.parts.clear();
    for bad in [foreign, blank, idless] {
        assert!(edit(&provider, &bad, &text_intent("x")).await.is_err());
        assert!(delete(&provider, &bad).await.is_err());
    }
    let mut elsewhere = text_intent("x");
    elsewhere.conversation_id = "c2".into();
    assert!(edit(&provider, &sent, &elsewhere).await.is_err());
    let mut other_channel = text_intent("x");
    other_channel.channel_ref = Some(ChannelRef {
        id: "other".into(),
        account_id: None,
    });
    assert!(edit(&provider, &sent, &other_channel).await.is_err());

    // Only plain text edits, within the limit.
    let mut not_text = text_intent("x");
    not_text.payload = OutboundPayload::Poll {
        question: "q".into(),
        options: vec![],
    };
    assert!(edit(&provider, &sent, &not_text).await.is_err());
    assert!(edit(&provider, &sent, &text_intent("  ")).await.is_err());
    let too_long = edit(&provider, &sent, &text_intent(&"x".repeat(21)))
        .await
        .unwrap_err();
    assert_eq!(too_long.kind, SendErrorKind::TooLong);
    assert_eq!(provider.edited.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn an_id_can_come_from_the_first_part_and_deletes_reach_the_provider() {
    let provider = FakeProvider::default();
    let mut sent = receipt("m9", "c1");
    sent.primary_platform_message_id = None;
    delete(&provider, &sent).await.unwrap();
    assert_eq!(provider.deleted.lock().unwrap()[0], "m9");
}

#[tokio::test]
async fn drafts_send_once_then_edit_in_place() {
    let provider = FakeProvider::default();
    let created = upsert_draft(&provider, None, text_intent("draft"))
        .await
        .unwrap();
    assert_eq!(created.platform_message_ids, ["m1"]);
    let updated = upsert_draft(&provider, Some(&created), text_intent("more"))
        .await
        .unwrap();
    assert_eq!(updated, created);
    assert_eq!(provider.delivered.lock().unwrap().len(), 1);
    assert_eq!(provider.edited.lock().unwrap()[0].1, "more");

    // A draft of something that is not text never sends its first message.
    let mut poll = text_intent("x");
    poll.payload = OutboundPayload::Poll {
        question: "q".into(),
        options: vec![],
    };
    assert!(upsert_draft(&provider, None, poll).await.is_err());
    assert_eq!(provider.delivered.lock().unwrap().len(), 1);

    let no_drafts = FakeProvider {
        drafts: false,
        ..Default::default()
    };
    assert!(
        upsert_draft(&no_drafts, None, text_intent("x"))
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(no_drafts.delivered.lock().unwrap().is_empty());
}

#[test]
fn reactions_for_another_channel_are_forbidden() {
    let provider = FakeProvider::default();
    let own = ChannelRef {
        id: "fake".into(),
        account_id: None,
    };
    assert!(edit::check_channel(&provider, &own).is_ok());
    let other = ChannelRef {
        id: "slack".into(),
        account_id: None,
    };
    assert_eq!(
        edit::check_channel(&provider, &other).unwrap_err().kind,
        SendErrorKind::Forbidden
    );
}
