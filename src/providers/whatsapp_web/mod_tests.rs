#![allow(deprecated)]

use super::*;
use crate::channel::{ChannelAdapter, ChannelTyping};
use tinychannels_bus::SendMessage;

#[cfg(feature = "whatsapp-web")]
fn make_channel() -> WhatsAppWebChannel {
    WhatsAppWebChannel::new(
        "/tmp/test-whatsapp.db".into(),
        None,
        None,
        vec!["+1234567890".into()],
    )
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_channel_name() {
    let ch = make_channel();
    assert_eq!(ch.descriptor().id, "whatsapp");
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_number_allowed_exact() {
    let ch = make_channel();
    assert!(ch.is_number_allowed("+1234567890"));
    assert!(!ch.is_number_allowed("+9876543210"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_number_allowed_wildcard() {
    let ch = WhatsAppWebChannel::new("/tmp/test.db".into(), None, None, vec!["*".into()]);
    assert!(ch.is_number_allowed("+1234567890"));
    assert!(ch.is_number_allowed("+9999999999"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_number_denied_empty() {
    let ch = WhatsAppWebChannel::new("/tmp/test.db".into(), None, None, vec![]);
    // Empty allowed_numbers means "allow all" (same behavior as Cloud API)
    assert!(ch.is_number_allowed("+1234567890"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_normalize_phone_adds_plus() {
    let ch = make_channel();
    assert_eq!(ch.normalize_phone("1234567890"), "+1234567890");
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_normalize_phone_preserves_plus() {
    let ch = make_channel();
    assert_eq!(ch.normalize_phone("+1234567890"), "+1234567890");
}

#[tokio::test]
#[cfg(feature = "whatsapp-web")]
async fn whatsapp_web_health_check_disconnected() {
    let ch = make_channel();
    assert!(!crate::provider_test_support::is_healthy(&ch).await);
}

#[tokio::test]
#[cfg(feature = "whatsapp-web")]
async fn whatsapp_web_health_check_tracks_connected_flag() {
    let ch = make_channel();
    assert!(!crate::provider_test_support::is_healthy(&ch).await);
    ch.connected.store(true, Ordering::Release);
    assert!(crate::provider_test_support::is_healthy(&ch).await);
    ch.connected.store(false, Ordering::Release);
    assert!(!crate::provider_test_support::is_healthy(&ch).await);
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_compute_reply_target_dm_pn() {
    assert_eq!(
        WhatsAppWebChannel::compute_reply_target("123@s.whatsapp.net", "+1234567890"),
        "+1234567890"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_compute_reply_target_dm_lid() {
    assert_eq!(
        WhatsAppWebChannel::compute_reply_target("abc@lid", "+1234567890"),
        "+1234567890"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_compute_reply_target_group() {
    assert_eq!(
        WhatsAppWebChannel::compute_reply_target("987654@g.us", "+1234567890"),
        "987654@g.us"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_redact_phone_e164() {
    assert_eq!(WhatsAppWebChannel::redact_phone("+1234567890"), "+***7890");
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_redact_phone_no_plus() {
    assert_eq!(WhatsAppWebChannel::redact_phone("1234567890"), "***7890");
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_redact_phone_short_input() {
    // Pathological short inputs collapse to a generic mask rather than
    // exposing the entire identifier.
    assert_eq!(WhatsAppWebChannel::redact_phone("+12"), "+****");
    assert_eq!(WhatsAppWebChannel::redact_phone("12"), "****");
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_extract_message_text_prefers_conversation() {
    assert_eq!(
        WhatsAppWebChannel::extract_message_text(Some("hello"), Some("ignored")),
        "hello"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_extract_message_text_falls_back_to_extended() {
    assert_eq!(
        WhatsAppWebChannel::extract_message_text(None, Some("from extended")),
        "from extended"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_extract_message_text_empty_when_missing() {
    assert_eq!(WhatsAppWebChannel::extract_message_text(None, None), "");
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_batch_skips_self_echoes_without_dropping_following_messages() {
    let batch = [false, true, false];
    let delivered: Vec<_> = WhatsAppWebChannel::non_self_echoes(&batch, |is_from_me| *is_from_me)
        .copied()
        .collect();

    assert_eq!(delivered, [false, false]);
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_is_group_jid_recognises_group() {
    assert!(WhatsAppWebChannel::is_group_jid("123456@g.us"));
    assert!(WhatsAppWebChannel::is_group_jid("  4567@g.us  "));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_is_group_jid_rejects_non_group() {
    assert!(!WhatsAppWebChannel::is_group_jid("+1234567890"));
    assert!(!WhatsAppWebChannel::is_group_jid("123@s.whatsapp.net"));
    assert!(!WhatsAppWebChannel::is_group_jid("abc@lid"));
    assert!(!WhatsAppWebChannel::is_group_jid(""));
}

/// Regression for CodeRabbit finding: an `@g.us` reply target was being
/// silently dropped because the outbound path normalised the JID to
/// `+<group-id>` and missed the per-number allowlist. After provenance
/// is recorded, an allowed user replying back into the group they came
/// from must succeed.
#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_should_allow_outbound_provenanced_group_allowed() {
    let ch = make_channel(); // allowed_numbers = ["+1234567890"]
    ch.allowed_groups
        .lock()
        .insert("987654321@g.us".to_string());
    assert!(ch.should_allow_outbound("987654321@g.us"));
}

/// Regression for the follow-up CodeRabbit finding: a blanket `@g.us`
/// bypass is itself a vulnerability — a caller able to set `recipient`
/// could post into arbitrary joined groups. Groups without recorded
/// provenance must stay blocked.
#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_should_allow_outbound_unrelated_group_blocked() {
    let ch = make_channel();
    ch.allowed_groups
        .lock()
        .insert("987654321@g.us".to_string());
    assert!(!ch.should_allow_outbound("11111@g.us"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_should_allow_outbound_group_without_provenance_blocked() {
    let ch = make_channel();
    // empty allowed_groups
    assert!(!ch.should_allow_outbound("987654321@g.us"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_redact_recipient_pn_jid() {
    assert_eq!(
        WhatsAppWebChannel::redact_recipient("1234567890@s.whatsapp.net"),
        "***7890@s.whatsapp.net"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_redact_recipient_group_jid() {
    assert_eq!(
        WhatsAppWebChannel::redact_recipient("987654321@g.us"),
        "***4321@g.us"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_redact_recipient_bare_phone() {
    assert_eq!(
        WhatsAppWebChannel::redact_recipient("+1234567890"),
        "+***7890"
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_should_allow_outbound_dm_blocks_unallowed() {
    let ch = make_channel();
    assert!(!ch.should_allow_outbound("+9999999999"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_should_allow_outbound_dm_allows_match() {
    let ch = make_channel();
    assert!(ch.should_allow_outbound("+1234567890"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_should_allow_outbound_wildcard_passes_dm() {
    let ch = WhatsAppWebChannel::new("/tmp/t.db".into(), None, None, vec!["*".into()]);
    assert!(ch.should_allow_outbound("+9999999999"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn whatsapp_web_should_allow_outbound_empty_allowlist_passes_dm() {
    let ch = WhatsAppWebChannel::new("/tmp/t.db".into(), None, None, vec![]);
    assert!(ch.should_allow_outbound("+9999999999"));
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn a_phone_addressed_sender_is_its_number() {
    let phone = whatsapp_rust::Jid::pn("15550001111");
    let (sender, allow) = WhatsAppWebChannel::sender_identity(&phone, None);
    assert_eq!(sender, "+15550001111");
    assert_eq!(allow, vec!["+15550001111".to_string()]);
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn a_lid_sender_with_its_phone_is_the_phone() {
    let lid = whatsapp_rust::Jid::lid("98765432101234");
    let phone = whatsapp_rust::Jid::pn("15550001111");
    let (sender, allow) = WhatsAppWebChannel::sender_identity(&lid, Some(&phone));
    assert_eq!(sender, "+15550001111");
    // The phone, and the form earlier builds compared, both match.
    assert_eq!(
        allow,
        vec!["+15550001111".to_string(), "+98765432101234".to_string()]
    );
}

#[test]
#[cfg(feature = "whatsapp-web")]
fn a_lid_sender_alone_stays_a_lid_jid_not_a_phone() {
    let lid = whatsapp_rust::Jid::lid("98765432101234");
    let (sender, allow) = WhatsAppWebChannel::sender_identity(&lid, None);
    assert_eq!(sender, "98765432101234@lid");
    assert!(!sender.starts_with('+'));
    assert_eq!(allow, vec!["+98765432101234".to_string()]);
    // A DM reply goes back to the LID JID, not to a phone JID of its digits.
    let reply = WhatsAppWebChannel::compute_reply_target("98765432101234@lid", &sender);
    assert_eq!(reply, "98765432101234@lid");
    let jid = make_channel().recipient_to_jid(&reply).unwrap();
    assert!(jid.server.is_lid_family());
}

// ---------------------------------------------------------------------------
// Inbound event handling (`InboundState`)
// ---------------------------------------------------------------------------

use std::time::Duration;
use tokio::sync::mpsc;
use wacore::types::events::{
    BatchOrigin, Connected, Event, InboundMessage, LoggedOut, MessageBatch, PairingCode,
    PairingQrCode, PairingQrCodesExhausted, StreamError,
};
use wacore::types::message::{MessageInfo, MessageSource};
use whatsapp_rust::Jid;
use whatsapp_rust::waproto::whatsapp as wa;

/// Upper bound on any wait in these tests; a hang guard, never a race.
const HANG_GUARD: Duration = Duration::from_secs(30);

fn inbound_state(allowed: &[&str]) -> (InboundState, mpsc::Receiver<ChannelMessage>) {
    let (tx, rx) = mpsc::channel(16);
    (super::fakes::base_state(tx, allowed), rx)
}

struct Inbound {
    chat: Jid,
    sender: Jid,
    sender_alt: Option<Jid>,
    from_me: bool,
    push_name: &'static str,
    conversation: Option<&'static str>,
    extended: Option<&'static str>,
}

impl Inbound {
    fn dm(phone: &str, text: &'static str) -> Self {
        Self {
            chat: Jid::pn(phone),
            sender: Jid::pn(phone),
            sender_alt: None,
            from_me: false,
            push_name: "",
            conversation: Some(text),
            extended: None,
        }
    }

    fn build(self) -> InboundMessage {
        let mut message = wa::Message {
            conversation: self.conversation.map(str::to_string),
            ..Default::default()
        };
        if let Some(text) = self.extended {
            message.extended_text_message = wa::message::ExtendedTextMessage {
                text: Some(text.to_string()),
                ..Default::default()
            }
            .into();
        }
        let info = MessageInfo {
            source: MessageSource {
                chat: self.chat,
                sender: self.sender,
                sender_alt: self.sender_alt,
                is_from_me: self.from_me,
                ..Default::default()
            },
            push_name: self.push_name.to_string(),
            ..Default::default()
        };
        InboundMessage::builder()
            .message(Arc::new(message))
            .info(Arc::new(info))
            .build()
    }
}

fn batch(messages: Vec<Inbound>) -> Event {
    let messages: Vec<InboundMessage> = messages.into_iter().map(Inbound::build).collect();
    Event::Messages(
        MessageBatch::builder()
            .messages(messages.into())
            .origin(BatchOrigin::Live)
            .build(),
    )
}

fn drain(rx: &mut mpsc::Receiver<ChannelMessage>) -> Vec<ChannelMessage> {
    let mut out = Vec::new();
    while let Ok(message) = rx.try_recv() {
        out.push(message);
    }
    out
}

#[tokio::test]
async fn an_allowed_dm_is_forwarded_with_its_sender_and_reply_target() {
    let (state, mut rx) = inbound_state(&["+15550001111"]);
    let mut dm = Inbound::dm("15550001111", "hello there");
    dm.push_name = "  Ada  ";
    state.handle_event(&batch(vec![dm])).await;

    let got = drain(&mut rx);
    assert_eq!(got.len(), 1);
    let message = &got[0];
    assert_eq!(message.channel, "whatsapp");
    assert_eq!(message.sender, "+15550001111");
    assert_eq!(message.reply_target, "+15550001111");
    assert_eq!(message.content, "hello there");
    assert_eq!(message.sender_name.as_deref(), Some("Ada"));
    assert_eq!(message.thread_ts, None);
    assert!(!message.id.is_empty());
    assert!(message.timestamp > 0);
    // A DM records no group provenance.
    assert!(state.allowed_groups.lock().is_empty());
}

#[tokio::test]
async fn a_blank_push_name_leaves_the_sender_name_unset() {
    let (state, mut rx) = inbound_state(&[]);
    let mut dm = Inbound::dm("15550001111", "hi");
    dm.push_name = "   ";
    state.handle_event(&batch(vec![dm])).await;
    let got = drain(&mut rx);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].sender_name, None);
}

#[tokio::test]
async fn extended_text_is_used_when_there_is_no_plain_conversation() {
    let (state, mut rx) = inbound_state(&["*"]);
    let mut dm = Inbound::dm("15550001111", "unused");
    dm.conversation = None;
    dm.extended = Some("linked text");
    state.handle_event(&batch(vec![dm])).await;
    assert_eq!(drain(&mut rx)[0].content, "linked text");
}

#[tokio::test]
async fn self_echoes_in_a_batch_are_dropped_and_the_rest_delivered() {
    let (state, mut rx) = inbound_state(&[]);
    let mut echo = Inbound::dm("15550001111", "mine");
    echo.from_me = true;
    state
        .handle_event(&batch(vec![
            Inbound::dm("15550001111", "first"),
            echo,
            Inbound::dm("15550002222", "third"),
        ]))
        .await;
    let contents: Vec<_> = drain(&mut rx).into_iter().map(|m| m.content).collect();
    assert_eq!(contents, ["first", "third"]);
}

#[tokio::test]
async fn a_sender_off_the_allow_list_is_not_forwarded() {
    let (state, mut rx) = inbound_state(&["+15550001111"]);
    let mut group_msg = Inbound::dm("15559999999", "spam");
    group_msg.chat = "120363000000000001@g.us".parse().unwrap();
    state
        .handle_event(&batch(vec![Inbound::dm("15559999999", "nope"), group_msg]))
        .await;
    assert!(drain(&mut rx).is_empty());
    // A rejected sender must not grant outbound provenance to their group.
    assert!(state.allowed_groups.lock().is_empty());
}

#[tokio::test]
async fn an_allowed_group_message_replies_to_the_group_and_records_provenance() {
    let (state, mut rx) = inbound_state(&["+15550001111"]);
    let mut group_msg = Inbound::dm("15550001111", "in the group");
    group_msg.chat = "120363000000000001@g.us".parse().unwrap();
    state.handle_event(&batch(vec![group_msg])).await;

    let got = drain(&mut rx);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].sender, "+15550001111");
    assert_eq!(got[0].reply_target, "120363000000000001@g.us");
    assert!(
        state
            .allowed_groups
            .lock()
            .contains("120363000000000001@g.us")
    );
}

#[tokio::test]
async fn a_lid_sender_is_allowed_through_its_phone_alternate() {
    let (state, mut rx) = inbound_state(&["+15550001111"]);
    let mut msg = Inbound::dm("15550001111", "via lid");
    msg.sender = Jid::lid("98765432101234");
    msg.chat = Jid::lid("98765432101234");
    msg.sender_alt = Some(Jid::pn("15550001111"));
    state.handle_event(&batch(vec![msg])).await;
    let got = drain(&mut rx);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].sender, "+15550001111");
    assert_eq!(got[0].reply_target, "+15550001111");
}

#[tokio::test]
async fn a_closed_inbound_sink_does_not_panic_or_record_state() {
    let (state, rx) = inbound_state(&[]);
    drop(rx);
    state
        .handle_event(&batch(vec![Inbound::dm("15550001111", "lost")]))
        .await;
    assert!(!state.connected.load(Ordering::Acquire));
}

#[test]
fn sender_allow_check_matches_any_identity_form_or_wildcard() {
    let (open, _rx) = inbound_state(&[]);
    assert!(open.is_sender_allowed(&["+1".to_string()]));
    let (wildcard, _rx) = inbound_state(&["*"]);
    assert!(wildcard.is_sender_allowed(&["+1".to_string()]));
    let (listed, _rx) = inbound_state(&["+2"]);
    assert!(listed.is_sender_allowed(&["+1".to_string(), "+2".to_string()]));
    assert!(!listed.is_sender_allowed(&["+1".to_string()]));
    assert!(!listed.is_sender_allowed(&[]));
}

#[tokio::test]
async fn connection_events_drive_the_liveness_flag() {
    let (state, mut rx) = inbound_state(&[]);
    state
        .handle_event(&Event::Connected(Connected::builder().build()))
        .await;
    assert!(state.connected.load(Ordering::Acquire));

    state
        .handle_event(&Event::LoggedOut(
            LoggedOut::builder()
                .on_connect(false)
                .reason(wacore::types::events::ConnectFailureReason::LoggedOut)
                .build(),
        ))
        .await;
    assert!(!state.connected.load(Ordering::Acquire));

    state.connected.store(true, Ordering::Release);
    state
        .handle_event(&Event::StreamError(
            StreamError::builder().code("515".to_string()).build(),
        ))
        .await;
    assert!(!state.connected.load(Ordering::Acquire));
    assert!(drain(&mut rx).is_empty());
}

#[tokio::test]
async fn pairing_and_unrelated_events_change_nothing() {
    let (state, mut rx) = inbound_state(&[]);
    state.connected.store(true, Ordering::Release);
    for event in [
        Event::PairingCode(
            PairingCode::builder()
                .code("ABCD-EFGH".to_string())
                .timeout(Duration::from_secs(180))
                .build(),
        ),
        Event::PairingQrCode(
            PairingQrCode::builder()
                .code("qr-payload".to_string())
                .timeout(Duration::from_secs(20))
                .build(),
        ),
        Event::PairingQrCodesExhausted(
            PairingQrCodesExhausted::builder()
                .disconnected(true)
                .build(),
        ),
    ] {
        state.clone().handle_owned(Arc::new(event)).await;
    }
    assert!(state.connected.load(Ordering::Acquire));
    assert!(drain(&mut rx).is_empty());
    assert!(state.allowed_groups.lock().is_empty());
}

// ---------------------------------------------------------------------------
// Outbound paths over the `OutboundClient` seam
// ---------------------------------------------------------------------------

use super::fakes::{Call, RecordingClient};

fn connected_channel(fail: bool) -> (WhatsAppWebChannel, Arc<RecordingClient>) {
    let ch = make_channel(); // allowed_numbers = ["+1234567890"]
    let fake = Arc::new(RecordingClient {
        fail,
        ..Default::default()
    });
    *ch.client.lock() = Some(fake.clone() as Arc<dyn OutboundClient>);
    (ch, fake)
}

fn message(recipient: &str, content: &str) -> SendMessage {
    SendMessage::new(content, recipient)
}

#[tokio::test]
async fn outbound_calls_fail_before_the_bot_is_built() {
    let ch = make_channel();
    let send_error = crate::provider_test_support::send_v1(&ch, &message("+1234567890", "hi"))
        .await
        .unwrap_err()
        .to_string();
    let start_error = ch.start_typing("+1234567890").await.unwrap_err().message;
    let stop_error = ch.stop_typing("+1234567890").await.unwrap_err().message;
    for err in [send_error, start_error, stop_error] {
        assert!(err.to_string().contains("not connected"), "{err}");
    }
}

#[tokio::test]
async fn send_delivers_text_to_an_allowed_number_as_a_phone_jid() {
    let (ch, fake) = connected_channel(false);
    crate::provider_test_support::send_v1(&ch, &message("+1234567890", "hello"))
        .await
        .unwrap();
    assert_eq!(
        fake.texts(),
        [("1234567890@s.whatsapp.net".to_owned(), "hello".to_owned())]
    );
}

#[tokio::test]
async fn send_into_a_provenanced_group_uses_the_group_jid() {
    let (ch, fake) = connected_channel(false);
    ch.allowed_groups.lock().insert("987654321@g.us".into());
    crate::provider_test_support::send_v1(&ch, &message("987654321@g.us", "to group"))
        .await
        .unwrap();
    assert_eq!(
        fake.texts(),
        [("987654321@g.us".to_owned(), "to group".to_owned())]
    );
}

#[tokio::test]
async fn blocked_recipients_are_rejected_before_provider_io() {
    let (ch, fake) = connected_channel(false);
    let dm = crate::provider_test_support::send_v1(&ch, &message("+9999999999", "nope"))
        .await
        .unwrap_err();
    let group = crate::provider_test_support::send_v1(&ch, &message("11111@g.us", "nope"))
        .await
        .unwrap_err();
    let dm = dm
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .unwrap()
        .0
        .clone();
    let group = group
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .unwrap()
        .0
        .clone();
    for error in [dm, group] {
        assert_eq!(error.kind, crate::channel::SendErrorKind::Forbidden);
        assert!(!error.sent_before_error);
    }
    ch.start_typing("+9999999999").await.unwrap();
    ch.stop_typing("+9999999999").await.unwrap();
    assert!(fake.calls().is_empty());
}

#[tokio::test]
async fn unparseable_recipients_are_rejected() {
    let ch = WhatsAppWebChannel::new("/tmp/t.db".into(), None, None, vec![]);
    let fake = Arc::new(RecordingClient::default());
    *ch.client.lock() = Some(fake.clone() as Arc<dyn OutboundClient>);
    ch.connected.store(true, Ordering::Release);

    let empty = crate::provider_test_support::send_v1(&ch, &message("   ", "x"))
        .await
        .unwrap_err();
    let empty = empty
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .unwrap()
        .0
        .clone();
    assert_eq!(
        empty.kind,
        crate::channel::SendErrorKind::BadFormat,
        "invalid recipient must be rejected before provider I/O"
    );
    assert!(!empty.sent_before_error);
    let no_digits = ch.start_typing("+abc").await.unwrap_err();
    assert_eq!(no_digits.kind, crate::channel::SendErrorKind::BadFormat);
    assert!(!no_digits.sent_before_error);
    let bad_jid = ch.stop_typing("@@").await.unwrap_err();
    assert_eq!(bad_jid.kind, crate::channel::SendErrorKind::BadFormat);
    assert!(!bad_jid.sent_before_error);
    assert!(fake.calls().is_empty());
}

#[tokio::test]
async fn typing_states_go_to_the_recipient_jid() {
    let (ch, fake) = connected_channel(false);
    ch.start_typing("+1234567890").await.unwrap();
    ch.stop_typing("1234567890@s.whatsapp.net").await.unwrap();
    assert!(matches!(
        fake.calls().as_slice(),
        [Call::Composing(first), Call::Paused(second)]
            if first == "1234567890@s.whatsapp.net" && second == "1234567890@s.whatsapp.net"
    ));
}

#[tokio::test]
async fn client_failures_surface_with_their_operation() {
    let (ch, _fake) = connected_channel(true);
    let send = crate::provider_test_support::send_v1(&ch, &message("+1234567890", "x"))
        .await
        .unwrap_err();
    let send = send
        .downcast_ref::<crate::provider_test_support::TypedSendTestError>()
        .unwrap()
        .0
        .clone();
    assert_eq!(send.kind, crate::channel::SendErrorKind::Unknown);
    assert!(send.sent_before_error);
    assert_eq!(send.message, "WhatsApp Web send outcome is uncertain");
    let composing = ch.start_typing("+1234567890").await.unwrap_err();
    assert_eq!(
        composing.message,
        "WhatsApp Web operation outcome is uncertain"
    );
    let paused = ch.stop_typing("+1234567890").await.unwrap_err();
    assert_eq!(
        paused.message,
        "WhatsApp Web operation outcome is uncertain"
    );
}

// ---------------------------------------------------------------------------
// The real bot, over offline transport and HTTP doubles
// ---------------------------------------------------------------------------

/// Transport factory that never connects: every dial fails.
struct OfflineTransport;

#[async_trait]
impl whatsapp_rust::transport::TransportFactory for OfflineTransport {
    async fn create_transport(
        &self,
    ) -> Result<
        (
            Arc<dyn whatsapp_rust::transport::Transport>,
            whatsapp_rust::async_channel::Receiver<whatsapp_rust::transport::TransportEvent>,
        ),
        anyhow::Error,
    > {
        anyhow::bail!("offline test transport")
    }
}

/// HTTP client that never reaches the network: every request fails.
struct OfflineHttp;

#[async_trait]
impl whatsapp_rust::http::HttpClient for OfflineHttp {
    async fn execute(
        &self,
        _request: whatsapp_rust::http::HttpRequest,
    ) -> Result<whatsapp_rust::http::HttpResponse> {
        anyhow::bail!("offline test http client")
    }
}

/// Lifecycle registry that keeps registered hooks so the test can fire them.
#[derive(Default)]
struct RecordingLifecycle {
    hooks: Mutex<Vec<(String, crate::host::ShutdownHook)>>,
}

impl crate::host::LifecycleRegistry for RecordingLifecycle {
    fn register_shutdown(&self, name: &str, hook: crate::host::ShutdownHook) {
        self.hooks.lock().push((name.to_string(), hook));
    }
}

/// Wait until `listen_with` has built the bot and published its client.
async fn wait_for_client(ch: &WhatsAppWebChannel) {
    tokio::time::timeout(HANG_GUARD, async {
        while ch.client.lock().is_none() || ch.bot_handle.lock().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bot client was never published");
}

#[cfg(feature = "whatsapp-web")]
struct TypedRecordingSink {
    ingress: tokio::sync::Mutex<Vec<tinychannels_bus::ChannelIngress>>,
}

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::channel::ChannelInboundSink for TypedRecordingSink {
    async fn push(
        &self,
        envelope: tinychannels_bus::ChannelInboundEnvelope,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.push_ingress(tinychannels_bus::ChannelIngress::Inbound(envelope))
            .await
    }

    async fn push_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.ingress.lock().await.push(ingress);
        Ok(())
    }
}

#[tokio::test]
#[cfg(feature = "whatsapp-web")]
async fn typed_adapter_start_can_stop_and_restart_without_retaining_a_legacy_listener() {
    let channel = Arc::new(make_channel());
    let sink = Arc::new(TypedRecordingSink {
        ingress: tokio::sync::Mutex::new(Vec::new()),
    });

    for iteration in 0..2 {
        let adapter = Arc::clone(&channel);
        let listener_sink = Arc::clone(&sink);
        let listener = tokio::spawn(async move {
            adapter
                .start_typed_with(listener_sink.as_ref(), OfflineTransport, OfflineHttp)
                .await
        });

        wait_for_client(&channel).await;
        assert!(channel.bot_handle.lock().is_some());
        assert!(channel.tx.lock().is_some());
        let second_start = channel
            .start_typed_with(sink.as_ref(), OfflineTransport, OfflineHttp)
            .await;
        assert!(
            second_start.is_err(),
            "only one typed listener may own the bot"
        );
        if iteration == 0 {
            listener.abort();
            assert!(listener.await.unwrap_err().is_cancelled());
        } else {
            tokio::time::timeout(HANG_GUARD, channel.stop())
                .await
                .expect("typed stop must cancel the active bot")
                .unwrap();
            tokio::time::timeout(HANG_GUARD, listener)
                .await
                .expect("typed listener must return after stop")
                .expect("typed listener task panicked")
                .unwrap();
        }
        tokio::time::timeout(HANG_GUARD, async {
            while channel.typed_listener_active.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("typed listener ownership must be released before restart");
        assert!(channel.bot_handle.lock().is_none());
        assert!(channel.client.lock().is_none());
        assert!(channel.tx.lock().is_none());
        assert!(!channel.typed_listener_active.load(Ordering::Acquire));
    }

    assert!(sink.ingress.lock().await.is_empty());
}

async fn run_listen_until_shutdown(ch: WhatsAppWebChannel, lifecycle: Arc<RecordingLifecycle>) {
    let ch = Arc::new(ch);
    let (tx, _rx) = mpsc::channel(4);
    let listener = {
        let ch = Arc::clone(&ch);
        tokio::spawn(async move { ch.listen_with(tx, OfflineTransport, OfflineHttp).await })
    };

    wait_for_client(&ch).await;
    assert!(ch.tx.lock().is_some());
    // The hook is registered before `listen` parks.
    let (name, hook) = tokio::time::timeout(HANG_GUARD, async {
        loop {
            if let Some(entry) = lifecycle.hooks.lock().pop() {
                return entry;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown hook was never registered");
    assert_eq!(name, "whatsapp_web");
    assert!(!listener.is_finished());

    ch.connected.store(true, Ordering::Release);
    hook().await;

    let result = tokio::time::timeout(HANG_GUARD, listener)
        .await
        .expect("listen did not return after shutdown")
        .expect("listen task panicked");
    assert!(result.is_ok());
    assert!(!ch.connected.load(Ordering::Acquire));
    assert!(ch.client.lock().is_none());
    assert!(ch.bot_handle.lock().is_none());
}

#[tokio::test]
async fn listen_runs_the_bot_until_the_host_shutdown_hook_fires() {
    let lifecycle = Arc::new(RecordingLifecycle::default());
    let ch = make_channel().with_lifecycle(lifecycle.clone());
    run_listen_until_shutdown(ch, lifecycle).await;
}

#[tokio::test]
async fn listen_accepts_pair_code_configuration() {
    for (phone, code) in [
        (
            Some("15551234567".to_string()),
            Some("ABCDEFGH".to_string()),
        ),
        (None, Some("ABCDEFGH".to_string())),
    ] {
        let lifecycle = Arc::new(RecordingLifecycle::default());
        let ch = WhatsAppWebChannel::new("/tmp/t.db".into(), phone, code, vec![])
            .with_lifecycle(lifecycle.clone());
        run_listen_until_shutdown(ch, lifecycle).await;
    }
}

#[tokio::test]
async fn listen_without_a_lifecycle_registry_parks() {
    let ch = Arc::new(make_channel());
    let (tx, _rx) = mpsc::channel(4);
    let listener = {
        let ch = Arc::clone(&ch);
        tokio::spawn(async move { ch.listen_with(tx, OfflineTransport, OfflineHttp).await })
    };
    wait_for_client(&ch).await;
    // Nothing can wake the park: no hook was registered anywhere.
    assert!(!listener.is_finished());
    listener.abort();
    assert!(listener.await.unwrap_err().is_cancelled());
    if let Some(handle) = ch.bot_handle.lock().take() {
        handle.abort();
    }
}

#[tokio::test]
async fn the_real_client_refuses_outbound_while_logged_out() {
    let lifecycle = Arc::new(RecordingLifecycle::default());
    let ch = Arc::new(make_channel().with_lifecycle(lifecycle.clone()));
    let (tx, _rx) = mpsc::channel(4);
    let listener = {
        let ch = Arc::clone(&ch);
        tokio::spawn(async move { ch.listen_with(tx, OfflineTransport, OfflineHttp).await })
    };
    wait_for_client(&ch).await;

    let send = tokio::time::timeout(
        HANG_GUARD,
        crate::provider_test_support::send_v1(ch.as_ref(), &message("+1234567890", "hi")),
    )
    .await
    .expect("send hung");
    assert!(send.is_err());
    let composing = tokio::time::timeout(HANG_GUARD, ch.start_typing("+1234567890"))
        .await
        .expect("start_typing hung")
        .unwrap_err();
    assert_eq!(composing.kind, crate::channel::SendErrorKind::Unknown);
    assert!(composing.sent_before_error);
    let paused = tokio::time::timeout(HANG_GUARD, ch.stop_typing("+1234567890"))
        .await
        .expect("stop_typing hung")
        .unwrap_err();
    assert_eq!(paused.kind, crate::channel::SendErrorKind::Unknown);
    assert!(paused.sent_before_error);

    let (_, hook) = lifecycle.hooks.lock().pop().expect("hook registered");
    hook().await;
    tokio::time::timeout(HANG_GUARD, listener)
        .await
        .expect("listen did not return")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_websocket_transport_dials_the_configured_url() {
    use whatsapp_rust::transport::TransportFactory;
    let err = match WhatsAppWebChannel::websocket_transport(Some("not a url".into()))
        .create_transport()
        .await
    {
        Ok(_) => panic!("an unparseable override must not dial"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("Failed to parse URL"), "{err}");
}

#[test]
fn a_sender_user_already_in_e164_is_not_double_prefixed() {
    let phone = Jid::pn("+15550001111");
    let (sender, allow) = WhatsAppWebChannel::sender_identity(&phone, None);
    assert_eq!(sender, "+15550001111");
    assert_eq!(allow, vec!["+15550001111".to_string()]);
}

#[cfg(feature = "whatsapp-web")]
struct FixedStorage(Result<Arc<dyn tinystoragedrivers_api::DocumentStore>, &'static str>);

#[cfg(feature = "whatsapp-web")]
#[async_trait]
impl crate::host::StorageProvider for FixedStorage {
    async fn documents(&self) -> anyhow::Result<Arc<dyn tinystoragedrivers_api::DocumentStore>> {
        self.0.clone().map_err(|message| anyhow::anyhow!(message))
    }
}

#[cfg(feature = "whatsapp-web")]
fn memory_docs() -> Arc<dyn tinystoragedrivers_api::DocumentStore> {
    use tinystoragedrivers_api::StorageBackend;
    let backend = tinystoragedrivers_testkit::MemoryBackend::new();
    let storage = backend
        .for_scope(&tinystoragedrivers_types::Scope::new("tenant:whatsapp-tests").unwrap())
        .unwrap();
    Arc::clone(storage.documents())
}

#[cfg(feature = "whatsapp-web")]
#[tokio::test]
async fn host_storage_makes_the_session_durable_across_channel_restarts() {
    let docs = memory_docs();
    let channel = || make_channel().with_storage(Arc::new(FixedStorage(Ok(Arc::clone(&docs)))));

    let backend = channel().session_backend().await;
    wacore::store::traits::DeviceStore::create(&*backend)
        .await
        .unwrap();
    let before = serde_json::to_value(
        wacore::store::traits::DeviceStore::load(&*backend)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    drop(backend);

    let restarted = channel().session_backend().await;
    let after = serde_json::to_value(
        wacore::store::traits::DeviceStore::load(&*restarted)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        before, after,
        "the same linked device resumes after a restart"
    );
}

#[cfg(feature = "whatsapp-web")]
#[tokio::test]
async fn without_usable_host_storage_the_session_falls_back_to_memory() {
    for channel in [
        make_channel(),
        make_channel().with_storage(Arc::new(FixedStorage(Err("no storage object")))),
    ] {
        let backend = channel.session_backend().await;
        wacore::store::traits::DeviceStore::create(&*backend)
            .await
            .unwrap();
        assert!(
            wacore::store::traits::DeviceStore::exists(&*backend)
                .await
                .unwrap()
        );
        // A second backend from the same channel shares nothing: no persistence.
        let other = channel.session_backend().await;
        assert!(
            !wacore::store::traits::DeviceStore::exists(&*other)
                .await
                .unwrap()
        );
    }
}

// ---------------------------------------------------------------------------
// v2 channel behaviour (linked-session double)
// ---------------------------------------------------------------------------

mod v2 {
    use super::*;
    use crate::providers::whatsapp_shared::fakes::{CollectingSink, MemoryMedia, MemoryVoices};
    use crate::providers::whatsapp_web::fakes::{Call, RecordingClient, base_state};
    use tinychannels_bus::channel::{ChannelRef, ConversationRef};
    use wacore::types::events::Receipt;
    use wacore::types::presence::ReceiptType;

    fn linked() -> (WhatsAppWebChannel, Arc<RecordingClient>) {
        let channel = WhatsAppWebChannel::new(
            "/tmp/test.db".into(),
            None,
            None,
            vec!["+15550001111".into()],
        );
        let client = Arc::new(RecordingClient::linked("15559998888@s.whatsapp.net"));
        *channel.client.lock() = Some(client.clone() as Arc<dyn OutboundClient>);
        (channel, client)
    }

    fn conversation() -> ConversationRef {
        ConversationRef {
            id: "+15550001111".into(),
            ..Default::default()
        }
    }

    fn whatsapp() -> ChannelRef {
        ChannelRef {
            id: "whatsapp".into(),
            account_id: None,
        }
    }

    fn typed(body: &str) -> tinychannels_bus::ChannelOutboundMessage {
        tinychannels_bus::ChannelOutboundMessage::from_legacy_message(
            "whatsapp",
            &SendMessage::new(body, "+15550001111"),
        )
    }

    #[test]
    fn capabilities_follow_the_wired_ports_and_the_protocol() {
        let (bare, _) = linked();
        let facts = bare.declared_capabilities();
        assert!(facts.reactions && facts.addressing.reply_to_id && !facts.addressing.thread_id);
        assert!(facts.actions.supported.iter().any(|a| a == "edit"));
        assert!(!facts.content.image.outbound && !facts.voice_out.supported);
        assert!(crate::provider_test_support::supports_reactions(&bare));
        assert!(crate::provider_test_support::supports_edit(&bare));
        assert!(crate::provider_test_support::supports_reply_to_id(&bare));
        assert!(crate::provider_test_support::history_key_ignores_thread(
            &bare
        ));
        let wired = WhatsAppWebChannel::new("/tmp/t.db".into(), None, None, vec![])
            .with_media_repository(Arc::new(MemoryMedia::default()))
            .with_voice_media_store(Arc::new(MemoryVoices::default()))
            .with_voice_synthesis(true);
        let facts = wired.declared_capabilities();
        assert!(facts.content.sticker.outbound && facts.voice_out.native_waveform);
        assert!(facts.voice_out.synthesis);
        assert_eq!(wired.declared_capabilities(), wired.declared_capabilities());
        let descriptor = wired.descriptor();
        assert_eq!(
            descriptor.metadata.get("backend").map(String::as_str),
            Some("web")
        );
    }

    #[tokio::test]
    async fn typed_sends_edits_deletes_reactions_and_read_receipts_run_on_the_session() {
        let (channel, client) = linked();
        crate::channel::ChannelTypedSend::send_typed(&channel, &typed("one"))
            .await
            .unwrap();
        let outcome = crate::channel::ChannelTypedSend::send_typed(&channel, &typed("two"))
            .await
            .unwrap();
        let receipt = outcome.receipt;
        let edited = crate::channel::ChannelEdit::edit(
            &channel,
            &receipt,
            tinychannels_bus::ChannelOutboundIntent {
                conversation_id: "+15550001111".into(),
                channel_id: "whatsapp".into(),
                payload: tinychannels_bus::OutboundPayload::Text {
                    text: "two!".into(),
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(edited.platform_message_ids, receipt.platform_message_ids);
        crate::channel::ChannelDelete::delete(&channel, &receipt)
            .await
            .unwrap();
        let thumbs = tinychannels_bus::ChannelReaction {
            value: "👍".into(),
            custom_emoji_id: None,
        };
        crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &ChannelRef {
                id: "whatsapp".into(),
                account_id: None,
            },
            &conversation(),
            "3EB0X",
            &thumbs,
            false,
        )
        .await
        .unwrap();
        crate::channel::ChannelAdapter::mark_read(&channel, &conversation(), "3EB0X")
            .await
            .unwrap();
        let kinds: Vec<_> = client
            .calls()
            .iter()
            .map(|call| match call {
                Call::Send { .. } => "send",
                Call::Edit { .. } => "edit",
                Call::Revoke { .. } => "revoke",
                Call::Reaction { .. } => "reaction",
                Call::Read { .. } => "read",
                _ => "other",
            })
            .collect();
        assert_eq!(
            kinds,
            ["send", "send", "edit", "revoke", "reaction", "read"]
        );
    }

    #[tokio::test]
    async fn adapter_ports_map_failures_to_structured_errors() {
        let (channel, _client) = linked();
        let thumbs = tinychannels_bus::ChannelReaction {
            value: "👍".into(),
            custom_emoji_id: None,
        };
        crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &whatsapp(),
            &conversation(),
            "3EB0X",
            &thumbs,
            false,
        )
        .await
        .unwrap();
        let wrong = ChannelRef {
            id: "telegram".into(),
            account_id: None,
        };
        let refused = crate::channel::LegacyChannelReaction::send_reaction(
            &channel,
            &wrong,
            &conversation(),
            "3EB0X",
            &thumbs,
            false,
        )
        .await
        .unwrap_err();
        assert!(!refused.unsupported && !refused.sent_before_error);
        crate::channel::ChannelTyping::start_typing(&channel, "+15550001111")
            .await
            .unwrap();
        crate::channel::ChannelTyping::stop_typing(&channel, "+15550001111")
            .await
            .unwrap();
        // Typing toward a number the account does not admit is silently skipped.
        crate::channel::ChannelTyping::start_typing(&channel, "+19998887777")
            .await
            .unwrap();

        let sent = crate::channel::ChannelTypedSend::send_typed(&channel, &typed("hi"))
            .await
            .unwrap();
        let intent = tinychannels_bus::ChannelOutboundIntent {
            conversation_id: "+15550001111".into(),
            channel_id: "whatsapp".into(),
            payload: tinychannels_bus::OutboundPayload::Text {
                text: "edited".into(),
            },
            ..Default::default()
        };
        crate::channel::ChannelEdit::edit(&channel, &sent.receipt, intent.clone())
            .await
            .unwrap();
        crate::channel::ChannelDelete::delete(&channel, &sent.receipt)
            .await
            .unwrap();
        let mut orphan = sent.receipt.clone();
        orphan.conversation = None;
        assert!(
            crate::channel::ChannelDelete::delete(&channel, &orphan)
                .await
                .is_err()
        );
        assert!(
            crate::channel::ChannelStreamingDraft::upsert_draft(&channel, None, intent)
                .await
                .unwrap_err()
                .unsupported
        );
        assert!(channel.stop().await.is_ok());
    }

    #[tokio::test]
    async fn status_reports_the_liveness_flag() {
        let (channel, _client) = linked();
        let down = channel.status().await.unwrap();
        assert_eq!(down.health_state.as_deref(), Some("unhealthy"));
        channel.connected.store(true, Ordering::Release);
        assert_eq!(
            channel.status().await.unwrap().health_state.as_deref(),
            Some("healthy")
        );
    }

    #[test]
    fn failures_that_are_not_ours_are_uncertain() {
        assert!(matches!(
            failure_of(&anyhow!("socket reset")),
            WaFailure::Uncertain(_)
        ));
        assert!(matches!(
            failure_of(&anyhow::Error::new(WaFailure::rejected("no"))),
            WaFailure::Rejected(_)
        ));
    }

    // --- event handling ------------------------------------------------------

    fn state_with_session() -> (
        InboundState,
        Arc<RecordingClient>,
        Arc<CollectingSink>,
        mpsc::Receiver<ChannelMessage>,
    ) {
        let (tx, rx) = mpsc::channel(8);
        let client = Arc::new(RecordingClient::linked("15559998888@s.whatsapp.net"));
        let sink = Arc::new(CollectingSink::new());
        let mut state = base_state(tx, &["*"]);
        state.client = Arc::new(Mutex::new(Some(client.clone() as Arc<dyn OutboundClient>)));
        state.events = Some(sink.clone());
        (state, client, sink, rx)
    }

    fn batch_of(id: &str, chat: &str, sender: &str, body: &str) -> Event {
        let info = MessageInfo {
            source: MessageSource {
                chat: chat.parse().unwrap(),
                sender: sender.parse().unwrap(),
                ..Default::default()
            },
            id: id.to_owned(),
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            ..Default::default()
        };
        let message = InboundMessage::builder()
            .message(Arc::new(wa::Message {
                conversation: Some(body.into()),
                ..Default::default()
            }))
            .info(Arc::new(info))
            .build();
        Event::Messages(
            MessageBatch::builder()
                .messages(vec![message].into())
                .origin(BatchOrigin::Live)
                .build(),
        )
    }

    #[tokio::test]
    async fn connecting_binds_the_linked_account_and_typed_ingress_records_authors() {
        let (state, _client, sink, mut rx) = state_with_session();
        state
            .handle_event(&Event::Connected(Connected::builder().build()))
            .await;
        assert_eq!(
            state.identity.current().as_deref(),
            Some("15559998888@s.whatsapp.net")
        );
        state
            .handle_event(&batch_of(
                "3EB0ONE",
                "987@g.us",
                "15550001111@s.whatsapp.net",
                "hi group",
            ))
            .await;
        assert_eq!(
            state.authors.lock().get("3EB0ONE").as_deref(),
            Some("15550001111@s.whatsapp.net")
        );
        assert_eq!(
            sink.ingress.lock().len(),
            1,
            "published through the typed path"
        );
        assert!(
            rx.try_recv().is_err(),
            "and not duplicated onto the legacy channel"
        );
    }

    #[tokio::test]
    async fn a_session_linked_to_another_account_drops_inbound_traffic() {
        let (state, client, sink, _rx) = state_with_session();
        state
            .handle_event(&Event::Connected(Connected::builder().build()))
            .await;
        *client.account.lock() = Some("15557776666@s.whatsapp.net".parse().unwrap());
        state
            .handle_event(&Event::Connected(Connected::builder().build()))
            .await;
        assert!(state.identity.is_conflicted());
        state
            .handle_event(&batch_of(
                "3EB0TWO",
                "15550001111@s.whatsapp.net",
                "15550001111@s.whatsapp.net",
                "hello",
            ))
            .await;
        assert!(sink.ingress.lock().is_empty());
    }

    #[tokio::test]
    async fn a_typed_delivery_failure_is_logged_and_dropped_and_declined_ingress_uses_the_legacy_path()
     {
        let (mut state, _client, _sink, mut rx) = state_with_session();
        state.events = Some(Arc::new(CollectingSink {
            fail: true,
            ..CollectingSink::new()
        }));
        state
            .handle_event(&batch_of(
                "3EB0THREE",
                "15550001111@s.whatsapp.net",
                "15550001111@s.whatsapp.net",
                "x",
            ))
            .await;
        assert!(rx.try_recv().is_err());

        let (mut declining, _client, _sink, mut rx) = state_with_session();
        declining.events = Some(Arc::new(CollectingSink {
            accept: false,
            ..CollectingSink::new()
        }));
        declining
            .handle_event(&batch_of(
                "3EB0FOUR",
                "15550001111@s.whatsapp.net",
                "15550001111@s.whatsapp.net",
                "legacy",
            ))
            .await;
        assert_eq!(rx.try_recv().unwrap().content, "legacy");
    }

    #[tokio::test]
    async fn receipt_events_are_published_for_our_messages() {
        let (state, _client, sink, _rx) = state_with_session();
        let receipt = Receipt::builder()
            .source(MessageSource {
                chat: "15550001111@s.whatsapp.net".parse().unwrap(),
                sender: "15550001111@s.whatsapp.net".parse().unwrap(),
                ..Default::default()
            })
            .message_ids(vec!["3EB0OUT".to_owned()])
            .timestamp(chrono::DateTime::from_timestamp(1_700_000_001, 0).unwrap())
            .r#type(ReceiptType::Delivered)
            .offline(false)
            .build();
        state.handle_event(&Event::Receipt(receipt)).await;
        assert_eq!(sink.ingress.lock().len(), 1);
    }

    #[tokio::test]
    async fn the_real_client_implements_every_session_operation_and_refuses_them_while_logged_out()
    {
        use crate::providers::whatsapp_web::media_build::MediaBuild;
        let lifecycle = Arc::new(RecordingLifecycle::default());
        let ch = Arc::new(make_channel().with_lifecycle(lifecycle.clone()));
        let (tx, _rx) = mpsc::channel(4);
        let listener = {
            let ch = Arc::clone(&ch);
            tokio::spawn(async move { ch.listen_with(tx, OfflineTransport, OfflineHttp).await })
        };
        wait_for_client(&ch).await;
        let client = ch.client.lock().clone().expect("the real client");
        let jid: Jid = "15551230000@s.whatsapp.net".parse().unwrap();
        assert!(
            client.linked_account().is_none(),
            "no account is linked yet"
        );
        let text = wa::Message {
            conversation: Some("x".into()),
            ..Default::default()
        };
        let guard = |future: std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>| async {
            tokio::time::timeout(HANG_GUARD, future)
                .await
                .expect("operation hung")
        };
        assert!(
            guard(Box::pin({
                let (client, jid, text) = (client.clone(), jid.clone(), text.clone());
                async move { client.send_message(jid, text).await.is_err() }
            }))
            .await
        );
        for build in [
            MediaBuild::Image {
                caption: None,
                mimetype: "image/png".into(),
            },
            MediaBuild::Video {
                caption: None,
                duration_seconds: None,
            },
            MediaBuild::Audio {
                mimetype: "audio/mpeg".into(),
                duration_seconds: None,
            },
            MediaBuild::Voice {
                duration_seconds: None,
                waveform: None,
            },
            MediaBuild::Document {
                mimetype: "application/pdf".into(),
                file_name: None,
                caption: None,
            },
            MediaBuild::Sticker,
        ] {
            let client = client.clone();
            assert!(
                guard(Box::pin(async move {
                    client
                        .media_message(vec![1, 2, 3], build, None)
                        .await
                        .is_err()
                }))
                .await
            );
        }
        let image = wa::message::ImageMessage {
            direct_path: Some("/v/x".into()),
            media_key: Some(vec![1; 32]),
            ..Default::default()
        };
        assert!(
            guard(Box::pin({
                let client = client.clone();
                async move { client.download(&image).await.is_err() }
            }))
            .await
        );
        assert!(
            guard(Box::pin({
                let (client, jid) = (client.clone(), jid.clone());
                async move {
                    client
                        .send_reaction(jid, wa::MessageKey::default(), "👍")
                        .await
                        .is_err()
                }
            }))
            .await
        );
        assert!(
            guard(Box::pin({
                let (client, jid, text) = (client.clone(), jid.clone(), text.clone());
                async move {
                    client
                        .edit_message(jid, "3EB0X".into(), text)
                        .await
                        .is_err()
                }
            }))
            .await
        );
        assert!(
            guard(Box::pin({
                let (client, jid) = (client.clone(), jid.clone());
                async move { client.revoke_message(jid, "3EB0X".into()).await.is_err() }
            }))
            .await
        );
        assert!(
            guard(Box::pin({
                let (client, jid) = (client.clone(), jid.clone());
                async move {
                    client
                        .mark_read(&jid, None, &["3EB0X".into()])
                        .await
                        .is_err()
                }
            }))
            .await
        );

        let (_, hook) = lifecycle.hooks.lock().pop().expect("hook registered");
        hook().await;
        tokio::time::timeout(HANG_GUARD, listener)
            .await
            .expect("listen did not return")
            .unwrap()
            .unwrap();
    }
}
