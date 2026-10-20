use super::*;

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
    assert_eq!(ch.name(), "whatsapp");
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
    assert!(!ch.health_check().await);
}

#[tokio::test]
#[cfg(feature = "whatsapp-web")]
async fn whatsapp_web_health_check_tracks_connected_flag() {
    let ch = make_channel();
    assert!(!ch.health_check().await);
    ch.connected.store(true, Ordering::Release);
    assert!(ch.health_check().await);
    ch.connected.store(false, Ordering::Release);
    assert!(!ch.health_check().await);
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
    let state = InboundState {
        tx,
        allowed_numbers: allowed.iter().map(|s| s.to_string()).collect(),
        connected: Arc::new(AtomicBool::new(false)),
        allowed_groups: Arc::new(Mutex::new(HashSet::new())),
    };
    (state, rx)
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

#[derive(Debug, Clone, PartialEq)]
enum Call {
    Text(String, String),
    Composing(String),
    Paused(String),
}

#[derive(Default)]
struct FakeClient {
    calls: Mutex<Vec<Call>>,
    fail: bool,
}

#[async_trait]
impl OutboundClient for FakeClient {
    async fn send_text(&self, to: Jid, text: String) -> Result<()> {
        if self.fail {
            anyhow::bail!("send boom");
        }
        self.calls.lock().push(Call::Text(to.to_string(), text));
        Ok(())
    }

    async fn send_composing(&self, to: &Jid) -> Result<()> {
        if self.fail {
            anyhow::bail!("composing boom");
        }
        self.calls.lock().push(Call::Composing(to.to_string()));
        Ok(())
    }

    async fn send_paused(&self, to: &Jid) -> Result<()> {
        if self.fail {
            anyhow::bail!("paused boom");
        }
        self.calls.lock().push(Call::Paused(to.to_string()));
        Ok(())
    }
}

fn connected_channel(fail: bool) -> (WhatsAppWebChannel, Arc<FakeClient>) {
    let ch = make_channel(); // allowed_numbers = ["+1234567890"]
    let fake = Arc::new(FakeClient {
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
    for err in [
        ch.send(&message("+1234567890", "hi")).await.unwrap_err(),
        ch.start_typing("+1234567890").await.unwrap_err(),
        ch.stop_typing("+1234567890").await.unwrap_err(),
    ] {
        assert!(err.to_string().contains("client not connected"), "{err}");
    }
}

#[tokio::test]
async fn send_delivers_text_to_an_allowed_number_as_a_phone_jid() {
    let (ch, fake) = connected_channel(false);
    ch.send(&message("+1234567890", "hello")).await.unwrap();
    assert_eq!(
        *fake.calls.lock(),
        [Call::Text(
            "1234567890@s.whatsapp.net".into(),
            "hello".into()
        )]
    );
}

#[tokio::test]
async fn send_into_a_provenanced_group_uses_the_group_jid() {
    let (ch, fake) = connected_channel(false);
    ch.allowed_groups.lock().insert("987654321@g.us".into());
    ch.send(&message("987654321@g.us", "to group"))
        .await
        .unwrap();
    assert_eq!(
        *fake.calls.lock(),
        [Call::Text("987654321@g.us".into(), "to group".into())]
    );
}

#[tokio::test]
async fn blocked_recipients_are_skipped_silently() {
    let (ch, fake) = connected_channel(false);
    ch.send(&message("+9999999999", "nope")).await.unwrap();
    ch.send(&message("11111@g.us", "nope")).await.unwrap();
    ch.start_typing("+9999999999").await.unwrap();
    ch.stop_typing("+9999999999").await.unwrap();
    assert!(fake.calls.lock().is_empty());
}

#[tokio::test]
async fn unparseable_recipients_are_rejected() {
    let ch = WhatsAppWebChannel::new("/tmp/t.db".into(), None, None, vec![]);
    let fake = Arc::new(FakeClient::default());
    *ch.client.lock() = Some(fake.clone() as Arc<dyn OutboundClient>);

    let empty = ch.send(&message("   ", "x")).await.unwrap_err();
    assert!(empty.to_string().contains("cannot be empty"), "{empty}");
    let no_digits = ch.start_typing("+abc").await.unwrap_err();
    assert!(
        no_digits.to_string().contains("valid phone number"),
        "{no_digits}"
    );
    let bad_jid = ch.stop_typing("@@").await.unwrap_err();
    assert!(
        bad_jid.to_string().contains("Invalid WhatsApp JID"),
        "{bad_jid}"
    );
    assert!(fake.calls.lock().is_empty());
}

#[tokio::test]
async fn typing_states_go_to_the_recipient_jid() {
    let (ch, fake) = connected_channel(false);
    ch.start_typing("+1234567890").await.unwrap();
    ch.stop_typing("1234567890@s.whatsapp.net").await.unwrap();
    assert_eq!(
        *fake.calls.lock(),
        [
            Call::Composing("1234567890@s.whatsapp.net".into()),
            Call::Paused("1234567890@s.whatsapp.net".into()),
        ]
    );
}

#[tokio::test]
async fn client_failures_surface_with_their_operation() {
    let (ch, _fake) = connected_channel(true);
    let send = ch.send(&message("+1234567890", "x")).await.unwrap_err();
    assert_eq!(send.to_string(), "send boom");
    let composing = ch.start_typing("+1234567890").await.unwrap_err();
    assert_eq!(
        composing.to_string(),
        "Failed to send typing state (composing): composing boom"
    );
    let paused = ch.stop_typing("+1234567890").await.unwrap_err();
    assert_eq!(
        paused.to_string(),
        "Failed to send typing state (paused): paused boom"
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

    let send = tokio::time::timeout(HANG_GUARD, ch.send(&message("+1234567890", "hi")))
        .await
        .expect("send hung");
    assert!(send.is_err());
    let composing = tokio::time::timeout(HANG_GUARD, ch.start_typing("+1234567890"))
        .await
        .expect("start_typing hung")
        .unwrap_err();
    assert!(
        composing
            .to_string()
            .starts_with("Failed to send typing state (composing): "),
        "{composing}"
    );
    let paused = tokio::time::timeout(HANG_GUARD, ch.stop_typing("+1234567890"))
        .await
        .expect("stop_typing hung")
        .unwrap_err();
    assert!(
        paused
            .to_string()
            .starts_with("Failed to send typing state (paused): "),
        "{paused}"
    );

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
