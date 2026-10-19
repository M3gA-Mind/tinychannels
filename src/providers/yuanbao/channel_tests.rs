use crate::traits::Channel;
use std::sync::Arc;

use super::*;

fn good_cfg() -> YuanbaoConfig {
    let mut c = YuanbaoConfig::default();
    c.app_key = "ak".into();
    c.ws_domain = "wss://example".into();
    c.token = "tok".into();
    c
}

#[test]
fn channel_construction_validates() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    assert_eq!(ch.name(), "yuanbao");
}

#[test]
fn host_voice_store_is_retained_for_native_inbound_services() {
    let store = Arc::new(crate::providers::native::fixtures::StubVoice::default());
    let channel = YuanbaoChannel::new(good_cfg())
        .unwrap()
        .with_voice_media_store(store);

    assert!(
        crate::providers::native::NativeProvider::voice_media_store(&channel).is_some(),
        "the channel retains the host's scoped voice store"
    );
}

#[tokio::test]
async fn downloadable_sound_is_stored_and_published_as_typed_voice() {
    use crate::providers::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::{RecordingEvents, StubVoice};
    use crate::providers::yuanbao::inbound::PipelineCtx;
    use crate::providers::yuanbao::types::{
        InboundMessage, MessageKind, MsgBodyElement, MsgContent, Source,
    };
    use base64::Engine as _;
    use tinychannels_bus::{ChannelIngress, ContentPart};
    let server = HttpsServer::start_with_bytes(
        |request| {
            assert_eq!(request.method, "GET");
            assert_eq!(request.target, "/voice.ogg");
            (
                200,
                include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg").to_vec(),
            )
        },
        "audio/ogg",
    )
    .await;
    let voice = Arc::new(StubVoice::default());
    let events = Arc::new(RecordingEvents::delivering());
    let channel = YuanbaoChannel::new(good_cfg())
        .unwrap()
        .with_voice_media_store(voice.clone())
        .with_events(events.clone())
        .with_voice_http_client(server.client.clone());
    let ctx = PipelineCtx {
        msg: InboundMessage {
            msg_id: "voice-message-9".into(),
            msg_time: 1_700_000_000,
            from_account: "alice".into(),
            sender_nickname: "Alice".into(),
            msg_body: vec![MsgBodyElement {
                msg_type: "TIMSoundElem".into(),
                msg_content: MsgContent {
                    sound: Some("sound-id".into()),
                    url: Some("https://api.dingtalk.com/voice.ogg".into()),
                    file_name: Some("note.ogg".into()),
                    ..Default::default()
                },
            }],
            ..Default::default()
        },
        source: Source {
            from_account: "alice".into(),
            sender_nickname: "Alice".into(),
            ..Default::default()
        },
        text: String::new(),
        image_urls: Vec::new(),
        is_at_bot: false,
        is_owner_command: false,
        kind: MessageKind::Voice,
    };

    assert!(channel.publish_voice(&ctx).await);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    let stored = voice.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].mime_type, "audio/ogg");
    assert_eq!(stored[0].file_name.as_deref(), Some("note.ogg"));
    assert_eq!(stored[0].scope.turn_id, "voice-message-9");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&stored[0].audio_base64)
            .unwrap(),
        include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg")
    );
    drop(stored);
    let ingress = events.inbound();
    assert_eq!(ingress.len(), 1);
    assert_eq!(ingress[0].message_id, "voice-message-9");
    assert!(matches!(
        ingress[0].parts.as_slice(),
        [ContentPart::VoiceNote { .. }]
    ));
    assert!(matches!(
        events.ingress.lock().unwrap().as_slice(),
        [ChannelIngress::Inbound(_)]
    ));
}

#[tokio::test]
async fn failed_voice_ingress_releases_dedupe_for_provider_redelivery() {
    use crate::host::EventSink;
    use crate::providers::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::StubVoice;
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinychannels_bus::ChannelIngress;

    #[derive(Default)]
    struct RetrySink {
        calls: AtomicUsize,
        accepted: std::sync::Mutex<Vec<ChannelIngress>>,
    }

    #[async_trait::async_trait]
    impl EventSink for RetrySink {
        async fn publish(&self, _domain: &str, _kind: &str, _payload: Value) -> anyhow::Result<()> {
            Ok(())
        }

        async fn publish_ingress(&self, ingress: ChannelIngress) -> anyhow::Result<bool> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Ok(false);
            }
            self.accepted.lock().unwrap().push(ingress);
            Ok(true)
        }
    }

    let server = HttpsServer::start_with_bytes(
        |request| {
            assert_eq!(request.method, "GET");
            assert_eq!(request.target, "/voice.ogg");
            (
                200,
                include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg").to_vec(),
            )
        },
        "audio/ogg",
    )
    .await;
    let voice = Arc::new(StubVoice::default());
    let events = Arc::new(RetrySink::default());
    let mut config = good_cfg();
    config.dm_access = "open".into();
    let channel = YuanbaoChannel::new(config)
        .unwrap()
        .with_voice_media_store(voice.clone())
        .with_events(events.clone())
        .with_voice_http_client(server.client.clone());
    let body = json!({
        "from_account":"alice",
        "to_account":"bot",
        "msg_id":"voice-retry-12",
        "msg_time":1700000000,
        "msg_body":[{"msg_type":"TIMSoundElem","msg_content":{
            "sound":"voice-id","url":"https://api.dingtalk.com/voice.ogg","file_name":"note.ogg"
        }}]
    })
    .to_string()
    .into_bytes();
    let (tx, mut legacy_rx) = tokio::sync::mpsc::channel(1);

    for _ in 0..2 {
        channel
            .dispatch_push(
                crate::providers::yuanbao::types::ConnFrame {
                    cmd_type: 2,
                    cmd: "inbound_message".into(),
                    module: String::new(),
                    seq_no: 0,
                    msg_id: String::new(),
                    need_ack: false,
                    status: 0,
                    data: body.clone(),
                },
                &tx,
            )
            .await;
    }

    assert_eq!(events.calls.load(Ordering::SeqCst), 2);
    assert_eq!(events.accepted.lock().unwrap().len(), 1);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    assert_eq!(voice.stored.lock().unwrap().len(), 2);
    assert!(
        legacy_rx.try_recv().is_err(),
        "voice URLs must not fall back to model-visible text"
    );
}

#[test]
fn invalid_config_rejected() {
    let mut c = YuanbaoConfig::default();
    c.app_key = "ak".into();
    // missing ws_domain
    assert!(YuanbaoChannel::new(c).is_err());
}

#[test]
fn split_short_message_returns_one() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    let chunks = ch.split_message("hello");
    assert_eq!(chunks, vec!["hello"]);
}

#[test]
fn split_respects_newlines() {
    let mut c = good_cfg();
    c.max_message_length = 12;
    let ch = YuanbaoChannel::new(c).unwrap();
    let chunks = ch.split_message("line one\nline two\nline three");
    assert!(chunks.len() >= 2);
    // No chunk exceeds the limit.
    for chunk in &chunks {
        assert!(chunk.len() <= 12, "chunk too long: {chunk:?}");
    }
}

#[tokio::test]
async fn resolve_recipient_returns_input_when_no_alias_registered() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    assert_eq!(ch.resolve_recipient("short_uid").await, "short_uid");
}

#[tokio::test]
async fn register_and_resolve_dm_alias_recovers_original_uid() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    let original = "x".repeat(64);
    let shortened = shorten_account_id(&original);
    assert_ne!(shortened, original, "test premise: should actually shorten");
    ch.register_recipient_alias(&shortened, &original).await;
    assert_eq!(ch.resolve_recipient(&shortened).await, original);
}

#[tokio::test]
async fn register_recipient_alias_is_noop_for_equal_pair() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    // Short uid that wouldn't be shortened — caller still hands us
    // (s, s); we should silently skip and not eat a map slot.
    ch.register_recipient_alias("short", "short").await;
    let m = ch.recipient_aliases.lock().await;
    assert!(m.is_empty());
}

#[tokio::test]
async fn resolve_recipient_preserves_group_prefix_via_alias() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    let long_group_code = "g".repeat(64);
    let original = format!("g:{long_group_code}");
    let shortened = shorten_reply_target(&original);
    assert_ne!(shortened, original);
    assert!(shortened.starts_with("g:"));
    ch.register_recipient_alias(&shortened, &original).await;
    assert_eq!(ch.resolve_recipient(&shortened).await, original);
}

#[tokio::test]
async fn alias_map_clears_when_cap_is_hit() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    // Pre-fill up to the cap with distinct entries.
    for i in 0..RECIPIENT_ALIAS_CAP {
        ch.register_recipient_alias(&format!("s{i}"), &format!("o{i}"))
            .await;
    }
    assert_eq!(ch.recipient_aliases.lock().await.len(), RECIPIENT_ALIAS_CAP);
    // One more entry must trigger a clear, then insert the new entry.
    ch.register_recipient_alias("new_short", "new_original")
        .await;
    let m = ch.recipient_aliases.lock().await;
    assert_eq!(m.len(), 1);
    assert_eq!(m.get("new_short").map(String::as_str), Some("new_original"));
}

// ─── trivial trait methods ─────────────────────────────────────

#[test]
fn supports_draft_updates_is_true() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    assert!(ch.supports_draft_updates());
    assert!(Channel::supports_remote_control(&ch));
    assert!(Channel::supports_chat_approvals(&ch));
    assert!(crate::providers::long_tail::LongTail::typing_supported(&ch));
    let capabilities = ch.capabilities();
    assert!(capabilities.streaming_reply_pipeline);
    assert!(!capabilities.progressive_edits);
    assert!(
        !capabilities
            .actions
            .supported
            .iter()
            .any(|name| name == "draft-update")
    );
    assert!(
        !ch.capabilities()
            .actions
            .supported
            .iter()
            .any(|name| name == "edit")
    );
}

#[test]
fn supports_reactions_is_false() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    assert!(!ch.supports_reactions());
}

#[tokio::test]
async fn send_draft_returns_marker_id() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    let msg = SendMessage::new("ignored", "user-42");
    let id = ch.send_draft(&msg).await.unwrap();
    assert_eq!(id.as_deref(), Some("yb-draft:user-42"));
}

#[tokio::test]
async fn update_draft_is_a_noop_ok() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    assert!(ch.update_draft("user-42", "any-id", "text").await.is_ok());
}

#[tokio::test]
async fn health_check_is_false_when_socket_not_connected() {
    // Real connect requires a WebSocket; we only verify the
    // disconnected default here. The connected branch is exercised
    // by `connection::tests::set_state_connected_flips_is_connected_flag`.
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    assert!(!ch.health_check().await);
}

// ─── dispatch_push branches ────────────────────────────────────

fn make_push_frame(cmd: &str, data: Vec<u8>) -> types::ConnFrame {
    types::ConnFrame {
        cmd_type: super::super::proto_constants::cmd_type::PUSH,
        cmd: cmd.into(),
        module: "yuanbao_openclaw_proxy".into(),
        seq_no: 0,
        msg_id: String::new(),
        need_ack: false,
        status: 0,
        data,
    }
}

#[tokio::test]
async fn dispatch_push_empty_body_is_skipped() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    let (tx, mut rx) = mpsc::channel::<ChannelMessage>(4);
    let frame = make_push_frame("noop", Vec::new());
    ch.dispatch_push(frame, &tx).await;
    // No message should reach the sender.
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn dispatch_push_garbage_body_does_not_dispatch() {
    // Body is not a valid protobuf push *and* not valid JSON → Failed.
    // dispatch_push should log + swallow, not propagate panic.
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    let (tx, mut rx) = mpsc::channel::<ChannelMessage>(4);
    let frame = make_push_frame("inbound_message", vec![0xFF, 0xFF, 0xFF, 0xFF]);
    ch.dispatch_push(frame, &tx).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn dispatch_push_dm_text_reaches_listener() {
    // Build a minimal `InboundMessagePush` directly in ConnFrame.data
    // (no PushMsg envelope), with a single TIMTextElem so the pipeline
    // dispatches.
    use super::super::proto::{encode_msg_body_element, encode_varint};
    let elem = types::MsgBodyElement {
        msg_type: "TIMTextElem".into(),
        msg_content: types::MsgContent {
            text: Some("hello".into()),
            ..Default::default()
        },
    };
    let elem_bytes = encode_msg_body_element(&elem);

    // Hand-roll an InboundMessagePush so we don't depend on a helper:
    // field 2 = from_account, field 3 = to_account, field 12 = msg_id,
    // field 13 = repeated MsgBodyElement.
    let mut biz = Vec::new();
    let put_string = |fnum: u32, s: &str, b: &mut Vec<u8>| {
        encode_varint(((fnum as u64) << 3) | 2, b);
        encode_varint(s.len() as u64, b);
        b.extend_from_slice(s.as_bytes());
    };
    put_string(2, "alice", &mut biz);
    put_string(3, "bot1", &mut biz);
    put_string(12, "mid-x", &mut biz);
    encode_varint(((13u64) << 3) | 2, &mut biz);
    encode_varint(elem_bytes.len() as u64, &mut biz);
    biz.extend_from_slice(&elem_bytes);

    // Disable group_at_required and use open dm_access so the
    // pipeline passes all stages for this DM.
    let mut cfg = good_cfg();
    cfg.dm_access = "open".into();
    cfg.bot_id = "bot1".into();
    let ch = YuanbaoChannel::new(cfg).unwrap();

    let frame = make_push_frame("inbound_message", biz);
    let (tx, mut rx) = mpsc::channel::<ChannelMessage>(4);
    ch.dispatch_push(frame, &tx).await;
    let msg = rx.try_recv().expect("dispatch should produce one message");
    assert_eq!(msg.id, "mid-x");
    assert_eq!(msg.content, "hello");
    assert_eq!(msg.channel, "yuanbao");
}

#[tokio::test]
async fn dispatch_push_filtered_by_dedup_does_not_double_dispatch() {
    use super::super::proto::{encode_msg_body_element, encode_varint};
    let elem = types::MsgBodyElement {
        msg_type: "TIMTextElem".into(),
        msg_content: types::MsgContent {
            text: Some("dup".into()),
            ..Default::default()
        },
    };
    let elem_bytes = encode_msg_body_element(&elem);
    let mut biz = Vec::new();
    let put_string = |fnum: u32, s: &str, b: &mut Vec<u8>| {
        encode_varint(((fnum as u64) << 3) | 2, b);
        encode_varint(s.len() as u64, b);
        b.extend_from_slice(s.as_bytes());
    };
    put_string(2, "alice", &mut biz);
    put_string(3, "bot1", &mut biz);
    put_string(12, "dup-id", &mut biz);
    encode_varint(((13u64) << 3) | 2, &mut biz);
    encode_varint(elem_bytes.len() as u64, &mut biz);
    biz.extend_from_slice(&elem_bytes);

    let mut cfg = good_cfg();
    cfg.dm_access = "open".into();
    cfg.bot_id = "bot1".into();
    let ch = YuanbaoChannel::new(cfg).unwrap();
    let (tx, mut rx) = mpsc::channel::<ChannelMessage>(4);
    ch.dispatch_push(make_push_frame("inbound_message", biz.clone()), &tx)
        .await;
    assert!(rx.try_recv().is_ok(), "first should dispatch");
    ch.dispatch_push(make_push_frame("inbound_message", biz), &tx)
        .await;
    assert!(rx.try_recv().is_err(), "second (same id) should dedup");
}

// ─── heartbeat task lifecycle ──────────────────────────────────

#[tokio::test]
async fn start_heartbeat_task_inserts_and_stop_removes() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    ch.start_heartbeat_task("recipient-1").await;
    assert!(
        ch.heartbeat_tasks.lock().await.contains_key("recipient-1"),
        "should have spawned a task for recipient-1"
    );
    // Second start for same recipient is a no-op (does not double-spawn).
    ch.start_heartbeat_task("recipient-1").await;
    assert_eq!(ch.heartbeat_tasks.lock().await.len(), 1);

    ch.stop_heartbeat_task("recipient-1").await;
    assert!(ch.heartbeat_tasks.lock().await.is_empty());
    // Stopping a recipient with no task is also a no-op.
    ch.stop_heartbeat_task("never-started").await;
}

#[tokio::test]
async fn disconnected_sends_and_typing_return_errors_without_leaving_heartbeat_tasks() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    ch.register_recipient_alias("alias", "original").await;
    assert!(ch.send(&SendMessage::new("hello", "alias")).await.is_err());
    assert!(
        ch.finalize_draft("alias", "draft", "hello", Some("thread"))
            .await
            .is_err()
    );
    assert!(ch.start_typing("alias").await.is_err());
    assert!(ch.heartbeat_tasks.lock().await.is_empty());
    ch.start_heartbeat_task("original").await;
    assert!(ch.stop_typing("alias").await.is_err());
    assert!(ch.heartbeat_tasks.lock().await.is_empty());
}

#[tokio::test]
async fn heartbeat_task_exits_when_disconnected_send_fails() {
    let ch = YuanbaoChannel::new(good_cfg()).unwrap();
    ch.start_heartbeat_task("alice").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ch
                .heartbeat_tasks
                .lock()
                .await
                .get("alice")
                .unwrap()
                .is_finished()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    ch.stop_heartbeat_task("alice").await;
    assert!(ch.heartbeat_tasks.lock().await.is_empty());
}

#[tokio::test]
async fn listener_dispatches_push_then_stops_on_kickout_and_second_listen_is_noop() {
    let mut config = good_cfg();
    config.ws_domain = "ws://127.0.0.1:0".into();
    config.dm_access = "open".into();
    let ch = YuanbaoChannel::new(config).unwrap();
    let (incoming, rx) = mpsc::unbounded_channel();
    *ch.inbound_rx.lock().await = Some(rx);
    incoming
        .send(InboundEvent::Push(make_push_frame(
            "inbound_message",
            serde_json::to_vec(&serde_json::json!({
                "from_account":"alice", "msg_id":"listener-message",
                "msg_body":[{"msg_type":"TIMTextElem","msg_content":{"text":"hello"}}],
            }))
            .unwrap(),
        )))
        .unwrap();
    incoming
        .send(InboundEvent::Kickout("signed in elsewhere".into()))
        .unwrap();
    let (tx, mut received) = mpsc::channel(4);
    tokio::time::timeout(Duration::from_secs(3), ch.listen(tx.clone()))
        .await
        .unwrap()
        .unwrap();
    let message = received.try_recv().unwrap();
    assert_eq!(message.id, "listener-message");
    assert_eq!(message.sender, "alice");
    assert_eq!(message.content, "hello");
    assert!(*ch.shutdown_tx.borrow());
    ch.listen(tx).await.unwrap();
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn listener_stops_when_inbound_channel_closes() {
    let mut config = good_cfg();
    config.ws_domain = "ws://127.0.0.1:0".into();
    let ch = YuanbaoChannel::new(config).unwrap();
    let (incoming, rx) = mpsc::unbounded_channel();
    *ch.inbound_rx.lock().await = Some(rx);
    drop(incoming);
    let (tx, _rx) = mpsc::channel(1);
    tokio::time::timeout(Duration::from_secs(3), ch.listen(tx))
        .await
        .unwrap()
        .unwrap();
    assert!(*ch.shutdown_tx.borrow());
}

#[tokio::test]
async fn listener_stops_when_shutdown_is_signaled() {
    let mut config = good_cfg();
    config.ws_domain = "ws://127.0.0.1:0".into();
    let ch = Arc::new(YuanbaoChannel::new(config).unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let listener = {
        let ch = ch.clone();
        tokio::spawn(async move { ch.listen(tx).await })
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while ch.inbound_rx.lock().await.is_some() {
            tokio::task::yield_now().await;
        }
        ch.shutdown_tx.send(true).unwrap();
        listener.await.unwrap().unwrap();
    })
    .await
    .unwrap();
    assert!(!ch.connection.is_connected());
}

#[derive(Clone)]
struct PublicAdapterSink(tokio::sync::mpsc::UnboundedSender<tinychannels_bus::ChannelIngress>);

#[async_trait::async_trait]
impl crate::channel::ChannelInboundSink for PublicAdapterSink {
    async fn push(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> Result<(), crate::channel::ChannelSendError> {
        self.0
            .send(ingress)
            .map_err(|_| crate::channel::ChannelSendError {
                kind: crate::channel::SendErrorKind::Transient,
                message: "test ingress receiver closed".into(),
                retryable: true,
                ..Default::default()
            })
    }
}

fn yuanbao_push(message_id: &str, sender: &str, text: &str) -> Vec<u8> {
    crate::providers::yuanbao::connection::test_support::encode_frame(
        crate::providers::yuanbao::proto_constants::cmd_type::PUSH,
        "inbound_message",
        message_id,
        "yuanbao_openclaw_proxy",
        true,
        0,
        &serde_json::to_vec(&serde_json::json!({
            "from_account": sender,
            "msg_id": message_id,
            "msg_time": 1_700_000_000,
            "sender_nickname": "Alice",
            "msg_body": [{
                "msg_type": "TIMTextElem",
                "msg_content": { "text": text },
            }],
        }))
        .unwrap(),
    )
}

fn yuanbao_sound_push(message_id: &str, sender: &str, url: &str) -> Vec<u8> {
    crate::providers::yuanbao::connection::test_support::encode_frame(
        crate::providers::yuanbao::proto_constants::cmd_type::PUSH,
        "inbound_message",
        message_id,
        "yuanbao_openclaw_proxy",
        true,
        0,
        &serde_json::to_vec(&serde_json::json!({
            "from_account": sender,
            "msg_id": message_id,
            "msg_time": 1_700_000_000,
            "sender_nickname": "Alice",
            "msg_body": [{
                "msg_type": "TIMSoundElem",
                "msg_content": {
                    "sound": "sound-id",
                    "url": url,
                    "file_name": "voice.ogg",
                },
            }],
        }))
        .unwrap(),
    )
}

async fn expect_yuanbao_push_ack(
    session: &mut crate::providers::yuanbao::connection::test_support::GatewaySession,
    message_id: &str,
) {
    let ack = tokio::time::timeout(std::time::Duration::from_secs(3), session.recv_frame())
        .await
        .expect("gateway push should be acknowledged");
    assert_eq!(
        ack.cmd_type,
        crate::providers::yuanbao::proto_constants::cmd_type::PUSH_ACK
    );
    assert_eq!(ack.msg_id, message_id);
}

async fn next_public_yuanbao_message(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<tinychannels_bus::ChannelIngress>,
) -> tinychannels_bus::channel::ChannelInboundEnvelope {
    match tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .expect("native Yuanbao listener should publish an ingress")
        .expect("native Yuanbao listener dropped its supplied sink")
    {
        tinychannels_bus::ChannelIngress::Inbound(envelope) => envelope,
        other => panic!("expected inbound message, got {other:?}"),
    }
}

#[tokio::test]
async fn public_adapter_start_delivers_admitted_deduplicated_pushes_and_restarts() {
    use crate::channel::ChannelAdapter;
    use crate::providers::yuanbao::connection::test_support::{MockGateway, gateway_config};
    let gateway = MockGateway::bind().await;
    let mut config = gateway_config(&gateway.url);
    config.dm_access = "allowlist".into();
    config.allowed_users = vec!["alice".into()];
    let channel = Arc::new(YuanbaoChannel::new(config).unwrap());
    let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(PublicAdapterSink(events));

    let start = {
        let channel = Arc::clone(&channel);
        let sink = Arc::clone(&sink);
        tokio::spawn(async move { channel.start(sink.as_ref()).await })
    };
    let mut first_session = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        gateway.accept_authed("connect-first"),
    )
    .await
    .expect("ChannelAdapter::start must open the configured local gateway");

    for (message_id, sender, text) in [
        ("denied-before-allowed", "mallory", "must be filtered"),
        ("accepted-once", "alice", "first delivery"),
        ("accepted-once", "alice", "duplicate delivery"),
        ("ordered-barrier", "alice", "barrier"),
    ] {
        first_session
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                yuanbao_push(message_id, sender, text).into(),
            ))
            .await;
        expect_yuanbao_push_ack(&mut first_session, message_id).await;
    }

    let first = next_public_yuanbao_message(&mut received).await;
    assert_eq!(first.channel.id, "yuanbao");
    assert_eq!(first.message_id, "accepted-once");
    assert_eq!(first.sender.id, "alice");
    assert_eq!(
        first.conversation.kind,
        tinychannels_bus::channel::ConversationKind::Dm
    );
    assert_eq!(first.conversation.id, "alice");
    assert_eq!(first.text, "first delivery");
    let barrier = next_public_yuanbao_message(&mut received).await;
    assert_eq!(barrier.message_id, "ordered-barrier");
    assert_eq!(barrier.sender.id, "alice");
    assert_eq!(barrier.text, "barrier");

    channel.stop().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), start)
        .await
        .expect("stopping Yuanbao should join its listener")
        .expect("listener task should not panic")
        .expect("listener should stop cleanly");
    assert!(
        received.try_recv().is_err(),
        "denied and duplicate pushes stay filtered"
    );

    let restarted = {
        let channel = Arc::clone(&channel);
        let sink = Arc::clone(&sink);
        tokio::spawn(async move { channel.start(sink.as_ref()).await })
    };
    let mut second_session = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        gateway.accept_authed("connect-second"),
    )
    .await
    .expect("the stopped adapter should reconnect on restart");
    second_session
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            yuanbao_push("after-restart", "alice", "restarted").into(),
        ))
        .await;
    expect_yuanbao_push_ack(&mut second_session, "after-restart").await;
    let after_restart = next_public_yuanbao_message(&mut received).await;
    assert_eq!(after_restart.message_id, "after-restart");
    assert_eq!(after_restart.text, "restarted");

    channel.stop().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), restarted)
        .await
        .expect("second stop should join the restarted listener")
        .expect("restarted listener task should not panic")
        .expect("restarted listener should stop cleanly");
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn public_adapter_stores_url_backed_voice_and_publishes_typed_media() {
    use crate::channel::ChannelAdapter;
    use crate::providers::http_test_support::HttpsServer;
    use crate::providers::native::fixtures::StubVoice;
    use crate::providers::yuanbao::connection::test_support::{MockGateway, gateway_config};
    use base64::Engine as _;
    use tinychannels_bus::ContentPart;

    let voice_fixture = include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg").to_vec();
    let received_body = voice_fixture.clone();
    let media_server = HttpsServer::start_with_bytes(
        move |request| {
            assert_eq!(request.method, "GET");
            assert_eq!(request.target, "/voice.ogg");
            (200, received_body.clone())
        },
        "audio/ogg",
    )
    .await;
    let gateway = MockGateway::bind().await;
    let config = gateway_config(&gateway.url);
    let voice_store = Arc::new(StubVoice::default());
    let channel = Arc::new(
        YuanbaoChannel::new(config)
            .unwrap()
            .with_voice_media_store(voice_store.clone())
            .with_voice_http_client(media_server.client.clone()),
    );
    let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(PublicAdapterSink(events));
    let listener = {
        let channel = Arc::clone(&channel);
        let sink = Arc::clone(&sink);
        tokio::spawn(async move { channel.start(sink.as_ref()).await })
    };
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        gateway.accept_authed("voice-connect"),
    )
    .await
    .expect("Yuanbao listener should connect to the local gateway");
    session
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            yuanbao_sound_push(
                "voice-public-1",
                "alice",
                "https://api.dingtalk.com/voice.ogg",
            )
            .into(),
        ))
        .await;
    expect_yuanbao_push_ack(&mut session, "voice-public-1").await;

    let envelope = next_public_yuanbao_message(&mut received).await;
    assert_eq!(envelope.message_id, "voice-public-1");
    assert_eq!(envelope.channel.account_id.as_deref(), Some("bot1"));
    assert!(
        envelope.text.is_empty(),
        "signed voice URL must not become model text"
    );
    assert!(matches!(
        envelope.parts.as_slice(),
        [ContentPart::VoiceNote { .. }]
    ));
    let stored = voice_store.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].scope.turn_id, "voice-public-1");
    assert_eq!(stored[0].mime_type, "audio/ogg");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&stored[0].audio_base64)
            .unwrap(),
        voice_fixture
    );
    drop(stored);
    assert_eq!(media_server.requests.lock().unwrap().len(), 1);

    channel.stop().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), listener)
        .await
        .expect("stopping the typed listener should join it")
        .expect("listener task should not panic")
        .expect("listener should stop cleanly");
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn public_adapter_retries_a_message_after_the_supplied_sink_rejects_it() {
    use crate::channel::ChannelAdapter;
    use crate::providers::yuanbao::connection::test_support::{MockGateway, gateway_config};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinychannels_bus::ChannelIngress;
    use tinychannels_bus::channel::SendErrorKind;

    struct RejectFirstSink {
        attempts: AtomicUsize,
        accepted: tokio::sync::mpsc::UnboundedSender<ChannelIngress>,
    }

    #[async_trait::async_trait]
    impl crate::channel::ChannelInboundSink for RejectFirstSink {
        async fn push(
            &self,
            ingress: ChannelIngress,
        ) -> Result<(), crate::channel::ChannelSendError> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(crate::channel::ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "temporary sink rejection".into(),
                    retryable: true,
                    ..Default::default()
                });
            }
            self.accepted
                .send(ingress)
                .map_err(|_| crate::channel::ChannelSendError {
                    kind: SendErrorKind::Transient,
                    message: "test receiver closed".into(),
                    retryable: true,
                    ..Default::default()
                })
        }
    }

    let gateway = MockGateway::bind().await;
    let channel = Arc::new(YuanbaoChannel::new(gateway_config(&gateway.url)).unwrap());
    let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(RejectFirstSink {
        attempts: AtomicUsize::new(0),
        accepted: events,
    });

    let first_start = {
        let channel = Arc::clone(&channel);
        let sink = Arc::clone(&sink);
        tokio::spawn(async move { channel.start(sink.as_ref()).await })
    };
    let mut first_session = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        gateway.accept_authed("reject-first"),
    )
    .await
    .expect("first listener should connect");
    first_session
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            yuanbao_push("retry-after-sink-error", "alice", "retry me").into(),
        ))
        .await;
    expect_yuanbao_push_ack(&mut first_session, "retry-after-sink-error").await;
    let first_error = tokio::time::timeout(std::time::Duration::from_secs(3), first_start)
        .await
        .expect("a rejected typed publication must end this listener attempt")
        .expect("listener task should not panic");
    assert!(
        first_error.is_err(),
        "sink rejection must not be reported as accepted"
    );
    assert!(
        received.try_recv().is_err(),
        "rejected publication must not leak to the sink"
    );

    let restarted = {
        let channel = Arc::clone(&channel);
        let sink = Arc::clone(&sink);
        tokio::spawn(async move { channel.start(sink.as_ref()).await })
    };
    let mut second_session = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        gateway.accept_authed("retry-same-id"),
    )
    .await
    .expect("the failed listener should release ownership for retry");
    second_session
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            yuanbao_push("retry-after-sink-error", "alice", "retry me").into(),
        ))
        .await;
    expect_yuanbao_push_ack(&mut second_session, "retry-after-sink-error").await;
    let accepted = next_public_yuanbao_message(&mut received).await;
    assert_eq!(accepted.message_id, "retry-after-sink-error");
    assert_eq!(accepted.text, "retry me");
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 2);

    channel.stop().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), restarted)
        .await
        .expect("stopping retry listener should join it")
        .expect("retry listener task should not panic")
        .expect("retry listener should stop cleanly");
    assert!(received.try_recv().is_err());
}
