use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction, ChannelStreamingDraft,
    ChannelTypedSend, ChannelTyping,
};
use crate::providers::imessage::fixtures::*;
use crate::providers::native::fixtures::stored_media;
use crate::traits::Channel;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tinychannels_bus::channel::{ChannelSendOutcome, MessageReceipt, SendErrorKind};
use tinychannels_bus::{
    ChannelIngress, ChannelOutboundIntent, ChannelOutboundMessage, ContentPart, ConversationKind,
    ConversationRef, MediaKind, MediaReference, OutboundContentPart, OutboundPayload,
    OutboundVoice,
};
use tokio::sync::{Notify, oneshot, watch};

#[derive(Default)]
struct RecordingIngress {
    items: Mutex<Vec<ChannelIngress>>,
    entered: Notify,
}

#[async_trait]
impl crate::channel::ChannelInboundSink for RecordingIngress {
    async fn push(
        &self,
        ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.items.lock().unwrap().push(ingress);
        self.entered.notify_one();
        Ok(())
    }
}

#[derive(Default)]
struct RejectSecondIngress {
    calls: AtomicUsize,
    accepted: Mutex<Vec<ChannelIngress>>,
}

#[async_trait]
impl crate::channel::ChannelInboundSink for RejectSecondIngress {
    async fn push(
        &self,
        ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            return Err(tinychannels_bus::channel::ChannelSendError {
                message: "fixture rejects second delivery".into(),
                retryable: true,
                ..Default::default()
            });
        }
        self.accepted.lock().unwrap().push(ingress);
        Ok(())
    }
}

#[derive(Default)]
struct BlockingIngress {
    entered: Notify,
}

#[async_trait]
impl crate::channel::ChannelInboundSink for BlockingIngress {
    async fn push(
        &self,
        _ingress: ChannelIngress,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.entered.notify_one();
        std::future::pending().await
    }
}

fn message(to: &str, parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "imessage".into(),
        conversation_id: to.into(),
        channel_ref: None,
        conversation_ref: Some(ConversationRef {
            kind: ConversationKind::Dm,
            id: to.into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "k".into(),
        parts,
    }
}

#[tokio::test]
async fn typed_listener_retries_rejected_rows_without_replaying_accepted_predecessors() {
    let rig = rig();
    let reject_second = Arc::new(RejectSecondIngress::default());
    let (stop, cancellation) = watch::channel(false);
    let (ready, started) = oneshot::channel();
    let channel = rig.channel.clone();
    let sink = reject_second.clone();
    let listener = tokio::spawn(async move {
        channel
            .listen_typed_with_ready(sink.as_ref(), cancellation, ready)
            .await
    });
    started.await.unwrap();

    rig.inbound(1, Some("first"), false, &[]);
    rig.inbound(2, Some("second"), false, &[]);
    let failure = tokio::time::timeout(Duration::from_secs(2), listener)
        .await
        .expect("rejected ingress terminates the current listener")
        .unwrap()
        .unwrap_err();
    assert!(failure.retryable);

    {
        let accepted = reject_second.accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1);
        assert!(matches!(
            &accepted[0],
            ChannelIngress::Inbound(envelope) if envelope.message_id == "IN1"
        ));
        if let ChannelIngress::Inbound(envelope) = &accepted[0] {
            assert_eq!(envelope.channel.id, "imessage");
            assert_eq!(envelope.channel.account_id, None);
            assert_eq!(envelope.conversation.id, PEER);
            assert_eq!(envelope.sender.id, PEER);
        }
    }

    let recording = Arc::new(RecordingIngress::default());
    let (stop_retry, retry_cancellation) = watch::channel(false);
    let (ready_retry, retry_started) = oneshot::channel();
    let channel = rig.channel.clone();
    let sink = recording.clone();
    let retry = tokio::spawn(async move {
        channel
            .listen_typed_with_ready(sink.as_ref(), retry_cancellation, ready_retry)
            .await
    });
    retry_started.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), recording.entered.notified())
        .await
        .expect("the rejected row is retried");
    stop_retry.send(true).unwrap();
    retry.await.unwrap().unwrap();

    let delivered = recording.items.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    assert!(matches!(
        &delivered[0],
        ChannelIngress::Inbound(envelope) if envelope.message_id == "IN2"
    ));
    let _ = stop.send(true);
}

#[tokio::test]
async fn stopping_typed_listener_cancels_backpressure_and_releases_registration() {
    let rig = rig();
    let blocked = Arc::new(BlockingIngress::default());
    let channel = rig.channel.clone();
    let sink = blocked.clone();
    let listener =
        tokio::spawn(async move { ChannelAdapter::start(&channel, sink.as_ref()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if rig.channel.listener_cursor.lock().unwrap().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener fixes its startup cursor");

    let duplicate = ChannelAdapter::start(&rig.channel, blocked.as_ref())
        .await
        .unwrap_err();
    assert!(
        duplicate.retryable,
        "a concurrent start is reported as retryable"
    );

    rig.inbound(1, Some("blocked"), false, &[]);
    tokio::time::timeout(Duration::from_secs(2), blocked.entered.notified())
        .await
        .expect("listener reaches the blocking ingress sink");
    ChannelAdapter::stop(&rig.channel).await.unwrap();
    // A repeated stop is harmless after the registration guard releases.
    ChannelAdapter::stop(&rig.channel).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), listener)
        .await
        .expect("stop interrupts sink backpressure")
        .unwrap()
        .unwrap();
    assert!(rig.channel.listener_cancel.lock().unwrap().is_none());
    assert_eq!(
        rig.channel
            .listener_cursor
            .lock()
            .unwrap()
            .unwrap()
            .inbound_rowid,
        0,
        "a cancelled push must not advance the accepted cursor"
    );

    let recording = Arc::new(RecordingIngress::default());
    let channel = rig.channel.clone();
    let sink = recording.clone();
    let restarted =
        tokio::spawn(async move { ChannelAdapter::start(&channel, sink.as_ref()).await });
    tokio::time::timeout(Duration::from_secs(2), recording.entered.notified())
        .await
        .expect("restart retries the message that was not accepted");
    ChannelAdapter::stop(&rig.channel).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), restarted)
        .await
        .expect("restarted listener stops")
        .unwrap()
        .unwrap();
    let delivered = recording.items.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    assert!(matches!(
        &delivered[0],
        ChannelIngress::Inbound(envelope) if envelope.message_id == "IN1"
    ));
}

#[tokio::test]
async fn typed_listener_reports_unavailable_and_invalid_messages_databases() {
    let rig = rig();
    let sink = RecordingIngress::default();
    let (_, cancellation) = watch::channel(false);
    let (ready, _started) = oneshot::channel();
    let mut missing = rig.channel.clone();
    missing.db_path = Some(rig.dir.path().join("missing-chat.db"));
    let error = missing
        .listen_typed_with_ready(&sink, cancellation, ready)
        .await
        .unwrap_err();
    assert!(error.message.contains("database does not exist"));

    let invalid_path = rig.dir.path().join("invalid-chat.db");
    std::fs::write(&invalid_path, b"not a sqlite database").unwrap();
    let mut invalid = rig.channel.clone();
    invalid.db_path = Some(invalid_path);
    let (_, cancellation) = watch::channel(false);
    let (ready, _started) = oneshot::channel();
    let error = invalid
        .listen_typed_with_ready(&sink, cancellation, ready)
        .await
        .unwrap_err();
    assert!(error.message.contains("startup cursor could not be read"));
}

#[tokio::test]
async fn typed_tapbacks_are_scoped_reaction_events_and_ignore_orphans() {
    let rig = rig();
    let sink = RecordingIngress::default();
    let (_, mut cancellation) = watch::channel(false);
    let mut added = rig.inbound(20, Some("Liked a message"), false, &[]);
    added.associated_type = 2001;
    added.associated_guid = Some("p:0/TARGET".into());
    rig.channel
        .dispatch_row_typed(added.clone(), &sink, &mut cancellation)
        .await
        .unwrap();
    // A committed tapback is idempotently acknowledged without a second push.
    rig.channel
        .dispatch_row_typed(added, &sink, &mut cancellation)
        .await
        .unwrap();

    let mut removed = rig.inbound(21, Some("Removed a like"), false, &[]);
    removed.associated_type = 3001;
    removed.associated_guid = Some("bp:TARGET".into());
    rig.channel
        .dispatch_row_typed(removed, &sink, &mut cancellation)
        .await
        .unwrap();

    let mut orphan = rig.inbound(22, Some("orphan"), false, &[]);
    orphan.associated_type = 2000;
    rig.channel
        .dispatch_row_typed(orphan, &sink, &mut cancellation)
        .await
        .unwrap();

    let items = sink.items.lock().unwrap();
    assert_eq!(items.len(), 2);
    assert!(matches!(
        &items[0],
        ChannelIngress::Event(event)
            if event.channel.id == "imessage"
                && event.conversation.as_ref().is_some_and(|c| c.id == PEER)
                && matches!(&event.event, ChannelEventKind::ReactionAdded { message_id, reaction, .. }
                    if message_id == "TARGET" && reaction.value == "👍")
    ));
    assert!(matches!(
        &items[1],
        ChannelIngress::Event(event)
            if matches!(&event.event, ChannelEventKind::ReactionRemoved { message_id, .. }
                if message_id == "TARGET")
    ));
}

#[tokio::test]
async fn typed_dispatch_drops_disallowed_and_blank_messages() {
    let rig = rig();
    let sink = RecordingIngress::default();
    let (_, mut cancellation) = watch::channel(false);
    let mut restricted = rig.channel.clone();
    restricted.allowed_contacts = vec!["+19999999999".into()];
    let foreign = rig.inbound(23, Some("foreign"), false, &[]);
    restricted
        .dispatch_row_typed(foreign, &sink, &mut cancellation)
        .await
        .unwrap();

    let blank = rig.inbound(24, Some("\u{FFFC}"), false, &[]);
    rig.channel
        .dispatch_row_typed(blank, &sink, &mut cancellation)
        .await
        .unwrap();
    assert!(sink.items.lock().unwrap().is_empty());
}

#[tokio::test]
async fn typed_attachments_use_the_scoped_media_ingest_path() {
    let rig = rig();
    let file = rig.attachment("typed-photo.png", PNG);
    let row = rig.inbound(25, Some("look"), false, &[(&file, "image/png")]);
    let sink = RecordingIngress::default();
    let (_, mut cancellation) = watch::channel(false);

    rig.channel
        .dispatch_row_typed(row, &sink, &mut cancellation)
        .await
        .unwrap();

    assert_eq!(rig.media.stored.lock().unwrap()[0].bytes, PNG);
    let items = sink.items.lock().unwrap();
    assert!(matches!(
        &items[0],
        ChannelIngress::Inbound(envelope)
            if envelope.message_id == "IN25"
                && envelope.conversation.id == PEER
                && matches!(envelope.parts.get(1), Some(ContentPart::Image { .. }))
    ));
}

#[tokio::test]
async fn typed_read_receipts_keep_the_recipient_conversation_scope() {
    let rig = rig();
    let conn = rig.conn();
    conn.execute(
        "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date, date_read) \
         VALUES (90, 'OUT90', 1, 'sent', 1, 700000000000000000, 0)",
        [],
    )
    .unwrap();

    let recording = Arc::new(RecordingIngress::default());
    let (stop, cancellation) = watch::channel(false);
    let (ready, started) = oneshot::channel();
    let channel = rig.channel.clone();
    let sink = recording.clone();
    let listener = tokio::spawn(async move {
        channel
            .listen_typed_with_ready(sink.as_ref(), cancellation, ready)
            .await
    });
    started.await.unwrap();
    rig.conn()
        .execute(
            "UPDATE message SET date_read = 700000000000000001 WHERE ROWID = 90",
            [],
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), recording.entered.notified())
        .await
        .expect("typed listener publishes the read receipt");
    stop.send(true).unwrap();
    listener.await.unwrap().unwrap();

    let delivered = recording.items.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    match &delivered[0] {
        ChannelIngress::Event(event) => {
            assert_eq!(event.channel.id, "imessage");
            assert_eq!(event.conversation.as_ref().unwrap().id, PEER);
            assert!(matches!(
                event.event,
                tinychannels_bus::ChannelEventKind::ReadReceipt { ref message_id, .. }
                    if message_id == "OUT90"
            ));
        }
        other => panic!("expected a typed read receipt event, got {other:?}"),
    }
}

#[tokio::test]
async fn typed_ingress_does_not_acknowledge_a_pending_reservation() {
    let rig = rig();
    let row = rig.inbound(3, Some("pending"), false, &[]);
    let key = "imessage:IN3".to_owned();
    let reservation = match rig.channel.native.reserve_seen(key) {
        crate::providers::native::SeenReservationResult::Reserved(reservation) => reservation,
        other => panic!("expected a new reservation, got {other:?}"),
    };
    let sink = RecordingIngress::default();
    let (_, mut cancellation) = watch::channel(false);

    let error = rig
        .channel
        .dispatch_row_typed(row, &sink, &mut cancellation)
        .await
        .unwrap_err();
    assert!(error.retryable);
    assert!(sink.items.lock().unwrap().is_empty());
    drop(reservation);
}

fn text(body: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: body.into(),
            markdown_dialect: None,
        },
    }
}

fn media(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some("media-1".into()),
        kind,
        ..Default::default()
    }
}

async fn send(
    rig: &Rig,
    message: &ChannelOutboundMessage,
) -> Result<ChannelSendOutcome, ChannelSendError> {
    ChannelTypedSend::send_typed(&rig.channel, message).await
}

#[tokio::test]
async fn text_is_sent_through_applescript_and_the_guid_comes_from_chat_db() {
    let rig = rig();
    let recorder = rig.spawn_messages();
    let outcome = send(&rig, &message(PEER, vec![text("say \"hi\"")]))
        .await
        .unwrap();
    recorder.abort();
    assert_eq!(outcome.receipt.platform_message_ids, ["SENT1"]);
    assert!(
        outcome.receipt.parts[0].edit_token.is_none(),
        "iMessage cannot edit"
    );
    let script = &rig.scripts()[0];
    assert!(
        script.contains(&format!("participant \"{PEER}\"")),
        "{script}"
    );
    assert!(
        script.contains("send \"say \\\"hi\\\"\" to targetBuddy"),
        "{script}"
    );
}

#[tokio::test]
async fn group_chats_are_addressed_by_chat_id() {
    let rig = rig();
    let recorder = rig.spawn_messages();
    send(&rig, &message(GROUP, vec![text("hello team")]))
        .await
        .unwrap();
    recorder.abort();
    let script = &rig.scripts()[0];
    assert!(script.contains(&format!("chat id \"{GROUP}\"")), "{script}");
    assert!(
        script.contains("send \"hello team\" to targetChat"),
        "{script}"
    );
}

#[tokio::test]
async fn invalid_targets_never_run_a_script() {
    let rig = rig();
    for target in [
        "not a target",
        "iMessage;x;chat1",
        "iMessage;+;",
        "iMessage;+;a;b",
        "bogus;+;chat1",
    ] {
        let error = send(&rig, &message(target, vec![text("x")]))
            .await
            .unwrap_err();
        assert!(!error.unsupported, "{target}");
    }
    assert!(!rig.out.exists());
}

#[tokio::test]
async fn files_are_staged_sent_and_cleaned_up_with_the_caption_first() {
    let rig = rig();
    let recorder = rig.spawn_messages();
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    let outcome = send(
        &rig,
        &message(
            PEER,
            vec![OutboundContentPart::Content {
                part: ContentPart::Image {
                    media: media(MediaKind::Image),
                    caption: Some("look".into()),
                    alt_text: None,
                },
            }],
        ),
    )
    .await
    .unwrap();
    recorder.abort();
    assert_eq!(outcome.receipt.platform_message_ids, ["SENT1", "SENT2"]);
    let scripts = rig.scripts();
    assert!(scripts[0].contains("send \"look\""));
    assert!(scripts[1].contains("send (POSIX file \""), "{}", scripts[1]);
    assert!(scripts[1].contains("report.pdf"));
    let staged = scripts[1].split('"').nth(3).unwrap();
    assert!(
        !std::path::Path::new(staged).exists(),
        "the staged file is removed"
    );
}

#[tokio::test]
async fn voice_clips_are_sent_as_audio_attachments() {
    let rig = rig();
    let recorder = rig.spawn_messages();
    let outcome = send(
        &rig,
        &message(
            PEER,
            vec![OutboundContentPart::Voice {
                voice: OutboundVoice::Media {
                    media: media(MediaKind::Audio),
                    audio_base64: None,
                    codec: None,
                    duration_ms: None,
                    waveform: None,
                },
            }],
        ),
    )
    .await
    .unwrap();
    recorder.abort();
    assert_eq!(
        outcome.receipt.parts[0].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Voice
    );
    assert!(rig.scripts()[0].contains("reply.m4a"));
}

#[tokio::test]
async fn runner_failures_and_missing_rows_are_distinguished() {
    // The runner fails: nothing was sent, so a retry is safe.
    let mut failing = rig();
    failing.channel.script_command = vec!["sh".into(), "-c".into(), "exit 3".into(), "x".into()];
    let error = send(&failing, &message(PEER, vec![text("x")]))
        .await
        .unwrap_err();
    assert!(error.retryable && !error.sent_before_error);

    // The runner cannot be started at all, or is not configured.
    failing.channel.script_command = vec!["/definitely/not/a/runner".into()];
    assert!(
        send(&failing, &message(PEER, vec![text("x")]))
            .await
            .is_err()
    );
    failing.channel.script_command = Vec::new();
    assert!(
        send(&failing, &message(PEER, vec![text("x")]))
            .await
            .is_err()
    );

    // The runner succeeds but Messages never records the message: uncertain.
    let mut silent = rig();
    silent.channel.resolve_timeout = Duration::from_millis(60);
    let error = send(&silent, &message(PEER, vec![text("x")]))
        .await
        .unwrap_err();
    assert!(error.sent_before_error && !error.retryable);
}

#[tokio::test]
async fn sends_need_the_messages_database() {
    let mut rig = rig();
    rig.channel.db_path = Some(rig.dir.path().join("missing.db"));
    let error = send(&rig, &message(PEER, vec![text("x")]))
        .await
        .unwrap_err();
    assert!(error.message.contains("Messages database"));
    assert!(!rig.out.exists());
}

#[tokio::test]
async fn a_failed_attachment_after_the_caption_is_a_partial_send() {
    let mut rig = rig();
    // The first script (the caption) records a row; later scripts fail.
    rig.channel.script_command = vec![
        "sh".into(),
        "-c".into(),
        format!(
            "if [ -f '{0}' ]; then exit 1; fi; printf 'x\\n---\\n' > '{0}'",
            rig.out.display()
        ),
        "fake".into(),
    ];
    let recorder = rig.spawn_messages();
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    let result = send(
        &rig,
        &message(
            PEER,
            vec![OutboundContentPart::Content {
                part: ContentPart::Image {
                    media: media(MediaKind::Image),
                    caption: Some("look".into()),
                    alt_text: None,
                },
            }],
        ),
    )
    .await;
    recorder.abort();
    // The caption was delivered, so the failure must say a retry is unsafe.
    let error = result.unwrap_err();
    assert!(error.sent_before_error && !error.retryable);
    assert_eq!(rig.scripts().len(), 1);
}

#[test]
fn staged_file_names_cannot_escape_or_carry_script_syntax() {
    assert_eq!(safe_name("../../etc/passwd"), "passwd");
    assert_eq!(safe_name("a \"b\".png"), "a__b_.png");
    assert_eq!(safe_name("..hidden"), "hidden");
    assert_eq!(safe_name("///"), "attachment");
    assert_eq!(safe_name("ok-1_2.m4a"), "ok-1_2.m4a");
}

#[tokio::test]
async fn operations_applescript_cannot_do_are_explicitly_unsupported() {
    let rig = rig();
    let channel = &rig.channel;
    assert!(
        ChannelTyping::start_typing(channel, PEER)
            .await
            .unwrap_err()
            .unsupported
    );
    let reaction = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    let chan = tinychannels_bus::ChannelRef {
        id: "imessage".into(),
        account_id: None,
    };
    let conv = ConversationRef {
        id: PEER.into(),
        ..Default::default()
    };
    assert!(
        ChannelReaction::send_reaction(channel, &chan, &conv, "G1", &reaction, false)
            .await
            .unwrap_err()
            .unsupported
    );
    let receipt = MessageReceipt {
        conversation: Some(conv),
        primary_platform_message_id: Some("G1".into()),
        ..Default::default()
    };
    let intent = ChannelOutboundIntent {
        idempotency_key: "i".into(),
        channel_id: "imessage".into(),
        conversation_id: PEER.into(),
        payload: OutboundPayload::Text { text: "x".into() },
        ..Default::default()
    };
    assert!(
        ChannelEdit::edit(channel, &receipt, intent.clone())
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(
        ChannelDelete::delete(channel, &receipt)
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(
        ChannelStreamingDraft::upsert_draft(channel, None, intent.clone())
            .await
            .unwrap_err()
            .unsupported
    );
    let mut threaded = message(PEER, vec![text("x")]);
    threaded.thread_id = Some("t".into());
    assert!(send(&rig, &threaded).await.unwrap_err().unsupported);
    let mut reply = message(PEER, vec![text("x")]);
    reply.reply_to_id = Some("r".into());
    assert!(send(&rig, &reply).await.unwrap_err().unsupported);
    assert!(!rig.out.exists());
}

#[tokio::test]
async fn the_descriptor_status_and_legacy_methods_use_the_native_surface() {
    let rig = rig();
    let descriptor = ChannelAdapter::descriptor(&rig.channel);
    assert_eq!(descriptor.id, "imessage");
    assert_eq!(descriptor.metadata["adapterMode"], "native-v2");
    assert_eq!(descriptor.metadata["voiceOut"], "attachment");
    assert_eq!(Channel::capabilities(&rig.channel), rig.channel.declared());
    assert_eq!(
        ChannelAdapter::status(&rig.channel)
            .await
            .unwrap()
            .channel_id,
        "imessage"
    );
    let recorder = rig.spawn_messages();
    let outcome =
        Channel::send_typed_with_receipt(&rig.channel, &message(PEER, vec![text("typed")]))
            .await
            .unwrap();
    recorder.abort();
    assert_eq!(outcome.receipt.platform_message_ids, ["SENT1"]);
    let error = Channel::send_typed_with_receipt(
        &rig.channel,
        &message(
            PEER,
            vec![OutboundContentPart::Content {
                part: ContentPart::Poll {
                    question: "q".into(),
                    options: vec![],
                    multiple_choice: false,
                    closes_at_unix_ms: None,
                    provider_poll_id: None,
                },
            }],
        ),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .downcast_ref::<crate::providers::native::NativeFailure>()
            .unwrap()
            .0
            .unsupported
    );
    let _ = SendErrorKind::Unknown;
}
