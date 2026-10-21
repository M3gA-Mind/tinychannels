use super::*;

#[tokio::test]
async fn typed_status_uses_the_configured_messages_database_health() {
    let rig = rig();
    let snapshot = ChannelAdapter::status(&rig.channel).await.unwrap();
    let expected = if cfg!(target_os = "macos") {
        "healthy"
    } else {
        "unhealthy"
    };
    assert_eq!(snapshot.health_state.as_deref(), Some(expected));

    let mut missing = IMessageChannel::new(vec!["*".into()]);
    missing.db_path = Some(rig.dir.path().join("missing-chat.db"));
    let snapshot = ChannelAdapter::status(&missing).await.unwrap();
    assert_eq!(snapshot.health_state.as_deref(), Some("unhealthy"));
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
    let (_stop, mut cancellation) = watch::channel(false);
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
    let file = rig.attachment("typed-photo.png", TYPED_PNG);
    let row = rig.inbound(25, Some("look"), false, &[(&file, "image/png")]);
    let sink = RecordingIngress::default();
    let (_stop, mut cancellation) = watch::channel(false);

    rig.channel
        .dispatch_row_typed(row, &sink, &mut cancellation)
        .await
        .unwrap();

    assert_eq!(rig.media.stored.lock().unwrap()[0].bytes, TYPED_PNG);
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
async fn typed_read_receipts_retry_a_rejected_row_sharing_the_accepted_timestamp() {
    let rig = rig();
    let conn = rig.conn();
    conn.execute_batch(
        "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date, date_read) VALUES
            (91, 'OUT91', 1, 'first', 1, 700000000000000000, 0),
            (92, 'OUT92', 1, 'second', 1, 700000000000000000, 0);",
    )
    .unwrap();

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
    let conn = rig.conn();
    conn.execute_batch(
        "UPDATE message SET date_read = 700000000000000001 WHERE ROWID IN (91, 92);",
    )
    .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), listener)
        .await
        .expect("the listener reaches the rejected second receipt")
        .unwrap()
        .unwrap_err();
    assert!(error.retryable);
    {
        let accepted = reject_second.accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1);
        assert!(matches!(
            &accepted[0],
            ChannelIngress::Event(event)
                if matches!(&event.event, ChannelEventKind::ReadReceipt { message_id, .. } if message_id == "OUT91")
        ));
    }

    let recording = Arc::new(RecordingIngress::default());
    let (stop_retry, cancellation) = watch::channel(false);
    let (ready, started) = oneshot::channel();
    let channel = rig.channel.clone();
    let sink = recording.clone();
    let retry = tokio::spawn(async move {
        channel
            .listen_typed_with_ready(sink.as_ref(), cancellation, ready)
            .await
    });
    started.await.unwrap();
    let retry_result =
        tokio::time::timeout(Duration::from_secs(2), recording.entered.notified()).await;
    stop_retry.send(true).unwrap();
    retry.await.unwrap().unwrap();
    assert!(
        retry_result.is_ok(),
        "the rejected tied-timestamp receipt is retried"
    );
    let delivered = recording.items.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    assert!(matches!(
        &delivered[0],
        ChannelIngress::Event(event)
            if matches!(&event.event, ChannelEventKind::ReadReceipt { message_id, .. } if message_id == "OUT92")
    ));
    let _ = stop.send(true);
}

#[tokio::test]
async fn typed_read_receipts_respect_the_configured_recipient_allowlist() {
    let rig = rig();
    let mut restricted = rig.channel.clone();
    restricted.allowed_contacts = vec!["+19999999999".into()];
    let sink = RecordingIngress::default();
    let (_stop, mut cancellation) = watch::channel(false);
    let row = crate::providers::imessage::db::ReadRow {
        rowid: 1,
        guid: "SENT-DENIED".into(),
        recipient: PEER.into(),
        date_read: 700_000_000_000_000_000,
    };

    restricted
        .dispatch_read_typed(row, &sink, &mut cancellation)
        .await
        .unwrap();

    assert!(sink.items.lock().unwrap().is_empty());
}

#[tokio::test]
async fn typed_ingress_does_not_acknowledge_a_pending_reservation() {
    let rig = rig();
    let row = rig.inbound(3, Some("pending"), false, &[]);
    let key = "imessage:IN3".to_owned();
    let reservation = match rig.channel.native.reserve_seen(key) {
        crate::providers::native::SeenReservationResult::Reserved(reservation) => reservation,
        _ => panic!("expected a new reservation"),
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
