use super::*;
use crate::providers::imessage::fixtures::*;
use std::time::Duration;
use tinychannels_bus::{ChannelEventKind, ContentPart};

fn pair() -> (mpsc::Sender<ChannelMessage>, mpsc::Receiver<ChannelMessage>) {
    mpsc::channel(8)
}

/// A structurally valid PNG-ish payload is not needed: the stub repository
/// accepts any bytes.
const PNG: &[u8] = b"png-bytes";

#[test]
fn placeholder_characters_and_names_are_cleaned() {
    assert_eq!(clean_text(Some("\u{FFFC} hello \u{FFFC}")), "hello");
    assert_eq!(clean_text(None), "");
    assert_eq!(mime_from_name("a.PNG"), "image/png");
    assert_eq!(mime_from_name("voice.caf"), "audio/x-caf");
    assert_eq!(mime_from_name("noext"), "application/octet-stream");
    for (name, mime) in [
        ("a.jpg", "image/jpeg"),
        ("a.gif", "image/gif"),
        ("a.webp", "image/webp"),
        ("a.mp4", "video/mp4"),
        ("a.mov", "video/quicktime"),
        ("a.m4a", "audio/mp4"),
        ("a.mp3", "audio/mpeg"),
        ("a.wav", "audio/wav"),
        ("a.pdf", "application/pdf"),
        ("a.txt", "text/plain"),
    ] {
        assert_eq!(mime_from_name(name), mime, "{name}");
    }
}

#[tokio::test]
async fn text_is_published_on_the_ordered_ingress_path() {
    let rig = rig();
    let (tx, mut rx) = pair();
    let row = rig.inbound(1, Some("hello"), false, &[]);
    assert!(rig.channel.dispatch_row(row, &tx).await);
    let inbound = rig.events.inbound();
    assert_eq!(inbound[0].message_id, "IN1");
    assert_eq!(inbound[0].conversation.id, PEER);
    assert_eq!(inbound[0].conversation.kind, ConversationKind::Dm);
    assert_eq!(inbound[0].sender.id, PEER);
    assert_eq!(
        inbound[0].timestamp_unix_ms,
        db::unix_ms(700_000_000_000_000_000)
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn group_messages_are_addressed_by_chat_guid() {
    let rig = rig();
    let (tx, _rx) = pair();
    let mut row = rig.inbound(2, Some("team"), false, &[]);
    row.group_guid = Some(GROUP.into());
    rig.channel.dispatch_row(row, &tx).await;
    let inbound = rig.events.inbound();
    assert_eq!(inbound[0].conversation.id, GROUP);
    assert_eq!(inbound[0].conversation.kind, ConversationKind::Group);
}

#[tokio::test]
async fn undelivered_text_falls_back_and_blank_or_foreign_rows_are_dropped() {
    let rig = rig();
    let (tx, mut rx) = pair();
    let plain = IMessageChannel::new(vec![PEER.into()]);
    let row = rig.inbound(3, Some("legacy"), false, &[]);
    assert!(plain.dispatch_row(row.clone(), &tx).await);
    let message = rx.recv().await.unwrap();
    assert_eq!(
        (message.content.as_str(), message.id.as_str()),
        ("legacy", "3")
    );
    // The same row again is a duplicate.
    assert!(plain.dispatch_row(row, &tx).await);
    assert!(rx.try_recv().is_err());
    // Blank text, a sender outside the allowlist, and a gone receiver.
    let blank = rig.inbound(4, Some("\u{FFFC}"), false, &[]);
    assert!(plain.dispatch_row(blank, &tx).await);
    let mut stranger = rig.inbound(5, Some("hi"), false, &[]);
    stranger.sender = "+19999999999".into();
    assert!(plain.dispatch_row(stranger, &tx).await);
    assert!(rx.try_recv().is_err());
    drop(rx);
    let late = rig.inbound(6, Some("nobody home"), false, &[]);
    assert!(!plain.dispatch_row(late, &tx).await);
}

#[tokio::test]
async fn an_attachment_only_message_has_no_legacy_fallback() {
    let rig = rig();
    let (tx, mut rx) = pair();
    let file = rig.attachment("a.png", PNG);
    let row = rig.inbound(7, None, false, &[(&file, "image/png")]);
    let plain = IMessageChannel::new(vec!["*".into()]);
    assert!(plain.dispatch_row(row, &tx).await);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn attachments_become_typed_media_from_inside_the_attachments_folder() {
    let rig = rig();
    let (tx, _rx) = pair();
    let inside = rig.attachment("photo.png", PNG);
    let row = rig.inbound(8, Some("look"), false, &[(&inside, "image/png")]);
    rig.channel.dispatch_row(row, &tx).await;
    let inbound = rig.events.inbound();
    assert!(matches!(inbound[0].parts[0], ContentPart::Text { .. }));
    assert!(matches!(inbound[0].parts[1], ContentPart::Image { .. }));
    assert_eq!(rig.media.stored.lock().unwrap()[0].bytes, PNG);

    // A path outside the Messages attachments folder is never read.
    let outside = rig.dir.path().join("secret.png");
    std::fs::write(&outside, PNG).unwrap();
    let row = rig.inbound(9, Some("sneaky"), false, &[(&outside, "image/png")]);
    rig.channel.dispatch_row(row, &tx).await;
    assert_eq!(rig.media.stored.lock().unwrap().len(), 1);
    let sneaky = &rig.events.inbound()[1];
    assert_eq!(sneaky.parts.len(), 1);
}

#[tokio::test]
async fn unreadable_missing_and_oversized_attachments_are_skipped() {
    let rig = rig();
    let (tx, _rx) = pair();
    let missing = rig.dir.path().join("Attachments/gone.png");
    let huge = rig.attachment("huge.png", &vec![0u8; 12 * 1024 * 1024 * 8 + 1]);
    let row = rig.inbound(
        10,
        Some("none usable"),
        false,
        &[(&missing, "image/png"), (&huge, "image/png")],
    );
    rig.channel.dispatch_row(row, &tx).await;
    assert!(rig.media.stored.lock().unwrap().is_empty());
    assert_eq!(rig.events.inbound()[0].parts.len(), 1);
    // Without any storage no attachment is even opened.
    let bare = IMessageChannel::new(vec!["*".into()]).with_events(rig.events.clone());
    let file = rig.attachment("x.png", PNG);
    let row = rig.inbound(11, Some("t"), false, &[(&file, "image/png")]);
    bare.dispatch_row(row, &tx).await;
    assert_eq!(rig.events.inbound()[1].parts.len(), 1);
}

#[tokio::test]
async fn an_audio_message_is_converted_stored_and_transcribed() {
    let rig = rig();
    let (tx, _rx) = pair();
    let caf = rig.attachment("Audio Message.caf", b"\0\0\0\x20ftypM4A voice");
    let row = rig.inbound(12, None, true, &[(&caf, "audio/x-caf")]);
    rig.channel.dispatch_row(row, &tx).await;
    let inbound = rig.events.inbound();
    let Some(ContentPart::VoiceNote { voice_note }) = inbound[0].parts.first() else {
        panic!("expected a voice note: {:?}", inbound[0].parts);
    };
    assert_eq!(voice_note.codec.as_deref(), Some("m4a_aac"));
    assert_eq!(voice_note.transcript.as_ref().unwrap().text, "hello");
    let stored = rig.voice.stored.lock().unwrap();
    assert_eq!(stored[0].mime_type, "audio/mp4");
}

#[tokio::test]
async fn a_failing_converter_drops_the_audio_but_keeps_the_message() {
    let mut rig = rig();
    rig.channel.converter_command = vec!["sh".into(), "-c".into(), "exit 1".into(), "x".into()];
    let (tx, _rx) = pair();
    let caf = rig.attachment("a.caf", b"caf");
    let row = rig.inbound(13, Some("with text"), true, &[(&caf, "audio/x-caf")]);
    rig.channel.dispatch_row(row, &tx).await;
    assert_eq!(rig.events.inbound()[0].parts.len(), 1);
    rig.channel.converter_command = Vec::new();
    let row = rig.inbound(14, Some("no converter"), true, &[(&caf, "audio/x-caf")]);
    rig.channel.dispatch_row(row, &tx).await;
    assert_eq!(rig.events.inbound()[1].parts.len(), 1);
}

#[tokio::test]
async fn attachments_without_a_mime_type_are_typed_by_extension() {
    let rig = rig();
    let (tx, _rx) = pair();
    let file = rig.attachment("clip.mp4", b"video");
    let mut row = rig.inbound(15, None, false, &[(&file, "")]);
    row.attachments[0].mime_type = None;
    rig.channel.dispatch_row(row, &tx).await;
    assert!(matches!(
        rig.events.inbound()[0].parts[0],
        ContentPart::Video { .. }
    ));
}

#[tokio::test]
async fn tapbacks_become_reaction_events_once() {
    let rig = rig();
    let (tx, _rx) = pair();
    let mut like = rig.inbound(16, Some("Liked a message"), false, &[]);
    like.associated_type = 2001;
    like.associated_guid = Some("p:0/TARGET".into());
    rig.channel.dispatch_row(like.clone(), &tx).await;
    rig.channel.dispatch_row(like, &tx).await;
    let mut unlike = rig.inbound(17, Some("Removed a like"), false, &[]);
    unlike.associated_type = 3001;
    unlike.associated_guid = Some("bp:TARGET".into());
    rig.channel.dispatch_row(unlike, &tx).await;
    let mut orphan = rig.inbound(18, Some("x"), false, &[]);
    orphan.associated_type = 2000;
    orphan.associated_guid = None;
    rig.channel.dispatch_row(orphan, &tx).await;
    let events = rig.events.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0].event,
        ChannelEventKind::ReactionAdded { message_id, reaction, .. }
            if message_id == "TARGET" && reaction.value == "👍"
    ));
    assert!(matches!(
        &events[1].event,
        ChannelEventKind::ReactionRemoved { .. }
    ));
    assert!(rig.events.inbound().is_empty());
}

#[tokio::test]
async fn read_state_of_sent_messages_becomes_read_receipts() {
    let rig = rig();
    let row = db::ReadRow {
        rowid: 1,
        guid: "SENT9".into(),
        recipient: PEER.into(),
        date_read: 700_000_000_000_000_000,
    };
    rig.channel.dispatch_read(row.clone()).await;
    rig.channel.dispatch_read(row.clone()).await;
    let blocked = IMessageChannel::new(vec!["+10000000000".into()]).with_events(rig.events.clone());
    blocked.dispatch_read(row).await;
    let events = rig.events.events();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].event,
        ChannelEventKind::ReadReceipt { message_id, .. } if message_id == "SENT9"
    ));
}

#[test]
fn attachment_paths_must_resolve_inside_the_attachments_folder() {
    let rig = rig();
    let inside = rig.attachment("ok.png", PNG);
    assert_eq!(
        rig.channel
            .resolve_attachment_path(&inside.to_string_lossy())
            .unwrap(),
        inside.canonicalize().unwrap()
    );
    assert!(
        rig.channel
            .resolve_attachment_path("/etc/hostname")
            .is_none()
    );
    assert!(
        rig.channel
            .resolve_attachment_path("~/does/not/exist")
            .is_none()
    );
    let traversal = format!(
        "{}/../chat.db",
        rig.dir.path().join("Attachments").display()
    );
    assert!(rig.channel.resolve_attachment_path(&traversal).is_none());
}

#[tokio::test]
async fn listen_publishes_rich_messages_and_read_receipts_end_to_end() {
    let rig = rig();
    let conn = rig.conn();
    conn.execute(
        "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date) VALUES \
         (1, 'OLD', 1, 'before startup', 0, 1), (2, 'OLDSENT', 1, 'mine', 1, 1)",
        [],
    )
    .unwrap();
    let channel = rig.channel.clone();
    let (tx, _rx) = pair();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let listen = tokio::spawn(async move { channel.listen_with_ready(tx, Some(ready_tx)).await });
    ready_rx
        .await
        .expect("listener reports readiness after capturing its startup cursor");
    conn.execute(
        "INSERT INTO message (ROWID, guid, handle_id, text, is_from_me, date) \
         VALUES (3, 'NEW', 1, 'fresh', 0, 700000000000000000)",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE message SET date_read = 700000000000000000 WHERE ROWID = 2",
        [],
    )
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while (rig.events.inbound().is_empty() || rig.events.events().is_empty())
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    listen.abort();
    let inbound = rig.events.inbound();
    assert_eq!(
        inbound
            .first()
            .expect("listener publishes the post-startup message")
            .message_id,
        "NEW"
    );
    let events = rig.events.events();
    assert!(matches!(
        events
            .first()
            .expect("listener publishes the post-startup read receipt")
            .event,
        ChannelEventKind::ReadReceipt { .. }
    ));
}
