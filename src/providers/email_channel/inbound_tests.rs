use super::*;
use crate::config::EmailConfig;
use crate::providers::email_channel::fixtures::*;
use tinychannels_bus::{ContentPart, ConversationKind};

fn plain_channel() -> EmailChannel {
    EmailChannel::new(EmailConfig {
        allowed_senders: vec!["*".into()],
        ..Default::default()
    })
}

#[tokio::test]
async fn a_message_is_published_with_subject_and_thread_headers() {
    let smtp = smtp(false).await;
    let rig = rig(&smtp);
    let raw = raw_mail(
        "Subject: Budget\r\nMessage-ID: <m3@x>\r\nIn-Reply-To: <m2@x>\r\nReferences: <root@x> <m2@x>\r\n",
        "Numbers attached soon.\r\n",
    );
    let (alive, legacy) = dispatch(&rig.channel, "m3@x", raw).await;
    assert!(alive && legacy.is_empty());
    let inbound = rig.events.inbound();
    assert_eq!(inbound[0].message_id, "m3@x");
    assert_eq!(inbound[0].reply_to_id.as_deref(), Some("m2@x"));
    assert_eq!(inbound[0].conversation.id, PEER);
    assert_eq!(inbound[0].conversation.kind, ConversationKind::Dm);
    assert_eq!(inbound[0].conversation.thread_id.as_deref(), Some("root@x"));
    assert_eq!(inbound[0].sender.id, PEER);
    assert!(
        inbound[0]
            .text
            .starts_with("Subject: Budget\n\nNumbers attached soon.")
    );
    assert_eq!(inbound[0].timestamp_unix_ms, Some(1_736_942_400_000));
    assert_eq!(inbound[0].raw.as_ref().unwrap()["subject"], "Budget");
    assert_eq!(rig.channel.subjects.get("m3@x").as_deref(), Some("Budget"));
}

#[tokio::test]
async fn an_unthreaded_message_roots_its_own_thread() {
    let smtp = smtp(false).await;
    let rig = rig(&smtp);
    let (_, _) = dispatch(
        &rig.channel,
        "solo@x",
        raw_mail("Subject: Hi\r\n", "yo\r\n"),
    )
    .await;
    assert_eq!(
        rig.events.inbound()[0].conversation.thread_id.as_deref(),
        Some("solo@x")
    );
    // Only an In-Reply-To names the root when there are no References.
    let (_, _) = dispatch(
        &rig.channel,
        "r@x",
        raw_mail("Subject: Re: Hi\r\nIn-Reply-To: <solo@x>\r\n", "again\r\n"),
    )
    .await;
    assert_eq!(
        rig.events.inbound()[1].conversation.thread_id.as_deref(),
        Some("solo@x")
    );
}

#[tokio::test]
async fn attachments_are_stored_and_audio_becomes_a_transcribed_voice_note() {
    let smtp = smtp(false).await;
    let rig = rig(&smtp);
    let image = raw_with_attachment(
        "Subject: Pic\r\n",
        "look",
        "image/png",
        "a.png",
        b"png-bytes",
    );
    dispatch(&rig.channel, "img@x", image).await;
    let voice = raw_with_attachment(
        "Subject: Voice\r\n",
        "",
        "audio/mp4",
        "memo.m4a",
        b"\0\0\0\x20ftypM4A voice",
    );
    dispatch(&rig.channel, "voice@x", voice).await;
    let inbound = rig.events.inbound();
    assert!(matches!(inbound[0].parts[1], ContentPart::Image { .. }));
    let Some(ContentPart::VoiceNote { voice_note }) = inbound[1].parts.get(1) else {
        panic!("expected a voice note: {:?}", inbound[1].parts);
    };
    assert_eq!(voice_note.transcript.as_ref().unwrap().text, "hello");
    assert!(inbound[1].text.starts_with("Subject: Voice"));
    assert_eq!(rig.voice.stored.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn without_a_sink_or_when_the_host_declines_the_legacy_channel_gets_the_text() {
    let smtp = smtp(false).await;
    let raw = raw_mail("Subject: Hello\r\n", "legacy body\r\n");
    let (alive, legacy) = dispatch(&plain_channel(), "l1@x", raw.clone()).await;
    assert!(alive);
    assert_eq!(legacy[0].id, "l1@x");
    assert_eq!(legacy[0].sender, PEER);
    assert!(legacy[0].content.contains("legacy body"));

    let mut rig = rig(&smtp);
    let declining =
        std::sync::Arc::new(crate::providers::native::fixtures::RecordingEvents::default());
    rig.channel = plain_channel().with_events(declining);
    let (_, legacy) = dispatch(&rig.channel, "l2@x", raw).await;
    assert_eq!(legacy.len(), 1);
}

#[tokio::test]
async fn a_closed_receiver_stops_the_listener() {
    let channel = plain_channel();
    let raw = raw_mail("Subject: Hello\r\n", "x\r\n");
    let parsed = MessageParser::default().parse(&raw).unwrap();
    let email = ParsedEmail {
        _uid: 1,
        msg_id: "c@x".into(),
        sender: EmailChannel::extract_sender(&parsed),
        content: "Subject: Hello\n\nx".into(),
        timestamp: 1,
        raw,
    };
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    assert!(!channel.dispatch_email(email, &tx).await);
}

#[tokio::test]
async fn unparseable_mail_falls_back_to_the_legacy_channel() {
    let smtp = smtp(false).await;
    let rig = rig(&smtp);
    let email = ParsedEmail {
        _uid: 1,
        msg_id: "bad@x".into(),
        sender: PEER.into(),
        content: "Subject: x\n\ny".into(),
        timestamp: 1,
        raw: Vec::new(),
    };
    let (tx, mut rx) = mpsc::channel(2);
    assert!(rig.channel.dispatch_email(email, &tx).await);
    assert_eq!(rx.recv().await.unwrap().id, "bad@x");
}
