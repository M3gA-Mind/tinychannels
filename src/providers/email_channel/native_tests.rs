use super::*;
use crate::channel::{ChannelAdapter, ChannelTypedSend, ChannelTyping};
use crate::config::EmailConfig;
use crate::providers::email_channel::fixtures::*;
use crate::providers::native::fixtures::stored_media;
use tinychannels_bus::channel::{ConversationRef, MessageReceiptPartKind};
use tinychannels_bus::{
    ChannelOutboundMessage, ChannelRef, ContentPart, ConversationKind, MediaKind, MediaReference,
    OutboundContentPart, OutboundVoice,
};

fn message(to: &str, parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "email".into(),
        conversation_id: to.into(),
        channel_ref: Some(ChannelRef {
            id: "email".into(),
            account_id: None,
        }),
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

fn text(body: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: body.into(),
            markdown_dialect: None,
        },
    }
}

fn media_ref(kind: MediaKind) -> MediaReference {
    MediaReference {
        opaque_id: Some("media-1".into()),
        kind,
        ..Default::default()
    }
}

async fn send(
    channel: &EmailChannel,
    message: &ChannelOutboundMessage,
) -> Result<tinychannels_bus::channel::ChannelSendOutcome, ChannelSendError> {
    ChannelTypedSend::send_typed(channel, message).await
}

fn only(smtp: &Smtp) -> String {
    let messages = smtp.messages.lock().unwrap();
    assert_eq!(messages.len(), 1, "one reply is one message");
    messages[0].clone()
}

#[tokio::test]
async fn text_is_one_message_whose_message_id_is_the_receipt_id() {
    let smtp = smtp(false).await;
    let channel = EmailChannel::new(config_for(&smtp));
    let outcome = send(&channel, &message(PEER, vec![text("hello there")]))
        .await
        .unwrap();
    let sent = only(&smtp);
    assert_eq!(header(&sent, "To").as_deref(), Some(PEER));
    assert_eq!(
        header(&sent, "Subject").as_deref(),
        Some("OpenHuman Message")
    );
    assert!(sent.contains("hello there"));
    let id = outcome.receipt.primary_platform_message_id.unwrap();
    assert_eq!(header(&sent, "Message-ID"), Some(format!("<{id}>")));
    assert!(id.ends_with("@example.com"));
    assert!(header(&sent, "In-Reply-To").is_none());
}

#[tokio::test]
async fn an_explicit_subject_line_becomes_the_subject() {
    let smtp = smtp(false).await;
    let channel = EmailChannel::new(config_for(&smtp));
    send(
        &channel,
        &message(PEER, vec![text("Subject: Weekly report\nAll green")]),
    )
    .await
    .unwrap();
    let sent = only(&smtp);
    assert_eq!(header(&sent, "Subject").as_deref(), Some("Weekly report"));
    assert!(sent.contains("All green") && !sent.contains("Subject: Weekly report\nAll"));
}

#[tokio::test]
async fn v1_subject_is_sent_as_mail_header_without_leaking_into_body() {
    let smtp = smtp(false).await;
    let channel = EmailChannel::new(config_for(&smtp));
    let mut legacy = crate::traits::SendMessage::new("All green", PEER);
    legacy.subject = Some("Weekly report".into());
    crate::provider_test_support::send_v1(&channel, &legacy)
        .await
        .unwrap();

    let sent = only(&smtp);
    assert_eq!(header(&sent, "Subject").as_deref(), Some("Weekly report"));
    assert!(sent.contains("All green"));
    assert!(!sent.contains("Subject: Weekly report\nAll green"));
}

#[tokio::test]
async fn replies_keep_the_thread_subject_and_headers() {
    let smtp = smtp(false).await;
    let channel = EmailChannel::new(config_for(&smtp));
    channel.subjects.remember("root@x", "Budget");
    let mut reply = message(PEER, vec![text("done")]);
    reply.reply_to_id = Some("<m2@x>".into());
    reply.thread_id = Some("root@x".into());
    send(&channel, &reply).await.unwrap();
    let sent = only(&smtp);
    assert_eq!(header(&sent, "Subject").as_deref(), Some("Re: Budget"));
    assert_eq!(header(&sent, "In-Reply-To").as_deref(), Some("<m2@x>"));
    assert_eq!(
        header(&sent, "References").as_deref(),
        Some("<root@x> <m2@x>")
    );
}

#[tokio::test]
async fn a_thread_without_a_reply_target_answers_the_thread_root() {
    let smtp = smtp(false).await;
    let channel = EmailChannel::new(config_for(&smtp));
    channel.subjects.remember("root@x", "Re: Budget");
    let mut threaded = message(PEER, vec![text("ping")]);
    threaded.thread_id = Some("root@x".into());
    send(&channel, &threaded).await.unwrap();
    let sent = only(&smtp);
    assert_eq!(header(&sent, "Subject").as_deref(), Some("Re: Budget"));
    assert_eq!(header(&sent, "In-Reply-To").as_deref(), Some("<root@x>"));
    assert_eq!(header(&sent, "References").as_deref(), Some("<root@x>"));
}

#[tokio::test]
async fn text_and_a_file_travel_as_one_message_with_the_attachment() {
    let smtp = smtp(false).await;
    let rig = rig(&smtp);
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Image, "image/png", b"png"));
    let outcome = send(
        &rig.channel,
        &message(
            PEER,
            vec![
                text("see attached"),
                OutboundContentPart::Content {
                    part: ContentPart::Image {
                        media: media_ref(MediaKind::Image),
                        caption: Some("the chart".into()),
                        alt_text: None,
                    },
                },
            ],
        ),
    )
    .await
    .unwrap();
    let sent = only(&smtp);
    assert!(sent.contains("see attached") && sent.contains("the chart"));
    assert!(
        sent.to_ascii_lowercase()
            .contains("content-type: image/png")
    );
    assert_eq!(outcome.receipt.parts.len(), 1);
    assert_eq!(outcome.receipt.parts[0].kind, MessageReceiptPartKind::Media);
}

#[tokio::test]
async fn a_voice_clip_is_an_audio_attachment() {
    let smtp = smtp(false).await;
    let rig = rig(&smtp);
    *rig.media.read.lock().unwrap() = Some(stored_media(MediaKind::Audio, "audio/mp4", b"m4a"));
    let outcome = send(
        &rig.channel,
        &message(
            PEER,
            vec![OutboundContentPart::Voice {
                voice: OutboundVoice::Media {
                    media: media_ref(MediaKind::Audio),
                    codec: Some("m4a_aac".into()),
                    duration_ms: None,
                },
            }],
        ),
    )
    .await
    .unwrap();
    assert!(
        only(&smtp)
            .to_ascii_lowercase()
            .contains("content-type: audio/mp4")
    );
    assert_eq!(outcome.receipt.parts[0].kind, MessageReceiptPartKind::Voice);
}

#[tokio::test]
async fn recipients_that_are_not_a_single_address_never_reach_the_relay() {
    let smtp = smtp(false).await;
    let channel = EmailChannel::new(config_for(&smtp));
    for bad in [
        "ada",
        "a@b.c, d@e.f",
        "a@b.c\r\nBcc: x@y.z",
        "Ada <a@b.c>",
        "",
    ] {
        let error = send(&channel, &message(bad, vec![text("x")]))
            .await
            .unwrap_err();
        assert!(!error.retryable && !error.sent_before_error, "{bad:?}");
    }
    assert!(smtp.messages.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_relay_that_refuses_the_message_is_a_retryable_transient_failure() {
    let smtp = smtp(true).await;
    let channel = EmailChannel::new(config_for(&smtp));
    let error = send(&channel, &message(PEER, vec![text("x")]))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Transient);
    assert!(error.retryable && !error.sent_before_error);
    assert!(!error.message.contains(PEER));
}

#[tokio::test]
async fn an_address_the_sender_cannot_parse_is_rejected_before_dialing() {
    let smtp = smtp(false).await;
    let mut config = config_for(&smtp);
    config.from_address = "not an address".into();
    let channel = EmailChannel::new(config);
    let error = send(&channel, &message(PEER, vec![text("x")]))
        .await
        .unwrap_err();
    assert!(!error.retryable && !error.sent_before_error);
    assert!(smtp.messages.lock().unwrap().is_empty());
}

#[test]
fn reply_subjects_do_not_stack_prefixes_and_the_window_is_bounded() {
    assert_eq!(reply_subject("Budget"), "Re: Budget");
    assert_eq!(reply_subject("RE: Budget"), "RE: Budget");
    assert_eq!(bare_id(" <a@b> "), "a@b");
    let subjects = Subjects::default();
    for n in 0..(SUBJECT_WINDOW + 10) {
        subjects.remember(&format!("m{n}"), "s");
    }
    subjects.remember("m20", "updated");
    assert!(subjects.get("m0").is_none());
    assert_eq!(subjects.get("m20").as_deref(), Some("updated"));
    assert_eq!(
        subjects.get(&format!("m{}", SUBJECT_WINDOW + 9)).as_deref(),
        Some("s")
    );
}

#[test]
fn the_message_id_domain_follows_the_from_address() {
    let mut config = EmailConfig {
        from_address: "Bot <bot@mail.example.com>".into(),
        ..Default::default()
    };
    assert_eq!(
        EmailChannel::new(config.clone()).message_id_domain(),
        "mail.example.com"
    );
    config.from_address = "nodomain".into();
    assert_eq!(EmailChannel::new(config).message_id_domain(), "localhost");
}

#[tokio::test]
async fn unsupported_operations_and_the_descriptor_use_the_native_surface() {
    let smtp = smtp(false).await;
    let channel = EmailChannel::new(config_for(&smtp));
    let descriptor = ChannelAdapter::descriptor(&channel);
    assert_eq!(descriptor.id, "email");
    assert_eq!(
        descriptor.metadata.get("voiceOut").map(String::as_str),
        Some("attachment")
    );
    assert!(
        ChannelTyping::start_typing(&channel, PEER)
            .await
            .unwrap_err()
            .unsupported
    );
    assert!(ChannelAdapter::declared_capabilities(&channel).remote_control);
    let outcome = ChannelTypedSend::send_typed(&channel, &message(PEER, vec![text("legacy")]))
        .await
        .unwrap();
    assert!(outcome.receipt.primary_platform_message_id.is_some());
    let step = crate::providers::native::Step::Text("direct".into());
    let target =
        crate::providers::native::outbound::target_of(&message(PEER, vec![]), "email", "Email")
            .unwrap();
    assert_eq!(
        NativeProvider::deliver(&channel, &target, &step)
            .await
            .unwrap()
            .len(),
        1
    );
}
