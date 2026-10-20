use super::*;
use crate::channel::{
    ChannelAdapter, ChannelDelete, ChannelEdit, ChannelReaction, ChannelStreamingDraft,
    ChannelTypedSend, ChannelTyping,
};
use crate::providers::imessage::fixtures::*;
use crate::providers::native::fixtures::stored_media;
use crate::traits::Channel;
use tinychannels_bus::channel::{ChannelSendOutcome, MessageReceipt, SendErrorKind};
use tinychannels_bus::{
    ChannelOutboundIntent, ChannelOutboundMessage, ContentPart, ConversationKind, ConversationRef,
    MediaKind, MediaReference, OutboundContentPart, OutboundPayload, OutboundVoice,
};

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
