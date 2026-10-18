use super::*;
use crate::providers::slack::fixtures::*;
use tinychannels_bus::ConversationKind;
use tinychannels_bus::channel::{ChannelRef, SendErrorKind};

fn conversation() -> ConversationRef {
    ConversationRef {
        kind: ConversationKind::Channel,
        id: "C1".into(),
        ..Default::default()
    }
}

fn receipt(id: &str) -> MessageReceipt {
    MessageReceipt {
        channel: Some(ChannelRef {
            id: "slack".into(),
            account_id: None,
        }),
        conversation: Some(conversation()),
        primary_platform_message_id: Some(id.into()),
        platform_message_ids: vec![id.into()],
        ..Default::default()
    }
}

fn intent(text: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        channel_id: "slack".into(),
        conversation_id: "C1".into(),
        channel_ref: Some(ChannelRef {
            id: "slack".into(),
            account_id: None,
        }),
        conversation_ref: Some(conversation()),
        payload: tinychannels_bus::OutboundPayload::Text { text: text.into() },
        ..Default::default()
    }
}

fn reaction(value: &str) -> ChannelReaction {
    ChannelReaction {
        value: value.into(),
        custom_emoji_id: None,
    }
}

#[tokio::test]
async fn reactions_use_slack_names_and_treat_repeats_as_success() {
    let server = mock_api().await;
    mount(&server, "reactions.add", ok(json!({}))).await;
    mount(
        &server,
        "reactions.remove",
        json!({"ok": false, "error": "no_reaction"}),
    )
    .await;
    let channel = channel_for(&server, &[]);
    channel
        .send_reaction_v2(&conversation(), "1.1", &reaction("👍"), false)
        .await
        .unwrap();
    channel
        .send_reaction_v2(&conversation(), "1.1", &reaction(":tada:"), true)
        .await
        .unwrap();
    let added = &bodies(&server, "reactions.add").await[0];
    assert_eq!(
        (
            added["channel"].as_str(),
            added["timestamp"].as_str(),
            added["name"].as_str()
        ),
        (Some("C1"), Some("1.1"), Some("thumbsup"))
    );
    assert_eq!(bodies(&server, "reactions.remove").await[0]["name"], "tada");

    let again = mock_api().await;
    mount(
        &again,
        "reactions.add",
        json!({"ok": false, "error": "already_reacted"}),
    )
    .await;
    channel_for(&again, &[])
        .send_reaction_v2(&conversation(), "1.1", &reaction("eyes"), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn reactions_reject_unaddressable_requests_and_surface_slack_errors() {
    let server = mock_api().await;
    mount(
        &server,
        "reactions.add",
        json!({"ok": false, "error": "message_not_found"}),
    )
    .await;
    let channel = channel_for(&server, &[]);
    for (conversation, id, value) in [
        (conversation(), "1.1", "🤷"),
        (conversation(), "", "👍"),
        (ConversationRef::default(), "1.1", "👍"),
    ] {
        let error = channel
            .send_reaction_v2(&conversation, id, &reaction(value), false)
            .await
            .unwrap_err();
        assert!(!error.sent_before_error);
    }
    assert!(server.received_requests().await.unwrap().is_empty());
    let error = channel
        .send_reaction_v2(&conversation(), "1.1", &reaction("👍"), false)
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::NotFound);
}

#[tokio::test]
async fn edits_update_the_message_text_and_keep_the_receipt() {
    let server = mock_api().await;
    mount(&server, "chat.update", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    let original = receipt("1.1");
    let edited = channel
        .edit_message_v2(&original, &intent("**new** text"))
        .await
        .unwrap();
    assert_eq!(edited, original);
    let body = &bodies(&server, "chat.update").await[0];
    assert_eq!(
        (
            body["channel"].as_str(),
            body["ts"].as_str(),
            body["text"].as_str()
        ),
        (Some("C1"), Some("1.1"), Some("*new* text"))
    );
}

#[tokio::test]
async fn edits_that_do_not_match_their_receipt_are_rejected_without_a_request() {
    let server = mock_api().await;
    mount(&server, "chat.update", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    let mut foreign = receipt("1.1");
    foreign.channel = Some(ChannelRef {
        id: "telegram".into(),
        account_id: None,
    });
    let mut no_conversation = receipt("1.1");
    no_conversation.conversation = None;
    let mut no_id = receipt("1.1");
    no_id.primary_platform_message_id = None;
    let mut other_intent_channel = intent("x");
    other_intent_channel.channel_ref = Some(ChannelRef {
        id: "discord".into(),
        account_id: None,
    });
    let mut other_conversation = intent("x");
    other_conversation.conversation_id = "C9".into();
    let mut media = intent("x");
    media.payload = tinychannels_bus::OutboundPayload::Media {
        text: None,
        media_urls: vec!["https://x.test/a.png".into()],
    };
    let cases = vec![
        (foreign, intent("x")),
        (no_conversation, intent("x")),
        (no_id, intent("x")),
        (receipt("1.1"), other_intent_channel),
        (receipt("1.1"), other_conversation),
        (receipt("1.1"), media),
        (receipt("1.1"), intent("  ")),
        (receipt("1.1"), intent(&"x".repeat(4000))),
    ];
    for (receipt, intent) in cases {
        assert!(channel.edit_message_v2(&receipt, &intent).await.is_err());
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_edit_can_address_the_first_part_when_no_primary_id_is_set() {
    let server = mock_api().await;
    mount(
        &server,
        "chat.update",
        json!({"ok": false, "error": "edit_window_closed"}),
    )
    .await;
    let channel = channel_for(&server, &[]);
    let mut by_part = receipt("1.1");
    by_part.primary_platform_message_id = None;
    by_part.parts = vec![tinychannels_bus::channel::MessageReceiptPart {
        platform_message_id: "2.2".into(),
        ..Default::default()
    }];
    let error = channel
        .edit_message_v2(&by_part, &intent("x"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Forbidden);
    assert_eq!(bodies(&server, "chat.update").await[0]["ts"], "2.2");
}

#[tokio::test]
async fn deletes_remove_messages_by_timestamp_and_files_by_id() {
    let server = mock_api().await;
    mount(&server, "chat.delete", ok(json!({}))).await;
    mount(&server, "files.delete", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    channel.delete_message_v2(&receipt("1.1")).await.unwrap();
    channel.delete_message_v2(&receipt("F123")).await.unwrap();
    assert_eq!(bodies(&server, "chat.delete").await[0]["ts"], "1.1");
    assert_eq!(bodies(&server, "files.delete").await[0]["file"], "F123");

    let denied = mock_api().await;
    mount(
        &denied,
        "chat.delete",
        json!({"ok": false, "error": "cant_delete_message"}),
    )
    .await;
    let error = channel_for(&denied, &[])
        .delete_message_v2(&receipt("1.1"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Forbidden);
    let mut foreign = receipt("1.1");
    foreign.channel = Some(ChannelRef {
        id: "telegram".into(),
        account_id: None,
    });
    assert!(channel.delete_message_v2(&foreign).await.is_err());
}

#[tokio::test]
async fn typing_sets_and_clears_the_assistant_thread_status() {
    let server = mock_api().await;
    mount(&server, "assistant.threads.setStatus", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    channel
        .set_typing_status("C1:1.0", "is typing…")
        .await
        .unwrap();
    channel.set_typing_status("C1:1.0", "").await.unwrap();
    let calls = bodies(&server, "assistant.threads.setStatus").await;
    assert_eq!(
        (
            calls[0]["channel_id"].as_str(),
            calls[0]["thread_ts"].as_str(),
            calls[0]["status"].as_str()
        ),
        (Some("C1"), Some("1.0"), Some("is typing…"))
    );
    assert_eq!(calls[1]["status"], "");
    for recipient in ["C1", ":1.0", "C1:"] {
        let error = channel.set_typing_status(recipient, "x").await.unwrap_err();
        assert!(error.unsupported, "{recipient}");
    }
    let refused = mock_api().await;
    mount(
        &refused,
        "assistant.threads.setStatus",
        json!({"ok": false, "error": "missing_scope"}),
    )
    .await;
    assert!(
        channel_for(&refused, &[])
            .set_typing_status("C1:1.0", "x")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn drafts_post_once_then_update_in_place() {
    let server = mock_api().await;
    mount(&server, "chat.postMessage", ok(json!({"ts": "5.5"}))).await;
    mount(&server, "chat.update", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    let mut first = intent("draft");
    first.thread_id = Some("1.0".into());
    let created = channel.upsert_draft_v2(None, first).await.unwrap();
    assert_eq!(created.primary_platform_message_id.as_deref(), Some("5.5"));
    assert_eq!(
        (
            created.edit_token.as_deref(),
            created.delete_token.as_deref()
        ),
        (Some("5.5"), Some("5.5"))
    );
    assert_eq!(created.thread_id.as_deref(), Some("1.0"));
    assert_eq!(
        bodies(&server, "chat.postMessage").await[0]["thread_ts"],
        "1.0"
    );
    let updated = channel
        .upsert_draft_v2(Some(&created), intent("draft, longer"))
        .await
        .unwrap();
    assert_eq!(updated, created);
    assert_eq!(
        bodies(&server, "chat.update").await[0]["text"],
        "draft, longer"
    );
}

#[tokio::test]
async fn drafts_reject_non_text_and_report_post_failures() {
    let server = mock_api().await;
    mount(&server, "chat.postMessage", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    assert!(channel.upsert_draft_v2(None, intent(" ")).await.is_err());
    let error = channel
        .upsert_draft_v2(None, intent("x"))
        .await
        .unwrap_err();
    assert!(
        error.sent_before_error,
        "a post without a timestamp is uncertain"
    );
    let refused = mock_api().await;
    mount(
        &refused,
        "chat.postMessage",
        json!({"ok": false, "error": "channel_not_found"}),
    )
    .await;
    assert_eq!(
        channel_for(&refused, &[])
            .upsert_draft_v2(None, intent("x"))
            .await
            .unwrap_err()
            .kind,
        SendErrorKind::NotFound
    );
    let mut no_target = intent("x");
    no_target.conversation_id = String::new();
    no_target.conversation_ref = None;
    assert!(channel.upsert_draft_v2(None, no_target).await.is_err());
}

#[tokio::test]
async fn legacy_draft_edits_validate_their_text() {
    let server = mock_api().await;
    mount(&server, "chat.update", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    edit_text_for_legacy(&channel, "C1", "1.1", "fine <tool>x</tool>")
        .await
        .unwrap();
    assert!(
        edit_text_for_legacy(&channel, "C1", "1.1", "   ")
            .await
            .is_err()
    );
    assert_eq!(bodies(&server, "chat.update").await.len(), 1);
}
