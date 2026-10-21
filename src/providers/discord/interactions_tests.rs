use super::*;
use crate::channel::{ChannelDelete, ChannelEdit, ChannelStreamingDraft};
use crate::providers::discord::support::*;
use serde_json::json;
use tinychannels_bus::channel::{MessageReceiptPart, MessageReceiptPartKind};
use tinychannels_bus::{ChannelRef, ConversationKind};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn receipt(ids: &[&str]) -> MessageReceipt {
    MessageReceipt {
        channel: Some(account()),
        conversation: Some(conversation()),
        primary_platform_message_id: ids.first().map(|id| (*id).to_owned()),
        platform_message_ids: ids.iter().map(|id| (*id).to_owned()).collect(),
        parts: ids
            .iter()
            .enumerate()
            .map(|(index, id)| MessageReceiptPart {
                platform_message_id: (*id).to_owned(),
                kind: MessageReceiptPartKind::Text,
                index,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn intent(text: &str) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        idempotency_key: "k".into(),
        channel_id: "discord".into(),
        conversation_id: CHANNEL.into(),
        channel_ref: Some(account()),
        conversation_ref: Some(conversation()),
        turn_id: Some("turn-1".into()),
        payload: OutboundPayload::Text { text: text.into() },
        ..Default::default()
    }
}

fn parts_intent(parts: Vec<OutboundContentPart>) -> ChannelOutboundIntent {
    ChannelOutboundIntent {
        payload: OutboundPayload::Parts { parts },
        ..intent("")
    }
}

#[test]
fn intent_text_accepts_only_plain_text() {
    assert_eq!(
        intent_text(&OutboundPayload::Text { text: "a".into() }).as_deref(),
        Some("a")
    );
    let two = OutboundPayload::Parts {
        parts: vec![text_part("a"), text_part("b")],
    };
    assert_eq!(intent_text(&two).as_deref(), Some("a\nb"));
    assert_eq!(intent_text(&OutboundPayload::Parts { parts: vec![] }), None);
    let mixed = OutboundPayload::Parts {
        parts: vec![content(ContentPart::Buttons { rows: vec![] })],
    };
    assert_eq!(intent_text(&mixed), None);
    assert_eq!(
        intent_text(&OutboundPayload::Poll {
            question: "q".into(),
            options: vec![]
        }),
        None
    );
}

#[test]
fn receipt_targets_prefer_part_then_receipt_then_conversation_threads() {
    let mut r = receipt(&["10", "11"]);
    r.parts[0].thread_id = Some("80".into());
    r.thread_id = Some("90".into());
    let targets = part_targets(&r).unwrap();
    assert_eq!(
        targets,
        vec![("80".into(), "10".into()), ("90".into(), "11".into())]
    );

    let mut by_conversation = receipt(&["10"]);
    by_conversation.conversation = Some(thread_conversation());
    assert_eq!(part_targets(&by_conversation).unwrap()[0].0, THREAD);

    let mut primary_only = receipt(&[]);
    primary_only.primary_platform_message_id = Some("77".into());
    primary_only.thread_id = Some("90".into());
    assert_eq!(
        part_targets(&primary_only).unwrap(),
        vec![("90".into(), "77".into())]
    );
}

#[test]
fn receipt_targets_reject_foreign_unaddressed_and_non_numeric_receipts() {
    let mut foreign = receipt(&["10"]);
    foreign.channel = Some(ChannelRef {
        id: "slack".into(),
        account_id: None,
    });
    assert!(
        part_targets(&foreign)
            .unwrap_err()
            .message
            .contains("Discord")
    );

    let mut unscoped = receipt(&["10"]);
    unscoped.conversation = None;
    assert!(part_targets(&unscoped).is_err());
    assert!(
        part_targets(&receipt(&[]))
            .unwrap_err()
            .message
            .contains("no provider message id")
    );
    assert!(
        part_targets(&receipt(&["x1"]))
            .unwrap_err()
            .message
            .contains("numeric")
    );
    let mut bad_primary = receipt(&[]);
    bad_primary.primary_platform_message_id = Some("zz".into());
    assert!(part_targets(&bad_primary).is_err());
}

#[test]
fn emoji_segments_cover_unicode_and_custom_emoji() {
    let unicode = ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    assert_eq!(emoji_segment(&unicode).unwrap(), "👍");
    let custom = ChannelReaction {
        value: ":party:".into(),
        custom_emoji_id: Some("123".into()),
    };
    assert_eq!(emoji_segment(&custom).unwrap(), "party:123");
    assert!(
        emoji_segment(&ChannelReaction {
            value: " ".into(),
            custom_emoji_id: None
        })
        .is_err()
    );
    assert!(
        emoji_segment(&ChannelReaction {
            value: "x".into(),
            custom_emoji_id: Some("a".into())
        })
        .is_err()
    );
    assert!(
        emoji_segment(&ChannelReaction {
            value: "".into(),
            custom_emoji_id: Some("1".into())
        })
        .is_err()
    );
}

// ── edit ──────────────────────────────────────────────────────

#[tokio::test]
async fn edit_patches_the_message_content_and_returns_the_receipt() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path(format!("/channels/{CHANNEL}/messages/10")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "10"})))
        .mount(&server)
        .await;
    let original = receipt(&["10"]);

    let channel = channel_for(&server);
    let edited = channel.edit(&original, intent("new text")).await.unwrap();

    assert_eq!(edited, original);
    let sent = requests(&server, "PATCH").await;
    assert_eq!(body_json(&sent[0])["content"], "new text");
    assert_eq!(
        body_json(&sent[0])["allowed_mentions"],
        json!({"parse": ["users"]})
    );
}

#[tokio::test]
async fn edit_rejects_mismatched_scope_oversize_and_non_text_before_dispatch() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let original = receipt(&["10"]);

    let mut other_conversation = intent("x");
    other_conversation.conversation_id = "42".into();
    assert!(
        channel
            .edit(&original, other_conversation)
            .await
            .unwrap_err()
            .message
            .contains("scope")
    );

    let mut other_ref = intent("x");
    other_ref.conversation_ref = Some(ConversationRef {
        id: CHANNEL.into(),
        kind: ConversationKind::Dm,
        ..Default::default()
    });
    assert!(channel.edit(&original, other_ref).await.is_err());

    let mut other_account = intent("x");
    other_account.channel_ref = Some(ChannelRef {
        id: "discord".into(),
        account_id: Some("other".into()),
    });
    assert!(channel.edit(&original, other_account).await.is_err());

    let too_long = channel
        .edit(&original, intent(&"a".repeat(2001)))
        .await
        .unwrap_err();
    assert_eq!(too_long.kind, SendErrorKind::TooLong);

    let buttons = parts_intent(vec![content(ContentPart::Buttons { rows: vec![] })]);
    assert!(
        channel
            .edit(&original, buttons)
            .await
            .unwrap_err()
            .message
            .contains("plain text")
    );

    let mut unscoped = receipt(&["10"]);
    unscoped.conversation = None;
    assert!(channel.edit(&unscoped, intent("x")).await.is_err());
    assert!(requests(&server, "PATCH").await.is_empty());
}

// ── delete ────────────────────────────────────────────────────

#[tokio::test]
async fn delete_removes_every_message_of_the_receipt_and_tolerates_missing_ones() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(format!("/channels/{CHANNEL}/messages/10")))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(format!("/channels/{CHANNEL}/messages/11")))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"code": 10008})))
        .mount(&server)
        .await;

    channel_for(&server)
        .delete(&receipt(&["10", "11"]))
        .await
        .unwrap();

    assert_eq!(requests(&server, "DELETE").await.len(), 2);
}

#[tokio::test]
async fn delete_surfaces_real_failures() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({"code": 50013})))
        .mount(&server)
        .await;

    let channel = channel_for(&server);
    let error = channel.delete(&receipt(&["10"])).await.unwrap_err();

    assert_eq!(error.kind, SendErrorKind::Forbidden);
    assert!(channel.delete(&receipt(&[])).await.is_err());
}

// ── reactions and typing ──────────────────────────────────────

#[tokio::test]
async fn reactions_use_put_to_add_and_delete_to_remove() {
    let server = MockServer::start().await;
    Mock::given(path_regex(r"^/channels/\d+/messages/\d+/reactions/.+/@me$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let thumbs = ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };

    channel
        .send_reaction_v2(&conversation(), "42", &thumbs, false)
        .await
        .unwrap();
    channel
        .send_reaction_v2(&conversation(), "42", &thumbs, true)
        .await
        .unwrap();
    let custom = ChannelReaction {
        value: "party".into(),
        custom_emoji_id: Some("555".into()),
    };
    channel
        .send_reaction_v2(&thread_conversation(), "43", &custom, false)
        .await
        .unwrap();

    let all = server.received_requests().await.unwrap();
    assert_eq!(all[0].method.to_string(), "PUT");
    assert_eq!(
        all[0].url.path(),
        format!("/channels/{CHANNEL}/messages/42/reactions/%F0%9F%91%8D/@me")
    );
    assert_eq!(all[1].method.to_string(), "DELETE");
    assert_eq!(
        all[2].url.path(),
        format!("/channels/{THREAD}/messages/43/reactions/party:555/@me")
    );
}

#[tokio::test]
async fn reactions_reject_non_numeric_targets_and_empty_emoji() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let ok = ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    assert!(
        channel
            .send_reaction_v2(&conversation(), "x", &ok, false)
            .await
            .is_err()
    );
    let bad_channel = ConversationRef {
        id: "c".into(),
        ..conversation()
    };
    assert!(
        channel
            .send_reaction_v2(&bad_channel, "1", &ok, false)
            .await
            .is_err()
    );
    let empty = ChannelReaction::default();
    assert!(
        channel
            .send_reaction_v2(&conversation(), "1", &empty, false)
            .await
            .is_err()
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typing_posts_the_indicator_once() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/channels/{CHANNEL}/typing")))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let channel = channel_for(&server);

    channel.send_typing_once(CHANNEL).await.unwrap();

    assert_eq!(requests(&server, "POST").await.len(), 1);
    assert!(channel.send_typing_once("../x").await.is_err());
}

// ── drafts ────────────────────────────────────────────────────

#[tokio::test]
async fn a_draft_is_created_then_edited_in_place_and_clipped() {
    let server = MockServer::start().await;
    mount_message_post(&server, 300).await;
    Mock::given(method("PATCH"))
        .and(path(format!("/channels/{CHANNEL}/messages/300")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "300"})))
        .mount(&server)
        .await;
    let channel = channel_for(&server);

    let created = channel
        .upsert_draft_v2(None, intent("start"))
        .await
        .unwrap();
    assert_eq!(created.platform_message_ids, vec!["300"]);
    let updated = channel
        .upsert_draft_v2(Some(&created), intent(&"long ".repeat(700)))
        .await
        .unwrap();
    assert_eq!(updated, created);

    let patch = requests(&server, "PATCH").await;
    let shown = body_json(&patch[0])["content"].as_str().unwrap().to_owned();
    assert!(shown.encode_utf16().count() <= 2000 && shown.starts_with("long"));
}

#[tokio::test]
async fn invalid_drafts_are_rejected_before_dispatch() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let buttons = parts_intent(vec![content(ContentPart::Buttons { rows: vec![] })]);
    assert!(
        channel
            .upsert_draft_v2(None, buttons)
            .await
            .unwrap_err()
            .message
            .contains("text")
    );
    assert!(
        channel
            .upsert_draft_v2(None, intent("  "))
            .await
            .unwrap_err()
            .message
            .contains("requires text")
    );

    let mut unscoped = receipt(&["10"]);
    unscoped.conversation = None;
    assert!(
        channel
            .upsert_draft_v2(Some(&unscoped), intent("x"))
            .await
            .is_err()
    );
    let mut elsewhere = intent("x");
    elsewhere.conversation_id = "99".into();
    assert!(
        channel
            .upsert_draft_v2(Some(&receipt(&["10"])), elsewhere)
            .await
            .unwrap_err()
            .message
            .contains("scope")
    );
    let mut bad_target = intent("x");
    bad_target.conversation_id = "nope".into();
    bad_target.conversation_ref = None;
    assert!(channel.upsert_draft_v2(None, bad_target).await.is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_failed_draft_create_is_an_error_not_a_receipt() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let error = channel_for(&server)
        .upsert_draft_v2(None, intent("x"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Forbidden);
}

#[tokio::test]
async fn typed_drafts_create_update_and_clip_to_provider_limit() {
    let server = MockServer::start().await;
    mount_message_post(&server, 400).await;
    Mock::given(method("PATCH"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "400"})))
        .mount(&server)
        .await;
    let channel = channel_for(&server);

    let created = ChannelStreamingDraft::upsert_draft(&channel, None, intent("draft"))
        .await
        .unwrap();
    assert_eq!(created.primary_platform_message_id.as_deref(), Some("400"));
    assert_eq!(
        body_json(&requests(&server, "POST").await[0])["content"],
        "draft"
    );

    let updated = ChannelStreamingDraft::upsert_draft(&channel, Some(&created), intent("partial"))
        .await
        .unwrap();
    assert_eq!(requests(&server, "PATCH").await.len(), 1);

    let long = "word ".repeat(700);
    ChannelStreamingDraft::upsert_draft(&channel, Some(&updated), intent(&long))
        .await
        .unwrap();
    assert_eq!(requests(&server, "PATCH").await.len(), 2);
    let patch = body_json(&requests(&server, "PATCH").await[1]);
    assert!(patch["content"].as_str().unwrap().len() <= 2000);
    assert_eq!(requests(&server, "POST").await.len(), 1);
    assert!(
        ChannelStreamingDraft::upsert_draft(&channel, Some(&created), intent(" "))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typed_draft_update_errors_preserve_provider_status() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"code": 10008})))
        .mount(&server)
        .await;
    let channel = channel_for(&server);

    let previous = receipt(&["1"]);
    let error = ChannelStreamingDraft::upsert_draft(&channel, Some(&previous), intent("x"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::NotFound);
}
