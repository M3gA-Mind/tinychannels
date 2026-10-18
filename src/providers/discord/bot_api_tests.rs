use super::*;
use crate::providers::discord::support::*;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn configured(server: &MockServer) -> DiscordChannel {
    DiscordChannel::new(
        BOT_TOKEN.into(),
        Some(GUILD.into()),
        Some(CHANNEL.into()),
        vec![],
        false,
        false,
    )
    .with_api_base(server.uri())
}

#[test]
fn thread_actions_parse_aliases_and_reject_unknowns() {
    assert_eq!(
        ThreadAction::parse(" Archive ").unwrap(),
        ThreadAction::Archive
    );
    assert_eq!(ThreadAction::parse("close").unwrap(), ThreadAction::Archive);
    assert_eq!(
        ThreadAction::parse("reopen").unwrap(),
        ThreadAction::Unarchive
    );
    assert_eq!(ThreadAction::parse("lock").unwrap(), ThreadAction::Lock);
    assert_eq!(ThreadAction::parse("unlock").unwrap(), ThreadAction::Unlock);
    assert!(ThreadAction::parse("bogus").unwrap_err().contains("bogus"));
    assert_eq!(ThreadAction::Archive.body(), json!({"archived": true}));
    assert_eq!(ThreadAction::Unarchive.body(), json!({"archived": false}));
    assert_eq!(ThreadAction::Lock.body(), json!({"locked": true}));
    assert_eq!(ThreadAction::Unlock.body(), json!({"locked": false}));
}

#[test]
fn reaction_requests_decode_either_key_spelling() {
    let request = DiscordReactionRequest::from_value(&json!({
        "conversationId": CHANNEL, "messageId": 42, "value": "👍", "remove": true,
        "customEmojiId": "9"
    }))
    .unwrap();
    assert_eq!(request.channel_id, CHANNEL);
    assert_eq!(request.message_id, "42");
    assert_eq!(request.emoji.as_deref(), Some("👍"));
    assert_eq!(request.custom_emoji_id.as_deref(), Some("9"));
    assert!(request.remove);
    assert!(
        !DiscordReactionRequest::from_value(
            &json!({"channel_id": "1", "message_id": "2", "emoji": "x"})
        )
        .unwrap()
        .remove
    );

    for bad in [
        json!({"messageId": "1", "emoji": "x"}),
        json!({"channelId": "1", "emoji": "x"}),
        json!({"channelId": "1", "messageId": "2"}),
        json!({"channelId": " ", "messageId": "2", "emoji": "x"}),
    ] {
        assert!(DiscordReactionRequest::from_value(&bad).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn controller_reactions_add_and_remove_through_the_rest_api() {
    let server = MockServer::start().await;
    for http in ["PUT", "DELETE"] {
        Mock::given(method(http))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
    }
    let channel = configured(&server);
    let request = |remove| DiscordReactionRequest {
        channel_id: CHANNEL.into(),
        message_id: "42".into(),
        emoji: Some("🔥".into()),
        custom_emoji_id: None,
        remove,
    };

    channel.controller_reaction(&request(false)).await.unwrap();
    channel.controller_reaction(&request(true)).await.unwrap();

    let bad = DiscordReactionRequest {
        message_id: "x".into(),
        ..request(false)
    };
    assert!(channel.controller_reaction(&bad).await.is_err());
}

#[tokio::test]
async fn threads_are_created_updated_and_listed_under_the_configured_channel() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/channels/{CHANNEL}/threads")))
        .respond_with(
            ResponseTemplate::new(201).set_body_json(json!({"id": THREAD, "name": "Plan"})),
        )
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/channels/{THREAD}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": THREAD})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/guilds/{GUILD}/threads/active")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"threads": [
            {"id": THREAD, "name": "Plan", "parent_id": CHANNEL, "thread_metadata": {"archived": false}},
            {"id": "5", "name": "Elsewhere", "parent_id": "other"},
            {"name": "no id", "parent_id": CHANNEL}
        ]})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/channels/{CHANNEL}/threads/archived/public")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"threads": [
            {"id": "6", "name": "Old", "parent_id": CHANNEL, "thread_metadata": {"archived": true}}
        ]})))
        .mount(&server)
        .await;
    let channel = configured(&server);

    let (id, name) = channel.create_thread("  Plan  ").await.unwrap();
    assert_eq!((id.as_str(), name.as_str()), (THREAD, "Plan"));
    assert_eq!(
        body_json(&requests(&server, "POST").await[0])["name"],
        "Plan"
    );
    channel
        .update_thread(THREAD, ThreadAction::Archive)
        .await
        .unwrap();
    assert_eq!(
        body_json(&requests(&server, "PATCH").await[0]),
        json!({"archived": true})
    );

    let active = channel.list_threads(None).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(
        (active[0].thread_id.as_str(), active[0].active),
        (THREAD, Some(true))
    );
    let archived = channel.list_threads(Some(false)).await.unwrap();
    assert_eq!(
        (archived[0].thread_id.as_str(), archived[0].active),
        ("6", Some(false))
    );
}

#[tokio::test]
async fn thread_operations_validate_configuration_and_ids_first() {
    let server = MockServer::start().await;
    let bare = channel_for(&server);
    assert!(
        bare.create_thread("x")
            .await
            .unwrap_err()
            .message
            .contains("channel_id")
    );
    assert!(
        bare.list_threads(None)
            .await
            .unwrap_err()
            .message
            .contains("channel_id")
    );

    let no_guild = DiscordChannel::new(
        BOT_TOKEN.into(),
        None,
        Some(CHANNEL.into()),
        vec![],
        false,
        false,
    )
    .with_api_base(server.uri());
    assert!(
        no_guild
            .list_threads(None)
            .await
            .unwrap_err()
            .message
            .contains("guild_id")
    );

    let channel = configured(&server);
    assert!(
        channel
            .create_thread("   ")
            .await
            .unwrap_err()
            .message
            .contains("empty")
    );
    assert!(
        channel
            .update_thread("../x", ThreadAction::Lock)
            .await
            .unwrap_err()
            .message
            .contains("numeric")
    );
    assert!(server.received_requests().await.unwrap().is_empty());

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"ok": 1})))
        .mount(&server)
        .await;
    assert!(
        channel
            .create_thread("ok")
            .await
            .unwrap_err()
            .message
            .contains("no thread id")
    );
}
