use super::*;
use serde_json::json;

#[test]
fn public_controller_requests_preserve_camel_case_wire_fields_and_defaults() {
    let connect: ChannelConnectRequest = serde_json::from_value(json!({
        "channel": "telegram",
        "authMode": "bot_token"
    }))
    .unwrap();
    assert_eq!(connect.auth_mode, ChannelAuthMode::BotToken);
    assert_eq!(connect.credentials, None);
    assert_eq!(
        serde_json::to_value(connect).unwrap(),
        json!({
            "channel": "telegram",
            "authMode": "bot_token",
            "credentials": null
        })
    );

    let disconnect: ChannelDisconnectRequest = serde_json::from_value(json!({
        "channel": "discord",
        "authMode": "bot_token"
    }))
    .unwrap();
    assert!(!disconnect.clear_memory);

    let status: ChannelStatusRequest = serde_json::from_value(json!({})).unwrap();
    assert_eq!(status.channel, None);
    assert_eq!(
        serde_json::to_value(status).unwrap(),
        json!({"channel": null})
    );

    let threads: ChannelListThreadsRequest = serde_json::from_value(json!({
        "channel": "discord"
    }))
    .unwrap();
    assert_eq!(threads.active, None);
}

#[test]
fn message_and_thread_requests_keep_existing_controller_shapes() {
    let message: ChannelSendMessageRequest = serde_json::from_value(json!({
        "channel": "telegram",
        "message": {"text": "hello", "thread_ts": "topic-1"}
    }))
    .unwrap();
    assert_eq!(message.message["thread_ts"], "topic-1");
    assert_eq!(
        serde_json::to_value(message).unwrap()["message"]["text"],
        "hello"
    );

    let update: ChannelUpdateThreadRequest = serde_json::from_value(json!({
        "channel": "telegram",
        "threadId": "topic-1",
        "action": "close"
    }))
    .unwrap();
    assert_eq!(update.thread_id, "topic-1");
    assert_eq!(update.action, "close");
}

#[test]
fn remaining_controller_request_fields_match_the_public_schema() {
    let describe: ChannelDescribeRequest =
        serde_json::from_value(json!({"channel": "telegram"})).unwrap();
    assert_eq!(describe.channel, "telegram");

    let test: ChannelTestRequest = serde_json::from_value(json!({
        "channel": "discord",
        "authMode": "bot_token",
        "credentials": {"token": "secret"}
    }))
    .unwrap();
    assert_eq!(test.auth_mode, ChannelAuthMode::BotToken);
    assert_eq!(test.credentials["token"], "secret");

    let discord: DiscordListChannelsRequest =
        serde_json::from_value(json!({"guildId": "guild-1"})).unwrap();
    assert_eq!(discord.guild_id, "guild-1");
    let permissions: DiscordCheckPermissionsRequest = serde_json::from_value(json!({
        "guildId": "guild-1",
        "channelId": "channel-2"
    }))
    .unwrap();
    assert_eq!(permissions.channel_id, "channel-2");

    let reaction: ChannelSendReactionRequest = serde_json::from_value(json!({
        "channel": "discord",
        "reaction": {"messageId": "message-1", "emoji": "👍"}
    }))
    .unwrap();
    assert_eq!(reaction.reaction["emoji"], "👍");
    let thread: ChannelCreateThreadRequest = serde_json::from_value(json!({
        "channel": "discord",
        "title": "topic"
    }))
    .unwrap();
    assert_eq!(thread.title, "topic");
}
