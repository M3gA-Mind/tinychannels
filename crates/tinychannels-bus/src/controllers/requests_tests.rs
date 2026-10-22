use super::*;

#[test]
fn connect_request_uses_the_public_camel_case_wire_shape() {
    let request = ChannelConnectRequest {
        channel: "telegram".into(),
        auth_mode: ChannelAuthMode::BotToken,
        credentials: Some(serde_json::json!({"bot_token":"secret"})),
    };

    assert_eq!(
        serde_json::to_value(request).unwrap(),
        serde_json::json!({
            "channel": "telegram",
            "authMode": "bot_token",
            "credentials": {"bot_token":"secret"}
        })
    );
}

#[test]
fn optional_controller_inputs_default_when_omitted() {
    let status: ChannelStatusRequest = serde_json::from_value(serde_json::json!({})).unwrap();
    assert_eq!(status.channel, None);

    let disconnect: ChannelDisconnectRequest = serde_json::from_value(serde_json::json!({
        "channel": "telegram",
        "authMode": "bot_token"
    }))
    .unwrap();
    assert!(!disconnect.clear_memory);
}

#[test]
fn all_controller_request_shapes_round_trip() {
    let cases = [
        serde_json::json!({"channel":"telegram"}),
        serde_json::json!({"channel":"telegram","authMode":"oauth","credentials":null}),
        serde_json::json!({"channel":"telegram","authMode":"oauth","clearMemory":true}),
        serde_json::json!({"channel":"telegram","authMode":"bot_token","credentials":{"x":1}}),
        serde_json::json!({"channel":"telegram"}),
        serde_json::json!({"guildId":"g"}),
        serde_json::json!({"guildId":"g","channelId":"c"}),
        serde_json::json!({"channel":"telegram","message":{"text":"hi"}}),
        serde_json::json!({"channel":"telegram","reaction":{"emoji":"✅"}}),
        serde_json::json!({"channel":"telegram","title":"topic"}),
        serde_json::json!({"channel":"telegram","threadId":"t","action":"close"}),
        serde_json::json!({"channel":"telegram","active":false}),
    ];
    let decoders: [fn(serde_json::Value) -> serde_json::Value; 12] = [
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelDescribeRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelConnectRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelDisconnectRequest>(v).unwrap())
                .unwrap()
        },
        |v| serde_json::to_value(serde_json::from_value::<ChannelTestRequest>(v).unwrap()).unwrap(),
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelSetDefaultRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<DiscordListChannelsRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(
                serde_json::from_value::<DiscordCheckPermissionsRequest>(v).unwrap(),
            )
            .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelSendMessageRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelSendReactionRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelCreateThreadRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelUpdateThreadRequest>(v).unwrap())
                .unwrap()
        },
        |v| {
            serde_json::to_value(serde_json::from_value::<ChannelListThreadsRequest>(v).unwrap())
                .unwrap()
        },
    ];

    for (input, decode) in cases.into_iter().zip(decoders) {
        let output = decode(input.clone());
        for key in input.as_object().unwrap().keys() {
            assert_eq!(output.get(key), input.get(key), "field {key}");
        }
    }
}
