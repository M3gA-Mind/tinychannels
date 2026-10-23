use super::*;

#[test]
fn configured_account_scope_is_stable_provider_specific_and_does_not_disclose_credentials() {
    let mut config = ChannelsConfig::default();
    config.dingtalk = Some(tinychannels_bus::config::DingTalkConfig {
        client_id: "ding-app".into(),
        client_secret: "ding-secret".into(),
        allowed_users: vec!["first".into()],
    });
    let first = configured_media_account_scope("dingtalk:primary", &config).unwrap();
    assert_eq!(
        first,
        configured_media_account_scope("dingtalk:primary", &config).unwrap()
    );
    assert!(!first.contains("ding-secret"));

    config.dingtalk.as_mut().unwrap().allowed_users = vec!["changed-policy".into()];
    assert_eq!(
        first,
        configured_media_account_scope("dingtalk:primary", &config).unwrap()
    );
    config.dingtalk.as_mut().unwrap().client_secret = "rotated-secret".into();
    assert_ne!(
        first,
        configured_media_account_scope("dingtalk:primary", &config).unwrap()
    );
    assert!(configured_media_account_scope("lark", &config).is_none());
}

#[test]
fn required_provider_accounts_need_complete_identity_material() {
    let mut config = ChannelsConfig::default();
    config.qq = Some(tinychannels_bus::config::QQConfig {
        app_id: "qq-app".into(),
        app_secret: "qq-secret".into(),
        allowed_users: vec![],
    });
    config.lark = Some(tinychannels_bus::config::LarkConfig {
        app_id: "lark-app".into(),
        app_secret: "lark-secret".into(),
        encrypt_key: None,
        verification_token: None,
        allowed_users: vec![],
        use_feishu: false,
        receive_mode: tinychannels_bus::config::LarkReceiveMode::default(),
        port: None,
    });
    config.mattermost = Some(tinychannels_bus::config::MattermostConfig {
        url: "https://chat.example".into(),
        bot_token: "mattermost-token".into(),
        channel_id: None,
        allowed_users: vec![],
        thread_replies: None,
        mention_only: None,
    });
    for provider in ["qq", "lark", "mattermost"] {
        assert!(
            configured_media_account_scope(provider, &config).is_some(),
            "{provider}"
        );
    }
    config.qq.as_mut().unwrap().app_secret.clear();
    assert!(configured_media_account_scope("qq", &config).is_none());
}

#[test]
fn telegram_media_scope_uses_stable_opaque_bot_identity() {
    let mut config = ChannelsConfig::default();
    config.telegram = Some(tinychannels_bus::config::TelegramConfig {
        bot_token: "123:private-token".into(),
        chat_id: None,
        allowed_users: vec!["42".into()],
        stream_mode: tinychannels_bus::config::StreamMode::Off,
        draft_update_interval_ms: 1_000,
        silent_streaming: false,
        mention_only: false,
    });

    let first = configured_media_account_scope("telegram:primary", &config).unwrap();
    assert_eq!(
        first,
        configured_media_account_scope("telegram:primary", &config).unwrap()
    );
    assert!(!first.contains("private-token"));

    config.telegram.as_mut().unwrap().allowed_users = vec!["changed-policy".into()];
    assert_eq!(
        first,
        configured_media_account_scope("telegram:primary", &config).unwrap()
    );
    config.telegram.as_mut().unwrap().bot_token = "456:rotated-token".into();
    assert_ne!(
        first,
        configured_media_account_scope("telegram:primary", &config).unwrap()
    );
    config.telegram.as_mut().unwrap().bot_token.clear();
    assert!(configured_media_account_scope("telegram:primary", &config).is_none());
}
