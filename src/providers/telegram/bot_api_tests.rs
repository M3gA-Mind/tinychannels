use super::*;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn api(server: &MockServer) -> TelegramBotApi {
    TelegramBotApi::new("tok", reqwest::Client::new()).with_api_base(server.uri())
}

async fn mount(
    server: &MockServer,
    name: &str,
    body: serde_json::Value,
    expect: serde_json::Value,
) {
    Mock::given(method("POST"))
        .and(path(format!("/bottok/{name}")))
        .and(body_partial_json(expect))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(server)
        .await;
}

#[test]
fn reaction_request_decodes_and_validates() {
    let ok = TelegramReactionRequest::from_value(
        &json!({"chatId": -100, "messageId": "7", "emoji": "👍"}),
    )
    .unwrap();
    assert_eq!(ok.chat_id, "-100");
    assert_eq!(ok.message_id, 7);
    assert!(!ok.remove);
    assert!(TelegramReactionRequest::from_value(&json!({"messageId": 1, "emoji": "x"})).is_err());
    assert!(TelegramReactionRequest::from_value(&json!({"chatId": "1"})).is_err());
    assert!(
        TelegramReactionRequest::from_value(
            &json!({"chatId": "1", "messageId": "abc", "emoji": "x"})
        )
        .is_err()
    );
    assert!(TelegramReactionRequest::from_value(&json!({"chatId": "1", "messageId": 1})).is_err());
    assert!(
        TelegramReactionRequest::from_value(
            &json!({"chatId": "1", "messageId": 1, "remove": true})
        )
        .unwrap()
        .remove
    );
}

#[test]
fn topic_action_parses_known_values_only() {
    assert_eq!(TopicAction::parse(" Close ").unwrap(), TopicAction::Close);
    assert_eq!(TopicAction::parse("reopen").unwrap(), TopicAction::Reopen);
    assert!(TopicAction::parse("delete").is_err());
}

#[tokio::test]
async fn reaction_posts_emoji_custom_and_removal_payloads() {
    let server = MockServer::start().await;
    let ok = json!({"ok": true, "result": true});
    mount(
        &server,
        "setMessageReaction",
        ok.clone(),
        json!({"reaction":[{"type":"emoji","emoji":"👍"}]}),
    )
    .await;
    let client = api(&server).await;
    let mut request =
        TelegramReactionRequest::from_value(&json!({"chatId":"1","messageId":2,"emoji":"👍"}))
            .unwrap();
    client.send_reaction(&request).await.unwrap();

    request.custom_emoji_id = Some("c1".into());
    let server2 = MockServer::start().await;
    mount(
        &server2,
        "setMessageReaction",
        ok.clone(),
        json!({"reaction":[{"type":"custom_emoji","custom_emoji_id":"c1"}]}),
    )
    .await;
    api(&server2).await.send_reaction(&request).await.unwrap();

    request.remove = true;
    let server3 = MockServer::start().await;
    mount(&server3, "setMessageReaction", ok, json!({"reaction":[]})).await;
    api(&server3).await.send_reaction(&request).await.unwrap();
}

#[tokio::test]
async fn bot_api_error_surfaces_description_without_token() {
    let server = MockServer::start().await;
    mount(
        &server,
        "setMessageReaction",
        json!({"ok": false, "description": "REACTION_INVALID"}),
        json!({}),
    )
    .await;
    let request =
        TelegramReactionRequest::from_value(&json!({"chatId":"1","messageId":2,"emoji":"x"}))
            .unwrap();
    let error = api(&server)
        .await
        .send_reaction(&request)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("REACTION_INVALID"));
    assert!(!error.contains("tok/"));
}

#[tokio::test]
async fn unreachable_api_reports_failure() {
    let client =
        TelegramBotApi::new("tok", reqwest::Client::new()).with_api_base("http://127.0.0.1:1");
    let request =
        TelegramReactionRequest::from_value(&json!({"chatId":"1","messageId":2,"emoji":"x"}))
            .unwrap();
    assert!(client.send_reaction(&request).await.is_err());
}

#[tokio::test]
async fn forum_topic_create_close_and_reopen() {
    let server = MockServer::start().await;
    mount(
        &server,
        "createForumTopic",
        json!({"ok": true, "result": {"message_thread_id": 55, "name": "Plan"}}),
        json!({"chat_id": "-1", "name": "Plan"}),
    )
    .await;
    mount(
        &server,
        "closeForumTopic",
        json!({"ok": true, "result": true}),
        json!({"message_thread_id": 55}),
    )
    .await;
    mount(
        &server,
        "reopenForumTopic",
        json!({"ok": true, "result": true}),
        json!({"message_thread_id": 55}),
    )
    .await;
    let client = api(&server).await;
    let (id, name) = client.create_topic("-1", "Plan").await.unwrap();
    assert_eq!((id.as_str(), name.as_str()), ("55", "Plan"));
    client
        .update_topic("-1", &id, TopicAction::Close)
        .await
        .unwrap();
    client
        .update_topic("-1", &id, TopicAction::Reopen)
        .await
        .unwrap();
    assert!(
        client
            .update_topic("-1", "abc", TopicAction::Close)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn forum_topic_create_without_thread_id_fails() {
    let server = MockServer::start().await;
    mount(
        &server,
        "createForumTopic",
        json!({"ok": true, "result": {}}),
        json!({}),
    )
    .await;
    assert!(api(&server).await.create_topic("-1", "x").await.is_err());
}
