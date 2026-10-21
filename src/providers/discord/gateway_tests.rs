use super::*;
use crate::providers::discord::support::*;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn the_gateway_asks_for_message_content_reactions_and_threads() {
    // GUILDS | GUILD_MESSAGES | GUILD_MESSAGE_REACTIONS | DIRECT_MESSAGES |
    // DIRECT_MESSAGE_REACTIONS | MESSAGE_CONTENT
    assert_eq!(GATEWAY_INTENTS, 1 + 512 + 1024 + 4096 + 8192 + 32768);
}

async fn guild_channel(server: &MockServer) -> (DiscordChannel, Arc<RecordingEvents>) {
    Mock::given(method("GET"))
        .and(path(format!("/channels/{CHANNEL}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": CHANNEL, "type": 0})))
        .mount(server)
        .await;
    let events = Arc::new(RecordingEvents::default());
    (channel_for(server).with_events(events.clone()), events)
}

#[tokio::test]
async fn dispatch_routes_each_gateway_event_to_its_handler() {
    let server = MockServer::start().await;
    let (channel, events) = guild_channel(&server).await;
    let (tx, mut rx) = mpsc::channel(8);
    let message = json!({
        "id": "11", "channel_id": CHANNEL, "guild_id": GUILD, "content": "hi",
        "author": {"id": "42"}, "attachments": []
    });

    assert!(
        channel
            .handle_dispatch_to(
                "MESSAGE_CREATE",
                &message,
                BOT_ID,
                &GatewaySink::Typed(tx.clone())
            )
            .await
    );
    let mut edit = message.clone();
    edit["edited_timestamp"] = json!("2026-01-02T03:04:05+00:00");
    assert!(
        channel
            .handle_dispatch_to(
                "MESSAGE_UPDATE",
                &edit,
                BOT_ID,
                &GatewaySink::Typed(tx.clone())
            )
            .await
    );
    assert!(
        channel
            .handle_dispatch_to(
                "MESSAGE_DELETE",
                &json!({"id": "11", "channel_id": CHANNEL, "guild_id": GUILD}),
                BOT_ID,
                &GatewaySink::Typed(tx.clone())
            )
            .await
    );
    let reaction = json!({"user_id": "42", "message_id": "11", "channel_id": CHANNEL, "guild_id": GUILD, "emoji": {"name": "x"}});
    assert!(
        channel
            .handle_dispatch_to(
                "MESSAGE_REACTION_ADD",
                &reaction,
                BOT_ID,
                &GatewaySink::Typed(tx.clone())
            )
            .await
    );
    assert!(
        channel
            .handle_dispatch_to(
                "MESSAGE_REACTION_REMOVE",
                &reaction,
                BOT_ID,
                &GatewaySink::Typed(tx.clone())
            )
            .await
    );
    assert!(
        channel
            .handle_dispatch_to(
                "TYPING_START",
                &json!({}),
                BOT_ID,
                &GatewaySink::Typed(tx.clone())
            )
            .await
    );

    let mut ingress = Vec::new();
    while let Ok(item) = rx.try_recv() {
        ingress.push(item);
    }
    assert_eq!(ingress.len(), 5);
    assert!(matches!(
        ingress[0],
        tinychannels_bus::ChannelIngress::Inbound(_)
    ));
    let kinds: Vec<_> = ingress
        .into_iter()
        .filter_map(|item| match item {
            tinychannels_bus::ChannelIngress::Event(e) => Some(match e.event {
                tinychannels_bus::ChannelEventKind::MessageUpdated { .. } => "updated",
                tinychannels_bus::ChannelEventKind::MessageDeleted { .. } => "deleted",
                tinychannels_bus::ChannelEventKind::ReactionAdded { .. } => "added",
                tinychannels_bus::ChannelEventKind::ReactionRemoved { .. } => "removed",
                _ => "other",
            }),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, vec!["updated", "deleted", "added", "removed"]);
    assert!(events.inbound().is_empty());
}

#[tokio::test]
async fn dispatch_reports_a_gone_receiver_only_for_messages() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": CHANNEL, "type": 0})))
        .mount(&server)
        .await;
    let channel = channel_for(&server);
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let message = json!({"id": "11", "channel_id": CHANNEL, "guild_id": GUILD, "content": "hi", "author": {"id": "42"}});

    assert!(
        !channel
            .handle_dispatch_to(
                "MESSAGE_CREATE",
                &message,
                BOT_ID,
                &GatewaySink::Typed(tx.clone())
            )
            .await
    );
    assert!(
        channel
            .handle_dispatch_to("READY", &json!({}), BOT_ID, &GatewaySink::Typed(tx.clone()))
            .await
    );
}

#[tokio::test]
async fn thread_events_prime_the_cache_so_no_lookup_is_needed() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let (tx, _rx) = mpsc::channel(1);

    channel
        .handle_dispatch_to(
            "THREAD_CREATE",
            &json!({"id": THREAD, "type": 11, "parent_id": CHANNEL}),
            BOT_ID,
            &GatewaySink::Typed(tx.clone()),
        )
        .await;
    channel
        .handle_dispatch_to(
            "THREAD_UPDATE",
            &json!({"id": "5", "type": 12, "parent_id": CHANNEL}),
            BOT_ID,
            &GatewaySink::Typed(tx.clone()),
        )
        .await;
    channel
        .handle_dispatch_to(
            "GUILD_CREATE",
            &json!({"id": GUILD, "threads": [{"id": "6", "type": 10, "parent_id": CHANNEL}]}),
            BOT_ID,
            &GatewaySink::Typed(tx.clone()),
        )
        .await;
    channel
        .handle_dispatch_to(
            "GUILD_CREATE",
            &json!({"id": GUILD}),
            BOT_ID,
            &GatewaySink::Typed(tx.clone()),
        )
        .await;

    for id in [THREAD, "5", "6"] {
        assert_eq!(
            channel
                .channel_info(id, true)
                .await
                .thread_parent
                .as_deref(),
            Some(CHANNEL)
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn button_interactions_dispatch_to_the_interaction_handler() {
    let server = MockServer::start().await;
    let (channel, events) = guild_channel(&server).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let (tx, mut rx) = mpsc::channel(1);
    let press = json!({
        "id": "77", "token": "tok", "type": 3, "channel_id": CHANNEL, "guild_id": GUILD,
        "data": {"custom_id": "go"}, "member": {"user": {"id": "42"}}
    });

    channel
        .handle_dispatch_to(
            "INTERACTION_CREATE",
            &press,
            BOT_ID,
            &GatewaySink::Typed(tx.clone()),
        )
        .await;

    assert!(matches!(
        rx.try_recv(),
        Ok(tinychannels_bus::ChannelIngress::Event(_))
    ));
    assert!(rx.try_recv().is_err());
    assert!(events.events().is_empty());
}
