//! Generate a fixture by running the released TinyChannels v0.1.38 writers.
//!
//! Copy this file into the archived v0.1.38 checkout's `examples/` directory
//! and run `cargo run --locked --example generate_v1_fixture` there. The JSON
//! contains public storage-port records, not hand-written storage DTOs.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tinychannels::{
    delivery::{
        DeliveryQueueStore, DeliveryResultStore, QueuedDelivery,
    },
    remote::session_store::RemoteSessionStore,
    tinychannels_bus::{
        ChannelOutboundIntent, DeliveryDurability, DeliveryTerminalResult, OutboundPayload,
    },
};
use tinystoragedrivers_core::{DocumentStoreExt, MemoryStorage, Query, Scope, StorageBackend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::local())?;
    let docs = Arc::clone(storage.documents());

    let delivery = tinychannels::storage::DocumentDeliveryStore::new(Arc::clone(&docs));
    delivery
        .save_pending(QueuedDelivery::new(
            "fixture-pending-1",
            ChannelOutboundIntent {
                idempotency_key: "fixture-pending-1".into(),
                channel_id: "telegram".into(),
                conversation_id: "room-42".into(),
                thread_id: Some("thread-9".into()),
                durability: DeliveryDurability::Required,
                payload: OutboundPayload::Text {
                    text: "legacy public writer fixture".into(),
                },
                ..Default::default()
            },
            1_700_000_000_123,
        ))
        .await?;
    delivery
        .save_failed(QueuedDelivery::new(
            "fixture-failed-1",
            ChannelOutboundIntent {
                idempotency_key: "fixture-failed-1".into(),
                channel_id: "telegram".into(),
                conversation_id: "room-42".into(),
                durability: DeliveryDurability::Required,
                payload: OutboundPayload::Text {
                    text: "failed public writer fixture".into(),
                },
                ..Default::default()
            },
            1_700_000_000_456,
        ))
        .await?;
    delivery
        .save_result(DeliveryTerminalResult {
            id: "fixture-result-1".into(),
            intent_fingerprint: "0123456789abcdef".repeat(4),
            completed_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis()
                .try_into()?,
        })
        .await?;

    let remote = RemoteSessionStore::new(Arc::clone(&docs));
    remote
        .set_binding(
            "telegram",
            "room:42",
            "thread-9".into(),
            "telegram_bot_room:42".into(),
            Some("Fixture chat".into()),
        )
        .await?;
    remote.set_busy("telegram", "room:42", true).await?;

    let mut records = BTreeMap::new();
    for collection in [
        "channels.delivery_pending",
        "channels.delivery_failed",
        "channels.delivery_results",
        "channels.remote_bindings",
        "channels.remote_busy",
    ] {
        let items = docs.query_all(collection, &Query::all()).await?;
        records.insert(
            collection,
            items
                .into_iter()
                .map(|item| {
                    serde_json::json!({
                        "id": item.id,
                        "body": item.doc,
                        "schema": item.meta.schema,
                        "created_ms": item.meta.created_ms,
                        "expires_ms": item.meta.expires_ms,
                    })
                })
                .collect::<Vec<_>>(),
        );
    }

    println!("{}", serde_json::to_string_pretty(&records)?);
    Ok(())
}
