use super::*;
use serde_json::{Value, json};
use tinystoragedrivers_core::schema::{MigrateOptions, Registry};
use tinystoragedrivers_core::{
    CollectionSpec, MemoryStorage, Precondition, Scope, ScopedStorage, StorageBackend, WriteOptions,
};

use crate::storage::schema::{self, storage_schema};

/// A migrated copy of `seed`: v1 documents written under their legacy names,
/// then the whole schema prepared and migrated over a module scope.
async fn migrated(seed: &[(&str, &str, Value)]) -> ScopedStorage {
    let backend = MemoryStorage::new();
    let registry = Registry::compose([storage_schema()]).unwrap();
    registry.prepare(&backend).await.unwrap();
    let storage = backend
        .for_scope(&Scope::local().module("tinychannels").unwrap())
        .unwrap();
    for (collection, id, body) in seed {
        storage
            .documents()
            .ensure_collection(&CollectionSpec::new(*collection))
            .await
            .unwrap();
        storage
            .documents()
            .put_with(
                collection,
                id,
                body.clone(),
                Precondition::Absent,
                WriteOptions::new().schema(1),
            )
            .await
            .unwrap();
    }
    registry
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap();
    storage
}

async fn body(storage: &ScopedStorage, collection: &str, id: &str) -> Option<Value> {
    storage
        .documents()
        .get(collection, id)
        .await
        .unwrap()
        .map(|found| found.doc)
}

fn v1_entry(id: &str) -> Value {
    json!({
        "id": id,
        "intent": {
            "idempotencyKey": id,
            "channelId": "telegram",
            "conversationId": "room",
            "replyToId": null,
            "threadId": "t-1",
            "durability": "required",
            "payload": { "kind": "media", "text": "hi", "media_urls": ["https://x/y.png"] },
        },
        "enqueuedAtUnixMs": 1_700_000_000_123_u64,
        "retryCount": 2,
        "lastAttemptAtUnixMs": 1_700_000_000_456_u64,
        "lastError": "boom",
        "platformSendStartedAtUnixMs": 1_700_000_000_789_u64,
        "recoveryState": "unknown_after_send",
    })
}

fn v2_entry(id: &str) -> Value {
    json!({
        "intent": {
            "idempotency_key": id,
            "channel_id": "telegram",
            "conversation_id": "room",
            "reply_to_id": null,
            "thread_id": "t-1",
            "durability": "required",
            "payload": { "kind": "media", "text": "hi", "media_urls": ["https://x/y.png"] },
        },
        "enqueued_ms": 1_700_000_000_123_i64,
        "retry_count": 2,
        "last_attempt_ms": 1_700_000_000_456_i64,
        "last_error": "boom",
        "platform_send_started_ms": 1_700_000_000_789_i64,
        "recovery_state": "unknown_after_send",
    })
}

#[tokio::test]
async fn pending_and_failed_deliveries_become_snake_case_ms_records_under_key_ids() {
    let storage = migrated(&[
        ("channels_delivery_pending", "op/1", v1_entry("op/1")),
        ("channels_delivery_failed", "op-2", v1_entry("op-2")),
    ])
    .await;

    assert_eq!(
        body(&storage, schema::DELIVERY_PENDING, "op%2F1").await,
        Some(v2_entry("op/1"))
    );
    assert_eq!(
        body(&storage, schema::DELIVERY_FAILED, "op-2").await,
        Some(v2_entry("op-2"))
    );
    let meta = storage
        .documents()
        .get(schema::DELIVERY_FAILED, "op-2")
        .await
        .unwrap()
        .unwrap()
        .meta;
    assert_eq!(meta.schema, 2);
    assert_eq!(
        body(&storage, "channels_delivery_pending", "op/1").await,
        None
    );
}

#[tokio::test]
async fn an_entry_whose_id_disagrees_with_its_idempotency_key_is_dropped_not_fatal() {
    let mut bad = v1_entry("good");
    bad["id"] = json!("other");
    let storage = migrated(&[
        ("channels_delivery_pending", "other", bad),
        ("channels_delivery_pending", "good", v1_entry("good")),
    ])
    .await;

    assert!(
        body(&storage, schema::DELIVERY_PENDING, "good")
            .await
            .is_some()
    );
    assert!(
        body(&storage, schema::DELIVERY_PENDING, "other")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn a_result_moves_its_lifetime_from_the_body_into_record_expiry() {
    let far_future = 4_102_444_800_000_u64;
    let storage = migrated(&[(
        "channels_delivery_results",
        "k",
        json!({
            "result": { "id": "k", "intentFingerprint": "ab", "completedAtUnixMs": 1_700_000_000_000_u64 },
            "completedAtUnixMs": 1_700_000_000_000_u64,
            "expiresAtUnixMs": far_future,
        }),
    )])
    .await;

    let found = storage
        .documents()
        .get(schema::DELIVERY_RESULTS, "k")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found.doc,
        json!({ "id": "k", "intent_fingerprint": "ab", "completed_ms": 1_700_000_000_000_i64 })
    );
    assert_eq!(found.meta.expires_ms, Some(far_future as i64));
    assert_eq!(found.meta.schema, 2);
}

#[tokio::test]
async fn bindings_and_busy_flags_are_rekeyed_by_channel_and_target() {
    let storage = migrated(&[
        (
            "channel_remote_bindings",
            "discord:room:42",
            json!({
                "thread_id": "th", "sender_key": "sk",
                "updated_at": "2026-01-01T00:00:00Z", "title": "Chat",
            }),
        ),
        (
            "channel_remote_bindings",
            "telegram:9",
            json!({ "thread_id": "t2", "sender_key": "s2", "updated_at": "bad", "title": null }),
        ),
        (
            "channel_remote_busy",
            "discord:room:42",
            json!({ "busy": true, "updated_at": "2026-01-01T00:00:00Z" }),
        ),
        (
            "channel_remote_bindings",
            "no-colon",
            json!({ "thread_id": "x", "sender_key": "y" }),
        ),
    ])
    .await;

    // The target keeps every colon after the first: only the channel is split.
    let found = storage
        .documents()
        .get(schema::REMOTE_BINDINGS, "discord/room:42")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found.doc,
        json!({ "thread_id": "th", "sender_key": "sk", "title": "Chat" })
    );
    assert_eq!(
        found.meta.created_ms, 1_767_225_600_000,
        "updated_at kept as creation time"
    );
    assert_eq!(
        body(&storage, schema::REMOTE_BINDINGS, "telegram/9").await,
        Some(json!({ "thread_id": "t2", "sender_key": "s2", "title": null }))
    );
    assert_eq!(
        body(&storage, schema::REMOTE_BUSY, "discord/room:42").await,
        Some(json!({ "busy": true }))
    );
    assert!(
        body(&storage, schema::REMOTE_BINDINGS, "no-colon")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn the_legacy_import_marker_is_carried_over_unchanged() {
    let storage = migrated(&[(
        "channel_remote_meta",
        "legacy-import",
        json!({ "imported": 3 }),
    )])
    .await;
    assert_eq!(
        body(&storage, schema::REMOTE_META, "legacy-import").await,
        Some(json!({ "imported": 3 }))
    );
}

#[tokio::test]
async fn whatsapp_key_material_is_copied_byte_for_byte() {
    // Strings chosen to expose any re-encoding: padding, `+`/`/` alphabet, case.
    let material = "+/9aZ0=";
    let storage = migrated(&[
        (
            "whatsapp_identity",
            "ab12:111@s.whatsapp.net.0",
            json!({ "account": "ab12", "key": "111@s.whatsapp.net.0", "value": material }),
        ),
        (
            "whatsapp_prekey",
            "ab12:7",
            json!({ "account": "ab12", "key": "7", "value": material, "uploaded": true }),
        ),
        (
            "whatsapp_session",
            "ab12:s",
            json!({ "account": "ab12", "key": "s", "value": "AQID" }),
        ),
    ])
    .await;

    assert_eq!(
        body(
            &storage,
            schema::WHATSAPP_IDENTITY,
            "ab12/111@s.whatsapp.net.0"
        )
        .await,
        Some(json!({ "account": "ab12", "key": "111@s.whatsapp.net.0", "data_b64": material }))
    );
    assert_eq!(
        body(&storage, schema::WHATSAPP_PREKEY, "ab12/7").await,
        Some(json!({ "account": "ab12", "key": "7", "data_b64": material, "uploaded": true }))
    );
    assert_eq!(
        body(&storage, schema::WHATSAPP_SESSION, "ab12/s").await,
        Some(json!({ "account": "ab12", "key": "s", "data_b64": "AQID" }))
    );
}

#[tokio::test]
async fn whatsapp_library_records_keep_their_payload_and_only_move_ids() {
    let device = json!({
        "account": "ab12", "key": "device",
        "device": { "registration_id": 5, "account": { "details": "AAEC" } },
    });
    let sync = json!({
        "account": "ab12", "key": "0a0b", "key_id": "Cgs=",
        "sync_key": { "key_data": [1, 2, 3], "fingerprint": [4], "timestamp": 9 },
    });
    let lid = json!({
        "account": "ab12", "key": "100",
        "entry": { "lid": "100", "phone_number": "55", "created_at": 1, "updated_at": 2, "learning_source": "usync" },
    });
    let storage = migrated(&[
        ("whatsapp_device", "ab12:device", device.clone()),
        ("whatsapp_sync_key", "ab12:0a0b", sync.clone()),
        ("whatsapp_lid_mapping", "ab12:100", lid.clone()),
    ])
    .await;

    assert_eq!(
        body(&storage, schema::WHATSAPP_DEVICE, "ab12/device").await,
        Some(device)
    );
    assert_eq!(
        body(&storage, schema::WHATSAPP_SYNC_KEY, "ab12/0a0b").await,
        Some(sync)
    );
    assert_eq!(
        body(&storage, schema::WHATSAPP_LID_MAPPING, "ab12/100").await,
        Some(lid)
    );
}

#[tokio::test]
async fn migrating_twice_leaves_the_v2_data_alone() {
    let backend = MemoryStorage::new();
    let registry = Registry::compose([storage_schema()]).unwrap();
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    registry
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap();
    registry
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap();
    registry.check_ready(schema::DELIVERY_PENDING).unwrap();
}

#[tokio::test]
async fn unreadable_v1_rows_are_dropped_and_the_rest_still_migrate() {
    let storage = migrated(&[
        ("channels_delivery_pending", "junk", json!({ "id": 5 })),
        ("channels_delivery_results", "junk", json!({ "nope": true })),
        (
            "channels_delivery_results",
            "ok",
            json!({
                "result": { "id": "ok", "intentFingerprint": "f", "completedAtUnixMs": 1 },
                "expiresAtUnixMs": 4_102_444_800_000_u64,
            }),
        ),
        (
            "channel_remote_bindings",
            "telegram:junk",
            json!({ "thread_id": 1 }),
        ),
        ("channel_remote_busy", "no-colon", json!({ "busy": true })),
        ("whatsapp_session", "nocolon", json!({ "value": "AQID" })),
        // Account and key are recovered from the id when the body lacks them.
        ("whatsapp_session", "acct:k1", json!({ "value": "AQID" })),
    ])
    .await;

    assert!(
        body(&storage, schema::DELIVERY_PENDING, "junk")
            .await
            .is_none()
    );
    assert!(
        body(&storage, schema::DELIVERY_RESULTS, "junk")
            .await
            .is_none()
    );
    assert!(
        body(&storage, schema::DELIVERY_RESULTS, "ok")
            .await
            .is_some()
    );
    assert!(
        body(&storage, schema::REMOTE_BINDINGS, "telegram/junk")
            .await
            .is_none()
    );
    assert!(
        body(&storage, schema::REMOTE_BUSY, "no-colon")
            .await
            .is_none()
    );
    assert!(
        body(&storage, schema::WHATSAPP_SESSION, "nocolon")
            .await
            .is_none()
    );
    assert_eq!(
        body(&storage, schema::WHATSAPP_SESSION, "acct/k1").await,
        Some(json!({ "account": "acct", "key": "k1", "data_b64": "AQID" }))
    );
}

#[test]
fn rfc3339_timestamps_parse_to_unix_milliseconds() {
    assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(
        parse_rfc3339_ms("2026-09-29T15:04:00Z"),
        Some(1_790_694_240_000)
    );
    assert_eq!(
        parse_rfc3339_ms("2026-09-29T17:04:00.250+02:00"),
        Some(1_790_694_240_250)
    );
    assert_eq!(
        parse_rfc3339_ms("2024-02-29T00:00:00-01:00"),
        Some(1_709_168_400_000)
    );
    assert_eq!(parse_rfc3339_ms("not a time"), None);
    assert_eq!(parse_rfc3339_ms("2026-13-01T00:00:00Z"), None);
}
