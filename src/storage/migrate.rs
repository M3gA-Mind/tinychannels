//! The v1 to v2 steps of the `channels` owner.
//!
//! Each v1 collection (`channels_delivery_*`, `channel_remote_*`,
//! `whatsapp_*`) is read through [`Step::source`] and rewritten into its
//! `channels.*` collection. Ids move from the ad hoc `a:b` joins to
//! [`Key::of`], creation times are kept, and bodies take the v2 shape (see
//! [`super::records`]). The steps are pure: the same batch always yields the
//! same records, so a resumed run reproduces an interrupted one.
//!
//! A record that was already unreadable under v1 (it failed the same checks the
//! stores apply on read) is dropped with a warning rather than carried forward
//! or allowed to fail the whole step: one poisoned row must not keep a
//! collection in `migrating` forever.
//!
//! WhatsApp key material is never reinterpreted. The base64 strings the v1
//! backend wrote are copied byte for byte (only the field that holds them is
//! renamed from the `value` wrapper to `data_b64`), and the device, sync-key and
//! LID-mapping payloads, which are the WhatsApp library's own serialized types,
//! are carried over untouched.

use serde::Deserialize;
use serde_json::{Map, Value};
use tinychannels_bus::DeliveryTerminalResult;
use tinystoragedrivers_core::Key;
use tinystoragedrivers_core::Result;
use tinystoragedrivers_core::schema::{SourceRecord, Step, TargetRecord};

use super::records::{StoredDelivery, StoredResult, to_stored_ms};
use super::schema::{
    DELIVERY_FAILED, DELIVERY_PENDING, DELIVERY_RESULTS, REMOTE_BINDINGS, REMOTE_BUSY, REMOTE_META,
    WHATSAPP_DEVICE, WHATSAPP_IDENTITY, WHATSAPP_LID_MAPPING, WHATSAPP_PREKEY, WHATSAPP_SENDER_KEY,
    WHATSAPP_SESSION, WHATSAPP_SIGNED_PREKEY, WHATSAPP_SYNC_KEY,
};
use crate::delivery::QueuedDelivery;
use crate::remote::ChatBinding;

const LOG_PREFIX: &str = "[channel-storage-migrate]";

/// Every step, in the order the schema runs them.
pub(crate) fn steps() -> Vec<Step> {
    vec![
        delivery_entries(
            "delivery-pending-v2",
            "channels_delivery_pending",
            DELIVERY_PENDING,
        ),
        delivery_entries(
            "delivery-failed-v2",
            "channels_delivery_failed",
            DELIVERY_FAILED,
        ),
        Step::new("delivery-results-v2", DELIVERY_RESULTS, 1, 2, |batch| {
            Ok(batch.into_iter().filter_map(delivery_result).collect())
        })
        .source("channels_delivery_results"),
        Step::new("remote-bindings-v2", REMOTE_BINDINGS, 1, 2, |batch| {
            Ok(batch.into_iter().filter_map(remote_binding).collect())
        })
        .source("channel_remote_bindings"),
        Step::new("remote-busy-v2", REMOTE_BUSY, 1, 2, |batch| {
            Ok(batch.into_iter().filter_map(remote_busy).collect())
        })
        .source("channel_remote_busy"),
        Step::new("remote-meta-v2", REMOTE_META, 1, 2, |batch| {
            Ok(batch.into_iter().map(remote_meta).collect())
        })
        .source("channel_remote_meta"),
        whatsapp(
            "whatsapp-device-v2",
            "whatsapp_device",
            WHATSAPP_DEVICE,
            false,
        ),
        whatsapp(
            "whatsapp-identity-v2",
            "whatsapp_identity",
            WHATSAPP_IDENTITY,
            true,
        ),
        whatsapp(
            "whatsapp-session-v2",
            "whatsapp_session",
            WHATSAPP_SESSION,
            true,
        ),
        whatsapp(
            "whatsapp-prekey-v2",
            "whatsapp_prekey",
            WHATSAPP_PREKEY,
            true,
        ),
        whatsapp(
            "whatsapp-signed-prekey-v2",
            "whatsapp_signed_prekey",
            WHATSAPP_SIGNED_PREKEY,
            true,
        ),
        whatsapp(
            "whatsapp-sender-key-v2",
            "whatsapp_sender_key",
            WHATSAPP_SENDER_KEY,
            true,
        ),
        whatsapp(
            "whatsapp-sync-key-v2",
            "whatsapp_sync_key",
            WHATSAPP_SYNC_KEY,
            false,
        ),
        whatsapp(
            "whatsapp-lid-mapping-v2",
            "whatsapp_lid_mapping",
            WHATSAPP_LID_MAPPING,
            false,
        ),
    ]
}

/// `record`'s output under `collection`, keeping its creation time.
fn rewritten(collection: &str, id: Key, body: Value, source: &SourceRecord) -> TargetRecord {
    let target = TargetRecord::new(collection, id.as_str(), body);
    if source.meta.created_ms > 0 {
        target.created_ms(source.meta.created_ms)
    } else {
        target
    }
}

fn skipped(collection: &str, id: &str, why: &str) {
    tracing::warn!(
        "{LOG_PREFIX} dropping unreadable v1 record collection={collection} id={id}: {why}"
    );
}

fn key_of(parts: &[&str]) -> Result<Key> {
    Key::of(parts)
}

/// `pending` and `failed` hold the same record: a [`QueuedDelivery`] by its
/// idempotency key.
fn delivery_entries(id: &'static str, legacy: &'static str, target: &'static str) -> Step {
    Step::new(id, target, 1, 2, move |batch| {
        let mut out = Vec::with_capacity(batch.len());
        for record in batch {
            let entry: QueuedDelivery = match serde_json::from_value(record.body.clone()) {
                Ok(entry) => entry,
                Err(error) => {
                    skipped(target, &record.id, &error.to_string());
                    continue;
                }
            };
            if entry.id.is_empty() || entry.intent.idempotency_key != entry.id {
                skipped(
                    target,
                    &record.id,
                    "entry id does not match its idempotency key",
                );
                continue;
            }
            let body = serde_json::to_value(StoredDelivery::from(&entry))
                .map_err(|e| tinystoragedrivers_core::StorageError::invalid_input(e.to_string()))?;
            out.push(rewritten(target, key_of(&[&entry.id])?, body, &record));
        }
        Ok(out)
    })
    .source(legacy)
}

/// v1 result body: the wire result beside two camelCase `UnixMs` times.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyResult {
    result: DeliveryTerminalResult,
    expires_at_unix_ms: u64,
}

fn delivery_result(record: SourceRecord) -> Option<TargetRecord> {
    let legacy: LegacyResult = match serde_json::from_value(record.body.clone()) {
        Ok(legacy) => legacy,
        Err(error) => {
            skipped(DELIVERY_RESULTS, &record.id, &error.to_string());
            return None;
        }
    };
    let id = key_of(&[&legacy.result.id]).ok()?;
    let body = serde_json::to_value(StoredResult::from(&legacy.result)).ok()?;
    // The lifetime moves from a body field into record metadata.
    Some(
        rewritten(DELIVERY_RESULTS, id, body, &record)
            .expires_ms(Some(to_stored_ms(legacy.expires_at_unix_ms))),
    )
}

/// v1 binding body, whose `updated_at` is an RFC 3339 string.
#[derive(Deserialize)]
struct LegacyBinding {
    #[serde(flatten)]
    binding: ChatBinding,
    #[serde(default)]
    updated_at: Option<String>,
}

/// A v1 chat id is `<channel>:<reply target>`; the channel never holds a colon.
fn chat_id(collection: &str, id: &str) -> Option<Key> {
    let Some((channel, target)) = id.split_once(':') else {
        skipped(collection, id, "id is not <channel>:<reply target>");
        return None;
    };
    key_of(&[channel, target]).ok()
}

fn remote_binding(record: SourceRecord) -> Option<TargetRecord> {
    let id = chat_id(REMOTE_BINDINGS, &record.id)?;
    let legacy: LegacyBinding = match serde_json::from_value(record.body.clone()) {
        Ok(legacy) => legacy,
        Err(error) => {
            skipped(REMOTE_BINDINGS, &record.id, &error.to_string());
            return None;
        }
    };
    let body = serde_json::to_value(&legacy.binding).ok()?;
    let target = rewritten(REMOTE_BINDINGS, id, body, &record);
    // `updated_at` was the binding's last write: the closest thing v2 keeps is
    // the creation time of the migrated record.
    let updated_ms = legacy
        .updated_at
        .as_deref()
        .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.timestamp_millis());
    Some(match updated_ms {
        Some(ms) if ms > 0 => target.created_ms(ms),
        _ => target,
    })
}

fn remote_busy(record: SourceRecord) -> Option<TargetRecord> {
    let id = chat_id(REMOTE_BUSY, &record.id)?;
    Some(rewritten(
        REMOTE_BUSY,
        id,
        serde_json::json!({ "busy": true }),
        &record,
    ))
}

/// The legacy import marker keeps its id and body.
fn remote_meta(record: SourceRecord) -> TargetRecord {
    let target = TargetRecord::new(REMOTE_META, record.id.clone(), record.body.clone());
    if record.meta.created_ms > 0 {
        target.created_ms(record.meta.created_ms)
    } else {
        target
    }
}

/// A v1 WhatsApp id is `<account>:<key>`; the body repeats both parts.
fn whatsapp_parts(record: &SourceRecord) -> Option<(String, String)> {
    let field = |name: &str| {
        record
            .body
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    match (field("account"), field("key")) {
        (Some(account), Some(key)) => Some((account, key)),
        _ => record
            .id
            .split_once(':')
            .map(|(account, key)| (account.to_owned(), key.to_owned())),
    }
}

/// `unwrap_value` renames the `value` wrapper of a raw-bytes record to
/// `data_b64`; the other collections keep their body as it was.
fn whatsapp(
    id: &'static str,
    legacy: &'static str,
    target: &'static str,
    unwrap_value: bool,
) -> Step {
    Step::new(id, target, 1, 2, move |batch| {
        let mut out = Vec::with_capacity(batch.len());
        for record in batch {
            let (Some((account, key)), Some(object)) =
                (whatsapp_parts(&record), record.body.as_object())
            else {
                skipped(target, &record.id, "not an <account>:<key> object");
                continue;
            };
            let mut body: Map<String, Value> = object.clone();
            if unwrap_value && let Some(bytes) = body.remove("value") {
                body.insert("data_b64".into(), bytes);
            }
            out.push(rewritten(
                target,
                key_of(&[&account, &key])?,
                Value::Object(body),
                &record,
            ));
        }
        Ok(out)
    })
    .source(legacy)
}

#[cfg(test)]
#[path = "migrate_tests.rs"]
mod tests;
