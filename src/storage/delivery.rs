//! The required-delivery queue on the document port.
//!
//! One implementation serves a linked host (a [`DocumentStore`] it owns) and
//! the loaded module (the same trait over the storage bus). Three collections
//! hold the state machine's records:
//!
//! | collection | id | document |
//! | --- | --- | --- |
//! | `channels_delivery_pending` | idempotency key | the [`QueuedDelivery`] |
//! | `channels_delivery_failed` | idempotency key | the [`QueuedDelivery`] |
//! | `channels_delivery_results` | idempotency key | terminal result + expiry |
//!
//! Writes that must not race use the port's preconditions: a new key is
//! inserted with `Absent`, an update is a compare-and-swap on the version read.
//! The record bounds the old host callbacks enforced (record size, pending
//! capacity, result capacity and lifetime) are enforced here, in the module,
//! before any I/O.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinychannels_bus::DeliveryTerminalResult;
use tinychannels_bus::module_delivery::{
    DELIVERY_PENDING_CAPACITY, DELIVERY_RECORD_BYTES, DELIVERY_RESULT_CAPACITY,
    DELIVERY_RESULT_TTL_SECONDS,
};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, DocumentStoreExt, ErrorKind, Filter, Precondition, Query, Sort,
    StorageError,
};

use crate::delivery::{DeliveryQueueError, DeliveryQueueStore, DeliveryResultStore, QueuedDelivery};

const LOG_PREFIX: &str = "[channel-delivery-store]";

/// Collection of entries awaiting a terminal outcome.
pub const PENDING_COLLECTION: &str = "channels_delivery_pending";
/// Collection of permanently failed entries.
pub const FAILED_COLLECTION: &str = "channels_delivery_failed";
/// Collection of idempotent terminal results.
pub const RESULTS_COLLECTION: &str = "channels_delivery_results";

/// Compare-and-swap attempts before an update reports contention.
const CAS_ATTEMPTS: usize = 4;
const MAX_ID_BYTES: usize = 256;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultRecord {
    result: DeliveryTerminalResult,
    completed_at_unix_ms: u64,
    expires_at_unix_ms: u64,
}

/// [`DeliveryQueueStore`] and [`DeliveryResultStore`] over a [`DocumentStore`].
#[derive(Clone, Debug)]
pub struct DocumentDeliveryStore {
    docs: Arc<dyn DocumentStore>,
    ready: Arc<tokio::sync::OnceCell<()>>,
}

impl DocumentDeliveryStore {
    /// A store over `docs`. Collections are declared on first use.
    #[must_use]
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            docs,
            ready: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    async fn docs(&self) -> &Arc<dyn DocumentStore> {
        self.ready
            .get_or_init(|| async {
                for name in [PENDING_COLLECTION, FAILED_COLLECTION, RESULTS_COLLECTION] {
                    super::ensure_collection(&self.docs, CollectionSpec::new(name)).await;
                }
            })
            .await;
        &self.docs
    }
}

pub(crate) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn store_error(error: StorageError) -> DeliveryQueueError {
    tracing::warn!("{LOG_PREFIX} storage failure: {:?}", error.kind());
    DeliveryQueueError::Store(super::describe(&error))
}

fn invalid(message: &str) -> DeliveryQueueError {
    DeliveryQueueError::Store(message.into())
}

fn validate_id(id: &str) -> Result<(), DeliveryQueueError> {
    if id.is_empty() || id.len() > MAX_ID_BYTES {
        return Err(invalid("delivery id is invalid"));
    }
    Ok(())
}

fn validate_entry(entry: &QueuedDelivery) -> Result<(), DeliveryQueueError> {
    validate_id(&entry.id)?;
    if entry.intent.idempotency_key != entry.id
        || serde_json::to_vec(entry).map_or(true, |bytes| bytes.len() > DELIVERY_RECORD_BYTES)
    {
        return Err(invalid(
            "delivery record is invalid or exceeds the record limit",
        ));
    }
    Ok(())
}

fn decode_entry(doc: Value) -> Result<QueuedDelivery, DeliveryQueueError> {
    let entry: QueuedDelivery = serde_json::from_value(doc)
        .map_err(|_| invalid("stored delivery record is unreadable"))?;
    validate_entry(&entry)?;
    Ok(entry)
}

fn encode_entry(entry: &QueuedDelivery) -> Result<Value, DeliveryQueueError> {
    serde_json::to_value(entry).map_err(|_| invalid("delivery record could not be encoded"))
}

#[async_trait]
impl DeliveryQueueStore for DocumentDeliveryStore {
    async fn save_pending(&self, entry: QueuedDelivery) -> Result<(), DeliveryQueueError> {
        validate_entry(&entry)?;
        let docs = self.docs().await;
        let doc = encode_entry(&entry)?;
        for _ in 0..CAS_ATTEMPTS {
            let existing = docs
                .get(PENDING_COLLECTION, &entry.id)
                .await
                .map_err(store_error)?;
            let precondition = match existing {
                Some(found) => {
                    if decode_entry(found.doc)?.intent != entry.intent {
                        return Err(invalid("delivery key conflicts with a different intent"));
                    }
                    Precondition::Version(found.version)
                }
                None => {
                    let pending = docs
                        .count(PENDING_COLLECTION, &Filter::default())
                        .await
                        .map_err(store_error)?;
                    if pending >= DELIVERY_PENDING_CAPACITY as u64 {
                        return Err(invalid("delivery queue is full"));
                    }
                    Precondition::Absent
                }
            };
            match docs
                .put(PENDING_COLLECTION, &entry.id, doc.clone(), precondition)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) => {
                    tracing::debug!("{LOG_PREFIX} save_pending raced; retrying");
                }
                Err(error) => return Err(store_error(error)),
            }
        }
        Err(invalid("delivery queue is busy"))
    }

    async fn load_pending(&self, id: &str) -> Result<Option<QueuedDelivery>, DeliveryQueueError> {
        validate_id(id)?;
        self.docs()
            .await
            .get(PENDING_COLLECTION, id)
            .await
            .map_err(store_error)?
            .map(|found| decode_entry(found.doc))
            .transpose()
    }

    async fn load_pending_all(&self) -> Result<Vec<QueuedDelivery>, DeliveryQueueError> {
        let query = Query::all().sort(Sort::asc("enqueuedAtUnixMs"));
        let found = self
            .docs()
            .await
            .query_all(PENDING_COLLECTION, &query)
            .await
            .map_err(store_error)?;
        if found.len() > DELIVERY_PENDING_CAPACITY {
            return Err(invalid("stored pending queue is oversized"));
        }
        found.into_iter().map(|v| decode_entry(v.doc)).collect()
    }

    async fn remove_pending(&self, id: &str) -> Result<(), DeliveryQueueError> {
        validate_id(id)?;
        self.docs()
            .await
            .delete(PENDING_COLLECTION, id, Precondition::None)
            .await
            .map(|_| ())
            .map_err(store_error)
    }

    async fn save_failed(&self, entry: QueuedDelivery) -> Result<(), DeliveryQueueError> {
        validate_entry(&entry)?;
        let doc = encode_entry(&entry)?;
        self.docs()
            .await
            .put(FAILED_COLLECTION, &entry.id, doc, Precondition::None)
            .await
            .map(|_| ())
            .map_err(store_error)
    }
}

#[async_trait]
impl DeliveryResultStore for DocumentDeliveryStore {
    async fn load_failed(&self, id: &str) -> Result<Option<QueuedDelivery>, DeliveryQueueError> {
        validate_id(id)?;
        self.docs()
            .await
            .get(FAILED_COLLECTION, id)
            .await
            .map_err(store_error)?
            .map(|found| decode_entry(found.doc))
            .transpose()
    }

    async fn load_result(
        &self,
        id: &str,
    ) -> Result<Option<DeliveryTerminalResult>, DeliveryQueueError> {
        validate_id(id)?;
        let Some(found) = self
            .docs()
            .await
            .get(RESULTS_COLLECTION, id)
            .await
            .map_err(store_error)?
        else {
            return Ok(None);
        };
        let record: ResultRecord = serde_json::from_value(found.doc)
            .map_err(|_| invalid("stored delivery result is unreadable"))?;
        Ok((record.expires_at_unix_ms > now_unix_ms()).then_some(record.result))
    }

    async fn save_result(&self, result: DeliveryTerminalResult) -> Result<(), DeliveryQueueError> {
        validate_id(&result.id)?;
        if serde_json::to_vec(&result).map_or(true, |bytes| bytes.len() > DELIVERY_RECORD_BYTES) {
            return Err(invalid("delivery result exceeds the record limit"));
        }
        let docs = self.docs().await;
        let record = ResultRecord {
            completed_at_unix_ms: result.completed_at_unix_ms,
            expires_at_unix_ms: result
                .completed_at_unix_ms
                .saturating_add(DELIVERY_RESULT_TTL_SECONDS.saturating_mul(1000)),
            result,
        };
        let id = record.result.id.clone();
        let doc = serde_json::to_value(&record).map_err(|_| invalid("result could not be encoded"))?;
        docs.put(RESULTS_COLLECTION, &id, doc, Precondition::None)
            .await
            .map_err(store_error)?;
        self.trim_results(&id).await
    }
}

impl DocumentDeliveryStore {
    /// Drop expired results, then the oldest beyond the capacity. The record
    /// just written is never the one evicted.
    async fn trim_results(&self, keep: &str) -> Result<(), DeliveryQueueError> {
        let docs = self.docs().await;
        docs.delete_where(
            RESULTS_COLLECTION,
            &Filter::lte("expiresAtUnixMs", now_unix_ms()),
        )
        .await
        .map_err(store_error)?;
        let query = Query::all().sort(Sort::asc("completedAtUnixMs"));
        let results = docs
            .query_all(RESULTS_COLLECTION, &query)
            .await
            .map_err(store_error)?;
        let excess = results.len().saturating_sub(DELIVERY_RESULT_CAPACITY);
        for old in results.into_iter().filter(|r| r.id != keep).take(excess) {
            docs.delete(RESULTS_COLLECTION, &old.id, Precondition::None)
                .await
                .map_err(store_error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
