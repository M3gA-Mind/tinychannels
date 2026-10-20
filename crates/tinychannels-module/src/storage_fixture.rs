//! Test host for the storage bus: a real in-process broker serving the
//! `Storage` objects over an in-memory backend, with the knobs the delivery
//! tests need (scope switching and one-shot faults on the delivery records).
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use tinybus::Connection;
use tinychannels::storage::delivery::{PENDING_COLLECTION, RESULTS_COLLECTION};
use tinychannels::storage::records::{StoredDelivery, StoredResult};
use tinychannels_bus::{DeliveryQueueEntry, DeliveryTerminalResult};
use tinystoragedrivers_api::{DocumentStore, DocumentStoreExt, Storage, StorageBackend};
use tinystoragedrivers_busproxy::{PerCallerEndpoint, ServedStorage, StorageServer, serve};
use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
use tinystoragedrivers_types::{
    Capabilities, CollectionSpec, Filter, Precondition, Query, ReadCheck, Result, Scope, SearchHit,
    StorageError, Version, Versioned, WriteOp, WriteOptions, WriteResult,
};

/// One-shot faults and counters on the delivery records.
#[derive(Debug, Default)]
pub(crate) struct Faults {
    pub(crate) fail_pending_write: AtomicBool,
    pub(crate) fail_queue_read: AtomicBool,
    pub(crate) lose_result_reply: AtomicBool,
    pub(crate) pending_reads: AtomicUsize,
}

/// A storage host on a bus connection.
pub(crate) struct StorageHost {
    pub(crate) backend: MemoryBackend,
    pub(crate) faults: Arc<Faults>,
    /// The scope the host binds callers to; changing it re-binds live callers.
    pub(crate) scope: Arc<Mutex<String>>,
    known: Mutex<std::collections::BTreeSet<String>>,
    _served: ServedStorage,
}

/// The delivery records of one scope.
#[derive(Default)]
pub(crate) struct QueueScope {
    pub(crate) pending: BTreeMap<String, DeliveryQueueEntry>,
    pub(crate) failed: BTreeMap<String, DeliveryQueueEntry>,
    pub(crate) results: BTreeMap<String, DeliveryTerminalResult>,
}

/// Scopes that hold at least one delivery record.
pub(crate) struct Snapshot(std::collections::HashMap<String, QueueScope>);

impl std::ops::Deref for Snapshot {
    type Target = std::collections::HashMap<String, QueueScope>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl StorageHost {
    /// Serve storage on `connection`, bound to `scope`.
    pub(crate) async fn serve(connection: &Connection, scope: &str) -> Self {
        let backend = MemoryBackend::new();
        let faults = Arc::new(Faults::default());
        let current = Arc::new(Mutex::new(scope.to_owned()));
        let bound = (backend.clone(), faults.clone(), current.clone());
        let endpoint = PerCallerEndpoint::new(move |_sender| {
            let (backend, faults, current) = bound.clone();
            let local = backend
                .for_scope(&Scope::new("tenant:fixture").ok()?)
                .ok()?;
            let docs = Arc::new(ScopeSwitchDocs {
                backend,
                faults,
                current,
            });
            Some(StorageServer::new(Storage::new(
                docs,
                Arc::clone(local.streams()),
                Arc::clone(local.kv()),
                Arc::clone(local.blobs()),
                Arc::clone(local.queues()),
                Arc::clone(local.secrets()),
            )))
        });
        let served = serve(connection, Arc::new(endpoint)).await.unwrap();
        Self {
            backend,
            faults,
            scope: current,
            known: Mutex::new(std::iter::once(scope.to_owned()).collect()),
            _served: served,
        }
    }

    pub(crate) fn set_scope(&self, scope: &str) {
        self.known.lock().unwrap().insert(scope.to_owned());
        *self.scope.lock().unwrap() = scope.to_owned();
    }

    /// Every known scope that holds delivery records, as plain maps.
    pub(crate) async fn snapshot(&self) -> Snapshot {
        let known: Vec<String> = self.known.lock().unwrap().iter().cloned().collect();
        let mut scopes = std::collections::HashMap::new();
        for scope in known {
            let queue = QueueScope {
                pending: self.pending(&scope).await,
                failed: self.failed(&scope).await,
                results: self.results(&scope).await,
            };
            if !(queue.pending.is_empty() && queue.failed.is_empty() && queue.results.is_empty()) {
                scopes.insert(scope, queue);
            }
        }
        Snapshot(scopes)
    }

    /// Direct (fault-free) handle on `scope`'s documents.
    pub(crate) fn docs(&self, scope: &str) -> Arc<dyn DocumentStore> {
        Arc::clone(
            self.backend
                .for_scope(&Scope::new(format!("tenant:{scope}")).unwrap())
                .unwrap()
                .documents(),
        )
    }

    async fn collection<T: serde::de::DeserializeOwned>(
        &self,
        scope: &str,
        name: &str,
    ) -> BTreeMap<String, T> {
        self.docs(scope)
            .query_all(name, &Query::all())
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|found| (found.id, serde_json::from_value(found.doc).unwrap()))
            .collect()
    }

    /// Entries are keyed by their idempotency key, as the wire type reports it.
    async fn entries(&self, scope: &str, name: &str) -> BTreeMap<String, DeliveryQueueEntry> {
        let raw: BTreeMap<String, StoredDelivery> = self.collection(scope, name).await;
        raw.into_values()
            .map(DeliveryQueueEntry::from)
            .map(|entry| (entry.id.clone(), entry))
            .collect()
    }

    pub(crate) async fn pending(&self, scope: &str) -> BTreeMap<String, DeliveryQueueEntry> {
        self.entries(scope, PENDING_COLLECTION).await
    }

    pub(crate) async fn failed(&self, scope: &str) -> BTreeMap<String, DeliveryQueueEntry> {
        self.entries(scope, tinychannels::storage::delivery::FAILED_COLLECTION)
            .await
    }

    pub(crate) async fn results(&self, scope: &str) -> BTreeMap<String, DeliveryTerminalResult> {
        let raw: BTreeMap<String, StoredResult> = self.collection(scope, RESULTS_COLLECTION).await;
        raw.into_values()
            .map(DeliveryTerminalResult::from)
            .map(|result| (result.id.clone(), result))
            .collect()
    }

    /// Remove every pending record of `scope`.
    pub(crate) async fn clear_pending(&self, scope: &str) {
        self.docs(scope)
            .delete_where(PENDING_COLLECTION, &Filter::default())
            .await
            .unwrap();
    }

    /// Seed a pending record through the store the module itself uses.
    pub(crate) async fn seed_pending(&self, scope: &str, entry: DeliveryQueueEntry) {
        use tinychannels::delivery::DeliveryQueueStore;
        self.known.lock().unwrap().insert(scope.to_owned());
        tinychannels::storage::DocumentDeliveryStore::new(self.docs(scope))
            .save_pending(entry)
            .await
            .unwrap();
    }

    /// Seed a terminal result.
    pub(crate) async fn seed_result(&self, scope: &str, result: DeliveryTerminalResult) {
        use tinychannels::delivery::DeliveryResultStore;
        tinychannels::storage::DocumentDeliveryStore::new(self.docs(scope))
            .save_result(result)
            .await
            .unwrap();
    }
}

/// The documents of whichever scope the host currently binds, with faults.
#[derive(Debug)]
struct ScopeSwitchDocs {
    backend: MemoryStorage,
    faults: Arc<Faults>,
    current: Arc<Mutex<String>>,
}

impl ScopeSwitchDocs {
    fn inner(&self) -> Arc<dyn DocumentStore> {
        let scope = Scope::new(format!("tenant:{}", self.current.lock().unwrap())).unwrap();
        Arc::clone(self.backend.for_scope(&scope).unwrap().documents())
    }
}

#[async_trait]
impl DocumentStore for ScopeSwitchDocs {
    fn capabilities(&self) -> Capabilities {
        self.inner().capabilities()
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        self.inner().ensure_collection(spec).await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        self.inner().get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.put_with(collection, id, doc, precondition, Default::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        if collection == PENDING_COLLECTION
            && self.faults.fail_pending_write.swap(false, Ordering::SeqCst)
        {
            return Err(StorageError::backend("fixture pending write failed"));
        }
        let version = self
            .inner()
            .put_with(collection, id, doc, precondition, options)
            .await?;
        if collection == RESULTS_COLLECTION
            && self.faults.lose_result_reply.swap(false, Ordering::SeqCst)
        {
            return Err(StorageError::backend("fixture lost terminal reply"));
        }
        Ok(version)
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        self.inner().delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &Query,
    ) -> Result<tinystoragedrivers_types::Page<Versioned<Value>>> {
        if collection == PENDING_COLLECTION {
            self.faults.pending_reads.fetch_add(1, Ordering::SeqCst);
            if self.faults.fail_queue_read.load(Ordering::SeqCst) {
                return Err(StorageError::backend("fixture pending read failed"));
            }
        }
        self.inner().query(collection, query).await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        if collection == PENDING_COLLECTION && self.faults.fail_queue_read.load(Ordering::SeqCst) {
            return Err(StorageError::backend("fixture pending read failed"));
        }
        self.inner().count(collection, filter).await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        self.inner().delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[tinystoragedrivers_types::Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        self.inner().claim(collection, filter, sort, patch).await
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.inner().atomic_batch(ops).await
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        self.inner().atomic_batch_checked(checks, ops).await
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        self.inner().search(collection, text, limit).await
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.inner().drop_collection(collection).await
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner().collections(prefix).await
    }
}
