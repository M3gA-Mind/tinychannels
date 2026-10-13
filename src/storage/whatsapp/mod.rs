//! A durable WhatsApp Web session backend on the document port.
//!
//! `whatsapp-rust` keeps everything a linked device needs (its identity, the
//! Signal sessions and pre-keys, the app-state sync keys) behind the
//! [`Backend`](wacore::store::traits::Backend) trait. This crate used to hand
//! it wacore's in-memory backend, so every restart lost the pairing and the
//! user scanned a QR code again.
//!
//! [`DocumentWhatsAppBackend`] keeps wacore's in-memory backend as the working
//! set and writes the **pairing-critical** state through to a
//! [`DocumentStore`] before the call
//! returns; [`open`](DocumentWhatsAppBackend::open) reads it back. Durable:
//! the device record, Signal identities, sessions, pre-keys, signed pre-keys,
//! sender keys, app-state sync keys, and LID to phone number mappings.
//! Everything else (collection versions and mutation MACs, which only make
//! sense together and so make app state resync from the durable keys, privacy tokens, device
//! lists, base keys, sent-message and pending-inbound buffers, message
//! secrets, group metadata) is a cache the protocol re-derives or refetches,
//! and stays in memory.
//!
//! State is keyed by an account label derived from the configured
//! `session_path`, so two WhatsApp accounts in one scope never share a device.

mod appsync;
mod protocol;
mod signal;

use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, DocumentStoreExt, Filter, Precondition, Query, StorageError,
};
use wacore::store::InMemoryBackend;
use wacore::store::error::{Result as StoreResult, StoreError};
use wacore::store::traits::DeviceStore;

const LOG_PREFIX: &str = "[whatsapp-store]";

pub(crate) const DEVICE: &str = "whatsapp_device";
pub(crate) const IDENTITY: &str = "whatsapp_identity";
pub(crate) const SESSION: &str = "whatsapp_session";
pub(crate) const PREKEY: &str = "whatsapp_prekey";
pub(crate) const SIGNED_PREKEY: &str = "whatsapp_signed_prekey";
pub(crate) const SENDER_KEY: &str = "whatsapp_sender_key";
pub(crate) const SYNC_KEY: &str = "whatsapp_sync_key";
pub(crate) const LID_MAPPING: &str = "whatsapp_lid_mapping";

/// Every collection the backend writes, for declaration and tests.
pub const COLLECTIONS: [&str; 8] = [
    DEVICE,
    IDENTITY,
    SESSION,
    PREKEY,
    SIGNED_PREKEY,
    SENDER_KEY,
    SYNC_KEY,
    LID_MAPPING,
];

/// A WhatsApp Web session backend whose pairing state survives a restart.
pub struct DocumentWhatsAppBackend {
    pub(crate) inner: InMemoryBackend,
    docs: Arc<dyn DocumentStore>,
    account: String,
}

impl std::fmt::Debug for DocumentWhatsAppBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentWhatsAppBackend")
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}

/// The account label for a configured session path: stable, and safe to use in
/// a document id.
#[must_use]
pub fn account_label(session_path: &str) -> String {
    let digest = Sha256::digest(session_path.as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn b64(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

pub(crate) fn unb64(value: &Value, field: &str) -> StoreResult<Vec<u8>> {
    let text = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| StoreError::Validation(format!("stored record has no {field}")))?;
    BASE64
        .decode(text)
        .map_err(|e| StoreError::Serialization(Box::new(e)))
}

pub(crate) fn store_err(error: StorageError) -> StoreError {
    tracing::warn!("{LOG_PREFIX} storage failure: {:?}", error.kind());
    StoreError::Database(Box::new(error))
}

pub(crate) fn json_err(error: serde_json::Error) -> StoreError {
    StoreError::Serialization(Box::new(error))
}

impl DocumentWhatsAppBackend {
    /// Open the backend for `session_path`'s account on `docs`, loading any
    /// state a previous run wrote.
    ///
    /// # Errors
    /// The store cannot be read, or a persisted record is unreadable.
    pub async fn open(docs: Arc<dyn DocumentStore>, session_path: &str) -> StoreResult<Self> {
        for name in COLLECTIONS {
            if let Err(error) = docs.ensure_collection(&CollectionSpec::new(name)).await {
                tracing::debug!(
                    "{LOG_PREFIX} ensure_collection {name} not applied: {:?}",
                    error.kind()
                );
            }
        }
        let backend = Self {
            inner: InMemoryBackend::new(),
            docs,
            account: account_label(session_path),
        };
        backend.hydrate().await?;
        tracing::debug!("{LOG_PREFIX} opened account={}", backend.account);
        Ok(backend)
    }

    fn id(&self, key: &str) -> String {
        format!("{}:{key}", self.account)
    }

    /// Write one record: the document carries `account` and `key` beside `body`.
    pub(crate) async fn put(
        &self,
        collection: &str,
        key: &str,
        mut body: Value,
    ) -> StoreResult<()> {
        if let Some(object) = body.as_object_mut() {
            object.insert("account".into(), json!(self.account));
            object.insert("key".into(), json!(key));
        }
        self.docs
            .put(collection, &self.id(key), body, Precondition::None)
            .await
            .map(|_| ())
            .map_err(store_err)
    }

    pub(crate) async fn get(&self, collection: &str, key: &str) -> StoreResult<Option<Value>> {
        Ok(self
            .docs
            .get(collection, &self.id(key))
            .await
            .map_err(store_err)?
            .map(|found| found.doc))
    }

    pub(crate) async fn remove(&self, collection: &str, key: &str) -> StoreResult<()> {
        self.docs
            .delete(collection, &self.id(key), Precondition::None)
            .await
            .map(|_| ())
            .map_err(store_err)
    }

    /// Every record this account wrote to `collection`.
    pub(crate) async fn all(&self, collection: &str) -> StoreResult<Vec<Value>> {
        let query = Query::filter(Filter::eq("account", self.account.clone()));
        Ok(self
            .docs
            .query_all(collection, &query)
            .await
            .map_err(store_err)?
            .into_iter()
            .map(|found| found.doc)
            .collect())
    }

    async fn hydrate(&self) -> StoreResult<()> {
        if let Some(doc) = self.get(DEVICE, "device").await? {
            let device = serde_json::from_value(doc.get("device").cloned().unwrap_or(Value::Null))
                .map_err(json_err)?;
            self.inner.save(&device).await?;
        }
        self.hydrate_signal().await?;
        self.hydrate_app_state().await?;
        self.hydrate_protocol().await
    }
}

#[async_trait]
impl DeviceStore for DocumentWhatsAppBackend {
    async fn save(&self, device: &wacore::store::Device) -> StoreResult<()> {
        let body = json!({ "device": serde_json::to_value(device).map_err(json_err)? });
        self.put(DEVICE, "device", body).await?;
        self.inner.save(device).await
    }

    async fn load(&self) -> StoreResult<Option<wacore::store::Device>> {
        self.inner.load().await
    }

    async fn exists(&self) -> StoreResult<bool> {
        self.inner.exists().await
    }

    async fn create(&self) -> StoreResult<i32> {
        let id = self.inner.create().await?;
        // Persist the freshly materialised device so its identity key survives
        // a restart that happens before the first explicit save.
        if self.get(DEVICE, "device").await?.is_none()
            && let Some(device) = self.inner.load().await?
        {
            let body = json!({ "device": serde_json::to_value(&device).map_err(json_err)? });
            self.put(DEVICE, "device", body).await?;
        }
        Ok(id)
    }

    async fn snapshot_db(&self, name: &str, extra_content: Option<&[u8]>) -> StoreResult<()> {
        self.inner.snapshot_db(name, extra_content).await
    }

    async fn resource_report(&self) -> wacore::stats::StorageResourceReport {
        self.inner.resource_report().await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
