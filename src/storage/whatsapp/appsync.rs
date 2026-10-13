//! App-state sync keys are durable. Collection versions and mutation MACs are
//! a consistent pair that stays in memory.

use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::json;
use wacore::appstate::hash::HashState;
use wacore::store::error::Result;
use wacore::store::traits::{AppStateSyncKey, AppSyncStore};
use wacore_appstate::processor::AppStateMutationMAC;

use super::{DocumentWhatsAppBackend, SYNC_KEY, json_err};

fn key_name(key_id: &[u8]) -> String {
    key_id.iter().map(|b| format!("{b:02x}")).collect()
}

impl DocumentWhatsAppBackend {
    pub(super) async fn hydrate_app_state(&self) -> Result<()> {
        for doc in self.all(SYNC_KEY).await? {
            let key_id = super::unb64(&doc, "key_id")?;
            let key: AppStateSyncKey =
                serde_json::from_value(doc.get("sync_key").cloned().unwrap_or_default()).map_err(json_err)?;
            self.inner.set_sync_key(&key_id, key).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AppSyncStore for DocumentWhatsAppBackend {
    async fn get_sync_key(&self, key_id: &[u8]) -> Result<Option<AppStateSyncKey>> {
        self.inner.get_sync_key(key_id).await
    }

    async fn set_sync_key(&self, key_id: &[u8], key: AppStateSyncKey) -> Result<()> {
        let body = json!({
            "key_id": super::b64(key_id),
            "sync_key": serde_json::to_value(&key).map_err(json_err)?,
        });
        self.put(SYNC_KEY, &key_name(key_id), body).await?;
        self.inner.set_sync_key(key_id, key).await
    }

    async fn get_version(&self, name: &str) -> Result<HashState> {
        self.inner.get_version(name).await
    }

    async fn set_version(&self, name: &str, state: HashState) -> Result<()> {
        // Versions and mutation MACs are one consistent pair; both stay in
        // memory so a restart resyncs app state from scratch (with the durable
        // sync keys) instead of trusting a version whose MACs are gone.
        self.inner.set_version(name, state).await
    }

    async fn put_mutation_macs(
        &self,
        name: &str,
        version: u64,
        mutations: &[AppStateMutationMAC],
    ) -> Result<()> {
        self.inner.put_mutation_macs(name, version, mutations).await
    }

    async fn get_mutation_mac(&self, name: &str, index_mac: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_mutation_mac(name, index_mac).await
    }

    async fn get_mutation_macs(
        &self,
        name: &str,
        index_macs: &[[u8; 32]],
    ) -> Result<HashMap<[u8; 32], Vec<u8>>> {
        self.inner.get_mutation_macs(name, index_macs).await
    }

    async fn delete_mutation_macs(&self, name: &str, index_macs: &[Vec<u8>]) -> Result<()> {
        self.inner.delete_mutation_macs(name, index_macs).await
    }

    async fn clear_mutation_macs(&self, name: &str) -> Result<()> {
        self.inner.clear_mutation_macs(name).await
    }

    async fn get_latest_sync_key_id(&self) -> Result<Option<Vec<u8>>> {
        self.inner.get_latest_sync_key_id().await
    }
}
