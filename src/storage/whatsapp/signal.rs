//! Signal protocol state: durable write-through over the in-memory working set.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{Value, json};
use wacore::store::error::Result;
use wacore::store::traits::SignalStore;

use super::{
    DocumentWhatsAppBackend, IDENTITY, PREKEY, SENDER_KEY, SESSION, SIGNED_PREKEY, b64, unb64,
};

impl DocumentWhatsAppBackend {
    pub(super) async fn hydrate_signal(&self) -> Result<()> {
        for doc in self.all(IDENTITY).await? {
            let key: [u8; 32] = unb64(&doc, "data_b64")?.try_into().map_err(|_| {
                wacore::store::error::StoreError::Validation("identity key length".into())
            })?;
            self.inner.put_identity(&text(&doc, "key"), key).await?;
        }
        for doc in self.all(SESSION).await? {
            self.inner
                .put_session(&text(&doc, "key"), &unb64(&doc, "data_b64")?)
                .await?;
        }
        for doc in self.all(PREKEY).await? {
            let uploaded = doc
                .get("uploaded")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            self.inner
                .store_prekey(id(&doc)?, &unb64(&doc, "data_b64")?, uploaded)
                .await?;
        }
        for doc in self.all(SIGNED_PREKEY).await? {
            self.inner
                .store_signed_prekey(id(&doc)?, &unb64(&doc, "data_b64")?)
                .await?;
        }
        for doc in self.all(SENDER_KEY).await? {
            self.inner
                .put_sender_key(&text(&doc, "key"), &unb64(&doc, "data_b64")?)
                .await?;
        }
        Ok(())
    }
}

fn text(doc: &Value, field: &str) -> String {
    doc.get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn id(doc: &Value) -> Result<u32> {
    text(doc, "key")
        .parse()
        .map_err(|_| wacore::store::error::StoreError::Validation("pre-key id".into()))
}

#[async_trait]
impl SignalStore for DocumentWhatsAppBackend {
    async fn put_identity(&self, address: &str, key: [u8; 32]) -> Result<()> {
        self.put(IDENTITY, address, json!({ "data_b64": b64(&key) }))
            .await?;
        self.inner.put_identity(address, key).await
    }

    async fn put_identities_batch(&self, identities: &[(Arc<str>, [u8; 32])]) -> Result<()> {
        for (address, key) in identities {
            self.put_identity(address, *key).await?;
        }
        Ok(())
    }

    async fn load_identity(&self, address: &str) -> Result<Option<[u8; 32]>> {
        self.inner.load_identity(address).await
    }

    async fn delete_identity(&self, address: &str) -> Result<()> {
        self.remove(IDENTITY, address).await?;
        self.inner.delete_identity(address).await
    }

    async fn get_session(&self, address: &str) -> Result<Option<Bytes>> {
        self.inner.get_session(address).await
    }

    async fn put_session(&self, address: &str, session: &[u8]) -> Result<()> {
        self.put(SESSION, address, json!({ "data_b64": b64(session) }))
            .await?;
        self.inner.put_session(address, session).await
    }

    async fn put_sessions_batch(&self, sessions: &[(Arc<str>, Bytes)]) -> Result<()> {
        for (address, session) in sessions {
            self.put_session(address, session).await?;
        }
        Ok(())
    }

    async fn delete_session(&self, address: &str) -> Result<()> {
        self.remove(SESSION, address).await?;
        self.inner.delete_session(address).await
    }

    async fn has_session(&self, address: &str) -> Result<bool> {
        self.inner.has_session(address).await
    }

    async fn has_signal_state_for_user(&self, user: &str) -> Result<bool> {
        self.inner.has_signal_state_for_user(user).await
    }

    async fn store_prekey(&self, id: u32, record: &[u8], uploaded: bool) -> Result<()> {
        self.put(
            PREKEY,
            &id.to_string(),
            json!({ "data_b64": b64(record), "uploaded": uploaded }),
        )
        .await?;
        self.inner.store_prekey(id, record, uploaded).await
    }

    async fn store_prekeys_batch(&self, keys: &[(u32, Bytes)], uploaded: bool) -> Result<()> {
        for (id, record) in keys {
            self.store_prekey(*id, record, uploaded).await?;
        }
        Ok(())
    }

    async fn load_prekey(&self, id: u32) -> Result<Option<Bytes>> {
        self.inner.load_prekey(id).await
    }

    async fn load_prekeys_batch(&self, ids: &[u32]) -> Result<Vec<(u32, Bytes)>> {
        self.inner.load_prekeys_batch(ids).await
    }

    async fn mark_prekeys_uploaded(&self, ids: &[u32]) -> Result<()> {
        for id in ids {
            if let Some(mut doc) = self.get(PREKEY, &id.to_string()).await? {
                doc["uploaded"] = json!(true);
                self.put(PREKEY, &id.to_string(), doc).await?;
            }
        }
        self.inner.mark_prekeys_uploaded(ids).await
    }

    async fn remove_prekey(&self, id: u32) -> Result<()> {
        self.remove(PREKEY, &id.to_string()).await?;
        self.inner.remove_prekey(id).await
    }

    async fn get_max_prekey_id(&self) -> Result<u32> {
        self.inner.get_max_prekey_id().await
    }

    async fn store_signed_prekey(&self, id: u32, record: &[u8]) -> Result<()> {
        self.put(
            SIGNED_PREKEY,
            &id.to_string(),
            json!({ "data_b64": b64(record) }),
        )
        .await?;
        self.inner.store_signed_prekey(id, record).await
    }

    async fn load_signed_prekey(&self, id: u32) -> Result<Option<Vec<u8>>> {
        self.inner.load_signed_prekey(id).await
    }

    async fn load_all_signed_prekeys(&self) -> Result<Vec<(u32, Vec<u8>)>> {
        self.inner.load_all_signed_prekeys().await
    }

    async fn remove_signed_prekey(&self, id: u32) -> Result<()> {
        self.remove(SIGNED_PREKEY, &id.to_string()).await?;
        self.inner.remove_signed_prekey(id).await
    }

    async fn put_sender_key(&self, address: &str, record: &[u8]) -> Result<()> {
        self.put(SENDER_KEY, address, json!({ "data_b64": b64(record) }))
            .await?;
        self.inner.put_sender_key(address, record).await
    }

    async fn put_sender_keys_batch(&self, sender_keys: &[(Arc<str>, Bytes)]) -> Result<()> {
        for (address, record) in sender_keys {
            self.put_sender_key(address, record).await?;
        }
        Ok(())
    }

    async fn get_sender_key(&self, address: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_sender_key(address).await
    }

    async fn delete_sender_key(&self, address: &str) -> Result<()> {
        self.remove(SENDER_KEY, address).await?;
        self.inner.delete_sender_key(address).await
    }
}
