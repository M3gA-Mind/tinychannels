//! Protocol-level state. LID to phone-number mappings are durable (they decide
//! which session a message belongs to); the rest is cache the protocol
//! refetches, so it is forwarded to the in-memory working set.

use async_trait::async_trait;
use wacore::store::error::Result;
use wacore::store::traits::{
    DeviceListRecord, LidPnMappingEntry, MsgSecretEntry, MsgSecretStore, PendingInboundKey,
    PendingInboundRow, ProtocolStore, TcTokenEntry,
};

use super::{DocumentWhatsAppBackend, LID_MAPPING, json_err};

impl DocumentWhatsAppBackend {
    pub(super) async fn hydrate_protocol(&self) -> Result<()> {
        for doc in self.all(LID_MAPPING).await? {
            let entry: LidPnMappingEntry =
                serde_json::from_value(doc.get("entry").cloned().unwrap_or_default())
                    .map_err(json_err)?;
            self.inner.put_lid_mapping(&entry).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ProtocolStore for DocumentWhatsAppBackend {
    async fn get_sender_key_devices(&self, group_jid: &str) -> Result<Vec<(String, bool)>> {
        self.inner.get_sender_key_devices(group_jid).await
    }

    async fn set_sender_key_status(&self, group_jid: &str, entries: &[(&str, bool)]) -> Result<()> {
        self.inner.set_sender_key_status(group_jid, entries).await
    }

    async fn clear_sender_key_devices(&self, group_jid: &str) -> Result<()> {
        self.inner.clear_sender_key_devices(group_jid).await
    }

    async fn delete_sender_key_device_rows(&self, device_jids: &[&str]) -> Result<()> {
        self.inner.delete_sender_key_device_rows(device_jids).await
    }

    async fn clear_all_sender_key_devices(&self) -> Result<()> {
        self.inner.clear_all_sender_key_devices().await
    }

    async fn get_lid_mapping(&self, lid: &str) -> Result<Option<LidPnMappingEntry>> {
        self.inner.get_lid_mapping(lid).await
    }

    async fn get_pn_mapping(&self, phone: &str) -> Result<Option<LidPnMappingEntry>> {
        self.inner.get_pn_mapping(phone).await
    }

    async fn put_lid_mapping(&self, entry: &LidPnMappingEntry) -> Result<()> {
        let body = serde_json::json!({ "entry": serde_json::to_value(entry).map_err(json_err)? });
        self.put(LID_MAPPING, &entry.lid, body).await?;
        self.inner.put_lid_mapping(entry).await
    }

    async fn put_lid_mappings(&self, entries: &[LidPnMappingEntry]) -> Result<()> {
        for entry in entries {
            self.put_lid_mapping(entry).await?;
        }
        Ok(())
    }

    async fn get_all_lid_mappings(&self) -> Result<Vec<LidPnMappingEntry>> {
        self.inner.get_all_lid_mappings().await
    }

    async fn save_base_key(&self, address: &str, message_id: &str, base_key: &[u8]) -> Result<()> {
        self.inner
            .save_base_key(address, message_id, base_key)
            .await
    }

    async fn has_same_base_key(
        &self,
        address: &str,
        message_id: &str,
        current_base_key: &[u8],
    ) -> Result<bool> {
        self.inner
            .has_same_base_key(address, message_id, current_base_key)
            .await
    }

    async fn delete_base_key(&self, address: &str, message_id: &str) -> Result<()> {
        self.inner.delete_base_key(address, message_id).await
    }

    async fn update_device_list(&self, record: DeviceListRecord) -> Result<()> {
        self.inner.update_device_list(record).await
    }

    async fn update_device_lists(&self, records: Vec<DeviceListRecord>) -> Result<()> {
        self.inner.update_device_lists(records).await
    }

    async fn get_devices(&self, user: &str) -> Result<Option<DeviceListRecord>> {
        self.inner.get_devices(user).await
    }

    async fn delete_devices(&self, user: &str) -> Result<()> {
        self.inner.delete_devices(user).await
    }

    async fn get_group_metadata(&self, group_jid: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_group_metadata(group_jid).await
    }

    async fn put_group_metadata(&self, group_jid: &str, blob: &[u8]) -> Result<()> {
        self.inner.put_group_metadata(group_jid, blob).await
    }

    async fn delete_group_metadata(&self, group_jid: &str) -> Result<()> {
        self.inner.delete_group_metadata(group_jid).await
    }

    async fn get_tc_token(&self, jid: &str) -> Result<Option<TcTokenEntry>> {
        self.inner.get_tc_token(jid).await
    }

    async fn put_tc_token(&self, jid: &str, entry: &TcTokenEntry) -> Result<()> {
        self.inner.put_tc_token(jid, entry).await
    }

    async fn delete_tc_token(&self, jid: &str) -> Result<()> {
        self.inner.delete_tc_token(jid).await
    }

    async fn get_all_tc_token_jids(&self) -> Result<Vec<String>> {
        self.inner.get_all_tc_token_jids().await
    }

    async fn delete_expired_tc_tokens(&self, token_cutoff: i64, sender_cutoff: i64) -> Result<u32> {
        self.inner
            .delete_expired_tc_tokens(token_cutoff, sender_cutoff)
            .await
    }

    async fn touch_tc_token_sender_timestamp(
        &self,
        jid: &str,
        sender_timestamp: i64,
    ) -> Result<()> {
        self.inner
            .touch_tc_token_sender_timestamp(jid, sender_timestamp)
            .await
    }

    async fn store_received_tc_token(
        &self,
        jid: &str,
        token: &[u8],
        token_timestamp: i64,
    ) -> Result<()> {
        self.inner
            .store_received_tc_token(jid, token, token_timestamp)
            .await
    }

    async fn store_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
        payload: &[u8],
    ) -> Result<()> {
        self.inner
            .store_sent_message(chat_jid, message_id, payload)
            .await
    }

    async fn take_sent_message(&self, chat_jid: &str, message_id: &str) -> Result<Option<Vec<u8>>> {
        self.inner.take_sent_message(chat_jid, message_id).await
    }

    async fn delete_expired_sent_messages(&self, cutoff_timestamp: i64) -> Result<u32> {
        self.inner
            .delete_expired_sent_messages(cutoff_timestamp)
            .await
    }

    async fn store_pending_inbound(
        &self,
        chat: &str,
        sender: &str,
        id: &str,
        message: &[u8],
    ) -> Result<()> {
        self.inner
            .store_pending_inbound(chat, sender, id, message)
            .await
    }

    async fn get_pending_inbound(
        &self,
        chat: &str,
        sender: &str,
        id: &str,
    ) -> Result<Option<Vec<u8>>> {
        self.inner.get_pending_inbound(chat, sender, id).await
    }

    async fn delete_pending_inbound(&self, chat: &str, sender: &str, id: &str) -> Result<()> {
        self.inner.delete_pending_inbound(chat, sender, id).await
    }

    async fn delete_expired_pending_inbound(&self, cutoff_timestamp: i64) -> Result<u32> {
        self.inner
            .delete_expired_pending_inbound(cutoff_timestamp)
            .await
    }

    async fn store_pending_inbound_batch(&self, rows: &[PendingInboundRow<'_>]) -> Result<()> {
        self.inner.store_pending_inbound_batch(rows).await
    }

    async fn delete_pending_inbound_batch(&self, keys: &[PendingInboundKey<'_>]) -> Result<()> {
        self.inner.delete_pending_inbound_batch(keys).await
    }
}

#[async_trait]
impl MsgSecretStore for DocumentWhatsAppBackend {
    async fn put_msg_secret(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
        secret: &[u8; wacore::reporting_token::MESSAGE_SECRET_SIZE],
    ) -> Result<()> {
        self.inner
            .put_msg_secret(chat, sender, msg_id, secret)
            .await
    }

    async fn put_msg_secrets(&self, entries: Vec<MsgSecretEntry>) -> Result<usize> {
        self.inner.put_msg_secrets(entries).await
    }

    async fn get_msg_secret(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> Result<Option<Vec<u8>>> {
        self.inner.get_msg_secret(chat, sender, msg_id).await
    }

    async fn get_msg_secret_with_ts(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> Result<Option<(Vec<u8>, i64)>> {
        self.inner
            .get_msg_secret_with_ts(chat, sender, msg_id)
            .await
    }

    async fn delete_expired_msg_secrets(&self, cutoff_timestamp: i64) -> Result<u32> {
        self.inner
            .delete_expired_msg_secrets(cutoff_timestamp)
            .await
    }
}
