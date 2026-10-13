use super::*;
use bytes::Bytes;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
use wacore::store::PersistenceManager;
use wacore::store::traits::{
    AppStateSyncKey, AppSyncStore, Backend, LidPnMappingEntry, ProtocolStore, SignalStore,
};

fn docs() -> Arc<dyn DocumentStore> {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    Arc::clone(storage.documents())
}

async fn open(docs: &Arc<dyn DocumentStore>, path: &str) -> DocumentWhatsAppBackend {
    DocumentWhatsAppBackend::open(Arc::clone(docs), path)
        .await
        .unwrap()
}

#[test]
fn account_labels_are_stable_distinct_and_id_safe() {
    let a = account_label("~/.openhuman/a.db");
    assert_eq!(a, account_label("~/.openhuman/a.db"));
    assert_ne!(a, account_label("~/.openhuman/b.db"));
    assert_eq!(a.len(), 16);
    assert!(a.bytes().all(|b| b.is_ascii_hexdigit()));
}

#[tokio::test]
async fn a_linked_device_is_the_same_device_after_a_restart() {
    let docs = docs();
    let backend: Arc<dyn Backend> = Arc::new(open(&docs, "session").await);
    let manager = PersistenceManager::new(Arc::clone(&backend)).await.unwrap();
    let device = manager.get_device_snapshot().await;
    backend.save(&device).await.unwrap();
    let before = serde_json::to_value(&device).unwrap();
    drop(manager);
    drop(backend);

    let backend: Arc<dyn Backend> = Arc::new(open(&docs, "session").await);
    assert!(
        backend.exists().await.unwrap(),
        "the device record was loaded"
    );
    let manager = PersistenceManager::new(backend).await.unwrap();
    let after = serde_json::to_value(manager.get_device_snapshot().await).unwrap();
    assert_eq!(
        before, after,
        "the identity keys survive, so no new QR scan is needed"
    );
}

#[tokio::test]
async fn a_fresh_store_has_no_device_and_creating_one_persists_it() {
    let docs = docs();
    let backend = open(&docs, "session").await;
    assert!(!backend.exists().await.unwrap());
    backend.create().await.unwrap();
    let created = serde_json::to_value(backend.load().await.unwrap().unwrap()).unwrap();
    drop(backend);

    let reopened = open(&docs, "session").await;
    let loaded = serde_json::to_value(reopened.load().await.unwrap().unwrap()).unwrap();
    assert_eq!(created, loaded);
}

#[tokio::test]
async fn signal_state_survives_a_restart_and_deletes_stay_deleted() {
    let docs = docs();
    let backend = open(&docs, "session").await;
    backend.put_identity("alice.0", [7; 32]).await.unwrap();
    backend.put_identity("bob.0", [8; 32]).await.unwrap();
    backend
        .put_session("alice.0", b"session-bytes")
        .await
        .unwrap();
    backend.put_session("bob.0", b"gone").await.unwrap();
    backend.store_prekey(5, b"prekey-5", false).await.unwrap();
    backend.store_prekey(9, b"prekey-9", false).await.unwrap();
    backend.mark_prekeys_uploaded(&[9]).await.unwrap();
    backend.store_signed_prekey(1, b"signed-1").await.unwrap();
    backend
        .put_sender_key("group.alice", b"sender-key")
        .await
        .unwrap();
    backend.delete_identity("bob.0").await.unwrap();
    backend.delete_session("bob.0").await.unwrap();
    backend.remove_prekey(5).await.unwrap();
    drop(backend);

    let reopened = open(&docs, "session").await;
    assert_eq!(
        reopened.load_identity("alice.0").await.unwrap(),
        Some([7; 32])
    );
    assert_eq!(reopened.load_identity("bob.0").await.unwrap(), None);
    assert_eq!(
        reopened.get_session("alice.0").await.unwrap(),
        Some(Bytes::from_static(b"session-bytes"))
    );
    assert_eq!(reopened.get_session("bob.0").await.unwrap(), None);
    assert_eq!(reopened.load_prekey(5).await.unwrap(), None);
    assert_eq!(
        reopened.load_prekey(9).await.unwrap(),
        Some(Bytes::from_static(b"prekey-9"))
    );
    assert_eq!(reopened.get_max_prekey_id().await.unwrap(), 9);
    assert_eq!(
        reopened.load_signed_prekey(1).await.unwrap().as_deref(),
        Some(&b"signed-1"[..])
    );
    assert_eq!(
        reopened
            .get_sender_key("group.alice")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"sender-key"[..])
    );
}

#[tokio::test]
async fn batch_writes_are_durable() {
    let docs = docs();
    let backend = open(&docs, "session").await;
    backend
        .put_sessions_batch(&[
            ("a".into(), Bytes::from_static(b"1")),
            ("b".into(), Bytes::from_static(b"2")),
        ])
        .await
        .unwrap();
    backend
        .store_prekeys_batch(
            &[
                (1, Bytes::from_static(b"p1")),
                (2, Bytes::from_static(b"p2")),
            ],
            true,
        )
        .await
        .unwrap();
    backend
        .put_identities_batch(&[("a".into(), [1; 32]), ("b".into(), [2; 32])])
        .await
        .unwrap();
    drop(backend);

    let reopened = open(&docs, "session").await;
    assert_eq!(
        reopened.get_session("b").await.unwrap(),
        Some(Bytes::from_static(b"2"))
    );
    assert_eq!(reopened.load_prekeys_batch(&[1, 2]).await.unwrap().len(), 2);
    assert_eq!(reopened.load_identity("b").await.unwrap(), Some([2; 32]));
}

#[tokio::test]
async fn app_state_sync_keys_and_lid_mappings_are_durable() {
    let docs = docs();
    let backend = open(&docs, "session").await;
    backend
        .set_sync_key(
            &[0xAB, 0xCD],
            AppStateSyncKey {
                key_data: vec![1, 2, 3],
                fingerprint: vec![4],
                timestamp: 1700,
            },
        )
        .await
        .unwrap();
    backend
        .put_lid_mapping(&LidPnMappingEntry {
            lid: "100".into(),
            phone_number: "15550001".into(),
            created_at: 1,
            updated_at: 2,
            learning_source: "usync".into(),
        })
        .await
        .unwrap();
    drop(backend);

    let reopened = open(&docs, "session").await;
    let key = reopened.get_sync_key(&[0xAB, 0xCD]).await.unwrap().unwrap();
    assert_eq!(key.key_data, vec![1, 2, 3]);
    assert_eq!(
        reopened.get_latest_sync_key_id().await.unwrap(),
        Some(vec![0xAB, 0xCD])
    );
    let mapping = reopened.get_lid_mapping("100").await.unwrap().unwrap();
    assert_eq!(mapping.phone_number, "15550001");
    assert_eq!(
        reopened
            .get_pn_mapping("15550001")
            .await
            .unwrap()
            .unwrap()
            .lid,
        "100"
    );
}

#[tokio::test]
async fn caches_the_protocol_refetches_stay_in_memory() {
    let docs = docs();
    let backend = open(&docs, "session").await;
    backend.put_group_metadata("g@g.us", b"meta").await.unwrap();
    backend.save_base_key("addr", "msg", b"base").await.unwrap();
    drop(backend);

    let reopened = open(&docs, "session").await;
    assert_eq!(reopened.get_group_metadata("g@g.us").await.unwrap(), None);
    assert!(
        !reopened
            .has_same_base_key("addr", "msg", b"base")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn two_accounts_in_one_scope_never_share_a_device() {
    let docs = docs();
    let one = open(&docs, "one.db").await;
    one.put_identity("peer", [1; 32]).await.unwrap();
    one.create().await.unwrap();
    let other = open(&docs, "two.db").await;
    assert_eq!(other.load_identity("peer").await.unwrap(), None);
    assert!(!other.exists().await.unwrap());
    drop(one);
    let one_again = open(&docs, "one.db").await;
    assert_eq!(
        one_again.load_identity("peer").await.unwrap(),
        Some([1; 32])
    );
}

#[tokio::test]
async fn a_failing_store_fails_the_write_instead_of_pretending_it_is_durable() {
    let docs = docs();
    let backend = open(&docs, "session").await;
    docs.drop_collection(IDENTITY).await.unwrap();
    // The collection is gone but the port recreates on demand; make the
    // account unreadable instead by writing a malformed record and reopening.
    docs.put(
        IDENTITY,
        &format!("{}:bad", account_label("session")),
        serde_json::json!({ "account": account_label("session"), "key": "bad", "value": "!!not-base64!!" }),
        Precondition::None,
    )
    .await
    .unwrap();
    drop(backend);
    assert!(
        DocumentWhatsAppBackend::open(docs, "session")
            .await
            .is_err(),
        "a corrupt persisted record is reported, not silently skipped"
    );
}
