use super::*;
use bytes::Bytes;
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;
use wacore::store::PersistenceManager;
use wacore::store::traits::{
    AppStateSyncKey, AppSyncStore, Backend, LidPnMappingEntry, ProtocolStore, SignalStore,
};

fn docs() -> Arc<dyn DocumentStore> {
    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("tenant:whatsapp-tests").unwrap())
        .unwrap();
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
        serde_json::json!({ "account": account_label("session"), "key": "bad", "data_b64": "!!not-base64!!" }),
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

#[tokio::test]
async fn signal_store_reads_writes_and_deletes_through_the_working_set() {
    let docs = docs();
    let b = open(&docs, "session").await;
    b.put_identity("a", [1; 32]).await.unwrap();
    assert_eq!(b.load_identity("a").await.unwrap(), Some([1; 32]));
    b.delete_identity("a").await.unwrap();
    assert_eq!(b.load_identity("a").await.unwrap(), None);

    b.put_session("s.0", b"sess").await.unwrap();
    assert!(b.has_session("s.0").await.unwrap());
    // Answered by the working set; the call must reach it without erroring.
    b.has_signal_state_for_user("s").await.unwrap();
    b.delete_session("s.0").await.unwrap();
    assert!(!b.has_session("s.0").await.unwrap());

    b.store_prekey(3, b"k3", false).await.unwrap();
    assert_eq!(
        b.load_prekey(3).await.unwrap(),
        Some(Bytes::from_static(b"k3"))
    );
    assert_eq!(b.get_max_prekey_id().await.unwrap(), 3);
    b.mark_prekeys_uploaded(&[3, 99]).await.unwrap();
    b.remove_prekey(3).await.unwrap();
    assert_eq!(b.load_prekey(3).await.unwrap(), None);

    b.store_signed_prekey(7, b"sp").await.unwrap();
    assert_eq!(b.load_signed_prekey(7).await.unwrap(), Some(b"sp".to_vec()));
    assert_eq!(b.load_all_signed_prekeys().await.unwrap().len(), 1);
    b.remove_signed_prekey(7).await.unwrap();
    assert_eq!(b.load_signed_prekey(7).await.unwrap(), None);

    b.put_sender_keys_batch(&[("g".into(), Bytes::from_static(b"sk"))])
        .await
        .unwrap();
    assert_eq!(b.get_sender_key("g").await.unwrap(), Some(b"sk".to_vec()));
    drop(b);
    let reopened = open(&docs, "session").await;
    assert_eq!(
        reopened.get_sender_key("g").await.unwrap(),
        Some(b"sk".to_vec())
    );
    reopened.delete_sender_key("g").await.unwrap();
    assert_eq!(reopened.get_sender_key("g").await.unwrap(), None);
}

#[tokio::test]
async fn hydration_restores_every_durable_signal_kind_after_a_restart() {
    let docs = docs();
    let b = open(&docs, "session").await;
    b.put_identity("a", [4; 32]).await.unwrap();
    b.put_session("a", b"sess").await.unwrap();
    b.store_prekey(1, b"p1", true).await.unwrap();
    b.store_signed_prekey(2, b"sp2").await.unwrap();
    b.put_sender_key("g", b"sk").await.unwrap();
    drop(b);

    let r = open(&docs, "session").await;
    assert_eq!(r.load_identity("a").await.unwrap(), Some([4; 32]));
    assert!(r.has_session("a").await.unwrap());
    assert_eq!(
        r.load_prekey(1).await.unwrap(),
        Some(Bytes::from_static(b"p1"))
    );
    assert_eq!(
        r.load_signed_prekey(2).await.unwrap(),
        Some(b"sp2".to_vec())
    );
    assert_eq!(r.get_sender_key("g").await.unwrap(), Some(b"sk".to_vec()));
}

#[tokio::test]
async fn app_sync_state_is_served_from_the_working_set() {
    use wacore::appstate::hash::HashState;
    use wacore::appstate::processor::AppStateMutationMAC;

    let b = open(&docs(), "session").await;
    assert!(b.get_sync_key(&[1]).await.unwrap().is_none());
    b.set_version("regular", HashState::default())
        .await
        .unwrap();
    assert_eq!(b.get_version("regular").await.unwrap().version, 0);

    let index = [9u8; 32];
    b.put_mutation_macs(
        "regular",
        1,
        &[AppStateMutationMAC {
            index_mac: index.to_vec(),
            value_mac: b"v".to_vec(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(
        b.get_mutation_mac("regular", &index).await.unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(
        b.get_mutation_macs("regular", &[index])
            .await
            .unwrap()
            .len(),
        1
    );
    b.delete_mutation_macs("regular", &[index.to_vec()])
        .await
        .unwrap();
    assert_eq!(b.get_mutation_mac("regular", &index).await.unwrap(), None);
    b.put_mutation_macs(
        "regular",
        2,
        &[AppStateMutationMAC {
            index_mac: index.to_vec(),
            value_mac: b"w".to_vec(),
        }],
    )
    .await
    .unwrap();
    b.clear_mutation_macs("regular").await.unwrap();
    assert_eq!(b.get_mutation_mac("regular", &index).await.unwrap(), None);
}

#[tokio::test]
async fn protocol_store_caches_round_trip_through_the_working_set() {
    use wacore::store::traits::{
        DeviceInfo, DeviceListRecord, MsgSecretEntry, MsgSecretStore, PendingInboundKey,
        PendingInboundRow, TcTokenEntry,
    };

    let b = open(&docs(), "session").await;

    b.set_sender_key_status("g@g.us", &[("d1", true), ("d2", false)])
        .await
        .unwrap();
    assert_eq!(b.get_sender_key_devices("g@g.us").await.unwrap().len(), 2);
    b.delete_sender_key_device_rows(&["d2"]).await.unwrap();
    b.clear_sender_key_devices("g@g.us").await.unwrap();
    b.clear_all_sender_key_devices().await.unwrap();
    assert!(b.get_sender_key_devices("g@g.us").await.unwrap().is_empty());

    let lid = |lid: &str, pn: &str| LidPnMappingEntry {
        lid: lid.into(),
        phone_number: pn.into(),
        created_at: 1,
        updated_at: 2,
        learning_source: "usync".into(),
    };
    b.put_lid_mappings(&[lid("1", "11"), lid("2", "22")])
        .await
        .unwrap();
    assert_eq!(b.get_all_lid_mappings().await.unwrap().len(), 2);

    b.save_base_key("addr", "m", b"bk").await.unwrap();
    assert!(b.has_same_base_key("addr", "m", b"bk").await.unwrap());
    b.delete_base_key("addr", "m").await.unwrap();
    assert!(!b.has_same_base_key("addr", "m", b"bk").await.unwrap());

    let record = |user: &str| DeviceListRecord {
        user: user.into(),
        devices: vec![DeviceInfo::new(0, None)],
        timestamp: 5,
        phash: None,
        raw_id: None,
    };
    b.update_device_list(record("u1")).await.unwrap();
    b.update_device_lists(vec![record("u2")]).await.unwrap();
    assert!(b.get_devices("u2").await.unwrap().is_some());
    b.delete_devices("u1").await.unwrap();
    assert!(b.get_devices("u1").await.unwrap().is_none());

    b.put_group_metadata("g", b"m").await.unwrap();
    assert_eq!(
        b.get_group_metadata("g").await.unwrap(),
        Some(b"m".to_vec())
    );
    b.delete_group_metadata("g").await.unwrap();
    assert_eq!(b.get_group_metadata("g").await.unwrap(), None);

    b.put_tc_token(
        "j",
        &TcTokenEntry {
            token: b"t".to_vec(),
            token_timestamp: 10,
            sender_timestamp: None,
        },
    )
    .await
    .unwrap();
    assert!(b.get_tc_token("j").await.unwrap().is_some());
    b.touch_tc_token_sender_timestamp("j", 11).await.unwrap();
    b.store_received_tc_token("k", b"t2", 12).await.unwrap();
    assert_eq!(b.get_all_tc_token_jids().await.unwrap().len(), 2);
    assert_eq!(b.delete_expired_tc_tokens(100, 100).await.unwrap(), 2);
    b.put_tc_token(
        "j",
        &TcTokenEntry {
            token: b"t".to_vec(),
            token_timestamp: 10,
            sender_timestamp: None,
        },
    )
    .await
    .unwrap();
    b.delete_tc_token("j").await.unwrap();
    assert!(b.get_tc_token("j").await.unwrap().is_none());

    b.store_sent_message("c", "m1", b"payload").await.unwrap();
    assert_eq!(
        b.take_sent_message("c", "m1").await.unwrap(),
        Some(b"payload".to_vec())
    );
    assert_eq!(b.take_sent_message("c", "m1").await.unwrap(), None);
    b.store_sent_message("c", "m2", b"p").await.unwrap();
    b.delete_expired_sent_messages(i64::MAX).await.unwrap();

    b.store_pending_inbound("c", "s", "i1", b"m").await.unwrap();
    assert_eq!(
        b.get_pending_inbound("c", "s", "i1").await.unwrap(),
        Some(b"m".to_vec())
    );
    b.delete_pending_inbound("c", "s", "i1").await.unwrap();
    b.store_pending_inbound_batch(&[PendingInboundRow {
        chat: "c",
        sender: "s",
        id: "i2",
        message: b"m2",
    }])
    .await
    .unwrap();
    assert!(
        b.get_pending_inbound("c", "s", "i2")
            .await
            .unwrap()
            .is_some()
    );
    b.delete_pending_inbound_batch(&[PendingInboundKey {
        chat: "c",
        sender: "s",
        id: "i2",
    }])
    .await
    .unwrap();
    assert!(
        b.get_pending_inbound("c", "s", "i2")
            .await
            .unwrap()
            .is_none()
    );
    b.store_pending_inbound("c", "s", "i3", b"m").await.unwrap();
    b.delete_expired_pending_inbound(i64::MAX).await.unwrap();

    b.put_msg_secret("c", "s", "ms1", &[5; 32]).await.unwrap();
    assert_eq!(
        b.get_msg_secret("c", "s", "ms1").await.unwrap(),
        Some(vec![5; 32])
    );
    assert!(
        b.get_msg_secret_with_ts("c", "s", "ms1")
            .await
            .unwrap()
            .is_some()
    );
    let entry = MsgSecretEntry {
        chat: "c".into(),
        sender: "s".into(),
        msg_id: "ms2".into(),
        secret: [6; 32],
        expires_at: 1,
        message_ts: 0,
    };
    assert_eq!(b.put_msg_secrets(vec![entry]).await.unwrap(), 1);
    b.delete_expired_msg_secrets(i64::MAX).await.unwrap();
}

#[tokio::test]
async fn debug_output_names_the_account_but_not_its_secrets() {
    let b = open(&docs(), "session").await;
    b.put_identity("a", [9; 32]).await.unwrap();
    let shown = format!("{b:?}");
    assert!(shown.contains("DocumentWhatsAppBackend"));
    assert!(shown.contains(&account_label("session")));
    assert!(!shown.contains("09, 09"));
}

#[test]
fn storage_and_json_errors_map_to_distinct_store_errors() {
    let storage = store_err(StorageError::unavailable("down"));
    assert!(matches!(storage, StoreError::Database(_)));
    let json = json_err(serde_json::from_str::<Value>("{").unwrap_err());
    assert!(matches!(json, StoreError::Serialization(_)));
}

#[tokio::test]
async fn snapshots_and_resource_reports_are_served_by_the_working_set() {
    let b = open(&docs(), "session").await;
    b.snapshot_db("snap", None).await.unwrap();
    let _ = b.resource_report().await;
}
