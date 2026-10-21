use super::*;
use tinystoragedrivers_core::schema::{Item, Registry};

#[test]
fn the_schema_composes_and_declares_every_collection_as_module_scoped() {
    let schema = storage_schema();
    Registry::compose([schema.clone()]).expect("the channels schema composes");

    let mut declared = Vec::new();
    for item in &schema.items {
        match item {
            Item::Collection {
                name,
                version,
                scope,
                ..
            } => {
                assert_eq!(*scope, ScopeClass::Module, "{name}");
                let expected_version = if name == MEDIA_METADATA {
                    MEDIA_METADATA_VERSION
                } else {
                    2
                };
                assert_eq!(*version, expected_version, "{name}");
                declared.push(name.as_str());
            }
            Item::Blobs { prefix, scope } => {
                assert_eq!(prefix, MEDIA_BLOB_PREFIX);
                assert_eq!(*scope, ScopeClass::Module);
            }
            other => panic!("unexpected channel storage item: {}", other.name()),
        }
    }
    assert_eq!(declared, COLLECTIONS);
    assert!(
        schema
            .items
            .iter()
            .any(|item| item.name() == MEDIA_BLOB_PREFIX)
    );
}

#[test]
fn every_v1_collection_has_one_step_into_its_v2_name() {
    let schema = storage_schema();
    let legacy: Vec<_> = schema.steps.iter().filter_map(|step| step.source).collect();
    let mut expected = vec![
        "channels_delivery_pending",
        "channels_delivery_failed",
        "channels_delivery_results",
        "channel_remote_bindings",
        "channel_remote_busy",
        "channel_remote_meta",
        "whatsapp_device",
        "whatsapp_identity",
        "whatsapp_session",
        "whatsapp_prekey",
        "whatsapp_signed_prekey",
        "whatsapp_sender_key",
        "whatsapp_sync_key",
        "whatsapp_lid_mapping",
    ];
    let mut actual = legacy.clone();
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
    for step in &schema.steps {
        assert_eq!((step.from, step.to), (1, 2), "{}", step.id);
    }
}
