use super::*;
use tinystoragedrivers_core::schema::{Item, Registry};

#[test]
fn the_schema_composes_and_declares_every_collection_as_module_scoped() {
    let schema = storage_schema();
    Registry::compose([schema.clone()]).expect("the channels schema composes");

    let declared: Vec<_> = schema
        .items
        .iter()
        .filter_map(|item| {
            assert_eq!(item.scope(), ScopeClass::Module, "{}", item.name());
            let Item::Collection { version, .. } = item else {
                return None;
            };
            let expected_version = if item.name() == MEDIA_METADATA {
                MEDIA_METADATA_VERSION
            } else {
                SCHEMA_VERSION
            };
            assert_eq!(
                *version, expected_version,
                "{}",
                item.name()
            );
            Some(item.name())
        })
        .collect();
    assert_eq!(declared, COLLECTIONS);
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

#[test]
fn scoped_channel_media_collection_is_declared_for_module_storage() {
    let schema = storage_schema();
    let media = schema
        .items
        .iter()
        .find(|item| item.name() == MEDIA_METADATA)
        .expect("the channel media store collection is declared");
    assert!(matches!(
        media,
        Item::Collection {
            scope: ScopeClass::Module,
            spec,
            ..
        } if spec == &media_metadata_spec()
    ));
}

#[test]
fn scoped_channel_media_blob_prefix_is_declared_for_module_storage() {
    let schema = storage_schema();
    let media_blobs = schema
        .items
        .iter()
        .find(|item| matches!(item, Item::Blobs { prefix, .. } if prefix == MEDIA_BLOB_PREFIX))
        .expect("the channel media blob prefix is declared");
    assert_eq!(media_blobs.scope(), ScopeClass::Module);
}
