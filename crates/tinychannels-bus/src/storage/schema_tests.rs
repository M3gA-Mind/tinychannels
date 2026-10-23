use super::*;

#[test]
fn collections_is_pure_and_declares_each_owned_collection_once() {
    let specs = collections();
    let names: Vec<_> = specs.iter().map(|spec| spec.name.as_str()).collect();
    assert_eq!(names, COLLECTIONS);
    assert!(specs.iter().all(|spec| spec.indexes.is_empty()));
    assert_eq!(media_metadata_spec().name, MEDIA_METADATA);
}

#[test]
fn every_migration_source_is_declared_for_host_storage_grants() {
    let declared: std::collections::HashSet<_> =
        collections().into_iter().map(|spec| spec.name).collect();

    for source in MIGRATION_SOURCE_COLLECTIONS {
        assert!(
            declared.contains(source),
            "migration source not declared: {source}"
        );
    }
}
