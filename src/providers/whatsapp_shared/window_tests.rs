use super::*;

#[test]
fn seen_set_remembers_only_its_capacity_and_ignores_repeats() {
    let mut seen = SeenSet::new(2);
    seen.insert("a".into());
    seen.insert("a".into());
    seen.insert("b".into());
    assert!(seen.contains("a") && seen.contains("b"));
    seen.insert("c".into());
    assert!(!seen.contains("a"), "the oldest id is evicted");
    assert!(seen.contains("b") && seen.contains("c"));
}

#[test]
fn bounded_map_evicts_the_oldest_key_and_updates_in_place() {
    let mut map = BoundedMap::new(2);
    map.insert("x", "1");
    map.insert("y", "2");
    map.insert("x", "3");
    assert_eq!(map.get("x").as_deref(), Some("3"));
    map.insert("z", "4");
    assert_eq!(map.get("x"), None, "x was created first");
    assert_eq!(map.get("y").as_deref(), Some("2"));
    assert_eq!(map.get("z").as_deref(), Some("4"));
}

#[test]
fn zero_capacity_is_clamped_to_one() {
    let mut seen = SeenSet::new(0);
    seen.insert("a".into());
    assert!(seen.contains("a"));
    let mut map = BoundedMap::new(0);
    map.insert("a", "1");
    assert_eq!(map.get("a").as_deref(), Some("1"));
}

#[test]
fn the_sender_slot_is_cleared_when_its_guard_drops() {
    let slot = parking_lot::Mutex::new(Some(7));
    {
        let _guard = ClearOnDrop(&slot);
        assert_eq!(*slot.lock(), Some(7));
    }
    assert_eq!(*slot.lock(), None);
}
