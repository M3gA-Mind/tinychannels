use super::*;

#[test]
fn inbound_thread_key_preserves_provider_compatibility_and_escapes_collisions() {
    assert_eq!(
        derive_inbound_thread_id("telegram", Some("u1"), Some("c1"), Some("topic")),
        "channel:telegram/u1/c1"
    );
    assert_eq!(
        derive_inbound_thread_id("slack", Some("u1"), Some("c1"), Some("thread/1")),
        "channel:slack/u1/c1#thread:thread%2F1"
    );
    assert_eq!(
        derive_inbound_thread_id("discord", Some("  "), Some("#room"), None),
        "channel:discord/%23room"
    );
    assert_ne!(
        derive_inbound_thread_id("a/b", Some("c"), None, None),
        derive_inbound_thread_id("a", Some("b/c"), None, None)
    );
}
