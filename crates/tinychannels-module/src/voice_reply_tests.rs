use super::*;

#[test]
fn latest_inbound_message_decides_whether_a_conversation_is_mirrored() {
    inbound_voice::record("chat-a", true);
    assert!(inbound_voice::latest_has_voice("chat-a"));
    inbound_voice::record("chat-a", false);
    assert!(!inbound_voice::latest_has_voice("chat-a"));
    assert!(!inbound_voice::latest_has_voice("chat-unknown"));
}

#[test]
fn inbound_voice_ignores_unidentified_conversations() {
    inbound_voice::record("", true);
    assert!(!inbound_voice::latest_has_voice(""));
}

#[test]
fn inbound_voice_registry_is_bounded() {
    let mut latest = inbound_voice::Latest::new(8);
    for index in 0..20 {
        latest.record(&format!("chat-{index}"), true);
    }
    assert!(!latest.has_voice("chat-0"));
    assert!(latest.has_voice("chat-19"));
}
