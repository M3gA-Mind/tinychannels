use super::*;

#[test]
fn inbound_voice_turns_are_remembered_per_conversation_and_turn() {
    inbound_voice::record("chat-a", "turn-voice");
    assert!(inbound_voice::contains("chat-a", "turn-voice"));
    assert!(!inbound_voice::contains("chat-b", "turn-voice"));
    assert!(!inbound_voice::contains("chat-a", "turn-text"));
}

#[test]
fn inbound_voice_ignores_unidentified_turns() {
    inbound_voice::record("", "turn-x");
    inbound_voice::record("chat-x", " ");
    assert!(!inbound_voice::contains("", "turn-x"));
    assert!(!inbound_voice::contains("chat-x", " "));
}

#[test]
fn inbound_voice_registry_is_bounded() {
    for index in 0..2000 {
        inbound_voice::record("chat-bound", &format!("turn-{index}"));
    }
    assert!(!inbound_voice::contains("chat-bound", "turn-0"));
    assert!(inbound_voice::contains("chat-bound", "turn-1999"));
}
