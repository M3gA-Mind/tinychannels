use super::*;
use crate::providers::native::fixtures::{StubMedia, StubVoice};
use std::sync::Arc;

#[test]
fn a_bare_account_declares_text_and_inbound_events_only() {
    let caps = IMessageChannel::new(vec![]).declared();
    assert_eq!(caps.content.parts, ["text"]);
    assert!(!caps.reactions && !caps.addressing.reply_to_id && !caps.addressing.thread_id);
    assert_eq!(caps.actions.supported, ["send"]);
    for event in ["reaction_added", "reaction_removed", "read_receipt"] {
        assert!(caps.events.supported.iter().any(|e| e == event), "{event}");
    }
    assert!(!caps.voice_in.supported && !caps.content.image.outbound);
}

#[test]
fn media_and_voice_ports_widen_the_declaration() {
    let caps = IMessageChannel::new(vec![])
        .with_media_repository(Arc::new(StubMedia::default()))
        .with_voice_media_store(Arc::new(StubVoice::default()))
        .with_voice_synthesis(true)
        .declared();
    for part in ["image", "video", "audio", "file", "voice_note"] {
        assert!(caps.content.parts.iter().any(|p| p == part), "{part}");
    }
    assert_eq!(caps.voice_out.accepted_codecs, ["m4a_aac", "mp3"]);
    assert!(caps.voice_out.synthesis && !caps.voice_in.synthesis);
}
