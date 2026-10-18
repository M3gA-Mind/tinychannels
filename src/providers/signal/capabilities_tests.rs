use super::*;
use crate::providers::native::fixtures::{StubMedia, StubVoice};
use crate::providers::signal::fixtures::*;
use std::sync::Arc;

#[test]
fn a_bare_account_declares_text_reactions_and_edits_only() {
    let caps = channel_for_plain().declared();
    assert_eq!(caps.content.parts, ["text"]);
    assert!(!caps.content.image.outbound && !caps.voice_in.supported);
    assert!(caps.reactions && caps.addressing.reply_to_id && !caps.addressing.thread_id);
    assert!(!caps.progressive_edits);
    for action in ["send", "typing", "edit", "delete", "react"] {
        assert!(
            caps.actions.supported.iter().any(|a| a == action),
            "{action}"
        );
    }
    assert!(!caps.actions.supported.iter().any(|a| a == "draft-update"));
    for event in ["reaction_added", "read_receipt", "message_deleted"] {
        assert!(caps.events.supported.iter().any(|e| e == event), "{event}");
    }
}

#[test]
fn media_and_voice_ports_widen_the_declaration() {
    let caps = channel_for_plain()
        .with_media_repository(Arc::new(StubMedia::default()))
        .with_voice_media_store(Arc::new(StubVoice::default()))
        .with_voice_synthesis(true)
        .declared();
    for part in ["image", "video", "audio", "file", "voice_note"] {
        assert!(caps.content.parts.iter().any(|p| p == part), "{part}");
    }
    assert_eq!(
        caps.content.image.max_size_bytes,
        Some(SIGNAL_MAX_FILE_BYTES)
    );
    assert!(caps.voice_in.supported && caps.voice_out.supported);
    assert!(caps.voice_out.synthesis && !caps.voice_in.synthesis);
    assert_eq!(caps.voice_in.max_size_bytes, Some(SIGNAL_MAX_VOICE_BYTES));
}

fn channel_for_plain() -> SignalChannel {
    SignalChannel::new(
        "http://127.0.0.1:1".into(),
        ACCOUNT.into(),
        None,
        vec![],
        false,
        false,
    )
}
