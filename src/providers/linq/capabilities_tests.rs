use super::*;
use crate::providers::linq::fixtures::*;
use crate::providers::native::fixtures::{StubMedia, StubVoice};
use std::sync::Arc;

fn plain() -> LinqChannel {
    LinqChannel::new("token".into(), FROM.into(), vec![])
}

#[test]
fn a_bare_account_declares_text_and_typing_only() {
    let caps = plain().declared();
    assert_eq!(caps.content.parts, ["text"]);
    assert!(!caps.content.image.outbound && !caps.voice_in.supported);
    assert!(!caps.reactions && !caps.addressing.reply_to_id);
    for action in ["send", "typing"] {
        assert!(
            caps.actions.supported.iter().any(|a| a == action),
            "{action}"
        );
    }
    assert!(!caps.actions.supported.iter().any(|a| a == "edit"));
}

#[test]
fn media_and_voice_ports_widen_the_declaration() {
    let caps = plain()
        .with_media_repository(Arc::new(StubMedia::default()))
        .with_voice_media_store(Arc::new(StubVoice::default()))
        .with_voice_synthesis(true)
        .declared();
    for part in ["image", "video", "audio", "file", "voice_note"] {
        assert!(caps.content.parts.iter().any(|p| p == part), "{part}");
    }
    assert_eq!(caps.content.image.max_size_bytes, Some(LINQ_MAX_FILE_BYTES));
    assert!(caps.voice_in.supported && caps.voice_out.supported);
    assert!(caps.voice_out.synthesis && !caps.voice_in.synthesis);
    assert_eq!(caps.voice_in.max_size_bytes, Some(LINQ_MAX_VOICE_BYTES));
}
