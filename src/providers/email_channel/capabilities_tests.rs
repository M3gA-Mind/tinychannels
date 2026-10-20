use super::*;
use crate::config::EmailConfig;
use crate::providers::email_channel::fixtures::*;
use crate::providers::native::fixtures::{StubMedia, StubVoice};
use std::sync::Arc;

fn plain() -> EmailChannel {
    EmailChannel::new(EmailConfig::default())
}

#[test]
fn a_bare_account_declares_text_and_reply_threading_only() {
    let caps = plain().declared();
    assert_eq!(caps.content.parts, ["text"]);
    assert!(!caps.content.image.outbound && !caps.voice_in.supported);
    assert!(caps.addressing.reply_to_id && caps.addressing.thread_id);
    assert!(!caps.reactions);
    for action in ["send", "reply"] {
        assert!(
            caps.actions.supported.iter().any(|a| a == action),
            "{action}"
        );
    }
    assert!(
        !caps
            .actions
            .supported
            .iter()
            .any(|a| a == "edit" || a == "typing")
    );
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
    assert_eq!(
        caps.content.image.max_size_bytes,
        Some(EMAIL_MAX_FILE_BYTES)
    );
    assert!(caps.voice_in.supported && caps.voice_out.supported);
    assert!(caps.voice_out.synthesis && !caps.voice_in.synthesis);
    assert_eq!(caps.voice_in.max_size_bytes, Some(EMAIL_MAX_VOICE_BYTES));
    let _ = BOT;
}
