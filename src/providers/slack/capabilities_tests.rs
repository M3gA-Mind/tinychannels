use super::*;
use crate::providers::slack::fixtures::{StubMedia, StubVoice};
use std::sync::Arc;

#[test]
fn bare_account_declares_text_buttons_threads_and_interactions_only() {
    let caps = SlackChannel::new("bot-token".into(), None, vec![]).declared();
    assert!(caps.content.text.outbound && caps.content.buttons.outbound);
    assert!(caps.content.presentation.outbound);
    assert!(!caps.content.image.outbound && !caps.content.file.outbound);
    assert!(!caps.voice_in.supported && !caps.voice_out.supported);
    assert!(caps.addressing.thread_id && caps.addressing.reply_to_id);
    assert!(caps.reactions && caps.progressive_edits);
    for action in ["send", "typing", "edit", "delete", "draft-update", "react"] {
        assert!(
            caps.actions.supported.iter().any(|a| a == action),
            "{action}"
        );
    }
    assert!(caps.events.supported.iter().any(|e| e == "message_deleted"));
}

#[test]
fn media_and_voice_ports_enable_their_content_kinds() {
    let caps = SlackChannel::new("bot-token".into(), None, vec![])
        .with_media_repository(Arc::new(StubMedia::default()))
        .with_voice_media_store(Arc::new(StubVoice::default()))
        .with_voice_synthesis(true)
        .declared();
    assert!(caps.content.image.outbound && caps.content.video.inbound);
    assert!(caps.content.audio.outbound && caps.content.file.outbound);
    assert_eq!(caps.content.file.max_size_bytes, Some(SLACK_MAX_FILE_BYTES));
    assert!(caps.content.parts.contains(&"voice_note".to_owned()));
    assert!(caps.voice_in.supported && !caps.voice_in.synthesis);
    assert!(caps.voice_out.supported && caps.voice_out.synthesis);
    assert_eq!(
        caps.voice_out.accepted_codecs,
        ["m4a_aac", "mp3", "ogg_opus"]
    );
}
