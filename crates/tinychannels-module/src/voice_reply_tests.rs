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

use super::test_support::*;
use std::sync::atomic::Ordering;

fn conversation(id: &str) -> ConversationRef {
    ConversationRef {
        id: id.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn mirror_replies_in_voice_only_after_an_inbound_voice_note() {
    let synth = Arc::new(StubSynth::default());
    let spoken = spoken_with(Some(HostVoiceReplyMode::Mirror), synth.clone());
    let caps = voice_capabilities();

    inbound_voice::record("mirror-chat", true);
    let part = spoken
        .voice_part(&conversation("mirror-chat"), "turn-1", "hi there", &caps)
        .await
        .expect("a voice note is answered with a voice note");
    match part {
        OutboundContentPart::Voice {
            voice:
                OutboundVoice::Media {
                    media,
                    codec,
                    duration_ms,
                },
        } => {
            assert_eq!(media.opaque_id.as_deref(), Some("voice-1"));
            assert_eq!(codec.as_deref(), Some("ogg_opus"));
            assert_eq!(duration_ms, Some(900));
        }
        other => panic!("expected stored voice media, got {other:?}"),
    }

    inbound_voice::record("mirror-chat", false);
    assert!(
        spoken
            .voice_part(&conversation("mirror-chat"), "turn-2", "hi there", &caps)
            .await
            .is_none()
    );
    assert_eq!(
        synth.calls.load(Ordering::SeqCst),
        1,
        "text turns never synthesize"
    );
}

#[tokio::test]
async fn always_speaks_and_never_stays_silent() {
    let caps = voice_capabilities();
    let always = spoken_reply(HostVoiceReplyMode::Always);
    assert!(
        always
            .voice_part(&conversation("always-chat"), "t", "hello", &caps)
            .await
            .is_some()
    );
    let speak = spoken_reply(HostVoiceReplyMode::SpeakReply);
    assert!(
        speak
            .voice_part(&conversation("always-chat"), "t", "hello", &caps)
            .await
            .is_none(),
        "speak_reply needs an explicit request, which the module never infers"
    );
    let never = spoken_reply(HostVoiceReplyMode::Never);
    assert!(
        never
            .voice_part(&conversation("always-chat"), "t", "hello", &caps)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn text_only_when_the_channel_policy_or_pipeline_cannot_speak() {
    let caps = voice_capabilities();
    let chat = conversation("fallback-chat");

    let mut no_synthesis = caps.clone();
    no_synthesis.voice_out.synthesis = false;
    let synth = Arc::new(StubSynth::default());
    let spoken = spoken_with(Some(HostVoiceReplyMode::Always), synth.clone());
    assert!(
        spoken
            .voice_part(&chat, "t", "hello", &no_synthesis)
            .await
            .is_none()
    );
    assert!(spoken.voice_part(&chat, "t", "   ", &caps).await.is_none());
    assert_eq!(synth.calls.load(Ordering::SeqCst), 0);

    let unavailable = spoken_with(None, Arc::new(StubSynth::default()));
    assert!(
        unavailable
            .voice_part(&chat, "t", "hello", &caps)
            .await
            .is_none()
    );

    let failing = spoken_with(
        Some(HostVoiceReplyMode::Always),
        Arc::new(StubSynth {
            fail: true,
            ..Default::default()
        }),
    );
    assert!(
        failing
            .voice_part(&chat, "t", "hello", &caps)
            .await
            .is_none()
    );

    let mut no_codec = caps.clone();
    no_codec.voice_out.accepted_codecs.clear();
    let spoken = spoken_with(
        Some(HostVoiceReplyMode::Always),
        Arc::new(StubSynth::default()),
    );
    assert!(
        spoken
            .voice_part(&chat, "t", "hello", &no_codec)
            .await
            .is_none()
    );
}
