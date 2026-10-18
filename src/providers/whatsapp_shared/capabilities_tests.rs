use super::*;
use crate::providers::whatsapp_shared::plan::MediaLimits;

fn spec() -> CapabilitySpec {
    CapabilitySpec {
        media: false,
        voice_store: false,
        voice_synthesis: false,
        native_waveform: false,
        edit_delete: false,
        limits: MediaLimits {
            image: 5,
            video: 16,
            audio: 17,
            document: 100,
            sticker: 1,
            caption_chars: 1024,
        },
        text_limit: 4096,
    }
}

#[test]
fn a_bare_account_advertises_text_location_contact_and_the_always_available_actions() {
    let facts = capabilities(&spec());
    assert_eq!(facts.content.parts, ["text", "location", "contact"]);
    assert!(facts.content.text.inbound && facts.content.text.outbound);
    assert!(facts.content.location.outbound && facts.content.contact.inbound);
    assert!(!facts.content.image.outbound && !facts.content.sticker.inbound);
    assert!(!facts.voice_in.supported && !facts.voice_out.supported);
    assert!(facts.reactions && facts.addressing.reply_to_id && !facts.addressing.thread_id);
    for action in ["send", "typing", "mark-read", "react", "reply"] {
        assert!(
            facts.actions.supported.iter().any(|a| a == action),
            "{action}"
        );
    }
    for action in ["edit", "delete", "unsend", "draft-update"] {
        assert!(
            !facts.actions.supported.iter().any(|a| a == action),
            "{action}"
        );
    }
    assert!(!facts.progressive_edits && !facts.streaming_reply_pipeline);
    assert_eq!(facts.text.text_limit, Some(4096));
    for event in [
        "reaction_added",
        "reaction_removed",
        "read_receipt",
        "delivered_receipt",
    ] {
        assert!(facts.events.supported.iter().any(|e| e == event), "{event}");
    }
}

#[test]
fn scoped_media_storage_unlocks_every_attachment_kind_with_its_limit() {
    let facts = capabilities(&CapabilitySpec {
        media: true,
        ..spec()
    });
    for part in ["image", "video", "audio", "file", "sticker"] {
        assert!(facts.content.parts.iter().any(|p| p == part), "{part}");
    }
    assert_eq!(facts.content.image.max_size_bytes, Some(5));
    assert_eq!(facts.content.video.max_size_bytes, Some(16));
    assert_eq!(facts.content.audio.max_size_bytes, Some(17));
    assert_eq!(facts.content.file.max_size_bytes, Some(100));
    assert_eq!(facts.content.sticker.accepted_types, ["image/webp"]);
    assert_eq!(
        facts.content.image.emitted_types,
        ["image/jpeg", "image/png"]
    );
    assert!(
        facts
            .content
            .audio
            .emitted_types
            .contains(&"audio/ogg".to_owned())
    );
}

#[test]
fn voice_follows_the_voice_store_and_reports_waveform_and_synthesis_honestly() {
    let stored = capabilities(&CapabilitySpec {
        voice_store: true,
        ..spec()
    });
    assert!(stored.voice_in.supported && stored.voice_out.supported);
    assert!(stored.content.parts.iter().any(|p| p == "voice_note"));
    assert_eq!(stored.voice_out.accepted_codecs, ["ogg_opus"]);
    assert!(!stored.voice_out.native_waveform && !stored.voice_out.synthesis);
    assert_eq!(
        stored.voice_in.transcript_sources,
        [tinychannels_bus::VoiceTranscriptSource::HostStt]
    );

    let full = capabilities(&CapabilitySpec {
        voice_store: true,
        voice_synthesis: true,
        native_waveform: true,
        ..spec()
    });
    assert!(full.voice_out.native_waveform && full.voice_out.synthesis);
}

#[test]
fn edit_and_unsend_are_advertised_only_when_the_protocol_carries_them() {
    let facts = capabilities(&CapabilitySpec {
        edit_delete: true,
        ..spec()
    });
    for action in ["edit", "delete", "unsend"] {
        assert!(
            facts.actions.supported.iter().any(|a| a == action),
            "{action}"
        );
    }
}
