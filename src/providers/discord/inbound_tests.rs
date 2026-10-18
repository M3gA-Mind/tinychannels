use super::*;
use crate::providers::discord::support::*;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MESSAGE_ID: &str = "1234567890123456789";

fn guild_message(extra: Value) -> Value {
    let mut base = json!({
        "id": MESSAGE_ID,
        "channel_id": CHANNEL,
        "guild_id": GUILD,
        "content": "hello bot",
        "author": {"id": "42", "username": "alice", "global_name": "Alice"},
        "attachments": []
    });
    for (key, value) in extra.as_object().unwrap() {
        base[key] = value.clone();
    }
    base
}

async fn mount_channel_lookup(server: &MockServer, id: &str, body: Value) {
    Mock::given(method("GET"))
        .and(path(format!("/channels/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn mount_cdn(server: &MockServer, name: &str, bytes: Vec<u8>, content_type: &str) -> String {
    Mock::given(method("GET"))
        .and(path(format!("/cdn/{name}")))
        .respond_with(ResponseTemplate::new(200).set_body_raw(bytes, content_type))
        .mount(server)
        .await;
    format!("{}/cdn/{name}", server.uri())
}

async fn deliver(channel: &DiscordChannel, d: &Value) -> (bool, Vec<ChannelMessage>) {
    let (tx, mut rx) = mpsc::channel(8);
    let keep_going = channel.handle_message_create(d, BOT_ID, &tx).await;
    drop(tx);
    let mut received = Vec::new();
    while let Some(message) = rx.recv().await {
        received.push(message);
    }
    (keep_going, received)
}

// ── pure helpers ──────────────────────────────────────────────

#[test]
fn snowflakes_carry_their_creation_time() {
    assert_eq!(
        snowflake_unix_ms("175928847299117063"),
        Some(1_462_015_105_796)
    );
    assert_eq!(snowflake_unix_ms("nope"), None);
    assert!(is_snowflake("123") && !is_snowflake("") && !is_snowflake("12a"));
    assert!(!is_snowflake(&"1".repeat(21)));
}

#[test]
fn user_refs_prefer_global_names_and_flag_bots() {
    let user = user_ref(&json!({"id": "1", "username": "u", "global_name": "G", "bot": true}));
    assert_eq!(
        (user.id.as_str(), user.name.as_deref(), user.is_bot),
        ("1", Some("G"), true)
    );
    let plain = user_ref(&json!({"id": "2", "username": "u", "global_name": ""}));
    assert_eq!(plain.name.as_deref(), Some("u"));
    assert!(!plain.is_bot);
    assert_eq!(user_ref(&json!({})).id, "");
}

#[test]
fn message_text_handles_mentions_attachments_and_replies() {
    let m = |c, mention, att, reply| message_text(c, mention, BOT_ID, att, reply);
    assert_eq!(m("hi", false, false, false).as_deref(), Some("hi"));
    assert_eq!(m("  hi  ", false, false, false).as_deref(), Some("hi"));
    // Mention gating applies to text.
    assert_eq!(m("hi", true, false, false), None);
    assert_eq!(m("<@123456> hi", true, false, false).as_deref(), Some("hi"));
    // Attachment-only messages (voice messages) pass unless mention-gated.
    assert_eq!(m("", false, true, false).as_deref(), Some(""));
    assert_eq!(m("", true, true, false), None);
    assert_eq!(m("", true, true, true).as_deref(), Some(""));
    assert_eq!(m("", false, false, false), None);
    // A bare mention with a file still addresses the bot.
    assert_eq!(m("<@123456>", true, true, false).as_deref(), Some(""));
    assert_eq!(m("<@123456>", true, false, false), None);
    assert_eq!(m("   ", false, true, false).as_deref(), Some(""));
}

#[test]
fn media_kinds_and_content_types_normalize() {
    assert_eq!(media_kind_for("image/png"), MediaKind::Image);
    assert_eq!(media_kind_for("video/mp4"), MediaKind::Video);
    assert_eq!(media_kind_for("audio/ogg"), MediaKind::Audio);
    assert_eq!(media_kind_for("application/pdf"), MediaKind::Document);
    assert_eq!(
        normalized_content_type(Some("Image/PNG; charset=x")),
        "image/png"
    );
    assert_eq!(
        normalized_content_type(Some(" ")),
        "application/octet-stream"
    );
    assert_eq!(normalized_content_type(None), "application/octet-stream");
}

#[test]
fn waveforms_decode_to_unit_samples() {
    assert_eq!(
        decode_waveform(&BASE64.encode([0u8, 255])),
        Some(vec![0.0, 1.0])
    );
    assert_eq!(decode_waveform(""), None);
    assert_eq!(decode_waveform("***"), None);
}

#[test]
fn conversations_are_dm_channel_or_thread_aware() {
    let dm = DiscordChannel::conversation_for("9", None, &ChannelInfo::default());
    assert_eq!((dm.kind, dm.id.as_str()), (ConversationKind::Dm, "9"));
    let channel = DiscordChannel::conversation_for("9", Some("g"), &ChannelInfo::default());
    assert_eq!(channel.kind, ConversationKind::Channel);
    assert_eq!(channel.scope_id.as_deref(), Some("g"));
    let thread = DiscordChannel::conversation_for(
        "9",
        Some("g"),
        &ChannelInfo {
            thread_parent: Some("5".into()),
        },
    );
    assert_eq!(thread.kind, ConversationKind::Thread);
    assert_eq!(
        (
            thread.id.as_str(),
            thread.thread_id.as_deref(),
            thread.parent_id.as_deref()
        ),
        ("5", Some("9"), Some("5"))
    );
}

// ── channel and thread resolution ─────────────────────────────

#[tokio::test]
async fn threads_are_resolved_once_and_cached() {
    let server = MockServer::start().await;
    mount_channel_lookup(
        &server,
        THREAD,
        json!({"id": THREAD, "type": 11, "parent_id": CHANNEL}),
    )
    .await;
    let channel = channel_for(&server);

    for _ in 0..2 {
        let info = channel.channel_info(THREAD, true).await;
        assert_eq!(info.thread_parent.as_deref(), Some(CHANNEL));
    }

    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn plain_channels_dms_and_failures_are_not_threads() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let channel = channel_for(&server);

    assert_eq!(
        channel.channel_info(CHANNEL, true).await,
        ChannelInfo::default()
    );
    assert_eq!(
        channel.channel_info(CHANNEL, true).await,
        ChannelInfo::default()
    );
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "plain channels are cached too"
    );
    // DMs and malformed ids never hit the API.
    assert_eq!(
        channel.channel_info("77", false).await,
        ChannelInfo::default()
    );
    assert_eq!(
        channel.channel_info("../x", true).await,
        ChannelInfo::default()
    );
    // A rejected lookup is retried next time.
    assert_eq!(
        channel.channel_info("888", true).await,
        ChannelInfo::default()
    );
    assert_eq!(
        channel.channel_info("888", true).await,
        ChannelInfo::default()
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
    // An unreadable body is not a thread either.
    Mock::given(method("GET"))
        .and(path("/channels/999"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>"))
        .mount(&server)
        .await;
    assert_eq!(
        channel.channel_info("999", true).await,
        ChannelInfo::default()
    );
}

#[tokio::test]
async fn an_unreachable_api_is_treated_as_a_plain_channel() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let channel =
        DiscordChannel::new(BOT_TOKEN.into(), None, None, vec![], false, false).with_api_base(dead);
    assert_eq!(
        channel.channel_info(CHANNEL, true).await,
        ChannelInfo::default()
    );
}

#[test]
fn gateway_thread_objects_prime_the_cache_without_a_lookup() {
    let channel = DiscordChannel::new(BOT_TOKEN.into(), None, None, vec![], false, false);
    channel.remember_thread(&json!({"id": THREAD, "type": 12, "parent_id": CHANNEL}));
    channel.remember_thread(&json!({"id": "1", "type": 0, "parent_id": CHANNEL}));
    channel.remember_thread(&json!({"id": "2", "type": 11}));
    let cache = channel.channel_cache.lock();
    assert_eq!(
        cache.get(THREAD).unwrap().thread_parent.as_deref(),
        Some(CHANNEL)
    );
    assert!(cache.get("1").is_none() && cache.get("2").is_none());
}

// ── MESSAGE_CREATE ────────────────────────────────────────────

#[tokio::test]
async fn a_text_message_reaches_the_module_as_a_normalized_envelope() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let (channel, events, ..) = full_channel(&server);
    let d = guild_message(json!({"message_reference": {"message_id": "88"}}));

    let (keep_going, legacy) = deliver(&channel, &d).await;

    assert!(keep_going);
    assert!(legacy.is_empty(), "module ingress consumed it");
    let envelope = events.inbound().remove(0);
    assert_eq!(envelope.message_id, format!("discord_{MESSAGE_ID}"));
    assert_eq!(envelope.text, "hello bot");
    assert_eq!(envelope.reply_to_id.as_deref(), Some("88"));
    assert_eq!(envelope.sender.id, "42");
    assert_eq!(envelope.sender.name.as_deref(), Some("Alice"));
    assert_eq!(envelope.conversation.kind, ConversationKind::Channel);
    assert_eq!(envelope.conversation.scope_id.as_deref(), Some(GUILD));
    assert_eq!(envelope.timestamp_unix_ms, snowflake_unix_ms(MESSAGE_ID));
    assert_eq!(envelope.raw.unwrap()["providerMessageId"], MESSAGE_ID);
    assert!(matches!(&envelope.parts[0], ContentPart::Text { text, .. } if text == "hello bot"));
}

#[tokio::test]
async fn without_a_module_sink_the_legacy_message_carries_thread_and_reply() {
    let server = MockServer::start().await;
    mount_channel_lookup(
        &server,
        THREAD,
        json!({"id": THREAD, "type": 11, "parent_id": CHANNEL}),
    )
    .await;
    let channel = channel_for(&server);
    let d = guild_message(json!({"channel_id": THREAD, "message_reference": {"message_id": "88"}}));

    let (_, legacy) = deliver(&channel, &d).await;

    let message = &legacy[0];
    assert_eq!(message.id, format!("discord_{MESSAGE_ID}"));
    assert_eq!(message.reply_target, THREAD);
    assert_eq!(message.thread_ts.as_deref(), Some(THREAD));
    assert_eq!(message.reply_to_id.as_deref(), Some("88"));
    assert_eq!(message.sender_name.as_deref(), Some("Alice"));
}

#[tokio::test]
async fn declined_or_failed_module_ingress_falls_back_to_the_legacy_receiver() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    for events in [
        RecordingEvents {
            decline_inbound: true,
            ..Default::default()
        },
        RecordingEvents {
            fail: true,
            ..Default::default()
        },
    ] {
        let channel = channel_for(&server).with_events(Arc::new(events));
        let (keep_going, legacy) = deliver(&channel, &guild_message(json!({}))).await;
        assert!(keep_going);
        assert_eq!(legacy.len(), 1);
    }
}

#[tokio::test]
async fn a_closed_receiver_ends_the_session() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let channel = channel_for(&server);
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    assert!(
        !channel
            .handle_message_create(&guild_message(json!({})), BOT_ID, &tx)
            .await
    );
}

#[tokio::test]
async fn senders_scopes_and_empty_messages_are_filtered() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    mount_channel_lookup(
        &server,
        THREAD,
        json!({"id": THREAD, "type": 11, "parent_id": CHANNEL}),
    )
    .await;
    let restricted = DiscordChannel::new(
        BOT_TOKEN.into(),
        Some(GUILD.into()),
        Some(CHANNEL.into()),
        vec!["42".into()],
        false,
        false,
    )
    .with_api_base(server.uri());

    for dropped in [
        guild_message(json!({"author": {"id": BOT_ID}})),
        guild_message(json!({"author": {"id": "42", "bot": true}})),
        guild_message(json!({"author": {"id": "99"}})),
        guild_message(json!({"guild_id": "other"})),
        guild_message(json!({"channel_id": "123"})),
        guild_message(json!({"content": ""})),
    ] {
        let (keep_going, legacy) = deliver(&restricted, &dropped).await;
        assert!(keep_going && legacy.is_empty(), "{dropped}");
    }
    // A thread under the configured channel is in scope.
    let (_, legacy) = deliver(&restricted, &guild_message(json!({"channel_id": THREAD}))).await;
    assert_eq!(legacy.len(), 1);
}

#[tokio::test]
async fn bot_authors_pass_when_listening_to_bots() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let channel = DiscordChannel::new(BOT_TOKEN.into(), None, None, vec![], true, false)
        .with_api_base(server.uri());
    let (_, legacy) = deliver(
        &channel,
        &guild_message(json!({"author": {"id": "7", "bot": true}})),
    )
    .await;
    assert_eq!(legacy.len(), 1);
}

// ── attachments and voice messages ────────────────────────────

#[tokio::test]
async fn image_video_audio_and_file_attachments_become_typed_media() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let (channel, events, media, _) = full_channel(&server);
    let image = mount_cdn(&server, "a.png", png_bytes(), "image/png").await;
    let video = mount_cdn(&server, "b.mp4", vec![1; 16], "video/mp4").await;
    let audio = mount_cdn(&server, "c.mp3", vec![2; 16], "audio/mpeg").await;
    let file = mount_cdn(&server, "d.pdf", b"%PDF".to_vec(), "application/pdf").await;
    let d = guild_message(json!({
        "content": "",
        "attachments": [
            {"id": "1", "url": image, "filename": "a.png", "content_type": "image/png", "description": "a cat"},
            {"id": "2", "url": video, "filename": "b.mp4", "content_type": "video/mp4"},
            {"id": "3", "url": audio, "filename": "c.mp3", "content_type": "audio/mpeg", "duration_secs": 1.5},
            {"id": "4", "url": file, "filename": "d.pdf", "content_type": "application/pdf"}
        ]
    }));

    let (_, legacy) = deliver(&channel, &d).await;

    assert!(legacy.is_empty());
    let envelope = events.inbound().remove(0);
    assert_eq!(envelope.parts.len(), 4);
    assert!(
        matches!(&envelope.parts[0], ContentPart::Image { alt_text, .. } if alt_text.as_deref() == Some("a cat"))
    );
    assert!(matches!(envelope.parts[1], ContentPart::Video { .. }));
    assert!(
        matches!(&envelope.parts[2], ContentPart::Audio { duration_ms, .. } if *duration_ms == Some(1500))
    );
    assert!(
        matches!(&envelope.parts[3], ContentPart::File { file_name, .. } if file_name.as_deref() == Some("d.pdf"))
    );
    assert_eq!(envelope.media.len(), 4);
    assert_eq!(media.stored.lock().len(), 4);
}

#[tokio::test]
async fn attachments_without_host_storage_or_with_failures_never_drop_the_text() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let url = mount_cdn(&server, "a.png", png_bytes(), "image/png").await;
    let d = guild_message(
        json!({"attachments": [{"id": "1", "url": url, "filename": "a.png", "content_type": "image/png"}]}),
    );

    // No repository: text still arrives.
    let (_, legacy) = deliver(&channel_for(&server), &d).await;
    assert_eq!(legacy[0].content, "hello bot");

    // A rejecting repository: text still arrives, no media part.
    let events = Arc::new(RecordingEvents::default());
    let rejecting = channel_for(&server)
        .with_events(events.clone())
        .with_media_repository(Arc::new(MemoryMedia::rejecting()));
    deliver(&rejecting, &d).await;
    let envelope = events.inbound().remove(0);
    assert_eq!(envelope.parts.len(), 1);

    // An attachment with nothing else to say is dropped entirely.
    let only = guild_message(
        json!({"content": "", "attachments": [{"id": "1", "url": "http://127.0.0.1:1/x", "content_type": "image/png"}]}),
    );
    let (_, legacy) = deliver(&channel_for(&server), &only).await;
    assert!(legacy.is_empty());
    let skipped = guild_message(json!({"content": "", "attachments": [{"id": "1"}]}));
    assert!(deliver(&channel_for(&server), &skipped).await.1.is_empty());
}

#[tokio::test]
async fn downloads_are_bounded_and_https_only_for_the_real_api() {
    let server = MockServer::start().await;
    let channel = channel_for(&server);
    let url = mount_cdn(&server, "big.bin", vec![9; 64], "application/octet-stream").await;
    assert!(
        channel
            .download_attachment(&url, 10)
            .await
            .unwrap_err()
            .to_string()
            .contains("size limit")
    );
    assert_eq!(
        channel.download_attachment(&url, 64).await.unwrap().0.len(),
        64
    );

    Mock::given(method("GET"))
        .and(path("/cdn/empty"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    assert!(
        channel
            .download_attachment(&format!("{}/cdn/empty", server.uri()), 10)
            .await
            .unwrap_err()
            .to_string()
            .contains("empty")
    );
    Mock::given(method("GET"))
        .and(path("/cdn/gone"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    assert!(
        channel
            .download_attachment(&format!("{}/cdn/gone", server.uri()), 10)
            .await
            .is_err()
    );
    assert!(channel.download_attachment("not a url", 10).await.is_err());

    // The default API base only ever fetches over https.
    let real = DiscordChannel::new(BOT_TOKEN.into(), None, None, vec![], false, false);
    assert!(
        real.download_attachment("http://cdn.example/x", 10)
            .await
            .unwrap_err()
            .to_string()
            .contains("https")
    );
}

#[tokio::test]
async fn a_voice_message_is_stored_transcribed_and_delivered_as_a_voice_note() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let (channel, events, _, voice) = full_channel(&server);
    let url = mount_cdn(
        &server,
        "voice-message.ogg",
        OGG_FIXTURE.to_vec(),
        "audio/ogg",
    )
    .await;
    let d = guild_message(json!({
        "content": "",
        "flags": 8192,
        "attachments": [{
            "id": "5", "url": url, "filename": "voice-message.ogg", "content_type": "audio/ogg",
            "duration_secs": 0.2, "waveform": BASE64.encode([0u8, 128, 255])
        }]
    }));

    let (_, legacy) = deliver(&channel, &d).await;

    assert!(legacy.is_empty());
    let envelope = events.inbound().remove(0);
    assert_eq!(
        envelope.text, "spoken words",
        "the transcript stands in for the text"
    );
    let ContentPart::VoiceNote { voice_note } = &envelope.parts[0] else {
        panic!("expected a voice note, got {:?}", envelope.parts);
    };
    assert_eq!(voice_note.duration_ms, Some(200));
    assert_eq!(voice_note.codec.as_deref(), Some("ogg_opus"));
    assert_eq!(
        voice_note.waveform.as_deref(),
        Some(&[0.0, 128.0 / 255.0, 1.0][..])
    );
    assert!(voice_note.push_to_talk);
    let transcript = voice_note.transcript.as_ref().unwrap();
    assert_eq!(transcript.text, "spoken words");
    assert_eq!(
        transcript.source,
        tinychannels_bus::VoiceTranscriptSource::HostStt
    );
    // Only the host reference survives; the expiring CDN URL does not.
    assert!(voice_note.media.url.is_none() && voice_note.media.opaque_id.is_some());
    let stored = voice.requests.lock();
    assert_eq!(
        stored[0].direction,
        tinychannels_bus::HostVoiceMediaDirection::Inbound
    );
    assert_eq!(stored[0].scope.turn_id, format!("discord_{MESSAGE_ID}"));
    assert_eq!(stored[0].scope.conversation.id, CHANNEL);
}

#[tokio::test]
async fn a_voice_message_keeps_its_audio_when_transcription_fails() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let events = Arc::new(RecordingEvents::default());
    let voice = Arc::new(MemoryVoice::default());
    let channel = channel_for(&server)
        .with_events(events.clone())
        .with_voice_media_store(voice)
        .with_transcriber(Arc::new(FixedTranscriber(None)));
    let url = mount_cdn(&server, "v.ogg", OGG_FIXTURE.to_vec(), "audio/ogg").await;
    let d = guild_message(
        json!({"content": "", "flags": 8192, "attachments": [{"id": "5", "url": url}]}),
    );

    deliver(&channel, &d).await;

    let envelope = events.inbound().remove(0);
    let ContentPart::VoiceNote { voice_note } = &envelope.parts[0] else {
        panic!("expected a voice note");
    };
    assert_eq!(
        voice_note.transcript.as_ref().unwrap().state,
        tinychannels_bus::VoiceTranscriptState::Unavailable
    );
    assert_eq!(envelope.text, "");
}

#[tokio::test]
async fn a_voice_message_without_voice_storage_is_dropped_not_misfiled() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let url = mount_cdn(&server, "v.ogg", OGG_FIXTURE.to_vec(), "audio/ogg").await;
    let d = guild_message(
        json!({"content": "", "flags": 8192, "attachments": [{"id": "5", "url": url}]}),
    );
    let (keep_going, legacy) = deliver(&channel_for(&server), &d).await;
    assert!(keep_going && legacy.is_empty());

    // A rejecting store behaves the same way.
    let rejecting = channel_for(&server).with_voice_media_store(Arc::new(MemoryVoice::rejecting()));
    assert!(deliver(&rejecting, &d).await.1.is_empty());
    // Oversized audio never reaches the store.
    let (full, ..) = full_channel(&server);
    let huge = mount_cdn(
        &server,
        "huge.ogg",
        vec![0; 8 * 1024 * 1024 + 1],
        "audio/ogg",
    )
    .await;
    let big = guild_message(
        json!({"content": "", "flags": 8192, "attachments": [{"id": "6", "url": huge}]}),
    );
    assert!(deliver(&full, &big).await.1.is_empty());
}

#[tokio::test]
async fn voice_messages_in_a_mention_gated_bot_need_a_reply_to_the_bot() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let events = Arc::new(RecordingEvents::default());
    let channel = DiscordChannel::new(BOT_TOKEN.into(), None, None, vec![], false, true)
        .with_api_base(server.uri())
        .with_events(events.clone())
        .with_voice_media_store(Arc::new(MemoryVoice::default()));
    let url = mount_cdn(&server, "v.ogg", OGG_FIXTURE.to_vec(), "audio/ogg").await;
    let voice = |reply: Value| {
        guild_message(
            json!({"content": "", "flags": 8192, "attachments": [{"id": "5", "url": url}], "referenced_message": reply}),
        )
    };

    deliver(&channel, &voice(json!({"author": {"id": "7"}}))).await;
    assert!(events.inbound().is_empty());
    deliver(&channel, &voice(json!({"author": {"id": BOT_ID}}))).await;
    assert_eq!(events.inbound().len(), 1);
}

// ── lifecycle events ──────────────────────────────────────────

fn only_event(events: &RecordingEvents) -> tinychannels_bus::ChannelEvent {
    let mut all = events.events();
    assert_eq!(all.len(), 1, "{all:?}");
    all.remove(0)
}

#[tokio::test]
async fn edits_publish_the_new_text_and_ignore_unedited_updates() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let (channel, events, ..) = full_channel(&server);

    // Embed unfurls arrive without an edit time.
    channel
        .handle_message_update(&guild_message(json!({})), BOT_ID)
        .await;
    assert!(events.events().is_empty());
    channel
        .handle_message_update(
            &guild_message(
                json!({"edited_timestamp": "2026-01-02T03:04:05+00:00", "content": "fixed"}),
            ),
            BOT_ID,
        )
        .await;

    let event = only_event(&events);
    assert_eq!(event.source_timestamp, Some(1_767_323_045_000));
    assert!(matches!(
        event.event,
        tinychannels_bus::ChannelEventKind::MessageUpdated { ref message_id, ref parts, .. }
            if message_id == &format!("discord_{MESSAGE_ID}")
                && matches!(&parts[0], ContentPart::Text { text, .. } if text == "fixed")
    ));
}

#[tokio::test]
async fn edits_from_the_bot_strangers_and_out_of_scope_channels_are_ignored() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let events = Arc::new(RecordingEvents::default());
    let channel = DiscordChannel::new(
        BOT_TOKEN.into(),
        None,
        Some("1".into()),
        vec!["42".into()],
        false,
        false,
    )
    .with_api_base(server.uri())
    .with_events(events.clone());
    let edit = |extra: Value| {
        let mut d = guild_message(json!({"edited_timestamp": "2026-01-02T03:04:05+00:00"}));
        for (k, v) in extra.as_object().unwrap() {
            d[k] = v.clone();
        }
        d
    };
    channel
        .handle_message_update(&edit(json!({"author": {"id": BOT_ID}})), BOT_ID)
        .await;
    channel
        .handle_message_update(&edit(json!({"author": {"id": "99"}})), BOT_ID)
        .await;
    channel
        .handle_message_update(&edit(json!({})), BOT_ID)
        .await; // wrong channel
    channel
        .handle_message_update(&edit(json!({"author": null})), BOT_ID)
        .await;
    channel
        .handle_message_update(
            &json!({"id": "1", "edited_timestamp": "x", "author": {"id": "42"}}),
            BOT_ID,
        )
        .await;
    assert!(events.events().is_empty());
    // Without a sink nothing is looked up or published.
    channel_for(&server)
        .handle_message_update(&edit(json!({})), BOT_ID)
        .await;
}

#[tokio::test]
async fn deletions_publish_the_provider_message_id_when_in_scope() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let (channel, events, ..) = full_channel(&server);

    channel
        .handle_message_delete(&json!({"id": MESSAGE_ID, "channel_id": CHANNEL, "guild_id": GUILD}))
        .await;
    channel
        .handle_message_delete(&json!({"channel_id": CHANNEL}))
        .await;

    let event = only_event(&events);
    assert!(matches!(
        event.event,
        tinychannels_bus::ChannelEventKind::MessageDeleted { ref provider_message_id, .. }
            if provider_message_id.as_deref() == Some(MESSAGE_ID)
    ));
    assert_eq!(event.conversation.unwrap().scope_id.as_deref(), Some(GUILD));

    let scoped = DiscordChannel::new(
        BOT_TOKEN.into(),
        Some("other".into()),
        None,
        vec![],
        false,
        false,
    )
    .with_api_base(server.uri())
    .with_events(events.clone());
    scoped
        .handle_message_delete(&json!({"id": "5", "channel_id": CHANNEL, "guild_id": GUILD}))
        .await;
    assert_eq!(events.events().len(), 1);
    channel_for(&server)
        .handle_message_delete(&json!({"id": "5", "channel_id": CHANNEL}))
        .await;
}

#[tokio::test]
async fn reactions_publish_added_and_removed_events_with_custom_emoji_ids() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let (channel, events, ..) = full_channel(&server);
    let base = |emoji: Value| {
        json!({
            "user_id": "42", "message_id": "88", "channel_id": CHANNEL, "guild_id": GUILD,
            "emoji": emoji, "member": {"user": {"id": "42", "username": "alice"}}
        })
    };

    channel
        .handle_reaction(&base(json!({"id": null, "name": "🔥"})), BOT_ID, true)
        .await;
    channel
        .handle_reaction(&base(json!({"id": "321", "name": "party"})), BOT_ID, false)
        .await;

    let all = events.events();
    assert!(matches!(
        &all[0].event,
        tinychannels_bus::ChannelEventKind::ReactionAdded { reaction, provider_message_id, actor, .. }
            if reaction.value == "🔥" && reaction.custom_emoji_id.is_none()
                && provider_message_id.as_deref() == Some("88") && actor.name.as_deref() == Some("alice")
    ));
    assert!(matches!(
        &all[1].event,
        tinychannels_bus::ChannelEventKind::ReactionRemoved { reaction, .. }
            if reaction.custom_emoji_id.as_deref() == Some("321")
    ));
    assert!(all[0].event_id.contains("added") && all[1].event_id.contains("removed"));
}

#[tokio::test]
async fn reactions_from_the_bot_bots_strangers_and_malformed_payloads_are_ignored() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    let events = Arc::new(RecordingEvents::default());
    let channel = DiscordChannel::new(
        BOT_TOKEN.into(),
        None,
        None,
        vec!["42".into()],
        false,
        false,
    )
    .with_api_base(server.uri())
    .with_events(events.clone());
    let reaction = |user: Value, emoji: Value| json!({"user_id": user, "message_id": "88", "channel_id": CHANNEL, "guild_id": GUILD, "emoji": emoji});
    channel
        .handle_reaction(&reaction(json!(BOT_ID), json!({"name": "x"})), BOT_ID, true)
        .await;
    channel
        .handle_reaction(&reaction(json!("99"), json!({"name": "x"})), BOT_ID, true)
        .await;
    channel
        .handle_reaction(&reaction(json!("42"), json!({"name": ""})), BOT_ID, true)
        .await;
    channel
        .handle_reaction(&reaction(json!("42"), json!(null)), BOT_ID, true)
        .await;
    channel
        .handle_reaction(&json!({"user_id": "42"}), BOT_ID, true)
        .await;
    let mut bot_member = reaction(json!("42"), json!({"name": "x"}));
    bot_member["member"] = json!({"user": {"bot": true}});
    channel.handle_reaction(&bot_member, BOT_ID, true).await;
    assert!(events.events().is_empty());
    // No sink: nothing happens.
    channel_for(&server)
        .handle_reaction(&reaction(json!("42"), json!({"name": "x"})), BOT_ID, true)
        .await;
    // Out of scope.
    let scoped = DiscordChannel::new(
        BOT_TOKEN.into(),
        Some("other".into()),
        None,
        vec![],
        false,
        false,
    )
    .with_api_base(server.uri())
    .with_events(events.clone());
    scoped
        .handle_reaction(&reaction(json!("42"), json!({"name": "x"})), BOT_ID, true)
        .await;
    assert!(events.events().is_empty());
}

#[tokio::test]
async fn button_presses_are_acknowledged_and_published_as_opaque_actions() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    Mock::given(method("POST"))
        .and(path("/interactions/4242/tok_en-1/callback"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let (channel, events, ..) = full_channel(&server);
    let press = json!({
        "id": "4242", "token": "tok_en-1", "type": 3, "guild_id": GUILD,
        "data": {"custom_id": "approval-v1:req-1:0", "component_type": 2},
        "member": {"user": {"id": "42", "username": "alice"}},
        "message": {"id": "88", "channel_id": CHANNEL}
    });

    channel.handle_interaction(&press).await;

    let ack = requests(&server, "POST").await;
    assert_eq!(body_json(&ack[0]), json!({"type": 6}));
    let event = only_event(&events);
    assert!(matches!(
        event.event,
        tinychannels_bus::ChannelEventKind::ButtonCallback { ref query_id, ref action_id, ref provider_message_id, ref actor }
            if query_id == "4242" && action_id == "approval-v1:req-1:0"
                && provider_message_id.as_deref() == Some("88") && actor.id == "42"
    ));
}

#[tokio::test]
async fn interactions_that_are_not_valid_allowed_button_presses_are_ignored() {
    let server = MockServer::start().await;
    mount_channel_lookup(&server, CHANNEL, json!({"id": CHANNEL, "type": 0})).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let events = Arc::new(RecordingEvents::default());
    let channel = DiscordChannel::new(
        BOT_TOKEN.into(),
        None,
        None,
        vec!["42".into()],
        false,
        false,
    )
    .with_api_base(server.uri())
    .with_events(events.clone());
    let press = |extra: Value| {
        let mut d = json!({
            "id": "4242", "token": "tok", "type": 3, "channel_id": CHANNEL,
            "data": {"custom_id": "go"}, "user": {"id": "42"}
        });
        for (k, v) in extra.as_object().unwrap() {
            d[k] = v.clone();
        }
        d
    };
    channel.handle_interaction(&press(json!({"type": 2}))).await;
    channel
        .handle_interaction(&press(json!({"token": "bad/token"})))
        .await;
    channel.handle_interaction(&press(json!({"id": "x"}))).await;
    channel
        .handle_interaction(&press(json!({"user": {"id": "99"}})))
        .await;
    channel
        .handle_interaction(&press(json!({"data": {"custom_id": ""}})))
        .await;
    channel
        .handle_interaction(&press(json!({"data": {"custom_id": "c".repeat(101)}})))
        .await;
    channel
        .handle_interaction(&press(json!({"user": null})))
        .await;
    channel
        .handle_interaction(&press(json!({"channel_id": null})))
        .await;
    channel.handle_interaction(&json!({"type": 3})).await;
    assert!(events.events().is_empty());

    // A valid press from a DM carries no guild and no message.
    channel.handle_interaction(&press(json!({}))).await;
    let event = only_event(&events);
    assert!(matches!(
        event.event,
        tinychannels_bus::ChannelEventKind::ButtonCallback {
            provider_message_id: None,
            ..
        }
    ));
    assert_eq!(event.conversation.unwrap().kind, ConversationKind::Dm);
}
