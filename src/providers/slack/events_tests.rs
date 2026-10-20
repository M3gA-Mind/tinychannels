use super::*;
use crate::providers::slack::fixtures::*;
use serde_json::json;
use std::sync::Arc;
use tinychannels_bus::{ChannelEventKind, ChannelIngress};

fn message(extra: serde_json::Value) -> serde_json::Value {
    let mut event = json!({
        "type": "message", "channel": "C1", "ts": "1700000000.000100",
        "user": "U1", "text": "hello", "channel_type": "channel",
    });
    event
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    event
}

fn sink(deliver: bool) -> Arc<RecordingEvents> {
    Arc::new(RecordingEvents {
        deliver,
        ..Default::default()
    })
}

#[test]
fn emoji_names_round_trip_through_the_known_table() {
    assert_eq!(emoji_for_name("thumbsup"), "👍");
    assert_eq!(emoji_for_name("custom_party"), ":custom_party:");
    assert_eq!(name_for_emoji("👍").as_deref(), Some("thumbsup"));
    assert_eq!(
        name_for_emoji(":custom_party:").as_deref(),
        Some("custom_party")
    );
    assert_eq!(
        name_for_emoji("white_check_mark").as_deref(),
        Some("white_check_mark")
    );
    assert_eq!(name_for_emoji("a b"), None);
    assert_eq!(name_for_emoji("  "), None);
    assert_eq!(name_for_emoji("🤷"), None);
}

#[tokio::test]
async fn a_message_reaches_the_legacy_channel_with_its_thread() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let channel = channel_for(&server, &["U1"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let flow = channel
        .handle_event(
            &message(json!({"thread_ts": "1700000000.000050"})),
            Some(&tx),
        )
        .await;
    assert_eq!(flow, Flow::Continue);
    let received = rx.recv().await.unwrap();
    assert_eq!(received.id, "slack_C1_1700000000.000100");
    assert_eq!(received.sender, "U1");
    assert_eq!(received.reply_target, "C1");
    assert_eq!(received.content, "hello");
    assert_eq!(received.thread_ts.as_deref(), Some("1700000000.000050"));
}

#[tokio::test]
async fn a_delivered_message_uses_the_ordered_ingress_only() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let events = sink(true);
    let media = Arc::new(StubMedia::default());
    let channel = channel_for(&server, &["*"])
        .with_events(events.clone())
        .with_media_repository(media);
    let png = mount_file(&server, "/f/a.png", "image/png", b"png").await;
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let event = message(
        json!({"files": [{"id": "F1", "mimetype": "image/png", "url_private_download": png}]}),
    );
    assert_eq!(
        channel.handle_event(&event, Some(&tx)).await,
        Flow::Continue
    );
    assert!(
        rx.try_recv().is_err(),
        "ingress delivery must not also use the legacy channel"
    );
    let envelope = events.inbound().remove(0);
    assert_eq!(envelope.conversation.id, "C1");
    assert_eq!(
        envelope.conversation.thread_id.as_deref(),
        Some("1700000000.000100")
    );
    assert_eq!(envelope.sender.id, "U1");
    assert_eq!(envelope.timestamp_unix_ms, Some(1_700_000_000_000));
    assert_eq!(
        envelope.raw.unwrap()["providerMessageId"],
        "1700000000.000100"
    );
    assert!(matches!(envelope.parts[0], ContentPart::Text { .. }));
    assert!(matches!(envelope.parts[1], ContentPart::Image { .. }));
    assert_eq!(envelope.media.len(), 1);
}

#[tokio::test]
async fn voice_clips_travel_as_voice_notes_in_the_envelope() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let clip = mount_file(&server, "/f/c.m4a", "audio/mp4", b"audio").await;
    let events = sink(true);
    let channel = channel_for(&server, &["*"])
        .with_events(events.clone())
        .with_voice_media_store(Arc::new(StubVoice::default()));
    let event = message(json!({"text": "", "files": [{
        "id": "F1", "mimetype": "audio/mp4", "subtype": "slack_audio", "url_private_download": clip,
        "transcription": {"status": "complete", "preview": {"content": "hi"}},
    }]}));
    channel.handle_event(&event, None).await;
    let envelope = events.inbound().remove(0);
    assert_eq!(envelope.parts.len(), 1);
    assert!(
        matches!(&envelope.parts[0], ContentPart::VoiceNote { voice_note } if voice_note.transcript.is_some())
    );
    assert_eq!(envelope.media.len(), 1);
}

#[tokio::test]
async fn undelivered_or_failed_ingress_falls_back_and_a_closed_receiver_stops_the_listener() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    for events in [
        sink(false),
        Arc::new(RecordingEvents {
            fail: true,
            ..Default::default()
        }),
    ] {
        let channel = channel_for(&server, &["*"]).with_events(events);
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        channel.handle_event(&message(json!({})), Some(&tx)).await;
        assert_eq!(rx.recv().await.unwrap().content, "hello");
    }
    let channel = channel_for(&server, &["*"]);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    assert_eq!(
        channel.handle_event(&message(json!({})), Some(&tx)).await,
        Flow::Stop
    );
}

#[tokio::test]
async fn unwanted_messages_are_dropped() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let channel = channel_for(&server, &["U1", "UBOT"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let dropped = [
        message(json!({"bot_id": "B1"})),
        message(json!({"subtype": "bot_message"})),
        message(json!({"user": "UBOT"})),
        message(json!({"user": "U-stranger"})),
        message(json!({"channel": "C-other"})),
        message(json!({"text": ""})),
        message(json!({"user": ""})),
        message(json!({"channel": ""})),
        message(json!({"ts": ""})),
        json!({"type": "channel_joined"}),
    ];
    for event in &dropped {
        assert_eq!(channel.handle_event(event, Some(&tx)).await, Flow::Continue);
    }
    assert!(rx.try_recv().is_err());
    // A direct message passes the channel filter; a repeat of the same ts does not.
    let dm = message(json!({"channel": "D1", "channel_type": "im"}));
    channel.handle_event(&dm, Some(&tx)).await;
    channel.handle_event(&dm, Some(&tx)).await;
    assert_eq!(rx.recv().await.unwrap().reply_target, "D1");
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn an_app_mention_and_its_message_event_deliver_once() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let channel = channel_for(&server, &["*"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    channel.handle_event(&message(json!({})), Some(&tx)).await;
    channel
        .handle_event(&message(json!({"type": "app_mention"})), Some(&tx))
        .await;
    assert!(rx.recv().await.is_some());
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn edits_and_deletes_become_events_and_skip_bots_and_strangers() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let events = sink(true);
    let channel = channel_for(&server, &["U1", "UBOT"]).with_events(events.clone());
    let edited = |user: &str, extra: serde_json::Value| {
        let mut inner = json!({"user": user, "ts": "1.1", "text": "new text", "thread_ts": "1.0"});
        inner
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!({"type": "message", "subtype": "message_changed", "channel": "C1", "event_ts": "5.5", "message": inner})
    };
    channel.handle_event(&edited("U1", json!({})), None).await;
    for skipped in [
        edited("UBOT", json!({})),
        edited("U-stranger", json!({})),
        edited("U1", json!({"bot_id": "B"})),
        edited("U1", json!({"ts": ""})),
    ] {
        channel.handle_event(&skipped, None).await;
    }
    let deleted = |author: &str, ts: &str| {
        json!({
            "type": "message", "subtype": "message_deleted", "channel": "C1", "deleted_ts": ts,
            "event_ts": "6.6", "previous_message": {"user": author},
        })
    };
    channel.handle_event(&deleted("U1", "1.1"), None).await;
    for skipped in [
        deleted("UBOT", "1.1"),
        deleted("U-stranger", "1.1"),
        deleted("U1", ""),
    ] {
        channel.handle_event(&skipped, None).await;
    }
    let recorded = events.events();
    assert_eq!(recorded.len(), 2);
    match &recorded[0].event {
        ChannelEventKind::MessageUpdated {
            message_id,
            provider_message_id,
            parts,
        } => {
            assert_eq!(message_id, "slack_C1_1.1");
            assert_eq!(provider_message_id.as_deref(), Some("1.1"));
            assert!(matches!(&parts[0], ContentPart::Text { text, .. } if text == "new text"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        recorded[0]
            .conversation
            .as_ref()
            .unwrap()
            .thread_id
            .as_deref(),
        Some("1.0")
    );
    assert_eq!(recorded[0].source_timestamp, Some(5500));
    assert!(
        matches!(&recorded[1].event, ChannelEventKind::MessageDeleted { message_id, .. } if message_id == "slack_C1_1.1")
    );
}

#[tokio::test]
async fn edits_without_an_event_sink_are_ignored() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let channel = channel_for(&server, &["*"]);
    let event = json!({"type": "message", "subtype": "message_changed", "channel": "C1", "message": {"user": "U1", "ts": "1.1"}});
    assert_eq!(channel.handle_event(&event, None).await, Flow::Continue);
}

#[tokio::test]
async fn reactions_become_added_and_removed_events_once() {
    let server = mock_api().await;
    mount_auth(&server, "UBOT").await;
    let events = sink(true);
    let channel = channel_for(&server, &["U1", "UBOT"]).with_events(events.clone());
    let reaction = |kind: &str, user: &str, name: &str, item_type: &str| {
        json!({
            "type": kind, "user": user, "reaction": name, "event_ts": "9.9",
            "item": {"type": item_type, "channel": "C1", "ts": "1.1"},
        })
    };
    channel
        .handle_event(
            &reaction("reaction_added", "U1", "thumbsup", "message"),
            None,
        )
        .await;
    channel
        .handle_event(
            &reaction("reaction_added", "U1", "thumbsup", "message"),
            None,
        )
        .await;
    channel
        .handle_event(
            &reaction("reaction_removed", "U1", "party_parrot", "message"),
            None,
        )
        .await;
    for skipped in [
        reaction("reaction_added", "U1", "x", "file"),
        reaction("reaction_added", "UBOT", "x", "message"),
        reaction("reaction_added", "U-stranger", "x", "message"),
        reaction("reaction_added", "U1", "", "message"),
    ] {
        channel.handle_event(&skipped, None).await;
    }
    let recorded = events.events();
    assert_eq!(recorded.len(), 2);
    match &recorded[0].event {
        ChannelEventKind::ReactionAdded {
            message_id,
            provider_message_id,
            actor,
            reaction,
        } => {
            assert_eq!(message_id, "slack_C1_1.1");
            assert_eq!(provider_message_id.as_deref(), Some("1.1"));
            assert_eq!(actor.id, "U1");
            assert_eq!(reaction.value, "👍");
        }
        other => panic!("{other:?}"),
    }
    assert!(
        matches!(&recorded[1].event, ChannelEventKind::ReactionRemoved { reaction, .. } if reaction.value == ":party_parrot:")
    );
}

fn press(extra: serde_json::Value) -> serde_json::Value {
    let mut payload = json!({
        "type": "block_actions", "trigger_id": "trig-1",
        "user": {"id": "U1", "username": "ada"},
        "channel": {"id": "C1"},
        "message": {"ts": "1.1", "thread_ts": "1.0"},
        "actions": [{"action_id": "approval-v1:r:0", "type": "button"}],
    });
    payload
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    payload
}

#[tokio::test]
async fn a_button_press_becomes_a_callback_event_for_allowed_actors() {
    let server = mock_api().await;
    let events = sink(true);
    let channel = channel_for(&server, &["ada"]).with_events(events.clone());
    channel.handle_interactive(&press(json!({}))).await;
    let container = press(json!({
        "channel": null, "message": null, "container": {"channel_id": "C2", "message_ts": "2.2", "thread_ts": "2.0"},
    }));
    channel.handle_interactive(&container).await;
    for dropped in [
        press(json!({"type": "view_submission"})),
        press(json!({"user": {"id": "U-stranger"}})),
        press(json!({"actions": []})),
        press(json!({"channel": null})),
        press(json!({"trigger_id": ""})),
    ] {
        channel.handle_interactive(&dropped).await;
    }
    let recorded = events.events();
    assert_eq!(recorded.len(), 2);
    match &recorded[0].event {
        ChannelEventKind::ButtonCallback {
            query_id,
            action_id,
            provider_message_id,
            actor,
        } => {
            assert_eq!(query_id, "trig-1");
            assert_eq!(action_id, "approval-v1:r:0");
            assert_eq!(provider_message_id.as_deref(), Some("1.1"));
            assert_eq!(actor.id, "U1");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(recorded[1].conversation.as_ref().unwrap().id, "C2");
    assert_eq!(
        recorded[1]
            .conversation
            .as_ref()
            .unwrap()
            .thread_id
            .as_deref(),
        Some("2.0")
    );
    assert!(matches!(
        recorded[0].event,
        ChannelEventKind::ButtonCallback { .. }
    ));
    assert!(matches!(
        ChannelIngress::Event(recorded[0].clone()),
        ChannelIngress::Event(_)
    ));
}

#[tokio::test]
async fn events_are_ignored_when_no_sink_is_attached() {
    let server = mock_api().await;
    let channel = channel_for(&server, &["*"]);
    channel.handle_interactive(&press(json!({}))).await;
    let sink = Arc::new(RecordingEvents {
        fail: true,
        ..Default::default()
    });
    let failing = channel_for(&server, &["*"]).with_events(sink);
    failing.handle_interactive(&press(json!({}))).await;
}
