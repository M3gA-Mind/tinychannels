use super::*;
use serde_json::json;

#[test]
fn approval_reply_context_keeps_optional_thread_wire_shape() {
    let reply = ReplyContext {
        reply_target: "room-7".into(),
        thread_ts: Some("thread-2".into()),
    };
    assert_eq!(
        serde_json::to_value(&reply).unwrap(),
        json!({"replyTarget":"room-7", "threadTs":"thread-2"})
    );
    let round_trip: ReplyContext = serde_json::from_value(json!({
        "replyTarget": "room-7"
    }))
    .unwrap();
    assert_eq!(round_trip.reply_target, "room-7");
    assert_eq!(round_trip.thread_ts, None);
}
