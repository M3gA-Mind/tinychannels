use super::*;

#[test]
fn reply_context_preserves_camel_case_thread_address() {
    let context = ReplyContext {
        reply_target: "room-1".into(),
        thread_ts: Some("thread-2".into()),
    };

    let value = serde_json::to_value(&context).unwrap();
    assert_eq!(value["replyTarget"], "room-1");
    assert_eq!(value["threadTs"], "thread-2");
    assert_eq!(
        serde_json::from_value::<ReplyContext>(value).unwrap(),
        context
    );
}

#[test]
fn reply_context_accepts_missing_optional_thread() {
    let context: ReplyContext = serde_json::from_value(serde_json::json!({
        "replyTarget": "room-1"
    }))
    .unwrap();
    assert_eq!(context.thread_ts, None);
}
