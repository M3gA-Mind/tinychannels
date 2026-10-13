use super::*;

fn map_with(key: &str, turns: Vec<ChatMessage>) -> ConversationHistoryMap {
    let mut map = HashMap::new();
    map.insert(key.to_string(), turns);
    Arc::new(Mutex::new(map))
}

#[test]
fn compact_history_keeps_recent_turns_and_truncates_content() {
    let mut history = (0..20)
        .map(|idx| {
            if idx == 19 {
                ChatMessage::assistant("x".repeat(700))
            } else {
                ChatMessage::user(format!("turn {idx}"))
            }
        })
        .collect::<Vec<_>>();

    assert!(compact_history(&mut history));
    assert_eq!(history.len(), CHANNEL_HISTORY_COMPACT_KEEP_MESSAGES);
    assert_eq!(history[0].content, "turn 8");
    let last = history.last().unwrap();
    assert_eq!(
        last.content.chars().count(),
        CHANNEL_HISTORY_COMPACT_CONTENT_CHARS + 1
    );
    assert!(last.content.ends_with('\u{2026}'));
}

#[test]
fn compact_history_reports_empty_history() {
    let mut history = Vec::new();
    assert!(!compact_history(&mut history));
}

#[test]
fn compact_history_does_not_mark_content_at_the_limit() {
    let mut history = vec![ChatMessage::user(
        "y".repeat(CHANNEL_HISTORY_COMPACT_CONTENT_CHARS),
    )];
    assert!(compact_history(&mut history));
    assert_eq!(
        history[0].content.chars().count(),
        CHANNEL_HISTORY_COMPACT_CONTENT_CHARS
    );
    assert!(!history[0].content.ends_with('\u{2026}'));
}

#[test]
fn compact_and_clear_sender_history_target_one_sender() {
    let histories = map_with(
        "a",
        (0..30).map(|i| ChatMessage::user(format!("{i}"))).collect(),
    );
    histories
        .lock()
        .unwrap()
        .insert("b".into(), vec![ChatMessage::user("keep")]);

    assert!(!compact_sender_history(&histories, "missing"));
    assert!(compact_sender_history(&histories, "a"));
    assert_eq!(
        histories.lock().unwrap()["a"].len(),
        CHANNEL_HISTORY_COMPACT_KEEP_MESSAGES
    );

    clear_sender_history(&histories, "a");
    let guard = histories.lock().unwrap();
    assert!(!guard.contains_key("a"));
    assert_eq!(guard["b"].len(), 1);
}

#[test]
fn history_helpers_recover_from_a_poisoned_lock() {
    let histories = map_with("a", vec![ChatMessage::user("x")]);
    let clone = histories.clone();
    let _ = std::thread::spawn(move || {
        let _guard = clone.lock().unwrap();
        panic!("poison");
    })
    .join();
    assert!(compact_sender_history(&histories, "a"));
    clear_sender_history(&histories, "a");
}

#[test]
fn worker_crash_message_names_the_failure() {
    assert_eq!(
        worker_crash_message(&"boom"),
        "Channel message worker crashed: boom"
    );
}
