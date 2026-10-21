use super::*;

#[test]
fn acknowledgement_selection_is_stable_and_contextual() {
    assert!(matches!(
        select_acknowledgment_reaction("thanks"),
        "❤️" | "🙏"
    ));
    assert_eq!(
        select_acknowledgment_reaction("thanks"),
        select_acknowledgment_reaction("thanks")
    );
    assert!(jitter_millis(1) < MAX_JITTER_MS);
    assert_eq!(jitter_millis(0), 0);
    assert_eq!(compute_max_in_flight_messages(100), 64);
}

#[test]
fn acknowledgement_categories_keep_their_context_and_priority() {
    for (content, choices) in [
        ("THANKS for the code", ["❤️", "🙏"]),
        ("awesome!!", ["🔥", "🎉"]),
        ("btc price?", ["💯", "⚡"]),
        ("rust function?", ["👨‍💻", "🤓"]),
        ("hello there", ["🤗", "😁"]),
        ("yo", ["🤗", "😁"]),
        ("yo friend", ["🤗", "😁"]),
        ("what happened", ["🤔", "✍️"]),
        ("really?", ["🤔", "✍️"]),
        ("ordinary message", ["👀", "✍️"]),
        ("", ["👀", "✍️"]),
    ] {
        assert!(
            choices.contains(&select_acknowledgment_reaction(content)),
            "{content}"
        );
    }
    assert_eq!(compute_max_in_flight_messages(0), 8);
    assert_eq!(compute_max_in_flight_messages(3), 12);
    assert_eq!(compute_max_in_flight_messages(usize::MAX), 64);
    assert!(jitter_millis(u64::MAX) < MAX_JITTER_MS);
}

#[tokio::test]
async fn worker_join_logging_accepts_success_and_panic() {
    log_worker_join_result(Ok(()));
    let failure = tokio::spawn(async { panic!("worker failed") }).await;
    assert!(failure.as_ref().unwrap_err().is_panic());
    log_worker_join_result(failure);
}
