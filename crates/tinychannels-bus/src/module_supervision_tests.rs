use super::*;

#[test]
fn supervision_policy_preserves_selected_retry_timings_on_the_wire() {
    let policy = ChannelSupervisionPolicy {
        initial_backoff_secs: 7,
        max_backoff_secs: 120,
    };
    let wire = serde_json::json!({"initial_backoff_secs":7,"max_backoff_secs":120});
    assert_eq!(serde_json::to_value(policy).unwrap(), wire);
    assert_eq!(
        serde_json::from_value::<ChannelSupervisionPolicy>(wire).unwrap(),
        policy
    );
}
