use super::*;

#[test]
fn defaults_point_at_slack_and_deny_every_sender() {
    let channel = SlackChannel::new("t".into(), Some("C1".into()), vec![]);
    assert_eq!(channel.api_base, "https://slack.com/api");
    assert_eq!(channel.files_base, "https://files.slack.com/");
    assert!(channel.app_token.is_none() && channel.media_repository.is_none());
    assert!(!channel.is_user_allowed("U1"));
}

#[test]
fn allowlists_match_exactly_or_by_wildcard() {
    let channel = SlackChannel::new("t".into(), None, vec!["U111".into()]);
    assert!(channel.is_user_allowed("U111"));
    for denied in ["U1111", "U11", "u111", ""] {
        assert!(!channel.is_user_allowed(denied), "{denied}");
    }
    assert!(SlackChannel::new("t".into(), None, vec!["*".into()]).is_user_allowed("anyone"));
}

#[test]
fn app_tokens_must_be_non_blank() {
    let channel = SlackChannel::new("t".into(), None, vec![]);
    assert!(
        channel
            .with_app_token(Some("  ".into()))
            .app_token
            .is_none()
    );
    let channel =
        SlackChannel::new("t".into(), None, vec![]).with_app_token(Some("app-token".into()));
    assert_eq!(channel.app_token.as_deref(), Some("app-token"));
}

#[test]
fn an_api_base_override_also_scopes_the_trusted_file_origin() {
    let channel =
        SlackChannel::new("t".into(), None, vec![]).with_api_base("http://127.0.0.1:9/api/");
    assert_eq!(channel.api_base, "http://127.0.0.1:9/api");
    assert_eq!(channel.files_base, "http://127.0.0.1:9/");
    assert_eq!(
        channel.api_url("auth.test"),
        "http://127.0.0.1:9/api/auth.test"
    );
    let bare = SlackChannel::new("t".into(), None, vec![]).with_api_base("localhost");
    assert_eq!(bare.files_base, "localhost/");
}

#[test]
fn timing_and_voice_synthesis_are_configurable() {
    let channel = SlackChannel::new("t".into(), None, vec![])
        .with_timing(Duration::from_millis(1), Duration::from_millis(2))
        .with_voice_synthesis(true);
    assert_eq!(
        (channel.reconnect_backoff, channel.poll_interval),
        (Duration::from_millis(1), Duration::from_millis(2))
    );
    assert!(channel.voice_synthesis);
}

#[test]
fn seen_keys_are_remembered_and_the_window_evicts_the_oldest() {
    let channel = SlackChannel::new("t".into(), None, vec![]);
    assert!(channel.mark_seen("C1:1".into()));
    assert!(!channel.mark_seen("C1:1".into()));
    for index in 0..SLACK_SEEN_WINDOW {
        channel.mark_seen(format!("C1:fill-{index}"));
    }
    assert!(
        channel.mark_seen("C1:1".into()),
        "the oldest key was evicted"
    );
}

#[test]
fn a_send_failure_displays_its_message() {
    let failure = SlackFailure(ChannelSendError {
        message: "boom".into(),
        ..Default::default()
    });
    assert_eq!(failure.to_string(), "boom");
}
