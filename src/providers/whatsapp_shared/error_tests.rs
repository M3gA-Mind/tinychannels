use super::*;

#[test]
fn rejected_unsupported_and_uncertain_failures_keep_their_retry_contract() {
    let rejected = WaFailure::rejected("bad scope").into_send_error();
    assert_eq!(rejected.kind, SendErrorKind::Forbidden);
    assert!(!rejected.unsupported && !rejected.sent_before_error && !rejected.retryable);

    let unsupported = WaFailure::unsupported("no polls").into_send_error();
    assert_eq!(unsupported.kind, SendErrorKind::BadFormat);
    assert!(unsupported.unsupported && !unsupported.sent_before_error && !unsupported.retryable);

    let uncertain = WaFailure::uncertain("socket closed").into_send_error();
    assert_eq!(uncertain.kind, SendErrorKind::Unknown);
    assert!(uncertain.sent_before_error && !uncertain.retryable && !uncertain.unsupported);
    assert_eq!(uncertain.message, "socket closed");
}

#[test]
fn failure_displays_its_message_for_every_class() {
    for failure in [
        WaFailure::rejected("a"),
        WaFailure::unsupported("a"),
        WaFailure::uncertain("a"),
    ] {
        assert_eq!(failure.to_string(), "a");
    }
}
