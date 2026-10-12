use super::*;

struct UnsupportedApprovals;

#[async_trait]
impl ApprovalGate for UnsupportedApprovals {}

#[tokio::test]
async fn default_approval_gate_rejects_requests_and_does_not_parse_replies() {
    let gate: &dyn ApprovalGate = &UnsupportedApprovals;
    for reply in ["yes", "no", "1", "arbitrary text", ""] {
        assert_eq!(gate.parse_reply(reply), None);
    }
    let error = gate
        .request(ApprovalAsk {
            id: "approval".into(),
            prompt: "Proceed?".into(),
            choices: vec![],
            session_key: "session".into(),
            timeout_secs: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "interactive approval requests are not supported by this host"
    );
}
