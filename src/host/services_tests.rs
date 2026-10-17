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

struct LegacyIngressSink;

#[async_trait]
impl EventSink for LegacyIngressSink {
    async fn publish(
        &self,
        _domain: &str,
        _kind: &str,
        _payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[allow(
    deprecated,
    reason = "this pins the one-release v21 inbound compatibility default"
)]
async fn legacy_event_sink_declines_v22_inbound_for_message_channel_fallback() {
    let sink = LegacyIngressSink;
    assert!(
        !sink
            .publish_inbound(tinychannels_bus::ChannelMessage::default())
            .await
            .unwrap()
    );
    assert!(
        !sink
            .publish_ingress(tinychannels_bus::ChannelIngress::Inbound(
                tinychannels_bus::ChannelInboundEnvelope::default()
            ))
            .await
            .unwrap()
    );
}
