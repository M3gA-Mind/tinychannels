use super::*;

#[test]
fn web_projection_contract_keeps_host_scope_out_of_wire_context() {
    let begin = BeginWebTurn {
        scope_key: "opaque-host-scope".into(),
        context: WebEventContext {
            client_id: "client".into(),
            thread_id: "thread".into(),
            request_id: "request".into(),
            ..Default::default()
        },
    };
    let encoded = serde_json::to_value(begin).unwrap();
    assert_eq!(encoded["scopeKey"], "opaque-host-scope");
    assert_eq!(encoded["context"]["thread_id"], "thread");
    assert!(encoded["context"].get("tenant_id").is_none());
    let decoded: BeginWebTurn = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded.scope_key, "opaque-host-scope");
    assert_eq!(decoded.context.client_id, "client");
    assert_eq!(decoded.context.request_id, "request");
    assert_eq!(decoded.context.seq, None);
    assert_eq!(decoded.context.ts, None);
}
