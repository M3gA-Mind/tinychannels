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
}
