use super::*;
use std::sync::Arc;
use tinychannels_bus::module_progressive::ChannelOutputEvent;
use tinychannels_bus::web::WebEventContext;
use tokio::sync::Mutex;

fn begin(scope: &str) -> BeginWebTurn {
    BeginWebTurn {
        scope_key: scope.to_owned(),
        context: WebEventContext {
            client_id: "same-client".into(),
            thread_id: "same-thread".into(),
            request_id: "same-request".into(),
            turn_request_id: None,
            seq: None,
            ts: None,
        },
    }
}

fn large_begin() -> BeginWebTurn {
    BeginWebTurn {
        scope_key: "s".repeat(MAX_WEB_TURN_SCOPE_BYTES),
        context: WebEventContext {
            client_id: "c".repeat(MAX_WEB_TURN_CONTEXT_FIELD_BYTES),
            thread_id: "t".repeat(MAX_WEB_TURN_CONTEXT_FIELD_BYTES),
            request_id: "r".repeat(MAX_WEB_TURN_CONTEXT_FIELD_BYTES),
            turn_request_id: Some("u".repeat(MAX_WEB_TURN_CONTEXT_FIELD_BYTES)),
            seq: None,
            ts: None,
        },
    }
}

fn text(handle: &WebTurnHandle, value: &str) -> ProjectWebOutput {
    ProjectWebOutput {
        handle: handle.handle.clone(),
        event: ChannelOutputEvent::TextDelta {
            text: value.to_owned(),
            iteration: Some(1),
        },
    }
}

fn sequence(events: &WebOutputEvents) -> u64 {
    let WebChannelEvent::TextDelta { context, .. } = &events.events[0] else {
        panic!("expected projected text delta: {events:?}");
    };
    context.seq.expect("module stamps sequence")
}

#[test]
fn projects_ordered_outputs_with_module_owned_sequence_and_timestamp() {
    let mut registry = WebTurnRegistry::default();
    let handle = registry.begin(begin("tenant-a")).unwrap();
    let first = registry.project(text(&handle, "first")).unwrap();
    let second = registry.project(text(&handle, "second")).unwrap();
    assert_eq!(sequence(&first), 1);
    assert_eq!(sequence(&second), 2);
    let WebChannelEvent::TextDelta { context, .. } = &first.events[0] else {
        unreachable!();
    };
    assert_eq!(context.client_id, "same-client");
    assert_eq!(context.thread_id, "same-thread");
    assert_eq!(context.request_id, "same-request");
    assert!(context.ts.is_some());
}

#[test]
fn context_cannot_supply_module_owned_sequence_or_timestamp() {
    let mut registry = WebTurnRegistry::default();
    let mut request = begin("tenant-a");
    request.context.seq = Some(77);
    assert!(registry.begin(request).is_err());
    let mut request = begin("tenant-a");
    request.context.ts = Some(123);
    assert!(registry.begin(request).is_err());
    assert!(registry.turns.is_empty());
}

#[test]
fn context_and_scope_strings_are_bounded_before_retention() {
    let mut registry = WebTurnRegistry::default();
    let mut request = begin("tenant-a");
    request.scope_key = "s".repeat(MAX_WEB_TURN_SCOPE_BYTES + 1);
    assert!(registry.begin(request).is_err());
    let mut request = begin("tenant-a");
    request.context.client_id = "c".repeat(MAX_WEB_TURN_CONTEXT_FIELD_BYTES + 1);
    assert!(registry.begin(request).is_err());
    assert!(registry.turns.is_empty());
}

#[test]
fn retained_context_byte_budget_rejects_new_turn_without_evicting_active_turns() {
    let mut registry = WebTurnRegistry::default();
    let mut first = None;
    while let Ok(handle) = registry.begin(large_begin()) {
        first.get_or_insert(handle);
    }
    let first = first.expect("one maximum-sized context fits");
    let retained_bytes = registry.retained_bytes;
    assert!(retained_bytes > MAX_WEB_TURN_RETAINED_BYTES - 6144);
    assert!(retained_bytes <= MAX_WEB_TURN_RETAINED_BYTES);
    assert!(registry.begin(large_begin()).is_err());
    assert_eq!(registry.retained_bytes, retained_bytes);
    assert_eq!(
        sequence(&registry.project(text(&first, "still active")).unwrap()),
        1
    );
}

#[test]
fn malformed_project_and_end_handles_are_rejected_before_lookup() {
    let mut registry = WebTurnRegistry::default();
    for handle in [String::new(), "h".repeat(MAX_WEB_TURN_HANDLE_BYTES + 1)] {
        assert!(
            registry
                .project(ProjectWebOutput {
                    handle: handle.clone(),
                    event: ChannelOutputEvent::TextDelta {
                        text: "ignored".into(),
                        iteration: Some(1),
                    },
                })
                .is_err()
        );
        assert!(registry.end(EndWebTurn { handle }).is_err());
    }
    assert!(registry.turns.is_empty());
}

#[test]
fn equal_wire_contexts_under_distinct_scopes_have_independent_handles() {
    let mut registry = WebTurnRegistry::default();
    let tenant_a = registry.begin(begin("tenant-a")).unwrap();
    let tenant_b = registry.begin(begin("tenant-b")).unwrap();
    assert_ne!(tenant_a.handle, tenant_b.handle);
    assert_eq!(
        sequence(&registry.project(text(&tenant_a, "a")).unwrap()),
        1
    );
    assert_eq!(
        sequence(&registry.project(text(&tenant_b, "b")).unwrap()),
        1
    );
}

#[test]
fn explicit_end_and_terminal_output_reject_late_projection_and_release_capacity() {
    let mut registry = WebTurnRegistry::default();
    let ended = registry.begin(begin("tenant-a")).unwrap();
    registry
        .end(EndWebTurn {
            handle: ended.handle.clone(),
        })
        .unwrap();
    assert!(registry.project(text(&ended, "late")).is_err());

    let terminal = registry.begin(begin("tenant-a")).unwrap();
    let output = registry
        .project(ProjectWebOutput {
            handle: terminal.handle.clone(),
            event: ChannelOutputEvent::FinalMessage {
                text: "done".into(),
                parts: Vec::new(),
                receipt: None,
            },
        })
        .unwrap();
    assert!(matches!(
        output.events.as_slice(),
        [WebChannelEvent::ChatDone { .. }]
    ));
    assert!(registry.project(text(&terminal, "late")).is_err());
    assert_eq!(registry.turns.len(), 0);
    assert_eq!(registry.retained_bytes, 0);
}

#[test]
fn capacity_rejects_new_turns_without_evicting_or_resetting_active_turns() {
    let mut registry = WebTurnRegistry::default();
    let first = registry.begin(begin("tenant-a")).unwrap();
    for index in 1..MAX_ACTIVE_WEB_TURNS {
        registry.begin(begin(&format!("tenant-{index}"))).unwrap();
    }
    assert!(registry.begin(begin("overflow")).is_err());
    assert_eq!(
        sequence(&registry.project(text(&first, "still active")).unwrap()),
        1
    );
    registry
        .end(EndWebTurn {
            handle: first.handle.clone(),
        })
        .unwrap();
    let replacement = registry.begin(begin("replacement")).unwrap();
    assert_eq!(
        sequence(&registry.project(text(&replacement, "new turn")).unwrap()),
        1
    );
    assert_eq!(registry.turns.len(), MAX_ACTIVE_WEB_TURNS);
}

#[test]
fn shutdown_clear_invalidates_active_handles_and_releases_capacity() {
    let mut registry = WebTurnRegistry::default();
    let handle = registry.begin(begin("tenant-a")).unwrap();
    registry.clear();
    assert!(registry.project(text(&handle, "late")).is_err());
    assert_eq!(registry.retained_bytes, 0);
}

#[tokio::test]
async fn concurrent_projection_calls_are_serialized_per_handle() {
    let registry = Arc::new(Mutex::new(WebTurnRegistry::default()));
    let handle = registry.lock().await.begin(begin("tenant-a")).unwrap();
    let left = {
        let registry = registry.clone();
        let request = text(&handle, "left");
        tokio::spawn(async move { registry.lock().await.project(request).unwrap() })
    };
    let right = {
        let registry = registry.clone();
        let request = text(&handle, "right");
        tokio::spawn(async move { registry.lock().await.project(request).unwrap() })
    };
    let first = left.await.unwrap();
    let second = right.await.unwrap();
    let mut seqs = [sequence(&first), sequence(&second)];
    seqs.sort_unstable();
    assert_eq!(seqs, [1, 2]);
}
