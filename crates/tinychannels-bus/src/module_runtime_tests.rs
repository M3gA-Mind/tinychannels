use super::*;

#[test]
fn runtime_reservation_wire_preserves_the_module_supplied_opaque_paths() {
    let value = serde_json::json!({
        "handle": "opaque",
        "object_path": "/ai/tinyhumans/tinychannels/Channels/runtime_opaque",
        "host_object_path": "/ai/tinyhumans/tinychannels/ChannelsHost/runtime_opaque"
    });
    let reservation: ChannelRuntimeReservation = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(reservation).unwrap(), value);
}

#[test]
fn runtime_reservation_rejects_unrecognized_identity_fields() {
    assert!(
        serde_json::from_value::<ChannelRuntimeReservation>(serde_json::json!({
            "handle": "opaque", "object_path": "/module", "host_object_path": "/host",
            "user_id": "must-not-be-module-authority"
        }))
        .is_err()
    );
}
