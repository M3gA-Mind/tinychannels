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

#[test]
fn configured_catalog_and_message_preparation_keep_wire_vocabulary() {
    let catalog = serde_json::json!({
        "name": "linq", "ingress": "external_webhook",
        "interaction": {
            "capabilities": {
                "version": 2,
                "remoteControl": true, "chatApprovals": true,
                "progressiveEdits": false, "historyKeyIgnoresThread": false,
                "reactions": false,
                "account": {"proactiveTarget": null}
            }
        }
    });
    let typed: ConfiguredChannelInfo = serde_json::from_value(catalog.clone()).unwrap();
    assert_eq!(typed.ingress, ChannelIngressKind::ExternalWebhook);
    assert_eq!(typed.capabilities.version, 2);
    assert!(typed.capabilities.remote_control && typed.capabilities.chat_approvals);
    let message = serde_json::json!({"kind":"legacy","message":{"text":"fixture"}});
    let typed: ChannelMessagePreparation = serde_json::from_value(message.clone()).unwrap();
    assert_eq!(serde_json::to_value(typed).unwrap(), message);
}
