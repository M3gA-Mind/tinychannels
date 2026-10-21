use super::*;

#[test]
fn v2_descriptor_frame_round_trips_canonical_capabilities() {
    let mut capabilities = ChannelCapabilities::unknown();
    capabilities.voice_out.synthesis = true;
    let frame = ConnectorToGatewayFrame::DescriptorV2 {
        platform: "discord".into(),
        bot_id: "bot".into(),
        capabilities: Box::new(capabilities),
    };
    let json = frame.to_json().unwrap();
    assert!(json.contains("\"type\":\"descriptor_v2\""));
    assert_eq!(ConnectorToGatewayFrame::from_json(&json).unwrap(), frame);
}

#[test]
fn frozen_v1_descriptor_frame_keeps_its_wire_shape() {
    let frame = ConnectorToGatewayFrame::Descriptor {
        descriptor: CapabilityDescriptor::default(),
    };
    assert_eq!(
        frame.to_json().unwrap(),
        r#"{"type":"descriptor","descriptor":{"contract_version":1,"platform":"","label":"","max_message_length":4096,"supports_draft_streaming":false,"supports_edit":false,"supports_threads":false,"markdown_dialect":"plain","len_unit":"chars","emoji":"🔌","platform_hint":"","pii_safe":false}}"#
    );
}
