use super::*;
use crate::channel::{ChannelAdapter, ChannelInboundSink, ChannelTyping};
use crate::providers::native::NativeProvider;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

fn make_channel() -> SignalChannel {
    SignalChannel::new(
        "http://127.0.0.1:8686".to_string(),
        "+1234567890".to_string(),
        None,
        vec!["+1111111111".to_string()],
        false,
        false,
    )
}

fn make_channel_with_group(group_id: &str) -> SignalChannel {
    SignalChannel::new(
        "http://127.0.0.1:8686".to_string(),
        "+1234567890".to_string(),
        Some(group_id.to_string()),
        vec!["*".to_string()],
        true,
        true,
    )
}

fn make_envelope(source_number: Option<&str>, message: Option<&str>) -> Envelope {
    Envelope {
        source: source_number.map(String::from),
        source_number: source_number.map(String::from),
        data_message: message.map(|m| DataMessage {
            message: Some(m.to_string()),
            timestamp: Some(1_700_000_000_000),
            group_info: None,
            attachments: None,
            ..Default::default()
        }),
        story_message: None,
        timestamp: Some(1_700_000_000_000),
        source_name: None,
        ..Default::default()
    }
}

fn typed_text_intent(recipient: &str, content: &str) -> tinychannels_bus::ChannelOutboundIntent {
    let conversation_kind = if recipient.starts_with("group:") {
        tinychannels_bus::ConversationKind::Group
    } else {
        tinychannels_bus::ConversationKind::Dm
    };
    tinychannels_bus::ChannelOutboundIntent {
        channel_id: "signal".into(),
        conversation_id: recipient.into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "signal".into(),
            account_id: None,
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: conversation_kind,
            id: recipient.into(),
            ..Default::default()
        }),
        payload: tinychannels_bus::OutboundPayload::Text {
            text: content.into(),
        },
        ..Default::default()
    }
}

#[path = "configuration_tests.rs"]
mod configuration_tests;
#[path = "transport_tests.rs"]
mod transport_tests;
