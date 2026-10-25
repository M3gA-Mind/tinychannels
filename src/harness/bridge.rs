//! Legacy capability-driven bridge from harness output events to outbound intents.
//!
//! Progressive module dispatch uses `delivery::progressive`; this bridge remains
//! a compatibility projection for callers that already own channel intents.

use crate::channel::{
    ChannelOutboundIntent, ChannelPresentationCapabilities, ChannelStaticCapabilities,
    DeliveryDurability, OutboundPayload,
};
use crate::harness::types::{ChannelOutputEvent, ChannelTurn};
use serde_json::json;
use sha2::{Digest, Sha256};

/// Translation knobs for harness output delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeTranslationOptions {
    pub draft_throttle_ms: u64,
    pub flood_backoff_ms: u64,
    pub durability: DeliveryDurability,
    /// Monotonic sequence within the source turn. Replays of one event must
    /// reuse this value; distinct events must not.
    pub event_sequence: u64,
}

impl Default for BridgeTranslationOptions {
    fn default() -> Self {
        Self {
            draft_throttle_ms: 750,
            flood_backoff_ms: 3000,
            durability: DeliveryDurability::BestEffort,
            event_sequence: 0,
        }
    }
}

/// Translate one harness output event into outbound channel intents.
pub fn translate_output_event(
    turn: &ChannelTurn,
    event: ChannelOutputEvent,
    static_capabilities: &ChannelStaticCapabilities,
    presentation: &ChannelPresentationCapabilities,
    options: BridgeTranslationOptions,
) -> Result<Vec<ChannelOutboundIntent>, serde_json::Error> {
    let payload = match event {
        ChannelOutputEvent::TextDelta { text, .. } => {
            if presentation.supports_edit && !static_capabilities.block_streaming {
                OutboundPayload::NativeChannelData {
                    data: json!({
                        "draft": true,
                        "editInPlace": true,
                        "text": text,
                        "throttleMs": options.draft_throttle_ms,
                        "floodBackoffMs": options.flood_backoff_ms,
                    }),
                }
            } else {
                OutboundPayload::Text { text }
            }
        }
        ChannelOutputEvent::FinalMessage {
            text, mut parts, ..
        } => {
            if parts.is_empty() && !text.is_empty() {
                parts.insert(
                    0,
                    tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text,
                            markdown_dialect: None,
                        },
                    },
                );
            }
            OutboundPayload::Parts { parts }
        }
        ChannelOutputEvent::ToolProgress { label, message } => OutboundPayload::Text {
            text: match message {
                Some(message) => format!("{label}: {message}"),
                None => label,
            },
        },
        ChannelOutputEvent::ApprovalRequest {
            id,
            prompt,
            choices,
        } => {
            if supports_native_approval(presentation, choices.len()) {
                OutboundPayload::PresentationBlocks {
                    blocks: json!({
                        "kind": "approval",
                        "id": id,
                        "prompt": prompt,
                        "choices": choices,
                    }),
                }
            } else {
                OutboundPayload::Text {
                    text: format!("{prompt}\n\nReply with one of: {}", choices.join(", ")),
                }
            }
        }
        ChannelOutputEvent::Clarification { prompt } => OutboundPayload::Text { text: prompt },
        ChannelOutputEvent::Media { text, parts } => {
            let mut content = Vec::new();
            if let Some(text) = text.filter(|text| !text.is_empty()) {
                content.push(tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text,
                        markdown_dialect: None,
                    },
                });
            }
            content.extend(
                parts
                    .into_iter()
                    .map(|part| tinychannels_bus::OutboundContentPart::Content { part }),
            );
            OutboundPayload::Parts { parts: content }
        }
        ChannelOutputEvent::VoiceNote { text, voice } => {
            let mut parts = Vec::new();
            if let Some(text) = text.or_else(|| {
                voice
                    .transcript
                    .as_ref()
                    .map(|transcript| transcript.text.clone())
            }) {
                parts.push(tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text,
                        markdown_dialect: None,
                    },
                });
            }
            parts.push(tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: voice.media,
                    codec: voice.codec,
                    duration_ms: voice.duration_ms,
                },
            });
            OutboundPayload::Parts { parts }
        }
        ChannelOutputEvent::Cancellation { reason } => OutboundPayload::Text {
            text: reason.unwrap_or_else(|| "Cancelled.".to_string()),
        },
        ChannelOutputEvent::Lifecycle {
            stage,
            message,
            iteration,
            max_iterations,
            stop,
        } => OutboundPayload::NativeChannelData {
            data: {
                let mut lifecycle = serde_json::Map::new();
                lifecycle.insert("stage".into(), json!(stage));
                if let Some(message) = message {
                    lifecycle.insert("message".into(), json!(message));
                }
                if let Some(iteration) = iteration {
                    lifecycle.insert("iteration".into(), json!(iteration));
                }
                if let Some(max_iterations) = max_iterations {
                    lifecycle.insert("maxIterations".into(), json!(max_iterations));
                }
                if let Some(stop) = stop {
                    lifecycle.insert("stop".into(), json!(stop));
                }
                json!({"lifecycle": lifecycle})
            },
        },
        ChannelOutputEvent::Native { data } => OutboundPayload::NativeChannelData { data },
        event => OutboundPayload::NativeChannelData {
            data: serde_json::to_value(event)?,
        },
    };

    let scope = serde_json::to_vec(&(
        &turn.envelope.channel,
        &turn.envelope.conversation,
        options.event_sequence,
    ))?;
    let scoped_key = hex::encode(Sha256::digest(scope));
    Ok(vec![ChannelOutboundIntent {
        idempotency_key: format!("{}:{}", turn.id, scoped_key),
        channel_id: turn.envelope.channel.id.clone(),
        conversation_id: turn.envelope.conversation.id.clone(),
        channel_ref: Some(turn.envelope.channel.clone()),
        conversation_ref: Some(turn.envelope.conversation.clone()),
        reply_to_id: Some(turn.envelope.message_id.clone()).filter(|id| !id.is_empty()),
        thread_id: turn.envelope.conversation.thread_id.clone(),
        turn_id: Some(turn.id.clone()),
        durability: options.durability,
        payload,
    }])
}

fn supports_native_approval(
    presentation: &ChannelPresentationCapabilities,
    choice_count: usize,
) -> bool {
    presentation
        .max_actions
        .is_some_and(|max| max >= choice_count)
        && presentation.max_actions_per_row.unwrap_or(1) > 0
}
