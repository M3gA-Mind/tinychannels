//! Bounded module-owned progressive reply workers and retained terminal cleanup.
use crate::host::HostChannels;
use crate::provider::Running;
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tinybus::{Error, Result};
use tinychannels::delivery::progressive::{
    EDIT_FLUSH_INTERVAL, FILLER_INTERVAL, ProgressiveOutputContext, ProgressiveOutputDispatch,
    ProgressiveReply, ProgressiveSendError, ProgressiveSender, TYPING_REFRESH_INTERVAL,
};
use tinychannels_bus::module_progressive::{
    ChannelOutputEvent, ProgressiveCommand, ProgressiveResult, ProgressiveTransportRequest,
    ProgressiveTransportResult,
};
use tinychannels_bus::{ChannelBackendRequest, ChannelBackendResult};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

const MAX_ACTIVE: usize = 16;
const MAX_RETAINED: usize = 128;
const MAX_CONTENT: usize = 384 * 1024;
// Both text and thinking can fill their content budget in one feed. JSON
// escaping expands a byte to at most six bytes; retain bounded envelope space.
const MAX_REQUEST: usize = MAX_CONTENT * 12 + 64 * 1024;
const LEGACY_TURN_LIFETIME: Duration = Duration::from_secs(180);

struct Sender {
    host: HostChannels,
    context: Option<ProgressiveOutputContext>,
    capabilities: tinychannels_bus::ChannelCapabilities,
    channel: String,
    send_sequence: std::sync::atomic::AtomicU64,
}

impl Sender {
    async fn invoke(
        &self,
        request: ProgressiveTransportRequest,
    ) -> std::result::Result<Value, ProgressiveSendError> {
        let scoped_effect = self.context.is_some()
            && matches!(
                &request,
                ProgressiveTransportRequest::Send { .. }
                    | ProgressiveTransportRequest::SendTyped { .. }
                    | ProgressiveTransportRequest::Edit { .. }
                    | ProgressiveTransportRequest::Delete { .. }
            );
        let authority = match &request {
            ProgressiveTransportRequest::Edit {
                receipt,
                message_id,
                ..
            }
            | ProgressiveTransportRequest::Delete {
                receipt,
                message_id,
                ..
            } => receipt.clone().or_else(|| {
                Some(tinychannels_bus::channel::MessageReceipt {
                    primary_platform_message_id: Some(message_id.clone()),
                    platform_message_ids: vec![message_id.clone()],
                    ..Default::default()
                })
            }),
            _ => None,
        };
        let result = self
            .host
            .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(Box::new(
                request,
            )))
            .await
            .map_err(|_| {
                if scoped_effect {
                    let mut outcome = tinychannels_bus::channel::ChannelSendOutcome {
                        receipt: authority.unwrap_or_default(),
                        ..Default::default()
                    };
                    outcome.error = Some(tinychannels_bus::channel::ChannelSendError {
                        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                        message: "progressive callback failed after an unknown side effect".into(),
                        sent_before_error: true,
                        ..Default::default()
                    });
                    ProgressiveSendError::PartialOutcome(Box::new(outcome))
                } else {
                    ProgressiveSendError::Unavailable
                }
            })?;
        match result {
            ChannelBackendResult::ProgressiveTransport(result) => match *result {
                ProgressiveTransportResult::Acknowledged => Ok(Value::Null),
                ProgressiveTransportResult::Outcome(outcome) => match outcome.error {
                    None => serde_json::to_value(outcome.receipt)
                        .map_err(|error| ProgressiveSendError::Other(error.into())),
                    Some(_) => Err(ProgressiveSendError::PartialOutcome(outcome)),
                },
                ProgressiveTransportResult::Success(value) => {
                    if value.get("error").is_some_and(|error| !error.is_null())
                        || value.get("success").and_then(Value::as_bool) == Some(false)
                        || value.get("status").and_then(Value::as_str) == Some("unknown_after_send")
                    {
                        Err(ProgressiveSendError::Other(anyhow::anyhow!(
                            "progressive delivery failed"
                        )))
                    } else {
                        Ok(value)
                    }
                }
                ProgressiveTransportResult::Unavailable => Err(ProgressiveSendError::Unavailable),
                ProgressiveTransportResult::EditUnsupported => {
                    Err(ProgressiveSendError::EditUnsupported)
                }
                ProgressiveTransportResult::MessageGone => Err(ProgressiveSendError::MessageGone),
                _ => Err(ProgressiveSendError::Other(anyhow::anyhow!(
                    "progressive transport failed"
                ))),
            },
            _ => Err(ProgressiveSendError::Other(anyhow::anyhow!(
                "progressive transport failed"
            ))),
        }
    }
}

#[async_trait]
impl ProgressiveSender for Sender {
    async fn output_event(
        &self,
        channel: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        event: &ChannelOutputEvent,
    ) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::OutputEvent {
            channel: channel.to_owned(),
            turn_id: context.map(|context| context.turn_id.clone()),
            channel_ref: context.map(|context| context.channel.clone()),
            conversation: context.map(|context| context.conversation.clone()),
            sequence,
            event: Box::new(event.clone()),
        })
        .await
        .map(drop)
    }
    async fn send(
        &self,
        channel: &str,
        text: &str,
    ) -> std::result::Result<Value, ProgressiveSendError> {
        let (message, sequence, idempotency_key) = if let Some(context) = &self.context {
            let sequence = self
                .send_sequence
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let scope = serde_json::to_vec(&(
                &context.turn_id,
                &context.channel,
                &context.conversation,
                sequence,
                text,
                "progressive_send",
            ))
            .expect("scoped progressive identity serializes");
            (
                serde_json::json!({"text": text}),
                Some(sequence),
                Some(hex::encode(Sha256::digest(scope))),
            )
        } else {
            let intent = tinychannels::outbound_intent_from_legacy_message(
                channel,
                serde_json::json!({"text": text}),
            );
            (
                tinychannels::legacy_message_value_from_outbound_intent(&intent),
                None,
                None,
            )
        };
        self.invoke(ProgressiveTransportRequest::Send {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            sequence,
            idempotency_key,
            message,
        })
        .await
    }
    async fn send_parts(
        &self,
        channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError>
    {
        send_typed_part(
            self,
            TypedPartRequest {
                channel,
                parts,
                text,
                context,
                sequence,
                segment_index: 0,
                final_phase: false,
            },
        )
        .await
    }

    async fn should_synthesize_final(
        &self,
        _channel: &str,
        context: Option<&ProgressiveOutputContext>,
        text: &str,
    ) -> std::result::Result<bool, ProgressiveSendError> {
        if text.trim().is_empty() || !self.capabilities.voice_out.synthesis {
            return Ok(false);
        }
        let Some(context) = context else {
            return Ok(false);
        };
        let scope = tinychannels_bus::HostVoiceScope {
            configured_instance: self.channel.clone(),
            channel: context.channel.clone(),
            conversation: context.conversation.clone(),
            turn_id: context.turn_id.clone(),
        };
        let policy = self
            .host
            .voice_reply_policy(tinychannels_bus::HostVoiceReplyPolicyRequest { scope })
            .await
            .ok();
        Ok(policy.is_some_and(|policy| match policy.mode {
            tinychannels_bus::HostVoiceReplyMode::Mirror => policy.inbound_has_voice,
            tinychannels_bus::HostVoiceReplyMode::Always => true,
            tinychannels_bus::HostVoiceReplyMode::Never => false,
            tinychannels_bus::HostVoiceReplyMode::SpeakReply => policy.explicitly_requested,
        }))
    }

    async fn send_parts_with_segment(
        &self,
        channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        segment_index: u32,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError>
    {
        send_typed_part(
            self,
            TypedPartRequest {
                channel,
                parts,
                text,
                context,
                sequence,
                segment_index,
                final_phase: false,
            },
        )
        .await
    }

    async fn send_final_parts_with_segment(
        &self,
        channel: &str,
        parts: &[tinychannels_bus::OutboundContentPart],
        text: &str,
        context: Option<&ProgressiveOutputContext>,
        sequence: u64,
        segment_index: u32,
    ) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError>
    {
        send_typed_part(
            self,
            TypedPartRequest {
                channel,
                parts,
                text,
                context,
                sequence,
                segment_index,
                final_phase: true,
            },
        )
        .await
    }

    async fn edit(
        &self,
        channel: &str,
        message_id: &str,
        text: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Edit {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            message_id: message_id.into(),
            receipt: None,
            text: text.into(),
        })
        .await
        .map(drop)
    }
    async fn delete(
        &self,
        channel: &str,
        message_id: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Delete {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            message_id: message_id.into(),
            receipt: None,
        })
        .await
        .map(drop)
    }

    async fn edit_with_receipt(
        &self,
        channel: &str,
        receipt: &tinychannels_bus::channel::MessageReceipt,
        text: &str,
    ) -> std::result::Result<(), ProgressiveSendError> {
        let message_id = receipt
            .primary_platform_message_id
            .as_deref()
            .ok_or_else(|| {
                ProgressiveSendError::Other(anyhow::anyhow!("receipt has no primary message ID"))
            })?;
        self.invoke(ProgressiveTransportRequest::Edit {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            message_id: message_id.into(),
            receipt: Some(receipt.clone()),
            text: text.into(),
        })
        .await
        .map(drop)
    }

    async fn delete_with_receipt(
        &self,
        channel: &str,
        receipt: &tinychannels_bus::channel::MessageReceipt,
    ) -> std::result::Result<(), ProgressiveSendError> {
        let message_id = receipt
            .primary_platform_message_id
            .as_deref()
            .ok_or_else(|| {
                ProgressiveSendError::Other(anyhow::anyhow!("receipt has no primary message ID"))
            })?;
        self.invoke(ProgressiveTransportRequest::Delete {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
            message_id: message_id.into(),
            receipt: Some(receipt.clone()),
        })
        .await
        .map(drop)
    }
    async fn typing(&self, channel: &str) -> std::result::Result<(), ProgressiveSendError> {
        self.invoke(ProgressiveTransportRequest::Typing {
            channel: channel.into(),
            turn_id: self.context.as_ref().map(|context| context.turn_id.clone()),
            channel_ref: self.context.as_ref().map(|context| context.channel.clone()),
            conversation: self
                .context
                .as_ref()
                .map(|context| context.conversation.clone()),
        })
        .await
        .map(drop)
    }
}

struct TypedPartRequest<'a> {
    channel: &'a str,
    parts: &'a [tinychannels_bus::OutboundContentPart],
    text: &'a str,
    context: Option<&'a ProgressiveOutputContext>,
    sequence: u64,
    segment_index: u32,
    final_phase: bool,
}

async fn send_typed_part(
    sender: &Sender,
    request: TypedPartRequest<'_>,
) -> std::result::Result<tinychannels_bus::channel::ChannelSendOutcome, ProgressiveSendError> {
    let TypedPartRequest {
        channel,
        parts,
        text,
        context,
        sequence,
        segment_index,
        final_phase,
    } = request;
    let mut prepared_parts = parts.to_vec();
    let mut prepared_text = text.to_owned();
    if let Some(context) = context {
        let synths = prepared_parts
            .iter()
            .filter_map(|part| match part {
                tinychannels_bus::OutboundContentPart::Voice {
                    voice: tinychannels_bus::OutboundVoice::Synthesize { text, voice },
                } => Some((text.clone(), voice.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        if synths.len() > 1 {
            return Err(ProgressiveSendError::UnsupportedContent);
        }
        let explicit_synthesis = !synths.is_empty();
        let (requested_text, requested_voice) = synths
            .into_iter()
            .next()
            .unwrap_or_else(|| (prepared_text.clone(), None));
        if (final_phase || explicit_synthesis)
            && !requested_text.trim().is_empty()
            && sender.capabilities.voice_out.synthesis
        {
            let scope = tinychannels_bus::HostVoiceScope {
                configured_instance: sender.channel.clone(),
                channel: context.channel.clone(),
                conversation: context.conversation.clone(),
                turn_id: context.turn_id.clone(),
            };
            let policy = sender
                .host
                .voice_reply_policy(tinychannels_bus::HostVoiceReplyPolicyRequest {
                    scope: scope.clone(),
                })
                .await
                .ok();
            let should_speak = policy.as_ref().is_some_and(|policy| match policy.mode {
                tinychannels_bus::HostVoiceReplyMode::Mirror => policy.inbound_has_voice,
                tinychannels_bus::HostVoiceReplyMode::Always => true,
                tinychannels_bus::HostVoiceReplyMode::Never => false,
                tinychannels_bus::HostVoiceReplyMode::SpeakReply => policy.explicitly_requested,
            });
            let had_synthesis = prepared_parts.iter().any(|part| {
                matches!(
                    part,
                    tinychannels_bus::OutboundContentPart::Voice {
                        voice: tinychannels_bus::OutboundVoice::Synthesize { .. }
                    }
                )
            });
            prepared_parts.retain(|part| {
                !matches!(
                    part,
                    tinychannels_bus::OutboundContentPart::Voice {
                        voice: tinychannels_bus::OutboundVoice::Synthesize { .. }
                    }
                )
            });
            if should_speak {
                let instance = sender.channel.clone();
                let synthesizer =
                    crate::provider::ModuleSynthesizer(sender.host.clone(), instance.clone());
                let codec = crate::provider::ModuleVoiceCodec(sender.host.clone());
                let store = crate::provider::ModuleVoiceMediaStore(sender.host.clone(), instance);
                let native_voice = sender.capabilities.voice_out.supported
                    && !sender.capabilities.voice_out.accepted_codecs.is_empty();
                let audio_codecs = sender
                    .capabilities
                    .content
                    .audio
                    .accepted_types
                    .iter()
                    .filter_map(|mime| match mime.as_str() {
                        "audio/ogg" | "audio/opus" => Some("ogg_opus".to_owned()),
                        "audio/mp4" => Some("m4a_aac".to_owned()),
                        "audio/mpeg" => Some("mp3".to_owned()),
                        "audio/wav" | "audio/wave" => Some("wav_pcm16".to_owned()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let audio_file = !native_voice
                    && sender.capabilities.content.audio.outbound
                    && sender
                        .capabilities
                        .content
                        .parts
                        .iter()
                        .any(|part| part == "audio")
                    && !audio_codecs.is_empty();
                // A link fallback is host-owned media and does not advertise
                // provider codec support. Store one bounded WAV in that case.
                let link_codec = ["wav_pcm16".to_owned()];
                let accepted_codecs: &[String] = if native_voice {
                    &sender.capabilities.voice_out.accepted_codecs
                } else if audio_file {
                    audio_codecs.as_slice()
                } else {
                    &link_codec
                };
                let (max_size_bytes, max_duration_ms) = if native_voice {
                    (
                        sender.capabilities.voice_out.max_size_bytes,
                        sender.capabilities.voice_out.max_duration_ms,
                    )
                } else if audio_file {
                    (
                        sender.capabilities.content.audio.max_size_bytes,
                        sender.capabilities.content.audio.max_duration_ms,
                    )
                } else {
                    (Some(8 * 1024 * 1024), Some(5 * 60 * 1000))
                };
                match tinychannels::host::prepare_voice_reply(
                    scope,
                    requested_text.clone(),
                    requested_voice,
                    &synthesizer,
                    &codec,
                    &store,
                    tinychannels::host::VoiceReplyConstraints {
                        accepted_codecs,
                        max_size_bytes,
                        max_duration_ms,
                    },
                )
                .await
                {
                    Ok(prepared) => {
                        let note = prepared.note;
                        if native_voice {
                            prepared_parts.push(tinychannels_bus::OutboundContentPart::Voice {
                                voice: tinychannels_bus::OutboundVoice::Media {
                                    media: note.media,
                                    audio_base64: None,
                                    codec: note.codec,
                                    duration_ms: note.duration_ms,
                                },
                            });
                        } else if audio_file {
                            prepared_parts.push(tinychannels_bus::OutboundContentPart::Content {
                                part: tinychannels_bus::ContentPart::Audio {
                                    media: note.media,
                                    codec: note.codec,
                                    duration_ms: note.duration_ms,
                                    title: None,
                                    transcript: Some(requested_text.clone()),
                                },
                            });
                        } else if let Some(url) =
                            prepared.accessible_url.filter(|url| !url.trim().is_empty())
                        {
                            prepared_text.push('\n');
                            prepared_text.push_str(&url);
                        } else {
                            prepared_text.push_str(
                                "\n\nAudio could not be attached; here is the text response.",
                            );
                        }
                    }
                    Err(error) => {
                        let detail = error.to_string();
                        let kind = if detail.contains("duration limit") {
                            prepared_text.push_str(
                                "\n\nAudio reply was too long to send; here is the text response.",
                            );
                            "duration_limit"
                        } else if detail.contains("size limit") {
                            prepared_text.push_str(
                                "\n\nAudio reply exceeded the channel size limit; here is the text response.",
                            );
                            "size_limit"
                        } else {
                            "unavailable"
                        };
                        tracing::warn!(channel = %sender.channel, kind, "shared voice preparation unavailable");
                        if prepared_text.trim().is_empty() {
                            prepared_text = requested_text;
                        }
                    }
                }
            } else if had_synthesis {
                // The model explicitly requested speech, but host policy denied it.
                if prepared_text.trim().is_empty() {
                    prepared_text = requested_text;
                }
            }
            if prepared_text != text {
                let suffix = prepared_text.strip_prefix(text).unwrap_or(&prepared_text);
                if let Some(text_part) = prepared_parts.iter_mut().rev().find_map(|part| match part
                {
                    tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text { text, .. },
                    } => Some(text),
                    _ => None,
                }) {
                    text_part.push_str(suffix);
                } else if !prepared_text.trim().is_empty() {
                    prepared_parts.push(tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text: prepared_text.clone(),
                            markdown_dialect: None,
                        },
                    });
                }
            }
        }
    }
    let idempotency_key = context.map(|context| {
        let scope = serde_json::to_vec(&(
            &context.turn_id,
            &context.channel,
            &context.conversation,
            sequence,
            segment_index,
            "provider_send",
        ))
        .expect("scoped channel identity serializes");
        hex::encode(Sha256::digest(scope))
    });
    match sender
        .host
        .dispatch_controller_backend(ChannelBackendRequest::ProgressiveTransport(Box::new(
            ProgressiveTransportRequest::SendTyped {
                channel: channel.into(),
                turn_id: context.map(|context| context.turn_id.clone()),
                channel_ref: context.map(|context| context.channel.clone()),
                conversation: context.map(|context| context.conversation.clone()),
                sequence: context.map(|_| sequence),
                segment_index: context.map(|_| segment_index),
                idempotency_key,
                parts: prepared_parts,
                text: prepared_text,
            },
        )))
        .await
        .map_err(|_| {
            let outcome = tinychannels_bus::channel::ChannelSendOutcome {
                error: Some(tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                    message: "typed progressive callback failed after an unknown side effect"
                        .into(),
                    sent_before_error: true,
                    ..Default::default()
                }),
                ..Default::default()
            };
            ProgressiveSendError::PartialOutcome(Box::new(outcome))
        })? {
        ChannelBackendResult::ProgressiveTransport(result) => match *result {
            ProgressiveTransportResult::Outcome(outcome) => Ok(*outcome),
            ProgressiveTransportResult::Unavailable => Err(ProgressiveSendError::Unavailable),
            ProgressiveTransportResult::Failed => Err(ProgressiveSendError::Other(
                anyhow::anyhow!("typed provider send failed with unknown outcome"),
            )),
            ProgressiveTransportResult::UnsupportedContent => {
                Err(ProgressiveSendError::UnsupportedContent)
            }
            _ => {
                let outcome = tinychannels_bus::channel::ChannelSendOutcome {
                    error: Some(tinychannels_bus::channel::ChannelSendError {
                        kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                        message: "typed progressive callback returned an invalid acknowledgement"
                            .into(),
                        sent_before_error: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                Err(ProgressiveSendError::PartialOutcome(Box::new(outcome)))
            }
        },
        _ => Err(ProgressiveSendError::Other(anyhow::anyhow!(
            "typed provider send failed with unknown outcome"
        ))),
    }
}

enum Command {
    Feed(
        Vec<ChannelOutputEvent>,
        bool,
        Option<[u8; 32]>,
        oneshot::Sender<Result<ProgressiveResult>>,
    ),
    Finish(String, oneshot::Sender<()>),
    Abort(oneshot::Sender<()>),
}

struct Closing {
    fingerprint: [u8; 32],
    task: Option<JoinHandle<std::result::Result<(), String>>>,
    outcome: Option<std::result::Result<(), String>>,
}
struct Entry {
    commands: mpsc::Sender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    closing: Mutex<Option<Closing>>,
    terminal_feed: Arc<Mutex<Option<TerminalMemo>>>,
}
struct TerminalMemo {
    fingerprint: [u8; 32],
    result: Option<std::result::Result<ProgressiveResult, String>>,
    notify: watch::Sender<Option<std::result::Result<ProgressiveResult, String>>>,
}
struct State {
    terminal: bool,
    entries: HashMap<String, Arc<Entry>>,
}

/// One private runtime's reply handles; all callbacks retain that runtime's host.
pub(crate) struct Replies {
    host: HostChannels,
    running: Arc<Mutex<HashMap<String, Arc<Running>>>>,
    state: Mutex<State>,
}

impl Replies {
    pub(crate) fn new(
        host: HostChannels,
        running: Arc<Mutex<HashMap<String, Arc<Running>>>>,
    ) -> Self {
        Self {
            host,
            running,
            state: Mutex::new(State {
                terminal: false,
                entries: HashMap::new(),
            }),
        }
    }

    pub(crate) async fn execute(&self, request: ProgressiveCommand) -> Result<ProgressiveResult> {
        if serde_json::to_vec(&request).map_or(true, |bytes| bytes.len() > MAX_REQUEST) {
            return Err(Error::failed("progressive request exceeds its limit"));
        }
        match request {
            ProgressiveCommand::Open { channel } => self.open(channel).await,
            ProgressiveCommand::OpenOutput {
                channel,
                conversation,
                turn_id,
                lifetime_ms,
            } => {
                self.open_output(channel, conversation, turn_id, lifetime_ms)
                    .await
            }
            ProgressiveCommand::Feed { handle, events } => {
                let events = events.into_iter().map(compat_event).collect();
                self.feed(handle, events, false).await
            }
            ProgressiveCommand::FeedOutput { handle, events } => {
                self.feed(handle, events, true).await
            }
            ProgressiveCommand::Finish { handle, text } => {
                if text.len() > MAX_CONTENT {
                    return Err(Error::failed("progressive content exceeds its limit"));
                }
                let entry = self.entry(&handle).await?;
                close_entry(entry, text, false).await?;
                Ok(ProgressiveResult::Closed)
            }
            ProgressiveCommand::Close { handle } => {
                let entry = self.state.lock().await.entries.get(&handle).cloned();
                if let Some(entry) = entry {
                    abort_entry(entry).await?;
                }
                Ok(ProgressiveResult::Closed)
            }
        }
    }

    async fn feed(
        &self,
        handle: String,
        events: Vec<ChannelOutputEvent>,
        forward: bool,
    ) -> Result<ProgressiveResult> {
        if events.len() > 32 {
            return Err(Error::failed("too many progressive events"));
        }
        let terminal_positions = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                matches!(
                    event,
                    ChannelOutputEvent::FinalMessage { .. }
                        | ChannelOutputEvent::Cancellation { .. }
                )
                .then_some(index)
            })
            .collect::<Vec<_>>();
        if terminal_positions.len() > 1
            || terminal_positions
                .first()
                .is_some_and(|position| *position + 1 != events.len())
        {
            return Err(Error::failed(
                "terminal progressive event must be unique and last",
            ));
        }
        let entry = self.entry(&handle).await?;
        let terminal = events.iter().any(|event| {
            matches!(
                event,
                ChannelOutputEvent::FinalMessage { .. } | ChannelOutputEvent::Cancellation { .. }
            )
        });
        let fingerprint = if terminal {
            Some(
                Sha256::digest(
                    serde_json::to_vec(&events)
                        .map_err(|_| Error::failed("terminal event could not be encoded"))?,
                )
                .into(),
            )
        } else {
            None
        };
        if let Some(fingerprint) = fingerprint {
            let wait = {
                let mut memo = entry.terminal_feed.lock().await;
                match memo.as_mut() {
                    Some(memo) if memo.fingerprint != fingerprint => {
                        return Err(Error::failed("progressive terminal content differs"));
                    }
                    Some(memo) => {
                        if let Some(result) = memo.result.clone() {
                            return result.map_err(Error::failed);
                        }
                        Some(memo.notify.subscribe())
                    }
                    None => {
                        let (notify, _) = watch::channel(None);
                        *memo = Some(TerminalMemo {
                            fingerprint,
                            result: None,
                            notify,
                        });
                        None
                    }
                }
            };
            if let Some(mut wait) = wait {
                loop {
                    if let Some(result) = wait.borrow().clone() {
                        return result.map_err(Error::failed);
                    }
                    if wait.changed().await.is_err() {
                        return Err(Error::failed("progressive terminal result was lost"));
                    }
                }
            }
        } else if entry.terminal_feed.lock().await.is_some() {
            return Err(Error::failed("progressive reply is terminal"));
        }
        let (reply, wait) = oneshot::channel();
        if let Some(fingerprint) = fingerprint {
            let retained = entry.clone();
            let (owner_tx, owner_rx) = oneshot::channel();
            tokio::spawn(async move {
                let result = async {
                    let closing = retained.closing.lock().await;
                    if closing.is_some() {
                        return Err(Error::failed("progressive reply is closed"));
                    }
                    if retained
                        .commands
                        .send(Command::Feed(events, forward, Some(fingerprint), reply))
                        .await
                        .is_err()
                    {
                        drop(closing);
                        return Err(Error::failed("progressive reply is closed"));
                    }
                    drop(closing);
                    wait.await
                        .map_err(|_| Error::failed("progressive worker stopped"))?
                }
                .await;
                record_terminal(&retained.terminal_feed, Some(fingerprint), &result).await;
                let _ = owner_tx.send(result);
            });
            // The spawned owner continues admission and delivery even if this caller disconnects.
            // Identical retries observe the retained watch result rather than sending again.
            return owner_rx
                .await
                .map_err(|_| Error::failed("terminal delivery owner stopped"))?;
        }
        let closing = entry.closing.lock().await;
        if closing.is_some() {
            return Err(Error::failed("progressive reply is closed"));
        }
        if entry
            .commands
            .send(Command::Feed(events, forward, None, reply))
            .await
            .is_err()
        {
            return Err(Error::failed("progressive reply is closed"));
        }
        drop(closing);
        wait.await
            .map_err(|_| Error::failed("progressive worker stopped"))?
    }

    async fn entry(&self, handle: &str) -> Result<Arc<Entry>> {
        let state = self.state.lock().await;
        if state.terminal {
            return Err(Error::failed("progressive replies are shutting down"));
        }
        state
            .entries
            .get(handle)
            .cloned()
            .ok_or_else(|| Error::failed("unknown progressive handle"))
    }

    async fn open(&self, channel: String) -> Result<ProgressiveResult> {
        self.open_inner(channel, None, LEGACY_TURN_LIFETIME).await
    }

    async fn open_output(
        &self,
        channel: tinychannels_bus::ChannelRef,
        conversation: tinychannels_bus::ConversationRef,
        turn_id: String,
        lifetime_ms: u64,
    ) -> Result<ProgressiveResult> {
        if channel.id.is_empty()
            || conversation.id.is_empty()
            || turn_id.is_empty()
            || lifetime_ms == 0
            || channel.id.len() > 4096
            || conversation.id.len() > 4096
            || turn_id.len() > 256
        {
            return Err(Error::failed("invalid scoped progressive target"));
        }
        let lifetime = Duration::from_millis(lifetime_ms);
        tokio::time::Instant::now()
            .checked_add(lifetime)
            .ok_or_else(|| Error::failed("scoped progressive lease is out of range"))?;
        let route = format!("{}:{}", channel.id, conversation.id);
        self.open_inner(
            route,
            Some(ProgressiveOutputContext {
                turn_id,
                channel,
                conversation,
            }),
            lifetime,
        )
        .await
    }

    async fn open_inner(
        &self,
        channel: String,
        context: Option<ProgressiveOutputContext>,
        lifetime: Duration,
    ) -> Result<ProgressiveResult> {
        if channel.is_empty() || channel.len() > 4096 {
            return Err(Error::failed("invalid progressive target"));
        }
        let provider_id = tinychannels_bus::provider_id(&channel);
        let running = self.running.lock().await;
        let (capabilities, media_account_scope) = if let Some(context) = context.as_ref() {
            let configured_instance = context
                .channel
                .account_id
                .as_deref()
                .unwrap_or(&context.channel.id);
            if tinychannels_bus::provider_id(configured_instance) != context.channel.id {
                return Err(Error::failed(
                    "scoped channel identity does not match its configured account",
                ));
            }
            let provider = running
                .get(configured_instance)
                .ok_or_else(|| Error::failed("scoped channel instance is not configured"))?;
            (
                provider.channel.capabilities(),
                provider.media_account_scope.clone(),
            )
        } else {
            running.get(provider_id).map_or_else(
                || (tinychannels_bus::ChannelCapabilities::unknown(), None),
                |provider| {
                    (
                        provider.channel.capabilities(),
                        provider.media_account_scope.clone(),
                    )
                },
            )
        };
        drop(running);
        let mut state = self.state.lock().await;
        if state.terminal {
            return Err(Error::failed("progressive replies are shutting down"));
        }
        let mut completed = Vec::new();
        let mut active = 0;
        for (handle, entry) in &state.entries {
            let finished = match entry.worker.try_lock() {
                Ok(worker) => match worker.as_ref() {
                    Some(worker) => worker.is_finished(),
                    None => match entry.closing.try_lock() {
                        Ok(closing) => closing.as_ref().is_some_and(|closing| {
                            closing.outcome.is_some()
                                || closing.task.as_ref().is_some_and(|task| task.is_finished())
                        }),
                        Err(_) => false,
                    },
                },
                Err(_) => false,
            };
            if finished {
                completed.push(handle.clone());
            } else {
                active += 1;
            }
        }
        if active >= MAX_ACTIVE {
            return Err(Error::failed("progressive reply capacity reached"));
        }
        if state.entries.len() >= MAX_RETAINED {
            if let Some(handle) = completed.into_iter().next() {
                state.entries.remove(&handle);
            } else {
                return Err(Error::failed("progressive reply capacity reached"));
            }
        }
        let handle = uuid::Uuid::new_v4().simple().to_string();
        let (commands, inbox) = mpsc::channel(16);
        let host = self.host.clone();
        let terminal_feed = Arc::new(Mutex::new(None));
        let worker = tokio::spawn(run(
            host,
            channel,
            capabilities,
            media_account_scope,
            context,
            lifetime,
            terminal_feed.clone(),
            inbox,
        ));
        state.entries.insert(
            handle.clone(),
            Arc::new(Entry {
                commands,
                worker: Mutex::new(Some(worker)),
                closing: Mutex::new(None),
                terminal_feed,
            }),
        );
        Ok(ProgressiveResult::Opened { handle })
    }

    /// Freeze admission and await terminal delivery before callback shutdown.
    pub(crate) async fn shutdown(&self) -> Result<()> {
        let entries = {
            let mut state = self.state.lock().await;
            state.terminal = true;
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        // Start every terminal task before awaiting any one provider. A blocked
        // delivery must not prevent unrelated reply workers from draining.
        let mut closes = tokio::task::JoinSet::new();
        for entry in entries {
            closes.spawn(abort_entry(entry));
        }
        let mut failure = None;
        while let Some(result) = closes.join_next().await {
            let result =
                result.unwrap_or_else(|_| Err(Error::failed("progressive shutdown worker failed")));
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            // Retained per-entry terminal tasks can finish after the timeout;
            // the next Shutdown awaits them without sending a second reply.
            return Err(error);
        }
        self.state.lock().await.entries.clear();
        Ok(())
    }
}

fn compat_event(
    event: tinychannels_bus::module_progressive::ProgressiveEvent,
) -> ChannelOutputEvent {
    use tinychannels_bus::module_progressive::ProgressiveEvent as Legacy;
    match event {
        Legacy::TextDelta(text) => ChannelOutputEvent::TextDelta {
            text,
            iteration: None,
        },
        Legacy::ThinkingDelta(text) => ChannelOutputEvent::ThinkingDelta {
            text,
            iteration: None,
        },
        Legacy::ToolCall(tool_name) => ChannelOutputEvent::ToolProgress {
            label: tool_name,
            message: None,
        },
        Legacy::ToolResult { tool_name, success } => ChannelOutputEvent::ToolCallCompleted {
            call_id: String::new(),
            tool_name,
            success,
            output_chars: 0,
            output: String::new(),
            arguments: None,
            elapsed_ms: 0,
            iteration: 0,
            failure: None,
            display_label: None,
            display_detail: None,
            structured: None,
        },
    }
}

async fn close_entry(entry: Arc<Entry>, text: String, retain_existing: bool) -> Result<()> {
    let fingerprint: [u8; 32] = Sha256::digest(text.as_bytes()).into();
    let mut closing = entry.closing.lock().await;
    if let Some(existing) = closing.as_ref() {
        if !retain_existing && existing.fingerprint != fingerprint {
            return Err(Error::failed("progressive terminal content differs"));
        }
    } else {
        let retained = entry.clone();
        let task = tokio::spawn(async move {
            let (reply, wait) = oneshot::channel();
            if retained
                .commands
                .send(Command::Finish(text, reply))
                .await
                .is_ok()
            {
                let _ = wait.await;
            }
            if let Some(worker) = retained.worker.lock().await.take() {
                worker
                    .await
                    .map_err(|_| "progressive worker failed".to_string())?;
            }
            Ok(())
        });
        *closing = Some(Closing {
            fingerprint,
            task: Some(task),
            outcome: None,
        });
    }
    let closing = closing.as_mut().expect("terminal work was installed");
    if closing.outcome.is_none() {
        let task = closing
            .task
            .as_mut()
            .expect("unfinished terminal task is retained");
        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .map_err(|_| Error::failed("progressive cleanup is pending"))?
            .map_err(|_| Error::failed("progressive cleanup worker failed"))?;
        closing.outcome = Some(result);
        closing.task = None;
    }
    closing
        .outcome
        .clone()
        .expect("terminal result was retained")
        .map_err(|_| Error::failed("progressive cleanup failed"))
}

async fn abort_entry(entry: Arc<Entry>) -> Result<()> {
    let fingerprint: [u8; 32] = Sha256::digest(b"abort").into();
    let mut closing = entry.closing.lock().await;
    if closing.is_none() {
        let retained = entry.clone();
        let task = tokio::spawn(async move {
            let (reply, wait) = oneshot::channel();
            if retained.commands.send(Command::Abort(reply)).await.is_ok() {
                let _ = wait.await;
            }
            if let Some(worker) = retained.worker.lock().await.take() {
                worker
                    .await
                    .map_err(|_| "progressive worker failed".to_string())?;
            }
            Ok(())
        });
        *closing = Some(Closing {
            fingerprint,
            task: Some(task),
            outcome: None,
        });
    }
    let closing = closing.as_mut().expect("abort work was installed");
    if closing.outcome.is_none() {
        let task = closing
            .task
            .as_mut()
            .expect("unfinished abort task is retained");
        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .map_err(|_| Error::failed("progressive cleanup is pending"))?
            .map_err(|_| Error::failed("progressive cleanup worker failed"))?;
        closing.outcome = Some(result);
        closing.task = None;
    }
    closing
        .outcome
        .clone()
        .expect("abort result was retained")
        .map_err(|_| Error::failed("progressive cleanup failed"))
}

async fn run(
    host: HostChannels,
    channel: String,
    capabilities: tinychannels_bus::ChannelCapabilities,
    context: Option<ProgressiveOutputContext>,
    lifetime: Duration,
    terminal_feed: Arc<Mutex<Option<TerminalMemo>>>,
    mut inbox: mpsc::Receiver<Command>,
) {
    let mut reply = if let Some(context) = context {
        ProgressiveReply::new_scoped(
            Arc::new(Sender {
                host,
                context: Some(context.clone()),
                capabilities: capabilities.clone(),
                channel: channel.clone(),
                send_sequence: std::sync::atomic::AtomicU64::new(0),
            }),
            channel,
            capabilities,
            context,
        )
    } else {
        ProgressiveReply::new(
            Arc::new(Sender {
                host,
                context: None,
                capabilities: capabilities.clone(),
                channel: channel.clone(),
                send_sequence: std::sync::atomic::AtomicU64::new(0),
            }),
            channel,
            capabilities,
        )
    };
    let mut edits = tokio::time::interval(EDIT_FLUSH_INTERVAL);
    let mut typing = tokio::time::interval(TYPING_REFRESH_INTERVAL);
    let mut fillers = tokio::time::interval(FILLER_INTERVAL);
    for timer in [&mut edits, &mut typing, &mut fillers] {
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer.tick().await;
    }
    reply.typing_tick().await;
    let expiry = tokio::time::sleep(lifetime);
    tokio::pin!(expiry);
    loop {
        tokio::select! {
            command = inbox.recv() => match command {
                Some(Command::Feed(events, forward, fingerprint, response)) => {
                    let processing = async {
                        let cancellation = events
                            .iter()
                            .any(|event| matches!(event, ChannelOutputEvent::Cancellation { .. }));
                        let final_message = events.iter().find_map(|event| {
                            matches!(event, ChannelOutputEvent::FinalMessage { .. })
                                .then(|| event.clone())
                        });
                        let mut result = apply(&mut reply, events, forward).await;
                        let mut terminal = false;
                        if result.is_err() {
                            reply.abort().await;
                            terminal = true;
                        } else if cancellation {
                            reply.abort().await;
                            if matches!(result, Ok(ProgressiveResult::Updated)) {
                                result = Ok(ProgressiveResult::Closed);
                            }
                            terminal = true;
                        } else if delivery_has_failure(&result) {
                            reply.abort().await;
                            terminal = true;
                        } else if let Some(
                            ref final_event @ ChannelOutputEvent::FinalMessage { ref text, .. },
                        ) = final_message
                        {
                            let mut dispatch = match &result {
                                Ok(result) => ProgressiveOutputDispatch::from_result(result),
                                Err(_) => ProgressiveOutputDispatch::default(),
                            };
                            if forward {
                                match reply.finalize_output_event(final_event).await {
                                    Ok(final_dispatch) => dispatch.absorb(final_dispatch),
                                    Err(error) => {
                                        reply.abort().await;
                                        absorb_progressive_error(&mut dispatch, error);
                                    }
                                }
                                result = Ok(dispatch.into_result());
                            } else {
                                reply.finalize(text).await;
                                result = Ok(ProgressiveResult::Closed);
                            }
                            terminal = true;
                        }
                        (result, terminal)
                    };
                    tokio::select! {
                        (result, terminal) = processing => {
                            if terminal {
                                record_terminal(&terminal_feed, fingerprint, &result).await;
                                let _ = response.send(result);
                                break;
                            }
                            let _ = response.send(result);
                        }
                        _ = &mut expiry => {
                            reply.abort().await;
                            let result = Err(Error::failed("progressive reply lease expired"));
                            record_terminal(&terminal_feed, fingerprint, &result).await;
                            let _ = response.send(result);
                            break;
                        }
                    }
                }
                Some(Command::Finish(text, response)) => {
                    reply.finalize(&text).await;
                    let _ = response.send(());
                    break;
                }
                Some(Command::Abort(response)) => {
                    reply.abort().await;
                    let _ = response.send(());
                    break;
                }
                None => { reply.abort().await; break; }
            },
            _ = edits.tick() => reply.edit_tick().await,
            _ = typing.tick() => reply.typing_tick().await,
            _ = fillers.tick() => reply.filler_tick().await,
            _ = &mut expiry => { reply.abort().await; break; }
        }
    }
}

async fn record_terminal(
    terminal_feed: &Mutex<Option<TerminalMemo>>,
    fingerprint: Option<[u8; 32]>,
    result: &Result<ProgressiveResult>,
) {
    let Some(fingerprint) = fingerprint else {
        return;
    };
    let mut memo = terminal_feed.lock().await;
    if let Some(memo) = memo.as_mut().filter(|memo| memo.fingerprint == fingerprint) {
        let result = match result {
            Ok(value) => Ok(value.clone()),
            Err(error) => Err(error.to_string()),
        };
        memo.result = Some(result.clone());
        memo.notify.send_replace(Some(result));
    }
}

async fn apply(
    reply: &mut ProgressiveReply,
    events: Vec<ChannelOutputEvent>,
    forward: bool,
) -> Result<ProgressiveResult> {
    let text = events
        .iter()
        .filter_map(|event| match event {
            ChannelOutputEvent::TextDelta { text, .. } => Some(text.len()),
            _ => None,
        })
        .sum::<usize>();
    let thinking = events
        .iter()
        .filter_map(|event| match event {
            ChannelOutputEvent::ThinkingDelta { text, .. } => Some(text.len()),
            _ => None,
        })
        .sum::<usize>();
    if reply.state().content.len().saturating_add(text) > MAX_CONTENT
        || reply
            .state()
            .thinking_accumulator
            .len()
            .saturating_add(thinking)
            > MAX_CONTENT
    {
        return Err(Error::failed("progressive content exceeds its limit"));
    }
    let mut dispatch = ProgressiveOutputDispatch::default();
    for event in events {
        if forward && reply.on_output_event(&event).await.is_err() {
            dispatch.failure = Some(
                tinychannels_bus::module_progressive::ProgressiveOutputFailure::ObservationCallback,
            );
            break;
        }
        match reply.apply_output_event(&event, forward).await {
            Ok(dispatched) => dispatch.absorb(dispatched),
            Err(error) => {
                absorb_progressive_error(&mut dispatch, error);
                break;
            }
        }
        if dispatch.failure.is_some()
            || dispatch
                .outcomes
                .iter()
                .any(|outcome| outcome.error.is_some())
        {
            break;
        }
    }
    Ok(dispatch.into_result())
}

fn absorb_progressive_error(dispatch: &mut ProgressiveOutputDispatch, error: ProgressiveSendError) {
    use tinychannels_bus::module_progressive::ProgressiveOutputFailure;
    match error {
        ProgressiveSendError::PartialOutcome(mut outcome) => {
            outcome
                .error
                .get_or_insert_with(|| tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::Unknown,
                    message: "progressive provider returned a partial or uncertain outcome".into(),
                    sent_before_error: true,
                    ..Default::default()
                });
            dispatch.outcomes.push(*outcome);
            dispatch.failure = Some(ProgressiveOutputFailure::ProviderTransport);
        }
        ProgressiveSendError::UnsupportedContent => {}
        ProgressiveSendError::Unavailable => {
            dispatch.outcomes.push(progressive_error_outcome(
                "progressive provider transport unavailable",
                !dispatch.outcomes.is_empty(),
            ));
            dispatch.failure = Some(ProgressiveOutputFailure::ProviderTransport);
        }
        _error => {
            dispatch.outcomes.push(progressive_error_outcome(
                "progressive provider send failed with an unknown outcome",
                true,
            ));
            dispatch.failure = Some(ProgressiveOutputFailure::ProviderTransport);
        }
    }
}

fn progressive_error_outcome(
    message: &str,
    sent_before_error: bool,
) -> tinychannels_bus::channel::ChannelSendOutcome {
    tinychannels_bus::channel::ChannelSendOutcome {
        error: Some(tinychannels_bus::channel::ChannelSendError {
            kind: tinychannels_bus::channel::SendErrorKind::Unknown,
            message: message.to_owned(),
            sent_before_error,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn delivery_has_failure(result: &Result<ProgressiveResult>) -> bool {
    matches!(
        result,
        Ok(ProgressiveResult::DeliveryOutcome { outcome }) if outcome.error.is_some()
    ) || matches!(
        result,
        Ok(ProgressiveResult::DeliveryOutcomes { outcomes })
            if outcomes.iter().any(|outcome| outcome.error.is_some())
    ) || matches!(
        result,
        Ok(ProgressiveResult::DeliveryBatch { outcomes, .. })
            if outcomes.iter().any(|outcome| outcome.error.is_some())
    ) || matches!(
        result,
        Ok(ProgressiveResult::DeliveryBatch {
            failure: Some(_),
            ..
        })
    )
}

#[cfg(test)]
#[path = "progressive_tests.rs"]
mod tests;
