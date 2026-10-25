//! Typed outbound sends: text, Block Kit buttons, presentations, files and
//! voice (as audio uploads), returning receipts with Slack message timestamps.

use super::api::{pre_dispatch, send_error, unsupported};
use super::blocks::{button_blocks, presentation};
use super::text::{chunk_text, strip_tool_call_tags, to_mrkdwn};
use super::types::{
    SLACK_MAX_FILE_BYTES, SLACK_MAX_VOICE_BYTES, SLACK_TEXT_CHUNK_CHARS, SlackChannel,
};
use super::upload::Upload;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use tinychannels_bus::channel::{
    ButtonAction, ChannelSendError, ChannelSendOutcome, MessageReceipt, MessageReceiptPart,
    MessageReceiptPartKind, MessageReceiptSourceResult, SendErrorKind,
};
use tinychannels_bus::{
    ChannelOutboundMessage, ChannelRef, ContentPart, ConversationRef, HostVoiceScope, MediaKind,
    MediaReference, OutboundContentPart, OutboundVoice,
};

/// A plain-text typed message for the legacy `SendMessage` entry points.
#[cfg(test)]
pub(crate) fn text_message(
    recipient: &str,
    thread_ts: Option<&str>,
    content: &str,
) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "slack".into(),
        conversation_id: recipient.to_owned(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: thread_ts.map(str::to_owned),
        turn_id: None,
        idempotency_key: format!("slack-legacy-{}", uuid::Uuid::new_v4()),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Text {
                text: content.to_owned(),
                markdown_dialect: None,
            },
        }],
    }
}

/// Carry a typed failure through `anyhow`; callers recover it by downcast.
#[cfg(test)]
pub(crate) fn send_error_to_anyhow(error: ChannelSendError) -> anyhow::Error {
    anyhow::Error::new(super::types::SlackFailure(error))
}

/// One validated unit of work, in message order.
enum Step {
    Text(String),
    Buttons {
        text: String,
        rows: Vec<Vec<ButtonAction>>,
    },
    Media {
        kind: MediaKind,
        media: MediaReference,
        name: String,
        caption: Option<String>,
    },
    Voice {
        media: MediaReference,
        caption: Option<String>,
    },
}

/// Where a message goes.
pub(crate) struct Target {
    pub(crate) channel: ChannelRef,
    pub(crate) conversation: ConversationRef,
    pub(crate) thread_ts: Option<String>,
}

pub(crate) fn target_of(message: &ChannelOutboundMessage) -> Result<Target, ChannelSendError> {
    let mut conversation = message
        .conversation_ref
        .clone()
        .unwrap_or_else(|| ConversationRef {
            id: message.conversation_id.clone(),
            ..Default::default()
        });
    if conversation.thread_id.is_none() {
        conversation.thread_id = message.thread_id.clone();
    }
    if conversation.id.trim().is_empty() {
        return Err(pre_dispatch("Slack send requires a conversation identity"));
    }
    // A reply target is a message timestamp; replying to it opens or continues its thread.
    let thread_ts = conversation
        .thread_id
        .clone()
        .or_else(|| conversation.topic_id.clone())
        .or_else(|| message.reply_to_id.clone());
    let channel = message.channel_ref.clone().unwrap_or(ChannelRef {
        id: "slack".into(),
        account_id: None,
    });
    Ok(Target {
        channel,
        conversation,
        thread_ts,
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn receipt_part(
    id: &str,
    kind: MessageReceiptPartKind,
    index: usize,
    target: &Target,
) -> MessageReceiptPart {
    let message_ts = id.contains('.');
    MessageReceiptPart {
        platform_message_id: id.to_owned(),
        kind,
        index,
        thread_id: target.thread_ts.clone(),
        reply_to_id: None,
        // A bare file ID (no message timestamp) can be deleted but not edited.
        edit_token: message_ts.then(|| id.to_owned()),
        delete_token: Some(id.to_owned()),
        raw: Some(MessageReceiptSourceResult {
            channel: Some("slack".into()),
            message_id: Some(id.to_owned()),
            channel_id: Some(target.conversation.id.clone()),
            conversation_id: Some(target.conversation.id.clone()),
            ..Default::default()
        }),
    }
}

fn plan(
    message: &ChannelOutboundMessage,
    slack: &SlackChannel,
) -> Result<Vec<Step>, ChannelSendError> {
    if message.parts.is_empty() {
        return Err(pre_dispatch("Slack typed sends require at least one part"));
    }
    let texts = |parts: &[OutboundContentPart]| -> Option<Vec<String>> {
        parts
            .iter()
            .map(|part| match part {
                OutboundContentPart::Content {
                    part: ContentPart::Text { text, .. },
                } => Some(strip_tool_call_tags(text)),
                _ => None,
            })
            .collect()
    };
    let buttons = message.parts.iter().find_map(|part| match part {
        OutboundContentPart::Content {
            part: ContentPart::Buttons { rows },
        } => Some(rows.clone()),
        _ => None,
    });
    if let Some(rows) = buttons {
        let only_text: Vec<_> = message
            .parts
            .iter()
            .filter(|part| {
                !matches!(
                    part,
                    OutboundContentPart::Content {
                        part: ContentPart::Buttons { .. }
                    }
                )
            })
            .cloned()
            .collect();
        let Some(texts) = texts(&only_text) else {
            return Err(pre_dispatch("Slack buttons only combine with text"));
        };
        return Ok(vec![Step::Buttons {
            text: texts.join("\n"),
            rows,
        }]);
    }
    let voice_count = message
        .parts
        .iter()
        .filter(|part| matches!(part, OutboundContentPart::Voice { .. }))
        .count();
    let mut steps = Vec::new();
    for part in &message.parts {
        match part {
            OutboundContentPart::Content {
                part: ContentPart::Text { text, .. },
            } if voice_count == 0 => {
                let text = strip_tool_call_tags(text);
                if !text.trim().is_empty() {
                    steps.push(Step::Text(text));
                }
            }
            OutboundContentPart::Content {
                part: ContentPart::Text { .. },
            } => {}
            OutboundContentPart::Content {
                part: ContentPart::Presentation { blocks },
            } => {
                let rendered = presentation(blocks)?;
                if rendered.text.trim().is_empty() {
                    return Err(pre_dispatch(
                        "Slack presentation contains no renderable text",
                    ));
                }
                steps.push(if rendered.rows.is_empty() {
                    Step::Text(rendered.text)
                } else {
                    Step::Buttons {
                        text: rendered.text,
                        rows: rendered.rows,
                    }
                });
            }
            OutboundContentPart::Content { part: content } => {
                let (kind, media, name, caption) = match content {
                    ContentPart::Image { media, caption, .. } => {
                        (MediaKind::Image, media, "image", caption.clone())
                    }
                    ContentPart::Video { media, caption, .. } => {
                        (MediaKind::Video, media, "video", caption.clone())
                    }
                    ContentPart::Audio { media, title, .. } => {
                        (MediaKind::Audio, media, "audio", title.clone())
                    }
                    ContentPart::File {
                        media,
                        file_name,
                        caption,
                    } => (
                        MediaKind::Document,
                        media,
                        file_name.as_deref().unwrap_or("file"),
                        caption.clone(),
                    ),
                    _ => return Err(unsupported("Slack does not support this content kind")),
                };
                if slack.media_repository.is_none() {
                    return Err(unsupported("Slack media needs scoped host storage"));
                }
                steps.push(Step::Media {
                    kind,
                    media: media.clone(),
                    name: name.to_owned(),
                    caption,
                });
            }
            OutboundContentPart::Voice {
                voice: OutboundVoice::Media { media, .. },
            } => {
                if slack.voice_media_store.is_none() {
                    return Err(unsupported("Slack voice needs host voice storage"));
                }
                if voice_count > 1 {
                    return Err(pre_dispatch("Slack sends one voice clip per message"));
                }
                let caption = texts(
                    &message
                        .parts
                        .iter()
                        .filter(|p| !matches!(p, OutboundContentPart::Voice { .. }))
                        .cloned()
                        .collect::<Vec<_>>(),
                )
                .ok_or_else(|| pre_dispatch("Slack voice only combines with transcript text"))?;
                steps.push(Step::Voice {
                    media: media.clone(),
                    caption: Some(caption.join("\n")).filter(|c| !c.trim().is_empty()),
                });
            }
            OutboundContentPart::Voice { .. } => {
                return Err(if slack.voice_synthesis {
                    pre_dispatch("voice synthesis must be prepared by the shared module runtime")
                } else {
                    unsupported("Slack voice synthesis is not configured")
                });
            }
        }
    }
    if steps.is_empty() {
        return Err(pre_dispatch("Slack message has no sendable content"));
    }
    Ok(steps)
}

impl SlackChannel {
    async fn post_message(
        &self,
        target: &Target,
        mut body: Value,
    ) -> Result<String, ChannelSendError> {
        body["channel"] = target.conversation.id.clone().into();
        if let Some(thread) = &target.thread_ts {
            body["thread_ts"] = thread.clone().into();
        }
        let value = self
            .api_json("chat.postMessage", &body)
            .await
            .map_err(|failure| send_error(&failure, "send"))?;
        value
            .get("ts")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| send_error(&super::api::ApiFailure::Malformed, "send"))
    }

    fn voice_scope(
        message: &ChannelOutboundMessage,
        target: &Target,
    ) -> Result<HostVoiceScope, ChannelSendError> {
        let turn_id = message
            .turn_id
            .clone()
            .filter(|turn_id| !turn_id.trim().is_empty())
            .ok_or_else(|| pre_dispatch("Slack uploads require the originating turn identity"))?;
        Ok(HostVoiceScope {
            configured_instance: target
                .channel
                .account_id
                .clone()
                .unwrap_or_else(|| target.channel.id.clone()),
            channel: target.channel.clone(),
            conversation: target.conversation.clone(),
            turn_id,
        })
    }

    async fn run_step(
        &self,
        step: &Step,
        target: &Target,
        scope: &Result<HostVoiceScope, ChannelSendError>,
    ) -> Result<Vec<(String, MessageReceiptPartKind)>, ChannelSendError> {
        match step {
            Step::Text(text) => {
                let mut sent = Vec::new();
                for chunk in chunk_text(&to_mrkdwn(text), SLACK_TEXT_CHUNK_CHARS) {
                    let ts = self
                        .post_message(target, json!({"text": chunk, "mrkdwn": true}))
                        .await
                        .map_err(|mut e| {
                            if !sent.is_empty() {
                                e.sent_before_error = true;
                                e.retryable = false;
                            }
                            e
                        })?;
                    sent.push((ts, MessageReceiptPartKind::Text));
                }
                Ok(sent)
            }
            Step::Buttons { text, rows } => {
                let mrkdwn = to_mrkdwn(text);
                let blocks = button_blocks(&mrkdwn, rows)?;
                let ts = self
                    .post_message(target, json!({"text": mrkdwn, "blocks": blocks}))
                    .await?;
                Ok(vec![(ts, MessageReceiptPartKind::Card)])
            }
            Step::Media {
                kind,
                media,
                name,
                caption,
            } => {
                let scope = scope.clone()?;
                let repository = self
                    .media_repository
                    .as_ref()
                    .ok_or_else(|| unsupported("Slack media needs scoped host storage"))?;
                let opaque = media
                    .opaque_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| {
                        pre_dispatch("Slack media has no host-owned opaque reference")
                    })?;
                let stored = repository
                    .read_media(scope, opaque.to_owned(), SLACK_MAX_FILE_BYTES)
                    .await
                    .map_err(|_| pre_dispatch("Slack scoped media could not be read"))?;
                if stored.media.kind != *kind
                    || !crate::host::is_allowed_content_type(*kind, &stored.content_type)
                    || stored.bytes.is_empty()
                    || stored.bytes.len() as u64 > SLACK_MAX_FILE_BYTES
                {
                    return Err(pre_dispatch("Slack media failed upload validation"));
                }
                let file_name = stored.file_name.clone().unwrap_or_else(|| name.clone());
                let comment = caption
                    .as_deref()
                    .map(strip_tool_call_tags)
                    .map(|c| to_mrkdwn(&c));
                let uploaded = self
                    .upload_file(Upload {
                        channel: &target.conversation.id,
                        thread_ts: target.thread_ts.as_deref(),
                        bytes: stored.bytes,
                        file_name: &file_name,
                        content_type: &stored.content_type,
                        comment: comment.as_deref(),
                    })
                    .await?;
                Ok(vec![(
                    uploaded.message_ts.unwrap_or(uploaded.file_id),
                    MessageReceiptPartKind::Media,
                )])
            }
            Step::Voice { media, caption } => {
                let scope = scope.clone()?;
                let store = self
                    .voice_media_store
                    .as_ref()
                    .ok_or_else(|| unsupported("Slack voice needs host voice storage"))?;
                let opaque = media
                    .opaque_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| {
                        pre_dispatch("Slack voice media has no host-owned opaque reference")
                    })?;
                let resolved = store
                    .read_voice(scope, opaque.to_owned())
                    .await
                    .map_err(|_| pre_dispatch("Slack voice media could not be read"))?;
                let audio = BASE64
                    .decode(resolved.audio_base64.as_bytes())
                    .ok()
                    .filter(|audio| {
                        !audio.is_empty() && audio.len() as u64 <= SLACK_MAX_VOICE_BYTES
                    })
                    .ok_or_else(|| pre_dispatch("Slack voice media failed upload validation"))?;
                let file_name = resolved
                    .file_name
                    .clone()
                    .unwrap_or_else(|| "voice.m4a".into());
                let uploaded = self
                    .upload_file(Upload {
                        channel: &target.conversation.id,
                        thread_ts: target.thread_ts.as_deref(),
                        bytes: audio,
                        file_name: &file_name,
                        content_type: &resolved.mime_type,
                        comment: caption.as_deref(),
                    })
                    .await?;
                Ok(vec![(
                    uploaded.message_ts.unwrap_or(uploaded.file_id),
                    MessageReceiptPartKind::Voice,
                )])
            }
        }
    }

    /// Send canonical content and return the actual Slack message timestamps.
    pub(crate) async fn send_v2_with_receipt(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        let target = target_of(message)?;
        let steps = plan(message, self)?;
        let scope = Self::voice_scope(message, &target);
        let mut parts: Vec<MessageReceiptPart> = Vec::new();
        let mut failure = None;
        for step in &steps {
            match self.run_step(step, &target, &scope).await {
                Ok(sent) => {
                    for (id, kind) in sent {
                        let index = parts.len();
                        parts.push(receipt_part(&id, kind, index, &target));
                    }
                }
                Err(error) if parts.is_empty() => return Err(error),
                Err(_) => {
                    failure = Some(ChannelSendError {
                        kind: SendErrorKind::Unknown,
                        message: "Slack sent only part of the typed message; retry is unsafe"
                            .into(),
                        sent_before_error: true,
                        ..Default::default()
                    });
                    break;
                }
            }
        }
        let ids: Vec<String> = parts
            .iter()
            .map(|p| p.platform_message_id.clone())
            .collect();
        let single = (ids.len() == 1).then(|| ids[0].clone());
        tracing::debug!(
            parts = ids.len(),
            partial = failure.is_some(),
            "[slack] typed send complete"
        );
        let receipt = MessageReceipt {
            idempotency_key: Some(message.idempotency_key.clone()),
            channel: Some(target.channel.clone()),
            conversation: Some(target.conversation.clone()),
            primary_platform_message_id: ids.first().cloned(),
            platform_message_ids: ids,
            parts,
            thread_id: target.thread_ts.clone(),
            reply_to_id: message.reply_to_id.clone(),
            edit_token: single.clone().filter(|id| id.contains('.')),
            delete_token: single,
            sent_at: now_ms(),
            raw: None,
        };
        Ok(ChannelSendOutcome {
            receipt,
            error: failure,
        })
    }
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;
