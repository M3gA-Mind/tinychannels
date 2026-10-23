//! Discord typed send: text, attachments, embeds, buttons, voice messages
//! and thread creation, with a receipt that names every message created.

use super::channel::{DISCORD_MAX_MESSAGE_LENGTH, DiscordChannel, split_message_for_discord};
use super::http::{Payload, Upload, nonce, pre_dispatch, unsupported};
use super::inbound::{channel_ref, is_snowflake};
use super::voice::VoiceUpload;
use serde_json::{Value, json};
use tinychannels_bus::channel::{
    ChannelSendError, ChannelSendOutcome, MessageReceipt, MessageReceiptPart,
    MessageReceiptPartKind, MessageReceiptSourceResult, SendErrorKind,
};
use tinychannels_bus::{
    ButtonAction, ButtonStyle, ChannelOutboundMessage, ChannelRef, ContentPart, ConversationRef,
    MediaReference, OutboundContentPart, OutboundVoice,
};

/// Discord's default per-message upload budget for bots.
pub(super) const MAX_UPLOAD_BYTES: u64 = 10 * 1024 * 1024;
const MAX_FILES: usize = 10;
const MAX_EMBEDS: usize = 10;
const MAX_EMBED_DESCRIPTION: usize = 4096;
const MAX_ROWS: usize = 5;
const MAX_BUTTONS_PER_ROW: usize = 5;
const MAX_BUTTON_LABEL: usize = 80;
const MAX_CUSTOM_ID_BYTES: usize = 100;
const VOICE_MESSAGE_FLAG: u64 = 1 << 13;

/// Where a send lands and how the receipt must describe it.
#[derive(Debug)]
pub(super) struct Target {
    pub(super) channel: ChannelRef,
    pub(super) conversation: ConversationRef,
    /// Channel id the REST call posts to: the thread when one is addressed.
    pub(super) post_channel: String,
    pub(super) thread_id: Option<String>,
    pub(super) reply_to: Option<String>,
}

/// One Discord API message (or action) built from adjacent typed parts.
enum Batch {
    Message {
        text: String,
        uploads: Vec<Upload>,
        embeds: Vec<Value>,
        components: Vec<Value>,
        kind: MessageReceiptPartKind,
    },
    Voice(VoiceUpload),
    Thread(ThreadSpec),
}

#[derive(Debug)]
struct ThreadSpec {
    name: String,
    message_id: Option<String>,
    auto_archive_minutes: u32,
    content: Option<String>,
}

/// Parts accumulated into the next Discord message.
struct Acc {
    text: String,
    uploads: Vec<Upload>,
    embeds: Vec<Value>,
    components: Vec<Value>,
    kind: MessageReceiptPartKind,
}

impl Acc {
    fn new() -> Self {
        Self {
            text: String::new(),
            uploads: Vec::new(),
            embeds: Vec::new(),
            components: Vec::new(),
            kind: MessageReceiptPartKind::Text,
        }
    }

    /// Close the accumulated message, if it holds anything.
    fn take(&mut self, batches: &mut Vec<Batch>) {
        let acc = std::mem::replace(self, Self::new());
        if acc.text.is_empty()
            && acc.uploads.is_empty()
            && acc.embeds.is_empty()
            && acc.components.is_empty()
        {
            return;
        }
        batches.push(Batch::Message {
            text: acc.text,
            uploads: acc.uploads,
            embeds: acc.embeds,
            components: acc.components,
            kind: acc.kind,
        });
    }
}

pub(super) fn target_for(message: &ChannelOutboundMessage) -> Result<Target, ChannelSendError> {
    let mut conversation = message
        .conversation_ref
        .clone()
        .unwrap_or_else(|| ConversationRef {
            id: message.conversation_id.clone(),
            thread_id: message.thread_id.clone(),
            ..Default::default()
        });
    if conversation.thread_id.is_none() {
        conversation.thread_id.clone_from(&message.thread_id);
    }
    if !is_snowflake(&conversation.id) {
        return Err(pre_dispatch("Discord send requires a numeric channel id"));
    }
    let thread_id = conversation
        .thread_id
        .clone()
        .or_else(|| conversation.topic_id.clone());
    if thread_id.as_deref().is_some_and(|id| !is_snowflake(id)) {
        return Err(pre_dispatch("Discord thread id must be numeric"));
    }
    if message
        .reply_to_id
        .as_deref()
        .is_some_and(|id| !is_snowflake(id))
    {
        return Err(pre_dispatch("Discord reply id must be numeric"));
    }
    Ok(Target {
        channel: message.channel_ref.clone().unwrap_or_else(channel_ref),
        post_channel: thread_id.clone().unwrap_or_else(|| conversation.id.clone()),
        conversation,
        thread_id,
        reply_to: message.reply_to_id.clone(),
    })
}

fn sanitize_file_name(name: Option<&str>, fallback: &str) -> String {
    let base = name
        .and_then(|name| name.rsplit(['/', '\\']).next())
        .map(str::trim)
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .unwrap_or(fallback);
    base.chars().filter(|c| !c.is_control()).take(100).collect()
}

fn button_style(style: Option<ButtonStyle>) -> u8 {
    match style {
        Some(ButtonStyle::Primary) => 1,
        Some(ButtonStyle::Danger) => 4,
        Some(ButtonStyle::Secondary) | None => 2,
    }
}

/// Convert neutral button rows into Discord action rows.
pub(super) fn button_rows(rows: &[Vec<ButtonAction>]) -> Result<Vec<Value>, ChannelSendError> {
    if rows.is_empty() || rows.len() > MAX_ROWS {
        return Err(pre_dispatch("Discord allows 1 to 5 button rows"));
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if row.is_empty() || row.len() > MAX_BUTTONS_PER_ROW {
            return Err(pre_dispatch("Discord allows 1 to 5 buttons per row"));
        }
        let mut components = Vec::with_capacity(row.len());
        for button in row {
            if button.id.is_empty() || button.id.len() > MAX_CUSTOM_ID_BYTES {
                return Err(pre_dispatch("Discord button id must be 1 to 100 bytes"));
            }
            if button.label.trim().is_empty() || button.label.chars().count() > MAX_BUTTON_LABEL {
                return Err(pre_dispatch(
                    "Discord button label must be 1 to 80 characters",
                ));
            }
            components.push(json!({
                "type": 2,
                "style": button_style(button.style),
                "label": button.label,
                "custom_id": button.id,
                "disabled": button.disabled,
            }));
        }
        out.push(json!({"type": 1, "components": components}));
    }
    Ok(out)
}

/// Presentation blocks become one embed; an approval also carries its
/// choice buttons.
pub(super) fn presentation_embed(blocks: &Value) -> Result<(Value, Vec<Value>), ChannelSendError> {
    if let Some(object) = blocks
        .as_object()
        .filter(|o| o.get("kind").and_then(Value::as_str) == Some("approval"))
    {
        if object.len() != 4
            || object
                .keys()
                .any(|key| !["kind", "id", "prompt", "choices"].contains(&key.as_str()))
        {
            return Err(pre_dispatch(
                "Discord approval presentation has unsupported fields",
            ));
        }
        let request_id = object
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| pre_dispatch("Discord approval request id is invalid"))?;
        let prompt = object
            .get("prompt")
            .and_then(Value::as_str)
            .filter(|p| !p.trim().is_empty())
            .ok_or_else(|| pre_dispatch("Discord approval prompt is invalid"))?;
        let choices = object
            .get("choices")
            .and_then(Value::as_array)
            .filter(|c| !c.is_empty() && c.len() <= MAX_ROWS * MAX_BUTTONS_PER_ROW)
            .ok_or_else(|| pre_dispatch("Discord approval choices are invalid"))?;
        let mut buttons = Vec::with_capacity(choices.len());
        for (index, choice) in choices.iter().enumerate() {
            let label = choice
                .as_str()
                .ok_or_else(|| pre_dispatch("Discord approval choice label is invalid"))?;
            let id = tinychannels_bus::channel::approval_action_id(request_id, index)
                .ok_or_else(|| pre_dispatch("Discord approval action id is too long"))?;
            buttons.push(ButtonAction {
                id,
                label: label.to_owned(),
                style: None,
                disabled: false,
            });
        }
        let rows: Vec<Vec<ButtonAction>> = buttons
            .chunks(MAX_BUTTONS_PER_ROW)
            .map(<[ButtonAction]>::to_vec)
            .collect();
        return Ok((
            json!({"description": clip(prompt, MAX_EMBED_DESCRIPTION)}),
            button_rows(&rows)?,
        ));
    }
    let mut texts = Vec::new();
    collect_block_text(blocks, &mut texts)?;
    if texts.is_empty() {
        return Err(pre_dispatch("Discord presentation has no text"));
    }
    Ok((
        json!({"description": clip(&texts.join("\n"), MAX_EMBED_DESCRIPTION)}),
        Vec::new(),
    ))
}

fn clip(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

fn collect_block_text(value: &Value, out: &mut Vec<String>) -> Result<(), ChannelSendError> {
    match value {
        Value::String(text) if !text.trim().is_empty() => out.push(text.clone()),
        Value::Array(items) => {
            for item in items {
                collect_block_text(item, out)?;
            }
        }
        Value::Object(object) => {
            const ALLOWED: &[&str] =
                &["type", "text", "title", "heading", "caption", "description"];
            if object.keys().any(|key| !ALLOWED.contains(&key.as_str())) {
                return Err(pre_dispatch(
                    "Discord presentation block has unsupported fields",
                ));
            }
            for key in ["title", "heading", "text", "caption", "description"] {
                if let Some(value) = object.get(key) {
                    let text = value.as_str().ok_or_else(|| {
                        pre_dispatch("Discord presentation text must be a string")
                    })?;
                    if !text.trim().is_empty() {
                        out.push(text.to_owned());
                    }
                }
            }
        }
        _ => {
            return Err(pre_dispatch(
                "Discord presentation only accepts text blocks",
            ));
        }
    }
    Ok(())
}

fn thread_spec(data: &Value) -> Result<ThreadSpec, ChannelSendError> {
    if data.get("action").and_then(Value::as_str) != Some("create_thread") {
        return Err(unsupported(
            "Discord native data only supports create_thread",
        ));
    }
    let name = data
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty() && name.chars().count() <= 100)
        .ok_or_else(|| pre_dispatch("Discord thread name must be 1 to 100 characters"))?;
    let message_id = match data.get("messageId").and_then(Value::as_str) {
        Some(id) if is_snowflake(id) => Some(id.to_owned()),
        Some(_) => return Err(pre_dispatch("Discord thread message id must be numeric")),
        None => None,
    };
    let auto_archive_minutes = match data.get("autoArchiveDuration").and_then(Value::as_u64) {
        None => 1440,
        Some(minutes @ (60 | 1440 | 4320 | 10080)) => minutes as u32,
        Some(_) => return Err(pre_dispatch("Discord thread archive duration is invalid")),
    };
    let content = data
        .get("content")
        .and_then(Value::as_str)
        .filter(|c| !c.trim().is_empty())
        .map(str::to_owned);
    if content
        .as_deref()
        .is_some_and(|c| c.encode_utf16().count() > DISCORD_MAX_MESSAGE_LENGTH)
    {
        return Err(pre_dispatch(
            "Discord thread opening message exceeds 2000 characters",
        ));
    }
    Ok(ThreadSpec {
        name: name.to_owned(),
        message_id,
        auto_archive_minutes,
        content,
    })
}

impl DiscordChannel {
    /// Resolve a host media reference to bytes for one native upload.
    async fn resolve_upload(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
        media: &MediaReference,
        fallback_name: &str,
    ) -> Result<Upload, ChannelSendError> {
        let repository = self
            .media_repository
            .as_ref()
            .ok_or_else(|| unsupported("Discord attachments require scoped host media storage"))?;
        let opaque_id = media
            .opaque_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| pre_dispatch("Discord attachments require a host media reference"))?;
        let scope = self.media_scope(message, target)?;
        let stored = repository
            .read_media(scope, opaque_id.to_owned(), MAX_UPLOAD_BYTES)
            .await
            .map_err(|error| {
                tracing::debug!(%error, "[discord] media reference could not be resolved");
                pre_dispatch("Discord media reference could not be resolved")
            })?;
        Ok(Upload {
            file_name: sanitize_file_name(
                media.file_name.as_deref().or(stored.file_name.as_deref()),
                fallback_name,
            ),
            content_type: stored.content_type,
            bytes: stored.bytes,
        })
    }

    /// Host scope (account, conversation, originating turn) for media I/O.
    pub(super) fn media_scope(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
    ) -> Result<tinychannels_bus::HostVoiceScope, ChannelSendError> {
        let turn_id = message
            .turn_id
            .clone()
            .filter(|turn| !turn.trim().is_empty())
            .ok_or_else(|| pre_dispatch("Discord media send requires the originating turn"))?;
        Ok(tinychannels_bus::HostVoiceScope {
            configured_instance: target
                .channel
                .account_id
                .clone()
                .filter(|id| !id.trim().is_empty())
                .unwrap_or_else(|| target.channel.id.clone()),
            channel: target.channel.clone(),
            conversation: target.conversation.clone(),
            turn_id,
        })
    }

    /// Validate every part and build the ordered Discord messages.
    async fn plan_batches(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
    ) -> Result<Vec<Batch>, ChannelSendError> {
        let mut batches = Vec::new();
        let mut acc = Acc::new();
        for part in &message.parts {
            match part {
                OutboundContentPart::Content { part } => match part {
                    ContentPart::Text { text: value, .. } => {
                        push_caption(&mut acc.text, Some(value));
                    }
                    ContentPart::Image { media, caption, .. } => {
                        acc.uploads
                            .push(self.resolve_upload(message, target, media, "image").await?);
                        push_caption(&mut acc.text, caption.as_deref());
                        acc.kind = MessageReceiptPartKind::Media;
                    }
                    ContentPart::Video { media, caption, .. } => {
                        acc.uploads
                            .push(self.resolve_upload(message, target, media, "video").await?);
                        push_caption(&mut acc.text, caption.as_deref());
                        acc.kind = MessageReceiptPartKind::Media;
                    }
                    ContentPart::Audio {
                        media,
                        title,
                        transcript,
                        ..
                    } => {
                        acc.uploads
                            .push(self.resolve_upload(message, target, media, "audio").await?);
                        push_caption(&mut acc.text, title.as_deref());
                        push_caption(&mut acc.text, transcript.as_deref());
                        acc.kind = MessageReceiptPartKind::Media;
                    }
                    ContentPart::File {
                        media,
                        file_name,
                        caption,
                    } => {
                        let mut upload =
                            self.resolve_upload(message, target, media, "file").await?;
                        if let Some(name) = file_name {
                            upload.file_name = sanitize_file_name(Some(name), &upload.file_name);
                        }
                        acc.uploads.push(upload);
                        push_caption(&mut acc.text, caption.as_deref());
                        acc.kind = MessageReceiptPartKind::Media;
                    }
                    ContentPart::Buttons { rows } => {
                        acc.components.extend(button_rows(rows)?);
                        if acc.components.len() > MAX_ROWS {
                            return Err(pre_dispatch("Discord allows at most 5 button rows"));
                        }
                    }
                    ContentPart::Presentation { blocks } => {
                        let (embed, rows) = presentation_embed(blocks)?;
                        acc.embeds.push(embed);
                        acc.components.extend(rows);
                        if acc.components.len() > MAX_ROWS {
                            return Err(pre_dispatch("Discord allows at most 5 button rows"));
                        }
                        acc.kind = MessageReceiptPartKind::Card;
                    }
                    ContentPart::Native { provider, data }
                        if tinychannels_bus::provider_id(provider) == "discord" =>
                    {
                        if message.parts.len() != 1 {
                            return Err(pre_dispatch(
                                "Discord native data must be the only part of a send",
                            ));
                        }
                        batches.push(Batch::Thread(thread_spec(data)?));
                    }
                    ContentPart::Native { .. } => {
                        return Err(unsupported(
                            "Discord ignores another provider's native data",
                        ));
                    }
                    ContentPart::Sticker { .. }
                    | ContentPart::Location { .. }
                    | ContentPart::Contact { .. }
                    | ContentPart::Poll { .. }
                    | ContentPart::VoiceNote { .. } => {
                        return Err(unsupported(
                            "Discord does not support this content kind through the bot API",
                        ));
                    }
                },
                OutboundContentPart::Voice { voice } => {
                    acc.take(&mut batches);
                    let OutboundVoice::Media {
                        media,
                        codec,
                        duration_ms,
                    } = voice
                    else {
                        return Err(pre_dispatch(
                            "voice synthesis must be prepared by the shared module runtime",
                        ));
                    };
                    batches.push(Batch::Voice(
                        self.prepare_voice_with_waveform(
                            message,
                            target,
                            media,
                            None,
                            codec.as_deref(),
                            *duration_ms,
                            None,
                        )
                        .await?,
                    ));
                }
            }
        }
        acc.take(&mut batches);
        if batches.is_empty() {
            return Err(pre_dispatch("Discord send has no content"));
        }
        for batch in &batches {
            if let Batch::Message {
                uploads, embeds, ..
            } = batch
            {
                if uploads.len() > MAX_FILES || embeds.len() > MAX_EMBEDS {
                    return Err(pre_dispatch(
                        "Discord allows 10 files and 10 embeds per message",
                    ));
                }
                let total: usize = uploads.iter().map(|u| u.bytes.len()).sum();
                if total as u64 > MAX_UPLOAD_BYTES {
                    return Err(pre_dispatch("Discord attachments exceed the upload limit"));
                }
            }
        }
        Ok(batches)
    }

    /// Send canonical typed content and return the receipt Discord supplied.
    pub(crate) async fn send_v2_with_receipt(
        &self,
        message: &ChannelOutboundMessage,
    ) -> Result<ChannelSendOutcome, ChannelSendError> {
        let target = target_for(message)?;
        let batches = self.plan_batches(message, &target).await?;
        tracing::debug!(
            channel_id = %target.post_channel,
            batches = batches.len(),
            "[discord] typed send planned"
        );
        let mut sent: Vec<(String, MessageReceiptPartKind, Option<String>)> = Vec::new();
        let mut next_nonce = 0usize;
        let mut failure = None;
        for batch in batches {
            let result = match batch {
                Batch::Message {
                    text,
                    uploads,
                    embeds,
                    components,
                    kind,
                } => {
                    self.send_message_batch(
                        message,
                        &target,
                        text,
                        uploads,
                        embeds,
                        components,
                        kind,
                        &mut next_nonce,
                        &mut sent,
                    )
                    .await
                }
                Batch::Voice(voice) => {
                    self.send_voice_batch(message, &target, voice, &mut next_nonce, &mut sent)
                        .await
                }
                Batch::Thread(spec) => {
                    self.send_thread_batch(message, &target, spec, &mut next_nonce, &mut sent)
                        .await
                }
            };
            if let Err(error) = result {
                failure = Some(error);
                break;
            }
        }
        match failure {
            None => Ok(ChannelSendOutcome {
                receipt: build_receipt(message, &target, &sent),
                error: None,
            }),
            Some(error) if sent.is_empty() => Err(error),
            Some(mut error) => {
                // Part of the logical send is already visible: report it and
                // forbid a blind retry that would duplicate it.
                error.sent_before_error = true;
                error.retryable = false;
                error.continuation_message_ids = sent.iter().map(|(id, _, _)| id.clone()).collect();
                Ok(ChannelSendOutcome {
                    receipt: build_receipt(message, &target, &sent),
                    error: Some(error),
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_message_batch(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
        text: String,
        uploads: Vec<Upload>,
        embeds: Vec<Value>,
        components: Vec<Value>,
        kind: MessageReceiptPartKind,
        next_nonce: &mut usize,
        sent: &mut Vec<(String, MessageReceiptPartKind, Option<String>)>,
    ) -> Result<(), ChannelSendError> {
        let mut chunks = if text.is_empty() {
            Vec::new()
        } else {
            split_message_for_discord(&text)
        };
        // Everything but the final chunk is plain text; the final message
        // carries the files, embeds and buttons so they appear together.
        let last = chunks.pop();
        let url = self.endpoint_url(&["channels", &target.post_channel, "messages"])?;
        for chunk in chunks {
            let body =
                self.message_body(message, target, Some(chunk), sent.is_empty(), *next_nonce);
            *next_nonce += 1;
            let response = self
                .call(
                    reqwest::Method::POST,
                    url.clone(),
                    Payload::Json(body),
                    "send message",
                    nonce_enabled(message),
                )
                .await?;
            sent.push((response_id(&response)?, MessageReceiptPartKind::Text, None));
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        let mut body = self.message_body(message, target, last, sent.is_empty(), *next_nonce);
        *next_nonce += 1;
        if !embeds.is_empty() {
            body["embeds"] = Value::Array(embeds);
        }
        if !components.is_empty() {
            body["components"] = Value::Array(components);
        }
        let payload = if uploads.is_empty() {
            Payload::Json(body)
        } else {
            body["attachments"] = Value::Array(
                uploads
                    .iter()
                    .enumerate()
                    .map(|(index, u)| json!({"id": index, "filename": u.file_name}))
                    .collect(),
            );
            Payload::Multipart {
                payload: body,
                uploads,
            }
        };
        let response = self
            .call(
                reqwest::Method::POST,
                url,
                payload,
                "send message",
                nonce_enabled(message),
            )
            .await?;
        sent.push((response_id(&response)?, kind, None));
        Ok(())
    }

    fn message_body(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
        content: Option<String>,
        first: bool,
        nonce_index: usize,
    ) -> Value {
        let mut body = json!({"allowed_mentions": {"parse": ["users"]}});
        if let Some(content) = content {
            body["content"] = Value::String(content);
        }
        if first && let Some(reply) = target.reply_to.as_deref() {
            body["message_reference"] = json!({"message_id": reply, "fail_if_not_exists": false});
        }
        if let Some(nonce) = nonce(&message.idempotency_key, nonce_index) {
            body["nonce"] = Value::String(nonce);
            body["enforce_nonce"] = Value::Bool(true);
        }
        body
    }

    async fn send_voice_batch(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
        voice: VoiceUpload,
        next_nonce: &mut usize,
        sent: &mut Vec<(String, MessageReceiptPartKind, Option<String>)>,
    ) -> Result<(), ChannelSendError> {
        let mut body = self.message_body(message, target, None, sent.is_empty(), *next_nonce);
        *next_nonce += 1;
        body["flags"] = json!(VOICE_MESSAGE_FLAG);
        body["attachments"] = json!([{
            "id": 0,
            "filename": "voice-message.ogg",
            "duration_secs": voice.duration_secs,
            "waveform": voice.waveform_base64,
        }]);
        let url = self.endpoint_url(&["channels", &target.post_channel, "messages"])?;
        let response = self
            .call(
                reqwest::Method::POST,
                url,
                Payload::Multipart {
                    payload: body,
                    uploads: vec![Upload {
                        file_name: "voice-message.ogg".into(),
                        content_type: "audio/ogg".into(),
                        bytes: voice.bytes,
                    }],
                },
                "send voice message",
                nonce_enabled(message),
            )
            .await?;
        sent.push((response_id(&response)?, MessageReceiptPartKind::Voice, None));
        Ok(())
    }

    async fn send_thread_batch(
        &self,
        message: &ChannelOutboundMessage,
        target: &Target,
        spec: ThreadSpec,
        next_nonce: &mut usize,
        sent: &mut Vec<(String, MessageReceiptPartKind, Option<String>)>,
    ) -> Result<(), ChannelSendError> {
        let (url, body) = match spec.message_id.as_deref() {
            Some(message_id) => (
                self.endpoint_url(&[
                    "channels",
                    &target.post_channel,
                    "messages",
                    message_id,
                    "threads",
                ])?,
                json!({"name": spec.name, "auto_archive_duration": spec.auto_archive_minutes}),
            ),
            None => (
                self.endpoint_url(&["channels", &target.post_channel, "threads"])?,
                json!({"name": spec.name, "auto_archive_duration": spec.auto_archive_minutes, "type": 11}),
            ),
        };
        let thread = self
            .call(
                reqwest::Method::POST,
                url,
                Payload::Json(body),
                "create thread",
                false,
            )
            .await?;
        let thread_id = response_id(&thread)?;
        sent.push((
            thread_id.clone(),
            MessageReceiptPartKind::Card,
            Some(thread_id.clone()),
        ));
        if let Some(content) = spec.content {
            let body = self.message_body(message, target, Some(content), false, *next_nonce);
            *next_nonce += 1;
            let url = self.endpoint_url(&["channels", &thread_id, "messages"])?;
            let response = self
                .call(
                    reqwest::Method::POST,
                    url,
                    Payload::Json(body),
                    "send message",
                    nonce_enabled(message),
                )
                .await?;
            sent.push((
                response_id(&response)?,
                MessageReceiptPartKind::Text,
                Some(thread_id),
            ));
        }
        Ok(())
    }
}

fn push_caption(text: &mut String, caption: Option<&str>) {
    if let Some(caption) = caption.filter(|c| !c.trim().is_empty()) {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(caption);
    }
}

fn nonce_enabled(message: &ChannelOutboundMessage) -> bool {
    !message.idempotency_key.is_empty()
}

fn response_id(response: &Value) -> Result<String, ChannelSendError> {
    response
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| is_snowflake(id))
        .map(str::to_owned)
        .ok_or_else(|| ChannelSendError {
            kind: SendErrorKind::Unknown,
            message: "Discord response omitted the message id".into(),
            retryable: false,
            sent_before_error: true,
            ..Default::default()
        })
}

pub(super) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn build_receipt(
    message: &ChannelOutboundMessage,
    target: &Target,
    sent: &[(String, MessageReceiptPartKind, Option<String>)],
) -> MessageReceipt {
    let mut thread_id = target.thread_id.clone();
    let parts: Vec<MessageReceiptPart> = sent
        .iter()
        .enumerate()
        .map(|(index, (id, kind, part_thread))| {
            let part_thread = part_thread.clone().or_else(|| target.thread_id.clone());
            if thread_id.is_none() {
                thread_id.clone_from(&part_thread);
            }
            MessageReceiptPart {
                platform_message_id: id.clone(),
                kind: *kind,
                index,
                thread_id: part_thread.clone(),
                reply_to_id: target.reply_to.clone(),
                edit_token: Some(id.clone()),
                delete_token: Some(id.clone()),
                raw: Some(MessageReceiptSourceResult {
                    channel: Some("discord".into()),
                    message_id: Some(id.clone()),
                    channel_id: Some(part_thread.unwrap_or_else(|| target.conversation.id.clone())),
                    ..Default::default()
                }),
            }
        })
        .collect();
    let single = (parts.len() == 1).then(|| parts[0].platform_message_id.clone());
    MessageReceipt {
        idempotency_key: Some(message.idempotency_key.clone()),
        channel: Some(target.channel.clone()),
        conversation: Some(target.conversation.clone()),
        primary_platform_message_id: parts.first().map(|p| p.platform_message_id.clone()),
        platform_message_ids: parts
            .iter()
            .map(|p| p.platform_message_id.clone())
            .collect(),
        parts,
        thread_id,
        reply_to_id: target.reply_to.clone(),
        edit_token: single.clone(),
        delete_token: single,
        sent_at: now_unix_ms(),
        raw: None,
    }
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;
