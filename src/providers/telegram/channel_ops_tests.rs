use super::*;
use crate::providers::telegram::fixtures::*;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_runtime::channel::ChannelAdapter;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct TelegramConformanceSink(Arc<TelegramChannel>);

#[async_trait::async_trait]
impl tinychannels_runtime::adapters::LocalOutboundSink for TelegramConformanceSink {
    async fn send(
        &self,
        intent: tinychannels_bus::ChannelOutboundIntent,
    ) -> Result<
        tinychannels_bus::channel::MessageReceipt,
        tinychannels_bus::channel::ChannelSendError,
    > {
        let outcome = crate::channel::ChannelAdapter::send(self.0.as_ref(), intent).await?;
        if let Some(error) = outcome.error {
            return Err(error);
        }
        Ok(outcome.receipt)
    }
}

struct TelegramWireFixture {
    adapter: tinychannels_runtime::adapters::LocalChannelAdapter,
    channel: Arc<TelegramChannel>,
    server: MockServer,
    observations: std::sync::Mutex<Vec<tinychannels_conformance::WireObservation>>,
    request_cursor: std::sync::Mutex<usize>,
    scope: tinychannels_conformance::ExpectedScope,
    events: Arc<TelegramConformanceEvents>,
    voice: Arc<TelegramConformanceVoice>,
    transcriber: Arc<TelegramConformanceTranscriber>,
}

#[derive(Default)]
struct TelegramConformanceEvents {
    events: std::sync::Mutex<Vec<tinychannels_bus::ChannelEvent>>,
    inbounds: std::sync::Mutex<Vec<tinychannels_bus::ChannelInboundEnvelope>>,
    notified: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl crate::host::EventSink for TelegramConformanceEvents {
    async fn publish(
        &self,
        _domain: &str,
        _kind: &str,
        _payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish_channel_event(
        &self,
        event: tinychannels_bus::ChannelEvent,
    ) -> anyhow::Result<()> {
        self.events.lock().unwrap().push(event);
        self.notified.notify_one();
        Ok(())
    }

    async fn publish_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> anyhow::Result<bool> {
        match ingress {
            tinychannels_bus::ChannelIngress::Event(event) => {
                self.events.lock().unwrap().push(event);
            }
            tinychannels_bus::ChannelIngress::Inbound(envelope) => {
                self.inbounds.lock().unwrap().push(envelope);
            }
        }
        self.notified.notify_one();
        Ok(true)
    }
}

fn rich_part(part: tinychannels_bus::ContentPart) -> tinychannels_bus::OutboundPayload {
    tinychannels_bus::OutboundPayload::Parts {
        parts: vec![tinychannels_bus::OutboundContentPart::Content { part }],
    }
}

fn conformance_media(
    kind: tinychannels_bus::MediaKind,
    name: &str,
) -> tinychannels_bus::MediaReference {
    tinychannels_bus::MediaReference {
        opaque_id: Some(format!("telegram-{name}")),
        kind,
        ..Default::default()
    }
}

struct TelegramConformanceMedia;

#[async_trait::async_trait]
impl crate::host::MediaRepository for TelegramConformanceMedia {
    async fn store_media(
        &self,
        request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        Ok(tinychannels_bus::MediaReference {
            opaque_id: Some("telegram-ingress-media".into()),
            kind: request.metadata.kind,
            content_type: Some(request.metadata.content_type),
            file_name: request.metadata.file_name,
            size_bytes: Some(request.bytes.len() as u64),
            ..Default::default()
        })
    }

    async fn read_media(
        &self,
        scope: crate::host::MediaScope,
        opaque_id: String,
        _max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        assert_eq!(scope.configured_instance, "telegram-conformance");
        assert_eq!(scope.channel.id, "telegram");
        assert_eq!(
            scope.channel.account_id.as_deref(),
            Some("telegram-conformance")
        );
        assert_eq!(scope.conversation.id, "100");
        assert_eq!(scope.turn_id, "conformance-turn");
        let (kind, content_type, file_name, bytes) = match opaque_id.as_str() {
            "telegram-Image" => (tinychannels_bus::MediaKind::Image, "image/png", "image.png", BASE64.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")?),
            "telegram-Video" => (tinychannels_bus::MediaKind::Video, "video/mp4", "video.mp4", b"video fixture".to_vec()),
            "telegram-Audio" => (tinychannels_bus::MediaKind::Audio, "audio/mpeg", "audio.mp3", valid_mp3_frame()),
            "telegram-Document" => (tinychannels_bus::MediaKind::Document, "application/pdf", "document.pdf", b"%PDF conformance".to_vec()),
            "telegram-Sticker" => (tinychannels_bus::MediaKind::Image, "image/webp", "sticker.webp", include_bytes!("fixtures/sticker-512.webp").to_vec()),
            _ => anyhow::bail!("unexpected Telegram conformance media id"),
        };
        Ok(crate::host::StoredMedia {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some(opaque_id),
                kind,
                ..Default::default()
            },
            content_type: content_type.into(),
            file_name: Some(file_name.into()),
            bytes,
        })
    }
}

#[derive(Default)]
struct TelegramConformanceVoice {
    stored: std::sync::Mutex<Vec<crate::host::VoiceMediaRequest>>,
}

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for TelegramConformanceVoice {
    async fn store_voice(
        &self,
        request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        let opaque_id = match request.direction {
            tinychannels_bus::HostVoiceMediaDirection::Inbound => "telegram-inbound-voice",
            tinychannels_bus::HostVoiceMediaDirection::Outbound => "telegram-synthesized-voice",
        };
        self.stored.lock().unwrap().push(request.clone());
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some(opaque_id.into()),
                kind: tinychannels_bus::MediaKind::Audio,
                content_type: Some(request.mime_type),
                file_name: request.file_name,
                size_bytes: Some(request.audio_base64.len() as u64),
                duration_ms: request.duration_ms,
                ..Default::default()
            },
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        if opaque_id == "telegram-synthesized-voice"
            && let Some(request) = self.stored.lock().unwrap().iter().rev().find(|request| {
                request.direction == tinychannels_bus::HostVoiceMediaDirection::Outbound
            })
        {
            return Ok(tinychannels_bus::HostReadVoiceMediaResult {
                audio_base64: request.audio_base64.clone(),
                mime_type: request.mime_type.clone(),
                file_name: request.file_name.clone(),
            });
        }
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: BASE64.encode(b"telegram fixture voice bytes"),
            mime_type: "audio/ogg".into(),
            file_name: Some("voice.ogg".into()),
        })
    }
}

struct TelegramConformanceTranscriber;

#[async_trait::async_trait]
impl crate::host::Transcriber for TelegramConformanceTranscriber {
    fn name(&self) -> &str {
        "telegram-conformance"
    }

    async fn transcribe(
        &self,
        request: crate::host::TranscriptionRequest,
    ) -> anyhow::Result<crate::host::TranscriptionResult> {
        anyhow::ensure!(
            !request.audio_base64.is_empty(),
            "voice bytes must reach STT"
        );
        Ok(crate::host::TranscriptionResult {
            text: "telegram conformance transcript".into(),
            language: Some("en".into()),
            duration_secs: Some(2.0),
        })
    }
}

struct TelegramConformanceSynthesizer;

#[async_trait::async_trait]
impl crate::host::SpeechSynthesizer for TelegramConformanceSynthesizer {
    async fn synthesize(
        &self,
        request: crate::host::SpeechRequest,
    ) -> anyhow::Result<crate::host::SpeechResult> {
        anyhow::ensure!(!request.text.is_empty(), "synthesis text must be present");
        Ok(crate::host::SpeechResult {
            audio_base64: BASE64.encode(b"synthesized telegram voice"),
            mime_type: "audio/wav".into(),
            duration_ms: Some(1_000),
            visemes: None,
        })
    }
}

struct TelegramConformanceCodec;

#[async_trait::async_trait]
impl crate::host::VoiceCodec for TelegramConformanceCodec {
    async fn transcode(
        &self,
        request: crate::host::VoiceCodecRequest,
    ) -> anyhow::Result<crate::host::VoiceCodecResult> {
        anyhow::ensure!(request.from == "wav_pcm16" && request.target == "ogg_opus");
        Ok(crate::host::VoiceCodecResult {
            audio_base64: request.audio_base64,
            duration_ms: Some(1_000),
            waveform: None,
        })
    }
}

fn unsupported_fixture_operation() -> tinychannels_bus::channel::ChannelSendError {
    tinychannels_bus::channel::ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
        message: "fixture operation is unsupported".into(),
        unsupported: true,
        ..Default::default()
    }
}

fn fixture_operation_failed(
    message: impl Into<String>,
) -> tinychannels_bus::channel::ChannelSendError {
    tinychannels_bus::channel::ChannelSendError {
        kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
        message: message.into(),
        ..Default::default()
    }
}

fn telegram_voice_update() -> serde_json::Value {
    serde_json::json!({
        "update_id": 902,
        "message": {
            "message_id": 322,
            "chat": {"id": 100, "type": "private"},
            "from": {"id": 9, "username": "conformance", "first_name": "Conformance"},
            "voice": {
                "file_id": "voice-file",
                "file_unique_id": "voice-file-unique",
                "file_size": 4,
                "mime_type": "audio/ogg",
                "duration": 2
            }
        }
    })
}

fn telegram_conformance_updates() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "update_id": 901,
            "message_reaction": {
                "chat": {"id": 100, "type": "private"},
                "message_id": 321,
                "user": {"id": 9, "username": "conformance"},
                "date": 1_700_000_000,
                "old_reaction": [],
                "new_reaction": [{"type": "emoji", "emoji": "👍"}]
            }
        }),
        telegram_voice_update(),
    ]
}

impl TelegramWireFixture {
    async fn capture_configured_ingress(
        &self,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        if !self.events.inbounds.lock().unwrap().is_empty()
            && !self.events.events.lock().unwrap().is_empty()
        {
            return Ok(());
        }
        self.events.events.lock().unwrap().clear();
        self.events.inbounds.lock().unwrap().clear();
        let (tx, _rx) = tokio::sync::mpsc::channel(4);
        let channel = self.channel.clone();
        let listener = tokio::spawn(async move {
            #[allow(deprecated)]
            tinychannels_runtime::traits::Channel::listen(channel.as_ref(), tx).await
        });
        let capture = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                self.events.notified.notified().await;
                if !self.events.events.lock().unwrap().is_empty()
                    && !self.events.inbounds.lock().unwrap().is_empty()
                {
                    break;
                }
            }
        })
        .await;
        listener.abort();
        capture.map_err(|_| {
            fixture_operation_failed(
                "configured listener did not publish reaction and voice ingress",
            )
        })
    }

    async fn record_operation(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
        operation: &str,
        receipt: Option<tinychannels_bus::channel::MessageReceipt>,
    ) {
        let mut requests = self.server.received_requests().await.unwrap_or_default();
        let mut cursor = self.request_cursor.lock().unwrap();
        let request_end = requests.len();
        requests = requests.drain(*cursor..).collect();
        *cursor = request_end;
        for request in requests {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let Some(chat) = body.get("chat_id").and_then(|value| {
                value
                    .as_i64()
                    .map(|id| id.to_string())
                    .or_else(|| value.as_str().map(str::to_owned))
            }) else {
                continue;
            };
            let id = receipt
                .as_ref()
                .map(|r| r.platform_message_ids.clone())
                .unwrap_or_default();
            self.observations
                .lock()
                .unwrap()
                .push(tinychannels_conformance::WireObservation {
                    feature,
                    operation: operation.into(),
                    channel: self.scope.channel.clone(),
                    conversation: tinychannels_bus::ConversationRef {
                        kind: tinychannels_bus::ConversationKind::Dm,
                        id: chat,
                        ..Default::default()
                    },
                    reply_to_id: body.get("reply_to_message_id").map(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| v.to_string())
                    }),
                    thread_id: body.get("message_thread_id").map(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| v.to_string())
                    }),
                    payload_kinds: vec!["text".into()],
                    platform_message_ids: id,
                    request: body,
                });
        }
    }
}

#[async_trait::async_trait]
impl tinychannels_conformance::AdapterFixture for TelegramWireFixture {
    fn adapter(&self) -> &dyn tinychannels_runtime::channel::ChannelAdapter {
        &self.adapter
    }

    fn expected_scope(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
    ) -> tinychannels_conformance::ExpectedScope {
        let mut scope = self.scope.clone();
        if feature == tinychannels_conformance::ConformanceFeature::ReplyAddressing {
            scope.reply_to_id = Some("321".into());
        }
        if feature == tinychannels_conformance::ConformanceFeature::ThreadAddressing {
            scope.thread_id = Some("77".into());
            scope.conversation.thread_id = Some("77".into());
        }
        if feature == tinychannels_conformance::ConformanceFeature::InboundReactions {
            // Telegram supplies no configured-account identifier in its
            // inbound reaction update; the event scope reflects that wire DTO.
            scope.channel.account_id = None;
        }
        if feature == tinychannels_conformance::ConformanceFeature::VoiceIn {
            scope.channel.account_id = None;
        }
        scope
    }

    fn clear_observations(&self) {
        self.observations.lock().unwrap().clear();
    }

    fn observations(&self) -> Vec<tinychannels_conformance::WireObservation> {
        self.observations.lock().unwrap().clone()
    }

    async fn exercise(
        &self,
        feature: tinychannels_conformance::ConformanceFeature,
    ) -> Result<
        Option<tinychannels_bus::channel::ChannelSendOutcome>,
        tinychannels_bus::channel::ChannelSendError,
    > {
        use tinychannels_conformance::ConformanceFeature as Feature;
        if feature == Feature::Typing {
            tinychannels_runtime::channel::ChannelTyping::start_typing(
                self.channel.as_ref(),
                "100",
            )
            .await?;
            self.record_operation(feature, "typing", None).await;
            return Ok(None);
        }
        if feature == Feature::Reactions {
            tinychannels_runtime::channel::ChannelReaction::send_reaction(
                self.channel.as_ref(),
                &self.scope.channel,
                &self.scope.conversation,
                "321",
                &tinychannels_bus::channel::ChannelReaction {
                    value: "👍".into(),
                    custom_emoji_id: None,
                },
                false,
            )
            .await?;
            self.record_operation(feature, "reaction_send", None).await;
            return Ok(None);
        }
        if feature == Feature::Edit || feature == Feature::Delete {
            let bootstrap = self
                .adapter
                .send(tinychannels_bus::ChannelOutboundIntent {
                    idempotency_key: "bootstrap-message".into(),
                    channel_id: "telegram".into(),
                    conversation_id: "100".into(),
                    channel_ref: Some(self.scope.channel.clone()),
                    conversation_ref: Some(self.scope.conversation.clone()),
                    turn_id: Some("conformance-turn".into()),
                    payload: tinychannels_bus::OutboundPayload::Text {
                        text: "existing message".into(),
                    },
                    ..Default::default()
                })
                .await?;
            let request_count = self
                .server
                .received_requests()
                .await
                .unwrap_or_default()
                .len();
            *self.request_cursor.lock().unwrap() = request_count;
            if feature == Feature::Edit {
                let receipt = tinychannels_runtime::channel::ChannelEdit::edit(
                    self.channel.as_ref(),
                    &bootstrap.receipt,
                    tinychannels_bus::ChannelOutboundIntent {
                        idempotency_key: "edit-kit".into(),
                        channel_id: "telegram".into(),
                        conversation_id: "100".into(),
                        channel_ref: Some(self.scope.channel.clone()),
                        conversation_ref: Some(self.scope.conversation.clone()),
                        turn_id: Some("conformance-turn".into()),
                        payload: tinychannels_bus::OutboundPayload::Text {
                            text: "edited conformance message".into(),
                        },
                        ..Default::default()
                    },
                )
                .await?;
                self.record_operation(feature, "edit", Some(receipt.clone()))
                    .await;
                return Ok(Some(tinychannels_bus::channel::ChannelSendOutcome {
                    receipt,
                    error: None,
                }));
            }
            tinychannels_runtime::channel::ChannelDelete::delete(
                self.channel.as_ref(),
                &bootstrap.receipt,
            )
            .await?;
            self.record_operation(feature, "delete", None).await;
            return Ok(None);
        }
        if feature == Feature::StreamingDraft {
            let draft_intent = |id: &str, text: &str| tinychannels_bus::ChannelOutboundIntent {
                idempotency_key: id.into(),
                channel_id: "telegram".into(),
                conversation_id: "100".into(),
                channel_ref: Some(self.scope.channel.clone()),
                conversation_ref: Some(self.scope.conversation.clone()),
                turn_id: Some("conformance-turn".into()),
                payload: tinychannels_bus::OutboundPayload::Text { text: text.into() },
                ..Default::default()
            };
            let created = tinychannels_runtime::channel::ChannelStreamingDraft::upsert_draft(
                self.channel.as_ref(),
                None,
                draft_intent("draft-create", "conformance draft"),
            )
            .await?;
            self.record_operation(feature, "draft_create", Some(created.clone()))
                .await;

            let updated = tinychannels_runtime::channel::ChannelStreamingDraft::upsert_draft(
                self.channel.as_ref(),
                Some(&created),
                draft_intent("draft-update", "conformance draft updated"),
            )
            .await?;
            self.record_operation(feature, "draft_update", Some(updated.clone()))
                .await;

            let finalized = tinychannels_runtime::channel::ChannelEdit::edit(
                self.channel.as_ref(),
                &updated,
                draft_intent("draft-finalize", "conformance draft final"),
            )
            .await?;
            self.record_operation(feature, "draft_finalize", Some(finalized.clone()))
                .await;

            let abandoned = tinychannels_runtime::channel::ChannelStreamingDraft::upsert_draft(
                self.channel.as_ref(),
                None,
                draft_intent("draft-cleanup", "conformance abandoned draft"),
            )
            .await?;
            let request_count = self
                .server
                .received_requests()
                .await
                .unwrap_or_default()
                .len();
            *self.request_cursor.lock().unwrap() = request_count;
            tinychannels_runtime::channel::ChannelDelete::delete(self.channel.as_ref(), &abandoned)
                .await?;
            self.record_operation(feature, "draft_cancel_cleanup", None)
                .await;
            return Ok(Some(tinychannels_bus::channel::ChannelSendOutcome {
                receipt: finalized,
                error: None,
            }));
        }
        if feature == Feature::InboundReactions {
            self.capture_configured_ingress().await?;
            let captured = self
                .events
                .events
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|event| {
                    matches!(
                        event.event,
                        tinychannels_bus::ChannelEventKind::ReactionAdded { .. }
                            | tinychannels_bus::ChannelEventKind::ReactionRemoved { .. }
                    )
                })
                .cloned()
                .ok_or_else(|| {
                    fixture_operation_failed("listener emitted no inbound reaction event")
                })?;
            let requests = self.server.received_requests().await.unwrap_or_default();
            let request = requests
                .iter()
                .find(|request| request.url.path().ends_with("/getUpdates"))
                .ok_or_else(|| {
                    fixture_operation_failed("listener emitted no getUpdates request")
                })?;
            let conversation = captured.conversation.clone().unwrap_or_default();
            let (operation, payload) = match &captured.event {
                tinychannels_bus::ChannelEventKind::ReactionAdded { reaction, .. } => {
                    ("reaction_receive", reaction.value.clone())
                }
                tinychannels_bus::ChannelEventKind::ReactionRemoved { reaction, .. } => {
                    ("reaction_receive", reaction.value.clone())
                }
                other => {
                    return Err(fixture_operation_failed(format!(
                        "unexpected inbound event: {other:?}"
                    )));
                }
            };
            self.observations.lock().unwrap().push(
                tinychannels_conformance::WireObservation {
                    feature,
                    operation: operation.into(),
                    channel: captured.channel,
                    conversation,
                    reply_to_id: None,
                    thread_id: None,
                    payload_kinds: vec![payload],
                    platform_message_ids: vec![],
                    request: serde_json::json!({"updates": telegram_conformance_updates(), "path": request.url.path()}),
                },
            );
            return Ok(None);
        }
        if feature == Feature::VoiceIn {
            self.capture_configured_ingress().await?;
            let envelope = self
                .events
                .inbounds
                .lock()
                .unwrap()
                .last()
                .cloned()
                .ok_or_else(|| fixture_operation_failed("voice envelope was missing"))?;
            let mut note = envelope
                .parts
                .iter()
                .find_map(|part| match part {
                    tinychannels_bus::ContentPart::VoiceNote { voice_note } => {
                        Some(voice_note.clone())
                    }
                    _ => None,
                })
                .ok_or_else(|| {
                    fixture_operation_failed("voice envelope had no typed voice note")
                })?;
            let stored = self
                .voice
                .stored
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|request| {
                    request.direction == tinychannels_bus::HostVoiceMediaDirection::Inbound
                })
                .cloned()
                .ok_or_else(|| {
                    fixture_operation_failed("voice store did not receive inbound audio")
                })?;
            let status = crate::host::enrich_voice_note(
                &mut note,
                stored.scope.clone(),
                Some(crate::host::TranscriptionRequest {
                    audio_base64: stored.audio_base64.clone(),
                    mime_type: Some(stored.mime_type.clone()),
                    file_name: stored.file_name.clone(),
                    language: None,
                }),
                Some(self.transcriber.clone()),
            )
            .await;
            if status != crate::host::VoiceEnrichmentStatus::HostTranscription {
                return Err(fixture_operation_failed(format!(
                    "voice enrichment status was {status:?}"
                )));
            }
            let Some(transcript) = note.transcript.as_ref() else {
                return Err(fixture_operation_failed("voice transcript was missing"));
            };
            if transcript.text != "telegram conformance transcript"
                || note.media.opaque_id.as_deref() != Some("telegram-inbound-voice")
                || note.media.url.is_some()
                || note.media.path.is_some()
                || note.media.local_cache_path.is_some()
            {
                return Err(fixture_operation_failed(
                    "voice transcript or opaque media scope did not match",
                ));
            }
            let requests = self.server.received_requests().await.unwrap_or_default();
            let request = requests
                .iter()
                .find(|r| r.url.path().ends_with("/getUpdates"))
                .ok_or_else(|| fixture_operation_failed("listener did not issue getUpdates"))?;
            self.observations.lock().unwrap().push(tinychannels_conformance::WireObservation {
                feature,
                operation: "voice_in".into(),
                channel: envelope.channel,
                conversation: envelope.conversation,
                reply_to_id: envelope.reply_to_id,
                thread_id: None,
                payload_kinds: vec!["audio".into(), "transcript".into()],
                platform_message_ids: vec![],
                request: serde_json::json!({"updates": telegram_conformance_updates(), "path": request.url.path()}),
            });
            return Ok(None);
        }

        let payload = match feature {
            Feature::Text | Feature::ReplyAddressing | Feature::ThreadAddressing => {
                tinychannels_bus::OutboundPayload::Text {
                    text: "hello-kit".into(),
                }
            }
            Feature::Image => rich_part(tinychannels_bus::ContentPart::Image {
                media: conformance_media(tinychannels_bus::MediaKind::Image, "Image"),
                caption: None,
                alt_text: None,
            }),
            Feature::Video => rich_part(tinychannels_bus::ContentPart::Video {
                media: conformance_media(tinychannels_bus::MediaKind::Video, "Video"),
                caption: None,
                duration_ms: Some(1_000),
                thumbnail: None,
            }),
            Feature::Audio => rich_part(tinychannels_bus::ContentPart::Audio {
                media: conformance_media(tinychannels_bus::MediaKind::Audio, "Audio"),
                codec: Some("mp3".into()),
                duration_ms: Some(1_000),
                title: None,
                transcript: None,
            }),
            Feature::File => rich_part(tinychannels_bus::ContentPart::File {
                media: conformance_media(tinychannels_bus::MediaKind::Document, "Document"),
                file_name: Some("document.pdf".into()),
                caption: None,
            }),
            Feature::Sticker => rich_part(tinychannels_bus::ContentPart::Sticker {
                media: conformance_media(tinychannels_bus::MediaKind::Image, "Sticker"),
                pack_id: None,
                sticker_id: None,
                emoji: Some("👍".into()),
                alt_text: None,
            }),
            Feature::VoiceOut => tinychannels_bus::OutboundPayload::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("telegram-voice-out".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        ..Default::default()
                    },
                    audio_base64: None,
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1_000),
                    waveform: None,
                },
            },
            Feature::VoiceSynthesis => {
                let accepted_codecs = vec!["ogg_opus".to_owned()];
                let prepared = crate::host::prepare_voice_reply(
                    tinychannels_bus::HostVoiceScope {
                        configured_instance: "telegram-conformance".into(),
                        channel: self.scope.channel.clone(),
                        conversation: self.scope.conversation.clone(),
                        turn_id: "conformance-turn".into(),
                    },
                    "speak this".into(),
                    None,
                    &TelegramConformanceSynthesizer,
                    &TelegramConformanceCodec,
                    self.voice.as_ref(),
                    crate::host::VoiceReplyConstraints {
                        accepted_codecs: &accepted_codecs,
                        max_size_bytes: Some(8 * 1024 * 1024),
                        max_duration_ms: None,
                    },
                )
                .await
                .map_err(|error| tinychannels_bus::channel::ChannelSendError {
                    kind: tinychannels_bus::channel::SendErrorKind::BadFormat,
                    message: error.to_string(),
                    ..Default::default()
                })?;
                tinychannels_bus::OutboundPayload::Voice {
                    voice: tinychannels_bus::OutboundVoice::Media {
                        media: prepared.note.media,
                        audio_base64: None,
                        codec: prepared.note.codec,
                        duration_ms: prepared.note.duration_ms,
                        waveform: prepared.note.waveform,
                    },
                }
            }
            Feature::Location => rich_part(tinychannels_bus::ContentPart::Location {
                latitude: 25.0,
                longitude: 55.0,
                label: Some("Conformance place".into()),
                address: Some("1 Test Street".into()),
                accuracy_meters: None,
                live_until_unix_ms: None,
            }),
            Feature::Contact => rich_part(tinychannels_bus::ContentPart::Contact {
                display_name: "Conformance User".into(),
                phone_numbers: vec![tinychannels_bus::ContactMethod {
                    value: "+15555550123".into(),
                    label: None,
                }],
                emails: vec![],
                organization: None,
            }),
            Feature::Buttons => tinychannels_bus::OutboundPayload::Parts {
                parts: vec![
                    tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Text {
                            text: "Choose an action".into(),
                            markdown_dialect: None,
                        },
                    },
                    tinychannels_bus::OutboundContentPart::Content {
                        part: tinychannels_bus::ContentPart::Buttons {
                            rows: vec![vec![tinychannels_bus::ButtonAction {
                                id: "conformance".into(),
                                label: "Continue".into(),
                                style: None,
                                disabled: false,
                            }]],
                        },
                    },
                ],
            },
            Feature::Poll => rich_part(tinychannels_bus::ContentPart::Poll {
                question: "Choose one".into(),
                options: vec!["Alpha".into(), "Beta".into()],
                multiple_choice: false,
                closes_at_unix_ms: None,
                provider_poll_id: None,
            }),
            Feature::Presentation => rich_part(tinychannels_bus::ContentPart::Presentation {
                blocks: serde_json::json!({"type":"text","text":"Conformance card"}),
            }),
            _ => {
                return Err(unsupported_fixture_operation());
            }
        };
        let intent = tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "kit-op".into(),
            channel_id: "telegram".into(),
            conversation_id: "100".into(),
            channel_ref: Some(self.scope.channel.clone()),
            conversation_ref: Some(self.scope.conversation.clone()),
            turn_id: Some("conformance-turn".into()),
            reply_to_id: (feature == Feature::ReplyAddressing).then(|| "321".into()),
            thread_id: (feature == Feature::ThreadAddressing).then(|| "77".into()),
            payload,
            ..Default::default()
        };
        let result = self.adapter.send(intent).await;
        let mut requests = self.server.received_requests().await.unwrap_or_default();
        let mut cursor = self.request_cursor.lock().unwrap();
        let request_end = requests.len();
        requests = requests.drain(*cursor..).collect();
        *cursor = request_end;
        for request in requests {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let multipart = String::from_utf8_lossy(&request.body).into_owned();
            let chat = body
                .get("chat_id")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    multipart
                        .split("name=\"chat_id\"\r\n\r\n")
                        .nth(1)
                        .and_then(|value| value.split("\r\n").next())
                })
                .unwrap_or_default();
            let conversation = tinychannels_bus::ConversationRef {
                kind: tinychannels_bus::ConversationKind::Dm,
                id: chat.to_owned(),
                thread_id: body.get("message_thread_id").map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string())
                }),
                ..Default::default()
            };
            let path = request.url.path();
            let (operation, kind) = if path.ends_with("/sendMessage") {
                (
                    "send",
                    match feature {
                        Feature::Text | Feature::ReplyAddressing | Feature::ThreadAddressing => {
                            "text"
                        }
                        Feature::Buttons => "buttons",
                        Feature::Presentation => "presentation",
                        _ => "text",
                    },
                )
            } else if path.ends_with("/sendLocation") || path.ends_with("/sendVenue") {
                ("send", "location")
            } else if path.ends_with("/sendContact") {
                ("send", "contact")
            } else if path.ends_with("/sendPoll") {
                ("send", "poll")
            } else if path.ends_with("/sendPhoto") {
                ("send", "image")
            } else if path.ends_with("/sendVideo") {
                ("send", "video")
            } else if path.ends_with("/sendAudio") {
                ("send", "audio")
            } else if path.ends_with("/sendDocument") {
                ("send", "file")
            } else if path.ends_with("/sendSticker") {
                ("send", "sticker")
            } else if path.ends_with("/sendVoice") {
                ("send", "voice")
            } else if path.ends_with("/sendChatAction") {
                ("typing", "text")
            } else if path.ends_with("/setMessageReaction") {
                ("reaction_send", "text")
            } else if path.ends_with("/editMessageText") {
                ("edit", "text")
            } else if path.ends_with("/deleteMessage") {
                ("delete", "text")
            } else {
                continue;
            };
            assert_eq!(
                chat, "100",
                "actual Telegram request must target the configured DM"
            );
            let wire_id = |field: &str| {
                body.get(field).map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string())
                })
            };
            self.observations
                .lock()
                .unwrap()
                .push(tinychannels_conformance::WireObservation {
                    feature,
                    operation: operation.into(),
                    channel: self.scope.channel.clone(),
                    conversation,
                    reply_to_id: wire_id("reply_to_message_id"),
                    thread_id: wire_id("message_thread_id"),
                    payload_kinds: vec![kind.into()],
                    platform_message_ids: result
                        .as_ref()
                        .ok()
                        .map(|outcome| outcome.receipt.platform_message_ids.clone())
                        .unwrap_or_default(),
                    request: if multipart.contains("Content-Disposition: form-data") {
                        serde_json::json!({"multipart": multipart})
                    } else {
                        body
                    },
                });
        }
        result.map(Some)
    }
}

struct VoiceReadFixture;
struct TypedMediaReadFixture;

struct UncheckedMediaReadFixture;
struct LargeVoiceReadFixture {
    expected_scope: tinychannels_bus::HostVoiceScope,
}

fn outbound_voice_message(
    opaque_id: Option<&str>,
    turn_id: Option<&str>,
) -> tinychannels_bus::ChannelOutboundMessage {
    tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: turn_id.map(str::to_owned),
        idempotency_key: "operation-key".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: opaque_id.map(str::to_owned),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
                waveform: None,
            },
        }],
    }
}

#[test]
fn telegram_voice_capabilities_require_durable_store_and_host_synthesis_ports() {
    let without_store = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false).capabilities();
    assert!(!without_store.voice_in.supported);
    assert!(!without_store.voice_out.supported);
    assert!(!without_store.voice_out.synthesis);

    let with_storage = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .capabilities();
    assert!(with_storage.voice_in.supported);
    assert!(with_storage.content.parts.contains(&"text".into()));
    assert!(with_storage.content.parts.contains(&"voice_note".into()));
    assert!(with_storage.voice_out.supported);
    assert!(!with_storage.voice_out.synthesis);
    assert_eq!(
        with_storage.voice_out.accepted_codecs,
        ["ogg_opus", "m4a_aac", "mp3"]
    );
    assert_eq!(with_storage.voice_out.max_size_bytes, Some(8 * 1024 * 1024));

    let with_all_ports = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .with_voice_synthesis(true)
        .capabilities();
    assert!(with_all_ports.voice_out.synthesis);
}

#[test]
fn telegram_voice_capabilities_preserve_base_text_and_stream_descriptor() {
    let off = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .capabilities();
    assert!(off.content.text.inbound && off.content.text.outbound);
    assert!(off.content.parts.contains(&"text".into()));
    assert!(off.content.parts.contains(&"voice_note".into()));
    assert!(off.actions.supported.contains(&"send".into()));
    assert!(!off.progressive_edits);
    assert!(!off.streaming_reply_pipeline);

    let on = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_streaming(StreamMode::Partial, 100, true)
        .with_voice_media_store(Arc::new(VoiceReadFixture))
        .capabilities();
    assert!(on.progressive_edits);
    assert!(on.streaming_reply_pipeline);
    assert!(on.actions.supported.contains(&"draft-update".into()));
}

#[test]
fn telegram_advertises_typed_media_only_with_scoped_storage() {
    let unavailable = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false).capabilities();
    assert!(!unavailable.content.image.outbound);
    assert!(!unavailable.content.file.inbound);

    let configured = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false)
        .with_media_repository(Arc::new(TypedMediaReadFixture))
        .capabilities();
    assert!(configured.content.image.inbound && configured.content.image.outbound);
    assert!(configured.content.video.inbound && configured.content.video.outbound);
    assert!(configured.content.audio.inbound && configured.content.audio.outbound);
    assert!(configured.content.file.inbound && configured.content.file.outbound);
    assert!(configured.content.sticker.inbound);
    assert!(configured.content.sticker.outbound);
    assert_eq!(
        configured.content.image.accepted_types,
        ["image/jpeg", "image/png", "image/gif", "image/webp"]
    );
    assert_eq!(configured.content.sticker.accepted_types, ["image/webp"]);
    assert_eq!(
        configured.content.image.max_size_bytes,
        Some(10 * 1024 * 1024)
    );
    assert_eq!(
        configured.content.video.max_size_bytes,
        Some(20 * 1024 * 1024)
    );
    assert_eq!(
        configured.content.file.max_size_bytes,
        Some(20 * 1024 * 1024)
    );
    assert_eq!(
        configured.content.audio.emitted_types,
        ["audio/mpeg", "audio/mp4"]
    );
    assert_eq!(configured.content.video.emitted_types, ["video/mp4"]);
}

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for VoiceReadFixture {
    async fn store_voice(
        &self,
        _request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        unreachable!()
    }

    async fn read_voice(
        &self,
        scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        assert_eq!(scope.turn_id, "turn-7");
        assert_eq!(scope.channel.id, "telegram");
        assert_eq!(scope.conversation.id, "100");
        assert_eq!(opaque_id, "host-media-1");
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: "AQIDBA==".into(),
            mime_type: "audio/ogg".into(),
            file_name: Some("reply.ogg".into()),
        })
    }
}

#[async_trait::async_trait]
impl crate::host::MediaRepository for TypedMediaReadFixture {
    async fn store_media(
        &self,
        _request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        unreachable!("outbound media fixture only resolves stored media")
    }

    async fn read_media(
        &self,
        scope: crate::host::MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        assert_eq!(scope.configured_instance, "telegram-main");
        assert_eq!(scope.channel.id, "telegram");
        assert_eq!(scope.channel.account_id.as_deref(), Some("telegram-main"));
        assert_eq!(scope.conversation.id, "-10042");
        assert_eq!(scope.conversation.scope_id.as_deref(), Some("scope-42"));
        assert_eq!(scope.conversation.parent_id.as_deref(), Some("parent-42"));
        assert_eq!(scope.conversation.thread_id.as_deref(), Some("77"));
        assert_eq!(scope.conversation.topic_id.as_deref(), Some("topic-77"));
        assert_eq!(scope.turn_id, "turn-42");
        let (kind, content_type, file_name, bytes) = match opaque_id.as_str() {
            "stored-sticker-webp" => (
                tinychannels_bus::MediaKind::Image,
                "image/webp",
                "sticker.webp",
                include_bytes!("fixtures/sticker-512.webp").to_vec(),
            ),
            "stored-photo-1" => (
                tinychannels_bus::MediaKind::Image,
                "image/png",
                "photo.png",
                BASE64
                    .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
                    .unwrap(),
            ),
            "oversized-photo" => (
                tinychannels_bus::MediaKind::Image,
                "image/jpeg",
                "large.jpg",
                oversized_jpeg_with_dimensions(),
            ),
            "stored-audio-1" => (
                tinychannels_bus::MediaKind::Audio,
                "audio/mpeg",
                "audio.mp3",
                valid_mp3_frame(),
            ),
            "stored-wav-1" => (
                tinychannels_bus::MediaKind::Audio,
                "audio/wav",
                "audio.wav",
                BASE64.decode("UklGRgAAAABXQVZF").unwrap(),
            ),
            "stored-video-mp4" => (
                tinychannels_bus::MediaKind::Video,
                "video/mp4",
                "clip.mp4",
                b"\0\0\0\x18ftypisom\0\0\0\0isom".to_vec(),
            ),
            "stored-document-pdf" => (
                tinychannels_bus::MediaKind::Document,
                "application/pdf",
                "report.pdf",
                b"%PDF-1.7\nfixture".to_vec(),
            ),
            "stored-video-webm" => (
                tinychannels_bus::MediaKind::Video,
                "video/webm",
                "clip.webm",
                b"\x1a\x45\xdf\xa3fixture".to_vec(),
            ),
            "stored-image-gif" => (
                tinychannels_bus::MediaKind::Image,
                "image/gif",
                "image.gif",
                b"GIF89a".to_vec(),
            ),
            "zero-dimension-photo" => {
                let mut bytes = vec![0; 24];
                bytes[..8].copy_from_slice(b"\x89PNG\r\n\x1a\n");
                bytes[16..20].copy_from_slice(&0_u32.to_be_bytes());
                bytes[20..24].copy_from_slice(&1_u32.to_be_bytes());
                (
                    tinychannels_bus::MediaKind::Image,
                    "image/png",
                    "zero.png",
                    bytes,
                )
            }
            _ => panic!("unexpected typed media id: {opaque_id}"),
        };
        assert!(bytes.len() as u64 <= max_bytes);
        Ok(crate::host::StoredMedia {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some(opaque_id),
                kind,
                content_type: Some(content_type.into()),
                file_name: Some(file_name.into()),
                size_bytes: Some(bytes.len() as u64),
                ..Default::default()
            },
            content_type: content_type.into(),
            file_name: Some(file_name.into()),
            bytes,
        })
    }
}

#[async_trait::async_trait]
impl crate::host::MediaRepository for UncheckedMediaReadFixture {
    async fn store_media(
        &self,
        _request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<tinychannels_bus::MediaReference> {
        unreachable!("outbound media fixture only resolves stored media")
    }

    async fn read_media(
        &self,
        _scope: crate::host::MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        let bytes = BASE64
            .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
            .unwrap();
        assert!(bytes.len() as u64 <= max_bytes);
        Ok(crate::host::StoredMedia {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some(opaque_id),
                kind: tinychannels_bus::MediaKind::Image,
                content_type: Some("image/png".into()),
                size_bytes: Some(bytes.len() as u64),
                ..Default::default()
            },
            content_type: "image/png".into(),
            file_name: Some("photo.png".into()),
            bytes,
        })
    }
}

#[async_trait::async_trait]
impl crate::host::VoiceMediaStore for LargeVoiceReadFixture {
    async fn store_voice(
        &self,
        _request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        unreachable!()
    }

    async fn read_voice(
        &self,
        scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        assert_eq!(scope, self.expected_scope);
        assert_eq!(opaque_id, "large-opaque-voice");
        let bytes = vec![0x5a_u8; 129 * 1024];
        assert!(bytes.len() > tinychannels_bus::module_relay::RELAY_FRAME_BYTES);
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: BASE64.encode(bytes),
            mime_type: "audio/ogg".into(),
            file_name: Some("large-reply.ogg".into()),
        })
    }
}

#[tokio::test]
async fn telegram_resolves_and_uploads_voice_larger_than_inline_intent_limit() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 53})),
    )
    .await;
    let expected_conversation = tinychannels_bus::ConversationRef {
        kind: tinychannels_bus::ConversationKind::Dm,
        id: "100".into(),
        scope_id: Some("scope-9".into()),
        parent_id: Some("parent-4".into()),
        thread_id: Some("thread-5".into()),
        topic_id: Some("77".into()),
    };
    let expected_channel = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram:bot-1".into()),
    };
    let expected_scope = tinychannels_bus::HostVoiceScope {
        configured_instance: "telegram:bot-1".into(),
        channel: expected_channel.clone(),
        conversation: expected_conversation.clone(),
        turn_id: "large-voice-origin-turn".into(),
    };
    let channel = channel_for(&server, &["*"])
        .with_voice_media_store(Arc::new(LargeVoiceReadFixture { expected_scope }));
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: Some(expected_channel),
        conversation_ref: Some(expected_conversation),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("large-voice-origin-turn".into()),
        idempotency_key: "send-idempotency-hash".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("large-opaque-voice".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(2_000),
                waveform: None,
            },
        }],
    };
    let inline = serde_json::to_vec(&message).unwrap();
    assert!(inline.len() < tinychannels_bus::module_relay::RELAY_FRAME_BYTES);
    assert!(!String::from_utf8_lossy(&inline).contains("audio_base64"));

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("53")
    );
    let uploads = raw_bodies(&server, "sendVoice").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("large-reply.ogg"));
    assert!(uploads[0].contains(&"Z".repeat(129 * 1024)));
    assert!(uploads[0].contains("reply_parameters"));
}

#[tokio::test]
async fn typed_voice_resolves_opaque_media_and_sends_real_telegram_voice_receipt() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({
            "message_id": 51
        })),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("host-media-1".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
                waveform: None,
            },
        }],
    };

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("51")
    );
    assert_eq!(
        outcome.receipt.parts[0].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Voice
    );
    let uploads = raw_bodies(&server, "sendVoice").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("name=\"voice\""));
    assert!(uploads[0].contains("reply.ogg"));
    assert!(uploads[0].contains("\u{1}\u{2}\u{3}\u{4}"));
    assert!(uploads[0].contains("reply_parameters"));
    assert!(uploads[0].contains("\"message_id\":42"));
}

#[tokio::test]
async fn typed_voice_wire_failure_keeps_remote_uncertainty() {
    let server = mock_api().await;
    mount_post(&server, "sendVoice", ok_with(serde_json::json!({}))).await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let message = outbound_voice_message(Some("host-media-1"), Some("turn-7"));

    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();

    assert_eq!(json_bodies(&server, "sendVoice").await.len(), 1);
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(error.sent_before_error);
    assert!(!error.retryable);
}

#[tokio::test]
async fn typed_voice_sends_long_transcript_as_receipted_followup_text() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 54})),
    )
    .await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({"message_id": 55})),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let transcript = "transcript ".repeat(110);
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            topic_id: Some("77".into()),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: transcript.clone(),
                    markdown_dialect: None,
                },
            },
            tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("host-media-1".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        ..Default::default()
                    },
                    audio_base64: None,
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1000),
                    waveform: None,
                },
            },
        ],
    };
    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(outcome.error, None);
    assert_eq!(outcome.receipt.platform_message_ids, ["54", "55"]);
    assert_eq!(
        outcome
            .receipt
            .parts
            .iter()
            .map(|part| part.kind)
            .collect::<Vec<_>>(),
        [
            tinychannels_bus::channel::MessageReceiptPartKind::Voice,
            tinychannels_bus::channel::MessageReceiptPartKind::Text,
        ]
    );
    let voice = raw_bodies(&server, "sendVoice").await;
    assert_eq!(voice.len(), 1);
    assert!(
        !voice[0].contains(&transcript),
        "overlong caption is omitted from multipart upload"
    );
    let text = json_bodies(&server, "sendMessage").await;
    assert_eq!(text.len(), 1);
    assert!(text[0]["text"].as_str().unwrap().contains("transcript"));
    assert_eq!(text[0]["message_thread_id"], "77");
    assert_eq!(text[0]["reply_to_message_id"], 42);
}

#[tokio::test]
async fn long_voice_transcript_failure_retains_partial_receipts_without_retry() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 56})),
    )
    .await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .respond_with(ok_with(serde_json::json!({"message_id": 57})))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(400).set_body_string("rejected"),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let transcript = "long transcript ".repeat(400);
    let message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![
            tinychannels_bus::OutboundContentPart::Content {
                part: tinychannels_bus::ContentPart::Text {
                    text: transcript,
                    markdown_dialect: None,
                },
            },
            tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some("host-media-1".into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        ..Default::default()
                    },
                    audio_base64: None,
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1000),
                    waveform: None,
                },
            },
        ],
    };
    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["56", "57"]);
    let error = outcome
        .error
        .expect("later transcript chunk failure stays visible");
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    assert_eq!(
        json_bodies(&server, "sendMessage").await.len(),
        2,
        "the operation makes one pass and preserves the accepted text chunk"
    );
    assert_eq!(
        raw_bodies(&server, "sendVoice").await.len(),
        1,
        "ambiguous transcript failure must not repeat the voice upload"
    );
}

#[tokio::test]
async fn typed_voice_keeps_transcript_as_caption_and_sends_one_native_receipt() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 52})),
    )
    .await;
    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let mut message = tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        }),
        reply_to_id: Some("42".into()),
        thread_id: None,
        turn_id: Some("turn-7".into()),
        idempotency_key: "send-key-hash".into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Voice {
            voice: tinychannels_bus::OutboundVoice::Media {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("host-media-1".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    ..Default::default()
                },
                audio_base64: None,
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1000),
                waveform: None,
            },
        }],
    };
    message.parts.insert(
        0,
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "recognized words".into(),
                markdown_dialect: None,
            },
        },
    );

    let result = channel.send_typed_with_receipt(&message).await;
    assert!(
        result.is_ok(),
        "mixed transcript+voice must be supported: {result:?}"
    );
    let uploads = raw_bodies(&server, "sendVoice").await;
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].contains("recognized words"));
    assert!(uploads[0].contains("reply_parameters"));
    assert_eq!(
        result
            .unwrap()
            .receipt
            .primary_platform_message_id
            .as_deref(),
        Some("52")
    );
}

#[tokio::test]
async fn typed_voice_rejects_missing_host_storage_reference_and_origin_turn() {
    let server = mock_api().await;
    let message = outbound_voice_message(Some("voice-ref"), Some("turn-1"));
    let missing_store = channel_for(&server, &["*"])
        .send_typed_with_receipt(&message)
        .await
        .unwrap_err();
    assert!(
        missing_store
            .to_string()
            .contains("requires host media resolution")
    );

    let channel = channel_for(&server, &["*"]).with_voice_media_store(Arc::new(VoiceReadFixture));
    let missing_reference = channel
        .send_typed_with_receipt(&outbound_voice_message(None, Some("turn-1")))
        .await
        .unwrap_err();
    assert!(
        missing_reference
            .to_string()
            .contains("no host-owned opaque reference")
    );

    let missing_turn = channel
        .send_typed_with_receipt(&outbound_voice_message(Some("voice-ref"), None))
        .await
        .unwrap_err();
    assert!(
        missing_turn
            .to_string()
            .contains("requires originating turn identity")
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

/// Hang guard for loops that must terminate on their own.
const GUARD: Duration = Duration::from_secs(20);

fn streaming(server: &MockServer) -> TelegramChannel {
    channel_for(server, &["*"]).with_streaming(StreamMode::Partial, 0, true)
}

#[tokio::test]
async fn send_draft_posts_placeholder_and_returns_message_id() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    let ch = streaming(&server);

    let id = ch
        .send_draft(&SendMessage::new("", "100:7").in_thread(Some("55".into())))
        .await
        .unwrap();

    assert_eq!(id.as_deref(), Some("321"));
    assert_eq!(
        json_bodies(&server, "sendMessage").await,
        vec![serde_json::json!({
            "chat_id": "100",
            "text": "...",
            "message_thread_id": "7",
            "reply_to_message_id": 55,
        })]
    );
    assert!(ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn send_draft_without_message_id_in_result_returns_none() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok_with(serde_json::json!({}))).await;
    let ch = streaming(&server);

    let id = ch
        .send_draft(&SendMessage::new("thinking", "100"))
        .await
        .unwrap();
    assert_eq!(id, None);
    assert_eq!(
        json_bodies(&server, "sendMessage").await[0]["text"],
        "thinking"
    );
}

#[tokio::test]
async fn send_draft_surfaces_api_rejection() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(400).set_body_string("chat not found"),
    )
    .await;
    let ch = streaming(&server);

    let err = ch
        .send_draft(&SendMessage::new("x", "100"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("sendMessage (draft) failed"), "{err}");
    assert!(err.contains("chat not found"), "{err}");
}

fn streaming_draft_intent(text: &str) -> tinychannels_bus::ChannelOutboundIntent {
    tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "telegram-draft-key".into(),
        channel_id: "telegram".into(),
        conversation_id: "100:7".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("configured-bot".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "100:7".into(),
            scope_id: Some("configured-bot".into()),
            thread_id: Some("7".into()),
            topic_id: Some("8".into()),
            ..Default::default()
        }),
        reply_to_id: Some("55".into()),
        payload: tinychannels_bus::OutboundPayload::Text { text: text.into() },
        ..Default::default()
    }
}

#[tokio::test]
async fn typed_draft_lifecycle_uses_telegram_placeholder_edit_and_final_wire_calls() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    mount_post(&server, "editMessageText", ok()).await;
    let channel = streaming(&server);

    let draft_id = tinychannels_runtime::channel::ChannelStreamingDraft::start_draft(
        &channel,
        streaming_draft_intent(""),
    )
    .await
    .unwrap()
    .expect("Telegram start returns its real provider message ID");
    tinychannels_runtime::channel::ChannelStreamingDraft::update_draft(
        &channel,
        &draft_id,
        streaming_draft_intent("partial"),
    )
    .await
    .unwrap();
    tinychannels_runtime::channel::ChannelStreamingDraft::finalize_draft(
        &channel,
        &draft_id,
        streaming_draft_intent("final <tool_call>{}</tool_call>"),
    )
    .await
    .unwrap();

    assert_eq!(draft_id, "321");
    assert_eq!(
        json_bodies(&server, "sendMessage").await,
        vec![serde_json::json!({
            "chat_id": "100",
            "text": "...",
            "message_thread_id": "8",
            "reply_to_message_id": 55,
        })]
    );
    let edits = json_bodies(&server, "editMessageText").await;
    assert_eq!(edits.len(), 2);
    assert_eq!(
        edits[0],
        serde_json::json!({ "chat_id": "100", "message_id": 321, "text": "partial" })
    );
    assert_eq!(edits[1]["parse_mode"], "MarkdownV2");
    assert_eq!(edits[1]["text"], "final");
}

#[tokio::test]
async fn typed_thread_scope_does_not_become_a_reply_target() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 322 })),
    )
    .await;
    let channel = streaming(&server);
    let mut intent = streaming_draft_intent("partial");
    intent.conversation_id = "100".into();
    intent.reply_to_id = None;
    let conversation = intent.conversation_ref.as_mut().unwrap();
    conversation.id = "100".into();
    conversation.thread_id = Some("7".into());
    conversation.topic_id = None;

    let id = tinychannels_runtime::channel::ChannelStreamingDraft::start_draft(&channel, intent)
        .await
        .unwrap();

    assert_eq!(id.as_deref(), Some("322"));
    let requests = json_bodies(&server, "sendMessage").await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["message_thread_id"], "7");
    assert!(requests[0].get("reply_to_message_id").is_none());
}

#[tokio::test]
async fn typed_draft_finalize_keeps_overflow_delete_and_chunk_fallback() {
    let server = mock_api().await;
    mount_post(&server, "deleteMessage", ok_with(serde_json::json!(true))).await;
    mount_post(&server, "sendMessage", ok()).await;
    let channel = streaming(&server);
    let mut intent = streaming_draft_intent(&"y".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 1));
    intent.conversation_id = "100".into();
    intent.conversation_ref.as_mut().unwrap().id = "100".into();

    tinychannels_runtime::channel::ChannelStreamingDraft::finalize_draft(&channel, "8", intent)
        .await
        .unwrap();

    assert_eq!(
        json_bodies(&server, "deleteMessage").await,
        vec![serde_json::json!({ "chat_id": "100", "message_id": 8 })]
    );
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

fn typed_text(id: &str, text: String) -> tinychannels_bus::ChannelOutboundMessage {
    tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "-10042".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "forged".into(),
            account_id: Some("untrusted-account".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "-10042".into(),
            scope_id: Some("scope".into()),
            parent_id: Some("parent".into()),
            thread_id: Some("77".into()),
            topic_id: Some("topic".into()),
        }),
        reply_to_id: Some("5".into()),
        thread_id: None,
        turn_id: None,
        idempotency_key: id.into(),
        parts: vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text,
                markdown_dialect: None,
            },
        }],
    }
}

fn typed_parts(
    id: &str,
    parts: Vec<tinychannels_bus::OutboundContentPart>,
) -> tinychannels_bus::ChannelOutboundMessage {
    tinychannels_bus::ChannelOutboundMessage {
        channel_id: "telegram".into(),
        conversation_id: "-10042".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram:main".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "-10042".into(),
            topic_id: Some("77".into()),
            ..Default::default()
        }),
        reply_to_id: Some("12".into()),
        thread_id: Some("77".into()),
        turn_id: Some("turn-1".into()),
        idempotency_key: id.into(),
        parts,
    }
}

#[tokio::test]
async fn native_location_contact_buttons_poll_and_presentation_use_bot_api_methods() {
    use tinychannels_bus::channel::{ButtonAction, ContactMethod, ContentPart};
    let server = mock_api().await;
    for method_name in [
        "sendLocation",
        "sendVenue",
        "sendContact",
        "sendMessage",
        "sendPoll",
    ] {
        mount_post(
            &server,
            method_name,
            ok_with(serde_json::json!({"message_id": 321})),
        )
        .await;
    }
    let channel = channel_for(&server, &["*"]);

    let location = typed_parts(
        "location",
        vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Location {
                latitude: 29.3,
                longitude: 47.9,
                label: None,
                address: None,
                accuracy_meters: Some(15.0),
                live_until_unix_ms: None,
            },
        }],
    );
    let outcome = crate::channel::ChannelTypedSend::send_typed(&channel, &location)
        .await
        .unwrap();
    assert_eq!(
        outcome.receipt.primary_platform_message_id.as_deref(),
        Some("321")
    );
    let body = json_bodies(&server, "sendLocation").await.remove(0);
    assert_eq!(body["latitude"], 29.3);
    assert_eq!(body["longitude"], 47.9);
    assert_eq!(body["message_thread_id"], "77");
    assert_eq!(body["reply_parameters"]["message_id"], 12);

    let venue = typed_parts(
        "venue",
        vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Location {
                latitude: 29.3,
                longitude: 47.9,
                label: Some("Office".into()),
                address: Some("Main Street".into()),
                accuracy_meters: None,
                live_until_unix_ms: None,
            },
        }],
    );
    crate::channel::ChannelTypedSend::send_typed(&channel, &venue)
        .await
        .unwrap();
    let body = json_bodies(&server, "sendVenue").await.remove(0);
    assert_eq!(body["title"], "Office");
    assert_eq!(body["address"], "Main Street");

    let contact = typed_parts(
        "contact",
        vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Contact {
                display_name: "Ada Lovelace".into(),
                phone_numbers: vec![ContactMethod {
                    value: "+1234567".into(),
                    label: Some("CELL".into()),
                }],
                emails: vec![ContactMethod {
                    value: "ada@example.test".into(),
                    label: None,
                }],
                organization: Some("Analytical Engine".into()),
            },
        }],
    );
    crate::channel::ChannelTypedSend::send_typed(&channel, &contact)
        .await
        .unwrap();
    let body = json_bodies(&server, "sendContact").await.remove(0);
    assert_eq!(body["phone_number"], "+1234567");
    assert_eq!(body["first_name"], "Ada");
    assert_eq!(body["last_name"], "Lovelace");
    assert!(
        body["vcard"]
            .as_str()
            .unwrap()
            .contains("EMAIL;TYPE=INTERNET:ada@example.test")
    );

    let buttons = typed_parts(
        "buttons",
        vec![
            tinychannels_bus::OutboundContentPart::Content {
                part: ContentPart::Text {
                    text: "Choose".into(),
                    markdown_dialect: None,
                },
            },
            tinychannels_bus::OutboundContentPart::Content {
                part: ContentPart::Buttons {
                    rows: vec![vec![ButtonAction {
                        id: "opaque-action-17".into(),
                        label: "Continue".into(),
                        style: None,
                        disabled: false,
                    }]],
                },
            },
        ],
    );
    crate::channel::ChannelTypedSend::send_typed(&channel, &buttons)
        .await
        .unwrap();
    let body = json_bodies(&server, "sendMessage").await.remove(0);
    assert_eq!(
        body["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
        "opaque-action-17"
    );
    assert_eq!(body["text"], "Choose");

    let poll = typed_parts(
        "poll",
        vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Poll {
                question: "Preferred option?".into(),
                options: vec!["A".into(), "B".into()],
                multiple_choice: true,
                closes_at_unix_ms: None,
                provider_poll_id: None,
            },
        }],
    );
    crate::channel::ChannelTypedSend::send_typed(&channel, &poll)
        .await
        .unwrap();
    let body = json_bodies(&server, "sendPoll").await.remove(0);
    assert_eq!(
        body["options"],
        serde_json::json!([{"text":"A"},{"text":"B"}])
    );
    assert_eq!(body["allows_multiple_answers"], true);
    assert_eq!(body["message_thread_id"], "77");

    let presentation = typed_parts(
        "presentation",
        vec![tinychannels_bus::OutboundContentPart::Content {
            part: ContentPart::Presentation {
                blocks: serde_json::json!({"title":"Update", "text":"Status"}),
            },
        }],
    );
    let before_presentation = json_bodies(&server, "sendMessage").await.len();
    crate::channel::ChannelTypedSend::send_typed(&channel, &presentation)
        .await
        .unwrap();
    let message_bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(message_bodies.len(), before_presentation + 1);
    let body = message_bodies.last().unwrap();
    assert_eq!(body["text"], "Update\nStatus");
    assert_eq!(body["parse_mode"], "MarkdownV2");
}

#[tokio::test]
async fn structured_payloads_reject_invalid_bot_api_values_before_http() {
    use tinychannels_bus::channel::{ButtonAction, ContactMethod, ContentPart};
    let server = mock_api().await;
    let channel = channel_for(&server, &["*"]);
    let text = || tinychannels_bus::OutboundContentPart::Content {
        part: ContentPart::Text {
            text: "prompt".into(),
            markdown_dialect: None,
        },
    };
    let part = |part| tinychannels_bus::OutboundContentPart::Content { part };
    let invalid = vec![
        part(ContentPart::Location {
            latitude: 91.0,
            longitude: 0.0,
            label: None,
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: None,
        }),
        part(ContentPart::Location {
            latitude: 1.0,
            longitude: 0.0,
            label: Some("venue".into()),
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: None,
        }),
        part(ContentPart::Location {
            latitude: 1.0,
            longitude: 0.0,
            label: None,
            address: None,
            accuracy_meters: Some(1501.0),
            live_until_unix_ms: None,
        }),
        part(ContentPart::Location {
            latitude: 1.0,
            longitude: 0.0,
            label: None,
            address: None,
            accuracy_meters: None,
            live_until_unix_ms: Some(0),
        }),
        part(ContentPart::Contact {
            display_name: "No phone".into(),
            phone_numbers: vec![],
            emails: vec![],
            organization: None,
        }),
        part(ContentPart::Contact {
            display_name: "Ada".into(),
            phone_numbers: vec![ContactMethod {
                value: "1".repeat(257),
                label: None,
            }],
            emails: vec![],
            organization: None,
        }),
        part(ContentPart::Contact {
            display_name: "Ada".into(),
            phone_numbers: vec![ContactMethod {
                value: "+1".into(),
                label: Some("CELL\\nINJECT".into()),
            }],
            emails: vec![],
            organization: Some("x".repeat(2100)),
        }),
        part(ContentPart::Buttons {
            rows: vec![vec![ButtonAction {
                id: "action".into(),
                label: "Disabled".into(),
                style: None,
                disabled: true,
            }]],
        }),
        part(ContentPart::Buttons {
            rows: vec![vec![ButtonAction {
                id: "action".into(),
                label: "Styled".into(),
                style: Some(tinychannels_bus::ButtonStyle::Primary),
                disabled: false,
            }]],
        }),
        part(ContentPart::Buttons {
            rows: vec![vec![ButtonAction {
                id: "x".repeat(65),
                label: "Too long".into(),
                style: None,
                disabled: false,
            }]],
        }),
        part(ContentPart::Buttons { rows: vec![] }),
        part(ContentPart::Poll {
            question: "".into(),
            options: vec!["A".into(), "B".into()],
            multiple_choice: false,
            closes_at_unix_ms: None,
            provider_poll_id: None,
        }),
        part(ContentPart::Poll {
            question: "Question".into(),
            options: vec!["A".into()],
            multiple_choice: false,
            closes_at_unix_ms: None,
            provider_poll_id: None,
        }),
        part(ContentPart::Poll {
            question: "Question".into(),
            options: vec!["A".into(), "B".into()],
            multiple_choice: false,
            closes_at_unix_ms: Some(0),
            provider_poll_id: None,
        }),
        part(ContentPart::Presentation {
            blocks: serde_json::json!({}),
        }),
        part(ContentPart::Native {
            provider: "other".into(),
            data: serde_json::json!({"x":1}),
        }),
    ];

    for (index, structured) in invalid.into_iter().enumerate() {
        let parts = if matches!(
            structured,
            tinychannels_bus::OutboundContentPart::Content {
                part: ContentPart::Buttons { .. }
            }
        ) {
            vec![text(), structured]
        } else {
            vec![structured]
        };
        let message = typed_parts(&format!("invalid-{index}"), parts);
        let result = crate::channel::ChannelTypedSend::send_typed(&channel, &message).await;
        assert!(
            result.is_err(),
            "invalid structured case {index} was accepted"
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn bridge_approval_presentation_renders_scoped_inline_choice_actions() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 991 })),
    )
    .await;
    let channel = channel_for(&server, &["*"]);
    let intent = crate::harness::translate_output_event(
        &crate::harness::ChannelTurn::default(),
        crate::harness::ChannelOutputEvent::ApprovalRequest {
            id: "request-42".into(),
            prompt: "Run the command?".into(),
            choices: vec!["Allow once".into(), "Deny".into()],
        },
        &Default::default(),
        &crate::channel::ChannelPresentationCapabilities {
            max_actions: Some(2),
            max_actions_per_row: Some(1),
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .remove(0);
    let tinychannels_bus::OutboundPayload::PresentationBlocks { blocks } = intent.payload else {
        panic!("bridge should preserve native approval presentation blocks");
    };
    let message = typed_parts(
        "approval-presentation",
        vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Presentation { blocks },
        }],
    );

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.platform_message_ids, ["991"]);
    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["text"], "Run the command?");
    let keyboard = &bodies[0]["reply_markup"]["inline_keyboard"];
    assert_eq!(keyboard[0][0]["text"], "Allow once");
    assert_eq!(keyboard[1][0]["text"], "Deny");
    assert_eq!(keyboard[0][0]["callback_data"], "approval-v1:request-42:0");
    assert_eq!(keyboard[1][0]["callback_data"], "approval-v1:request-42:1");
}

#[tokio::test]
async fn telegram_native_data_is_explicitly_unsupported_without_provider_io() {
    let server = mock_api().await;
    let channel = channel_for(&server, &["*"]);
    let message = typed_parts(
        "native-data",
        vec![tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::channel::ContentPart::Native {
                provider: "telegram".into(),
                data: serde_json::json!({"opaque":"payload"}),
            },
        }],
    );
    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert!(error.unsupported);
    assert!(!error.sent_before_error);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_image_upload_reads_scoped_media_and_keeps_ordered_receipts() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({"message_id": 11})),
    )
    .await;
    mount_post(
        &server,
        "sendPhoto",
        ok_with(serde_json::json!({"message_id": 22})),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("ordered-image", "before".into());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "before".into(),
                markdown_dialect: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-photo-1".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("photo caption".into()),
                alt_text: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "after".into(),
                markdown_dialect: None,
            },
        },
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["11", "22", "11"]);
    assert_eq!(outcome.receipt.parts.len(), 3);
    assert_eq!(
        outcome.receipt.parts[1].kind,
        tinychannels_bus::channel::MessageReceiptPartKind::Media
    );
    assert_eq!(outcome.receipt.parts[1].thread_id.as_deref(), Some("77"));
    assert_eq!(
        outcome
            .receipt
            .conversation
            .as_ref()
            .and_then(|conversation| conversation.topic_id.as_deref()),
        Some("topic-77")
    );
    assert_eq!(outcome.receipt.parts[1].reply_to_id.as_deref(), Some("5"));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].url.path(), "/bottest-token/sendMessage");
    assert_eq!(requests[1].url.path(), "/bottest-token/sendPhoto");
    assert_eq!(requests[2].url.path(), "/bottest-token/sendMessage");
    let multipart = String::from_utf8_lossy(&requests[1].body);
    assert!(multipart.contains("photo caption"));
    assert!(multipart.contains("name=\"chat_id\"\r\n\r\n-10042"));
    assert!(multipart.contains("name=\"reply_parameters\"\r\n\r\n{\"message_id\":5}"));
    assert!(multipart.contains("name=\"message_thread_id\"\r\n\r\n77"));
    assert!(multipart.contains("photo.png"));
    assert!(multipart.contains("Content-Type: image/png"));
    let expected_bytes = BASE64
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=")
        .unwrap();
    assert!(
        requests[1]
            .body
            .windows(expected_bytes.len())
            .any(|window| window == expected_bytes)
    );
}

#[tokio::test]
async fn typed_image_rejects_non_numeric_platform_thread_before_upload() {
    let server = mock_api().await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(UncheckedMediaReadFixture));
    let mut message = typed_text("invalid-thread", "caption".into());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        thread_id: None,
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-photo-1".into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }];

    assert!(channel.send_typed_with_receipt(&message).await.is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_media_preserves_legacy_address_and_requires_host_opaque_id() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendPhoto",
        ok_with(serde_json::json!({ "message_id": 701 })),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(UncheckedMediaReadFixture));
    let mut message = typed_text("legacy-address", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-legacy".into());
    message.thread_id = Some("77".into());
    message.conversation_ref = None;
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-photo-1".into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["701"]);
    assert_eq!(outcome.receipt.thread_id.as_deref(), Some("77"));

    let mut missing_reference = scoped_typed_media_message("stored-photo-1");
    if let tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image { media, .. },
    } = &mut missing_reference.parts[0]
    {
        media.opaque_id = None;
    }
    let error = channel
        .send_typed_with_receipt(&missing_reference)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("before provider dispatch"));
    assert_eq!(json_bodies(&server, "sendPhoto").await.len(), 1);
}

#[tokio::test]
async fn typed_text_partial_failure_keeps_real_ids_and_stops_before_media() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .respond_with(ok_with(serde_json::json!({ "message_id": 702 })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .respond_with(ResponseTemplate::new(500).set_body_string("temporary failure"))
        .mount(&server)
        .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = scoped_typed_media_message("stored-photo-1");
    let image = message.parts.remove(0);
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "x".repeat(5_000),
                markdown_dialect: None,
            },
        },
        image,
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["702"]);
    assert_eq!(outcome.receipt.parts.len(), 1);
    let error = outcome.error.expect("second text chunk failed");
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    assert!(error.message.contains("only part"));
    assert!(json_bodies(&server, "sendPhoto").await.is_empty());
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

fn scoped_typed_media_message(id: &str) -> tinychannels_bus::ChannelOutboundMessage {
    let mut message = typed_text(id, String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        kind: tinychannels_bus::ConversationKind::Group,
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some(id.into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }];
    message
}

#[tokio::test]
async fn typed_media_scope_and_provider_validation_fail_before_upload() {
    let server = mock_api().await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));

    let mut no_conversation = scoped_typed_media_message("stored-photo-1");
    no_conversation.conversation_ref.as_mut().unwrap().id = "  ".into();
    let error = channel
        .send_typed_with_receipt(&no_conversation)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("before provider dispatch"));
    assert!(server.received_requests().await.unwrap().is_empty());

    let mut no_account = scoped_typed_media_message("stored-photo-1");
    no_account.channel_ref.as_mut().unwrap().account_id = None;
    assert!(
        channel
            .send_typed_with_receipt(&no_account)
            .await
            .unwrap_err()
            .to_string()
            .contains("before provider dispatch")
    );

    let mut no_turn = scoped_typed_media_message("stored-photo-1");
    no_turn.turn_id = None;
    assert!(
        channel
            .send_typed_with_receipt(&no_turn)
            .await
            .unwrap_err()
            .to_string()
            .contains("before provider dispatch")
    );
    assert!(server.received_requests().await.unwrap().is_empty());

    let mut no_reply = scoped_typed_media_message("stored-photo-1");
    no_reply.reply_to_id = Some("not-a-platform-id".into());
    assert!(
        channel
            .send_typed_with_receipt(&no_reply)
            .await
            .unwrap_err()
            .to_string()
            .contains("before provider dispatch")
    );

    let mut unsupported = scoped_typed_media_message("stored-photo-1");
    for voice_id in ["voice-a", "voice-b"] {
        unsupported
            .parts
            .push(tinychannels_bus::OutboundContentPart::Voice {
                voice: tinychannels_bus::OutboundVoice::Media {
                    media: tinychannels_bus::MediaReference {
                        opaque_id: Some(voice_id.into()),
                        kind: tinychannels_bus::MediaKind::Audio,
                        ..Default::default()
                    },
                    audio_base64: None,
                    codec: Some("ogg_opus".into()),
                    duration_ms: Some(1_000),
                    waveform: None,
                },
            });
    }
    assert!(
        channel
            .send_typed_with_receipt(&unsupported)
            .await
            .unwrap_err()
            .to_string()
            .contains("before provider dispatch")
    );

    let zero_dimensions = scoped_typed_media_message("zero-dimension-photo");
    assert!(
        channel
            .send_typed_with_receipt(&zero_dimensions)
            .await
            .unwrap_err()
            .to_string()
            .contains("before provider dispatch")
    );

    let mut sticker = scoped_typed_media_message("stored-photo-1");
    sticker.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Sticker {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-photo-1".into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            pack_id: None,
            sticker_id: None,
            emoji: None,
            alt_text: None,
        },
    }];
    assert!(
        channel
            .send_typed_with_receipt(&sticker)
            .await
            .unwrap_err()
            .to_string()
            .contains("before provider dispatch")
    );

    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn native_webp_sticker_uses_send_sticker_and_returns_its_message_id() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendSticker",
        ok_with(serde_json::json!({ "message_id": 811 })),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = scoped_typed_media_message("stored-sticker-webp");
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Sticker {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-sticker-webp".into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            pack_id: None,
            sticker_id: None,
            emoji: Some("🌟".into()),
            alt_text: None,
        },
    }];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert!(outcome.error.is_none());
    assert_eq!(outcome.receipt.platform_message_ids, ["811"]);
    let bodies = raw_bodies(&server, "sendSticker").await;
    assert_eq!(bodies.len(), 1);
    assert!(bodies[0].contains("name=\"sticker\""));
    assert!(bodies[0].contains("sticker.webp"));
    assert!(bodies[0].contains("message_thread_id"));
}

#[tokio::test]
async fn typed_media_caption_failure_after_text_keeps_receipt_and_forbids_retry() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({"message_id": 700})),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = scoped_typed_media_message("stored-photo-1");
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "delivered before media".into(),
                markdown_dialect: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-photo-1".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("caption ".repeat(129)),
                alt_text: None,
            },
        },
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["700"]);
    assert_eq!(outcome.receipt.parts.len(), 1);
    let error = outcome.error.expect("partial typed send is reported");
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    assert_eq!(json_bodies(&server, "sendPhoto").await.len(), 0);
}

#[tokio::test]
async fn typed_media_failure_after_text_keeps_real_partial_ids_and_forbids_retry() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({"message_id": 11})),
    )
    .await;
    mount_post(
        &server,
        "sendPhoto",
        ResponseTemplate::new(400).set_body_string("rejected"),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("partial-image", "before".into());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message
        .parts
        .push(tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-photo-1".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        });

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["11"]);
    let error = outcome.error.expect("partial delivery must be explicit");
    assert!(error.sent_before_error);
    assert!(!error.retryable);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].url.path().ends_with("/sendMessage"));
    assert!(requests[1].url.path().ends_with("/sendPhoto"));
}

#[tokio::test]
async fn configured_local_adapter_accepts_and_delivers_typed_telegram_media() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendPhoto",
        ok_with(serde_json::json!({"message_id": 22})),
    )
    .await;
    let channel = Arc::new(
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture)),
    );
    let account = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    };
    let conversation = tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    };
    let adapter = tinychannels_runtime::adapters::LocalChannelAdapter::new(
        tinychannels_bus::channel::ChannelDescriptor {
            id: account.id.clone(),
            account_id: account.account_id.clone(),
            ..Default::default()
        },
        channel.capabilities(),
        Arc::new(TelegramConformanceSink(channel.clone())),
    );
    let outcome = adapter
        .send(tinychannels_bus::ChannelOutboundIntent {
            idempotency_key: "adapter-photo".into(),
            channel_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            channel_ref: Some(account.clone()),
            conversation_ref: Some(conversation.clone()),
            turn_id: Some("turn-42".into()),
            payload: tinychannels_bus::OutboundPayload::Parts {
                parts: vec![tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Image {
                        media: tinychannels_bus::MediaReference {
                            opaque_id: Some("stored-photo-1".into()),
                            kind: tinychannels_bus::MediaKind::Image,
                            ..Default::default()
                        },
                        caption: Some("caption".into()),
                        alt_text: None,
                    },
                }],
            },
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["22"]);
    assert!(outcome.error.is_none());
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].url.path().ends_with("/sendPhoto"));
}

#[tokio::test]
async fn typed_audio_upload_preserves_caption_title_and_duration() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendAudio",
        ok_with(serde_json::json!({"message_id": 44})),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("audio-metadata", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        topic_id: Some("topic-77".into()),
        thread_id: Some("77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Audio {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-audio-1".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            },
            codec: Some("mp3".into()),
            duration_ms: Some(1_250),
            title: Some("Original title".into()),
            transcript: Some("spoken words".into()),
        },
    }];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["44"]);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].url.path().ends_with("/sendAudio"));
    let body = String::from_utf8_lossy(&requests[0].body);
    assert!(body.contains("name=\"caption\"\r\n\r\nspoken words"));
    assert!(body.contains("name=\"title\"\r\n\r\nOriginal title"));
    assert!(body.contains("name=\"duration\"\r\n\r\n2"));
    assert!(body.contains("Content-Type: audio/mpeg"));
}

#[tokio::test]
async fn typed_video_and_file_upload_use_native_multipart_with_real_receipts() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendVideo",
        ok_with(serde_json::json!({"message_id": 51})),
    )
    .await;
    mount_post(
        &server,
        "sendDocument",
        ok_with(serde_json::json!({"message_id": 52})),
    )
    .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("video-and-file", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Video {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-video-mp4".into()),
                    kind: tinychannels_bus::MediaKind::Video,
                    ..Default::default()
                },
                caption: Some("video caption".into()),
                duration_ms: Some(2_200),
                thumbnail: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::File {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-document-pdf".into()),
                    kind: tinychannels_bus::MediaKind::Document,
                    ..Default::default()
                },
                file_name: Some("report.pdf".into()),
                caption: Some("file caption".into()),
            },
        },
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["51", "52"]);
    assert_eq!(outcome.receipt.parts.len(), 2);
    assert_eq!(outcome.receipt.parts[0].thread_id.as_deref(), Some("77"));
    assert_eq!(outcome.receipt.parts[1].thread_id.as_deref(), Some("77"));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].url.path().ends_with("/sendVideo"));
    assert!(requests[1].url.path().ends_with("/sendDocument"));
    let video = String::from_utf8_lossy(&requests[0].body);
    assert!(video.contains("Content-Type: video/mp4"));
    assert!(video.contains("name=\"caption\"\r\n\r\nvideo caption"));
    assert!(video.contains("name=\"duration\"\r\n\r\n3"));
    assert!(video.contains("name=\"message_thread_id\"\r\n\r\n77"));
    assert!(
        requests[0]
            .body
            .windows(8)
            .any(|window| window == b"ftypisom")
    );
    let file = String::from_utf8_lossy(&requests[1].body);
    assert!(file.contains("Content-Type: application/pdf"));
    assert!(file.contains("report.pdf"));
    assert!(file.contains("name=\"caption\"\r\n\r\nfile caption"));
    assert!(
        requests[1]
            .body
            .windows(8)
            .any(|window| window == b"%PDF-1.7")
    );
}

#[tokio::test]
async fn unsupported_native_video_and_image_formats_fall_back_to_documents() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendDocument"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true,
            "result": {"message_id": 61}
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendDocument"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true,
            "result": {"message_id": 62}
        })))
        .mount(&server)
        .await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("document-fallbacks", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Video {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-video-webm".into()),
                    kind: tinychannels_bus::MediaKind::Video,
                    ..Default::default()
                },
                caption: Some("webm fallback".into()),
                duration_ms: None,
                thumbnail: None,
            },
        },
        tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Image {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("stored-image-gif".into()),
                    kind: tinychannels_bus::MediaKind::Image,
                    ..Default::default()
                },
                caption: Some("gif fallback".into()),
                alt_text: None,
            },
        },
    ];

    let outcome = channel.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.receipt.platform_message_ids, ["61", "62"]);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(request.url.path().ends_with("/sendDocument"));
        let body = String::from_utf8_lossy(&request.body);
        assert!(body.contains("name=\"message_thread_id\"\r\n\r\n77"));
    }
    let webm = String::from_utf8_lossy(&requests[0].body);
    assert!(webm.contains("Content-Type: video/webm"));
    assert!(webm.contains("webm fallback"));
    assert!(
        requests[0]
            .body
            .windows(4)
            .any(|window| window == b"\x1a\x45\xdf\xa3")
    );
    let gif = String::from_utf8_lossy(&requests[1].body);
    assert!(gif.contains("Content-Type: image/gif"));
    assert!(gif.contains("gif fallback"));
    assert!(
        requests[1]
            .body
            .windows(6)
            .any(|window| window == b"GIF89a")
    );
}

#[tokio::test]
async fn telegram_rejects_wav_audio_before_native_upload() {
    let server = mock_api().await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("unsupported-wav", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Audio {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("stored-wav-1".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            },
            codec: Some("wav_pcm16".into()),
            duration_ms: None,
            title: None,
            transcript: None,
        },
    }];

    let error = channel.send_typed_with_receipt(&message).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("rejected before provider dispatch")
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn telegram_photo_over_ten_megabytes_is_rejected_before_http() {
    let server = mock_api().await;
    let channel =
        channel_for(&server, &["*"]).with_media_repository(Arc::new(TypedMediaReadFixture));
    let mut message = typed_text("oversized-photo", String::new());
    message.channel_ref = Some(tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: Some("telegram-main".into()),
    });
    message.turn_id = Some("turn-42".into());
    message.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "-10042".into(),
        scope_id: Some("scope-42".into()),
        parent_id: Some("parent-42".into()),
        thread_id: Some("77".into()),
        topic_id: Some("topic-77".into()),
        ..Default::default()
    });
    message.parts = vec![tinychannels_bus::OutboundContentPart::Content {
        part: tinychannels_bus::ContentPart::Image {
            media: tinychannels_bus::MediaReference {
                opaque_id: Some("oversized-photo".into()),
                kind: tinychannels_bus::MediaKind::Image,
                ..Default::default()
            },
            caption: None,
            alt_text: None,
        },
    }];

    let error = channel.send_typed_with_receipt(&message).await.unwrap_err();

    assert!(error.to_string().contains("before provider dispatch"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

fn valid_mp3_frame() -> Vec<u8> {
    // MPEG-1 Layer III, 128 kbps, 44.1 kHz frame (417 bytes).
    let mut bytes = vec![0_u8; 417];
    bytes[..4].copy_from_slice(&[0xff, 0xfb, 0x90, 0x64]);
    bytes
}

fn oversized_jpeg_with_dimensions() -> Vec<u8> {
    let mut bytes = vec![0_u8; 10 * 1024 * 1024 + 1];
    bytes[..11].copy_from_slice(&[
        0xff, 0xd8, 0xff, 0xc0, 0x00, 0x11, 0x08, 0x00, 0x01, 0x00, 0x01,
    ]);
    bytes
}

#[test]
fn telegram_image_dimensions_reads_png_and_jpeg_headers_safely() {
    let mut png = vec![0; 24];
    png[..8].copy_from_slice(b"\x89PNG\r\n\x1a\n");
    png[16..20].copy_from_slice(&640_u32.to_be_bytes());
    png[20..24].copy_from_slice(&480_u32.to_be_bytes());
    assert_eq!(
        telegram_image_dimensions("image/png", &png),
        Some((640, 480))
    );
    assert_eq!(telegram_image_dimensions("image/gif", &png), None);
    assert_eq!(telegram_image_dimensions("image/png", b"not png"), None);

    let mut jpeg = vec![0xff, 0xd8, 0x00, 0xff, 0xff, 0xd8, 0xff, 0xd9, 0xff, 0x01];
    jpeg.extend_from_slice(&[0xff, 0xd0, 0xff, 0xe0, 0x00, 0x02]);
    jpeg.extend_from_slice(&[0xff, 0xc0, 0x00, 0x11, 0x08, 0x01, 0xe0, 0x02, 0x80]);
    jpeg.extend_from_slice(&[0; 10]);
    assert_eq!(
        telegram_image_dimensions("image/jpeg", &jpeg),
        Some((640, 480))
    );

    assert_eq!(
        telegram_image_dimensions("image/jpeg", &[0xff, 0xd8, 0xff]),
        None
    );
    assert_eq!(
        telegram_image_dimensions("image/jpeg", &[0xff, 0xd8, 0xff, 0xe0, 0x00, 0x01]),
        None
    );
    assert_eq!(
        telegram_image_dimensions("image/jpeg", &[0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10]),
        None
    );
    assert_eq!(
        telegram_image_dimensions("image/jpeg", &[0xff, 0xd8, 0xff, 0xe0, 0x00, 0x02]),
        None
    );
}

#[tokio::test]
async fn typed_send_receipt_uses_actual_telegram_ids_and_full_address() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let outcome = ch
        .send_typed_with_receipt(&typed_text("idem", "hello".into()))
        .await
        .unwrap();
    assert_eq!(outcome.error, None);
    assert_eq!(outcome.receipt.platform_message_ids, ["321"]);
    assert_eq!(outcome.receipt.parts[0].edit_token.as_deref(), Some("321"));
    assert_eq!(
        outcome.receipt.parts[0].delete_token.as_deref(),
        Some("321")
    );
    assert_eq!(
        outcome
            .receipt
            .conversation
            .as_ref()
            .unwrap()
            .topic_id
            .as_deref(),
        Some("topic")
    );
    assert_eq!(outcome.receipt.parts[0].reply_to_id.as_deref(), Some("5"));
}

#[tokio::test]
async fn configured_telegram_adapter_passes_mock_wire_conformance() {
    use tinychannels_conformance::{ConformanceFeature, ExpectedScope, check_all_features};

    let server = mock_api().await;
    for endpoint in [
        "sendMessage",
        "sendLocation",
        "sendVenue",
        "sendContact",
        "sendPoll",
        "sendPhoto",
        "sendVideo",
        "sendAudio",
        "sendDocument",
        "sendSticker",
    ] {
        mount_post(
            &server,
            endpoint,
            ok_with(serde_json::json!({ "message_id": 321 })),
        )
        .await;
    }
    mount_post(
        &server,
        "sendVoice",
        ok_with(serde_json::json!({"message_id": 321})),
    )
    .await;
    for endpoint in ["setMessageReaction", "sendChatAction", "deleteMessage"] {
        mount_post(&server, endpoint, ok_with(serde_json::json!(true))).await;
    }
    mount_post(
        &server,
        "editMessageText",
        ok_with(serde_json::json!({ "message_id": 321 })),
    )
    .await;
    mount_post(
        &server,
        "getUpdates",
        ok_with(serde_json::Value::Array(telegram_conformance_updates())),
    )
    .await;
    mount_post(
        &server,
        "getFile",
        ok_with(serde_json::json!({"file_path":"voice/conformance.oga","file_size":4})),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("/file/bot{TOKEN}/voice/conformance.oga")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "audio/ogg")
                .set_body_bytes(b"OggS"),
        )
        .mount(&server)
        .await;
    let config = tinychannels_bus::ChannelsConfig {
        telegram: Some(tinychannels_bus::config::TelegramConfig {
            bot_token: TOKEN.into(),
            chat_id: Some("100".into()),
            allowed_users: vec!["*".into()],
            stream_mode: crate::config::StreamMode::Partial,
            draft_update_interval_ms: 1_000,
            silent_streaming: false,
            mention_only: false,
        }),
        ..Default::default()
    };
    let events = Arc::new(TelegramConformanceEvents::default());
    let voice = Arc::new(TelegramConformanceVoice::default());
    let transcriber = Arc::new(TelegramConformanceTranscriber);
    let host: Arc<dyn crate::host::ChannelHost> = crate::host::ChannelHostBuilder::new()
        .events(events.clone())
        .media_repository(Arc::new(TelegramConformanceMedia))
        .voice_media_store(voice.clone())
        .transcriber(transcriber.clone())
        .synthesizer(Arc::new(TelegramConformanceSynthesizer))
        .voice_codec(Arc::new(TelegramConformanceCodec))
        .build();
    let mut channel = crate::build_telegram_channel(&config, &host, &crate::DefaultHttpClients)
        .expect("configured factory builds Telegram");
    Arc::get_mut(&mut channel)
        .expect("new factory adapter is uniquely owned")
        .set_api_base_for_tests(server.uri());
    let scope = ExpectedScope {
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("telegram-conformance".into()),
        },
        conversation: tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Dm,
            id: "100".into(),
            ..Default::default()
        },
        reply_to_id: None,
        thread_id: None,
    };
    let adapter = tinychannels_runtime::adapters::LocalChannelAdapter::new(
        tinychannels_bus::channel::ChannelDescriptor {
            id: scope.channel.id.clone(),
            display_name: "Telegram test account".into(),
            account_id: scope.channel.account_id.clone(),
            ..Default::default()
        },
        channel.capabilities(),
        Arc::new(TelegramConformanceSink(channel.clone())),
    );
    let fixture = TelegramWireFixture {
        adapter,
        channel,
        server,
        observations: std::sync::Mutex::new(Vec::new()),
        request_cursor: std::sync::Mutex::new(0),
        scope,
        events,
        voice,
        transcriber,
    };
    let results = check_all_features(&fixture).await;
    assert_eq!(results.len(), ConformanceFeature::ALL.len());
    let failures = results
        .into_iter()
        .filter_map(|(feature, result)| result.err().map(|error| format!("{feature:?}: {error}")))
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "Telegram conformance failures: {failures:#?}"
    );
}

#[tokio::test]
async fn typed_send_receipt_keeps_ordered_multiple_text_parts() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 654 })),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let mut message = typed_text("multi-text", "first".into());
    message.conversation_ref = None;
    message.conversation_id = "600".into();
    message.thread_id = Some("18".into());
    message
        .parts
        .push(tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "second".into(),
                markdown_dialect: None,
            },
        });

    let outcome = ch.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(
        json_bodies(&server, "sendMessage").await[0]["text"],
        "first\nsecond"
    );
    assert_eq!(outcome.receipt.platform_message_ids, ["654"]);
    assert_eq!(
        outcome.receipt.idempotency_key.as_deref(),
        Some("multi-text")
    );
    assert_eq!(outcome.receipt.parts[0].edit_token.as_deref(), Some("654"));
    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(bodies[0]["chat_id"], "600");
    assert_eq!(bodies[0]["message_thread_id"], "18");
    assert_eq!(outcome.receipt.thread_id.as_deref(), Some("18"));
}

#[tokio::test]
async fn typed_send_receipt_applies_tool_call_tag_sanitization() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 655 })),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let message = typed_text(
        "sanitize",
        "before<tool_call>hidden</tool_call>[REACTION:👍|321] [IMAGE:https://example.test/image.png]".into(),
    );

    let outcome = ch.send_typed_with_receipt(&message).await.unwrap();

    assert_eq!(outcome.error, None);
    let bodies = json_bodies(&server, "sendMessage").await;
    assert_eq!(
        bodies[0]["text"],
        r"before\[REACTION:👍\|321\] \[IMAGE:https://example\.test/image\.png\]"
    );
    assert_eq!(bodies[0]["message_id"], serde_json::Value::Null);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "typed markers remain text, not operations"
    );
    assert_eq!(outcome.receipt.platform_message_ids, ["655"]);
}

#[tokio::test]
async fn typed_send_rejects_non_numeric_reply_reference_before_provider_io() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);
    let mut message = typed_text("bad-reply-id", "hello".into());
    message.reply_to_id = Some("reply:legacy".into());

    let error = ch.send_typed_with_receipt(&message).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("reply_to_id must be a numeric message ID")
    );
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[test]
#[allow(
    deprecated,
    reason = "the normalization fallback is specifically for the v21 provider adapter"
)]
fn inbound_envelope_keeps_chat_channel_and_legacy_identity_fallbacks() {
    let legacy = ChannelMessage {
        id: "telegram_-100_7".into(),
        sender: "legacy-sender".into(),
        sender_name: Some("Legacy display".into()),
        reply_target: "-100:topic-4".into(),
        content: "legacy body".into(),
        channel: "telegram".into(),
        ..Default::default()
    };
    let channel_update = serde_json::json!({
        "message": {
            "message_id": 7,
            "chat": {"type":"channel"},
            "sender_chat": {"id": 9001},
            "reply_to_message": {"message_id": 3}
        }
    });
    let channel = TelegramChannel::inbound_envelope_from_update(&channel_update, &legacy, None);
    assert_eq!(
        channel.conversation.kind,
        tinychannels_bus::ConversationKind::Channel
    );
    assert_eq!(channel.conversation.id, "-100");
    assert_eq!(channel.sender.id, "9001");
    assert_eq!(channel.reply_to_id.as_deref(), Some("3"));
    assert_eq!(channel.sender.name.as_deref(), Some("Legacy display"));

    let unknown_update = serde_json::json!({
        "message": {
            "chat": {"id": 17, "type":"unknown"},
            "from": {}
        }
    });
    let unknown = TelegramChannel::inbound_envelope_from_update(&unknown_update, &legacy, None);
    assert_eq!(
        unknown.conversation.kind,
        tinychannels_bus::ConversationKind::Unknown
    );
    assert_eq!(unknown.conversation.id, "17");
    assert_eq!(unknown.sender.id, "legacy-sender");
}

#[test]
fn voice_ingress_keeps_audio_part_beside_provider_transcript() {
    let legacy = ChannelMessage {
        id: "telegram_100_9".into(),
        sender: "alice".into(),
        reply_target: "100".into(),
        content: "recognized words".into(),
        channel: "telegram".into(),
        ..Default::default()
    };
    let update = serde_json::json!({
        "message": {
            "message_id": 9,
            "chat": {"id": 100, "type": "private"},
            "from": {"id": 8},
            "voice": {
                "file_id": "opaque-provider-file-id",
                "file_unique_id": "opaque-unique-id",
                "file_size": 16,
                "mime_type": "audio/ogg",
                "duration": 3
            }
        }
    });

    let mut voice_note = tinychannels_bus::VoiceNote {
        media: tinychannels_bus::MediaReference {
            opaque_id: Some("store-owned-voice-7".into()),
            content_type: Some("audio/ogg".into()),
            kind: tinychannels_bus::MediaKind::Audio,
            ..Default::default()
        },
        duration_ms: Some(3_000),
        ..Default::default()
    };
    voice_note.transcript = Some(tinychannels_bus::VoiceTranscript {
        text: legacy.content.clone(),
        language: None,
        state: tinychannels_bus::VoiceTranscriptState::Complete,
        source: tinychannels_bus::VoiceTranscriptSource::Provider,
    });
    let envelope =
        TelegramChannel::inbound_envelope_from_update(&update, &legacy, Some(voice_note));

    assert_eq!(envelope.text, "recognized words");
    assert_eq!(envelope.parts.len(), 2);
    assert!(matches!(
        envelope.parts[0],
        tinychannels_bus::ContentPart::Text { .. }
    ));
    let tinychannels_bus::ContentPart::VoiceNote { voice_note } = &envelope.parts[1] else {
        panic!("voice ingress must retain a typed audio part")
    };
    assert_eq!(voice_note.media.content_type.as_deref(), Some("audio/ogg"));
    assert_eq!(voice_note.duration_ms, Some(3_000));
    assert_eq!(
        voice_note
            .transcript
            .as_ref()
            .map(|transcript| transcript.text.as_str()),
        Some("recognized words")
    );
    assert_eq!(
        voice_note
            .transcript
            .as_ref()
            .map(|transcript| transcript.source),
        Some(tinychannels_bus::VoiceTranscriptSource::Provider)
    );
    assert_ne!(
        voice_note.media.url.as_deref(),
        Some("opaque-provider-file-id"),
        "provider-private file IDs are not durable media URLs"
    );
}

#[tokio::test]
async fn typed_send_keeps_delivered_chunk_ids_when_later_chunk_fails() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("sendMessage")))
        .respond_with(ok_with(serde_json::json!({ "message_id": 321 })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(400).set_body_string("rejected"),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let mut message = typed_text("idem", "x".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 50));
    message
        .parts
        .push(tinychannels_bus::OutboundContentPart::Content {
            part: tinychannels_bus::ContentPart::Text {
                text: "tail".into(),
                markdown_dialect: None,
            },
        });
    let outcome = ch.send_typed_with_receipt(&message).await.unwrap();
    assert_eq!(outcome.receipt.platform_message_ids, ["321"]);
    assert_eq!(outcome.receipt.parts.len(), 1);
    assert!(outcome.error.as_ref().is_some_and(|error| !error.retryable));
    assert!(outcome.error.as_ref().unwrap().sent_before_error);
    assert_eq!(
        outcome.error.as_ref().unwrap().continuation_message_ids,
        ["321"]
    );
    let public_error = serde_json::to_string(&outcome.error).unwrap();
    assert!(
        !public_error.contains(TOKEN),
        "bot token must not enter structured errors"
    );
}

#[tokio::test]
async fn typed_send_without_provider_message_id_is_ambiguous_and_not_retried() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok_with(serde_json::json!({}))).await;
    let ch = channel_for(&server, &["*"]);
    let outcome = ch
        .send_typed_with_receipt(&typed_text("missing-id", "hello".into()))
        .await
        .unwrap();
    let error = outcome.error.expect("missing ID cannot report success");
    assert!(
        error.sent_before_error,
        "Telegram accepted the request, so retry safety is unknown"
    );
    assert!(outcome.receipt.platform_message_ids.is_empty());
    assert!(error.message.contains("usable message ID"));
    assert!(!error.message.contains(TOKEN));
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 1);
}

#[tokio::test]
async fn typed_send_connection_refusal_is_definitely_not_sent_and_sanitized() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = format!("http://{address}");
    let outcome = ch
        .send_typed_with_receipt(&typed_text("transport-error", "hello".into()))
        .await
        .unwrap();
    let error = outcome
        .error
        .expect("transport failure must not become success");
    assert!(
        !error.sent_before_error,
        "connection refusal precedes dispatch"
    );
    assert!(error.message.contains("request failed"));
    assert!(!error.message.contains(TOKEN));
    assert!(!serde_json::to_string(&error).unwrap().contains(TOKEN));
}

#[tokio::test]
async fn accepted_request_disconnect_remains_ambiguous() {
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or_default();
            if bytes.len() >= header_end + 4 + content_length {
                return bytes;
            }
        }
        bytes
    });
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = format!("http://{address}");

    let outcome = ch
        .send_typed_with_receipt(&typed_text("disconnected", "hello".into()))
        .await
        .unwrap();
    let request = accepted.await.unwrap();
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap();
    let body = &request[header_end + 4..];
    assert_eq!(body.len(), content_length);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(body).unwrap()["text"],
        "hello"
    );
    let error = outcome.error.expect("server accepted then disconnected");
    assert!(error.sent_before_error);
    assert!(outcome.receipt.platform_message_ids.is_empty());
}

#[tokio::test]
async fn fallback_connection_refusal_after_markdown_rejection_is_not_sent() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let first_request = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or_default();
            if bytes.len() >= header_end + 4 + content_length {
                let body = br#"{"description":"Bad Request: can't parse entities"}"#;
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.write_all(body).await.unwrap();
                return bytes;
            }
        }
        bytes
    });
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = format!("http://{address}");

    let outcome = ch
        .send_typed_with_receipt(&typed_text("fallback-refused", "hello".into()))
        .await
        .unwrap();
    let request = first_request.await.unwrap();
    assert!(request.windows(10).any(|window| window == b"parse_mode"));
    let error = outcome.error.expect("fallback connection is refused");
    assert!(!error.sent_before_error);
    assert!(outcome.receipt.platform_message_ids.is_empty());
}

#[tokio::test]
async fn update_draft_edits_message_and_records_edit_time() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);

    ch.update_draft("100:3", "12", "partial").await.unwrap();

    assert_eq!(
        json_bodies(&server, "editMessageText").await,
        vec![serde_json::json!({ "chat_id": "100", "message_id": 12, "text": "partial" })]
    );
    assert!(ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn update_draft_truncates_oversized_text_to_limit() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);
    let text = "é".repeat(TELEGRAM_MAX_MESSAGE_LENGTH);

    ch.update_draft("100", "12", &text).await.unwrap();

    let sent = json_bodies(&server, "editMessageText").await[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(sent.len() <= TELEGRAM_MAX_MESSAGE_LENGTH);
    assert_eq!(
        sent.len(),
        TELEGRAM_MAX_MESSAGE_LENGTH,
        "two-byte chars fill the limit"
    );
}

#[tokio::test]
async fn update_draft_rejection_is_swallowed_and_not_recorded() {
    let server = mock_api().await;
    mount_post(
        &server,
        "editMessageText",
        ResponseTemplate::new(400).set_body_string("message is not modified"),
    )
    .await;
    let ch = streaming(&server);

    ch.update_draft("100", "12", "same").await.unwrap();
    assert!(!ch.last_draft_edit.lock().contains_key("100"));
}

#[tokio::test]
async fn update_draft_invalid_message_id_skips_network() {
    let server = mock_api().await;
    let ch = streaming(&server);
    ch.update_draft("100", "abc", "text").await.unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn finalize_draft_edits_with_markdown_and_clears_rate_limit() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);
    ch.last_draft_edit
        .lock()
        .insert("100".into(), std::time::Instant::now());

    ch.finalize_draft("100", "8", "done <tool_call>{}</tool_call>", None)
        .await
        .unwrap();

    let bodies = json_bodies(&server, "editMessageText").await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["parse_mode"], "MarkdownV2");
    assert_eq!(bodies[0]["text"], "done");
    assert!(ch.last_draft_edit.lock().is_empty());
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn finalize_draft_retries_plain_edit_when_markdown_rejected() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path(api_path("editMessageText")))
        .and(body_partial_json(
            serde_json::json!({ "parse_mode": "MarkdownV2" }),
        ))
        .respond_with(ResponseTemplate::new(400))
        .with_priority(1)
        .mount(&server)
        .await;
    mount_post(&server, "editMessageText", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100", "8", "final *text", None)
        .await
        .unwrap();

    let bodies = json_bodies(&server, "editMessageText").await;
    assert_eq!(bodies.len(), 2);
    assert!(bodies[1].get("parse_mode").is_none());
    assert!(json_bodies(&server, "sendMessage").await.is_empty());
}

#[tokio::test]
async fn finalize_draft_falls_back_to_new_message_when_edits_fail() {
    let server = mock_api().await;
    mount_post(&server, "editMessageText", ResponseTemplate::new(400)).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100:4", "8", "final", Some("77"))
        .await
        .unwrap();

    assert_eq!(json_bodies(&server, "editMessageText").await.len(), 2);
    let sent = json_bodies(&server, "sendMessage").await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["text"], "final");
    assert_eq!(sent[0]["message_thread_id"], "4");
    assert_eq!(sent[0]["reply_to_message_id"], 77);
}

#[tokio::test]
async fn finalize_draft_short_text_with_invalid_id_sends_new_message() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);

    ch.finalize_draft("100", "nope", "final", None)
        .await
        .unwrap();

    assert!(json_bodies(&server, "editMessageText").await.is_empty());
    assert_eq!(sent_texts(&server).await, vec!["final".to_string()]);
}

#[tokio::test]
async fn finalize_draft_oversized_text_deletes_draft_and_sends_chunks() {
    let server = mock_api().await;
    mount_post(&server, "deleteMessage", ok_with(serde_json::json!(true))).await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);
    let text = "y".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 1);

    ch.finalize_draft("100", "8", &text, None).await.unwrap();

    assert_eq!(
        json_bodies(&server, "deleteMessage").await,
        vec![serde_json::json!({ "chat_id": "100", "message_id": 8 })]
    );
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

#[tokio::test]
async fn finalize_draft_oversized_text_with_invalid_id_sends_chunks_only() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = streaming(&server);
    let text = "y".repeat(TELEGRAM_MAX_MESSAGE_LENGTH + 1);

    ch.finalize_draft("100", "bad", &text, None).await.unwrap();

    assert!(json_bodies(&server, "deleteMessage").await.is_empty());
    assert_eq!(json_bodies(&server, "sendMessage").await.len(), 2);
}

#[tokio::test]
async fn legacy_reaction_marker_is_plain_text_not_a_telegram_operation() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);

    Channel::send(&ch, &SendMessage::new("[REACTION:🔥] hello", "100"))
        .await
        .unwrap();

    assert!(json_bodies(&server, "setMessageReaction").await.is_empty());
    assert_eq!(sent_texts(&server).await, ["\\[REACTION:🔥\\] hello"]);
}

#[tokio::test]
async fn callback_ingress_acknowledges_but_drops_disallowed_identity() {
    let server = mock_api().await;
    mount_post(&server, "answerCallbackQuery", ok()).await;
    mount_get_updates_once(
        &server,
        1,
        updates(serde_json::json!([
            {
                "update_id": 40,
                "callback_query": {
                    "id": "spoofed-first-name", "data": "action-a",
                    "from": {"id": 31, "first_name": "alice"},
                    "message": {"message_id": 80, "chat": {"id": 100, "type": "private"}}
                }
            },
            {
                "update_id": 41,
                "callback_query": {
                    "id": "allowed-id", "data": "opaque-action",
                    "from": {"id": 42, "username": "outsider"},
                    "message": {"message_id": 81, "message_thread_id": 7,
                        "chat": {"id": -100, "type": "supergroup"}}
                }
            },
            {
                "update_id": 42,
                "callback_query": {
                    "id": "allowed-username", "data": "other-action",
                    "from": {"id": 43, "username": "alice"},
                    "message": {"message_id": 84, "chat": {"id": 101, "type": "private"}}
                }
            },
            {
                "update_id": 43,
                "message": {
                    "message_id": 82, "chat": {"id": 100, "type": "private"},
                    "from": {"id": 42, "username": "outsider"}, "text": "stop listener"
                }
            },
            {
                "update_id": 44,
                "message": {
                    "message_id": 83, "chat": {"id": 100, "type": "private"},
                    "from": {"id": 42, "username": "outsider"}, "text": "close listener"
                }
            }
        ])),
    )
    .await;
    mount_post(&server, "sendMessage", ok()).await;
    let events = Arc::new(RecordingEvents::default());
    let channel = Arc::new(fast_backoff(
        channel_for(&server, &["alice", "42"]).with_events(events.clone()),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn({
        let channel = channel.clone();
        async move { channel.listen(tx).await }
    });
    tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("allowed message terminates listener")
        .expect("message delivered");
    drop(rx);
    tokio::time::timeout(GUARD, listener)
        .await
        .expect("listener exits after receiver closes")
        .unwrap()
        .unwrap();

    let callbacks = events
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, kind, _)| kind == "button_callback")
        .map(|(_, _, payload)| payload.clone())
        .collect::<Vec<_>>();
    assert_eq!(callbacks.len(), 2);
    assert_eq!(callbacks[0]["type"], "button_callback");
    assert_eq!(callbacks[0]["queryId"], "allowed-id");
    assert_eq!(callbacks[0]["actionId"], "opaque-action");
    assert_eq!(callbacks[0]["actor"]["id"], "42");
    assert_eq!(callbacks[0]["conversation"]["id"], "-100");
    assert_eq!(callbacks[0]["conversation"]["topicId"], "7");
    assert_eq!(callbacks[0]["providerMessageId"], "81");
    assert_eq!(callbacks[1]["queryId"], "allowed-username");
    assert_eq!(callbacks[1]["actor"]["id"], "43");
    let acknowledgments = json_bodies(&server, "answerCallbackQuery").await;
    assert_eq!(acknowledgments.len(), 3);
    assert_eq!(
        acknowledgments
            .iter()
            .map(|body| body["callback_query_id"].as_str().unwrap())
            .collect::<std::collections::HashSet<_>>(),
        ["spoofed-first-name", "allowed-id", "allowed-username"]
            .into_iter()
            .collect()
    );
}

#[test]
fn callback_event_keeps_authenticated_actor_message_and_topic_identity() {
    let action_id = tinychannels_bus::channel::approval_action_id("request-42", 1).unwrap();
    let update = serde_json::json!({
        "update_id": 12,
        "callback_query": {
            "id": "query-5",
            "data": action_id,
            "from": { "id": 31, "username": "alice", "is_bot": false },
            "message": {
                "message_id": 81,
                "message_thread_id": 6,
                "chat": { "id": -100, "type": "supergroup" }
            }
        }
    });

    let event = TelegramChannel::callback_event(&update).unwrap();
    assert_eq!(event.channel.id, "telegram");
    assert_eq!(event.conversation.as_ref().unwrap().id, "-100");
    assert_eq!(
        event.conversation.as_ref().unwrap().topic_id.as_deref(),
        Some("6")
    );
    assert!(
        matches!(event.event, tinychannels_bus::ChannelEventKind::ButtonCallback {
        ref query_id,
        ref action_id,
        provider_message_id: Some(ref message_id),
        ref actor,
        } if query_id == "query-5" && action_id == "approval-v1:request-42:1" && message_id == "81" && actor.id == "31" && actor.name.as_deref() == Some("alice"))
    );
    assert_eq!(
        tinychannels_bus::channel::parse_approval_action_id("approval-v1:request-42:1"),
        Some(("request-42", 1)),
        "the host can correlate the opaque callback with its pending choice without provider policy"
    );
}

#[test]
fn callback_event_rejects_oversized_or_unscoped_callback_data() {
    let oversized = serde_json::json!({
        "callback_query": {
            "id": "query",
            "data": "x".repeat(65),
            "from": { "id": 1 },
            "message": { "message_id": 2, "chat": { "id": 3, "type": "private" } }
        }
    });
    assert!(TelegramChannel::callback_event(&oversized).is_none());
    let inline = serde_json::json!({
        "callback_query": {
            "id": "query", "data": "action", "from": { "id": 1 },
            "inline_message_id": "inline"
        }
    });
    assert!(TelegramChannel::callback_event(&inline).is_none());
}

#[tokio::test]
async fn send_bare_existing_path_remains_plain_text() {
    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok()).await;
    let ch = channel_for(&server, &["*"]);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("report.pdf");
    std::fs::write(&file, b"%PDF").unwrap();

    Channel::send(&ch, &SendMessage::new(file.to_str().unwrap(), "100"))
        .await
        .unwrap();

    assert_eq!(json_bodies(&server, "sendDocument").await.len(), 0);
    let sent = json_bodies(&server, "sendMessage").await;
    assert_eq!(sent.len(), 1);
    assert!(sent[0]["text"].is_string());
}

#[tokio::test]
async fn health_check_reflects_get_me_status() {
    let server = mock_api().await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "username": "b" })))
        .mount(&server)
        .await;
    let ch = channel_for(&server, &["*"]);
    assert!(ch.health_check().await);
    assert_eq!(
        ChannelAdapter::status(&ch)
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("healthy"),
        "adapter status must report the actual getMe result"
    );

    let failing = mock_api().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&failing)
        .await;
    let failing_channel = channel_for(&failing, &["*"]);
    assert!(!failing_channel.health_check().await);
    assert_eq!(
        ChannelAdapter::status(&failing_channel)
            .await
            .unwrap()
            .health_state
            .as_deref(),
        Some("unhealthy")
    );

    let mut dead = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    dead.api_base = dead_base();
    assert!(!dead.health_check().await);
}

#[tokio::test]
async fn start_typing_sends_immediately_and_reuses_task_for_same_recipient() {
    let server = mock_api().await;
    mount_post(&server, "sendChatAction", ok_with(serde_json::json!(true))).await;
    let ch = channel_for(&server, &["*"]);

    ch.start_typing("100:6").await.unwrap();
    // The background refresher's first iteration posts right away; wait for it.
    tokio::time::timeout(GUARD, async {
        while json_bodies(&server, "sendChatAction").await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typing refresher posted");
    let first_task_id = ch.typing_handle.lock().as_ref().unwrap().handle.id();

    ch.start_typing("100:6").await.unwrap();
    assert_eq!(
        ch.typing_handle.lock().as_ref().unwrap().handle.id(),
        first_task_id,
        "same recipient keeps the running refresher"
    );

    let bodies = json_bodies(&server, "sendChatAction").await;
    assert!(bodies.iter().all(|b| b["chat_id"] == "100"
        && b["action"] == "typing"
        && b["message_thread_id"] == "6"));

    ch.stop_typing("100:6").await.unwrap();
    assert!(ch.typing_handle.lock().is_none());
}

#[tokio::test]
async fn typing_refresh_reports_acceptance_rejection_and_transport_error() {
    let server = mock_api().await;
    Mock::given(method("POST"))
        .and(path("/accept"))
        .respond_with(ok_with(serde_json::json!(true)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/reject"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": false })))
        .mount(&server)
        .await;
    let client = reqwest::Client::new();
    let body = TelegramChannel::typing_body_for_recipient("100");

    let accept = format!("{}/accept", server.uri());
    let reject = format!("{}/reject", server.uri());
    let dead = format!("{}/x", dead_base());
    assert!(TelegramChannel::refresh_typing_once(&client, &accept, &body, "100").await);
    assert!(!TelegramChannel::refresh_typing_once(&client, &reject, &body, "100").await);
    assert!(!TelegramChannel::refresh_typing_once(&client, &dead, &body, "100").await);

    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 2);
    let sent: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(
        sent,
        serde_json::json!({ "chat_id": "100", "action": "typing" })
    );
}

#[test]
fn telegram_advertises_reaction_support() {
    let ch = TelegramChannel::new("tok".into(), vec!["*".into()], false);
    assert!(ch.supports_reactions());
    let caps = ch.capabilities();
    assert!(caps.actions.supported.contains(&"typing".into()));
    assert!(caps.actions.supported.contains(&"edit".into()));
    assert!(caps.actions.supported.contains(&"delete".into()));
    assert!(caps.reactions);
    assert!(caps.events.supported.contains(&"reaction_added".into()));
    assert!(caps.events.supported.contains(&"reaction_removed".into()));
    assert!(caps.events.supported.contains(&"button_callback".into()));
    assert!(!caps.events.supported.contains(&"read_receipt".into()));
    assert!(caps.content.location.outbound);
    assert!(caps.content.contact.outbound);
    assert!(caps.content.buttons.outbound);
    assert!(caps.content.poll.outbound);
    assert!(caps.content.presentation.outbound);
    assert!(!caps.content.native.outbound);
}

fn fast_backoff(mut ch: TelegramChannel) -> TelegramChannel {
    ch.poll_error_backoff = Duration::from_millis(1);
    ch.poll_conflict_backoff = Duration::from_millis(1);
    ch
}

/// Mount a one-shot `getUpdates` response, consumed in mount order.
async fn mount_get_updates_once(server: &MockServer, priority: u8, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path(api_path("getUpdates")))
        .respond_with(response)
        .up_to_n_times(1)
        .with_priority(priority)
        .mount(server)
        .await;
}

fn updates(result: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": true, "result": result }))
}

#[test]
fn telegram_v2_ingress_preserves_plain_and_forum_addresses_without_legacy_topic_guessing() {
    let legacy = ChannelMessage {
        id: "telegram_100_5".into(),
        sender: "alice".into(),
        reply_target: "100:88".into(),
        content: "message".into(),
        channel: "telegram".into(),
        timestamp: 12,
        thread_ts: Some("5".into()),
        ..Default::default()
    };
    let plain = TelegramChannel::inbound_envelope_from_update(
        &serde_json::json!({"message":{"message_id":5,"date":12,"chat":{"id":100,"type":"private"},"from":{"id":7}}}),
        &legacy,
        None,
    );
    assert_eq!(plain.message_id, "telegram_100_5");
    assert_eq!(
        plain.conversation.kind,
        tinychannels_bus::ConversationKind::Dm
    );
    assert_eq!(plain.conversation.id, "100");
    assert_eq!(plain.conversation.topic_id, None);
    assert_eq!(plain.conversation.thread_id, None);
    assert_eq!(plain.sender.id, "7");
    assert_eq!(plain.raw.as_ref().unwrap()["providerMessageId"], "5");

    let forum = TelegramChannel::inbound_envelope_from_update(
        &serde_json::json!({"message":{"message_id":5,"date":12,"message_thread_id":88,"chat":{"id":100,"type":"supergroup"},"from":{"id":7}}}),
        &legacy,
        None,
    );
    assert_eq!(
        forum.conversation.kind,
        tinychannels_bus::ConversationKind::Group
    );
    assert_eq!(forum.conversation.id, "100");
    assert_eq!(forum.conversation.topic_id.as_deref(), Some("88"));
    assert_eq!(forum.conversation.thread_id, None);
}

#[tokio::test]
async fn listen_recovers_from_poll_errors_and_dispatches_updates() {
    let server = mock_api().await;
    // Poll sequence: unparseable body, generic API error, 409 without webhook,
    // 409 webhook conflict that deleteWebhook clears, then a real batch.
    mount_get_updates_once(
        &server,
        1,
        ResponseTemplate::new(200).set_body_string("<html>"),
    )
    .await;
    mount_get_updates_once(
        &server,
        2,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 500, "description": "Internal"
        })),
    )
    .await;
    mount_get_updates_once(
        &server,
        3,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "Conflict: terminated by other getUpdates request"
        })),
    )
    .await;
    mount_get_updates_once(
        &server,
        4,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "Conflict: can't use getUpdates method while webhook is active"
        })),
    )
    .await;
    mount_post(&server, "deleteWebhook", ok_with(serde_json::json!(true))).await;
    mount_get_updates_once(
        &server,
        5,
        updates(serde_json::json!([
            {
                "update_id": 11,
                "message": {
                    "message_id": 5, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 8, "username": "mallory" }, "text": "let me in"
                }
            },
            {
                "update_id": 12,
                "message": {
                    "message_id": 5, "message_thread_id": 88,
                    "chat": { "id": 100, "type": "supergroup" },
                    "from": { "id": 7, "username": "alice" }, "text": "original hello"
                }
            },
            {
                "update_id": 13,
                "edited_message": {
                    "message_id": 5, "edit_date": 1234,
                    "message_thread_id": 88,
                    "chat": { "id": 100, "type": "supergroup" },
                    "from": { "id": 7, "username": "alice" }, "text": "edited hello"
                }
            },
            {
                "update_id": 14,
                "edited_message": {
                    "message_id": 5, "edit_date": 1235,
                    "chat": { "id": 100, "type": "supergroup" },
                    "from": { "id": 8, "username": "mallory" }, "text": "unauthorized edit"
                }
            },
            {
                "update_id": 15,
                "message_reaction": {
                    "chat": { "id": 100, "type": "supergroup" }, "message_id": 5, "message_thread_id": 88,
                    "user": { "id": 7, "username": "alice" },
                    "new_reaction": [{ "type": "emoji", "emoji": "❤" }]
                }
            },
        ])),
    )
    .await;
    // Next poll re-delivers 12 (deduped) and a new message that the closed
    // receiver can no longer take, which ends the loop.
    mount_get_updates_once(
        &server,
        6,
        updates(serde_json::json!([
            {
                "update_id": 12,
                "message": {
                    "message_id": 6, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "hello bot"
                }
            },
            {
                "update_id": 15,
                "message": {
                    "message_id": 9, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "second"
                }
            }
        ])),
    )
    .await;
    // Any later poll delivers yet another update, so the loop ends on the
    // closed receiver no matter how far it ran ahead of the test.
    Mock::given(method("POST"))
        .and(path(api_path("getUpdates")))
        .respond_with(updates(serde_json::json!([
            {
                "update_id": 16,
                "message": {
                    "message_id": 10, "chat": { "id": 100, "type": "private" },
                    "from": { "id": 7, "username": "alice" }, "text": "third"
                }
            }
        ])))
        .with_priority(10)
        .mount(&server)
        .await;
    mount_post(&server, "sendMessage", ok()).await;

    let events = Arc::new(RecordingEvents::default());
    let ch = Arc::new(fast_backoff(
        channel_for(&server, &["alice"]).with_events(events.clone()),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn({
        let ch = ch.clone();
        async move { ch.listen(tx).await }
    });

    let first = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("message dispatched")
        .expect("channel open");
    assert_eq!(first.content, "original hello");
    assert_eq!(first.sender, "alice");
    drop(rx);

    tokio::time::timeout(GUARD, listener)
        .await
        .expect("listen returns once receiver is gone")
        .unwrap()
        .unwrap();

    let recorded = events.events.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].1, "message_updated");
    assert_eq!(recorded[0].2["type"], "message_updated");
    assert_eq!(recorded[0].2["sourceEventId"], "13");
    assert_eq!(recorded[0].2["conversation"]["id"], "100");
    assert_eq!(recorded[0].2["messageId"], "telegram_100_5");
    assert_eq!(recorded[0].2["providerMessageId"], "5");
    assert_eq!(recorded[0].2["conversation"]["kind"], "group");
    assert_eq!(recorded[0].2["conversation"]["topicId"], "88");
    assert_eq!(recorded[0].2["parts"][0]["text"], "edited hello");
    assert_eq!(recorded[1].1, "reaction_added");
    assert_eq!(recorded[1].2["type"], "reaction_added");
    assert_eq!(recorded[1].2["sourceEventId"], "15");
    assert_eq!(
        recorded[1].2["eventId"],
        "telegram:update:15:reaction:added:100:5:❤"
    );
    assert_eq!(recorded[1].2["messageId"], "telegram_100_5");
    assert_eq!(recorded[1].2["providerMessageId"], "5");
    assert_eq!(recorded[1].2["conversation"]["id"], "100");
    assert_eq!(recorded[1].2["conversation"]["topicId"], "88");
    assert_eq!(recorded[1].2["actor"]["id"], "7");
    assert_eq!(recorded[1].2["reaction"]["value"], "❤");
    assert_eq!(json_bodies(&server, "deleteWebhook").await.len(), 1);
    // The unauthorized sender got the approval prompt.
    let prompts = sent_texts(&server).await;
    assert_eq!(prompts.len(), 1);
    assert!(prompts[0].contains("operator approval"), "{prompts:?}");
    // Offsets advance past each delivered batch.
    let polls = json_bodies(&server, "getUpdates").await;
    assert!(polls.len() >= 6, "{polls:?}");
    assert_eq!(polls[0]["offset"], 0);
    assert!(polls.iter().any(|poll| poll["offset"] == 16));
}

#[derive(Default)]
struct RejectingInboundEvents(AtomicUsize);

#[async_trait::async_trait]
impl crate::host::EventSink for RejectingInboundEvents {
    async fn publish(
        &self,
        _domain: &str,
        _kind: &str,
        _payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish_ingress(
        &self,
        ingress: tinychannels_bus::ChannelIngress,
    ) -> anyhow::Result<bool> {
        if matches!(ingress, tinychannels_bus::ChannelIngress::Inbound(_)) {
            self.0.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("fixture ingress delivery failed");
        }
        Ok(true)
    }
}

#[tokio::test]
async fn listen_does_not_duplicate_inbound_after_ordered_sink_failure() {
    let server = mock_api().await;
    mount_get_updates_once(
        &server,
        1,
        updates(serde_json::json!([{
            "update_id": 21,
            "message": {
                "message_id": 7,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 8, "username": "alice"},
                "text": "one delivery attempt"
            }
        }])),
    )
    .await;
    let events = Arc::new(RejectingInboundEvents::default());
    let channel = Arc::new(fast_backoff(
        channel_for(&server, &["alice"]).with_events(events.clone()),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = tokio::spawn({
        let channel = channel.clone();
        async move { channel.listen(tx).await }
    });

    let delivered_on_legacy_path = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("listener must finish after the ingress callback fails");
    assert!(
        delivered_on_legacy_path.is_none(),
        "an ambiguous callback failure must not submit the same update through the legacy queue"
    );
    let result = tokio::time::timeout(GUARD, listener)
        .await
        .expect("listener must return after callback failure")
        .unwrap();
    assert!(result.is_err());
    assert_eq!(events.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn listen_backs_off_when_webhook_delete_fails_and_on_transport_error() {
    let server = mock_api().await;
    mount_get_updates_once(
        &server,
        1,
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": false, "error_code": 409, "description": "webhook is active"
        })),
    )
    .await;
    mount_post(&server, "deleteWebhook", ResponseTemplate::new(500)).await;
    mount_get_updates_once(
        &server,
        2,
        updates(serde_json::json!([
            {
                "update_id": 1,
                "message": {
                    "message_id": 2, "chat": { "id": 5, "type": "group" },
                    "from": { "id": 3, "username": "alice" }, "text": "hey @my_bot ping"
                }
            }
        ])),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(api_path("getMe")))
        .respond_with(ok_with(serde_json::json!({ "username": "my_bot" })))
        .mount(&server)
        .await;

    let mut ch = fast_backoff(channel_for(&server, &["alice"]));
    ch.mention_only = true;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let ch = Arc::new(ch);
    let listener = tokio::spawn({
        let ch = ch.clone();
        async move { ch.listen(tx).await }
    });

    let msg = tokio::time::timeout(GUARD, rx.recv())
        .await
        .expect("message dispatched")
        .expect("channel open");
    assert_eq!(
        msg.content, "hey ping",
        "mention stripped in mention-only group"
    );
    assert_eq!(json_bodies(&server, "deleteWebhook").await.len(), 1);
    listener.abort();

    // Transport failures back off and retry rather than ending the loop.
    let mut dead = fast_backoff(TelegramChannel::new("tok".into(), vec!["*".into()], true));
    dead.api_base = dead_base();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let outcome = tokio::time::timeout(Duration::from_millis(300), dead.listen(tx)).await;
    assert!(
        outcome.is_err(),
        "listen keeps retrying on transport errors"
    );
    assert!(dead.bot_username.lock().is_none());
}

#[tokio::test]
async fn native_edit_rejects_rich_parts_and_mismatched_receipt_scope_before_io() {
    let server = mock_api().await;
    mount_post(
        &server,
        "editMessageText",
        ok_with(serde_json::json!({"message_id": 7})),
    )
    .await;
    let channel = channel_for(&server, &["*"]);
    let channel_ref = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: None,
    };
    let conversation = tinychannels_bus::ConversationRef {
        kind: tinychannels_bus::ConversationKind::Group,
        id: "100".into(),
        thread_id: Some("4".into()),
        topic_id: Some("4".into()),
        ..Default::default()
    };
    let receipt = tinychannels_bus::channel::MessageReceipt {
        channel: Some(channel_ref.clone()),
        conversation: Some(conversation.clone()),
        primary_platform_message_id: Some("7".into()),
        platform_message_ids: vec!["7".into()],
        ..Default::default()
    };
    let rich_intent = tinychannels_bus::ChannelOutboundIntent {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: Some(channel_ref.clone()),
        conversation_ref: Some(conversation.clone()),
        payload: tinychannels_bus::OutboundPayload::Parts {
            parts: vec![
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Text {
                        text: "keep this text".into(),
                        markdown_dialect: None,
                    },
                },
                tinychannels_bus::OutboundContentPart::Content {
                    part: tinychannels_bus::ContentPart::Buttons {
                        rows: vec![vec![tinychannels_bus::ButtonAction {
                            id: "action".into(),
                            label: "Continue".into(),
                            style: None,
                            disabled: false,
                        }]],
                    },
                },
            ],
        },
        ..Default::default()
    };

    let rich = crate::channel::ChannelEdit::edit(&channel, &receipt, rich_intent.clone()).await;
    assert!(
        rich.is_err(),
        "native edit must reject buttons instead of silently dropping them"
    );
    let rich_error = rich.unwrap_err();
    assert_eq!(
        rich_error.kind,
        tinychannels_bus::channel::SendErrorKind::Forbidden
    );
    assert!(!rich_error.sent_before_error);

    let mut mismatched = rich_intent;
    mismatched.payload = tinychannels_bus::OutboundPayload::Text {
        text: "edited".into(),
    };
    mismatched.conversation_id = "200".into();
    mismatched.conversation_ref = Some(tinychannels_bus::ConversationRef {
        id: "200".into(),
        ..conversation
    });
    let mismatch = crate::channel::ChannelEdit::edit(&channel, &receipt, mismatched).await;
    assert!(
        mismatch.is_err(),
        "a receipt cannot authorize editing a different conversation"
    );
    assert_eq!(
        mismatch.unwrap_err().kind,
        tinychannels_bus::channel::SendErrorKind::Forbidden
    );
    assert!(
        json_bodies(&server, "editMessageText").await.is_empty(),
        "preflight rejection must perform no HTTP request"
    );
}

#[tokio::test]
async fn native_reaction_edit_and_delete_failures_preserve_attempt_uncertainty() {
    let server = mock_api().await;
    for endpoint in ["setMessageReaction", "editMessageText", "deleteMessage"] {
        Mock::given(method("POST"))
            .and(path(api_path(endpoint)))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ok": false, "description": "rejected"})),
            )
            .mount(&server)
            .await;
    }
    let channel = channel_for(&server, &["*"]);
    let channel_ref = tinychannels_bus::ChannelRef {
        id: "telegram".into(),
        account_id: None,
    };
    let conversation = tinychannels_bus::ConversationRef {
        kind: tinychannels_bus::ConversationKind::Group,
        id: "100".into(),
        ..Default::default()
    };
    let receipt = tinychannels_bus::channel::MessageReceipt {
        channel: Some(channel_ref.clone()),
        conversation: Some(conversation.clone()),
        primary_platform_message_id: Some("7".into()),
        platform_message_ids: vec!["7".into()],
        ..Default::default()
    };
    let intent = tinychannels_bus::ChannelOutboundIntent {
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        channel_ref: Some(channel_ref.clone()),
        conversation_ref: Some(conversation.clone()),
        payload: tinychannels_bus::OutboundPayload::Text {
            text: "edit".into(),
        },
        ..Default::default()
    };

    let reaction = crate::channel::ChannelReaction::send_reaction(
        &channel,
        &channel_ref,
        &conversation,
        "7",
        &tinychannels_bus::channel::ChannelReaction {
            value: "👍".into(),
            custom_emoji_id: None,
        },
        false,
    )
    .await
    .unwrap_err();
    let edit = crate::channel::ChannelEdit::edit(&channel, &receipt, intent)
        .await
        .unwrap_err();
    let delete = crate::channel::ChannelDelete::delete(&channel, &receipt)
        .await
        .unwrap_err();
    for error in [reaction, edit, delete] {
        assert_eq!(
            error.kind,
            tinychannels_bus::channel::SendErrorKind::Unknown
        );
        assert!(
            error.sent_before_error,
            "the request reached Telegram before the uncertain result"
        );
        assert!(!error.message.contains("test-token"));
    }
    for endpoint in ["setMessageReaction", "editMessageText", "deleteMessage"] {
        assert_eq!(
            json_bodies(&server, endpoint).await.len(),
            1,
            "expected the native request to reach {endpoint}"
        );
    }
}

#[tokio::test]
async fn native_custom_emoji_reaction_uses_telegram_custom_emoji_type() {
    let server = mock_api().await;
    mount_post(&server, "setMessageReaction", ok()).await;
    let channel = channel_for(&server, &["*"]);
    let conversation = tinychannels_bus::channel::ConversationRef {
        id: "100".into(),
        ..Default::default()
    };

    channel
        .send_reaction_v2(
            &conversation,
            "7",
            &tinychannels_bus::channel::ChannelReaction {
                value: "ignored display text".into(),
                custom_emoji_id: Some("custom-emoji-42".into()),
            },
            false,
        )
        .await
        .unwrap();

    let bodies = json_bodies(&server, "setMessageReaction").await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0]["reaction"],
        serde_json::json!([{"type":"custom_emoji", "custom_emoji_id":"custom-emoji-42"}])
    );
}

#[tokio::test]
async fn native_typing_does_not_report_success_when_telegram_rejects_action() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendChatAction",
        ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": false})),
    )
    .await;
    let channel = channel_for(&server, &["*"]);
    let error = crate::channel::ChannelTyping::start_typing(&channel, "100:4")
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Forbidden
    );
    assert!(!error.sent_before_error);
    assert_eq!(
        json_bodies(&server, "sendChatAction").await.len(),
        2,
        "thread rejection is retried once at chat scope"
    );
}

#[tokio::test]
async fn native_typing_transport_error_preserves_attempt_uncertainty() {
    let server = mock_api().await;
    mount_post(&server, "sendChatAction", ResponseTemplate::new(500)).await;
    let channel = channel_for(&server, &["*"]);
    let error = crate::channel::ChannelTyping::start_typing(&channel, "100")
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        tinychannels_bus::channel::SendErrorKind::Unknown
    );
    assert!(error.sent_before_error);
    assert_eq!(json_bodies(&server, "sendChatAction").await.len(), 1);
}

#[test]
fn typed_error_mapping_distinguishes_preflight_partial_and_ambiguous_failures() {
    use tinychannels_bus::channel::{ChannelSendError, SendErrorKind};

    let preflight = typed_send_error(anyhow::Error::new(TelegramPreDispatchError));
    assert_eq!(preflight.kind, SendErrorKind::Forbidden);
    assert!(!preflight.retryable);
    assert!(!preflight.sent_before_error);

    let unsupported = typed_send_error(anyhow::Error::new(TelegramUnsupportedError));
    assert_eq!(unsupported.kind, SendErrorKind::BadFormat);
    assert!(unsupported.unsupported);
    assert!(!unsupported.sent_before_error);

    let partial = ChannelSendError {
        kind: SendErrorKind::RateLimited,
        message: "partial delivery".into(),
        retryable: true,
        sent_before_error: true,
        ..Default::default()
    };
    let mapped_partial = typed_send_error(anyhow::Error::new(
        crate::providers::telegram::channel_send::PartialTextSendError {
            message_ids: vec!["17".into()],
            sent_before_error: true,
            error: partial.clone(),
        },
    ));
    assert_eq!(mapped_partial, partial);

    let uncertain = typed_send_error(anyhow::anyhow!("transport disconnected"));
    assert_eq!(uncertain.kind, SendErrorKind::Unknown);
    assert!(!uncertain.retryable);
    assert!(uncertain.sent_before_error);
    assert!(!uncertain.message.contains("disconnected"));

    let interaction_preflight =
        typed_interaction_error(anyhow::Error::new(TelegramPreDispatchError), "edit");
    assert_eq!(interaction_preflight.kind, SendErrorKind::Forbidden);
    assert!(!interaction_preflight.sent_before_error);
    let interaction_uncertain =
        typed_interaction_error(anyhow::anyhow!("token-bearing URL"), "delete");
    assert_eq!(interaction_uncertain.kind, SendErrorKind::Unknown);
    assert!(interaction_uncertain.sent_before_error);
    assert!(!interaction_uncertain.message.contains("token-bearing"));
}

#[test]
fn presentation_blocks_and_vcards_keep_only_supported_safe_content() {
    let presentation = serde_json::json!([
        {"type":"heading", "text":"Status"},
        [{"type":"quote", "text":"Ready"}]
    ]);
    assert_eq!(
        presentation_content(&presentation).unwrap(),
        ("Status\nReady".into(), None)
    );

    let approval = serde_json::json!({
        "kind":"approval", "id":"request-7", "prompt":"Run this?", "choices":["Allow", "Deny"]
    });
    let (prompt, markup) = presentation_content(&approval).unwrap();
    assert_eq!(prompt, "Run this?");
    assert_eq!(
        markup.unwrap()["inline_keyboard"][1][0]["callback_data"],
        "approval-v1:request-7:1"
    );

    for invalid in [
        serde_json::json!({"title":"x", "action":"shell"}),
        serde_json::json!({"type":"button", "text":"Run"}),
        serde_json::json!({"type":7, "text":"Run"}),
        serde_json::json!({"type":"text", "text":{"value":"Run"}}),
        serde_json::json!(["   ", null]),
        serde_json::json!({"kind":"approval", "id":"r", "prompt":" ", "choices":["Allow"]}),
        serde_json::json!({"kind":"approval", "id":"r", "prompt":"Run?", "choices":[]}),
        serde_json::json!({"kind":"approval", "id":"r", "prompt":"Run?", "choices":[""]}),
        serde_json::json!({"kind":"approval", "id":"r", "prompt":"Run?", "choices":["Allow"], "extra":true}),
    ] {
        assert!(
            presentation_content(&invalid).is_err(),
            "accepted {invalid}"
        );
    }

    assert_eq!(safe_vcard_type(Some(" cell "), "VOICE"), "cell");
    assert_eq!(safe_vcard_type(Some("internet"), "VOICE"), "internet");
    assert_eq!(safe_vcard_type(Some("CELL\nitem1.TEL"), "VOICE"), "VOICE");
    assert_eq!(safe_vcard_type(None, "VOICE"), "VOICE");
    assert_eq!(vcard_escape("a\\b;c,d\ne\r"), "a\\\\b\\;c\\,d\\ne");
    assert!(response_status_ok(&serde_json::json!({"ok": true})));
    for invalid in [
        serde_json::json!({}),
        serde_json::json!({"ok": false}),
        serde_json::json!({"ok": 1}),
    ] {
        assert!(!response_status_ok(&invalid));
    }
}

fn draft_intent(reply: Option<&str>) -> tinychannels_bus::ChannelOutboundIntent {
    tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "draft-key".into(),
        channel_id: "telegram".into(),
        conversation_id: "100".into(),
        reply_to_id: reply.map(str::to_owned),
        payload: tinychannels_bus::OutboundPayload::Text {
            text: "draft body".into(),
        },
        ..Default::default()
    }
}

fn draft_receipt(
    primary: Option<&str>,
    part_id: Option<&str>,
) -> tinychannels_bus::channel::MessageReceipt {
    tinychannels_bus::channel::MessageReceipt {
        channel: Some(tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: None,
        }),
        conversation: Some(tinychannels_bus::ConversationRef {
            id: "100".into(),
            ..Default::default()
        }),
        primary_platform_message_id: primary.map(str::to_owned),
        parts: part_id
            .map(|id| {
                vec![tinychannels_bus::channel::MessageReceiptPart {
                    platform_message_id: id.into(),
                    ..Default::default()
                }]
            })
            .unwrap_or_default(),
        ..Default::default()
    }
}

async fn upsert(
    channel: &TelegramChannel,
    previous: Option<&tinychannels_bus::channel::MessageReceipt>,
    intent: tinychannels_bus::ChannelOutboundIntent,
) -> Result<tinychannels_bus::channel::MessageReceipt, crate::channel::ChannelSendError> {
    crate::channel::ChannelStreamingDraft::upsert_draft(channel, previous, intent).await
}

fn refused_channel() -> TelegramChannel {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    ch.api_base = format!("http://{address}");
    ch
}

#[tokio::test]
async fn draft_creation_reports_each_failure_with_the_right_error_kind() {
    use tinychannels_bus::channel::SendErrorKind;
    let error = upsert(&refused_channel(), None, draft_intent(None))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Transient);
    assert!(error.sent_before_error);
    assert!(!error.message.contains(TOKEN));

    let server = mock_api().await;
    let channel = channel_for(&server, &["*"]);
    let error = upsert(&channel, None, draft_intent(Some("not-a-number")))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(json_bodies(&server, "sendMessage").await.is_empty());

    mount_post(
        &server,
        "sendMessage",
        ResponseTemplate::new(200).set_body_string("not json"),
    )
    .await;
    let error = upsert(&channel, None, draft_intent(None))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Transient);
    assert!(error.message.contains("response"));

    let server = mock_api().await;
    mount_post(&server, "sendMessage", ok_with(serde_json::json!({}))).await;
    let error = upsert(&channel_for(&server, &["*"]), None, draft_intent(Some("9")))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Unknown);
    assert!(error.message.contains("omitted message ID"));
    assert_eq!(
        json_bodies(&server, "sendMessage").await[0]["reply_parameters"]["message_id"],
        9
    );
}

#[tokio::test]
async fn draft_creation_without_channel_ref_defaults_to_the_telegram_channel() {
    let server = mock_api().await;
    mount_post(
        &server,
        "sendMessage",
        ok_with(serde_json::json!({ "message_id": 55 })),
    )
    .await;
    let receipt = upsert(&channel_for(&server, &["*"]), None, draft_intent(None))
        .await
        .unwrap();
    assert_eq!(receipt.channel.as_ref().unwrap().id, "telegram");
    assert_eq!(receipt.primary_platform_message_id.as_deref(), Some("55"));
}

#[tokio::test]
async fn draft_edit_validates_receipt_ids_and_reports_wire_failures() {
    use tinychannels_bus::channel::SendErrorKind;
    let server = mock_api().await;
    let channel = channel_for(&server, &["*"]);

    let error = upsert(
        &channel,
        Some(&draft_receipt(None, None)),
        draft_intent(None),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(error.message.contains("no provider message ID"));

    let error = upsert(
        &channel,
        Some(&draft_receipt(Some("abc"), None)),
        draft_intent(None),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::BadFormat);
    assert!(error.message.contains("invalid provider message ID"));

    let error = upsert(
        &refused_channel(),
        Some(&draft_receipt(None, Some("7"))),
        draft_intent(None),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Transient);
    assert!(error.sent_before_error);

    mount_post(
        &server,
        "editMessageText",
        ResponseTemplate::new(200).set_body_string("not json"),
    )
    .await;
    let error = upsert(
        &channel,
        Some(&draft_receipt(Some("7"), None)),
        draft_intent(None),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Transient);
    assert!(error.message.contains("response"));
}

#[test]
fn telegram_declares_remote_control_approvals_and_proactive_defaults() {
    let ch = TelegramChannel::new(TOKEN.into(), vec!["*".into()], false);
    assert!(Channel::supports_remote_control(&ch));
    assert!(Channel::supports_chat_approvals(&ch));
    assert!(Channel::history_key_ignores_thread(&ch));
    assert_eq!(
        crate::channel::ChannelAdapter::proactive_target(&ch),
        Channel::proactive_target(&ch)
    );
}

#[tokio::test]
async fn adapter_start_polls_and_delivers_original_typed_telegram_ingress() {
    use crate::channel::{ChannelInboundSink, ChannelSendError};

    #[derive(Default)]
    struct RecordingSink {
        items: std::sync::Mutex<Vec<tinychannels_bus::ChannelIngress>>,
        notified: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl ChannelInboundSink for RecordingSink {
        async fn push(
            &self,
            item: tinychannels_bus::ChannelIngress,
        ) -> Result<(), ChannelSendError> {
            self.items.lock().unwrap().push(item);
            self.notified.notify_one();
            Ok(())
        }
    }

    let server = mock_api().await;
    mount_get_updates_once(
        &server,
        0,
        updates(serde_json::json!([{
            "update_id": 41,
            "message": {
                "message_id": 9,
                "message_thread_id": 73,
                "date": 1700000000,
                "chat": { "id": -10042, "type": "supergroup" },
                "from": { "id": 7, "username": "alice", "first_name": "Alice" },
                "text": "hello from a forum topic"
            }
        }])),
    )
    .await;
    let mut channel = fast_backoff(channel_for(&server, &["alice"]));
    channel.poll_error_backoff = Duration::from_millis(1);
    let channel = Arc::new(channel);
    let sink = Arc::new(RecordingSink::default());
    let start = crate::channel::ChannelAdapter::start(channel.as_ref(), sink.as_ref());
    tokio::pin!(start);
    tokio::select! {
        result = &mut start => panic!("Telegram adapter stopped before delivery: {result:?}"),
        _ = tokio::time::timeout(Duration::from_secs(2), sink.notified.notified()) => {},
    }

    let requests = server.received_requests().await.unwrap();
    assert!(requests.iter().any(|request| request.url.path() == api_path("getUpdates")));
    let items = sink.items.lock().unwrap();
    let [tinychannels_bus::ChannelIngress::Inbound(envelope)] = items.as_slice() else {
        panic!("expected one original typed inbound envelope, got {items:?}");
    };
    assert_eq!(envelope.channel.id, "telegram");
    assert_eq!(envelope.conversation.id, "-10042");
    assert_eq!(envelope.conversation.topic_id.as_deref(), Some("73"));
    assert_eq!(envelope.provider_message_id.as_deref(), Some("9"));
    assert!(matches!(envelope.parts.as_slice(), [tinychannels_bus::ContentPart::Text { text, .. }] if text == "hello from a forum topic"));
    drop(items);
    crate::channel::ChannelAdapter::stop(channel.as_ref()).await.unwrap();
}

#[tokio::test]
async fn native_reaction_removal_clears_reactions_and_surfaces_rejection() {
    let server = mock_api().await;
    mount_post(
        &server,
        "setMessageReaction",
        ok_with(serde_json::json!(true)),
    )
    .await;
    let ch = channel_for(&server, &["*"]);
    let conversation = tinychannels_bus::channel::ConversationRef {
        id: "100".into(),
        ..Default::default()
    };
    let reaction = tinychannels_bus::channel::ChannelReaction {
        value: "👍".into(),
        custom_emoji_id: None,
    };
    ch.send_reaction_v2(&conversation, "7", &reaction, true)
        .await
        .unwrap();
    assert_eq!(
        json_bodies(&server, "setMessageReaction").await[0]["reaction"],
        serde_json::json!([])
    );
    // Invalid inputs are rejected before any HTTP request.
    assert!(
        ch.send_reaction_v2(&conversation, "nope", &reaction, true)
            .await
            .is_err()
    );
    let empty = tinychannels_bus::channel::ChannelReaction {
        value: " ".into(),
        custom_emoji_id: None,
    };
    assert!(
        ch.send_reaction_v2(&conversation, "7", &empty, false)
            .await
            .is_err()
    );
    assert!(
        ch.send_reaction_v2(
            &tinychannels_bus::channel::ConversationRef::default(),
            "7",
            &reaction,
            true
        )
        .await
        .is_err()
    );
    assert_eq!(json_bodies(&server, "setMessageReaction").await.len(), 1);

    let rejecting = mock_api().await;
    mount_post(
        &rejecting,
        "setMessageReaction",
        ResponseTemplate::new(200)
            .set_body_json(serde_json::json!({"ok": false, "description": "nope"})),
    )
    .await;
    let error = channel_for(&rejecting, &["*"])
        .send_reaction_v2(&conversation, "7", &reaction, true)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("reaction removal failed"));
}
