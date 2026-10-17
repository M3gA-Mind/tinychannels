use super::*;
use std::sync::atomic::AtomicUsize;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;

fn consume_failure(counter: &AtomicUsize) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    loop {
        if remaining == 0 {
            return false;
        }
        match counter.compare_exchange_weak(
            remaining,
            remaining - 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
}

#[derive(Clone)]
struct Callback {
    events: Arc<Mutex<Vec<String>>>,
    typed_events: Arc<Mutex<Vec<tinychannels_bus::ChannelEvent>>>,
    inbound_payloads: Arc<Mutex<Vec<serde_json::Value>>>,
    blocked: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
    transcription_blocked: Arc<AtomicBool>,
    transcriptions_started: Arc<AtomicUsize>,
    transcription_started: Arc<tokio::sync::Notify>,
    transcription_gate: Arc<tokio::sync::Notify>,
    voice_scopes: Arc<Mutex<Vec<tinychannels_bus::HostVoiceScope>>>,
    voice_requests: Arc<Mutex<Vec<serde_json::Value>>>,
    projected_voice_media_ids: Arc<Mutex<Vec<Option<String>>>>,
    voice_publication_failures: Arc<AtomicUsize>,
    controller_blocked: Arc<AtomicBool>,
    controllers_started: Arc<AtomicUsize>,
    controller_started: Arc<tokio::sync::Notify>,
    controller_gate: Arc<tokio::sync::Notify>,
    persist_failures: Arc<std::sync::atomic::AtomicUsize>,
    event_failures: Arc<AtomicUsize>,
    storage: Arc<crate::storage_fixture::StorageHost>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn deliver_event(&self, event: tinychannels_bus::ChannelEvent) -> Result<()> {
        if consume_failure(&self.event_failures) {
            return Err(tinybus::Error::failed("fixture event failure"));
        }
        self.typed_events.lock().await.push(event.clone());
        self.events
            .lock()
            .await
            .push(format!("typed_event:{}", event.event_id));
        Ok(())
    }

    async fn transcribe(
        &self,
        request: tinychannels_bus::HostTranscriptionRequest,
    ) -> Result<tinychannels_bus::HostTranscriptionResult> {
        assert_eq!(request.mime_type.as_deref(), Some("audio/wav"));
        self.transcriptions_started.fetch_add(1, Ordering::SeqCst);
        self.transcription_started.notify_one();
        while self.transcription_blocked.load(Ordering::SeqCst) {
            self.transcription_gate.notified().await;
        }
        self.events
            .lock()
            .await
            .push("transcription:complete".into());
        Ok(tinychannels_bus::HostTranscriptionResult {
            text: "spoken words".into(),
            language: request.language,
            duration_secs: Some(1.25),
        })
    }

    async fn transcribe_voice(
        &self,
        request: tinychannels_bus::HostVoiceTranscriptionRequest,
    ) -> Result<tinychannels_bus::HostTranscriptionResult> {
        self.voice_scopes.lock().await.push(request.scope.clone());
        self.voice_requests.lock().await.push(serde_json::json!({"kind":"transcribe","mimeType":request.mime_type,"fileName":request.file_name,"language":request.language}));
        self.transcriptions_started.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .await
            .push("transcription:complete".into());
        Ok(tinychannels_bus::HostTranscriptionResult {
            text: "spoken words".into(),
            language: request.language,
            duration_secs: Some(1.25),
        })
    }

    async fn synthesize_speech(
        &self,
        request: tinychannels_bus::HostSpeechSynthesisRequest,
    ) -> Result<tinychannels_bus::HostSpeechSynthesisResult> {
        self.voice_scopes.lock().await.push(request.scope);
        self.voice_requests.lock().await.push(
            serde_json::json!({"kind":"synthesize","text":request.text,"format":request.format}),
        );
        Ok(tinychannels_bus::HostSpeechSynthesisResult {
            audio_base64: if request.text == "oversized-response" {
                "x".repeat(super::MAX_VOICE_CALLBACK_BYTES + 1)
            } else {
                "UklGRg==".into()
            },
            mime_type: "audio/wav".into(),
            duration_ms: Some(1_250),
        })
    }

    async fn store_voice_media(
        &self,
        request: tinychannels_bus::HostStoreVoiceMediaRequest,
    ) -> Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        self.voice_scopes.lock().await.push(request.scope);
        self.projected_voice_media_ids.lock().await.push(
            request
                .media_reference
                .as_ref()
                .and_then(|media| media.opaque_id.clone()),
        );
        self.voice_requests.lock().await.push(serde_json::json!({"kind":"store","direction":request.direction,"codec":request.codec,"durationMs":request.duration_ms,"mediaReference":request.media_reference}));
        if consume_failure(&self.voice_publication_failures) {
            return Err(tinybus::Error::failed("fixture publication denial"));
        }
        let media = request
            .media_reference
            .unwrap_or(tinychannels_bus::MediaReference {
                opaque_id: Some("voice-1".into()),
                kind: tinychannels_bus::MediaKind::Audio,
                ..Default::default()
            });
        let opaque_id = media.opaque_id.clone().unwrap_or_else(|| "unknown".into());
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media,
            accessible_url: Some(format!("https://media.example.test/authorized/{opaque_id}")),
        })
    }

    async fn read_voice_media(
        &self,
        request: tinychannels_bus::HostReadVoiceMediaRequest,
    ) -> Result<tinychannels_bus::HostReadVoiceMediaResult> {
        self.voice_scopes.lock().await.push(request.scope);
        self.voice_requests.lock().await.push(serde_json::json!({"kind":"read","opaqueId":request.opaque_id,"maxBytes":request.max_bytes}));
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: "UklGRgAAAABXQVZF".into(),
            mime_type: "audio/wav".into(),
            file_name: Some("voice.wav".into()),
        })
    }

    async fn get_voice_reply_policy(
        &self,
        request: tinychannels_bus::HostVoiceReplyPolicyRequest,
    ) -> Result<tinychannels_bus::HostVoiceReplyPolicy> {
        self.voice_scopes.lock().await.push(request.scope);
        self.voice_requests
            .lock()
            .await
            .push(serde_json::json!({"kind":"policy"}));
        Ok(tinychannels_bus::HostVoiceReplyPolicy {
            mode: tinychannels_bus::HostVoiceReplyMode::SpeakReply,
            inbound_has_voice: true,
            explicitly_requested: false,
        })
    }

    async fn publish_event(
        &self,
        publication: tinychannels_bus::HostEventPublication,
    ) -> Result<()> {
        if consume_failure(&self.event_failures) {
            return Err(tinybus::Error::failed("fixture event failure"));
        }
        self.events
            .lock()
            .await
            .push(format!("event:{}:{}", publication.domain, publication.kind));
        Ok(())
    }

    async fn dispatch_controller_backend(
        &self,
        request: tinychannels_bus::ChannelBackendRequest,
    ) -> Result<tinychannels_bus::ChannelBackendResult> {
        self.controllers_started.fetch_add(1, Ordering::SeqCst);
        self.controller_started.notify_one();
        while self.controller_blocked.load(Ordering::SeqCst) {
            self.controller_gate.notified().await;
        }
        match request {
            tinychannels_bus::ChannelBackendRequest::SendReaction { channel, .. } => {
                self.events
                    .lock()
                    .await
                    .push(format!("controller:{channel}:reaction"));
                Ok(tinychannels_bus::ChannelBackendResult::SendReaction(
                    tinychannels_bus::controllers::ChannelReactionResult {
                        success: true,
                        message: None,
                        raw: None,
                    },
                ))
            }
            _ => Err(tinybus::Error::failed(
                "unexpected controller fixture request",
            )),
        }
    }

    async fn deliver_inbound(&self, channel: String, message: serde_json::Value) -> Result<()> {
        while self.blocked.load(Ordering::SeqCst) {
            self.gate.notified().await;
        }
        self.inbound_payloads.lock().await.push(message.clone());
        self.events.lock().await.push(format!(
            "inbound:{channel}:{}",
            message["text"].as_str().unwrap()
        ));
        Ok(())
    }

    async fn report_status(
        &self,
        channel: String,
        state: String,
        _detail: Option<String>,
    ) -> Result<()> {
        self.events
            .lock()
            .await
            .push(format!("status:{channel}:{state}"));
        Ok(())
    }

    async fn persist_allowed_identity(&self, channel: String, identity: String) -> Result<()> {
        if consume_failure(&self.persist_failures) {
            return Err(tinybus::Error::failed("fixture persistence failure"));
        }
        self.events
            .lock()
            .await
            .push(format!("allowlist:{channel}:{identity}"));
        Ok(())
    }
}

async fn connected_host() -> (HostChannels, Callback, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let storage = Arc::new(crate::storage_fixture::StorageHost::serve(&server, "scope-a").await);
    let callback = Callback {
        events: Arc::new(Mutex::new(Vec::new())),
        typed_events: Arc::new(Mutex::new(Vec::new())),
        inbound_payloads: Arc::new(Mutex::new(Vec::new())),
        blocked: Arc::new(AtomicBool::new(false)),
        gate: Arc::new(tokio::sync::Notify::new()),
        transcription_blocked: Arc::new(AtomicBool::new(false)),
        transcriptions_started: Arc::new(AtomicUsize::new(0)),
        transcription_started: Arc::new(tokio::sync::Notify::new()),
        transcription_gate: Arc::new(tokio::sync::Notify::new()),
        voice_scopes: Arc::new(Mutex::new(Vec::new())),
        voice_requests: Arc::new(Mutex::new(Vec::new())),
        projected_voice_media_ids: Arc::new(Mutex::new(Vec::new())),
        voice_publication_failures: Arc::new(AtomicUsize::new(0)),
        controller_blocked: Arc::new(AtomicBool::new(false)),
        controllers_started: Arc::new(AtomicUsize::new(0)),
        controller_started: Arc::new(tokio::sync::Notify::new()),
        controller_gate: Arc::new(tokio::sync::Notify::new()),
        persist_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        event_failures: Arc::new(AtomicUsize::new(0)),
        storage,
    };
    server
        .serve_at(HOST_OBJECT_PATH.try_into().unwrap(), callback.clone())
        .await
        .unwrap();
    server.request_name(HOST_BUS_NAME).await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (HostChannels::new(client), callback, server)
}

fn inbound_envelope(text: &str) -> tinychannels_bus::ChannelInboundEnvelope {
    tinychannels_bus::ChannelInboundEnvelope {
        text: text.into(),
        parts: vec![tinychannels_bus::ContentPart::Text {
            text: text.into(),
            markdown_dialect: None,
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn shutdown_fences_new_transcription_and_joins_accepted_callback() {
    let (host, callback, _server) = connected_host().await;
    callback.transcription_blocked.store(true, Ordering::SeqCst);

    let transcriber = host.clone();
    let transcription = tokio::spawn(async move {
        transcriber
            .transcribe(tinychannels_bus::HostTranscriptionRequest {
                audio_base64: "UklGRg==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
    });
    callback.transcription_started.notified().await;

    let shutdown_host = host.clone();
    let shutdown = tokio::spawn(async move { shutdown_host.shutdown().await });
    while !host.owner.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "shutdown must retain ownership until an accepted transcription completes"
    );
    assert!(
        host.transcribe(tinychannels_bus::HostTranscriptionRequest {
            audio_base64: "UklGRg==".into(),
            mime_type: Some("audio/wav".into()),
            file_name: None,
            language: None,
        })
        .await
        .is_err(),
        "new transcription must be rejected after shutdown fences publication"
    );

    callback
        .transcription_blocked
        .store(false, Ordering::SeqCst);
    callback.transcription_gate.notify_one();
    assert_eq!(transcription.await.unwrap().unwrap().text, "spoken words");
    shutdown.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_joins_owned_transcription_after_waiter_cancellation() {
    let (host, callback, _server) = connected_host().await;
    callback.transcription_blocked.store(true, Ordering::SeqCst);

    let transcriber = host.clone();
    let waiter = tokio::spawn(async move {
        transcriber
            .transcribe(tinychannels_bus::HostTranscriptionRequest {
                audio_base64: "UklGRg==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
    });
    callback.transcription_started.notified().await;
    waiter.abort();
    let _ = waiter.await;

    let shutdown_host = host.clone();
    let shutdown = tokio::spawn(async move { shutdown_host.shutdown().await });
    while !host.owner.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "canceling the caller must not release the accepted callback owner"
    );
    callback
        .transcription_blocked
        .store(false, Ordering::SeqCst);
    callback.transcription_gate.notify_one();
    shutdown.await.unwrap().unwrap();
    assert!(
        callback
            .events
            .lock()
            .await
            .contains(&"transcription:complete".into()),
        "shutdown must wait for the host callback to finish"
    );
}

#[tokio::test]
async fn transcription_admission_refuses_when_its_owned_task_limit_is_reached() {
    let (host, callback, _server) = connected_host().await;
    callback.transcription_blocked.store(true, Ordering::SeqCst);

    let mut active = Vec::new();
    for _ in 0..super::MAX_ACTIVE_HOST_CALLBACKS {
        let host = host.clone();
        active.push(tokio::spawn(async move {
            host.transcribe(tinychannels_bus::HostTranscriptionRequest {
                audio_base64: "UklGRg==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
        }));
    }

    while callback.transcriptions_started.load(Ordering::SeqCst) < super::MAX_ACTIVE_HOST_CALLBACKS
    {
        let notified = callback.transcription_started.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if callback.transcriptions_started.load(Ordering::SeqCst)
            >= super::MAX_ACTIVE_HOST_CALLBACKS
        {
            break;
        }
        notified.await;
    }

    let refused = host
        .transcribe(tinychannels_bus::HostTranscriptionRequest {
            audio_base64: "UklGRg==".into(),
            mime_type: Some("audio/wav".into()),
            file_name: None,
            language: None,
        })
        .await;
    assert!(refused.is_err(), "the seventeenth held callback is refused");

    callback
        .transcription_blocked
        .store(false, Ordering::SeqCst);
    callback.transcription_gate.notify_waiters();
    for transcription in active {
        assert_eq!(transcription.await.unwrap().unwrap().text, "spoken words");
    }
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn callback_consumer_preserves_inbound_then_status_order() {
    let (host, callback, _server) = connected_host().await;
    host.deliver_inbound("telegram", inbound_envelope("hello"))
        .await;
    host.report_status("telegram", "connected", None).await;
    host.flush().await.unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["inbound:telegram:hello", "status:telegram:connected"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn module_host_exposes_scoped_voice_callbacks_and_event_capabilities() {
    let (host, callback, _server) = connected_host().await;
    let provider_host = Arc::new(crate::provider::Hooks::default()).host(
        host.clone(),
        "telegram".into(),
        Some("telegram:media-v1:fixture".into()),
    );
    let transcription = provider_host
        .transcriber()
        .unwrap()
        .transcribe(tinychannels::host::TranscriptionRequest {
            audio_base64: "UklGRg==".into(),
            mime_type: Some("audio/wav".into()),
            file_name: Some("voice.wav".into()),
            language: Some("en".into()),
        })
        .await
        .unwrap();
    assert_eq!(transcription.text, "spoken words");
    assert_eq!(transcription.language.as_deref(), Some("en"));

    let scope = tinychannels_bus::HostVoiceScope {
        channel: tinychannels_bus::ChannelRef {
            id: "provider-forged-channel".into(),
            account_id: Some("provider-forged-account".into()),
        },
        conversation: tinychannels_bus::ConversationRef {
            id: "conversation-1".into(),
            thread_id: Some("thread-2".into()),
            topic_id: Some("topic-3".into()),
            ..Default::default()
        },
        turn_id: "message-4".into(),
        ..Default::default()
    };
    let speech = provider_host
        .synthesizer()
        .unwrap()
        .synthesize_scoped(
            scope.clone(),
            tinychannels::host::SpeechRequest {
                text: "hello".into(),
                voice: None,
                format: Some("wav".into()),
                thread_id: Some("thread-2".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(speech.mime_type, "audio/wav");
    assert_eq!(speech.audio_base64, "UklGRg==");

    let stored = provider_host
        .voice_media_store()
        .unwrap()
        .store_voice(tinychannels::host::VoiceMediaRequest {
            audio_base64: "UklGRgAAAABXQVZF".into(),
            mime_type: "audio/wav".into(),
            codec: Some("wav_pcm16".into()),
            duration_ms: Some(1_250),
            file_name: Some("voice.wav".into()),
            direction: tinychannels_bus::HostVoiceMediaDirection::Inbound,
            scope: scope.clone(),
        })
        .await
        .unwrap();
    let opaque_id = stored.media.opaque_id.as_deref().unwrap();
    assert!(opaque_id.starts_with("tinychannels_media_v1_"));
    assert_eq!(stored.media.codec.as_deref(), Some("wav_pcm16"));
    assert_eq!(
        stored.accessible_url.as_deref(),
        Some(format!("https://media.example.test/authorized/{opaque_id}").as_str())
    );
    let resolved = provider_host
        .voice_media_store()
        .unwrap()
        .read_voice(scope.clone(), opaque_id.to_owned())
        .await
        .unwrap();
    assert_eq!(resolved.audio_base64, "UklGRgAAAABXQVZF");
    let mut foreign_scope = scope.clone();
    foreign_scope.conversation.id = "foreign-conversation".into();
    assert!(
        provider_host
            .voice_media_store()
            .unwrap()
            .read_voice(foreign_scope, opaque_id.to_owned())
            .await
            .is_err()
    );
    assert_eq!(
        callback
            .voice_requests
            .lock()
            .await
            .iter()
            .filter(|request| request["kind"] == "read")
            .count(),
        0,
        "foreign namespaced media must fail before legacy callback fallback"
    );
    let legacy_id = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    assert!(
        provider_host
            .voice_media_store()
            .unwrap()
            .read_voice(scope.clone(), legacy_id.into())
            .await
            .is_ok()
    );
    assert!(
        provider_host
            .voice_media_store()
            .unwrap()
            .read_voice(scope.clone(), "tinychannels_media_v1_malformed".into())
            .await
            .is_err()
    );
    let scopes = callback.voice_scopes.lock().await;
    assert_eq!(scopes.len(), 3);
    for observed in scopes.iter() {
        assert_eq!(observed.configured_instance, "telegram");
        assert_eq!(observed.channel.id, "telegram");
        assert_eq!(observed.channel.account_id.as_deref(), Some("telegram"));
        assert!(
            !observed
                .channel
                .account_id
                .as_deref()
                .unwrap()
                .contains("media-v1")
        );
    }
    for observed in &scopes[1..] {
        assert_eq!(observed.conversation.id, "conversation-1");
        assert_eq!(observed.conversation.thread_id.as_deref(), Some("thread-2"));
        assert_eq!(observed.conversation.topic_id.as_deref(), Some("topic-3"));
        assert_eq!(observed.turn_id, "message-4");
    }
    drop(scopes);
    let voice_requests = callback.voice_requests.lock().await;
    let publication = voice_requests
        .iter()
        .find(|request| request["kind"] == "store")
        .unwrap();
    assert_eq!(publication["direction"], "inbound");
    assert_eq!(publication["codec"], "wav_pcm16");
    assert_eq!(publication["mediaReference"]["opaqueId"], opaque_id);
    assert!(
        voice_requests
            .iter()
            .any(|request| { request["kind"] == "read" && request["opaqueId"] == legacy_id })
    );
    drop(voice_requests);

    provider_host
        .events()
        .unwrap()
        .publish(
            "channel",
            "reaction_sent",
            serde_json::json!({"target_message_id":"tg_42"}),
        )
        .await
        .unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["transcription:complete", "event:channel:reaction_sent"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn denied_voice_publication_removes_unclaimed_shared_media() {
    let (host, callback, _server) = connected_host().await;
    callback
        .voice_publication_failures
        .store(1, Ordering::SeqCst);
    let provider_host = Arc::new(crate::provider::Hooks::default()).host(
        host.clone(),
        "telegram".into(),
        Some("telegram:media-v1:fixture".into()),
    );
    let store = provider_host.voice_media_store().unwrap();
    let scope = tinychannels_bus::HostVoiceScope {
        conversation: tinychannels_bus::ConversationRef {
            id: "conversation-denied".into(),
            ..Default::default()
        },
        turn_id: "turn-denied".into(),
        ..Default::default()
    };
    assert!(
        store
            .store_voice(tinychannels::host::VoiceMediaRequest {
                audio_base64: "UklGRgAAAABXQVZF".into(),
                mime_type: "audio/wav".into(),
                codec: Some("wav_pcm16".into()),
                duration_ms: None,
                file_name: Some("voice.wav".into()),
                direction: tinychannels_bus::HostVoiceMediaDirection::Inbound,
                scope: scope.clone(),
            })
            .await
            .is_err()
    );
    let opaque_id = callback.projected_voice_media_ids.lock().await[0]
        .clone()
        .unwrap();
    assert!(store.read_voice(scope, opaque_id).await.is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn scoped_voice_callbacks_round_trip_scope_and_enforce_media_limits() {
    let (host, callback, _server) = connected_host().await;
    let scope = tinychannels_bus::HostVoiceScope {
        configured_instance: "telegram-primary".into(),
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("bot-7".into()),
        },
        conversation: tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "group-42".into(),
            thread_id: Some("thread-9".into()),
            topic_id: Some("topic-3".into()),
            ..Default::default()
        },
        turn_id: "message-88".into(),
    };
    let transcription = host
        .transcribe_voice(tinychannels_bus::HostVoiceTranscriptionRequest {
            scope: scope.clone(),
            audio_base64: "UklGRg==".into(),
            mime_type: Some("audio/wav".into()),
            file_name: Some("voice.wav".into()),
            language: Some("en".into()),
        })
        .await
        .unwrap();
    assert_eq!(transcription.text, "spoken words");

    let speech = host
        .synthesize_speech(tinychannels_bus::HostSpeechSynthesisRequest {
            scope: scope.clone(),
            text: "hello".into(),
            voice: Some("alloy".into()),
            format: Some("wav".into()),
        })
        .await
        .unwrap();
    assert_eq!(speech.mime_type, "audio/wav");
    assert_eq!(speech.audio_base64, "UklGRg==");

    let stored = host
        .store_voice_media(tinychannels_bus::HostStoreVoiceMediaRequest {
            scope: scope.clone(),
            direction: tinychannels_bus::HostVoiceMediaDirection::Inbound,
            audio_base64: "UklGRg==".into(),
            mime_type: "audio/wav".into(),
            codec: Some("wav_pcm16".into()),
            duration_ms: Some(1_250),
            file_name: Some("voice.wav".into()),
            media_reference: None,
        })
        .await
        .unwrap();
    assert_eq!(stored.media.opaque_id.as_deref(), Some("voice-1"));

    let read = host
        .read_voice_media(tinychannels_bus::HostReadVoiceMediaRequest {
            scope: scope.clone(),
            opaque_id: "voice-1".into(),
            max_bytes: 12,
        })
        .await
        .unwrap();
    assert_eq!(read.file_name.as_deref(), Some("voice.wav"));
    assert!(
        host.read_voice_media(tinychannels_bus::HostReadVoiceMediaRequest {
            scope: scope.clone(),
            opaque_id: "voice-1".into(),
            max_bytes: 2,
        })
        .await
        .is_err(),
        "decoded bytes must respect the caller's requested maximum"
    );

    let policy = host
        .voice_reply_policy(tinychannels_bus::HostVoiceReplyPolicyRequest {
            scope: scope.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        policy.mode,
        tinychannels_bus::HostVoiceReplyMode::SpeakReply
    );
    assert!(policy.inbound_has_voice);

    assert!(
        host.synthesize_speech(tinychannels_bus::HostSpeechSynthesisRequest {
            scope: scope.clone(),
            text: "oversized-response".into(),
            voice: None,
            format: None,
        })
        .await
        .is_err(),
        "oversized callback results must be rejected after deserialization"
    );
    assert!(
        host.synthesize_speech(tinychannels_bus::HostSpeechSynthesisRequest {
            scope: scope.clone(),
            text: "x".repeat(super::MAX_VOICE_CALLBACK_BYTES),
            voice: None,
            format: None,
        })
        .await
        .is_err(),
        "oversized callback requests must fail before reaching the host"
    );

    let scopes = callback.voice_scopes.lock().await;
    assert_eq!(scopes.len(), 7);
    assert!(scopes.iter().all(|observed| observed == &scope));
    drop(scopes);
    let calls = callback.voice_requests.lock().await;
    assert_eq!(calls[0]["kind"], "transcribe");
    assert_eq!(
        calls[1],
        serde_json::json!({"kind":"synthesize","text":"hello","format":"wav"})
    );
    assert_eq!(calls[2]["direction"], "inbound");
    assert_eq!(
        calls[3],
        serde_json::json!({"kind":"read","opaqueId":"voice-1","maxBytes":12})
    );
    assert_eq!(calls[4]["kind"], "read");
    assert_eq!(calls[5]["kind"], "policy");
    assert_eq!(calls[6]["text"], "oversized-response");
    drop(calls);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn module_host_forwards_typed_backend_requests_and_bounds_payloads() {
    let (host, callback, _server) = connected_host().await;
    let request = tinychannels_bus::ChannelBackendRequest::SendReaction {
        channel: "discord".into(),
        reaction: serde_json::json!({"message_id":"fixture-message","emoji":"✅"}),
    };
    let result = host.dispatch_controller_backend(request).await.unwrap();
    assert!(matches!(
        result,
        tinychannels_bus::ChannelBackendResult::SendReaction(
            tinychannels_bus::controllers::ChannelReactionResult { success: true, .. }
        )
    ));
    assert_eq!(
        callback.events.lock().await.last().map(String::as_str),
        Some("controller:discord:reaction")
    );

    let oversized = tinychannels_bus::ChannelBackendRequest::SendReaction {
        channel: "discord".into(),
        reaction: serde_json::json!({"payload":"x".repeat(super::MAX_CONTROLLER_PAYLOAD_BYTES)}),
    };
    assert!(host.dispatch_controller_backend(oversized).await.is_err());
    assert_eq!(callback.events.lock().await.len(), 1);
    host.shutdown().await.unwrap();
    assert!(
        host.dispatch_controller_backend(tinychannels_bus::ChannelBackendRequest::SendReaction {
            channel: "discord".into(),
            reaction: serde_json::json!({"message_id":"fixture-message","emoji":"✅"}),
        })
        .await
        .is_err(),
        "the terminal host callback owner must refuse new controller work"
    );
}

#[tokio::test]
async fn shutdown_joins_owned_controller_callback_after_waiter_cancellation() {
    let (host, callback, _server) = connected_host().await;
    callback.controller_blocked.store(true, Ordering::SeqCst);

    let controller_host = host.clone();
    let waiter = tokio::spawn(async move {
        controller_host
            .dispatch_controller_backend(tinychannels_bus::ChannelBackendRequest::SendReaction {
                channel: "discord".into(),
                reaction: serde_json::json!({"message_id":"fixture-message","emoji":"✅"}),
            })
            .await
    });
    callback.controller_started.notified().await;
    waiter.abort();
    let _ = waiter.await;

    let shutdown_host = host.clone();
    let shutdown = tokio::spawn(async move { shutdown_host.shutdown().await });
    while !host.owner.terminal.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "shutdown must join an accepted host controller callback"
    );
    callback.controller_blocked.store(false, Ordering::SeqCst);
    callback.controller_gate.notify_one();
    shutdown.await.unwrap().unwrap();
    assert!(
        callback
            .events
            .lock()
            .await
            .contains(&"controller:discord:reaction".into()),
        "shutdown must retain the callback after its waiter is canceled"
    );
}

#[tokio::test]
async fn optional_event_callback_failure_does_not_poison_cleanup() {
    let (host, callback, _server) = connected_host().await;
    callback.event_failures.store(1, Ordering::SeqCst);
    let result = host
        .publish_event(tinychannels_bus::HostEventPublication {
            domain: "channel".into(),
            kind: "reaction_sent".into(),
            payload: serde_json::json!({"target_message_id":"tg_42"}),
        })
        .await;
    assert!(result.is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_host_event_sink_is_an_optional_capability() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    host.publish_event(tinychannels_bus::HostEventPublication {
        domain: "channel".into(),
        kind: "reaction_sent".into(),
        payload: serde_json::json!({"target_message_id":"tg_42"}),
    })
    .await
    .unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_channel_event_crosses_memory_bus_with_full_source_and_address() {
    assert!(
        tinychannels_bus::names::HOST_METHODS
            .contains(&tinychannels_bus::names::host_methods::DELIVER_EVENT),
        "the real callback used below must remain in the declared host method table"
    );
    let (host, callback, _server) = connected_host().await;
    let event = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "telegram:update:44".into(),
        source_event_id: Some("44".into()),
        source_timestamp: Some(1_700_000_000_000),
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: Some("bot-7".into()),
        },
        conversation: Some(tinychannels_bus::ConversationRef {
            kind: tinychannels_bus::ConversationKind::Group,
            id: "-10042".into(),
            parent_id: Some("workspace-2".into()),
            topic_id: Some("topic-8".into()),
            thread_id: Some("thread-9".into()),
            scope_id: Some("scope-1".into()),
        }),
        event: tinychannels_bus::ChannelEventKind::ReactionAdded {
            message_id: "msg-22".into(),
            provider_message_id: None,
            actor: tinychannels_bus::SenderRef {
                id: "actor-3".into(),
                ..Default::default()
            },
            reaction: tinychannels_bus::ChannelReaction {
                value: "🔥".into(),
                custom_emoji_id: None,
            },
        },
    };
    host.deliver_inbound("telegram", inbound_envelope("before"))
        .await;
    host.deliver_event(event.clone()).await.unwrap();
    host.flush().await.unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["inbound:telegram:before", "typed_event:telegram:update:44",]
    );
    assert_eq!(callback.typed_events.lock().await.as_slice(), &[event]);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_event_callback_failure_is_returned_without_poisoning_shutdown() {
    let (host, callback, _server) = connected_host().await;
    callback.event_failures.store(1, Ordering::SeqCst);
    let event = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "event-fails-once".into(),
        source_event_id: None,
        source_timestamp: None,
        channel: Default::default(),
        conversation: None,
        event: tinychannels_bus::ChannelEventKind::Lifecycle {
            state: "connected".into(),
            reason: None,
        },
    };

    assert!(host.deliver_event(event).await.is_err());
    assert!(callback.typed_events.lock().await.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_event_callback_is_optional_when_host_has_no_event_object() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    let event = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "legacy-host-event".into(),
        source_event_id: None,
        source_timestamp: None,
        channel: Default::default(),
        conversation: None,
        event: tinychannels_bus::ChannelEventKind::Lifecycle {
            state: "connected".into(),
            reason: None,
        },
    };

    host.deliver_event(event)
        .await
        .expect("older host callback surfaces may omit typed event delivery");
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_event_payload_limit_and_closed_owner_are_reported() {
    let (host, _callback, _server) = connected_host().await;
    let oversized = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "large-event".into(),
        source_event_id: None,
        source_timestamp: None,
        channel: Default::default(),
        conversation: None,
        event: tinychannels_bus::ChannelEventKind::Lifecycle {
            state: "connected".into(),
            reason: Some("x".repeat(super::MAX_CALLBACK_BYTES)),
        },
    };
    assert!(host.deliver_event(oversized).await.is_err());
    assert_eq!(format!("{host:?}"), "HostChannels { .. }");

    host.shutdown().await.unwrap();
    let closed = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "closed-owner-event".into(),
        source_event_id: None,
        source_timestamp: None,
        channel: Default::default(),
        conversation: None,
        event: tinychannels_bus::ChannelEventKind::Lifecycle {
            state: "connected".into(),
            reason: None,
        },
    };
    assert!(host.deliver_event(closed).await.is_err());
}

#[tokio::test]
async fn module_event_enrichment_separates_accounts_and_overwrites_provider_authority() {
    use tinychannels::host::EventSink;

    let (host, callback, _server) = connected_host().await;
    let source = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "telegram:update:44:reaction:added".into(),
        source_event_id: Some("44".into()),
        source_timestamp: Some(12),
        channel: tinychannels_bus::ChannelRef {
            id: "spoofed-channel".into(),
            account_id: Some("caller-spoof".into()),
        },
        conversation: None,
        event: tinychannels_bus::ChannelEventKind::Typing {
            active: true,
            actor: Default::default(),
            expires_at: None,
        },
    };
    crate::provider::ModuleEvents(host.clone(), "telegram:account-a".into(), None)
        .publish_channel_event(source.clone())
        .await
        .unwrap();
    crate::provider::ModuleEvents(host.clone(), "telegram:account-b".into(), None)
        .publish_channel_event(source)
        .await
        .unwrap();
    host.flush().await.unwrap();

    let events = callback.typed_events.lock().await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].source_event_id.as_deref(), Some("44"));
    assert_eq!(events[1].source_event_id.as_deref(), Some("44"));
    assert_eq!(events[0].channel.id, "telegram");
    assert_eq!(
        events[0].channel.account_id.as_deref(),
        Some("telegram:account-a")
    );
    assert_eq!(
        events[1].channel.account_id.as_deref(),
        Some("telegram:account-b")
    );
    assert_ne!(events[0].event_id, events[1].event_id);
    assert_eq!(
        events[0].event_id,
        "telegram:account-a:telegram:update:44:reaction:added:typing"
    );
    assert_eq!(
        events[1].event_id,
        "telegram:account-b:telegram:update:44:reaction:added:typing"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
#[allow(
    deprecated,
    reason = "this test covers the legacy inbound bridge ordering contract"
)]
async fn module_unified_ingress_acknowledges_message_before_following_event() {
    use tinychannels::host::EventSink;

    let (host, callback, _server) = connected_host().await;
    let events = crate::provider::ModuleEvents(host.clone(), "telegram:bot-a".into(), None);
    let handled = events
        .publish_inbound(tinychannels_bus::ChannelMessage {
            id: "telegram_100_5".into(),
            sender: "7".into(),
            reply_target: "100:88".into(),
            content: "message first".into(),
            channel: "telegram".into(),
            timestamp: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(handled);
    let event = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "update-6-edit".into(),
        source_event_id: Some("6".into()),
        source_timestamp: Some(11),
        channel: Default::default(),
        conversation: Some(tinychannels_bus::ConversationRef {
            id: "100".into(),
            topic_id: Some("88".into()),
            ..Default::default()
        }),
        event: tinychannels_bus::ChannelEventKind::MessageUpdated {
            message_id: "telegram_100_5".into(),
            provider_message_id: Some("5".into()),
            parts: vec![],
        },
    };
    events
        .publish_ingress(tinychannels_bus::ChannelIngress::Event(event))
        .await
        .unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        [
            "inbound:telegram:bot-a:message first",
            "typed_event:telegram:bot-a:update-6-edit:message_updated"
        ]
    );
    let envelope = callback.inbound_payloads.lock().await[0].clone();
    assert_eq!(envelope["messageId"], "telegram_100_5");
    assert_eq!(envelope["channel"]["accountId"], "telegram:bot-a");
    assert_eq!(envelope["conversation"]["id"], "100");
    assert_eq!(envelope["conversation"]["topicId"], "88");
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_event_callback_failure_is_reported_and_shutdown_closes_delivery() {
    let (host, callback, _server) = connected_host().await;
    callback.event_failures.store(1, Ordering::SeqCst);
    let event = tinychannels_bus::ChannelEvent {
        version: 2,
        event_id: "fixture:event".into(),
        source_event_id: Some("source".into()),
        source_timestamp: None,
        channel: tinychannels_bus::ChannelRef {
            id: "telegram".into(),
            account_id: None,
        },
        conversation: None,
        event: tinychannels_bus::ChannelEventKind::Lifecycle {
            state: "connected".into(),
            reason: None,
        },
    };
    assert!(host.deliver_event(event.clone()).await.is_err());
    host.shutdown().await.unwrap();
    assert!(host.deliver_event(event).await.is_err());
}

#[tokio::test]
async fn allowlist_persistence_returns_only_after_host_acknowledges() {
    let (host, callback, _server) = connected_host().await;
    host.persist_allowed_identity("telegram", "12345")
        .await
        .unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["allowlist:telegram:12345"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_allowlist_persistence_is_reported_and_can_be_retried() {
    let (host, callback, _server) = connected_host().await;
    callback.persist_failures.store(1, Ordering::SeqCst);
    assert!(
        host.persist_allowed_identity("telegram", "12345")
            .await
            .is_err()
    );
    host.persist_allowed_identity("telegram", "12345")
        .await
        .unwrap();
    assert_eq!(
        callback.events.lock().await.as_slice(),
        ["allowlist:telegram:12345"]
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_store_round_trips_records_inside_the_active_host_scope() {
    use tinychannels::delivery::{
        DeliveryQueueStore as _, DeliveryResultStore as _, QueuedDelivery, enqueue_delivery,
    };

    let (host, callback, _server) = connected_host().await;
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "op-scope-test".into(),
        channel_id: "telegram".into(),
        conversation_id: "room-1".into(),
        channel_ref: None,
        conversation_ref: None,
        turn_id: None,
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };
    let entry = QueuedDelivery::new("op-scope-test", intent.clone(), 42);
    enqueue_delivery(&store, entry.clone()).await.unwrap();
    assert_eq!(store.load_pending(&entry.id).await.unwrap(), Some(entry));

    let result = tinychannels_bus::DeliveryTerminalResult {
        id: "op-scope-test".into(),
        intent_fingerprint: "a".repeat(64),
        completed_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        receipt: None,
    };
    store.save_result(result.clone()).await.unwrap();
    assert_eq!(store.load_result(&result.id).await.unwrap(), Some(result));

    callback.storage.set_scope("scope-b");
    assert!(store.load_pending_all().await.unwrap().is_empty());
    assert!(store.load_result("op-scope-test").await.unwrap().is_none());
    callback.storage.set_scope("scope-a");
    assert_eq!(store.load_pending_all().await.unwrap().len(), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn updating_pending_delivery_does_not_read_the_full_queue() {
    use tinychannels::delivery::{DeliveryQueueStore as _, QueuedDelivery};

    let (host, callback, _server) = connected_host().await;
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "op-update-only".into(),
        turn_id: None,
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };
    let entry = QueuedDelivery::new("op-update-only", intent, 42);
    store.save_pending(entry.clone()).await.unwrap();
    let full_reads_after_insert = callback.storage.faults.pending_reads.load(Ordering::SeqCst);

    let mut update = entry;
    update.retry_count = 1;
    store.save_pending(update).await.unwrap();

    assert_eq!(
        callback.storage.faults.pending_reads.load(Ordering::SeqCst),
        full_reads_after_insert,
        "an update should load its existing record by id, not transfer the whole queue"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_store_rejects_invalid_and_expired_records() {
    use tinychannels::delivery::{
        DeliveryQueueStore as _, DeliveryResultStore as _, QueuedDelivery,
    };

    let (host, _callback, _server) = connected_host().await;
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "op-validate".into(),
        turn_id: None,
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };

    assert!(store.load_pending("").await.is_err());
    assert!(store.load_failed(&"x".repeat(257)).await.is_err());

    let mismatched = QueuedDelivery::new("different-id", intent.clone(), 1);
    assert!(store.save_pending(mismatched).await.is_err());
    let oversized = QueuedDelivery::new(
        "op-validate",
        tinychannels_bus::ChannelOutboundIntent {
            payload: tinychannels::OutboundPayload::Text {
                text: "x".repeat(tinychannels_bus::module_delivery::DELIVERY_RECORD_BYTES),
            },
            ..intent
        },
        1,
    );
    assert!(store.save_pending(oversized).await.is_err());

    let expired = tinychannels_bus::DeliveryTerminalResult {
        id: "op-validate".into(),
        intent_fingerprint: "a".repeat(64),
        completed_at_unix_ms: 1,
        receipt: None,
    };
    store.save_result(expired).await.unwrap();
    assert!(store.load_result("op-validate").await.unwrap().is_none());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_flush_waiter_keeps_callback_work_and_can_retry() {
    let (host, callback, _server) = connected_host().await;
    callback.blocked.store(true, Ordering::SeqCst);
    host.deliver_inbound("telegram", inbound_envelope("hello"))
        .await;
    let waiter = tokio::spawn({
        let host = host.clone();
        async move { host.flush().await }
    });
    tokio::task::yield_now().await;
    waiter.abort();
    let _ = waiter.await;
    callback.blocked.store(false, Ordering::SeqCst);
    callback.gate.notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(2), host.flush())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(callback.events.lock().await.len(), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_optional_host_ignores_status_but_reports_inbound_failure() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    host.report_status("telegram", "connecting", None).await;
    host.flush().await.unwrap();
    host.deliver_inbound("telegram", inbound_envelope("hello"))
        .await;
    assert!(host.flush().await.is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_callbacks_do_not_follow_the_default_user() {
    let (legacy, default_user, server) = connected_host().await;
    let mut runtime_user = default_user.clone();
    runtime_user.events = Arc::new(Mutex::new(Vec::new()));
    let path = format!("{HOST_OBJECT_PATH}/runtime_a");
    server
        .serve_at(path.clone().try_into().unwrap(), runtime_user.clone())
        .await
        .unwrap();
    let scoped = HostChannels::new_at(legacy.connection(), path);
    scoped
        .deliver_inbound("telegram", inbound_envelope("private"))
        .await;
    scoped.report_status("telegram", "connected", None).await;
    scoped
        .persist_allowed_identity("telegram", "allowed")
        .await
        .unwrap();
    scoped.flush().await.unwrap();
    assert!(
        default_user.events.lock().await.is_empty(),
        "runtime callbacks must never reach the current default user"
    );
    assert_eq!(
        runtime_user.events.lock().await.as_slice(),
        [
            "inbound:telegram:private",
            "status:telegram:connected",
            "allowlist:telegram:allowed"
        ]
    );
    scoped.shutdown().await.unwrap();
    legacy.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_without_a_storage_object_has_no_delivery_store_and_refuses_required_send() {
    use tinychannels::delivery::{
        DeliveryQueueError, DeliveryQueueStore as _, QueuedDelivery, enqueue_delivery,
    };
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(client);
    let store = crate::delivery::HostDeliveryStore::from_host(&host);
    assert!(matches!(
        store.load_pending_all().await,
        Err(DeliveryQueueError::Unsupported)
    ));
    let intent = tinychannels_bus::ChannelOutboundIntent {
        idempotency_key: "missing-store".into(),
        turn_id: None,
        durability: tinychannels_bus::DeliveryDurability::Required,
        ..Default::default()
    };
    assert!(
        enqueue_delivery(&store, QueuedDelivery::new("missing-store", intent, 0))
            .await
            .is_err()
    );
    host.shutdown().await.unwrap();
}
