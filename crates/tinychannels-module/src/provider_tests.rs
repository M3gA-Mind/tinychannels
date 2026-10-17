use super::*;
use base64::Engine as _;
use sha2::Digest as _;
use tinychannels::host::EventSink as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinychannels_bus::{ChannelInboundEnvelope, ContentPart, ConversationKind, ConversationRef};

#[derive(Clone, Default)]
struct IngressVoiceCallback {
    read_requests: Arc<Mutex<Vec<tinychannels_bus::HostReadVoiceMediaRequest>>>,
    stt_requests: Arc<Mutex<Vec<tinychannels_bus::HostVoiceTranscriptionRequest>>>,
    inbound: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    stt_calls: Arc<AtomicUsize>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl IngressVoiceCallback {
    async fn read_voice_media(
        &self,
        request: tinychannels_bus::HostReadVoiceMediaRequest,
    ) -> Result<tinychannels_bus::HostReadVoiceMediaResult> {
        self.read_requests.lock().await.push(request);
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64: base64::engine::general_purpose::STANDARD.encode([1_u8, 2, 3, 4]),
            mime_type: "audio/ogg".into(),
            file_name: Some("voice.oga".into()),
        })
    }

    async fn transcribe_voice(
        &self,
        request: tinychannels_bus::HostVoiceTranscriptionRequest,
    ) -> Result<tinychannels_bus::HostTranscriptionResult> {
        self.stt_calls.fetch_add(1, Ordering::SeqCst);
        self.stt_requests.lock().await.push(request);
        Err(tinybus::Error::failed("fixture STT unavailable"))
    }

    async fn deliver_inbound(&self, channel: String, message: serde_json::Value) -> Result<()> {
        self.inbound.lock().await.push((channel, message));
        Ok(())
    }
}

async fn ingress_fixture() -> (HostChannels, IngressVoiceCallback, tinybus::Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let server = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let callback = IngressVoiceCallback::default();
    server
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callback.clone(),
        )
        .await
        .unwrap();
    server.request_name(tinychannels_bus::HOST_BUS_NAME).await.unwrap();
    let client = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (HostChannels::new(client), callback, server)
}

fn inbound_voice(transcript: Option<tinychannels_bus::VoiceTranscript>) -> ChannelInboundEnvelope {
    ChannelInboundEnvelope {
        channel: tinychannels_bus::ChannelRef {
            id: "forged-provider".into(),
            account_id: Some("forged-account".into()),
        },
        message_id: "message-voice-13".into(),
        conversation: ConversationRef {
            kind: ConversationKind::Thread,
            id: "chat-43".into(),
            scope_id: Some("community-7".into()),
            parent_id: Some("chat-parent".into()),
            thread_id: Some("thread-8".into()),
            topic_id: Some("topic-9".into()),
            ..Default::default()
        },
        parts: vec![ContentPart::VoiceNote {
            voice_note: tinychannels_bus::VoiceNote {
                media: tinychannels_bus::MediaReference {
                    opaque_id: Some("opaque-voice".into()),
                    kind: tinychannels_bus::MediaKind::Audio,
                    content_type: Some("audio/ogg".into()),
                    ..Default::default()
                },
                codec: Some("ogg_opus".into()),
                duration_ms: Some(1400),
                waveform: Some(vec![0.1, 0.6, 1.0]),
                transcript,
                ..Default::default()
            },
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn ingress_retains_voice_and_full_scope_when_stt_fails() {
    let (host, callback, _server) = ingress_fixture().await;
    let events = ModuleEvents(host.clone(), "telegram:bot-a".into());
    assert!(events
        .publish_ingress(tinychannels_bus::ChannelIngress::Inbound(inbound_voice(None)))
        .await
        .unwrap());
    host.flush().await.unwrap();

    let read = callback.read_requests.lock().await;
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].max_bytes, 8 * 1024 * 1024);
    assert_eq!(read[0].opaque_id, "opaque-voice");
    let stt = callback.stt_requests.lock().await;
    assert_eq!(stt.len(), 1);
    assert_eq!(stt[0].scope.configured_instance, "telegram:bot-a");
    assert_eq!(stt[0].scope.channel.id, "telegram");
    assert_eq!(stt[0].scope.channel.account_id.as_deref(), Some("telegram:bot-a"));
    assert_eq!(
        stt[0].scope.conversation,
        ConversationRef {
            kind: ConversationKind::Thread,
            id: "chat-43".into(),
            scope_id: Some("community-7".into()),
            parent_id: Some("chat-parent".into()),
            thread_id: Some("thread-8".into()),
            topic_id: Some("topic-9".into()),
        }
    );
    assert_eq!(stt[0].scope.turn_id, "message-voice-13");
    drop(stt);
    drop(read);

    let inbound = callback.inbound.lock().await;
    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].0, "telegram:bot-a");
    let note = &inbound[0].1["parts"][0]["voiceNote"];
    assert_eq!(note["media"]["opaqueId"], "opaque-voice");
    assert_eq!(note["codec"], "ogg_opus");
    assert_eq!(note["durationMs"], 1400);
    let waveform = note["waveform"].as_array().unwrap();
    assert_eq!(waveform.len(), 3);
    for (actual, expected) in waveform.iter().zip([0.1_f64, 0.6, 1.0]) {
        assert!((actual.as_f64().unwrap() - expected).abs() < 0.000_001);
    }
    assert_eq!(note["transcript"]["state"], "unavailable");
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_provider_transcript_skips_media_read_and_host_stt() {
    let (host, callback, _server) = ingress_fixture().await;
    let events = ModuleEvents(host.clone(), "telegram:bot-a".into());
    let provider_transcript = tinychannels_bus::VoiceTranscript {
        text: "provider words".into(),
        language: Some("en".into()),
        state: tinychannels_bus::VoiceTranscriptState::Complete,
        source: tinychannels_bus::VoiceTranscriptSource::Provider,
    };
    events
        .publish_ingress(tinychannels_bus::ChannelIngress::Inbound(inbound_voice(Some(
            provider_transcript,
        ))))
        .await
        .unwrap();
    host.flush().await.unwrap();
    assert!(callback.read_requests.lock().await.is_empty());
    assert_eq!(callback.stt_calls.load(Ordering::SeqCst), 0);
    let inbound = callback.inbound.lock().await;
    assert_eq!(
        inbound[0].1["parts"][0]["voiceNote"]["transcript"]["text"],
        "provider words"
    );
    assert_eq!(
        inbound[0].1["parts"][0]["voiceNote"]["media"]["opaqueId"],
        "opaque-voice"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the separately built TinyVoice module; run with TINYVOICE_MODULE_ARTIFACT"]
async fn codec_adapter_calls_loaded_tinyvoice_module() {
    use tinybus::broker::Broker;
    use tinybus::module::ModuleHost;
    use tinychannels::host::VoiceCodec as _;
    use tinyvoice_bus::codec::AudioCodec;

    let path = std::env::var("TINYVOICE_MODULE_ARTIFACT").expect("module artifact env");
    let bytes = std::fs::read(&path).expect("read TinyVoice artifact");
    let expected_sha = std::env::var("TINYVOICE_MODULE_SHA256").expect("expected artifact SHA env");
    assert_eq!(hex::encode(sha2::Sha256::digest(&bytes)), expected_sha);
    let bus = tinybus::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let info = host
        .load_file(&path)
        .expect("load compiled TinyVoice module");
    assert_eq!(info.name, "tinyvoice-module");
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = ModuleVoiceCodec(HostChannels::new(connection.clone()));
    let wav = sine_wave_wav();
    let wav_b64 = base64::engine::general_purpose::STANDARD.encode(&wav);
    let probe_proxy = connection
        .proxy(
            tinyvoice_bus::BUS_NAME,
            tinyvoice_bus::OBJECT_PATH,
            tinyvoice_bus::BUS_NAME,
        )
        .unwrap();
    let info: tinyvoice_bus::codec::CodecInfo = probe_proxy
        .call(
            tinyvoice_bus::names::methods::CODEC_PROBE,
            (tinyvoice_bus::codec::CodecInput {
                audio_base64: wav_b64.clone(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(info.codec, AudioCodec::WavPcm16);
    assert!(info.duration_ms > 0);
    let opus = client
        .transcode(tinychannels::host::VoiceCodecRequest {
            audio_base64: wav_b64,
            from: "wav_pcm16".into(),
            target: "ogg_opus".into(),
        })
        .await
        .expect("compiled TinyVoice WAV→Opus transcode");
    let round_trip = client
        .transcode(tinychannels::host::VoiceCodecRequest {
            audio_base64: opus.audio_base64,
            from: "ogg_opus".into(),
            target: "wav_pcm16".into(),
        })
        .await
        .expect("compiled TinyVoice Opus→WAV transcode");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(round_trip.audio_base64)
        .unwrap();
    assert!(decoded.starts_with(b"RIFF"));
    assert!(decoded.iter().any(|byte| *byte != 0));
}

fn sine_wave_wav() -> Vec<u8> {
    let sample_rate = 16_000u32;
    let sample_count = sample_rate as usize;
    let data_len = sample_count * 2;
    let mut wav = Vec::with_capacity(44 + data_len);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36u32 + data_len as u32).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&(sample_rate * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(data_len as u32).to_le_bytes());
    for index in 0..sample_count {
        let phase = (index as f32 * 440.0 * std::f32::consts::TAU) / sample_rate as f32;
        let sample = (phase.sin() * 12_000.0) as i16;
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    wav
}

struct SlowChannel {
    started: StdMutex<Option<oneshot::Sender<()>>>,
    release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl Channel for SlowChannel {
    fn name(&self) -> &str {
        "fixture"
    }

    async fn send(&self, _message: &SendMessage) -> anyhow::Result<()> {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        if let Some(release) = self.release.lock().await.take() {
            let _ = release.await;
        }
        Ok(())
    }

    async fn listen(
        &self,
        _tx: tokio::sync::mpsc::Sender<tinychannels::ChannelMessage>,
    ) -> anyhow::Result<()> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn canceled_send_waiter_does_not_cancel_owned_send_or_shutdown_join() {
    let (started, started_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let channel = Arc::new(SlowChannel {
        started: StdMutex::new(Some(started)),
        release: tokio::sync::Mutex::new(Some(release_rx)),
    });
    let connection = tinybus::Connection::connect({
        let bus = tinybus::transport::memory::MemoryBus::new();
        tinybus::broker::Broker::new().spawn(bus.clone());
        bus.connect().await.unwrap()
    })
    .await
    .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start(channel, Arc::new(Hooks::default()), host);
    let send_waiter = tokio::spawn({
        let running = running.clone();
        async move { running.send(SendMessage::new("body", "recipient")).await }
    });
    started_rx.await.unwrap();
    send_waiter.abort();
    let _ = send_waiter.await;

    let stop = tokio::spawn({
        let running = running.clone();
        async move { running.stop().await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), async {
            while !stop.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err()
    );
    release.send(()).unwrap();
    stop.await.unwrap().unwrap();
}

#[derive(Default)]
struct InteractionChannel(StdMutex<Vec<String>>);
#[async_trait::async_trait]
impl Channel for InteractionChannel {
    fn name(&self) -> &str {
        "interaction-fixture"
    }
    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }
    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("send:{}", message.content));
        Ok(())
    }
    async fn start_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("typing:{recipient}"));
        Ok(())
    }
    async fn send_draft(&self, message: &SendMessage) -> anyhow::Result<Option<String>> {
        self.0
            .lock()
            .unwrap()
            .push(format!("draft:{}", message.content));
        Ok(Some("module-draft".into()))
    }
    async fn stop_typing(&self, recipient: &str) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(format!("stop:{recipient}"));
        Ok(())
    }
}

#[tokio::test]
async fn provider_actor_orders_interactions_with_sends_and_rejects_after_stop() {
    use tinychannels_bus::{ChannelInteraction as Op, ChannelInteractionResult};
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel.clone(),
        Arc::new(Hooks::default()),
        HostChannels::new(connection),
    );
    running
        .interact(Op::StartTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    running
        .send(SendMessage::new("ordinary", "room"))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let draft = running
        .interact(Op::SendDraft {
            message: SendMessage::new("partial", "room"),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(matches!(draft, ChannelInteractionResult::Draft(Some(id)) if id == "module-draft"));
    running
        .interact(Op::StopTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    running.stop().await.unwrap();
    assert_eq!(
        *channel.0.lock().unwrap(),
        ["typing:room", "send:ordinary", "draft:partial", "stop:room"]
    );
    assert!(matches!(
        running.interact(Op::Describe).await,
        Err(RunningSendError::NotDispatched(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_is_bounded_and_joined_by_provider_owner() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel.clone(),
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );

    running
        .interact(Op::StartTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap();
    let first_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "room".into(),
            interval_secs: 4,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh {
        handle: first_handle,
    } = first_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    let second_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "room".into(),
            interval_secs: 4,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh {
        handle: second_handle,
    } = second_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    assert_ne!(first_handle, second_handle);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(4)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        channel.0.lock().unwrap().len(),
        3,
        "both refresh handles remain independently active"
    );

    assert!(
        running
            .interact(Op::StartTypingRefresh {
                recipient: "other".into(),
                interval_secs: 0,
            })
            .await
            .is_err()
    );
    running
        .interact(Op::StopTypingRefresh {
            handle: first_handle,
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(channel.0.lock().unwrap().len(), 4);
    running
        .interact(Op::StopTypingRefresh {
            handle: second_handle.clone(),
        })
        .await
        .unwrap();
    running
        .interact(Op::StopTyping {
            recipient: "room".into(),
        })
        .await
        .unwrap();
    running.stop().await.unwrap();
    assert!(matches!(
        running
            .interact(Op::StopTypingRefresh {
                handle: second_handle,
            })
            .await,
        Err(RunningSendError::NotDispatched(_))
    ));
    assert_eq!(
        *channel.0.lock().unwrap(),
        [
            "typing:room",
            "typing:room",
            "typing:room",
            "typing:room",
            "stop:room"
        ]
    );
    connection.close().await.unwrap();
}

struct PendingTypingChannel {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl Channel for PendingTypingChannel {
    fn name(&self) -> &str {
        "pending-typing"
    }

    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        std::future::pending().await
    }

    async fn start_typing(&self, _: &str) -> anyhow::Result<()> {
        struct DropMark(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for DropMark {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let _drop_mark = DropMark(Arc::clone(&self.dropped));
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_stop_and_shutdown_cancel_inflight_provider_calls() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let channel = Arc::new(PendingTypingChannel {
        entered: Arc::clone(&entered),
        dropped: Arc::clone(&dropped),
    });
    let running = Running::start(
        channel,
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );

    let stop_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "stop-case".into(),
            interval_secs: 1,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh {
        handle: stop_handle,
    } = stop_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    entered.notified().await;
    running
        .interact(Op::StopTypingRefresh {
            handle: stop_handle,
        })
        .await
        .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let shutdown_handle = running
        .interact(Op::StartTypingRefresh {
            recipient: "shutdown-case".into(),
            interval_secs: 1,
        })
        .await
        .unwrap();
    let tinychannels_bus::ChannelInteractionResult::TypingRefresh { handle: _ } = shutdown_handle
    else {
        panic!("provider did not return a refresh handle")
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    entered.notified().await;
    running.stop().await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    connection.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn typing_refresh_capacity_counts_handles_not_recipients() {
    use tinychannels_bus::ChannelInteraction as Op;
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let channel = Arc::new(InteractionChannel::default());
    let running = Running::start(
        channel,
        Arc::new(Hooks::default()),
        HostChannels::new(connection.clone()),
    );
    let mut handles = Vec::new();
    for _ in 0..MAX_TYPING_REFRESH_RECIPIENTS {
        let result = running
            .interact(Op::StartTypingRefresh {
                recipient: "same-room".into(),
                interval_secs: MAX_TYPING_REFRESH_INTERVAL_SECS,
            })
            .await
            .unwrap();
        let tinychannels_bus::ChannelInteractionResult::TypingRefresh { handle } = result else {
            panic!("provider did not return a refresh handle")
        };
        handles.push(handle);
    }
    assert!(
        running
            .interact(Op::StartTypingRefresh {
                recipient: "same-room".into(),
                interval_secs: MAX_TYPING_REFRESH_INTERVAL_SECS,
            })
            .await
            .is_err()
    );
    for handle in handles {
        running
            .interact(Op::StopTypingRefresh { handle })
            .await
            .unwrap();
    }
    running.stop().await.unwrap();
    connection.close().await.unwrap();
}

struct ReconnectingChannel {
    attempts: StdMutex<Vec<oneshot::Sender<()>>>,
}
#[async_trait::async_trait]
impl Channel for ReconnectingChannel {
    fn name(&self) -> &str {
        "reconnect-fixture"
    }
    async fn send(&self, _: &SendMessage) -> anyhow::Result<()> {
        Ok(())
    }
    async fn listen(&self, _: mpsc::Sender<tinychannels::ChannelMessage>) -> anyhow::Result<()> {
        let last = {
            let mut attempts = self.attempts.lock().unwrap();
            let receipt = attempts.remove(0);
            let _ = receipt.send(());
            attempts.is_empty()
        };
        if last {
            std::future::pending::<()>().await;
        }
        anyhow::bail!("fixture disconnected")
    }
}

#[tokio::test(start_paused = true)]
async fn disconnected_module_provider_reconnects_and_stop_joins_the_new_listener() {
    let (first, first_rx) = oneshot::channel();
    let (second, mut second_rx) = oneshot::channel();
    let channel = Arc::new(ReconnectingChannel {
        attempts: StdMutex::new(vec![first, second]),
    });
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start_supervised(
        channel,
        Arc::new(Hooks::default()),
        host.clone(),
        ChannelSupervisionPolicy::default(),
    );
    first_rx.await.unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        second_rx.try_recv().is_ok(),
        "the module must own provider reconnects"
    );
    assert!(!running.is_finished());
    running.stop().await.unwrap();
    assert!(running.is_finished());
    host.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn stopping_during_selected_backoff_prevents_another_provider_attempt() {
    let (first, first_rx) = oneshot::channel();
    let (second, mut second_rx) = oneshot::channel();
    let channel = Arc::new(ReconnectingChannel {
        attempts: StdMutex::new(vec![first, second]),
    });
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    let running = Running::start_supervised(
        channel.clone(),
        Arc::new(Hooks::default()),
        host.clone(),
        ChannelSupervisionPolicy {
            initial_backoff_secs: 30,
            max_backoff_secs: 60,
        },
    );
    first_rx.await.unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        second_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    running.stop().await.unwrap();
    tokio::time::advance(Duration::from_secs(120)).await;
    assert_eq!(channel.attempts.lock().unwrap().len(), 1);
    assert!(running.is_finished());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn lifecycle_hooks_are_drained_once_in_registration_order() {
    let hooks = Hooks::default();
    let calls = Arc::new(StdMutex::new(Vec::new()));
    for index in 0..3 {
        let calls = calls.clone();
        hooks.register_shutdown(
            "fixture",
            Box::new(move || {
                Box::pin(async move {
                    calls.lock().unwrap().push(index);
                })
            }),
        );
    }
    hooks.drain().await;
    hooks.drain().await;
    assert_eq!(*calls.lock().unwrap(), [0, 1, 2]);
}

#[tokio::test]
async fn host_adapters_propagate_callback_failures() {
    use tinychannels::host::{AllowlistStore, EventSink, Transcriber};
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = HostChannels::new(connection);
    // Once host callbacks stop, adapters must surface admission failures.
    host.shutdown().await.unwrap();
    assert!(
        ModuleAllowlist(host.clone())
            .persist_allowed_identity("fixture", "member")
            .await
            .is_err()
    );
    assert!(
        ModuleEvents(host.clone(), "fixture".into())
            .publish("fixture", "event", serde_json::json!({}))
            .await
            .is_err()
    );
    let transcriber = ModuleTranscriber(host.clone(), "fixture".into());
    assert_eq!(transcriber.name(), "module-host");
    assert!(
        transcriber
            .transcribe(tinychannels::host::TranscriptionRequest {
                audio_base64: "YQ==".into(),
                mime_type: Some("audio/wav".into()),
                file_name: None,
                language: None,
            })
            .await
            .is_err()
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_send_worker_reply_distinguishes_dispatch_from_queueing() {
    for dispatched in [false, true] {
        let (send, mut commands) = mpsc::channel::<SendCommand>(1);
        let (cancel, _) = watch::channel(false);
        let running = Arc::new(Running {
            channel: Arc::new(InteractionChannel::default()),
            send,
            cancel,
            worker: Mutex::new(None),
            finished: Arc::new(AtomicBool::new(false)),
        });
        let waiter = tokio::spawn({
            let running = running.clone();
            async move { running.send(SendMessage::new("body", "room")).await }
        });
        let command = commands.recv().await.unwrap();
        if dispatched {
            command.started.send(()).unwrap();
        } else {
            drop(command.started);
        }
        drop(command.reply);
        let error = waiter.await.unwrap().unwrap_err();
        if dispatched {
            assert!(matches!(error, RunningSendError::AfterDispatch(_)));
            assert_eq!(
                error.to_string(),
                "provider send worker stopped after dispatch"
            );
        } else {
            assert!(matches!(error, RunningSendError::NotDispatched(_)));
            assert_eq!(
                error.to_string(),
                "provider send worker stopped before dispatch"
            );
        }
    }
}
