//! Shared fixtures for the Discord adapter tests: a wiremock Discord, in-memory
//! host services and message builders.

use super::channel::DiscordChannel;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinychannels_bus::{
    ChannelIngress, ChannelOutboundMessage, ChannelRef, ContentPart, ConversationRef,
    HostVoiceScope, MediaKind, MediaReference, OutboundContentPart,
};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// `base64("123456")` is the bot user id embedded in the token.
pub(super) const BOT_TOKEN: &str = "MTIzNDU2.ts.hmac";
pub(super) const BOT_ID: &str = "123456";
pub(super) const CHANNEL: &str = "555000111";
pub(super) const THREAD: &str = "777000222";
pub(super) const GUILD: &str = "999000333";

pub(super) const OGG_FIXTURE: &[u8] = include_bytes!("../telegram/fixtures/sine-ogg-opus.ogg");
pub(super) const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5f8AAAAASUVORK5CYII=";

pub(super) fn png_bytes() -> Vec<u8> {
    BASE64.decode(PNG).unwrap()
}

/// A channel pointed at `server`, open to every user.
pub(super) fn channel_for(server: &MockServer) -> DiscordChannel {
    DiscordChannel::new(BOT_TOKEN.into(), None, None, vec![], false, false)
        .with_api_base(server.uri())
}

/// Records every ingress item and reports it as delivered.
#[derive(Default)]
pub(super) struct RecordingEvents {
    pub(super) ingress: Mutex<Vec<ChannelIngress>>,
    pub(super) fail: bool,
    pub(super) decline_inbound: bool,
}

impl RecordingEvents {
    pub(super) fn events(&self) -> Vec<tinychannels_bus::ChannelEvent> {
        self.ingress
            .lock()
            .iter()
            .filter_map(|item| match item {
                ChannelIngress::Event(event) => Some(event.clone()),
                ChannelIngress::Inbound(_) => None,
            })
            .collect()
    }

    pub(super) fn inbound(&self) -> Vec<tinychannels_bus::ChannelInboundEnvelope> {
        self.ingress
            .lock()
            .iter()
            .filter_map(|item| match item {
                ChannelIngress::Inbound(envelope) => Some(envelope.clone()),
                ChannelIngress::Event(_) => None,
            })
            .collect()
    }
}

#[async_trait]
impl crate::host::EventSink for RecordingEvents {
    async fn publish(
        &self,
        _domain: &str,
        _kind: &str,
        _payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish_ingress(&self, ingress: ChannelIngress) -> anyhow::Result<bool> {
        if self.fail {
            anyhow::bail!("ingress unavailable");
        }
        let decline = self.decline_inbound && matches!(ingress, ChannelIngress::Inbound(_));
        self.ingress.lock().push(ingress);
        Ok(!decline)
    }
}

/// In-memory scoped media repository.
#[derive(Default)]
pub(super) struct MemoryMedia {
    pub(super) stored: Mutex<HashMap<String, (HostVoiceScope, crate::host::StoredMedia)>>,
    pub(super) reject_store: bool,
    next: AtomicUsize,
}

impl MemoryMedia {
    pub(super) fn rejecting() -> Self {
        Self {
            reject_store: true,
            ..Default::default()
        }
    }

    /// Seed a readable object for `scope` and return its reference.
    pub(super) fn seed(
        &self,
        scope: &HostVoiceScope,
        kind: MediaKind,
        content_type: &str,
        file_name: &str,
        bytes: Vec<u8>,
    ) -> MediaReference {
        let id = format!("media-{}", self.next.fetch_add(1, Ordering::SeqCst));
        let media = MediaReference {
            opaque_id: Some(id.clone()),
            content_type: Some(content_type.into()),
            file_name: Some(file_name.into()),
            size_bytes: Some(bytes.len() as u64),
            kind,
            ..Default::default()
        };
        self.stored.lock().insert(
            id,
            (
                scope.clone(),
                crate::host::StoredMedia {
                    media: media.clone(),
                    content_type: content_type.into(),
                    file_name: Some(file_name.into()),
                    bytes,
                },
            ),
        );
        media
    }
}

#[async_trait]
impl crate::host::MediaRepository for MemoryMedia {
    async fn store_media(
        &self,
        request: crate::host::MediaStoreRequest,
    ) -> anyhow::Result<MediaReference> {
        if self.reject_store {
            anyhow::bail!("media store unavailable");
        }
        let meta = request.metadata;
        Ok(self.seed(
            &meta.scope,
            meta.kind,
            &meta.content_type,
            meta.file_name.as_deref().unwrap_or("file"),
            request.bytes,
        ))
    }

    async fn read_media(
        &self,
        scope: crate::host::MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<crate::host::StoredMedia> {
        let stored = self.stored.lock();
        let (owner, media) = stored
            .get(&opaque_id)
            .ok_or_else(|| anyhow::anyhow!("unknown media"))?;
        if owner.conversation != scope.conversation || media.bytes.len() as u64 > max_bytes {
            anyhow::bail!("media outside scope");
        }
        Ok(media.clone())
    }
}

/// In-memory voice store: audio is kept by opaque id.
#[derive(Default)]
pub(super) struct MemoryVoice {
    pub(super) audio: Mutex<HashMap<String, (String, String)>>,
    pub(super) requests: Mutex<Vec<crate::host::VoiceMediaRequest>>,
    pub(super) reject_store: bool,
    next: AtomicUsize,
}

impl MemoryVoice {
    pub(super) fn rejecting() -> Self {
        Self {
            reject_store: true,
            ..Default::default()
        }
    }

    pub(super) fn seed(&self, bytes: &[u8], mime: &str) -> MediaReference {
        let id = format!("voice-{}", self.next.fetch_add(1, Ordering::SeqCst));
        self.audio
            .lock()
            .insert(id.clone(), (BASE64.encode(bytes), mime.into()));
        MediaReference {
            opaque_id: Some(id),
            content_type: Some(mime.into()),
            kind: MediaKind::Audio,
            ..Default::default()
        }
    }
}

#[async_trait]
impl crate::host::VoiceMediaStore for MemoryVoice {
    async fn store_voice(
        &self,
        request: crate::host::VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        if self.reject_store {
            anyhow::bail!("voice store unavailable");
        }
        let media = {
            let bytes = BASE64.decode(&request.audio_base64)?;
            self.seed(&bytes, &request.mime_type)
        };
        self.requests.lock().push(request);
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media,
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        _scope: HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        let audio = self.audio.lock();
        let (audio_base64, mime_type) = audio
            .get(&opaque_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown voice"))?;
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64,
            mime_type,
            file_name: Some("voice.ogg".into()),
        })
    }
}

pub(super) struct FixedTranscriber(pub(super) Option<&'static str>);

#[async_trait]
impl crate::host::Transcriber for FixedTranscriber {
    fn name(&self) -> &str {
        "fixed"
    }

    async fn transcribe(
        &self,
        _request: crate::host::TranscriptionRequest,
    ) -> anyhow::Result<crate::host::TranscriptionResult> {
        match self.0 {
            Some(text) => Ok(crate::host::TranscriptionResult {
                text: text.into(),
                language: Some("en".into()),
                duration_secs: Some(1.0),
            }),
            None => anyhow::bail!("stt unavailable"),
        }
    }
}

/// Codec stand-in: echoes the audio, reports a duration and a waveform.
pub(super) struct FixedCodec {
    pub(super) fail: bool,
    pub(super) calls: Mutex<Vec<(String, String)>>,
}

impl FixedCodec {
    pub(super) fn new(fail: bool) -> Self {
        Self {
            fail,
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl crate::host::VoiceCodec for FixedCodec {
    async fn transcode(
        &self,
        request: crate::host::VoiceCodecRequest,
    ) -> anyhow::Result<crate::host::VoiceCodecResult> {
        self.calls.lock().push((request.from, request.target));
        if self.fail {
            anyhow::bail!("codec unavailable");
        }
        // Pretend any input transcodes to the fixture Ogg/Opus file.
        Ok(crate::host::VoiceCodecResult {
            audio_base64: BASE64.encode(OGG_FIXTURE),
            duration_ms: Some(2500),
            waveform: Some(vec![0.0, 0.25, 0.5, 1.0]),
        })
    }
}

/// Everything wired: the configuration the module gives a real account.
pub(super) fn full_channel(
    server: &MockServer,
) -> (
    DiscordChannel,
    Arc<RecordingEvents>,
    Arc<MemoryMedia>,
    Arc<MemoryVoice>,
) {
    let events = Arc::new(RecordingEvents::default());
    let media = Arc::new(MemoryMedia::default());
    let voice = Arc::new(MemoryVoice::default());
    let channel = channel_for(server)
        .with_events(events.clone())
        .with_media_repository(media.clone())
        .with_voice_media_store(voice.clone())
        .with_transcriber(Arc::new(FixedTranscriber(Some("spoken words"))))
        .with_voice_codec(Arc::new(FixedCodec::new(false)))
        .with_voice_synthesis(true);
    (channel, events, media, voice)
}

/// Mount a message-create endpoint answering `{"id": <n>}` with counting ids.
pub(super) async fn mount_message_post(server: &MockServer, first_id: u64) {
    let next = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path_regex(r"^/channels/\d+/messages$"))
        .respond_with(move |_: &Request| {
            let id = first_id + next.fetch_add(1, Ordering::SeqCst) as u64;
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": id.to_string()}))
        })
        .mount(server)
        .await;
}

pub(super) fn text_part(text: &str) -> OutboundContentPart {
    OutboundContentPart::Content {
        part: ContentPart::Text {
            text: text.into(),
            markdown_dialect: None,
        },
    }
}

pub(super) fn content(part: ContentPart) -> OutboundContentPart {
    OutboundContentPart::Content { part }
}

pub(super) fn conversation() -> ConversationRef {
    ConversationRef {
        kind: tinychannels_bus::ConversationKind::Channel,
        id: CHANNEL.into(),
        ..Default::default()
    }
}

pub(super) fn thread_conversation() -> ConversationRef {
    ConversationRef {
        kind: tinychannels_bus::ConversationKind::Thread,
        id: CHANNEL.into(),
        parent_id: Some(CHANNEL.into()),
        thread_id: Some(THREAD.into()),
        ..Default::default()
    }
}

pub(super) fn account() -> ChannelRef {
    ChannelRef {
        id: "discord".into(),
        account_id: Some("discord:acct".into()),
    }
}

pub(super) fn message(parts: Vec<OutboundContentPart>) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        channel_id: "discord".into(),
        conversation_id: CHANNEL.into(),
        channel_ref: Some(account()),
        conversation_ref: Some(conversation()),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-1".into()),
        idempotency_key: "idem-1".into(),
        parts,
    }
}

/// Scope the adapter derives for media on `message(...)`.
pub(super) fn media_scope() -> HostVoiceScope {
    HostVoiceScope {
        configured_instance: "discord:acct".into(),
        channel: account(),
        conversation: conversation(),
        turn_id: "turn-1".into(),
    }
}

/// JSON bodies of every request that reached the mock with this method.
pub(super) async fn requests(server: &MockServer, http_method: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.method.to_string().eq_ignore_ascii_case(http_method))
        .collect()
}

pub(super) fn body_json(request: &Request) -> serde_json::Value {
    serde_json::from_slice(&request.body).unwrap_or(serde_json::Value::Null)
}

/// The `payload_json` field of a multipart request.
pub(super) fn multipart_payload(request: &Request) -> serde_json::Value {
    let raw = String::from_utf8_lossy(&request.body);
    let marker = "name=\"payload_json\"";
    let start = raw.find(marker).expect("payload_json part");
    let after = &raw[start..];
    let body_start = after.find("\r\n\r\n").expect("part body") + 4;
    let body = &after[body_start..];
    let end = body.find("\r\n--").expect("part end");
    serde_json::from_str(&body[..end]).expect("payload json")
}
