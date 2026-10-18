//! Recording host ports shared by the native adapters' tests.

use crate::host::{
    EventSink, MediaMetadata, MediaRepository, MediaScope, MediaStoreRequest, StoredMedia,
    Transcriber, TranscriptionRequest, TranscriptionResult, VoiceMediaRequest, VoiceMediaStore,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Mutex;
use tinychannels_bus::{ChannelEvent, ChannelIngress, MediaKind, MediaReference};

/// Host media repository with one canned stored file.
#[derive(Default)]
pub(crate) struct StubMedia {
    pub(crate) stored: Mutex<Vec<MediaStoreRequest>>,
    pub(crate) fail_store: bool,
    pub(crate) read: Mutex<Option<StoredMedia>>,
    pub(crate) scopes: Mutex<Vec<MediaScope>>,
}

#[async_trait]
impl MediaRepository for StubMedia {
    async fn store_media(&self, request: MediaStoreRequest) -> anyhow::Result<MediaReference> {
        if self.fail_store {
            anyhow::bail!("storage unavailable");
        }
        let MediaMetadata {
            kind,
            content_type,
            file_name,
            ..
        } = request.metadata.clone();
        self.stored.lock().unwrap().push(request);
        Ok(MediaReference {
            opaque_id: Some("media-1".into()),
            kind,
            content_type: Some(content_type),
            file_name,
            ..Default::default()
        })
    }

    async fn read_media(
        &self,
        scope: MediaScope,
        _id: String,
        _max: u64,
    ) -> anyhow::Result<StoredMedia> {
        self.scopes.lock().unwrap().push(scope);
        self.read
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("not found"))
    }
}

pub(crate) fn stored_media(kind: MediaKind, content_type: &str, bytes: &[u8]) -> StoredMedia {
    StoredMedia {
        media: MediaReference {
            opaque_id: Some("media-1".into()),
            kind,
            ..Default::default()
        },
        content_type: content_type.into(),
        file_name: Some("report.pdf".into()),
        bytes: bytes.to_vec(),
    }
}

/// Host voice store that records stored clips and serves one canned upload.
#[derive(Default)]
pub(crate) struct StubVoice {
    pub(crate) stored: Mutex<Vec<VoiceMediaRequest>>,
    pub(crate) empty_reference: bool,
    pub(crate) fail_store: bool,
    pub(crate) audio_base64: Mutex<Option<String>>,
}

#[async_trait]
impl VoiceMediaStore for StubVoice {
    async fn store_voice(
        &self,
        request: VoiceMediaRequest,
    ) -> anyhow::Result<tinychannels_bus::HostStoreVoiceMediaResult> {
        if self.fail_store {
            anyhow::bail!("storage unavailable");
        }
        self.stored.lock().unwrap().push(request);
        Ok(tinychannels_bus::HostStoreVoiceMediaResult {
            media: MediaReference {
                opaque_id: (!self.empty_reference).then(|| "voice-1".into()),
                url: Some("https://private.invalid/x".into()),
                ..Default::default()
            },
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        _opaque_id: String,
    ) -> anyhow::Result<tinychannels_bus::HostReadVoiceMediaResult> {
        let audio_base64 = self
            .audio_base64
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        Ok(tinychannels_bus::HostReadVoiceMediaResult {
            audio_base64,
            mime_type: "audio/mp4".into(),
            file_name: Some("reply.m4a".into()),
        })
    }
}

/// Speech-to-text answering a fixed transcript, or failing.
pub(crate) struct StubTranscriber {
    pub(crate) text: &'static str,
    pub(crate) fail: bool,
}

impl Default for StubTranscriber {
    fn default() -> Self {
        Self {
            text: "hello",
            fail: false,
        }
    }
}

#[async_trait]
impl Transcriber for StubTranscriber {
    fn name(&self) -> &str {
        "stub"
    }

    async fn transcribe(
        &self,
        _request: TranscriptionRequest,
    ) -> anyhow::Result<TranscriptionResult> {
        if self.fail {
            anyhow::bail!("stt unavailable");
        }
        Ok(TranscriptionResult {
            text: self.text.into(),
            language: Some("en".into()),
            duration_secs: None,
        })
    }
}

/// Event sink that records everything published; `deliver` is the answer
/// returned for inbound messages.
#[derive(Default)]
pub(crate) struct RecordingEvents {
    pub(crate) ingress: Mutex<Vec<ChannelIngress>>,
    pub(crate) deliver: bool,
    pub(crate) fail: bool,
}

impl RecordingEvents {
    pub(crate) fn delivering() -> Self {
        Self {
            deliver: true,
            ..Default::default()
        }
    }

    pub(crate) fn events(&self) -> Vec<ChannelEvent> {
        self.ingress
            .lock()
            .unwrap()
            .iter()
            .filter_map(|item| match item {
                ChannelIngress::Event(event) => Some(event.clone()),
                ChannelIngress::Inbound(_) => None,
            })
            .collect()
    }

    pub(crate) fn inbound(&self) -> Vec<tinychannels_bus::ChannelInboundEnvelope> {
        self.ingress
            .lock()
            .unwrap()
            .iter()
            .filter_map(|item| match item {
                ChannelIngress::Inbound(envelope) => Some(envelope.clone()),
                ChannelIngress::Event(_) => None,
            })
            .collect()
    }
}

#[async_trait]
impl EventSink for RecordingEvents {
    async fn publish(&self, _domain: &str, _kind: &str, _payload: Value) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish_ingress(&self, ingress: ChannelIngress) -> anyhow::Result<bool> {
        if self.fail {
            anyhow::bail!("sink unavailable");
        }
        self.ingress.lock().unwrap().push(ingress);
        Ok(self.deliver)
    }
}

/// A configurable [`NativeProvider`] that records what it is asked to deliver.
pub(crate) struct FakeProvider {
    pub(crate) media: Option<std::sync::Arc<dyn MediaRepository>>,
    pub(crate) voice: Option<std::sync::Arc<dyn VoiceMediaStore>>,
    pub(crate) synthesis: bool,
    pub(crate) edits: bool,
    pub(crate) drafts: bool,
    pub(crate) caps: tinychannels_bus::ChannelCapabilities,
    pub(crate) fail_after: Option<usize>,
    /// Deliver every step as one platform message.
    pub(crate) bundle: bool,
    pub(crate) delivered: Mutex<Vec<String>>,
    pub(crate) edited: Mutex<Vec<(String, String)>>,
    pub(crate) deleted: Mutex<Vec<String>>,
}

impl Default for FakeProvider {
    fn default() -> Self {
        let mut caps = tinychannels_bus::ChannelCapabilities::default();
        caps.addressing.reply_to_id = true;
        caps.addressing.thread_id = true;
        Self {
            media: None,
            voice: None,
            synthesis: false,
            edits: true,
            drafts: true,
            caps,
            fail_after: None,
            bundle: false,
            delivered: Mutex::default(),
            edited: Mutex::default(),
            deleted: Mutex::default(),
        }
    }
}

#[async_trait]
impl super::NativeProvider for FakeProvider {
    fn id(&self) -> &'static str {
        "fake"
    }

    fn display(&self) -> &'static str {
        "Fake"
    }

    fn limits(&self) -> super::Limits {
        super::Limits {
            text_chars: 20,
            file_bytes: 16,
            voice_bytes: 8,
        }
    }

    fn media_repository(&self) -> Option<&std::sync::Arc<dyn MediaRepository>> {
        self.media.as_ref()
    }

    fn voice_media_store(&self) -> Option<&std::sync::Arc<dyn VoiceMediaStore>> {
        self.voice.as_ref()
    }

    fn voice_synthesis(&self) -> bool {
        self.synthesis
    }

    fn declared_capabilities(&self) -> tinychannels_bus::ChannelCapabilities {
        self.caps.clone()
    }

    fn can_edit(&self) -> bool {
        self.edits
    }

    fn can_delete(&self) -> bool {
        true
    }

    fn can_draft(&self) -> bool {
        self.drafts
    }

    fn bundles_steps(&self) -> bool {
        self.bundle
    }

    async fn deliver_bundle(
        &self,
        _target: &super::Target,
        steps: &[super::Step],
    ) -> Result<Vec<super::Sent>, tinychannels_bus::channel::ChannelSendError> {
        self.delivered
            .lock()
            .unwrap()
            .push(format!("bundle:{}", steps.len()));
        Ok(vec![super::Sent::new(
            "bundle-1",
            tinychannels_bus::channel::MessageReceiptPartKind::Text,
        )])
    }

    async fn deliver(
        &self,
        _target: &super::Target,
        step: &super::Step,
    ) -> Result<Vec<super::Sent>, tinychannels_bus::channel::ChannelSendError> {
        let mut delivered = self.delivered.lock().unwrap();
        if self
            .fail_after
            .is_some_and(|limit| delivered.len() >= limit)
        {
            return Err(super::pre_dispatch("platform said no"));
        }
        let (label, kind) = match step {
            super::Step::Text(text) => (
                format!("text:{text}"),
                tinychannels_bus::channel::MessageReceiptPartKind::Text,
            ),
            super::Step::File(file) => (
                format!("file:{}", file.name),
                tinychannels_bus::channel::MessageReceiptPartKind::Media,
            ),
            super::Step::Voice(file) => (
                format!("voice:{}", file.name),
                tinychannels_bus::channel::MessageReceiptPartKind::Voice,
            ),
        };
        delivered.push(label);
        Ok(vec![
            super::Sent::new(format!("m{}", delivered.len()), kind)
                .editable(true)
                .deletable(true),
        ])
    }

    async fn edit_text(
        &self,
        address: &super::Address,
        text: &str,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.edited
            .lock()
            .unwrap()
            .push((address.message_id.clone(), text.to_owned()));
        Ok(())
    }

    async fn delete_message(
        &self,
        address: &super::Address,
    ) -> Result<(), tinychannels_bus::channel::ChannelSendError> {
        self.deleted
            .lock()
            .unwrap()
            .push(address.message_id.clone());
        Ok(())
    }
}
