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
