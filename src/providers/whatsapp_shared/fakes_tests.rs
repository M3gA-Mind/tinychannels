//! In-memory host ports shared by the WhatsApp adapter tests.

use crate::host::{
    EventSink, MediaMetadata, MediaRepository, MediaScope, MediaStoreRequest, StoredMedia,
    VoiceMediaRequest, VoiceMediaStore,
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use parking_lot::Mutex;
use std::collections::HashMap;
use tinychannels_bus::{
    ChannelIngress, HostReadVoiceMediaResult, HostStoreVoiceMediaResult, MediaKind, MediaReference,
};

/// Scoped media storage that remembers what it was asked to store.
#[derive(Default)]
pub(crate) struct MemoryMedia {
    items: Mutex<HashMap<String, StoredMedia>>,
    pub(crate) stored: Mutex<Vec<MediaMetadata>>,
    pub(crate) read_scopes: Mutex<Vec<MediaScope>>,
    /// When set, `store_media` fails.
    pub(crate) fail_store: Mutex<bool>,
}

impl MemoryMedia {
    /// Seed a readable item and return its opaque id.
    pub(crate) fn seed(
        &self,
        kind: MediaKind,
        content_type: &str,
        file_name: Option<&str>,
        bytes: &[u8],
    ) -> String {
        let id = format!("seed-{}", self.items.lock().len() + 1);
        self.items.lock().insert(
            id.clone(),
            StoredMedia {
                media: MediaReference {
                    opaque_id: Some(id.clone()),
                    kind,
                    content_type: Some(content_type.to_owned()),
                    ..Default::default()
                },
                content_type: content_type.to_owned(),
                file_name: file_name.map(str::to_owned),
                bytes: bytes.to_vec(),
            },
        );
        id
    }
}

#[async_trait::async_trait]
impl MediaRepository for MemoryMedia {
    async fn store_media(&self, request: MediaStoreRequest) -> anyhow::Result<MediaReference> {
        if *self.fail_store.lock() {
            anyhow::bail!("store unavailable");
        }
        let id = format!("stored-{}", self.items.lock().len() + 1);
        let metadata = request.metadata;
        let media = MediaReference {
            opaque_id: Some(id.clone()),
            kind: metadata.kind,
            content_type: Some(metadata.content_type.clone()),
            file_name: metadata.file_name.clone(),
            duration_ms: metadata.duration_ms,
            size_bytes: Some(request.bytes.len() as u64),
            ..Default::default()
        };
        self.items.lock().insert(
            id,
            StoredMedia {
                media: media.clone(),
                content_type: metadata.content_type.clone(),
                file_name: metadata.file_name.clone(),
                bytes: request.bytes,
            },
        );
        self.stored.lock().push(metadata);
        Ok(media)
    }

    async fn read_media(
        &self,
        scope: MediaScope,
        opaque_id: String,
        max_bytes: u64,
    ) -> anyhow::Result<StoredMedia> {
        self.read_scopes.lock().push(scope);
        let item = self
            .items
            .lock()
            .get(&opaque_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown media"))?;
        anyhow::ensure!(item.bytes.len() as u64 <= max_bytes, "media exceeds limit");
        Ok(item)
    }
}

/// Voice storage that remembers inbound notes and serves seeded replies.
#[derive(Default)]
pub(crate) struct MemoryVoices {
    pub(crate) stored: Mutex<Vec<VoiceMediaRequest>>,
    served: Mutex<HashMap<String, (String, String)>>,
    /// When set, `store_voice` returns a reference without an opaque id.
    pub(crate) omit_opaque_id: Mutex<bool>,
}

impl MemoryVoices {
    pub(crate) fn seed(&self, id: &str, mime_type: &str, bytes: &[u8]) {
        self.served_raw(id, mime_type, &BASE64.encode(bytes));
    }

    /// Serve a payload exactly as given (it need not be valid base64).
    pub(crate) fn served_raw(&self, id: &str, mime_type: &str, payload: &str) {
        self.served
            .lock()
            .insert(id.to_owned(), (mime_type.to_owned(), payload.to_owned()));
    }
}

#[async_trait::async_trait]
impl VoiceMediaStore for MemoryVoices {
    async fn store_voice(
        &self,
        request: VoiceMediaRequest,
    ) -> anyhow::Result<HostStoreVoiceMediaResult> {
        let id = format!("voice-{}", self.stored.lock().len() + 1);
        let media = MediaReference {
            opaque_id: (!*self.omit_opaque_id.lock()).then_some(id),
            url: Some("https://provider.invalid/private".into()),
            duration_ms: request.duration_ms,
            ..Default::default()
        };
        self.stored.lock().push(request);
        Ok(HostStoreVoiceMediaResult {
            media,
            accessible_url: None,
        })
    }

    async fn read_voice(
        &self,
        _scope: tinychannels_bus::HostVoiceScope,
        opaque_id: String,
    ) -> anyhow::Result<HostReadVoiceMediaResult> {
        let (mime_type, audio_base64) = self
            .served
            .lock()
            .get(&opaque_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown voice"))?;
        Ok(HostReadVoiceMediaResult {
            audio_base64,
            mime_type,
            file_name: Some("voice.ogg".into()),
        })
    }
}

/// An event sink that records ordered ingress.
pub(crate) struct CollectingSink {
    pub(crate) ingress: Mutex<Vec<ChannelIngress>>,
    /// What `publish_ingress` reports: `false` asks for the legacy path.
    pub(crate) accept: bool,
    pub(crate) fail: bool,
}

impl CollectingSink {
    pub(crate) fn new() -> Self {
        Self {
            ingress: Mutex::new(Vec::new()),
            accept: true,
            fail: false,
        }
    }
}

#[async_trait::async_trait]
impl EventSink for CollectingSink {
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
            anyhow::bail!("host unavailable");
        }
        self.ingress.lock().push(ingress);
        Ok(self.accept)
    }
}

/// A minimal valid Ogg capture pattern followed by padding.
pub(crate) fn ogg_bytes() -> Vec<u8> {
    let mut bytes = b"OggS".to_vec();
    bytes.extend_from_slice(&[0u8; 60]);
    bytes
}

/// A minimal valid PNG (signature, IHDR and IEND), large enough for the host
/// content check.
pub(crate) fn png_bytes() -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
    bytes.extend_from_slice(&[0u8; 40]);
    bytes.extend_from_slice(b"IEND\xaeB`\x82");
    bytes
}
