//! Configuration and host-port setup for iMessage.

use crate::host::{EventSink, MediaRepository, Transcriber, VoiceMediaStore};
use crate::providers::native::HostPorts as NativeState;
use directories::UserDirs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::sync::watch;

pub(super) type ListenerCancellationSlot = Arc<StdMutex<Option<(u64, watch::Sender<bool>)>>>;

pub(super) struct ListenerRegistration {
    pub(super) slot: ListenerCancellationSlot,
    pub(super) generation: u64,
}

impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        let mut active = self.slot.lock().unwrap_or_else(|error| error.into_inner());
        if active
            .as_ref()
            .is_some_and(|(generation, _)| *generation == self.generation)
        {
            *active = None;
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ListenerCursor {
    pub(super) inbound_rowid: i64,
    pub(super) read_date: i64,
    pub(super) read_rowid: i64,
}

/// iMessage channel using macOS `AppleScript` bridge.
/// Polls the Messages database for new messages and sends replies via `osascript`.
#[derive(Clone)]
pub struct IMessageChannel {
    pub(super) allowed_contacts: Vec<String>,
    pub(super) poll_interval: Duration,
    /// Overrides `~/Library/Messages/chat.db`.
    pub(super) db_path: Option<PathBuf>,
    /// The program and leading arguments that run an `AppleScript`;
    /// `-e <script>` is appended.
    pub(super) script_command: Vec<String>,
    /// The program that converts an Apple audio message to AAC; it is called
    /// as `<program> -f m4af -d aac <in> <out>` (macOS `afconvert`).
    pub(super) converter_command: Vec<String>,
    /// How long a send waits for Messages to record the message it made.
    pub(super) resolve_timeout: Duration,
    pub(super) native: NativeState,
    pub(super) listener_cancel: ListenerCancellationSlot,
    pub(super) listener_generation: Arc<AtomicU64>,
    pub(super) listener_cursor: Arc<StdMutex<Option<ListenerCursor>>>,
}

impl IMessageChannel {
    pub fn new(allowed_contacts: Vec<String>) -> Self {
        Self {
            allowed_contacts,
            poll_interval: Duration::from_secs(3),
            db_path: None,
            script_command: vec!["osascript".to_string()],
            converter_command: vec!["afconvert".to_string()],
            resolve_timeout: Duration::from_secs(5),
            native: NativeState::default(),
            listener_cancel: Arc::new(StdMutex::new(None)),
            listener_generation: Arc::new(AtomicU64::new(0)),
            listener_cursor: Arc::new(StdMutex::new(None)),
        }
    }

    /// Scoped media storage; enables typed attachments in both directions.
    pub fn with_media_repository(mut self, repository: Arc<dyn MediaRepository>) -> Self {
        self.native.media_repository = Some(repository);
        self
    }

    /// Durable voice storage; enables audio messages in both directions.
    pub fn with_voice_media_store(mut self, store: Arc<dyn VoiceMediaStore>) -> Self {
        self.native.voice_media_store = Some(store);
        self
    }

    /// Host speech-to-text for inbound audio messages.
    pub fn with_transcriber(mut self, transcriber: Arc<dyn Transcriber>) -> Self {
        self.native.transcriber = Some(transcriber);
        self
    }

    /// Enable shared final-turn synthesis when the host provides TTS, a codec
    /// and durable voice storage.
    pub fn with_voice_synthesis(mut self, enabled: bool) -> Self {
        self.native.voice_synthesis = enabled;
        self
    }

    /// Ordered ingress for typed inbound messages and lifecycle events.
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.native.events = Some(events);
        self
    }

    /// The Messages database: the override, else under the home directory.
    pub(super) fn messages_db_path(&self) -> Option<PathBuf> {
        if let Some(path) = &self.db_path {
            return Some(path.clone());
        }
        UserDirs::new().map(|u| u.home_dir().join("Library/Messages/chat.db"))
    }

    /// Healthy only on macOS with a Messages database present.
    pub(super) fn is_healthy_on(&self, os: &str) -> bool {
        if os != "macos" {
            return false;
        }
        self.messages_db_path().is_some_and(|path| path.exists())
    }
}
