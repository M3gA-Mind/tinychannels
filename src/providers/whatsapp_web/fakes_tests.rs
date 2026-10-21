//! A recording stand-in for a linked WhatsApp session.

use super::client::OutboundClient;
use super::media_build::{self, MediaBuild, UploadedMedia};
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use wacore::download::Downloadable;
use whatsapp_rust::Jid;
use whatsapp_rust::waproto::whatsapp as wa;

/// One operation the adapter asked the session to perform.
#[derive(Debug, Clone)]
pub(super) enum Call {
    Send {
        to: String,
        message: Box<wa::Message>,
        id: String,
    },
    Media {
        build: MediaBuild,
        bytes: usize,
    },
    Reaction {
        chat: String,
        key: wa::MessageKey,
        emoji: String,
    },
    Edit {
        chat: String,
        original_id: String,
        content: Box<wa::Message>,
    },
    Revoke {
        chat: String,
        id: String,
    },
    Read {
        chat: String,
        sender: Option<String>,
        ids: Vec<String>,
    },
    Composing(String),
    Paused(String),
}

#[derive(Default)]
pub(super) struct RecordingClient {
    pub calls: Mutex<Vec<Call>>,
    /// Every operation fails.
    pub fail: bool,
    /// Uploads fail while sends succeed.
    pub fail_media: bool,
    /// Sends after this many successes fail.
    pub fail_sends_after: Option<usize>,
    /// Inbound media by `direct_path`.
    pub downloads: Mutex<HashMap<String, Vec<u8>>>,
    pub account: Mutex<Option<Jid>>,
    pub sent: AtomicUsize,
}

impl RecordingClient {
    pub(super) fn linked(account: &str) -> Self {
        let client = Self::default();
        *client.account.lock() = Some(account.parse().expect("test account JID"));
        client
    }

    pub(super) fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }

    /// `(recipient, text)` for every plain or quoted text message sent.
    pub(super) fn texts(&self) -> Vec<(String, String)> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Send { to, message, .. } => {
                    let text = message
                        .conversation
                        .clone()
                        .or_else(|| message.extended_text_message.text.clone())?;
                    Some((to, text))
                }
                _ => None,
            })
            .collect()
    }

    /// The protocol messages sent, in order.
    pub(super) fn messages(&self) -> Vec<wa::Message> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Send { message, .. } => Some(*message),
                _ => None,
            })
            .collect()
    }
}

#[async_trait]
impl OutboundClient for RecordingClient {
    async fn send_message(&self, to: Jid, message: wa::Message) -> Result<String> {
        if self.fail {
            anyhow::bail!("send boom");
        }
        let n = self.sent.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_sends_after.is_some_and(|limit| n > limit) {
            anyhow::bail!("send boom");
        }
        let id = format!("3EB0{n:04}");
        self.calls.lock().push(Call::Send {
            to: to.to_string(),
            message: Box::new(message),
            id: id.clone(),
        });
        Ok(id)
    }

    async fn media_message(
        &self,
        bytes: Vec<u8>,
        build: MediaBuild,
        context: Option<wa::ContextInfo>,
    ) -> Result<wa::Message> {
        if self.fail || self.fail_media {
            anyhow::bail!("upload boom");
        }
        self.calls.lock().push(Call::Media {
            build: build.clone(),
            bytes: bytes.len(),
        });
        Ok(media_build::build(
            UploadedMedia {
                url: "https://mmg.invalid/upload".into(),
                direct_path: "/v/upload".into(),
                media_key: vec![7; 32],
                file_sha256: vec![8; 32],
                file_enc_sha256: vec![9; 32],
                file_length: bytes.len() as u64,
                media_key_timestamp: 1_700_000_000,
                streaming_sidecar: None,
            },
            build,
            context,
        ))
    }

    async fn download(&self, media: &dyn Downloadable) -> Result<Vec<u8>> {
        if self.fail {
            anyhow::bail!("download boom");
        }
        let key = media.direct_path().unwrap_or_default().to_owned();
        self.downloads
            .lock()
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no such media"))
    }

    async fn send_reaction(&self, chat: Jid, key: wa::MessageKey, emoji: &str) -> Result<String> {
        if self.fail {
            anyhow::bail!("reaction boom");
        }
        self.calls.lock().push(Call::Reaction {
            chat: chat.to_string(),
            key,
            emoji: emoji.to_owned(),
        });
        Ok("3EB0REACT".into())
    }

    async fn edit_message(
        &self,
        chat: Jid,
        original_id: String,
        content: wa::Message,
    ) -> Result<String> {
        if self.fail {
            anyhow::bail!("edit boom");
        }
        self.calls.lock().push(Call::Edit {
            chat: chat.to_string(),
            original_id,
            content: Box::new(content),
        });
        Ok("3EB0EDIT".into())
    }

    async fn revoke_message(&self, chat: Jid, message_id: String) -> Result<()> {
        if self.fail {
            anyhow::bail!("revoke boom");
        }
        self.calls.lock().push(Call::Revoke {
            chat: chat.to_string(),
            id: message_id,
        });
        Ok(())
    }

    async fn mark_read(&self, chat: &Jid, sender: Option<&Jid>, ids: &[String]) -> Result<()> {
        if self.fail {
            anyhow::bail!("read boom");
        }
        self.calls.lock().push(Call::Read {
            chat: chat.to_string(),
            sender: sender.map(ToString::to_string),
            ids: ids.to_vec(),
        });
        Ok(())
    }

    async fn send_composing(&self, to: &Jid) -> Result<()> {
        if self.fail {
            anyhow::bail!("composing boom");
        }
        self.calls.lock().push(Call::Composing(to.to_string()));
        Ok(())
    }

    async fn send_paused(&self, to: &Jid) -> Result<()> {
        if self.fail {
            anyhow::bail!("paused boom");
        }
        self.calls.lock().push(Call::Paused(to.to_string()));
        Ok(())
    }

    fn linked_account(&self) -> Option<Jid> {
        self.account.lock().clone()
    }
}

/// An inbound state with no typed ingress, sending to a fresh channel.
pub(super) fn base_state(
    tx: tokio::sync::mpsc::Sender<crate::traits::ChannelMessage>,
    allowed: &[&str],
) -> super::InboundState {
    use crate::providers::whatsapp_shared::window::{BoundedMap, SeenSet};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    super::InboundState {
        tx,
        typed_tx: None,
        allowed_numbers: allowed.iter().map(|value| (*value).to_owned()).collect(),
        connected: Arc::new(AtomicBool::new(false)),
        allowed_groups: Arc::new(Mutex::new(std::collections::HashSet::new())),
        events: None,
        media_repository: None,
        voice_media_store: None,
        client: Arc::new(Mutex::new(None)),
        seen: Arc::new(Mutex::new(SeenSet::new(64))),
        typed_delivery: Arc::new(tokio::sync::Mutex::new(())),
        authors: Arc::new(Mutex::new(BoundedMap::new(64))),
        identity: Arc::new(super::identity::LinkedIdentity::default()),
    }
}
