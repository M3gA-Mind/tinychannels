//! Slack channel on the v2 adapter contract.
//!
//! Inbound traffic arrives over Socket Mode (`apps.connections.open` with an
//! app-level token) and falls back to polling `conversations.history` when no
//! app token is configured. Outbound traffic uses the Web API: `chat.postMessage`
//! with Block Kit for buttons and approvals, the external upload flow for files
//! and audio, `reactions.*`, `chat.update`/`chat.delete`, and
//! `assistant.threads.setStatus` for typing.
//!
//! Slack has no native voice-note API. Inbound audio clips are stored as voice
//! notes (with Slack's own transcription when present); outbound voice is an
//! audio file upload, advertised on the descriptor as `voiceOut=file`.

mod adapter;
mod api;
mod blocks;
mod capabilities;
mod events;
mod inbound;
mod interact;
mod outbound;
mod socket;
pub(crate) mod text;
mod types;
mod upload;

pub use types::SlackChannel;

#[cfg(test)]
#[path = "fixtures_tests.rs"]
mod fixtures;
