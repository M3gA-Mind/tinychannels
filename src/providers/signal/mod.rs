//! Signal on the v2 adapter contract, over signal-cli's daemon JSON-RPC and SSE.
//!
//! Beyond text, the adapter ingests and sends attachments and voice notes,
//! reactions, typing, edits and remote deletes, and surfaces read and delivery
//! receipts as events. Inbound attachment bytes come from signal-cli's
//! `getAttachment` JSON-RPC method (base64 `data`); outbound attachments are
//! `data:` URIs in the `send` call's `attachments` array. Signal has no
//! separate voice-note send flag in signal-cli, so outbound voice is an audio
//! attachment (descriptor metadata `voiceOut=attachment`), while inbound audio
//! flagged `voiceNote` becomes a typed voice note.

mod capabilities;
mod config;
mod inbound;
mod listener;
mod outbound;

use config::ListenerCancellationSlot;
pub use config::SignalChannel;
#[cfg(test)]
use listener::{DataMessage, Envelope, GroupInfo, SseEnvelope};
#[cfg(test)]
use outbound::RecipientTarget;
#[cfg(test)]
use reqwest::Client;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use uuid::Uuid;

pub(super) struct ListenerRegistration {
    slot: ListenerCancellationSlot,
    generation: u64,
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

super::native::native_v2_adapter!(SignalChannel);

#[cfg(test)]
#[path = "fixtures_tests.rs"]
mod fixtures;

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod conformance;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
