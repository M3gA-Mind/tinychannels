//! iMessage on the v2 adapter contract (macOS: `chat.db` in, `osascript` out).
//!
//! Inbound traffic is polled from the Messages database: text, attachments,
//! Apple audio messages (converted to AAC with `afconvert`, then stored as
//! voice notes), tapbacks (reactions) and read state of what we sent. AppleScript
//! cannot tapback, edit, unsend, type or thread, so those are declared
//! unsupported; sends return the message guid read back from `chat.db`.

mod capabilities;
mod config;
mod db;
mod inbound;
mod listener;
mod outbound;

pub use config::IMessageChannel;

#[cfg(test)]
use db::{fetch_new_messages, get_max_rowid};
#[cfg(test)]
use outbound::{escape_applescript, is_valid_imessage_target};
#[cfg(test)]
use rusqlite::Connection;
#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio::sync::mpsc;

super::native::native_v2_adapter!(IMessageChannel);

#[cfg(test)]
#[path = "fixtures_tests.rs"]
mod fixtures;

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod conformance;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
