//! Durable state on the tinystoragedrivers ports.
//!
//! Everything TinyChannels must remember across restarts lives here and is
//! written through [`tinystoragedrivers_api::DocumentStore`],
//! the port a host hands to a linked library directly and to a loaded module
//! over the storage bus. There is no storage backend in this crate: no file
//! format, no database client, and no host callback per kind of record.
//!
//! * [`delivery`] - the pending, failed and terminal-result records behind
//!   required deliveries.
//! * [`whatsapp`] (feature `whatsapp-web`) - the WhatsApp Web session backend,
//!   so a linked device survives a restart.
//!
//! The remote-control chat bindings use the same port; see
//! [`crate::remote::session_store`].

pub mod delivery;
pub(crate) mod key;
mod migrations;
pub mod records;
pub mod schema;
#[cfg(feature = "whatsapp-web")]
pub mod whatsapp;

use std::sync::Arc;

use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::StorageError;

pub use delivery::DocumentDeliveryStore;
pub use schema::collections;

/// Prepare the collections owned by TinyChannels on an already-bound handle.
///
/// This is owner-local setup: it neither chooses a scope nor composes another
/// owner's declarations. It also copies and removes legacy rows before the
/// owner is opened. Hosts must quiesce writers from the old storage runtime
/// while this migration runs; source collections are rechecked and concurrent
/// growth is bounded, but no document port can fence an unrelated old writer.
/// Storage errors are returned to the opener.
pub async fn prepare(docs: &Arc<dyn DocumentStore>) -> Result<(), StorageError> {
    for spec in collections() {
        docs.ensure_collection(&spec).await?;
    }
    migrations::run(docs.as_ref()).await
}

/// Flatten a storage failure into a message that carries the category only.
pub(crate) fn describe(error: &StorageError) -> String {
    format!("{:?}: {error}", error.kind())
}
