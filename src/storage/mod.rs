//! Durable state on the tinystoragedrivers ports.
//!
//! Everything TinyChannels must remember across restarts lives here and is
//! written through [`tinystoragedrivers_core::DocumentStore`],
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
mod migrate;
pub mod records;
pub mod schema;
#[cfg(feature = "whatsapp-web")]
pub mod whatsapp;

use std::sync::Arc;

use tinystoragedrivers_core::{CollectionSpec, DocumentStore, StorageError};

pub use delivery::DocumentDeliveryStore;
pub use schema::storage_schema;

const LOG_PREFIX: &str = "[channel-storage]";

/// Declare `collection` on `docs`, best effort.
///
/// A module-side handle may be refused schema rights (only the host runs
/// migrations from declared specs); the collection is then expected to be
/// provisioned already, and a genuine fault surfaces on the first read or
/// write instead. Either way the failure is logged, never fatal here.
pub(crate) async fn ensure_collection(docs: &Arc<dyn DocumentStore>, spec: CollectionSpec) {
    let name = spec.name.clone();
    if let Err(error) = docs.ensure_collection(&spec).await {
        tracing::debug!(
            "{LOG_PREFIX} ensure_collection collection={name} not applied: {:?}",
            error.kind()
        );
    }
}

/// Flatten a storage failure into a message that carries the category only.
pub(crate) fn describe(error: &StorageError) -> String {
    format!("{:?}: {error}", error.kind())
}
