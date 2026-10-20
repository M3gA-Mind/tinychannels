//! What TinyChannels stores, declared for the host's schema registry.
//!
//! [`storage_schema`] is the one declaration of the `channels` owner: every
//! collection the delivery queue, the remote-control session store and the
//! WhatsApp Web session backend write, plus the steps that bring the v1
//! collections (`channels_delivery_*`, `channel_remote_*`, `whatsapp_*`)
//! forward onto the v2 names. A host builds
//! `Registry::compose([tinychannels_bus::storage::storage_schema(), ..])`, prepares
//! the backend and migrates; a loadable module's storage grant is generated
//! from the same schema (every item is [`ScopeClass::Module`]).
//!
//! The WhatsApp collections are declared whether or not the `whatsapp-web`
//! feature is compiled in: the grant and the migration must not depend on how
//! this crate was built.

use tinystoragedrivers_core::{
    CollectionSpec,
    schema::{Owner, Schema, ScopeClass},
};

use super::migrate;

/// The owner every collection below is prefixed with.
pub const OWNER: &str = "channels";

/// Body version of collections that have legacy v1 layouts and migrate to v2.
pub const SCHEMA_VERSION: u32 = 2;

/// Entries awaiting a terminal outcome.
pub const DELIVERY_PENDING: &str = "channels.delivery_pending";
/// Permanently failed entries.
pub const DELIVERY_FAILED: &str = "channels.delivery_failed";
/// Idempotent terminal results (they expire through record metadata).
pub const DELIVERY_RESULTS: &str = "channels.delivery_results";

/// Chat to thread bindings for remote control.
pub const REMOTE_BINDINGS: &str = "channels.remote_bindings";
/// Chats with a turn in flight.
pub const REMOTE_BUSY: &str = "channels.remote_busy";
/// The one-time legacy import marker.
pub const REMOTE_META: &str = "channels.remote_meta";

/// Scoped metadata for opaque channel media references.
pub const MEDIA_METADATA: &str = "channels.media_metadata";
/// The media metadata collection starts at v1; no earlier granted layout
/// exists to migrate.
pub const MEDIA_METADATA_VERSION: u32 = 1;

/// The canonical collection shape requested by the module's media store.
///
/// Keep this request and the host grant generated from [`storage_schema`]
/// identical so module-scoped storage can validate `ensure_collection`.
#[must_use]
pub fn media_metadata_spec() -> CollectionSpec {
    CollectionSpec::new(MEDIA_METADATA)
}

/// The linked device record.
pub const WHATSAPP_DEVICE: &str = "channels.whatsapp_device";
/// Signal identity keys.
pub const WHATSAPP_IDENTITY: &str = "channels.whatsapp_identity";
/// Signal sessions.
pub const WHATSAPP_SESSION: &str = "channels.whatsapp_session";
/// One-time pre-keys.
pub const WHATSAPP_PREKEY: &str = "channels.whatsapp_prekey";
/// Signed pre-keys.
pub const WHATSAPP_SIGNED_PREKEY: &str = "channels.whatsapp_signed_prekey";
/// Group sender keys.
pub const WHATSAPP_SENDER_KEY: &str = "channels.whatsapp_sender_key";
/// App-state sync keys.
pub const WHATSAPP_SYNC_KEY: &str = "channels.whatsapp_sync_key";
/// LID to phone-number mappings.
pub const WHATSAPP_LID_MAPPING: &str = "channels.whatsapp_lid_mapping";

/// Every WhatsApp Web collection, in declaration order.
pub const WHATSAPP_COLLECTIONS: [&str; 8] = [
    WHATSAPP_DEVICE,
    WHATSAPP_IDENTITY,
    WHATSAPP_SESSION,
    WHATSAPP_PREKEY,
    WHATSAPP_SIGNED_PREKEY,
    WHATSAPP_SENDER_KEY,
    WHATSAPP_SYNC_KEY,
    WHATSAPP_LID_MAPPING,
];

/// Every collection the `channels` owner declares, in declaration order.
pub const COLLECTIONS: [&str; 15] = [
    DELIVERY_PENDING,
    DELIVERY_FAILED,
    DELIVERY_RESULTS,
    REMOTE_BINDINGS,
    REMOTE_BUSY,
    REMOTE_META,
    MEDIA_METADATA,
    WHATSAPP_DEVICE,
    WHATSAPP_IDENTITY,
    WHATSAPP_SESSION,
    WHATSAPP_PREKEY,
    WHATSAPP_SIGNED_PREKEY,
    WHATSAPP_SENDER_KEY,
    WHATSAPP_SYNC_KEY,
    WHATSAPP_LID_MAPPING,
];

/// The `channels` owner's declaration: its collections and the v1 to v2 steps.
///
/// Every item is [`ScopeClass::Module`]: the records belong to the channels
/// module inside a tenant, never to the tenant as a whole.
#[must_use]
pub fn storage_schema() -> Schema {
    let owner = Owner::new(OWNER).unwrap_or_else(|_| unreachable!("`{OWNER}` is a valid owner"));
    let schema = COLLECTIONS.iter().fold(Schema::new(owner), |schema, name| {
        if *name == MEDIA_METADATA {
            schema.collection_with(
                MEDIA_METADATA_VERSION,
                media_metadata_spec(),
                ScopeClass::Module,
            )
        } else {
            schema.collection(name, SCHEMA_VERSION, ScopeClass::Module)
        }
    });
    migrate::steps()
        .into_iter()
        .fold(schema, |schema, step| schema.step(step))
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
