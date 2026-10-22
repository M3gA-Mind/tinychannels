//! Pure declarations for collections owned by TinyChannels.

use tinystoragedrivers_types::CollectionSpec;

/// Body version used by the current TinyChannels record layout.
pub const SCHEMA_VERSION: u32 = 2;

/// Entries awaiting a terminal outcome.
pub const DELIVERY_PENDING: &str = "channels.delivery_pending";
/// Permanently failed entries.
pub const DELIVERY_FAILED: &str = "channels.delivery_failed";
/// Idempotent terminal results.
pub const DELIVERY_RESULTS: &str = "channels.delivery_results";
/// Chat to thread bindings for remote control.
pub const REMOTE_BINDINGS: &str = "channels.remote_bindings";
/// Chats with a turn in flight.
pub const REMOTE_BUSY: &str = "channels.remote_busy";
/// The one-time legacy import marker.
pub const REMOTE_META: &str = "channels.remote_meta";
/// Scoped metadata for opaque channel media references.
pub const MEDIA_METADATA: &str = "channels.media_metadata";
/// Blob prefix holding channel media bytes referenced by `MEDIA_METADATA`.
pub const MEDIA_BLOB_PREFIX: &str = "channels/media/";
/// The metadata collection starts at v1; no legacy layout exists to migrate.
pub const MEDIA_METADATA_VERSION: u32 = 1;
/// Owner-local progress for TinyChannels' storage migrations.
pub const MIGRATIONS: &str = "channels.migrations";
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

/// Every collection owned by TinyChannels, in declaration order.
pub const COLLECTIONS: [&str; 16] = [
    DELIVERY_PENDING,
    DELIVERY_FAILED,
    DELIVERY_RESULTS,
    REMOTE_BINDINGS,
    REMOTE_BUSY,
    REMOTE_META,
    MEDIA_METADATA,
    MIGRATIONS,
    WHATSAPP_DEVICE,
    WHATSAPP_IDENTITY,
    WHATSAPP_SESSION,
    WHATSAPP_PREKEY,
    WHATSAPP_SIGNED_PREKEY,
    WHATSAPP_SENDER_KEY,
    WHATSAPP_SYNC_KEY,
    WHATSAPP_LID_MAPPING,
];

/// Canonical collection shape requested by the scoped media repository.
#[must_use]
pub fn media_metadata_spec() -> CollectionSpec {
    CollectionSpec::new(MEDIA_METADATA)
}

/// Return pure declarations for the collections this implementation owns.
#[must_use]
pub fn collections() -> Vec<CollectionSpec> {
    COLLECTIONS
        .iter()
        .map(|name| CollectionSpec::new(*name))
        .collect()
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
