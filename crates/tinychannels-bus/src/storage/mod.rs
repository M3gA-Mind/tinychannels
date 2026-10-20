//! The `channels` owner's storage declaration, carried by the contract.
//!
//! A host composes every owner's schema at boot and generates a module's
//! storage grant from it, so it must be able to call [`storage_schema`] without
//! linking the implementation crate. Everything here is pure: collection
//! names, the v1 to v2 migration steps and the stored-record DTOs they build.
//! The stores that read and write those records stay in `tinychannels`.

mod migrate;
pub mod records;
pub mod schema;

pub use records::ChatBinding;
pub use schema::{MEDIA_METADATA, media_metadata_spec, storage_schema};
