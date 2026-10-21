//! Deterministic JSON Schema generation for the web event contract.

use super::events::WebChannelEvent;

/// Deterministic JSON Schema document consumed by downstream TS generation.
pub fn web_event_schema_json() -> Result<String, serde_json::Error> {
    let schema = schemars::generate::SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator()
        .into_root_schema_for::<WebChannelEvent>();
    serde_json::to_string_pretty(&schema)
}
