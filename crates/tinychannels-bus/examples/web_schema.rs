//! Print the web event contract as deterministic JSON Schema for TS codegen.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", tinychannels_bus::web_event_schema_json()?);
    Ok(())
}
