//! Channel route selection wire vocabulary.

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct ChannelRouteSelection {
    pub provider: String,
    pub model: String,
}
