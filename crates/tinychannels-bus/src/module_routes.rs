//! Runtime command inputs and prepared replies. The host owns route mutations,
//! model policy and catalog storage; the module owns parsing and presentation.
use crate::context::ChannelRouteSelection;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Provider identity and accepted command aliases supplied by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderDescriptor {
    /// Canonical provider name.
    pub name: String,
    /// Case-insensitive alternate names.
    pub aliases: Vec<String>,
}

/// Host-resolved catalog snapshot; no storage paths cross the contract.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ModelCatalog {
    /// Provider model lists in their existing display order.
    pub entries: Vec<ModelCatalogEntry>,
}

/// One provider's cached model IDs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ModelCatalogEntry {
    /// Canonical provider name.
    pub provider: String,
    /// Model IDs in catalog order.
    pub models: Vec<String>,
}

/// Inputs for parsing one message and preparing a help reply or route proposal.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PrepareRuntimeCommandRequest {
    /// Channel identifier; runtime switching remains limited to supported channels.
    pub channel: String,
    /// Unmodified inbound text.
    pub content: String,
    /// Current host-selected route.
    pub current: ChannelRouteSelection,
    /// Available providers and aliases.
    pub providers: Vec<ProviderDescriptor>,
    /// Catalog loaded by the host.
    pub model_catalog: ModelCatalog,
}

/// Prepared command. Applying changes and validating model IDs remains host policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "command", content = "value", rename_all = "snake_case")]
pub enum PreparedRuntimeCommand {
    /// Provider proposal; an unknown alias has no resolved provider.
    SetProvider {
        /// Original token for the host's diagnostic.
        requested: String,
        /// Canonical name, when available.
        provider: Option<String>,
    },
    /// Raw model argument; no policy normalization is applied.
    SetModel(String),
    /// Ready-to-send provider help.
    ShowProviders(String),
    /// Ready-to-send current model and catalog preview.
    ShowModel(String),
}
