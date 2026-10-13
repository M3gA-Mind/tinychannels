//! Runtime command inputs and prepared replies. The host owns route mutations,
//! model policy and catalog storage; the module owns parsing and presentation.
use crate::context::ChannelRouteSelection;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use crate::module_control::ProviderDescriptor;

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

/// Slash-command vocabulary for runtime provider/model switching.
///
/// Parsing is owned by the module; hosts match on this enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "command", content = "value", rename_all = "snake_case")]
pub enum ChannelRuntimeCommand {
    /// `/models`: list providers.
    ShowProviders,
    /// `/models <provider>`: switch provider (raw token).
    SetProvider(String),
    /// `/model`: show the current model.
    ShowModel,
    /// `/model <id>`: switch model (raw argument).
    SetModel(String),
}
