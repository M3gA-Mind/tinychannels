//! Per-sender route selection and runtime command helpers.

use crate::context::ChannelRouteSelection;
use std::fmt::Write;
use std::path::Path;
pub use tinychannels_bus::module_routes::{
    ChannelRuntimeCommand, ModelCatalog, ModelCatalogEntry, ProviderDescriptor,
};

const MODEL_PREVIEW_LIMIT: usize = 10;

#[derive(Debug, Clone)]
pub struct RouteCommandContext {
    pub default_route: ChannelRouteSelection,
    pub providers: Vec<ProviderDescriptor>,
}

pub fn supports_runtime_model_switch(channel_name: &str) -> bool {
    matches!(channel_name, "telegram" | "discord")
}

pub fn parse_runtime_command(channel_name: &str, content: &str) -> Option<ChannelRuntimeCommand> {
    let trimmed = content.trim();
    if !trimmed.starts_with('/') || !supports_runtime_model_switch(channel_name) {
        return None;
    }

    let mut parts = trimmed.split_whitespace();
    let command_token = parts.next()?;
    let base_command = command_token
        .split('@')
        .next()
        .unwrap_or(command_token)
        .to_ascii_lowercase();

    match base_command.as_str() {
        "/models" => {
            if let Some(provider) = parts.next() {
                Some(ChannelRuntimeCommand::SetProvider(
                    provider.trim().to_string(),
                ))
            } else {
                Some(ChannelRuntimeCommand::ShowProviders)
            }
        }
        "/model" => {
            let model = parts.collect::<Vec<_>>().join(" ").trim().to_string();
            if model.is_empty() {
                Some(ChannelRuntimeCommand::ShowModel)
            } else {
                Some(ChannelRuntimeCommand::SetModel(model))
            }
        }
        _ => None,
    }
}

pub fn resolve_provider_alias(name: &str, providers: &[ProviderDescriptor]) -> Option<String> {
    let candidate = name.trim();
    if candidate.is_empty() {
        return None;
    }

    providers
        .iter()
        .find(|provider| {
            provider.name.eq_ignore_ascii_case(candidate)
                || provider
                    .aliases
                    .iter()
                    .any(|alias| alias.eq_ignore_ascii_case(candidate))
        })
        .map(|provider| provider.name.clone())
}

/// Return an empty preview through the legacy workspace-cache API.
///
/// This compatibility shim does not read `models_cache.json`, which is no
/// longer maintained. Use a host-resolved [`ModelCatalog`] to display models.
pub fn load_cached_model_preview(_workspace_dir: &Path, _provider_name: &str) -> Vec<String> {
    Vec::new()
}

/// Select the first exact provider match and preserve its first ten model IDs.
pub fn model_preview(catalog: &ModelCatalog, provider_name: &str) -> Vec<String> {
    catalog
        .entries
        .iter()
        .find(|entry| entry.provider == provider_name)
        .map(|entry| {
            entry
                .models
                .iter()
                .take(MODEL_PREVIEW_LIMIT)
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Build model help for legacy hosts without a resolved model catalog.
///
/// The workspace path is retained for API compatibility but is not read. Users
/// are directed to the web UI because refreshing its catalog cannot populate
/// this catalog-less channel response.
pub fn build_models_help_response(
    current: &ChannelRouteSelection,
    _workspace_dir: &Path,
) -> String {
    models_help_from_preview(
        current,
        &[],
        &format!(
            "Model catalog is unavailable in this channel for `{}`. View available models in the web UI.",
            current.provider
        ),
    )
}

/// Format model help from a host-resolved catalog without reading storage.
pub fn build_models_help_response_from_catalog(
    current: &ChannelRouteSelection,
    catalog: &ModelCatalog,
) -> String {
    models_help_from_preview(
        current,
        &model_preview(catalog, &current.provider),
        &format!(
            "No cached model list found for `{}`. Ask the operator to refresh the model list in the web UI.",
            current.provider
        ),
    )
}

fn models_help_from_preview(
    current: &ChannelRouteSelection,
    cached_models: &[String],
    empty_preview_message: &str,
) -> String {
    let mut response = String::new();
    let _ = writeln!(
        response,
        "Current provider: `{}`\nCurrent model: `{}`",
        current.provider, current.model
    );
    response.push_str("\nSwitch model with `/model <model-id>`.\n");

    if cached_models.is_empty() {
        let _ = writeln!(response, "\n{empty_preview_message}");
    } else {
        let _ = writeln!(
            response,
            "\nCached model IDs (top {}):",
            cached_models.len()
        );
        for model in cached_models {
            let _ = writeln!(response, "- `{model}`");
        }
    }

    response
}

pub fn build_providers_help_response(
    current: &ChannelRouteSelection,
    providers: &[ProviderDescriptor],
) -> String {
    let mut response = String::new();
    let _ = writeln!(
        response,
        "Current provider: `{}`\nCurrent model: `{}`",
        current.provider, current.model
    );
    response.push_str("\nSwitch provider with `/models <provider>`.\n");
    response.push_str("Switch model with `/model <model-id>`.\n\n");
    response.push_str("Available providers:\n");
    for provider in providers {
        if provider.aliases.is_empty() {
            let _ = writeln!(response, "- {}", provider.name);
        } else {
            let _ = writeln!(
                response,
                "- {} (aliases: {})",
                provider.name,
                provider.aliases.join(", ")
            );
        }
    }
    response
}

#[cfg(test)]
#[path = "routes_tests.rs"]
mod tests;
