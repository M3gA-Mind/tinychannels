//! Per-sender route selection and runtime command helpers.

use crate::context::ChannelRouteSelection;
use std::fmt::Write;
use std::path::Path;
pub use tinychannels_bus::module_routes::{
    ChannelRuntimeCommand, ModelCatalog, ModelCatalogEntry, ProviderDescriptor,
};

const MODEL_CACHE_FILE: &str = "models_cache.json";
const MODEL_CACHE_PREVIEW_LIMIT: usize = 10;

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

pub fn load_cached_model_preview(workspace_dir: &Path, provider_name: &str) -> Vec<String> {
    let cache_path = workspace_dir.join("state").join(MODEL_CACHE_FILE);
    let Ok(raw) = std::fs::read_to_string(cache_path) else {
        return Vec::new();
    };
    let Ok(state) = serde_json::from_str::<ModelCatalog>(&raw) else {
        return Vec::new();
    };

    model_preview(&state, provider_name)
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
                .take(MODEL_CACHE_PREVIEW_LIMIT)
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

pub fn build_models_help_response(current: &ChannelRouteSelection, workspace_dir: &Path) -> String {
    let models = load_cached_model_preview(workspace_dir, &current.provider);
    models_help_from_preview(current, &models)
}

/// Format model help from a host-resolved catalog without reading storage.
pub fn build_models_help_response_from_catalog(
    current: &ChannelRouteSelection,
    catalog: &ModelCatalog,
) -> String {
    models_help_from_preview(current, &model_preview(catalog, &current.provider))
}

fn models_help_from_preview(current: &ChannelRouteSelection, cached_models: &[String]) -> String {
    let mut response = String::new();
    let _ = writeln!(
        response,
        "Current provider: `{}`\nCurrent model: `{}`",
        current.provider, current.model
    );
    response.push_str("\nSwitch model with `/model <model-id>`.\n");

    if cached_models.is_empty() {
        let _ = writeln!(
            response,
            "\nNo cached model list found for `{}`. Ask the operator to refresh the model list in the web UI.",
            current.provider
        );
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
