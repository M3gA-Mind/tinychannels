use super::*;

fn providers() -> Vec<ProviderDescriptor> {
    vec![
        ProviderDescriptor {
            name: "openai".into(),
            aliases: vec!["oai".into()],
        },
        ProviderDescriptor {
            name: "anthropic".into(),
            aliases: vec!["claude".into()],
        },
    ]
}

#[test]
fn runtime_command_parsing_is_channel_scoped() {
    assert!(supports_runtime_model_switch("telegram"));
    assert!(supports_runtime_model_switch("discord"));
    assert!(!supports_runtime_model_switch("slack"));

    assert_eq!(
        parse_runtime_command("telegram", "/models"),
        Some(ChannelRuntimeCommand::ShowProviders)
    );
    assert_eq!(
        parse_runtime_command("discord", "/models openai"),
        Some(ChannelRuntimeCommand::SetProvider("openai".into()))
    );
    assert_eq!(
        parse_runtime_command("telegram", "/model gpt-5"),
        Some(ChannelRuntimeCommand::SetModel("gpt-5".into()))
    );
    assert_eq!(
        parse_runtime_command("telegram", "/model"),
        Some(ChannelRuntimeCommand::ShowModel)
    );
    assert_eq!(parse_runtime_command("slack", "/models"), None);
    assert_eq!(parse_runtime_command("telegram", "hello"), None);
}

#[test]
fn provider_alias_resolution_matches_registered_names_and_aliases() {
    let providers = providers();
    assert_eq!(
        resolve_provider_alias("OPENAI", &providers).as_deref(),
        Some("openai")
    );
    assert_eq!(
        resolve_provider_alias("claude", &providers).as_deref(),
        Some("anthropic")
    );
    assert!(resolve_provider_alias("   ", &providers).is_none());
    assert!(resolve_provider_alias("missing", &providers).is_none());
}

#[test]
fn legacy_model_help_ignores_stale_workspace_cache() {
    let tempdir = tempfile::tempdir().unwrap();
    let state_dir = tempdir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(
        state_dir.join("models_cache.json"),
        serde_json::json!({
            "entries": [{
                "provider": "openai",
                "models": ["stale-model"]
            }]
        })
        .to_string(),
    )
    .unwrap();

    let current = ChannelRouteSelection {
        provider: "openai".into(),
        model: "configured-model".into(),
    };
    assert!(load_cached_model_preview(tempdir.path(), "openai").is_empty());

    let model_help = build_models_help_response(&current, tempdir.path());
    assert!(model_help.contains("Current provider: `openai`"));
    assert!(model_help.contains("Model catalog is unavailable in this channel"));
    assert!(model_help.contains("View available models in the web UI"));
    assert!(!model_help.contains("refresh the model list"));
    assert!(!model_help.contains("stale-model"));

    let provider_help = build_providers_help_response(&current, &providers());
    assert!(provider_help.contains("Available providers"));
    assert!(provider_help.contains("openai"));
}

#[test]
fn resolved_catalog_preview_preserves_first_provider_order_and_ten_item_limit() {
    let catalog = ModelCatalog {
        entries: vec![
            ModelCatalogEntry {
                provider: "anthropic".into(),
                models: (0..12).map(|i| format!("model-{i}")).collect(),
            },
            ModelCatalogEntry {
                provider: "anthropic".into(),
                models: vec!["duplicate".into()],
            },
        ],
    };
    assert_eq!(
        model_preview(&catalog, "anthropic"),
        (0..10).map(|i| format!("model-{i}")).collect::<Vec<_>>()
    );
    assert!(model_preview(&catalog, "ANTHROPIC").is_empty());
    assert!(
        build_models_help_response_from_catalog(
            &ChannelRouteSelection {
                provider: "missing".into(),
                model: "configured".into()
            },
            &catalog,
        )
        .contains("No cached model list found for `missing`")
    );
    assert!(
        build_models_help_response_from_catalog(
            &ChannelRouteSelection {
                provider: "missing".into(),
                model: "configured".into()
            },
            &catalog,
        )
        .contains("Ask the operator to refresh the model list in the web UI")
    );
}
