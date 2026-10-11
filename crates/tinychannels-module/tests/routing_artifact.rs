use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf, sync::Arc};
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryTransport};
use tinychannels_bus::{BUS_NAME, OBJECT_PATH};

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_runtime_commands_preserve_alias_unknown_and_help_behavior() {
    let artifact = PathBuf::from(std::env::var_os("TINYCHANNELS_MODULE_ARTIFACT").unwrap());
    let directory = artifact
        .parent()
        .unwrap()
        .join(format!("routing-host-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&directory).unwrap();
    let _staged = StagedArtifact(directory.clone());
    let staged = directory.join(artifact.file_name().unwrap());
    fs::copy(&artifact, &staged).unwrap();
    let hash = hex::encode(Sha256::digest(fs::read(&staged).unwrap()));
    fs::write(
        directory.join("modules.toml"),
        format!(
            "{:?} = {hash:?}\n",
            staged.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();
    let broker = Broker::new();
    let modules = ModuleHost::new(broker.clone());
    let _loaded = modules.load_file(&staged).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let proxy = connection.proxy(BUS_NAME, OBJECT_PATH, BUS_NAME).unwrap();
    let request = |channel: &str, content: &str| {
        serde_json::json!({
            "channel": channel, "content": content,
            "current": {"provider": "anthropic", "model": "claude-fixture"},
            "providers": [{"name": "anthropic", "aliases": ["claude"]}],
            "model_catalog": {"entries": [{"provider": "anthropic", "models": ["first", "second"]}]}
        })
    };
    for (content, expected) in [
        (
            " /MODELS@Fixture ClAuDe ignored ",
            serde_json::json!({"command": "set_provider", "value": {"requested": "ClAuDe", "provider": "anthropic"}}),
        ),
        (
            "/models missing",
            serde_json::json!({"command": "set_provider", "value": {"requested": "missing", "provider": null}}),
        ),
        (
            "/model `custom model`",
            serde_json::json!({"command": "set_model", "value": "`custom model`"}),
        ),
        ("/unknown", serde_json::Value::Null),
        ("ordinary text", serde_json::Value::Null),
    ] {
        let actual: serde_json::Value = proxy
            .call_confidential("PrepareRuntimeCommand", (request("telegram", content),))
            .await
            .unwrap();
        assert_eq!(actual, expected, "command {content:?}");
    }
    let disabled: serde_json::Value = proxy
        .call_confidential("PrepareRuntimeCommand", (request("email", "/models"),))
        .await
        .unwrap();
    assert!(disabled.is_null());
    for (content, expected) in [
        (
            "/models",
            "Current provider: `anthropic`\nCurrent model: `claude-fixture`\n\nSwitch provider with `/models <provider>`.\nSwitch model with `/model <model-id>`.\n\nAvailable providers:\n- anthropic (aliases: claude)\n",
        ),
        (
            "/model",
            "Current provider: `anthropic`\nCurrent model: `claude-fixture`\n\nSwitch model with `/model <model-id>`.\n\nCached model IDs (top 2):\n- `first`\n- `second`\n",
        ),
    ] {
        let actual: serde_json::Value = proxy
            .call_confidential("PrepareRuntimeCommand", (request("discord", content),))
            .await
            .unwrap();
        assert_eq!(
            actual["command"],
            if content == "/models" {
                "show_providers"
            } else {
                "show_model"
            }
        );
        assert_eq!(actual["value"], expected);
    }
    assert!(
        proxy
            .call_confidential::<serde_json::Value>(
                "PrepareRuntimeCommand",
                (serde_json::json!({"channel": 7}),)
            )
            .await
            .is_err()
    );
    proxy
        .call::<()>(tinychannels_bus::methods::SHUTDOWN, ())
        .await
        .unwrap();
}

struct StagedArtifact(PathBuf);
impl Drop for StagedArtifact {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
