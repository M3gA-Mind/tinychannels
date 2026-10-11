use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf, sync::Arc};
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryTransport};
use tinychannels_bus::{BUS_NAME, ChannelRuntimeReservation, HOST_BUS_NAME, OBJECT_PATH, methods};

struct Callback;

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn load_pending_deliveries(
        &self,
    ) -> tinybus::Result<Vec<tinychannels_bus::DeliveryQueueEntry>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn runtime_only_host_closes_scoped_resources_without_a_legacy_callback_object() {
    let artifact = PathBuf::from(std::env::var_os("TINYCHANNELS_MODULE_ARTIFACT").unwrap());
    let directory = artifact
        .parent()
        .unwrap()
        .join(format!("runtime-only-host-{}", uuid::Uuid::new_v4()));
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
    let loaded = modules.load_file(&staged).unwrap();
    assert!(loaded.manifest.provides.iter().any(|p| {
        p.methods
            .iter()
            .any(|m| m.as_str() == methods::RESERVE_RUNTIME)
    }));
    let (client, broker_transport) = MemoryTransport::pair();
    broker.attach(Arc::new(broker_transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    connection.request_name(HOST_BUS_NAME).await.unwrap();
    let proxy = connection.proxy(BUS_NAME, OBJECT_PATH, BUS_NAME).unwrap();
    let first: ChannelRuntimeReservation = proxy
        .call_confidential(methods::RESERVE_RUNTIME, ())
        .await
        .unwrap();
    let second: ChannelRuntimeReservation = proxy
        .call_confidential(methods::RESERVE_RUNTIME, ())
        .await
        .unwrap();
    for runtime in [&first, &second] {
        connection
            .serve_at(
                runtime.host_object_path.clone().try_into().unwrap(),
                Callback,
            )
            .await
            .unwrap();
        proxy
            .call_confidential::<()>(methods::START_RUNTIME, runtime.handle.clone())
            .await
            .unwrap();
    }
    let first_client = connection
        .proxy(BUS_NAME, &first.object_path, BUS_NAME)
        .unwrap();
    let second_client = connection
        .proxy(BUS_NAME, &second.object_path, BUS_NAME)
        .unwrap();
    let _: Vec<String> = first_client.call(methods::LIST_CHANNELS, ()).await.unwrap();
    proxy
        .call_confidential::<()>(methods::STOP_RUNTIME, first.handle)
        .await
        .unwrap();
    assert!(
        first_client
            .call::<Vec<String>>(methods::LIST_CHANNELS, ())
            .await
            .is_err()
    );
    let _: Vec<String> = second_client
        .call(methods::LIST_CHANNELS, ())
        .await
        .unwrap();
    proxy.call::<()>(methods::SHUTDOWN, ()).await.unwrap();
    assert!(
        second_client
            .call::<Vec<String>>(methods::LIST_CHANNELS, ())
            .await
            .is_err()
    );
}

struct StagedArtifact(PathBuf);
impl Drop for StagedArtifact {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
