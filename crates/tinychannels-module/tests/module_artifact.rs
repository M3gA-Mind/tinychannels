use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::{fs, path::PathBuf};
use tinybus::broker::Broker;
use tinybus::module::ModuleHost;
use tinybus::transport::memory::MemoryTransport;
use tinybus::{Connection, Result};
use tinychannels::relay::{CapabilityDescriptor, ConnectorToGatewayFrame, GatewayToConnectorFrame};
use tinychannels_bus::module_email::EmailSendRequest;
use tinychannels_bus::module_relay::RelayReservation;
use tinychannels_bus::{ChannelOutboundIntent, ChannelsConfig, OutboundPayload, SendMessage};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
struct Callback(Arc<Mutex<Vec<serde_json::Value>>>);

#[tinybus::interface(name = "ai.tinyhumans.tinychannels.ChannelsHost")]
impl Callback {
    async fn deliver_inbound(&self, channel: String, message: serde_json::Value) -> Result<()> {
        self.0
            .lock()
            .await
            .push(serde_json::json!({"kind":"inbound","channel":channel,"message":message}));
        Ok(())
    }

    async fn report_status(
        &self,
        channel: String,
        state: String,
        detail: Option<String>,
    ) -> Result<()> {
        self.0.lock().await.push(
            serde_json::json!({"kind":"status","channel":channel,"state":state,"detail":detail}),
        );
        Ok(())
    }

    async fn persist_allowed_identity(&self, channel: String, identity: String) -> Result<()> {
        self.0
            .lock()
            .await
            .push(serde_json::json!({"kind":"allowlist","channel":channel,"identity":identity}));
        Ok(())
    }
}

#[tokio::test]
#[ignore = "run after cargo build -p tinychannels-module with TINYCHANNELS_MODULE_ARTIFACT set"]
async fn compiled_artifact_exposes_confidential_relay_lifecycle_and_real_websocket_send() {
    let artifact = std::env::var_os("TINYCHANNELS_MODULE_ARTIFACT")
        .expect("set TINYCHANNELS_MODULE_ARTIFACT to the built cdylib");
    let broker = Broker::new();
    let module_host = ModuleHost::new(broker.clone());
    // The local artifact is allowlisted by its current bytes so TinyBus can
    // attest it for this process's confidential-call test. This is a fixture,
    // not a release digest or a host pin.
    let (_staged, staged_artifact) = stage_allowlisted_artifact(PathBuf::from(artifact));
    let info = module_host.load_file(&staged_artifact).unwrap();
    let interface = info
        .manifest
        .provides
        .iter()
        .find(|provided| {
            provided.version.interface.as_str() == "ai.tinyhumans.tinychannels.Channels"
        })
        .unwrap();
    let members: Vec<_> = interface.methods.iter().map(ToString::to_string).collect();
    for member in [
        "StartChannel",
        "StopChannel",
        "SendMessage",
        "ListChannels",
        "ChannelStatus",
        "ReserveRelay",
        "StartRelay",
        "StopRelay",
        "RelayStatus",
        "SendRelayIntent",
        "SendChannelIntent",
        "SendEmail",
        "Shutdown",
    ] {
        assert!(
            members.iter().any(|item| item == member),
            "missing {member}"
        );
    }

    let (client_transport, broker_transport) = MemoryTransport::pair();
    broker.attach(Arc::new(broker_transport));
    let connection = Connection::connect(Box::new(client_transport))
        .await
        .unwrap();
    let callbacks = Callback(Arc::new(Mutex::new(Vec::new())));
    connection
        .serve_at(
            tinychannels_bus::HOST_OBJECT_PATH.try_into().unwrap(),
            callbacks.clone(),
        )
        .await
        .unwrap();
    connection
        .request_name(tinychannels_bus::HOST_BUS_NAME)
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            tinychannels_bus::BUS_NAME,
            tinychannels_bus::OBJECT_PATH,
            tinychannels_bus::BUS_NAME,
        )
        .unwrap();
    // Exercise every original argument shape on the loaded cdylib. The
    // expected NotConfigured/Unknown outcomes prove dispatch reached handlers.
    assert!(
        proxy
            .call::<()>("StartChannel", ("missing", ChannelsConfig::default()))
            .await
            .is_err()
    );
    proxy.call::<()>("StopChannel", ("missing",)).await.unwrap();
    assert!(
        proxy
            .call::<()>(
                "SendMessage",
                ("missing", SendMessage::new("message", "recipient")),
            )
            .await
            .is_err()
    );
    let _: Vec<String> = proxy.call("ListChannels", ()).await.unwrap();
    let status: String = proxy.call("ChannelStatus", ("missing",)).await.unwrap();
    assert_eq!(status, "stopped");
    let (url, remote) = relay_server().await;
    let reservation: RelayReservation = proxy.call("ReserveRelay", ()).await.unwrap();
    let config = tinychannels_bus::config::RelayRuntimeConfig {
        url,
        identities: vec![tinychannels_bus::config::RelayRuntimeIdentityConfig {
            platform: "telegram".into(),
            bot_id: "artifact-fixture".into(),
        }],
        ..Default::default()
    };
    assert!(
        proxy
            .call::<CapabilityDescriptor>("StartRelay", (reservation.clone(), config.clone()))
            .await
            .is_err(),
        "ordinary delivery must be refused for confidential startup"
    );
    assert!(
        proxy
            .call::<tinychannels_bus::controllers::ChannelSendMessageResult>(
                "SendEmail",
                EmailSendRequest {
                    config: Default::default(),
                    recipient: "recipient@example.test".into(),
                    subject: "test".into(),
                    body: "test".into(),
                    attachments: Vec::new(),
                },
            )
            .await
            .is_err(),
        "ordinary delivery must be refused for confidential email"
    );
    let descriptor: CapabilityDescriptor = proxy
        .call_confidential("StartRelay", (reservation.clone(), config))
        .await
        .unwrap();
    assert_eq!(descriptor.platform, "telegram");
    let result: tinychannels_bus::controllers::ChannelSendMessageResult = proxy
        .call(
            "SendRelayIntent",
            (
                reservation.clone(),
                ChannelOutboundIntent {
                    channel_id: "telegram".into(),
                    conversation_id: "chat-1".into(),
                    payload: OutboundPayload::Text {
                        text: "artifact send".into(),
                    },
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.message_id.as_deref(), Some("artifact-message"));
    proxy.call::<()>("StopRelay", (reservation,)).await.unwrap();
    proxy.call::<()>("Shutdown", ()).await.unwrap();
    let _frames = remote.await.unwrap();
    let callback_events = callbacks.0.lock().await;
    assert!(
        callback_events
            .iter()
            .any(|event| event["kind"] == "inbound")
    );
    assert!(
        callback_events
            .iter()
            .any(|event| event["state"] == "connected")
    );
}

struct StagedArtifact(PathBuf);

impl Drop for StagedArtifact {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn stage_allowlisted_artifact(artifact: PathBuf) -> (StagedArtifact, PathBuf) {
    let hash = hex::encode(Sha256::digest(fs::read(&artifact).unwrap()));
    let directory = artifact
        .parent()
        .expect("artifact has a parent directory")
        .join(format!(
            "tinychannels-artifact-test-{}",
            uuid::Uuid::new_v4()
        ));
    fs::create_dir(&directory).unwrap();
    let staged = directory.join(artifact.file_name().unwrap());
    fs::copy(artifact, &staged).unwrap();
    fs::write(
        directory.join("modules.toml"),
        format!(
            "{:?} = {hash:?}\n",
            staged.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();
    (StagedArtifact(directory), staged)
}

async fn relay_server() -> (String, tokio::task::JoinHandle<Vec<serde_json::Value>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap();
        let hello = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            serde_json::from_str::<GatewayToConnectorFrame>(hello.to_text().unwrap()).unwrap(),
            GatewayToConnectorFrame::Hello { .. }
        ));
        let descriptor = CapabilityDescriptor {
            platform: "telegram".into(),
            label: "artifact fixture".into(),
            ..Default::default()
        };
        stream
            .send(Message::Text(
                format!(
                    "{}\n",
                    serde_json::to_string(&ConnectorToGatewayFrame::Descriptor { descriptor })
                        .unwrap()
                )
                .into(),
            ))
            .await
            .unwrap();
        let mut envelope = tinychannels_bus::ChannelInboundEnvelope::default();
        envelope.channel.id = "telegram".into();
        envelope.text = "local inbound".into();
        stream
            .send(Message::Text(
                format!(
                    "{}\n",
                    serde_json::to_string(&ConnectorToGatewayFrame::Inbound {
                        event: serde_json::to_value(envelope).unwrap(),
                        buffer_id: Some("artifact-buffer".into()),
                    })
                    .unwrap()
                )
                .into(),
            ))
            .await
            .unwrap();
        let mut seen = Vec::new();
        while let Some(Ok(message)) = stream.next().await {
            let Ok(text) = message.to_text() else {
                continue;
            };
            let frame: GatewayToConnectorFrame = serde_json::from_str(text).unwrap();
            if let GatewayToConnectorFrame::Outbound { request_id, .. } = &frame {
                stream
                    .send(Message::Text(
                        format!(
                            "{}\n",
                            serde_json::to_string(&ConnectorToGatewayFrame::OutboundResult {
                                request_id: request_id.clone(),
                                result: serde_json::json!({"success":true,"message_id":"artifact-message"}),
                            })
                            .unwrap()
                        )
                        .into(),
                    ))
                    .await
                    .unwrap();
            }
            seen.push(serde_json::to_value(frame).unwrap());
        }
        seen
    });
    (url, task)
}
