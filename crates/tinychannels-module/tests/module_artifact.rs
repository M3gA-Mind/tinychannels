use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
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
struct Callback(
    Arc<Mutex<Vec<serde_json::Value>>>,
    Arc<Mutex<DeliveryState>>,
);

#[derive(Default)]
struct DeliveryState {
    pending: BTreeMap<String, tinychannels_bus::DeliveryQueueEntry>,
    failed: BTreeMap<String, tinychannels_bus::DeliveryQueueEntry>,
    results: BTreeMap<String, tinychannels_bus::DeliveryTerminalResult>,
}

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

    async fn save_pending_delivery(
        &self,
        entry: tinychannels_bus::DeliveryQueueEntry,
    ) -> Result<()> {
        let mut state = self.1.lock().await;
        if state.pending.len() >= tinychannels_bus::module_delivery::DELIVERY_PENDING_CAPACITY
            && !state.pending.contains_key(&entry.id)
        {
            return Err(tinybus::Error::failed("artifact delivery queue is full"));
        }
        if state
            .pending
            .get(&entry.id)
            .is_some_and(|existing| existing.intent != entry.intent)
        {
            return Err(tinybus::Error::failed("artifact delivery key conflicts"));
        }
        state.pending.insert(entry.id.clone(), entry);
        Ok(())
    }

    async fn load_pending_delivery(
        &self,
        id: String,
    ) -> Result<Option<tinychannels_bus::DeliveryQueueEntry>> {
        Ok(self.1.lock().await.pending.get(&id).cloned())
    }

    async fn load_pending_deliveries(&self) -> Result<Vec<tinychannels_bus::DeliveryQueueEntry>> {
        Ok(self.1.lock().await.pending.values().cloned().collect())
    }

    async fn remove_pending_delivery(&self, id: String) -> Result<()> {
        self.1.lock().await.pending.remove(&id);
        Ok(())
    }

    async fn save_failed_delivery(
        &self,
        entry: tinychannels_bus::DeliveryQueueEntry,
    ) -> Result<()> {
        self.1.lock().await.failed.insert(entry.id.clone(), entry);
        Ok(())
    }

    async fn load_failed_delivery(
        &self,
        id: String,
    ) -> Result<Option<tinychannels_bus::DeliveryQueueEntry>> {
        Ok(self.1.lock().await.failed.get(&id).cloned())
    }

    async fn save_delivery_result(
        &self,
        result: tinychannels_bus::DeliveryTerminalResult,
    ) -> Result<()> {
        let mut state = self.1.lock().await;
        if state.results.len() >= tinychannels_bus::module_delivery::DELIVERY_RESULT_CAPACITY
            && !state.results.contains_key(&result.id)
        {
            return Err(tinybus::Error::failed("artifact result table is full"));
        }
        if state
            .results
            .get(&result.id)
            .is_some_and(|existing| existing != &result)
        {
            return Err(tinybus::Error::failed("artifact result key conflicts"));
        }
        state.results.insert(result.id.clone(), result);
        Ok(())
    }

    async fn load_delivery_result(
        &self,
        id: String,
    ) -> Result<Option<tinychannels_bus::DeliveryTerminalResult>> {
        Ok(self.1.lock().await.results.get(&id).cloned())
    }
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
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
        "BeginEmailUpload",
        "AppendEmailUploadChunk",
        "SendEmailUpload",
        "AbortEmailUpload",
        "StartChannelConfigured",
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
    let callbacks = Callback(
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(DeliveryState::default())),
    );
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
    // The configured member stays callable through the loaded artifact. An
    // absent channel gives the typed configuration error before any provider
    // or network resource is created.
    let configured_args = (
        "missing",
        ChannelsConfig::default(),
        tinychannels_bus::HttpProxyPolicy::default(),
    );
    assert_eq!(
        proxy
            .call::<()>("StartChannelConfigured", configured_args.clone())
            .await
            .unwrap_err()
            .wire_name(),
        tinybus::Error::CONFIDENTIALITY_REQUIRED
    );
    let error = proxy
        .call_confidential::<()>("StartChannelConfigured", configured_args)
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinychannels.Error.NotConfigured"
    );
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
    let required = ChannelOutboundIntent {
        idempotency_key: "artifact-required".into(),
        channel_id: "not-running".into(),
        durability: tinychannels::DeliveryDurability::Required,
        payload: OutboundPayload::Text {
            text: "artifact fixture".into(),
        },
        ..Default::default()
    };
    assert!(
        proxy
            .call::<tinychannels_bus::controllers::ChannelSendMessageResult>(
                "SendChannelIntent",
                ("not-running", required.clone()),
            )
            .await
            .is_err()
    );
    assert!(
        callbacks
            .1
            .lock()
            .await
            .pending
            .contains_key("artifact-required")
    );
    let mut conflict = required;
    conflict.payload = OutboundPayload::Text {
        text: "different artifact fixture".into(),
    };
    assert!(
        proxy
            .call::<tinychannels_bus::controllers::ChannelSendMessageResult>(
                "SendChannelIntent",
                ("not-running", conflict),
            )
            .await
            .is_err()
    );
    let (url, remote) = relay_server().await;
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

    let (smtp_port, smtp_started, smtp_release, near_limit_smtp_server) = smtp_server().await;
    let attachment = vec![u8::MAX; tinychannels_bus::module_email::EMAIL_ATTACHMENT_BYTES];
    let upload: tinychannels_bus::EmailUploadHandle = proxy
        .call_confidential(
            "BeginEmailUpload",
            tinychannels_bus::EmailUploadBegin {
                config: tinychannels_bus::config::EmailConfig {
                    smtp_host: "127.0.0.1".into(),
                    smtp_port,
                    smtp_tls: false,
                    from_address: "from@example.test".into(),
                    ..Default::default()
                },
                recipient: "to@example.test".into(),
                subject: "near limit".into(),
                body: "fixture".into(),
                attachments: vec![tinychannels_bus::EmailAttachmentUpload {
                    filename: "near-limit.bin".into(),
                    content_type: "application/octet-stream".into(),
                    byte_count: attachment.len(),
                }],
            },
        )
        .await
        .unwrap();
    let mut first_chunk = None;
    for (sequence, bytes) in attachment
        .chunks(tinychannels_bus::module_email::EMAIL_UPLOAD_CHUNK_BYTES)
        .enumerate()
    {
        let chunk = tinychannels_bus::EmailUploadChunk {
            handle: upload.clone(),
            attachment_index: 0,
            sequence: sequence as u32,
            bytes: bytes.to_vec(),
        };
        let next: u32 = proxy
            .call_confidential("AppendEmailUploadChunk", chunk.clone())
            .await
            .unwrap();
        assert_eq!(next, sequence as u32 + 1);
        if sequence == 0 {
            first_chunk = Some(chunk);
        }
    }
    let first_chunk = first_chunk.unwrap();
    let replay: u32 = proxy
        .call_confidential("AppendEmailUploadChunk", first_chunk.clone())
        .await
        .unwrap();
    assert_eq!(
        replay,
        attachment
            .len()
            .div_ceil(tinychannels_bus::module_email::EMAIL_UPLOAD_CHUNK_BYTES) as u32,
        "old lost chunk acknowledgements replay safely"
    );
    let mut conflict = first_chunk;
    conflict.bytes[0] = 0;
    assert!(
        proxy
            .call_confidential::<u32>("AppendEmailUploadChunk", conflict)
            .await
            .is_err()
    );
    let send_waiter = tokio::spawn({
        let proxy = proxy.clone();
        let upload = upload.clone();
        async move {
            proxy
                .call_confidential::<tinychannels_bus::controllers::ChannelSendMessageResult>(
                    "SendEmailUpload",
                    upload,
                )
                .await
        }
    });
    smtp_started.await.unwrap();
    send_waiter.abort();
    let _ = send_waiter.await;
    let abort_waiter = tokio::spawn({
        let proxy = proxy.clone();
        let upload = upload.clone();
        async move {
            proxy
                .call_confidential::<()>("AbortEmailUpload", upload)
                .await
        }
    });
    tokio::task::yield_now().await;
    assert!(
        !abort_waiter.is_finished(),
        "abort waits for accepted SMTP work"
    );
    smtp_release.send(()).unwrap();
    assert!(abort_waiter.await.unwrap().is_err());
    let _: tinychannels_bus::controllers::ChannelSendMessageResult = proxy
        .call_confidential("SendEmailUpload", upload)
        .await
        .unwrap();
    let sent_mime = near_limit_smtp_server.await.unwrap();
    assert_eq!(
        sent_mime
            .iter()
            .filter(|line| line.starts_with("DATA"))
            .count(),
        1
    );
    assert_eq!(
        mime_attachment_bytes(&sent_mime, "near-limit.bin"),
        attachment
    );
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
    let (smtp_port, smtp_started, smtp_release, smtp_server) = smtp_server().await;
    let shutdown_upload: tinychannels_bus::EmailUploadHandle = proxy
        .call_confidential(
            "BeginEmailUpload",
            tinychannels_bus::EmailUploadBegin {
                config: tinychannels_bus::config::EmailConfig {
                    smtp_host: "127.0.0.1".into(),
                    smtp_port,
                    smtp_tls: false,
                    from_address: "from@example.test".into(),
                    ..Default::default()
                },
                recipient: "to@example.test".into(),
                subject: "shutdown drain".into(),
                body: "fixture".into(),
                attachments: vec![tinychannels_bus::EmailAttachmentUpload {
                    filename: "shutdown.txt".into(),
                    content_type: "text/plain".into(),
                    byte_count: 8,
                }],
            },
        )
        .await
        .unwrap();
    proxy
        .call_confidential::<u32>(
            "AppendEmailUploadChunk",
            tinychannels_bus::EmailUploadChunk {
                handle: shutdown_upload.clone(),
                attachment_index: 0,
                sequence: 0,
                bytes: b"shutdown".to_vec(),
            },
        )
        .await
        .unwrap();
    let send_waiter = tokio::spawn({
        let proxy = proxy.clone();
        async move {
            proxy
                .call_confidential::<tinychannels_bus::controllers::ChannelSendMessageResult>(
                    "SendEmailUpload",
                    shutdown_upload,
                )
                .await
        }
    });
    smtp_started.await.unwrap();
    send_waiter.abort();
    let _ = send_waiter.await;
    let shutdown = tokio::spawn({
        let proxy = proxy.clone();
        async move { proxy.call::<()>("Shutdown", ()).await }
    });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished(), "shutdown joins accepted SMTP work");
    smtp_release.send(()).unwrap();
    shutdown.await.unwrap().unwrap();
    let shutdown_mime = smtp_server.await.unwrap();
    assert_eq!(
        mime_attachment_bytes(&shutdown_mime, "shutdown.txt"),
        b"shutdown",
        "SMTP transcript: {shutdown_mime:#?}"
    );
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

fn mime_attachment_bytes(lines: &[String], filename: &str) -> Vec<u8> {
    let message = lines.join("\r\n");
    let marker = format!("filename=\"{filename}\"");
    let disposition = message.find(&marker).expect("MIME attachment disposition");
    let body_start = message[disposition..]
        .find("\r\n\r\n")
        .map(|offset| disposition + offset + 4)
        .expect("MIME attachment body");
    let encoded_end = message[body_start..]
        .find("\r\n--")
        .map(|offset| body_start + offset)
        .expect("next MIME boundary");
    let encoded = message[body_start..encoded_end].replace(['\r', '\n'], "");
    let headers = &message[disposition..body_start];
    if headers.contains("Content-Transfer-Encoding: base64") {
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("base64 MIME attachment")
    } else {
        message[body_start..encoded_end]
            .trim_end_matches("\r\n")
            .as_bytes()
            .to_vec()
    }
}

async fn smtp_server() -> (
    u16,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Vec<String>>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut started = Some(started);
        let mut released = Some(released);
        let (socket, _) = listener.accept().await.unwrap();
        let (read, mut write) = socket.into_split();
        let mut lines = BufReader::new(read).lines();
        let mut received = Vec::new();
        write.write_all(b"220 fixture ESMTP\r\n").await.unwrap();
        let mut in_data = false;
        while let Ok(Some(line)) = lines.next_line().await {
            received.push(line.clone());
            if in_data {
                if line == "." {
                    in_data = false;
                    write.write_all(b"250 accepted\r\n").await.unwrap();
                }
                continue;
            }
            let upper = line.to_ascii_uppercase();
            let response = if upper.starts_with("EHLO") {
                b"250-fixture\r\n250 SIZE 30000000\r\n".as_slice()
            } else if upper.starts_with("MAIL") || upper.starts_with("RCPT") {
                b"250 accepted\r\n".as_slice()
            } else if upper.starts_with("DATA") {
                in_data = true;
                if let Some(started) = started.take() {
                    let _ = started.send(());
                }
                b"354 continue\r\n".as_slice()
            } else if upper.starts_with("QUIT") {
                write.write_all(b"221 bye\r\n").await.unwrap();
                break;
            } else {
                b"250 ok\r\n".as_slice()
            };
            write.write_all(response).await.unwrap();
            if upper.starts_with("DATA")
                && let Some(released) = released.take()
            {
                let _ = released.await;
            }
        }
        received
    });
    (port, started_rx, release, task)
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
