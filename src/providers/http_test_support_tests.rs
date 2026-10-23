//! Local TLS fixtures for providers with fixed HTTPS endpoints. The bundled
//! self-signed certificate and private key are test-only, never credentials.

#[allow(unused_imports)]
use super::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug)]
pub(super) struct Request {
    pub method: String,
    pub target: String,
    pub headers: String,
    pub body: serde_json::Value,
    pub raw_body: Vec<u8>,
}

pub(super) struct HttpsServer {
    pub client: reqwest::Client,
    pub requests: Arc<Mutex<Vec<Request>>>,
    /// Loopback address the fixture listens on, for clients that need to
    /// resolve hosts outside the bundled certificate's names.
    // Some all-targets checks compile this fixture without those features.
    #[allow(dead_code)]
    pub address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl HttpsServer {
    pub async fn start(
        respond: impl Fn(&Request) -> (u16, String) + Send + Sync + 'static,
    ) -> Self {
        Self::start_with_body(
            move |request| {
                let (status, body) = respond(request);
                (status, body.into_bytes())
            },
            "application/json",
        )
        .await
    }

    async fn start_with_body(
        respond: impl Fn(&Request) -> (u16, Vec<u8>) + Send + Sync + 'static,
        content_type: &str,
    ) -> Self {
        let content_type = content_type.to_owned();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = include_bytes!("../../tests/fixtures/tls/cert.der");
        let key = include_bytes!("../../tests/fixtures/tls/key.der");
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls_pki_types::CertificateDer::from(cert.to_vec())],
                rustls_pki_types::PrivatePkcs8KeyDer::from(key.to_vec()).into(),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .http1_only()
            .timeout(std::time::Duration::from_secs(5))
            .add_root_certificate(reqwest::Certificate::from_der(cert).unwrap());
        for host in [
            "slack.com",
            "api.dingtalk.com",
            "oapi.dingtalk.com",
            "bots.qq.com",
            "api.sgroup.qq.com",
            "open.feishu.cn",
            "open.larksuite.com",
        ] {
            builder = builder.resolve(host, address);
        }
        let client = builder.build().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let respond = Arc::new(respond);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let acceptor = acceptor.clone();
                let captured = captured.clone();
                let respond = respond.clone();
                let content_type = content_type.clone();
                connections.spawn(async move {
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        let mut stream = acceptor.accept(socket).await.unwrap();
                        let mut bytes = Vec::new();
                        let mut buffer = [0; 4096];
                        let header_end = loop {
                            let count = stream.read(&mut buffer).await.unwrap();
                            assert!(count > 0, "connection closed before HTTP headers");
                            bytes.extend_from_slice(&buffer[..count]);
                            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                                break end + 4;
                            }
                        };
                        let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                        let length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        }).unwrap_or(0);
                        while bytes.len() < header_end + length {
                            let count = stream.read(&mut buffer).await.unwrap();
                            assert!(count > 0, "connection closed before HTTP body");
                            bytes.extend_from_slice(&buffer[..count]);
                        }
                        let mut first_line = headers.lines().next().unwrap().split_whitespace();
                        let request = Request {
                            method: first_line.next().unwrap().to_string(),
                            target: first_line.next().unwrap().to_string(),
                            headers,
                            body: serde_json::from_slice(&bytes[header_end..header_end + length])
                                .unwrap_or(serde_json::Value::Null),
                            raw_body: bytes[header_end..header_end + length].to_vec(),
                        };
                        let (status, body) = respond(&request);
                        captured.lock().unwrap().push(request);
                        let header = format!(
                            "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        stream.write_all(header.as_bytes()).await.unwrap();
                        stream.write_all(&body).await.unwrap();
                    }).await.unwrap();
                });
                while let Some(result) = connections.try_join_next() {
                    result.unwrap();
                }
            }
        });
        Self {
            client,
            address,
            requests,
            task,
        }
    }

    pub async fn start_with_bytes(
        respond: impl Fn(&Request) -> (u16, Vec<u8>) + Send + Sync + 'static,
        content_type: &str,
    ) -> Self {
        Self::start_with_body(respond, content_type).await
    }
}

impl Drop for HttpsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
