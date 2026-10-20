//! Local TLS fixtures for providers with fixed HTTPS endpoints. The bundled
//! self-signed certificate and private key are test-only, never credentials.

#[allow(unused_imports)]
use super::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

async fn read_chunked_body<R: AsyncRead + Unpin>(stream: &mut R, pending: &mut Vec<u8>) -> Vec<u8> {
    let mut body = Vec::new();
    loop {
        let line_end = loop {
            if let Some(end) = pending.windows(2).position(|window| window == b"\r\n") {
                break end;
            }
            let mut buffer = [0; 4096];
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0, "connection closed before chunk-size line");
            pending.extend_from_slice(&buffer[..count]);
        };
        let line = String::from_utf8(pending.drain(..line_end).collect()).unwrap();
        pending.drain(..2);
        let size = usize::from_str_radix(line.split(';').next().unwrap().trim(), 16).unwrap();
        if size == 0 {
            loop {
                let line_end = loop {
                    if let Some(end) = pending.windows(2).position(|window| window == b"\r\n") {
                        break end;
                    }
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "connection closed before chunk trailers");
                    pending.extend_from_slice(&buffer[..count]);
                };
                let empty = line_end == 0;
                pending.drain(..line_end + 2);
                if empty {
                    return body;
                }
            }
        }
        while pending.len() < size + 2 {
            let mut buffer = [0; 4096];
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0, "connection closed before chunk data");
            pending.extend_from_slice(&buffer[..count]);
        }
        body.extend(pending.drain(..size));
        assert_eq!(pending.drain(..2).collect::<Vec<_>>(), b"\r\n");
    }
}

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
    /// Loopback address used by provider fixtures that resolve fixed hosts.
    pub address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl HttpsServer {
    pub async fn start(
        respond: impl Fn(&Request) -> (u16, String) + Send + Sync + 'static,
    ) -> Self {
        Self::start_with_content_type(respond, "application/json").await
    }

    pub async fn start_with_content_type(
        respond: impl Fn(&Request) -> (u16, String) + Send + Sync + 'static,
        content_type: &'static str,
    ) -> Self {
        Self::start_with_bytes(
            move |request| {
                let (status, body) = respond(request);
                (status, body.into_bytes())
            },
            content_type,
        )
        .await
    }

    pub async fn start_with_bytes(
        respond: impl Fn(&Request) -> (u16, Vec<u8>) + Send + Sync + 'static,
        content_type: &'static str,
    ) -> Self {
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
                let content_type = content_type.to_owned();
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
                        });
                        let raw_body = if let Some(length) = length {
                            while bytes.len() < header_end + length {
                                let count = stream.read(&mut buffer).await.unwrap();
                                assert!(count > 0, "connection closed before HTTP body");
                                bytes.extend_from_slice(&buffer[..count]);
                            }
                            bytes[header_end..header_end + length].to_vec()
                        } else if headers.to_ascii_lowercase().contains("transfer-encoding: chunked") {
                            let mut pending = bytes[header_end..].to_vec();
                            read_chunked_body(&mut stream, &mut pending).await
                        } else {
                            Vec::new()
                        };
                        let mut first_line = headers.lines().next().unwrap().split_whitespace();
                        let request = Request {
                            method: first_line.next().unwrap().to_string(),
                            target: first_line.next().unwrap().to_string(),
                            headers,
                            body: serde_json::from_slice(&raw_body).unwrap_or(serde_json::Value::Null),
                            raw_body,
                        };
                        captured.lock().unwrap().push(request.clone());
                        let (status, body) = respond(&request);
                        let headers = format!(
                            "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        stream.write_all(headers.as_bytes()).await.unwrap();
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
            requests,
            address,
            task,
        }
    }
}

impl Drop for HttpsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
