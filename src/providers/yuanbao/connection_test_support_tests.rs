use super::*;
use crate::providers::yuanbao::proto::encode_conn_msg;
use crate::providers::yuanbao::wire::{
    encode_field_bytes, encode_field_string, encode_field_varint,
};

fn cfg() -> YuanbaoConfig {
    let mut c = YuanbaoConfig::default();
    c.app_key = "ak".into();
    c.ws_domain = "wss://example".into();
    c.token = "tok".into();
    c.bot_id = "bot1".into();
    c
}

pub fn auth_response_success_connect_id_for_test() -> Result<String, YuanbaoError> {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let mut body = Vec::new();
    encode_field_varint(1, 0, &mut body);
    encode_field_string(2, "ok", &mut body);
    encode_field_string(3, "connect-123", &mut body);
    let msg = Message::Binary(
        encode_conn_msg(
            cmd_type::RESPONSE,
            cmd::AUTH_BIND,
            1,
            "auth-1",
            module::CONN_ACCESS,
            &body,
        )
        .into(),
    );

    conn.handle_auth_response(&msg)?;
    Ok(conn.account().connect_id)
}

pub fn auth_response_rejects_status_for_test() -> String {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);

    let mut head = Vec::new();
    encode_field_varint(1, cmd_type::RESPONSE as u64, &mut head);
    encode_field_string(2, cmd::AUTH_BIND, &mut head);
    encode_field_string(4, "auth-2", &mut head);
    encode_field_string(5, module::CONN_ACCESS, &mut head);
    encode_field_varint(10, 401, &mut head);

    let mut frame = Vec::new();
    encode_field_bytes(1, &head, &mut frame);
    let msg = Message::Binary(frame.into());

    match conn.handle_auth_response(&msg).unwrap_err() {
        YuanbaoError::AuthFailed(message) => message,
        other => format!("{other:?}"),
    }
}

pub async fn handle_binary_routes_builtin_and_push_frames_for_test() -> Vec<String> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);

    conn.handle_binary(encode_conn_msg(
        cmd_type::RESPONSE,
        biz_cmd::QUERY_GROUP_INFO,
        1,
        "orphan-response",
        module::BIZ_PKG,
        b"response",
    ))
    .await;
    conn.handle_binary(encode_conn_msg(
        cmd_type::PUSH,
        cmd::UPDATE_META,
        2,
        "meta",
        module::CONN_ACCESS,
        b"ignored",
    ))
    .await;
    conn.handle_binary(encode_conn_msg(
        cmd_type::REQUEST,
        biz_cmd::SEND_C2C_MESSAGE,
        3,
        "request",
        module::BIZ_PKG,
        b"not-a-push",
    ))
    .await;
    conn.handle_binary(encode_conn_msg(
        cmd_type::PUSH,
        cmd::KICKOUT,
        4,
        "kick",
        module::CONN_ACCESS,
        b"logged out",
    ))
    .await;
    conn.handle_binary(encode_conn_msg(
        cmd_type::PUSH,
        "incoming-message",
        5,
        "push-1",
        module::BIZ_PKG,
        b"payload",
    ))
    .await;

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        match event {
            InboundEvent::Kickout(reason) => events.push(format!("kickout:{reason}")),
            InboundEvent::Push(frame) => {
                events.push(format!("push:{}:{}", frame.cmd, frame.msg_id));
            }
        }
    }
    events
}

/// Encode a `ConnMsg` with full control over the head (`need_ack`,
/// `status`), which `encode_conn_msg` does not expose.
pub fn encode_frame(
    frame_cmd_type: u32,
    frame_cmd: &str,
    msg_id: &str,
    frame_module: &str,
    need_ack: bool,
    status: u32,
    data: &[u8],
) -> Vec<u8> {
    let mut head = Vec::new();
    encode_field_varint(1, frame_cmd_type as u64, &mut head);
    encode_field_string(2, frame_cmd, &mut head);
    encode_field_string(4, msg_id, &mut head);
    encode_field_string(5, frame_module, &mut head);
    encode_field_varint(6, need_ack as u64, &mut head);
    encode_field_varint(10, status as u64, &mut head);
    let mut frame = Vec::new();
    encode_field_bytes(1, &head, &mut frame);
    encode_field_bytes(2, data, &mut frame);
    frame
}

/// Encode a successful `auth-bind` response body carrying `connect_id`.
pub fn auth_bind_ok(req: &ConnFrame, connect_id: &str) -> Message {
    let mut body = Vec::new();
    encode_field_varint(1, 0, &mut body);
    encode_field_string(2, "ok", &mut body);
    encode_field_string(3, connect_id, &mut body);
    Message::Binary(
        encode_frame(
            cmd_type::RESPONSE,
            cmd::AUTH_BIND,
            &req.msg_id,
            module::CONN_ACCESS,
            false,
            0,
            &body,
        )
        .into(),
    )
}

/// A local WebSocket server that speaks the Yuanbao gateway protocol, so
/// tests drive `YuanbaoConnection` end to end without a network.
pub struct MockGateway {
    /// `ws://127.0.0.1:<port>` — use as `YuanbaoConfig::ws_domain`.
    pub url: String,
    listener: tokio::net::TcpListener,
}

impl MockGateway {
    pub async fn bind() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        Self { url, listener }
    }

    /// Accept the next client connection and complete the WS handshake.
    pub async fn accept(&self) -> GatewaySession {
        let (socket, _) = self.listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        GatewaySession { ws }
    }

    /// Accept the next client and answer its `auth-bind` successfully.
    pub async fn accept_authed(&self, connect_id: &str) -> GatewaySession {
        let mut session = self.accept().await;
        let auth = session.recv_frame().await;
        assert_eq!(auth.cmd, cmd::AUTH_BIND);
        session.send(auth_bind_ok(&auth, connect_id)).await;
        session
    }

    /// Accept a raw TCP connection and drop it before the WS handshake.
    pub async fn accept_and_drop(&self) {
        let (socket, _) = self.listener.accept().await.unwrap();
        drop(socket);
    }
}

/// One accepted gateway-side WebSocket.
pub struct GatewaySession {
    pub ws: WebSocketStream<TcpStream>,
}

impl GatewaySession {
    pub async fn send(&mut self, msg: Message) {
        self.ws.send(msg).await.unwrap();
    }

    /// Next raw message from the client.
    pub async fn recv(&mut self) -> Message {
        self.ws.next().await.unwrap().unwrap()
    }

    /// Next message from the client, decoded as a `ConnMsg`.
    pub async fn recv_frame(&mut self) -> ConnFrame {
        decode_conn_msg(&self.recv().await.into_data()).unwrap()
    }

    /// Answer `req` with a `Response` frame carrying `status` and `data`.
    pub async fn reply(&mut self, req: &ConnFrame, status: u32, data: &[u8]) {
        let frame = encode_frame(
            cmd_type::RESPONSE,
            &req.cmd,
            &req.msg_id,
            &req.module,
            false,
            status,
            data,
        );
        self.send(Message::Binary(frame.into())).await;
    }
}

/// Spin until `conn` reports `Connected` (bounded by a hang guard).
pub async fn wait_connected(conn: &YuanbaoConnection) {
    wait_for_state(conn, ConnectionState::Connected).await;
}

/// Spin until `conn` reaches `state` (bounded by a hang guard).
pub async fn wait_for_state(conn: &YuanbaoConnection, state: ConnectionState) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while conn.state() != state {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("connection never reached the expected state");
}

/// Timings that make reconnect immediate and never fire a heartbeat
/// unless a test asks for one.
pub fn fast_timings() -> ConnTimings {
    ConnTimings {
        auth_timeout: Duration::from_secs(10),
        ping_interval: Some(Duration::from_secs(3600)),
        backoff_unit: Duration::from_millis(1),
    }
}

/// Config pointed at `url` with a static token (no sign endpoint).
pub fn gateway_config(url: &str) -> YuanbaoConfig {
    let mut c = cfg();
    c.ws_domain = url.to_string();
    c
}
