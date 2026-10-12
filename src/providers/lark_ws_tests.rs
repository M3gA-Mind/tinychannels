//! WebSocket long-connection tests for the Lark channel. The endpoint lookup
//! is answered by the local TLS fixture, which hands out a loopback `ws://`
//! URL served by a scripted pbbp2 gateway.

use super::*;
use crate::providers::http_test_support::HttpsServer;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;

/// Hang guard only; every step below is driven by an explicit read.
const GUARD: Duration = Duration::from_secs(10);

struct Gateway {
    _endpoint: HttpsServer,
    listener: TcpListener,
    channel: LarkChannel,
}

async fn gateway(allowed_users: Vec<String>) -> Gateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint = HttpsServer::start(move |_| {
        let body = serde_json::json!({
            "code": 0,
            "data": {
                "URL": format!("ws://127.0.0.1:{port}/ws?device_id=d1&service_id=42"),
                "ClientConfig": { "PingInterval": 120 }
            }
        });
        (200, body.to_string())
    })
    .await;
    let mut channel = LarkChannel::new(
        "cli_app".into(),
        "secret".into(),
        "token".into(),
        None,
        allowed_users,
    );
    channel.http_client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .timeout(GUARD)
        .danger_accept_invalid_certs(true)
        .resolve("open.feishu.cn", endpoint.address)
        .build()
        .unwrap();
    Gateway {
        _endpoint: endpoint,
        listener,
        channel,
    }
}

async fn accept(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (socket, _) = tokio::time::timeout(GUARD, listener.accept())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(GUARD, tokio_tungstenite::accept_async(socket))
        .await
        .unwrap()
        .unwrap()
}

async fn next_msg(ws: &mut WebSocketStream<TcpStream>) -> WsMsg {
    tokio::time::timeout(GUARD, ws.next())
        .await
        .unwrap()
        .expect("client closed the socket")
        .unwrap()
}

async fn next_frame(ws: &mut WebSocketStream<TcpStream>) -> PbFrame {
    match next_msg(ws).await {
        WsMsg::Binary(bytes) => PbFrame::decode(&bytes[..]).unwrap(),
        other => panic!("expected a binary frame, got {other:?}"),
    }
}

/// Next frame that is not a periodic heartbeat ping.
async fn next_non_ping(ws: &mut WebSocketStream<TcpStream>) -> PbFrame {
    loop {
        let frame = next_frame(ws).await;
        if !(frame.method == 0 && frame.header_value("type") == "ping") {
            return frame;
        }
    }
}

fn frame(seq_id: u64, method: i32, headers: &[(&str, &str)], payload: Option<Vec<u8>>) -> WsMsg {
    let frame = PbFrame {
        seq_id,
        log_id: 7,
        service: 42,
        method,
        headers: headers
            .iter()
            .map(|(key, value)| PbHeader {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect(),
        payload,
    };
    WsMsg::Binary(frame.encode_to_vec().into())
}

fn event(message_id: &str, open_id: &str, message: serde_json::Value) -> Vec<u8> {
    let mut msg = serde_json::json!({
        "message_id": message_id,
        "chat_id": "oc_chat",
        "chat_type": "p2p",
        "message_type": "text",
    });
    msg.as_object_mut()
        .unwrap()
        .extend(message.as_object().unwrap().clone());
    serde_json::json!({
        "header": { "event_type": "im.message.receive_v1", "event_id": format!("ev-{message_id}") },
        "event": {
            "sender": { "sender_id": { "open_id": open_id } },
            "message": msg,
        }
    })
    .to_string()
    .into_bytes()
}

fn text(t: &str) -> serde_json::Value {
    serde_json::json!({ "content": serde_json::json!({ "text": t }).to_string() })
}

/// Send one DATA frame and check the client's immediate ACK.
async fn send_data(
    ws: &mut WebSocketStream<TcpStream>,
    seq_id: u64,
    headers: &[(&str, &str)],
    payload: Vec<u8>,
) {
    ws.send(frame(seq_id, 1, headers, Some(payload)))
        .await
        .unwrap();
    let ack = next_non_ping(ws).await;
    assert_eq!(ack.seq_id, seq_id);
    assert_eq!(ack.method, 1);
    assert_eq!(ack.header_value("biz_rt"), "0");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(ack.payload.as_deref().unwrap()).unwrap(),
        serde_json::json!({"code": 200, "headers": {}, "data": []})
    );
}

async fn send_event(ws: &mut WebSocketStream<TcpStream>, seq_id: u64, payload: Vec<u8>) {
    send_data(ws, seq_id, &[("type", "event")], payload).await;
}

#[tokio::test]
async fn ws_listener_acks_reassembles_filters_and_forwards_messages() {
    let gw = gateway(vec!["ou_alice".into()]).await;
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let channel = gw.channel;
    let listen = tokio::spawn(async move { channel.listen(tx).await });
    let mut ws = accept(&gw.listener).await;

    let initial = next_frame(&mut ws).await;
    assert_eq!(
        (initial.seq_id, initial.method, initial.service),
        (1, 0, 42)
    );
    assert_eq!(initial.header_value("type"), "ping");

    // Transport-level traffic: pings are answered, the rest ignored.
    ws.send(WsMsg::Ping(b"hb".to_vec().into())).await.unwrap();
    assert_eq!(next_msg(&mut ws).await, WsMsg::Pong(b"hb".to_vec().into()));
    ws.send(WsMsg::Pong(Vec::new().into())).await.unwrap();
    ws.send(WsMsg::Text("ignored".into())).await.unwrap();
    ws.send(WsMsg::Binary(vec![0xff, 0xff, 0xff].into()))
        .await
        .unwrap();

    // A pong carrying a new interval restarts the heartbeat, whose first
    // tick sends a ping straight away.
    let pong_cfg = br#"{"PingInterval":15}"#.to_vec();
    ws.send(frame(2, 0, &[("type", "pong")], Some(pong_cfg.clone())))
        .await
        .unwrap();
    let ping = next_frame(&mut ws).await;
    assert_eq!((ping.seq_id, ping.method), (2, 0));
    assert_eq!(ping.header_value("type"), "ping");
    // Same interval again, and a bare pong: no restart.
    ws.send(frame(3, 0, &[("type", "pong")], Some(pong_cfg)))
        .await
        .unwrap();
    ws.send(frame(4, 0, &[("type", "pong")], None))
        .await
        .unwrap();

    // Non-event DATA is acknowledged but not dispatched.
    send_data(&mut ws, 100, &[("type", "card")], b"{}".to_vec()).await;

    // Fragmented event; the first fragment's slot count is superseded.
    let full = event("om_frag", "ou_alice", text("fragmented hello"));
    let (head, tail) = full.split_at(full.len() / 2);
    send_data(
        &mut ws,
        101,
        &[
            ("type", "event"),
            ("message_id", "om_frag"),
            ("sum", "3"),
            ("seq", "0"),
        ],
        head.to_vec(),
    )
    .await;
    send_data(
        &mut ws,
        102,
        &[
            ("type", "event"),
            ("message_id", "om_frag"),
            ("sum", "2"),
            ("seq", "0"),
        ],
        head.to_vec(),
    )
    .await;
    send_data(
        &mut ws,
        103,
        &[
            ("type", "event"),
            ("message_id", "om_frag"),
            ("sum", "2"),
            ("seq", "1"),
        ],
        tail.to_vec(),
    )
    .await;

    // Events that are dropped after the ACK.
    send_event(&mut ws, 104, b"not json".to_vec()).await;
    send_event(
        &mut ws,
        105,
        br#"{"header":{"event_type":"im.chat.disbanded_v1","event_id":"e"},"event":{}}"#.to_vec(),
    )
    .await;
    send_event(
        &mut ws,
        106,
        br#"{"header":{"event_type":"im.message.receive_v1","event_id":"e"},"event":{}}"#.to_vec(),
    )
    .await;
    let mut from_bot: serde_json::Value =
        serde_json::from_slice(&event("om_bot", "ou_alice", text("beep"))).unwrap();
    from_bot["event"]["sender"]["sender_type"] = "bot".into();
    send_event(&mut ws, 107, from_bot.to_string().into_bytes()).await;
    send_event(&mut ws, 108, event("om_m", "ou_mallory", text("let me in"))).await;

    send_event(
        &mut ws,
        109,
        event("om_2", "ou_alice", text("hello @_user_1 there")),
    )
    .await;
    send_event(
        &mut ws,
        110,
        event("om_2", "ou_alice", text("hello @_user_1 there")),
    )
    .await;
    send_event(
        &mut ws,
        111,
        event("om_3", "ou_alice", serde_json::json!({"content": "nope"})),
    )
    .await;
    send_event(&mut ws, 112, event("om_4", "ou_alice", text(""))).await;
    let post = serde_json::json!({
        "en_us": { "title": "Title", "content": [[{"tag": "text", "text": "Body"}]] }
    })
    .to_string();
    send_event(
        &mut ws,
        113,
        event(
            "om_5",
            "ou_alice",
            serde_json::json!({"message_type": "post", "content": post}),
        ),
    )
    .await;
    send_event(
        &mut ws,
        114,
        event(
            "om_6",
            "ou_alice",
            serde_json::json!({"message_type": "post", "content": "{}"}),
        ),
    )
    .await;
    send_event(
        &mut ws,
        115,
        event(
            "om_7",
            "ou_alice",
            serde_json::json!({"message_type": "image", "content": "{}"}),
        ),
    )
    .await;
    send_event(&mut ws, 116, event("om_8", "ou_alice", text("@_user_1"))).await;
    let mut group = text("group without mention");
    group["chat_type"] = "group".into();
    send_event(&mut ws, 117, event("om_9", "ou_alice", group)).await;
    let mut group = text("@_user_1 group hi");
    group["chat_type"] = "group".into();
    group["mentions"] = serde_json::json!([{"key": "@_user_1"}]);
    send_event(&mut ws, 118, event("om_10", "ou_alice", group)).await;

    ws.close(None).await.unwrap();
    tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .expect("a closed socket ends listen cleanly so the caller reconnects");

    let mut delivered = Vec::new();
    while let Ok(message) = rx.try_recv() {
        assert_eq!(message.channel, "lark");
        assert_eq!(message.sender, "oc_chat");
        assert_eq!(message.reply_target, "oc_chat");
        delivered.push(message.content);
    }
    assert_eq!(
        delivered,
        [
            "fragmented hello",
            "hello there",
            "Title\n\nBody",
            "group hi"
        ]
    );
}

#[tokio::test]
async fn ws_listener_stops_when_the_receiver_is_dropped() {
    let gw = gateway(vec!["*".into()]).await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let channel = gw.channel;
    let listen = tokio::spawn(async move { channel.listen(tx).await });
    let mut ws = accept(&gw.listener).await;
    next_frame(&mut ws).await;

    drop(rx);
    send_event(
        &mut ws,
        1,
        event("om_1", "ou_anyone", text("into the void")),
    )
    .await;
    tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn ws_listener_returns_after_an_abrupt_disconnect() {
    let gw = gateway(vec!["*".into()]).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let channel = gw.channel;
    let listen = tokio::spawn(async move { channel.listen(tx).await });
    let mut ws = accept(&gw.listener).await;
    next_frame(&mut ws).await;

    // Drop the TCP socket without a closing handshake.
    drop(ws);
    tokio::time::timeout(GUARD, listen)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
