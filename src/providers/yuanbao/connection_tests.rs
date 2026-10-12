use super::*;

#[test]
fn backoff_follows_schedule() {
    assert_eq!(backoff_seconds(1), 1);
    assert_eq!(backoff_seconds(2), 2);
    assert_eq!(backoff_seconds(3), 5);
    assert_eq!(backoff_seconds(6), 60);
    assert_eq!(backoff_seconds(100), 60);
    assert_eq!(backoff_seconds(0), 1);
}

fn cfg() -> YuanbaoConfig {
    let mut c = YuanbaoConfig::default();
    c.app_key = "ak".into();
    c.ws_domain = "wss://example".into();
    c.token = "tok".into();
    c.bot_id = "bot1".into();
    c
}

#[tokio::test]
async fn pending_correlator_times_out() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let err = conn
        .send_and_wait("missing_id", vec![1, 2, 3], Duration::from_millis(20))
        .await
        .unwrap_err();
    // Without a connected socket, send_frame fails first → SendFailed/NotConnected.
    assert!(matches!(
        err,
        YuanbaoError::NotConnected | YuanbaoError::Timeout(_) | YuanbaoError::SendFailed(_)
    ));
}

#[tokio::test]
async fn account_back_fill_picks_up_uid() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    assert_eq!(conn.account().uid, "bot1");
    conn.update_account(|a| a.connect_id = "cid_xyz".into());
    assert_eq!(conn.account().connect_id, "cid_xyz");
}

#[tokio::test]
async fn next_msg_id_is_monotonic_and_prefixed() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let a = conn.next_msg_id("pfx");
    let b = conn.next_msg_id("pfx");
    assert!(a.starts_with("pfx_"));
    assert!(b.starts_with("pfx_"));
    // Suffix is monotonically increasing.
    let na: u64 = a.strip_prefix("pfx_").unwrap().parse().unwrap();
    let nb: u64 = b.strip_prefix("pfx_").unwrap().parse().unwrap();
    assert!(nb > na);
}

#[tokio::test]
async fn initial_state_is_disconnected() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    assert_eq!(conn.state(), ConnectionState::Disconnected);
    assert!(!conn.is_connected());
}

#[tokio::test]
async fn set_state_connected_flips_is_connected_flag() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    conn.set_state(ConnectionState::Connected);
    assert_eq!(conn.state(), ConnectionState::Connected);
    assert!(conn.is_connected());
    conn.set_state(ConnectionState::Reconnecting);
    assert_eq!(conn.state(), ConnectionState::Reconnecting);
    assert!(!conn.is_connected());
}

#[tokio::test]
async fn send_frame_without_socket_returns_not_connected() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let err = conn.send_frame(vec![1, 2, 3]).await.unwrap_err();
    assert!(matches!(err, YuanbaoError::NotConnected));
    let err2 = conn.send_conn_msg(vec![4]).await.unwrap_err();
    assert!(matches!(err2, YuanbaoError::NotConnected));
}

#[tokio::test]
async fn shutdown_clears_pending_and_sets_disconnected() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    conn.set_state(ConnectionState::Connected);
    // Drop a phantom pending entry then shutdown.
    let (phantom_tx, _phantom_rx) = oneshot::channel();
    conn.pending.lock().insert("ghost".into(), phantom_tx);
    conn.shutdown().await;
    assert_eq!(conn.state(), ConnectionState::Disconnected);
    assert!(!conn.is_connected());
    assert!(conn.pending.lock().is_empty());
}

#[test]
fn backoff_caps_at_last_entry_for_huge_attempts() {
    let last = *RECONNECT_DELAYS.last().unwrap();
    assert_eq!(backoff_seconds(RECONNECT_DELAYS.len() as u32 + 5), last);
}

#[tokio::test]
async fn resolve_token_uses_static_token_when_present() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let (token, bot_id, source) = conn.resolve_token().await.unwrap();
    assert_eq!(token, "tok");
    assert_eq!(bot_id, "bot1");
    assert_eq!(source, "");
}

#[tokio::test]
async fn resolve_token_without_token_and_without_sign_manager_errors() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut c = cfg();
    c.token = String::new();
    let conn = YuanbaoConnection::new(c, tx, None);
    match conn.resolve_token().await.unwrap_err() {
        YuanbaoError::AuthFailed(m) => assert!(m.contains("no token"), "got {m}"),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn resolve_token_with_sign_manager_but_no_app_secret_errors() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut c = cfg();
    c.token = String::new();
    c.app_secret = String::new();
    let mgr = SignManager::new(reqwest::Client::new());
    let conn = YuanbaoConnection::new(c, tx, Some(mgr));
    match conn.resolve_token().await.unwrap_err() {
        YuanbaoError::AuthFailed(m) => assert!(m.contains("app_secret"), "got {m}"),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn send_auth_bind_without_socket_returns_not_connected() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let err = conn.send_auth_bind("tok", "bot1", "bot").await.unwrap_err();
    assert!(matches!(err, YuanbaoError::NotConnected));
}

#[tokio::test]
async fn send_auth_bind_falls_back_to_account_uid_when_bot_id_empty() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    // bot_id="" → reads from account.uid (which was seeded from cfg.bot_id="bot1")
    let err = conn.send_auth_bind("tok", "", "").await.unwrap_err();
    assert!(matches!(err, YuanbaoError::NotConnected));
    // Account uid still in place.
    assert_eq!(conn.account().uid, "bot1");
}

#[test]
fn handle_auth_response_rejects_non_binary_message() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let msg = Message::Text("nope".into());
    match conn.handle_auth_response(&msg).unwrap_err() {
        YuanbaoError::AuthFailed(m) => {
            assert!(m.contains("binary"), "got {m}")
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn handle_auth_response_rejects_undecodable_binary() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    // Wholly invalid wire data — decode_conn_msg fails.
    let msg = Message::Binary(vec![0xFF, 0xFF, 0xFF, 0xFF].into());
    let err = conn.handle_auth_response(&msg).unwrap_err();
    // Either Proto decode error or some other surface — must not be Ok.
    assert!(!matches!(err, YuanbaoError::AuthFailed(_) if format!("{err:?}").contains("binary")));
}

/// Regression guard for the post-`connect_once` shutdown short-circuit:
/// once shutdown is signaled, `run()` must not block on the reconnect
/// backoff. We force connect_once to fail synchronously (invalid WS URL),
/// then signal shutdown — total runtime must be well under the first
/// backoff slot (`backoff_seconds(1) == 1s`).
#[tokio::test]
async fn run_exits_promptly_after_shutdown_signal() {
    use std::time::Instant;
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut c = cfg();
    // tokio-tungstenite rejects the URL synchronously — connect_once
    // returns Err in microseconds, putting `run()` on the post-connect
    // cleanup path that the fix targets.
    c.ws_domain = "not-a-valid-ws-url".to_string();
    c.max_reconnect_attempts = 100;
    let conn = YuanbaoConnection::new(c, tx, None);
    let (sd_tx, sd_rx) = watch::channel(false);

    let handle = tokio::spawn(conn.clone().run(sd_rx));
    // Let `run()` enter the loop and attempt connect_once at least once.
    time::sleep(Duration::from_millis(20)).await;

    let started = Instant::now();
    sd_tx.send(true).unwrap();

    // The first reconnect backoff slot is 1s. Without responsive
    // shutdown handling, run() would sleep through it before checking
    // the flag. 500ms gives us comfortable headroom while staying
    // far enough below the backoff to detect a regression.
    let res = time::timeout(Duration::from_millis(500), handle).await;
    res.expect("run() did not exit within 500ms of shutdown signal")
        .expect("run() task panicked");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "run() took {:?} to exit after shutdown — backoff was not skipped",
        started.elapsed()
    );
}

#[tokio::test]
async fn handle_binary_with_garbage_does_not_panic() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    // Should silently log + return — no panic.
    conn.handle_binary(vec![0xFF, 0xFF, 0xFF, 0xFF]).await;
}

// ─── End-to-end against a local mock gateway ────────────────────────

use super::test_support::{
    GatewaySession, MockGateway, auth_bind_ok, encode_frame, fast_timings, gateway_config,
    wait_connected, wait_for_state,
};
use crate::providers::yuanbao::proto::encode_conn_msg;
use crate::providers::yuanbao::wire::{encode_field_string, encode_field_varint};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const HANG_GUARD: Duration = Duration::from_secs(10);

fn close_with(code: u16) -> Message {
    Message::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: "".into(),
    }))
}

fn connect(
    url: &str,
    timings: ConnTimings,
) -> (
    Arc<YuanbaoConnection>,
    mpsc::UnboundedReceiver<InboundEvent>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::with_timings(gateway_config(url), tx, None, timings);
    (conn, rx)
}

/// Run one `connect_once` in the background; the returned watch sender
/// must stay alive or the read loop treats the drop as a shutdown.
fn spawn_connect_once(
    conn: &Arc<YuanbaoConnection>,
) -> (
    watch::Sender<bool>,
    tokio::task::JoinHandle<Result<Option<u16>, YuanbaoError>>,
) {
    let (sd_tx, mut sd_rx) = watch::channel(false);
    let conn = conn.clone();
    let handle = tokio::spawn(async move { conn.connect_once(&mut sd_rx).await });
    (sd_tx, handle)
}

async fn join<T>(handle: tokio::task::JoinHandle<T>) -> T {
    time::timeout(HANG_GUARD, handle)
        .await
        .expect("task hung")
        .expect("task panicked")
}

async fn closed_port_url() -> String {
    let gw = MockGateway::bind().await;
    gw.url.clone()
}

#[tokio::test]
async fn session_handles_pings_pushes_responses_and_stops_on_no_reconnect_code() {
    let gw = MockGateway::bind().await;
    let (conn, mut rx) = connect(&gw.url, fast_timings());
    let (_sd_tx, sd_rx) = watch::channel(false);
    let run = tokio::spawn(conn.clone().run(sd_rx));

    let mut s = gw.accept_authed("cid-1").await;
    wait_connected(&conn).await;
    assert_eq!(conn.account().connect_id, "cid-1");

    // Transport-level ping is answered with a pong carrying the payload.
    s.send(Message::Ping(b"hi".to_vec().into())).await;
    assert_eq!(s.recv().await, Message::Pong(b"hi".to_vec().into()));

    // Text frames are ignored; a push that asks for an ACK gets one and
    // is forwarded to the listener.
    s.send(Message::Text("ignored".into())).await;
    let push = encode_frame(
        cmd_type::PUSH,
        "inbound_message",
        "push-7",
        module::BIZ_PKG,
        true,
        0,
        b"body",
    );
    s.send(Message::Binary(push.into())).await;
    let ack = s.recv_frame().await;
    assert_eq!(ack.cmd_type, cmd_type::PUSH_ACK);
    assert_eq!(ack.msg_id, "push-7");
    match rx.recv().await.unwrap() {
        InboundEvent::Push(frame) => {
            assert_eq!(frame.cmd, "inbound_message");
            assert_eq!(frame.data, b"body");
        }
        InboundEvent::Kickout(_) => unreachable!("expected a push"),
    }

    // Request/response correlation by msg_id.
    let waiter = {
        let conn = conn.clone();
        tokio::spawn(async move {
            let req = encode_conn_msg(cmd_type::REQUEST, "q", 1, "q-1", module::BIZ_PKG, b"x");
            conn.send_and_wait("q-1", req, HANG_GUARD).await
        })
    };
    let req = s.recv_frame().await;
    assert_eq!(req.msg_id, "q-1");
    s.reply(&req, 0, b"answer").await;
    let resp = join(waiter).await.unwrap();
    assert_eq!(resp.data, b"answer");

    // Unanswered request times out and is removed from the correlator.
    let req = encode_conn_msg(cmd_type::REQUEST, "q", 2, "q-2", module::BIZ_PKG, b"x");
    let err = conn
        .send_and_wait("q-2", req, Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(matches!(err, YuanbaoError::Timeout(ref m) if m.contains("q-2")));
    assert!(conn.pending.lock().is_empty());
    assert_eq!(s.recv_frame().await.msg_id, "q-2");

    // A no-reconnect close code ends `run()` without reconnecting.
    s.send(close_with(4012)).await;
    join(run).await;
}

#[tokio::test]
async fn run_reconnects_after_normal_close_and_heartbeats_until_shutdown() {
    let gw = MockGateway::bind().await;
    let mut timings = fast_timings();
    timings.ping_interval = Some(Duration::from_millis(10));
    let (conn, _rx) = connect(&gw.url, timings);
    let (sd_tx, sd_rx) = watch::channel(false);
    let run = tokio::spawn(conn.clone().run(sd_rx));

    let mut first = gw.accept_authed("cid-a").await;
    first.send(close_with(1000)).await;

    // A normal close is followed by a fresh connection.
    let mut second = gw.accept_authed("cid-b").await;
    let ping = loop {
        let frame = second.recv_frame().await;
        if frame.cmd == cmd::PING {
            break frame;
        }
    };
    assert_eq!(ping.cmd_type, cmd_type::REQUEST);
    assert!(ping.msg_id.starts_with("ping_"));
    wait_connected(&conn).await;
    assert_eq!(conn.account().connect_id, "cid-b");

    sd_tx.send(true).unwrap();
    join(run).await;
    assert_eq!(conn.state(), ConnectionState::Disconnected);
    assert!(!conn.is_connected());
    drop(first);
}

#[tokio::test]
async fn run_returns_immediately_when_already_shut_down() {
    let (conn, _rx) = connect("ws://127.0.0.1:1", fast_timings());
    let (_sd_tx, sd_rx) = watch::channel(true);
    join(tokio::spawn(conn.clone().run(sd_rx))).await;
    assert_eq!(conn.state(), ConnectionState::Disconnected);
}

#[tokio::test]
async fn run_gives_up_after_max_reconnect_attempts() {
    let gw = MockGateway::bind().await;
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut c = gateway_config(&gw.url);
    c.max_reconnect_attempts = 2;
    let conn = YuanbaoConnection::with_timings(c, tx, None, fast_timings());
    let (_sd_tx, sd_rx) = watch::channel(false);
    let run = tokio::spawn(conn.clone().run(sd_rx));
    // Both attempts fail during the WebSocket handshake.
    gw.accept_and_drop().await;
    gw.accept_and_drop().await;
    join(run).await;
    assert_eq!(conn.state(), ConnectionState::Disconnected);
}

#[tokio::test]
async fn shutdown_during_backoff_stops_run() {
    let gw = MockGateway::bind().await;
    let mut timings = fast_timings();
    // A backoff far longer than the hang guard: only shutdown can end it.
    timings.backoff_unit = Duration::from_secs(3600);
    let (conn, _rx) = connect(&gw.url, timings);
    let (sd_tx, sd_rx) = watch::channel(false);
    let run = tokio::spawn(conn.clone().run(sd_rx));
    wait_for_state(&conn, ConnectionState::Connecting).await;
    gw.accept_and_drop().await;
    wait_for_state(&conn, ConnectionState::Disconnected).await;
    sd_tx.send(true).unwrap();
    join(run).await;
    assert_eq!(conn.state(), ConnectionState::Disconnected);
}

#[tokio::test]
async fn connect_once_fails_when_gateway_is_unreachable() {
    let url = closed_port_url().await;
    let (conn, _rx) = connect(&url, fast_timings());
    let (_sd_tx, handle) = spawn_connect_once(&conn);
    assert!(matches!(
        join(handle).await,
        Err(YuanbaoError::WebSocket(_))
    ));
}

#[tokio::test]
async fn connect_once_rejects_non_binary_auth_reply() {
    let gw = MockGateway::bind().await;
    let (conn, _rx) = connect(&gw.url, fast_timings());
    let (_sd_tx, handle) = spawn_connect_once(&conn);
    let mut s = gw.accept().await;
    let auth = s.recv_frame().await;
    assert_eq!(auth.cmd, cmd::AUTH_BIND);
    assert_eq!(auth.module, module::CONN_ACCESS);
    assert_eq!(conn.state(), ConnectionState::Authenticating);
    s.send(Message::Text("nope".into())).await;
    let err = join(handle).await.unwrap_err();
    assert!(matches!(err, YuanbaoError::AuthFailed(ref m) if m.contains("binary")));
}

#[tokio::test]
async fn connect_once_times_out_waiting_for_auth_reply() {
    let gw = MockGateway::bind().await;
    let mut timings = fast_timings();
    timings.auth_timeout = Duration::from_millis(30);
    let (conn, _rx) = connect(&gw.url, timings);
    let (_sd_tx, handle) = spawn_connect_once(&conn);
    let mut s = gw.accept().await;
    s.recv_frame().await;
    assert!(matches!(join(handle).await, Err(YuanbaoError::AuthTimeout)));
}

#[tokio::test]
async fn connect_once_fails_when_gateway_drops_during_auth() {
    let gw = MockGateway::bind().await;
    let (conn, _rx) = connect(&gw.url, fast_timings());
    let (_sd_tx, handle) = spawn_connect_once(&conn);
    let mut s = gw.accept().await;
    s.recv_frame().await;
    drop(s);
    assert!(matches!(
        join(handle).await,
        Err(YuanbaoError::WebSocket(_))
    ));
}

#[tokio::test]
async fn read_loop_ends_on_transport_error() {
    let gw = MockGateway::bind().await;
    let (conn, _rx) = connect(&gw.url, fast_timings());
    let (_sd_tx, handle) = spawn_connect_once(&conn);
    let s = gw.accept_authed("cid").await;
    wait_connected(&conn).await;
    // Dropping the server socket without a close handshake is a read error.
    drop(s);
    assert_eq!(join(handle).await.unwrap(), None);
}

#[tokio::test]
async fn read_loop_drops_session_after_consecutive_ping_failures() {
    let gw = MockGateway::bind().await;
    let mut timings = fast_timings();
    timings.ping_interval = Some(Duration::from_millis(5));
    let (conn, _rx) = connect(&gw.url, timings);
    let (_sd_tx, handle) = spawn_connect_once(&conn);
    let _s: GatewaySession = gw.accept_authed("cid").await;
    wait_connected(&conn).await;
    // Losing the write half makes every heartbeat fail to send.
    *conn.sender.lock().await = None;
    assert_eq!(join(handle).await.unwrap(), None);
}

#[tokio::test]
async fn shutdown_releases_waiters_with_send_failed() {
    let gw = MockGateway::bind().await;
    let (conn, _rx) = connect(&gw.url, fast_timings());
    let (_sd_tx, _handle) = spawn_connect_once(&conn);
    let mut s = gw.accept_authed("cid").await;
    wait_connected(&conn).await;
    let waiter = {
        let conn = conn.clone();
        tokio::spawn(async move {
            let req = encode_conn_msg(cmd_type::REQUEST, "q", 1, "q-9", module::BIZ_PKG, b"x");
            conn.send_and_wait("q-9", req, HANG_GUARD).await
        })
    };
    assert_eq!(s.recv_frame().await.msg_id, "q-9");
    conn.shutdown().await;
    let err = join(waiter).await.unwrap_err();
    assert!(matches!(err, YuanbaoError::SendFailed(ref m) if m.contains("q-9")));
    assert!(matches!(s.recv().await, Message::Close(_)));
}

#[tokio::test]
async fn connect_once_signs_token_and_adopts_server_bot_id() {
    let sign = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": {"token": "signed", "bot_id": "srv-bot", "source": "app", "duration": 3600},
            })),
        )
        .mount(&sign)
        .await;
    let gw = MockGateway::bind().await;
    let mut c = gateway_config(&gw.url);
    c.token = String::new();
    c.bot_id = String::new();
    c.app_secret = "secret".into();
    c.api_domain = sign.uri();
    let (tx, _rx) = mpsc::unbounded_channel();
    let mgr = SignManager::new(reqwest::Client::new());
    let conn = YuanbaoConnection::with_timings(c, tx, Some(mgr), fast_timings());
    let (_sd_tx, handle) = spawn_connect_once(&conn);
    let mut s = gw.accept_authed("cid-s").await;
    wait_connected(&conn).await;
    assert_eq!(conn.account().uid, "srv-bot");
    s.send(close_with(4013)).await;
    assert_eq!(join(handle).await.unwrap(), Some(4013));
}

fn auth_frame(status: u32, body: &[u8], frame_cmd: &str) -> Message {
    Message::Binary(
        encode_frame(
            cmd_type::RESPONSE,
            frame_cmd,
            "auth",
            module::CONN_ACCESS,
            false,
            status,
            body,
        )
        .into(),
    )
}

#[test]
fn handle_auth_response_rejects_unexpected_cmd_and_error_codes() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);

    let err = conn
        .handle_auth_response(&auth_frame(0, b"", cmd::PING))
        .unwrap_err();
    assert!(matches!(err, YuanbaoError::AuthFailed(ref m) if m.contains("unexpected cmd")));

    let mut body = Vec::new();
    encode_field_varint(1, 40011, &mut body);
    encode_field_string(2, "bad biz id", &mut body);
    let err = conn
        .handle_auth_response(&auth_frame(0, &body, cmd::AUTH_BIND))
        .unwrap_err();
    assert!(
        matches!(err, YuanbaoError::AuthFailed(ref m) if m.contains("code=40011") && m.contains("bad biz id"))
    );

    // Success without a connect_id leaves the account untouched.
    let mut body = Vec::new();
    encode_field_varint(1, 0, &mut body);
    conn.handle_auth_response(&auth_frame(0, &body, cmd::AUTH_BIND))
        .unwrap();
    assert_eq!(conn.account().connect_id, "");
}

#[test]
fn auth_bind_ok_helper_round_trips_connect_id() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    let req = decode_conn_msg(&encode_conn_msg(
        cmd_type::REQUEST,
        cmd::AUTH_BIND,
        1,
        "auth-x",
        module::CONN_ACCESS,
        b"",
    ))
    .unwrap();
    conn.handle_auth_response(&auth_bind_ok(&req, "cid-z"))
        .unwrap();
    assert_eq!(conn.account().connect_id, "cid-z");
}

#[tokio::test]
async fn push_ack_failure_and_closed_listener_are_tolerated() {
    let (tx, rx) = mpsc::unbounded_channel();
    let conn = YuanbaoConnection::new(cfg(), tx, None);
    drop(rx);
    // No socket → the ACK send fails; no listener → the forward fails.
    // Both are logged, neither panics.
    let push = encode_frame(cmd_type::PUSH, "msg", "p-1", module::BIZ_PKG, true, 0, b"x");
    conn.handle_binary(push).await;
    assert!(conn.pending.lock().is_empty());
}

#[tokio::test]
async fn test_support_helpers_cover_auth_and_routing() {
    assert_eq!(
        test_support::auth_response_success_connect_id_for_test().unwrap(),
        "connect-123"
    );
    assert!(test_support::auth_response_rejects_status_for_test().contains("status=401"));
    assert_eq!(
        test_support::handle_binary_routes_builtin_and_push_frames_for_test().await,
        vec![
            "kickout:logged out".to_string(),
            "push:incoming-message:push-1".to_string()
        ]
    );
}
