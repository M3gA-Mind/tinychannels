use super::*;

#[test]
fn target_parse_dm() {
    match Target::parse("user_42") {
        Target::Dm(uid) => assert_eq!(uid, "user_42"),
        _ => panic!("should be DM"),
    }
}

#[test]
fn target_parse_group() {
    match Target::parse("g:room_99") {
        Target::Group(c) => assert_eq!(c, "room_99"),
        _ => panic!("should be group"),
    }
}

#[test]
fn target_parse_empty_dm() {
    assert!(matches!(Target::parse(""), Target::Dm("")));
}

#[test]
fn extract_filename_strips_query() {
    assert_eq!(extract_filename("https://x.com/a/b/cat.png"), "cat.png");
    assert_eq!(
        extract_filename("https://x.com/a/b/cat.png?sig=abc"),
        "cat.png"
    );
    assert_eq!(extract_filename("https://x.com/"), "file");
    assert_eq!(extract_filename(""), "file");
}

#[test]
fn extract_filename_from_bare_path() {
    // Not a valid URL → fall back to last non-empty `/`-segment.
    assert_eq!(extract_filename("/var/log/foo.bin"), "foo.bin");
    // Trailing slash gets skipped; last non-empty segment wins.
    assert_eq!(extract_filename("/var/log/"), "log");
    // Plain filename with no slashes.
    assert_eq!(extract_filename("plain.txt"), "plain.txt");
}

fn make_conn(cfg: super::super::config::YuanbaoConfig) -> Arc<YuanbaoConnection> {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    YuanbaoConnection::new(cfg, tx, None)
}

fn base_cfg() -> super::super::config::YuanbaoConfig {
    let mut c = super::super::config::YuanbaoConfig::default();
    c.app_key = "ak".into();
    c.ws_domain = "wss://x".into();
    c.token = "tok".into();
    c.bot_id = "cfg-bot".into();
    c
}

#[tokio::test]
async fn resolve_from_account_uses_config_bot_id_when_no_sign_manager() {
    let conn = make_conn(base_cfg());
    let sender = OutboundSender::new(conn, None, "ak".into(), "cfg-bot".into());
    assert_eq!(sender.resolve_from_account().await, "cfg-bot");
}

#[tokio::test]
async fn resolve_from_account_uses_sign_cache_when_bot_id_present() {
    let conn = make_conn(base_cfg());
    let mgr = super::super::sign::SignManager::new(reqwest::Client::new());
    // Seed the cache with a bot_id keyed on the same app_key.
    mgr.set_cached_for_test(
        "ak",
        super::super::sign::TokenEntry {
            token: "tok".into(),
            bot_id: "server-bot".into(),
            product: String::new(),
            source: "bot".into(),
            expire_ts: u64::MAX / 2,
        },
    )
    .await;
    let sender = OutboundSender::new(conn, Some(mgr), "ak".into(), "fallback-bot".into());
    // Sign cache hit → use server bot_id, not the fallback.
    assert_eq!(sender.resolve_from_account().await, "server-bot");
}

#[tokio::test]
async fn resolve_from_account_falls_back_when_sign_cache_bot_id_empty() {
    let conn = make_conn(base_cfg());
    let mgr = super::super::sign::SignManager::new(reqwest::Client::new());
    mgr.set_cached_for_test(
        "ak",
        super::super::sign::TokenEntry {
            token: "tok".into(),
            bot_id: String::new(),
            product: String::new(),
            source: String::new(),
            expire_ts: u64::MAX / 2,
        },
    )
    .await;
    let sender = OutboundSender::new(conn, Some(mgr), "ak".into(), "fallback-bot".into());
    assert_eq!(sender.resolve_from_account().await, "fallback-bot");
}

// ─── End-to-end against a local mock gateway ────────────────────────

use super::super::connection::test_support::{
    GatewaySession, MockGateway, fast_timings, gateway_config, wait_connected,
};
use super::super::proto_constants::{biz_cmd, cmd_type};
use super::super::types::ConnFrame;
use super::super::wire::{encode_field_bytes, encode_field_string, encode_field_varint};
use tokio::sync::watch;

const HANG_GUARD: Duration = Duration::from_secs(10);

/// A sender wired to a live connection on a mock gateway. Keep the
/// watch sender alive: dropping it shuts the connection down.
struct Harness {
    sender: Arc<OutboundSender>,
    gateway: GatewaySession,
    _shutdown: watch::Sender<bool>,
}

async fn harness(sender_bot_id: &str) -> Harness {
    let gw = MockGateway::bind().await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = YuanbaoConnection::with_timings(gateway_config(&gw.url), tx, None, fast_timings());
    let (sd_tx, sd_rx) = watch::channel(false);
    tokio::spawn(conn.clone().run(sd_rx));
    let gateway = gw.accept_authed("cid").await;
    wait_connected(&conn).await;
    let sender = Arc::new(OutboundSender::new(
        conn,
        None,
        "ak".into(),
        sender_bot_id.into(),
    ));
    Harness {
        sender,
        gateway,
        _shutdown: sd_tx,
    }
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|w| w == needle.as_bytes())
}

/// Run `op` on the sender while the gateway answers the single request it
/// produces with `status`/`data`. Returns the op's result and the request.
async fn round_trip<T, F, Fut>(
    h: &mut Harness,
    status: u32,
    data: &[u8],
    op: F,
) -> (Result<T, YuanbaoError>, ConnFrame)
where
    T: Send + 'static,
    F: FnOnce(Arc<OutboundSender>) -> Fut,
    Fut: std::future::Future<Output = Result<T, YuanbaoError>> + Send + 'static,
{
    let task = tokio::spawn(op(h.sender.clone()));
    let req = h.gateway.recv_frame().await;
    assert_eq!(req.cmd_type, cmd_type::REQUEST);
    h.gateway.reply(&req, status, data).await;
    let res = tokio::time::timeout(HANG_GUARD, task)
        .await
        .expect("send hung")
        .expect("send panicked");
    (res, req)
}

#[tokio::test]
async fn send_text_to_dm_returns_msg_id_on_ack() {
    let mut h = harness("cfg-bot").await;
    let (res, req) = round_trip(&mut h, 0, b"", |s| async move {
        s.send_text("user-1", "hello there", None).await
    })
    .await;
    let msg_id = res.unwrap();
    assert!(msg_id.starts_with("om_"));
    assert_eq!(req.msg_id, msg_id);
    assert_eq!(req.cmd, biz_cmd::SEND_C2C_MESSAGE);
    assert!(contains(&req.data, "user-1"));
    assert!(contains(&req.data, "cfg-bot"));
    assert!(contains(&req.data, "hello there"));
}

#[tokio::test]
async fn send_text_to_group_surfaces_server_rejection() {
    let mut h = harness("cfg-bot").await;
    let (res, req) = round_trip(&mut h, 7, b"", |s| async move {
        s.send_text("g:room-9", "hi group", Some("ref-1")).await
    })
    .await;
    assert_eq!(req.cmd, biz_cmd::SEND_GROUP_MESSAGE);
    assert!(contains(&req.data, "room-9"));
    assert!(contains(&req.data, "ref-1"));
    let err = res.unwrap_err();
    assert!(matches!(err, YuanbaoError::SendFailed(ref m) if m.contains("status=7")));
}

#[tokio::test]
async fn send_image_and_file_urls_carry_the_url() {
    let mut h = harness("cfg-bot").await;
    let (res, req) = round_trip(&mut h, 0, b"", |s| async move {
        s.send_image_url("user-1", "https://img.example/a.png", 10, 2, 3, "image/png")
            .await
    })
    .await;
    res.unwrap();
    assert!(contains(&req.data, "https://img.example/a.png"));

    let (res, req) = round_trip(&mut h, 0, b"", |s| async move {
        s.send_file_url("g:room", "https://files.example/r.pdf", "r.pdf", 99)
            .await
    })
    .await;
    res.unwrap();
    assert_eq!(req.cmd, biz_cmd::SEND_GROUP_MESSAGE);
    assert!(contains(&req.data, "https://files.example/r.pdf"));
    assert!(contains(&req.data, "r.pdf"));
}

#[tokio::test]
async fn heartbeats_are_fire_and_forget_for_dm_and_group() {
    let mut h = harness("cfg-bot").await;
    h.sender.start_heartbeat("user-1").await.unwrap();
    let frame = h.gateway.recv_frame().await;
    assert_eq!(frame.cmd, biz_cmd::SEND_PRIVATE_HEARTBEAT);
    assert!(frame.msg_id.starts_with("hb_"));
    assert!(contains(&frame.data, "user-1"));

    h.sender.stop_heartbeat("g:room-2").await.unwrap();
    let frame = h.gateway.recv_frame().await;
    assert_eq!(frame.cmd, biz_cmd::SEND_GROUP_HEARTBEAT);
    assert!(contains(&frame.data, "room-2"));
}

#[tokio::test]
async fn heartbeat_without_connection_reports_not_connected() {
    let sender = OutboundSender::new(make_conn(base_cfg()), None, "ak".into(), "b".into());
    let err = sender.start_heartbeat("user-1").await.unwrap_err();
    assert!(matches!(err, YuanbaoError::NotConnected));
    let err = sender.send_text("user-1", "x", None).await.unwrap_err();
    assert!(matches!(err, YuanbaoError::NotConnected));
}

#[tokio::test]
async fn query_group_info_decodes_reply() {
    let mut h = harness("cfg-bot").await;
    let mut gi = Vec::new();
    encode_field_string(1, "Team Room", &mut gi);
    encode_field_string(2, "owner-1", &mut gi);
    encode_field_string(3, "Owner", &mut gi);
    encode_field_varint(4, 12, &mut gi);
    let mut body = Vec::new();
    encode_field_varint(1, 0, &mut body);
    encode_field_string(2, "ok", &mut body);
    encode_field_bytes(3, &gi, &mut body);
    let (res, req) = round_trip(&mut h, 0, &body, |s| async move {
        s.query_group_info("room-5").await
    })
    .await;
    assert_eq!(req.cmd, biz_cmd::QUERY_GROUP_INFO);
    assert!(req.msg_id.starts_with("qgi_"));
    let info = res.unwrap();
    assert_eq!(info.group_name, "Team Room");
    assert_eq!(info.owner_id, "owner-1");
    assert_eq!(info.member_count, 12);
}

#[tokio::test]
async fn query_group_members_decodes_page() {
    let mut h = harness("cfg-bot").await;
    let mut member = Vec::new();
    encode_field_string(1, "u-1", &mut member);
    encode_field_string(2, "Alice", &mut member);
    let mut body = Vec::new();
    encode_field_varint(1, 0, &mut body);
    encode_field_bytes(3, &member, &mut body);
    encode_field_varint(4, 100, &mut body);
    encode_field_varint(5, 1, &mut body);
    let (res, req) = round_trip(&mut h, 0, &body, |s| async move {
        s.query_group_members("room-5", 0, 100).await
    })
    .await;
    assert_eq!(req.cmd, biz_cmd::GET_GROUP_MEMBER_LIST);
    let page = res.unwrap();
    assert_eq!(page.members.len(), 1);
    assert_eq!(page.members[0].user_id, "u-1");
    assert_eq!(page.members[0].nickname, "Alice");
    assert_eq!(page.next_offset, 100);
    assert!(page.is_complete);
}

fn png(width: u32, height: u32) -> Vec<u8> {
    let mut v = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    v.extend_from_slice(&width.to_be_bytes());
    v.extend_from_slice(&height.to_be_bytes());
    v
}

/// Mount download (HEAD+GET), `genUploadInfo` and the COS PUT on one
/// mock server; the COS PUT is reached through the endpoint override.
async fn mount_media(server: &wiremock::MockServer, download_ct: &str, bytes: Vec<u8>) {
    use wiremock::matchers::{header_exists, method, path};
    use wiremock::{Mock, ResponseTemplate};
    Mock::given(method("HEAD"))
        .and(path("/src/cat.png"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/src/cat.png"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(bytes, download_ct))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/resource/genUploadInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": 0,
            "data": {
                "bucketName": "bkt",
                "region": "ap-shanghai",
                "location": "up/cat.png",
                "encryptTmpSecretId": "AKID",
                "encryptTmpSecretKey": "SECRET",
                "encryptToken": "session",
                "resourceUrl": "https://cdn.example/cat.png",
            }
        })))
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/up/cat.png"))
        .and(header_exists("Authorization"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(server)
        .await;
}

async fn send_image_from_mock(download_ct: &str) -> ConnFrame {
    let server = wiremock::MockServer::start().await;
    mount_media(&server, download_ct, png(640, 480)).await;
    let mut h = harness("cfg-bot").await;
    let sender = Arc::new(
        OutboundSender::new(h.sender.conn.clone(), None, "ak".into(), "cfg-bot".into())
            .with_cos_endpoint(server.uri()),
    );
    h.sender = sender;
    let src = format!("{}/src/cat.png", server.uri());
    let api = server.uri();
    let (res, req) = round_trip(&mut h, 0, b"", move |s| async move {
        s.send_image_from_url("user-1", &src, "ak", "bot", "tok", &api, "", 5)
            .await
    })
    .await;
    res.unwrap();
    req
}

#[tokio::test]
async fn send_image_from_url_uploads_to_cos_and_sends_resource_url() {
    let req = send_image_from_mock("image/png").await;
    assert_eq!(req.cmd, biz_cmd::SEND_C2C_MESSAGE);
    assert!(contains(&req.data, "https://cdn.example/cat.png"));
    assert!(contains(&req.data, "cat.png"));
}

#[tokio::test]
async fn send_host_scoped_image_bytes_upload_to_cos_and_return_the_platform_id() {
    let server = wiremock::MockServer::start().await;
    mount_media(&server, "image/png", png(320, 200)).await;
    let mut h = harness("cfg-bot").await;
    h.sender = Arc::new(
        OutboundSender::new(h.sender.conn.clone(), None, "ak".into(), "cfg-bot".into())
            .with_cos_endpoint(server.uri()),
    );
    let api = server.uri();
    let bytes = png(320, 200);
    let (result, frame) = round_trip(&mut h, 0, b"", move |sender| async move {
        sender
            .send_media_bytes(
                "user-1",
                &bytes,
                "asset.png",
                "image/png",
                "ak",
                "bot",
                "tok",
                &api,
                "",
                Some("reply-1"),
            )
            .await
    })
    .await;

    let id = result.unwrap();
    assert!(!id.is_empty());
    assert_eq!(frame.cmd, biz_cmd::SEND_C2C_MESSAGE);
    assert!(contains(&frame.data, "https://cdn.example/cat.png"));
    assert!(contains(&frame.data, "asset.png"));
    assert!(contains(&frame.data, "reply-1"));
}

#[tokio::test]
async fn send_host_scoped_file_bytes_uses_the_file_message_element() {
    let server = wiremock::MockServer::start().await;
    mount_media(&server, "application/pdf", b"%PDF-1.7".to_vec()).await;
    let mut h = harness("cfg-bot").await;
    h.sender = Arc::new(
        OutboundSender::new(h.sender.conn.clone(), None, "ak".into(), "cfg-bot".into())
            .with_cos_endpoint(server.uri()),
    );
    let api = server.uri();
    let bytes = b"%PDF-1.7".to_vec();
    let (result, frame) = round_trip(&mut h, 0, b"", move |sender| async move {
        sender
            .send_media_bytes(
                "g:room",
                &bytes,
                "report.pdf",
                "application/pdf",
                "ak",
                "bot",
                "tok",
                &api,
                "",
                None,
            )
            .await
    })
    .await;

    assert!(result.is_ok());
    assert_eq!(frame.cmd, biz_cmd::SEND_GROUP_MESSAGE);
    assert!(contains(&frame.data, "TIMFileElem"));
    assert!(contains(&frame.data, "https://cdn.example/cat.png"));
    assert!(contains(&frame.data, "report.pdf"));
}

#[tokio::test]
async fn send_image_from_url_falls_back_to_downloaded_dimensions() {
    // A non-image content type means COS reports no dimensions, so the
    // sender keeps the ones parsed from the downloaded bytes.
    let req = send_image_from_mock("text/plain").await;
    assert!(contains(&req.data, "https://cdn.example/cat.png"));
}

#[tokio::test]
async fn send_image_from_url_surfaces_download_failure() {
    let server = wiremock::MockServer::start().await;
    let sender = OutboundSender::new(make_conn(base_cfg()), None, "ak".into(), "b".into())
        .with_cos_endpoint(server.uri());
    let src = format!("{}/missing.png", server.uri());
    let err = sender
        .send_image_from_url("user-1", &src, "ak", "bot", "tok", &server.uri(), "", 5)
        .await
        .unwrap_err();
    assert!(matches!(err, YuanbaoError::Media(ref m) if m.contains("404")));
}
