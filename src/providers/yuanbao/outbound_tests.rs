use super::*;
use crate::host::{MediaRepository, MediaScope, MediaStoreRequest, StoredMedia};
use async_trait::async_trait;
use parking_lot::Mutex as ParkingMutex;
use sha1::Digest as _;
use tinychannels_bus::channel::{
    ChannelOutboundMessage, ContentPart, MediaKind, MediaReference, OutboundContentPart,
};

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
                None,
            )
            .await
    })
    .await;

    let id = result.unwrap();
    assert!(!id.is_empty());
    assert_eq!(frame.cmd, biz_cmd::SEND_C2C_MESSAGE);
    assert!(contains(&frame.data, "https://cdn.example/cat.png"));
    let image_uuid = hex::encode(sha1::Sha1::digest(png(320, 200)));
    assert!(contains(&frame.data, &image_uuid));
    let requests = server.received_requests().await.unwrap();
    let credential_request = requests
        .iter()
        .find(|request| request.url.path() == "/api/resource/genUploadInfo")
        .expect("COS credential request");
    let credential_body: serde_json::Value =
        serde_json::from_slice(&credential_request.body).expect("credential request JSON");
    assert_eq!(credential_body["fileName"], "asset.png");
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

#[derive(Default)]
struct TypedMediaRepo {
    scopes: ParkingMutex<Vec<MediaScope>>,
}

#[async_trait]
impl MediaRepository for TypedMediaRepo {
    async fn store_media(&self, _request: MediaStoreRequest) -> anyhow::Result<MediaReference> {
        anyhow::bail!("not used")
    }

    async fn read_media(
        &self,
        scope: MediaScope,
        opaque_id: String,
        _max_bytes: u64,
    ) -> anyhow::Result<StoredMedia> {
        self.scopes.lock().push(scope.clone());
        let (kind, content_type, file_name, bytes) = match opaque_id.as_str() {
            "image-opaque" => (MediaKind::Image, "image/png", "photo.png", png(320, 200)),
            "file-opaque" => (
                MediaKind::Document,
                "application/pdf",
                "report.pdf",
                b"%PDF-1.7 typed bytes".to_vec(),
            ),
            _ => anyhow::bail!("unknown opaque media reference"),
        };
        Ok(StoredMedia {
            media: MediaReference {
                opaque_id: Some(opaque_id),
                kind,
                ..Default::default()
            },
            content_type: content_type.into(),
            file_name: Some(file_name.into()),
            bytes,
        })
    }
}

#[tokio::test]
async fn typed_image_and_file_use_scoped_host_bytes_and_return_real_receipts() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, Request, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    let uploaded = Arc::new(ParkingMutex::new(Vec::<Vec<u8>>::new()));
    Mock::given(method("POST"))
        .and(path("/api/resource/genUploadInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": 0,
            "data": {
                "bucketName": "bucket",
                "region": "region",
                "location": "up/object",
                "encryptTmpSecretId": "key-id",
                "encryptTmpSecretKey": "key-secret",
                "encryptToken": "session",
                "resourceUrl": "https://cdn.example/uploaded",
            }
        })))
        .expect(2)
        .mount(&server)
        .await;
    let captured_uploads = uploaded.clone();
    Mock::given(method("PUT"))
        .and(path("/up/object"))
        .respond_with(move |request: &Request| {
            captured_uploads.lock().push(request.body.clone());
            ResponseTemplate::new(200)
        })
        .expect(2)
        .mount(&server)
        .await;

    let mut h = harness("cfg-bot").await;
    let repo = Arc::new(TypedMediaRepo::default());
    let mut config = super::super::config::YuanbaoConfig::default();
    config.app_key = "ak".into();
    config.bot_id = "cfg-bot".into();
    config.token = "tok".into();
    config.api_domain = server.uri();
    config.ws_domain = "wss://example.invalid".into();
    let channel = Arc::new(
        super::super::channel::YuanbaoChannel::new(config)
            .unwrap()
            .with_media_repository(repo.clone())
            .with_test_outbound(Arc::new(
                OutboundSender::new(h.sender.conn.clone(), None, "ak".into(), "cfg-bot".into())
                    .with_cos_endpoint(server.uri()),
            )),
    );
    let message = ChannelOutboundMessage {
        channel_id: "yuanbao".into(),
        conversation_id: "g:room-7".into(),
        channel_ref: Some(tinychannels_bus::ChannelRef {
            id: "yuanbao".into(),
            account_id: Some("account-2".into()),
        }),
        conversation_ref: Some(tinychannels_bus::ConversationRef {
            id: "g:room-7".into(),
            ..Default::default()
        }),
        reply_to_id: Some("reply-9".into()),
        thread_id: None,
        turn_id: Some("turn-4".into()),
        idempotency_key: "send-5".into(),
        parts: vec![
            OutboundContentPart::Content {
                part: ContentPart::Image {
                    media: MediaReference {
                        opaque_id: Some("image-opaque".into()),
                        kind: MediaKind::Image,
                        ..Default::default()
                    },
                    caption: Some("photo caption".into()),
                    alt_text: None,
                },
            },
            OutboundContentPart::Content {
                part: ContentPart::File {
                    media: MediaReference {
                        opaque_id: Some("file-opaque".into()),
                        kind: MediaKind::Document,
                        ..Default::default()
                    },
                    file_name: Some("report.pdf".into()),
                    caption: Some("file caption".into()),
                },
            },
        ],
    };
    let send = tokio::spawn(async move {
        crate::channel::ChannelTypedSend::send_typed(channel.as_ref(), &message).await
    });
    let first = tokio::time::timeout(HANG_GUARD, h.gateway.recv_frame())
        .await
        .expect("first typed media send did not reach the mock gateway");
    assert_eq!(first.cmd, biz_cmd::SEND_GROUP_MESSAGE);
    assert!(contains(&first.data, "https://cdn.example/uploaded"));
    assert!(contains(&first.data, "photo caption"));
    assert!(contains(&first.data, "reply-9"));
    h.gateway.reply(&first, 0, b"").await;
    let second = tokio::time::timeout(HANG_GUARD, h.gateway.recv_frame())
        .await
        .expect("second typed media send did not reach the mock gateway");
    assert_eq!(second.cmd, biz_cmd::SEND_GROUP_MESSAGE);
    assert!(contains(&second.data, "file caption"));
    assert!(contains(&second.data, "report.pdf"));
    assert!(contains(&second.data, "reply-9"));
    h.gateway.reply(&second, 0, b"").await;
    let result = tokio::time::timeout(HANG_GUARD, send)
        .await
        .expect("typed send hung")
        .expect("typed send task panicked")
        .unwrap();

    assert_eq!(result.receipt.parts.len(), 2);
    assert_eq!(result.receipt.platform_message_ids.len(), 2);
    assert_eq!(
        result
            .receipt
            .channel
            .as_ref()
            .unwrap()
            .account_id
            .as_deref(),
        Some("account-2")
    );
    assert_eq!(result.receipt.thread_id, None);
    assert_eq!(result.receipt.reply_to_id.as_deref(), Some("reply-9"));
    assert_eq!(repo.scopes.lock().len(), 2);
    assert!(repo.scopes.lock().iter().all(|scope| {
        scope.turn_id == "turn-4"
            && scope.channel.account_id.as_deref() == Some("account-2")
            && scope.conversation.id == "g:room-7"
            && scope.conversation.thread_id.is_none()
    }));
    assert_eq!(uploaded.lock().len(), 2);
    assert_eq!(uploaded.lock()[0], png(320, 200));
    assert_eq!(uploaded.lock()[1], b"%PDF-1.7 typed bytes");
}

#[tokio::test]
async fn typed_media_upload_failure_is_retryable_and_never_reaches_yuanbao_send() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/resource/genUploadInfo"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let mut config = super::super::config::YuanbaoConfig::default();
    config.app_key = "ak".into();
    config.bot_id = "cfg-bot".into();
    config.token = "tok".into();
    config.api_domain = server.uri();
    config.ws_domain = "wss://example.invalid".into();
    let channel = super::super::channel::YuanbaoChannel::new(config)
        .unwrap()
        .with_media_repository(Arc::new(TypedMediaRepo::default()))
        .with_test_outbound(Arc::new(
            OutboundSender::new(make_conn(base_cfg()), None, "ak".into(), "cfg-bot".into())
                .with_cos_endpoint(server.uri()),
        ));
    let message = ChannelOutboundMessage {
        channel_id: "yuanbao".into(),
        conversation_id: "user-4".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-4".into()),
        idempotency_key: "send-6".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("image-opaque".into()),
                    kind: MediaKind::Image,
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        }],
    };

    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert!(error.retryable);
    assert!(!error.sent_before_error);
    assert_eq!(
        error.message,
        "Yuanbao media preparation failed before message delivery"
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/api/resource/genUploadInfo");
}

#[tokio::test]
async fn yuanbao_rejects_thread_addressing_before_resolving_or_uploading_media() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/resource/genUploadInfo"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let repo = Arc::new(TypedMediaRepo::default());
    let mut config = super::super::config::YuanbaoConfig::default();
    config.app_key = "ak".into();
    config.bot_id = "cfg-bot".into();
    config.token = "tok".into();
    config.api_domain = server.uri();
    config.ws_domain = "wss://example.invalid".into();
    let channel = super::super::channel::YuanbaoChannel::new(config)
        .unwrap()
        .with_media_repository(repo.clone())
        .with_test_outbound(Arc::new(
            OutboundSender::new(make_conn(base_cfg()), None, "ak".into(), "cfg-bot".into())
                .with_cos_endpoint(server.uri()),
        ));
    let message = ChannelOutboundMessage {
        channel_id: "yuanbao".into(),
        conversation_id: "g:room-7".into(),
        channel_ref: None,
        conversation_ref: Some(tinychannels_bus::channel::ConversationRef {
            id: "g:room-7".into(),
            thread_id: Some("thread-root".into()),
            ..Default::default()
        }),
        reply_to_id: None,
        thread_id: None,
        turn_id: Some("turn-4".into()),
        idempotency_key: "send-thread-unsupported".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("image-opaque".into()),
                    kind: MediaKind::Image,
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        }],
    };

    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert!(error.unsupported);
    assert_eq!(error.message, "Yuanbao does not support threads");
    assert!(
        repo.scopes.lock().is_empty(),
        "media resolution must not run"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn yuanbao_rejects_direct_message_reply_before_resolving_or_uploading_media() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/resource/genUploadInfo"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let repo = Arc::new(TypedMediaRepo::default());
    let mut config = super::super::config::YuanbaoConfig::default();
    config.app_key = "ak".into();
    config.bot_id = "cfg-bot".into();
    config.token = "tok".into();
    config.api_domain = server.uri();
    config.ws_domain = "wss://example.invalid".into();
    let channel = super::super::channel::YuanbaoChannel::new(config)
        .unwrap()
        .with_media_repository(repo.clone())
        .with_test_outbound(Arc::new(
            OutboundSender::new(make_conn(base_cfg()), None, "ak".into(), "cfg-bot".into())
                .with_cos_endpoint(server.uri()),
        ));
    let message = ChannelOutboundMessage {
        channel_id: "yuanbao".into(),
        conversation_id: "user-4".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: Some("reply-9".into()),
        thread_id: None,
        turn_id: Some("turn-4".into()),
        idempotency_key: "send-dm-reply-unsupported".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Image {
                media: MediaReference {
                    opaque_id: Some("image-opaque".into()),
                    kind: MediaKind::Image,
                    ..Default::default()
                },
                caption: None,
                alt_text: None,
            },
        }],
    };

    let error = crate::channel::ChannelTypedSend::send_typed(&channel, &message)
        .await
        .unwrap_err();
    assert!(error.unsupported);
    assert_eq!(error.message, "Yuanbao direct-message reply addressing");
    assert!(
        repo.scopes.lock().is_empty(),
        "media resolution must not run"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_final_write_rejection_reports_uncertain_delivery() {
    let mut h = harness("cfg-bot").await;
    let channel = super::super::channel::YuanbaoChannel::new(base_cfg())
        .unwrap()
        .with_test_outbound(h.sender.clone());
    let message = ChannelOutboundMessage {
        channel_id: "yuanbao".into(),
        conversation_id: "user-4".into(),
        channel_ref: None,
        conversation_ref: None,
        reply_to_id: None,
        thread_id: None,
        turn_id: None,
        idempotency_key: "send-7".into(),
        parts: vec![OutboundContentPart::Content {
            part: ContentPart::Text {
                text: "hello".into(),
                markdown_dialect: None,
            },
        }],
    };
    let send = tokio::spawn(async move {
        crate::channel::ChannelTypedSend::send_typed(&channel, &message).await
    });
    let request = h.gateway.recv_frame().await;
    h.gateway.reply(&request, 7, b"").await;
    let error = tokio::time::timeout(HANG_GUARD, send)
        .await
        .expect("typed send hung")
        .expect("typed send task panicked")
        .unwrap_err();
    assert!(!error.retryable);
    assert!(error.sent_before_error);
    assert_eq!(error.message, "Yuanbao send outcome is uncertain");
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
