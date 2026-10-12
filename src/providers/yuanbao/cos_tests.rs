use super::*;

#[test]
fn cos_sign_is_deterministic() {
    let s = cos_sign(&CosSignInput {
        method: "put",
        path: "/test/file.bin",
        params: &[],
        headers: &[("host", "bucket.cos.example.com")],
        secret_id: "AKID",
        secret_key: "SK",
        start_time: 1_700_000_000,
        expire_seconds: 3600,
    });
    let s2 = cos_sign(&CosSignInput {
        method: "put",
        path: "/test/file.bin",
        params: &[],
        headers: &[("host", "bucket.cos.example.com")],
        secret_id: "AKID",
        secret_key: "SK",
        start_time: 1_700_000_000,
        expire_seconds: 3600,
    });
    assert_eq!(s, s2);
    assert!(s.starts_with("q-sign-algorithm=sha1"));
    assert!(s.contains("q-ak=AKID"));
    assert!(s.contains("q-sign-time=1700000000;1700003600"));
}

#[test]
fn cos_sign_changes_with_path() {
    let base = CosSignInput {
        method: "put",
        path: "/a",
        params: &[],
        headers: &[("host", "h")],
        secret_id: "AKID",
        secret_key: "SK",
        start_time: 1_700_000_000,
        expire_seconds: 3600,
    };
    let s1 = cos_sign(&base);
    let s2 = cos_sign(&CosSignInput { path: "/b", ..base });
    assert_ne!(s1, s2);
}

#[test]
fn cos_sign_lowercases_method_and_includes_url_params() {
    let s = cos_sign(&CosSignInput {
        method: "PUT", // mixed case → should be lowercased into sig
        path: "/k",
        params: &[("Foo", "Bar Baz")], // url-encoded value
        headers: &[("Host", "h")],
        secret_id: "AKID",
        secret_key: "SK",
        start_time: 1_700_000_000,
        expire_seconds: 600,
    });
    assert!(s.contains("q-url-param-list=foo"));
    // header list also lowercased
    assert!(s.contains("q-header-list=host"));
}

fn ok_credentials_body(bucket: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "code": 0,
        "data": {
            "bucketName": bucket,
            "region": "ap-shanghai",
            "location": location,
            "encryptTmpSecretId": "AKID",
            "encryptTmpSecretKey": "SECRET",
            "encryptToken": "session-token",
            "startTime": 1_700_000_000u64,
            "expiredTime": 1_700_003_600u64,
            "resourceUrl": "https://cdn.example/r",
        }
    })
}

#[tokio::test]
async fn get_cos_credentials_parses_data_block() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path(UPLOAD_INFO_PATH))
        .and(wiremock::matchers::header("X-Token", "tok"))
        .and(wiremock::matchers::header("X-Source", "web"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(ok_credentials_body("bkt-1", "k/v/file.png")),
        )
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let creds = get_cos_credentials(&http, &server.uri(), "appk", "bot", "tok", "", "file.png")
        .await
        .unwrap();
    assert_eq!(creds.bucket, "bkt-1");
    assert_eq!(creds.region, "ap-shanghai");
    assert_eq!(creds.location, "k/v/file.png");
    assert_eq!(creds.secret_id, "AKID");
    assert_eq!(creds.secret_key, "SECRET");
    assert_eq!(creds.session_token, "session-token");
    assert_eq!(creds.resource_url, "https://cdn.example/r");
    assert_eq!(creds.start_time, 1_700_000_000);
    assert_eq!(creds.expired_time, 1_700_003_600);
}

#[tokio::test]
async fn get_cos_credentials_falls_back_to_app_key_for_xid_when_bot_id_empty() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path(UPLOAD_INFO_PATH))
        .and(wiremock::matchers::header("X-ID", "appk"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(ok_credentials_body("bkt", "loc")),
        )
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let creds = get_cos_credentials(&http, &server.uri(), "appk", "", "tok", "", "f")
        .await
        .unwrap();
    assert_eq!(creds.bucket, "bkt");
}

#[tokio::test]
async fn get_cos_credentials_sends_route_env_header_when_non_empty() {
    let server = wiremock::MockServer::start().await;
    // Bind the matcher to both the upload-info path AND the header so
    // this test fails if a future refactor routes the call elsewhere
    // but happens to still attach `X-Route-Env: canary` somewhere.
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path(UPLOAD_INFO_PATH))
        .and(wiremock::matchers::header("X-Route-Env", "canary"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(ok_credentials_body("bkt", "loc")),
        )
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    get_cos_credentials(&http, &server.uri(), "appk", "bot", "tok", "canary", "f")
        .await
        .expect("should send canary header");
}

#[tokio::test]
async fn get_cos_credentials_surfaces_http_error() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let err = get_cos_credentials(&http, &server.uri(), "appk", "bot", "tok", "", "f")
        .await
        .unwrap_err();
    match err {
        YuanbaoError::Media(m) => assert!(m.contains("HTTP 500"), "got {m}"),
        other => panic!("expected Media error, got {other:?}"),
    }
}

#[tokio::test]
async fn get_cos_credentials_surfaces_non_zero_business_code() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 4001,
                "msg": "quota",
            })),
        )
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let err = get_cos_credentials(&http, &server.uri(), "appk", "bot", "tok", "", "f")
        .await
        .unwrap_err();
    match err {
        YuanbaoError::Media(m) => {
            assert!(m.contains("code=4001"), "got {m}");
            assert!(m.contains("quota"), "got {m}");
        }
        other => panic!("expected Media error, got {other:?}"),
    }
}

#[tokio::test]
async fn upload_to_cos_rejects_missing_credentials() {
    let http = reqwest::Client::new();
    // empty credentials → fail without making any HTTP call
    let bad = CosCredentials::default();
    let err = upload_to_cos(
        &http,
        &bad,
        b"data",
        "f.bin",
        "application/octet-stream".into(),
    )
    .await
    .unwrap_err();
    match err {
        YuanbaoError::Media(m) => assert!(m.contains("credentials missing"), "got {m}"),
        other => panic!("expected Media error, got {other:?}"),
    }
}

// NOTE: upload_to_cos always targets `<bucket>.cos.accelerate.myqcloud.com`
// which we cannot redirect at the reqwest layer without DNS hacks, so we
// only cover the guard branch (missing creds) above. The PUT body itself
// is exercised by integration tests, not unit tests.

#[test]
fn encode_cos_key_keeps_slashes_but_escapes_segments() {
    assert_eq!(encode_cos_key("plain/file.png"), "plain/file.png");
    assert_eq!(encode_cos_key("a b/c d.png"), "a%20b/c%20d.png");
}

fn upload_creds(resource_url: &str, start_time: u64, expired_time: u64) -> CosCredentials {
    CosCredentials {
        bucket: "bkt".into(),
        region: "ap-shanghai".into(),
        location: "/dir/a b.png".into(),
        secret_id: "AKID".into(),
        secret_key: "SECRET".into(),
        session_token: "session".into(),
        start_time,
        expired_time,
        resource_url: resource_url.into(),
    }
}

fn png(width: u32, height: u32) -> Vec<u8> {
    let mut v = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    v.extend_from_slice(&width.to_be_bytes());
    v.extend_from_slice(&height.to_be_bytes());
    v
}

#[tokio::test]
async fn upload_to_cos_puts_signed_object_and_reports_image_dims() {
    use wiremock::matchers::{body_bytes, header, method, path};
    let server = wiremock::MockServer::start().await;
    let data = png(32, 16);
    wiremock::Mock::given(method("PUT"))
        .and(path("/dir/a%20b.png"))
        .and(header("Content-Type", "image/png"))
        .and(header("x-cos-security-token", "session"))
        .and(body_bytes(data.clone()))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    // octet-stream + an image filename → content type guessed from the name;
    // no resource_url → the canonical COS URL is returned.
    let res = upload_to_cos_via(
        &http,
        &upload_creds("", 0, 0),
        &data,
        "a b.png",
        "application/octet-stream".into(),
        Some(&server.uri()),
    )
    .await
    .unwrap();
    assert_eq!(
        res.url,
        "https://bkt.cos.accelerate.myqcloud.com/dir/a%20b.png"
    );
    assert_eq!((res.width, res.height), (32, 16));
    assert_eq!(res.size, data.len() as u64);
    assert_eq!(res.uuid, sha1_hex(&data));
}

#[tokio::test]
async fn upload_to_cos_non_image_uses_resource_url_and_no_dims() {
    use wiremock::matchers::{header, method};
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("PUT"))
        .and(header("Content-Type", "application/octet-stream"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let far_future = unix_now() + 7200;
    let res = upload_to_cos_via(
        &http,
        &upload_creds("https://cdn.example/r", 1_700_000_000, far_future),
        b"plain bytes",
        "notes.bin",
        String::new(),
        Some(&server.uri()),
    )
    .await
    .unwrap();
    assert_eq!(res.url, "https://cdn.example/r");
    assert_eq!((res.width, res.height), (0, 0));
}

#[tokio::test]
async fn upload_to_cos_surfaces_put_http_error() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("PUT"))
        .respond_with(wiremock::ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let err = upload_to_cos_via(
        &http,
        &upload_creds("", 0, 0),
        b"x",
        "f.txt",
        "text/plain".into(),
        Some(&server.uri()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, YuanbaoError::Media(ref m) if m.contains("COS PUT HTTP 403")));
}

/// An origin nothing listens on: bind an ephemeral port, then free it.
fn dead_origin() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("http://{}", l.local_addr().unwrap())
}

#[tokio::test]
async fn upload_and_credentials_surface_connection_errors() {
    let http = reqwest::Client::new();
    let origin = dead_origin();
    let err = upload_to_cos_via(
        &http,
        &upload_creds("", 0, 0),
        b"x",
        "f.txt",
        "text/plain".into(),
        Some(&origin),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, YuanbaoError::Connection(ref m) if m.contains("COS PUT")));

    let err = get_cos_credentials(&http, &origin, "appk", "bot", "tok", "", "f")
        .await
        .unwrap_err();
    assert!(matches!(err, YuanbaoError::Connection(ref m) if m.contains("genUploadInfo")));
}

#[tokio::test]
async fn upload_to_cos_without_override_rejects_missing_creds_before_network() {
    let http = reqwest::Client::new();
    let err = upload_to_cos(&http, &CosCredentials::default(), b"x", "f", String::new())
        .await
        .unwrap_err();
    assert!(matches!(err, YuanbaoError::Media(_)));
}

#[tokio::test]
async fn get_cos_credentials_rejects_non_json_body() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let err = get_cos_credentials(&http, &server.uri(), "appk", "bot", "tok", "", "f")
        .await
        .unwrap_err();
    assert!(matches!(err, YuanbaoError::Media(ref m) if m.contains("body parse")));
}
