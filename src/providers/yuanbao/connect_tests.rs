use super::*;
use serde_json::json;

fn creds(v: Value) -> serde_json::Map<String, Value> {
    v.as_object().cloned().unwrap()
}

#[test]
fn require_field_trims_and_rejects_blank() {
    let c = creds(json!({ "app_key": "  k1 ", "blank": "   ", "num": 5 }));
    assert_eq!(require_yuanbao_field(&c, "app_key").unwrap(), "k1");
    assert_eq!(
        require_yuanbao_field(&c, "blank").unwrap_err(),
        "missing required blank"
    );
    assert!(require_yuanbao_field(&c, "num").is_err());
    assert!(require_yuanbao_field(&c, "absent").is_err());
}

#[test]
fn effective_config_overlays_overrides_and_clears_secret() {
    let base = YuanbaoConfig {
        app_secret: "should-not-survive".into(),
        ..Default::default()
    };
    let c = creds(json!({
        "env": "pre",
        "api_domain": " api.example ",
        "ws_domain": "ws.example",
        "route_env": "",
    }));
    let cfg = build_effective_yuanbao_config(base, &c, "key-1".into());
    assert_eq!(cfg.app_key, "key-1");
    assert!(cfg.app_secret.is_empty());
    assert_eq!(cfg.env, "pre");
    assert_eq!(cfg.api_domain, "api.example");
    assert_eq!(cfg.ws_domain, "ws.example");
}

#[test]
fn effective_config_without_overrides_keeps_base_routes() {
    let mut base = YuanbaoConfig::default();
    base.api_domain = "custom.example".into();
    let cfg = build_effective_yuanbao_config(base, &creds(json!({})), "k".into());
    assert_eq!(cfg.api_domain, "custom.example");
}

fn verify_config(api_domain: &str) -> YuanbaoConfig {
    let mut c = YuanbaoConfig::default();
    c.app_key = "key-1".into();
    c.api_domain = api_domain.into();
    c.route_env = "canary".into();
    c
}

#[tokio::test]
async fn verify_credentials_accepts_valid_sign_token_response() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/api/v5/robotLogic/sign-token"))
        .and(wiremock::matchers::header("X-Route-Env", "canary"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "data": {"token": "t", "bot_id": "b", "duration": 600},
        })))
        .expect(1)
        .mount(&server)
        .await;
    verify_yuanbao_credentials(
        reqwest::Client::new(),
        &verify_config(&server.uri()),
        "secret",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn verify_credentials_reports_rejected_secret() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(json!({"code": 40001, "msg": "bad signature"})),
        )
        .mount(&server)
        .await;
    let err = verify_yuanbao_credentials(
        reqwest::Client::new(),
        &verify_config(&server.uri()),
        "nope",
    )
    .await
    .unwrap_err();
    assert!(
        err.starts_with("yuanbao credential verification failed:"),
        "{err}"
    );
    assert!(err.contains("bad signature"), "{err}");
}
