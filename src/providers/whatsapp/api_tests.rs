use super::*;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn channel(server: &MockServer) -> WhatsAppChannel {
    WhatsAppChannel::new(
        "tok".into(),
        "123".into(),
        "verify".into(),
        vec!["*".into()],
    )
    .with_api_base_for_test(&server.uri())
}

#[test]
fn graph_base_is_trimmed_and_defaults_when_blank() {
    assert_eq!(resolve_graph_base(None), "https://graph.facebook.com");
    assert_eq!(
        resolve_graph_base(Some("  ".into())),
        "https://graph.facebook.com"
    );
    assert_eq!(
        resolve_graph_base(Some(" http://127.0.0.1:9/ ".into())),
        "http://127.0.0.1:9"
    );
}

#[test]
fn cleartext_is_only_allowed_on_loopback() {
    for url in [
        "https://graph.facebook.com/v23.0/1",
        "http://127.0.0.1:8080/x",
        "http://localhost/x",
        "http://[::1]:80/x",
    ] {
        assert!(ensure_secure(url).is_ok(), "{url}");
    }
    for url in [
        "http://graph.facebook.com/x",
        "http://10.0.0.5/x",
        "ftp://127.0.0.1/x",
        "not a url",
        "",
    ] {
        assert!(ensure_secure(url).is_err(), "{url}");
    }
}

#[test]
fn urls_name_the_pinned_graph_version() {
    let channel = WhatsAppChannel::new("t".into(), "123".into(), "v".into(), vec![]);
    assert_eq!(
        channel.graph_url("/123/messages"),
        format!("https://graph.facebook.com/{GRAPH_VERSION}/123/messages")
    );
}

#[test]
fn answered_ids_are_never_inferred() {
    let ok = serde_json::json!({"messages": [{"id": "wamid.9"}]});
    assert_eq!(
        WhatsAppChannel::answered_message_id(&ok).unwrap(),
        "wamid.9"
    );
    for bad in [
        serde_json::json!({}),
        serde_json::json!({"messages": []}),
        serde_json::json!({"messages": [{"id": " "}]}),
        serde_json::json!({"messages": [{}]}),
    ] {
        assert!(matches!(
            WhatsAppChannel::answered_message_id(&bad),
            Err(WaFailure::Uncertain(_))
        ));
    }
}

#[tokio::test]
async fn posting_messages_authenticates_with_a_bearer_header_only() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/{GRAPH_VERSION}/123/messages")))
        .and(header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "wamid.1"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let answer = channel(&server)
        .post_messages(&serde_json::json!({"hello": "world"}))
        .await
        .unwrap();
    assert_eq!(answer["messages"][0]["id"], "wamid.1");
    let requests = server.received_requests().await.unwrap();
    assert!(
        !requests[0].url.as_str().contains("tok"),
        "token never in the URL"
    );
}

#[tokio::test]
async fn send_failures_are_classified_by_whether_the_account_was_touched() {
    let server = MockServer::start().await;
    for (status, rejected) in [
        (400u16, true),
        (401, true),
        (429, true),
        (408, false),
        (500, false),
    ] {
        server.reset().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let result = channel(&server).post_messages(&serde_json::json!({})).await;
        match (result, rejected) {
            (Err(WaFailure::Rejected(_)), true) | (Err(WaFailure::Uncertain(_)), false) => {}
            (other, _) => panic!("status {status}: {other:?}"),
        }
    }
    server.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;
    assert!(matches!(
        channel(&server).post_messages(&serde_json::json!({})).await,
        Err(WaFailure::Uncertain(_))
    ));
}

#[tokio::test]
async fn transport_failures_and_insecure_bases_are_handled() {
    let unreachable = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![])
        .with_api_base_for_test("http://127.0.0.1:1");
    assert!(matches!(
        unreachable.post_messages(&serde_json::json!({})).await,
        Err(WaFailure::Uncertain(_))
    ));
    assert!(matches!(
        unreachable
            .upload_media(vec![1], "image/png", "a.png")
            .await,
        Err(WaFailure::Rejected(_))
    ));
    let insecure = WhatsAppChannel::new("t".into(), "1".into(), "v".into(), vec![])
        .with_api_base_for_test("http://graph.example.invalid");
    assert!(matches!(
        insecure.post_messages(&serde_json::json!({})).await,
        Err(WaFailure::Rejected(_))
    ));
    assert!(matches!(
        insecure.download_media("abc", 10).await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn uploads_send_multipart_and_return_the_media_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/{GRAPH_VERSION}/123/media")))
        .and(header("authorization", "Bearer tok"))
        .and(body_string_contains("messaging_product"))
        .and(body_string_contains("image/png"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": "media-7"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let id = channel(&server)
        .upload_media(vec![1, 2, 3], "image/png", "a.png")
        .await
        .unwrap();
    assert_eq!(id, "media-7");
}

#[tokio::test]
async fn failed_uploads_are_always_retry_safe() {
    let server = MockServer::start().await;
    for response in [
        ResponseTemplate::new(500),
        ResponseTemplate::new(400),
        ResponseTemplate::new(200).set_body_string("nope"),
        ResponseTemplate::new(200).set_body_json(serde_json::json!({})),
        ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": " "})),
    ] {
        server.reset().await;
        Mock::given(method("POST"))
            .respond_with(response)
            .mount(&server)
            .await;
        assert!(matches!(
            channel(&server)
                .upload_media(vec![1], "image/png", "a")
                .await,
            Err(WaFailure::Rejected(_))
        ));
    }
    assert!(matches!(
        channel(&server)
            .upload_media(vec![1], "bad mime", "a")
            .await,
        Err(WaFailure::Rejected(_))
    ));
}

async fn mount_lookup(server: &MockServer, media_id: &str, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path(format!("/{GRAPH_VERSION}/{media_id}")))
        .and(header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn downloads_resolve_the_media_url_then_fetch_with_the_bearer_token() {
    let server = MockServer::start().await;
    mount_lookup(
        &server,
        "m1",
        serde_json::json!({
            "url": format!("{}/cdn/m1", server.uri()),
            "mime_type": "image/png",
            "file_size": 4
        }),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/cdn/m1"))
        .and(header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![9u8, 8, 7, 6]))
        .expect(1)
        .mount(&server)
        .await;
    let downloaded = channel(&server).download_media("m1", 1024).await.unwrap();
    assert_eq!(downloaded.bytes, [9, 8, 7, 6]);
    assert_eq!(downloaded.content_type, "image/png");
}

#[tokio::test]
async fn downloads_refuse_malformed_ids_and_oversized_media() {
    let server = MockServer::start().await;
    for bad in ["", "../etc", "a/b", "a b"] {
        assert!(matches!(
            channel(&server).download_media(bad, 10).await,
            Err(WaFailure::Rejected(_))
        ));
    }
    mount_lookup(
        &server,
        "big",
        serde_json::json!({"url": format!("{}/cdn/big", server.uri()), "file_size": 5000}),
    )
    .await;
    assert!(matches!(
        channel(&server).download_media("big", 100).await,
        Err(WaFailure::Rejected(_))
    ));
    mount_lookup(
        &server,
        "lying",
        serde_json::json!({"url": format!("{}/cdn/lying", server.uri()), "file_size": 1}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/cdn/lying"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 500]))
        .mount(&server)
        .await;
    assert!(matches!(
        channel(&server).download_media("lying", 100).await,
        Err(WaFailure::Rejected(_))
    ));
}

#[tokio::test]
async fn download_failures_are_classified() {
    let server = MockServer::start().await;
    // Lookup statuses.
    for (status, rejected) in [(404u16, true), (500, false)] {
        server.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let result = channel(&server).download_media("m", 10).await;
        match (result, rejected) {
            (Err(WaFailure::Rejected(_)), true) | (Err(WaFailure::Uncertain(_)), false) => {}
            (other, _) => panic!("lookup {status}: {other:?}"),
        }
    }
    // Unreadable lookup body, missing URL, insecure URL.
    server.reset().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("nope"))
        .mount(&server)
        .await;
    assert!(matches!(
        channel(&server).download_media("m", 10).await,
        Err(WaFailure::Uncertain(_))
    ));
    for body in [
        serde_json::json!({}),
        serde_json::json!({"url": "http://evil.example.invalid/x"}),
    ] {
        server.reset().await;
        mount_lookup(&server, "m", body).await;
        assert!(matches!(
            channel(&server).download_media("m", 10).await,
            Err(WaFailure::Rejected(_))
        ));
    }
    // CDN status and connection failures.
    server.reset().await;
    mount_lookup(
        &server,
        "m",
        serde_json::json!({"url": format!("{}/cdn/m", server.uri())}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/cdn/m"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    assert!(matches!(
        channel(&server).download_media("m", 10).await,
        Err(WaFailure::Uncertain(_))
    ));
    server.reset().await;
    mount_lookup(
        &server,
        "m",
        serde_json::json!({"url": "http://127.0.0.1:1/cdn"}),
    )
    .await;
    assert!(matches!(
        channel(&server).download_media("m", 10).await,
        Err(WaFailure::Uncertain(_))
    ));
}
