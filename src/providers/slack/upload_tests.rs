use super::*;
use crate::providers::slack::fixtures::*;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn start_ok(server: &MockServer) {
    mount(
        server,
        "files.getUploadURLExternal",
        ok(json!({"upload_url": format!("{}/upload/F1", server.uri()), "file_id": "F1"})),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/upload/F1"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
        .mount(server)
        .await;
}

fn upload<'a>(comment: Option<&'a str>, thread: Option<&'a str>) -> Upload<'a> {
    Upload {
        channel: "C1",
        thread_ts: thread,
        bytes: b"hello".to_vec(),
        file_name: "note.txt",
        content_type: "text/plain",
        comment,
    }
}

#[tokio::test]
async fn an_upload_runs_the_three_step_flow_and_reports_the_message_timestamp() {
    let server = mock_api().await;
    start_ok(&server).await;
    mount(
        &server,
        "files.completeUploadExternal",
        ok(json!({"files": [{"id": "F1", "shares": {"public": {"C1": [{"ts": "7.7"}]}}}]})),
    )
    .await;
    let channel = channel_for(&server, &[]);
    let done = channel
        .upload_file(upload(Some("caption"), Some("1.0")))
        .await
        .unwrap();
    assert_eq!(
        (done.file_id.as_str(), done.message_ts.as_deref()),
        ("F1", Some("7.7"))
    );
    let start = &bodies(&server, "files.getUploadURLExternal").await[0];
    assert_eq!(
        (start["filename"].as_str(), start["length"].as_str()),
        (Some("note.txt"), Some("5"))
    );
    let complete = &bodies(&server, "files.completeUploadExternal").await[0];
    assert_eq!(complete["channel_id"], "C1");
    assert_eq!(complete["thread_ts"], "1.0");
    assert_eq!(complete["initial_comment"], "caption");
    assert_eq!(complete["files"][0]["id"], "F1");
    let requests = server.received_requests().await.unwrap();
    let raw = requests
        .iter()
        .find(|r| r.url.path() == "/upload/F1")
        .unwrap();
    assert_eq!(raw.body, b"hello");
    assert!(
        raw.headers.get("authorization").is_none(),
        "the token must not reach the upload host"
    );
    assert_eq!(raw.headers.get("content-type").unwrap(), "text/plain");
}

#[tokio::test]
async fn without_shares_the_file_id_stands_in_for_the_message() {
    let server = mock_api().await;
    start_ok(&server).await;
    mount(&server, "files.completeUploadExternal", ok(json!({}))).await;
    let channel = channel_for(&server, &[]);
    let done = channel.upload_file(upload(Some("  "), None)).await.unwrap();
    assert_eq!((done.file_id.as_str(), done.message_ts), ("F1", None));
    let complete = &bodies(&server, "files.completeUploadExternal").await[0];
    assert!(complete.get("thread_ts").is_none() && complete.get("initial_comment").is_none());
}

#[tokio::test]
async fn a_refused_start_is_not_a_send_and_a_missing_target_is_an_error() {
    let server = mock_api().await;
    mount(
        &server,
        "files.getUploadURLExternal",
        json!({"ok": false, "error": "missing_scope"}),
    )
    .await;
    let channel = channel_for(&server, &[]);
    let error = channel.upload_file(upload(None, None)).await.unwrap_err();
    assert_eq!(error.kind, SendErrorKind::Forbidden);
    assert!(!error.sent_before_error);

    let bare = mock_api().await;
    mount(&bare, "files.getUploadURLExternal", ok(json!({}))).await;
    let error = channel_for(&bare, &[])
        .upload_file(upload(None, None))
        .await
        .unwrap_err();
    assert!(error.message.contains("no upload target"));

    let dead = SlackChannel::new("t".into(), None, vec![]).with_api_base("http://127.0.0.1:1/api");
    let error = dead.upload_file(upload(None, None)).await.unwrap_err();
    assert!(
        error.retryable && !error.sent_before_error,
        "nothing reached Slack, so a retry is safe"
    );
}

#[tokio::test]
async fn rejected_bytes_and_a_failed_completion_are_reported() {
    let server = mock_api().await;
    mount(
        &server,
        "files.getUploadURLExternal",
        ok(json!({"upload_url": format!("{}/upload/F1", server.uri()), "file_id": "F1"})),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/upload/F1"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let channel = channel_for(&server, &[]);
    let error = channel.upload_file(upload(None, None)).await.unwrap_err();
    assert!(error.retryable && error.message.contains("not accepted"));

    let server = mock_api().await;
    start_ok(&server).await;
    mount(
        &server,
        "files.completeUploadExternal",
        json!({"ok": false, "error": "channel_not_found"}),
    )
    .await;
    let error = channel_for(&server, &[])
        .upload_file(upload(None, None))
        .await
        .unwrap_err();
    assert_eq!(error.kind, SendErrorKind::NotFound);
}
