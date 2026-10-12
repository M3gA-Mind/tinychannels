use super::*;

#[tokio::test]
async fn a_successful_probe_verifies() {
    let result = verify_with_probe(async { true }, "imap.example.com", Duration::from_secs(5)).await;
    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn a_failed_probe_reports_a_login_failure() {
    let error = verify_with_probe(async { false }, "imap.example.com", Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(error.starts_with("IMAP connection failed"), "{error}");
}

#[tokio::test]
async fn a_stalled_probe_reports_a_timeout_naming_the_host() {
    let error = verify_with_probe(
        std::future::pending::<bool>(),
        "imap.example.com",
        Duration::from_millis(10),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error,
        "IMAP connection to imap.example.com timed out — check the host and port"
    );
}

/// The public entry point probes the configured server: nothing listens on
/// the port, so the login fails rather than verifying.
#[tokio::test]
async fn unreachable_credentials_fail_verification() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let cfg = EmailConfig {
        imap_host: "127.0.0.1".to_string(),
        imap_port: port,
        ..Default::default()
    };
    let error = verify_email_credentials(&cfg).await.unwrap_err();
    assert!(error.starts_with("IMAP connection failed"), "{error}");
}
