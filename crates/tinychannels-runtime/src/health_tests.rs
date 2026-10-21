use super::*;

#[test]
fn health_probe_results_are_classified_without_losing_timeout_state() {
    assert_eq!(
        classify_health_result(&Ok(true)),
        ChannelHealthState::Healthy
    );
    assert_eq!(
        classify_health_result(&Ok(false)),
        ChannelHealthState::Unhealthy
    );

    let timed_out = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime")
        .block_on(async {
            tokio::time::timeout(std::time::Duration::ZERO, std::future::pending::<bool>()).await
        });
    assert_eq!(
        classify_health_result(&timed_out),
        ChannelHealthState::Timeout
    );
}
