use super::*;
use std::sync::Arc;

struct FixtureChannel(&'static str, Option<bool>);

#[async_trait::async_trait]
impl tinychannels::channel::ChannelAdapter for FixtureChannel {
    fn descriptor(&self) -> tinychannels::channel::ChannelDescriptor {
        tinychannels::channel::ChannelDescriptor {
            id: self.0.into(),
            display_name: self.0.into(),
            ..Default::default()
        }
    }

    async fn start(
        &self,
        _: &(dyn tinychannels::channel::ChannelInboundSink + Send + Sync),
    ) -> Result<(), tinychannels::channel::ChannelSendError> {
        panic!("health checks must not start providers")
    }

    async fn stop(&self) -> Result<(), tinychannels::channel::ChannelSendError> {
        panic!("health checks must not stop providers")
    }

    async fn send(
        &self,
        _: tinychannels::channel::ChannelOutboundIntent,
    ) -> Result<tinychannels::channel::MessageReceipt, tinychannels::channel::ChannelSendError>
    {
        panic!("health checks must not send messages")
    }

    async fn status(
        &self,
    ) -> Result<
        tinychannels::controllers::ChannelAccountSnapshot,
        tinychannels::channel::ChannelSendError,
    > {
        match self.1 {
            Some(true) => Ok(tinychannels::controllers::ChannelAccountSnapshot {
                health_state: Some("healthy".into()),
                ..Default::default()
            }),
            Some(false) => Ok(tinychannels::controllers::ChannelAccountSnapshot {
                health_state: Some("unhealthy".into()),
                ..Default::default()
            }),
            None => std::future::pending().await,
        }
    }
}

#[tokio::test]
async fn module_health_checks_preserve_provider_order_and_bound_pending_calls() {
    let providers: Vec<tinychannels::factory::BuiltChannel> = vec![
        tinychannels::factory::BuiltChannel::from_adapter(Arc::new(FixtureChannel(
            "healthy",
            Some(true),
        ))),
        tinychannels::factory::BuiltChannel::from_adapter(Arc::new(FixtureChannel(
            "unhealthy",
            Some(false),
        ))),
        tinychannels::factory::BuiltChannel::from_adapter(Arc::new(FixtureChannel(
            "pending", None,
        ))),
    ];
    let reports = check_providers(&providers, Duration::ZERO).await;
    assert_eq!(reports.len(), 3);
    assert_eq!(reports[0].channel, "healthy");
    assert_eq!(
        reports[0].state,
        tinychannels_bus::ChannelHealthState::Healthy
    );
    assert_eq!(reports[1].channel, "unhealthy");
    assert_eq!(
        reports[1].state,
        tinychannels_bus::ChannelHealthState::Unhealthy
    );
    assert_eq!(reports[2].channel, "pending");
    assert_eq!(
        reports[2].state,
        tinychannels_bus::ChannelHealthState::Timeout
    );
}

#[tokio::test]
async fn module_health_checks_long_tail_adapter_status_as_unhealthy() {
    let config = tinychannels_bus::ChannelsConfig {
        irc: Some(tinychannels_bus::config::IrcConfig {
            server: "irc.example.test".into(),
            port: 6697,
            nickname: "openhuman".into(),
            username: None,
            channels: vec!["#test".into()],
            allowed_users: vec![],
            server_password: None,
            nickserv_password: None,
            sasl_password: None,
            verify_tls: Some(true),
        }),
        ..Default::default()
    };
    let adapter =
        tinychannels::build_long_tail_channel(&config, &tinychannels::DefaultHttpClients, "irc")
            .expect("configured IRC adapter");
    let provider = tinychannels::factory::BuiltChannel::from_adapter(adapter.clone());

    let reports = check_providers(&[provider], Duration::from_secs(1)).await;
    assert_eq!(reports[0].channel, "irc");
    assert_eq!(
        reports[0].state,
        tinychannels_bus::ChannelHealthState::Unhealthy,
        "the long-tail adapter reports unknown health as unhealthy"
    );
}

#[test]
fn module_health_budget_rejects_unbounded_or_zero_deadlines() {
    assert!(health_budget(0).is_err());
    assert!(health_budget(60_001).is_err());
    assert!(health_budget(u64::MAX).is_err());
    assert_eq!(health_budget(10_000).unwrap(), Duration::from_secs(10));
}
